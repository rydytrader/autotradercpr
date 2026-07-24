package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.StrangleAdjustStreamBroker;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NIFTY intraday neutral-to-directional strangleAdjust recovery. Runs every trading
 * day (instrument + weekday routing were removed once the strategy was locked to
 * NIFTY-only).
 *
 * <p><b>Entry (default 09:20 IST)</b> — SELL the NIFTY CE and PE strikes whose
 * current LTP is closest to the target premium (default ₹50). Each leg's SL price
 * is {@code entryPremium × slMultiplier} (default 2.0 = 100 % of received premium).
 *
 * <p><b>Adjustment (on either leg's SL hit)</b> — close the SL-hit leg, then on the
 * OPPOSITE side (a) BUY a deep-OTM hedge (default 10 strike-steps OTM, 2× base qty)
 * and (b) SELL a new leg at ~target premium. The hedge + new sell together form a
 * defined-risk vertical spread on that side. One-shot: if the adjustment leg's own
 * SL fires, close it without another cycle.
 *
 * <p><b>Squareoff (default 15:15 IST)</b> — flatten every open position (shorts and
 * hedges) at market.
 *
 * <p><b>Analytics</b> — reports one trade per session day, not per leg — see
 * {@link #aggregatesToDay()}.
 */
@Service
public class StrangleAdjust implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(StrangleAdjust.class);
    private static final String STRATEGY_ID = "strangle-adjust";
    /** Strategy ID written to DB rows for MANUAL-tagged trades. */
    public  static final String MANUAL_STRATEGY_ID = "manual";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/strangle-adjust-state.json";
    private static final String LEGACY_STATE_FILE = "../store/cache/strangle-state.json";
    private static final double OPTION_TICK_SIZE = 0.05;
    private static final int    RECENT_EVENTS_LIMIT = 60;

    /** Per-instrument contract specs. Change rarely at the exchange level — hardcoded.
     *  NIFTY-only after the strategy was locked to a single instrument. */
    public enum InstrumentSpec {
        NIFTY ("NSE:NIFTY50-INDEX", 65L,  50L);

        public final String spotSymbol;
        public final long   lotSize;
        public final long   strikeStep;

        InstrumentSpec(String spotSymbol, long lotSize, long strikeStep) {
            this.spotSymbol = spotSymbol;
            this.lotSize    = lotSize;
            this.strikeStep = strikeStep;
        }
    }

    /** Single setup name for the whole strategy. All legs (CE/PE entry, adjustment,
     *  hedge) share this. Analytics aggregates to day-level so per-leg categorization
     *  has no downstream consumer. */
    public enum ActiveSetup {
        STRANGLE,
        MANUAL
    }

    private static String posKey(Position p) {
        if (p == null) return "";
        String setup = p.setup == null ? "MANUAL" : p.setup.name();
        return setup + "|" + (p.symbol == null ? "" : p.symbol);
    }

    private final BalancedAtmSelector   atmSelector;
    private final MarketDataService     marketDataService;
    private final OrderService          orderService;
    private final EventService          eventService;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectProvider<StrangleAdjustStreamBroker>    streamBrokerProvider;

    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

    private volatile State state = new State();

    public StrangleAdjust(BalancedAtmSelector atmSelector,
                    MarketDataService marketDataService,
                    OrderService orderService,
                    EventService eventService,
                    RiskSettingsStore riskSettings,
                    ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                    ObjectProvider<StrangleAdjustStreamBroker> streamBrokerProvider) {
        this.atmSelector          = atmSelector;
        this.marketDataService    = marketDataService;
        this.orderService         = orderService;
        this.eventService         = eventService;
        this.riskSettings         = riskSettings;
        this.tradeRepoProvider    = tradeRepoProvider;
        this.streamBrokerProvider = streamBrokerProvider;
    }

    private void publishStream() {
        try {
            StrangleAdjustStreamBroker b = streamBrokerProvider == null ? null : streamBrokerProvider.getIfAvailable();
            if (b != null) b.publish();
        } catch (Exception ignored) {}
    }

    // ── Boot / lifecycle ────────────────────────────────────────────────────

    @PostConstruct
    public void boot() {
        loadFromDisk();
        backfillLegacyDbRowsFromState();
        rolloverIfNewDay();
        pruneStaleEventsBeforeToday();

        // Re-subscribe any restored open positions so the SL watcher has live LTP.
        for (Position p : state.openPositions.values()) {
            if (p != null && p.symbol != null && !p.symbol.isBlank()) {
                try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(p.symbol)); }
                catch (Exception ignored) {}
            }
        }

        log.info("[StrangleAdjust] booted — enabled={}, entryTime={}, sqoff={}, restoredPositions={}",
            riskSettings.isStrangleAdjustEnabled(), riskSettings.getStrangleAdjustEntryTime(),
            riskSettings.getStrangleAdjustSquareOffTime(), state.openPositions.size());
    }

    private void pruneStaleEventsBeforeToday() {
        if (state.recentEvents == null || state.recentEvents.isEmpty()) return;
        long startOfTodayMillis = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
        int before = state.recentEvents.size();
        state.recentEvents.removeIf(e -> {
            Object ts = e.get("ts");
            return !(ts instanceof Number) || ((Number) ts).longValue() < startOfTodayMillis;
        });
        int removed = before - state.recentEvents.size();
        if (removed > 0) {
            log.info("[StrangleAdjust] pruned {} stale event(s) before today's 00:00 IST", removed);
            saveToDisk();
            publishStream();
        }
    }

    private void backfillLegacyDbRowsFromState() {
        if (state.todayClosedTrades == null || state.todayClosedTrades.isEmpty()) return;
        StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
        if (repo == null) return;
        try {
            List<StrategyTradeEntity> rows = repo.findByStrategyIdAndSessionDateOrderByClosedAtMillisAsc(
                STRATEGY_ID, state.dayKey);
            int patched = 0;
            for (StrategyTradeEntity row : rows) {
                boolean needSymbol = row.getSymbol() == null || row.getSymbol().isBlank();
                boolean needSetup  = row.getSetup()  == null || row.getSetup().isBlank();
                if (!needSymbol && !needSetup) continue;
                for (Map<String, Object> m : state.todayClosedTrades) {
                    Object ts = m.get("closedAtMillis");
                    if (!(ts instanceof Number)) continue;
                    long ms = ((Number) ts).longValue();
                    if (Math.abs(ms - row.getClosedAtMillis()) > 5_000L) continue;
                    if (needSymbol) {
                        Object sym = m.get("symbol");
                        if (sym != null) row.setSymbol(String.valueOf(sym));
                    }
                    if (needSetup) {
                        Object setup = m.get("setup");
                        if (setup != null) row.setSetup(String.valueOf(setup));
                    }
                    patched++;
                    break;
                }
            }
            if (patched > 0) {
                repo.saveAll(rows);
                log.info("[StrangleAdjust] backfilled symbol/setup on {} legacy DB row(s) for {}",
                    patched, state.dayKey);
            }
        } catch (Exception e) {
            log.warn("[StrangleAdjust] backfill failed: {}", e.getMessage());
        }
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id() { return STRATEGY_ID; }
    @Override public String displayName() { return "Strangle + Adjustments"; }
    @Override public double initialCapital() { return riskSettings.getStrangleAdjustInitialCapital(); }
    @Override public String description() {
        return "NIFTY intraday ATM strangle with 100 %-SL recovery adjustment";
    }
    @Override public String currentState() {
        if (state.openPositions.isEmpty()) return state.entered ? "DONE_FOR_DAY" : "IDLE";
        return "OPEN(" + state.openPositions.size() + ")";
    }
    @Override public boolean isEnabled() { return riskSettings.isStrangleAdjustEnabled(); }
    /** One session day = one trade for analytics purposes (see AnalyticsService rollup). */
    @Override public boolean aggregatesToDay() { return true; }

    @Override
    public boolean forceClose(String reason) {
        boolean anyClosed = false;
        synchronized (this) {
            if (state.openPositions.isEmpty()) return false;
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                if (closePosition(p, reason == null ? "MANUAL" : reason)) anyClosed = true;
            }
        }
        return anyClosed;
    }

    public boolean forceCloseSymbol(String symbol, String reason) {
        if (symbol == null || symbol.isBlank()) return false;
        synchronized (this) {
            boolean anyClosed = false;
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                if (p != null && symbol.equals(p.symbol)) {
                    if (closePosition(p, reason == null ? "MANUAL" : reason)) anyClosed = true;
                }
            }
            return anyClosed;
        }
    }

    @Override
    public void resetToIdle(String reason) {
        synchronized (this) {
            state.openPositions.clear();
            state.entered = false;
            state.ceAdjusted = false;
            state.peAdjusted = false;
            state.originalCeStrike = 0;
            state.originalPeStrike = 0;
            state.todaysInstrument = "";
            saveToDisk();
            event("[INFO]", "System", "reset — " + (reason == null ? "" : reason));
        }
    }

    @Override
    public java.util.List<java.util.Map<String, Object>> todayClosedTrades() {
        rolloverIfNewDay();
        synchronized (this) { return new ArrayList<>(state.todayClosedTrades); }
    }

    @Override
    public double liveNetPnlToday() {
        rolloverIfNewDay();
        synchronized (this) {
            double net = 0;
            for (Map<String, Object> m : state.todayClosedTrades) net += asDouble(m.get("netPnl"));
            for (Position p : state.openPositions.values()) {
                net += openPositionMtm(p) - cycleChargesFor(p);
            }
            return round2(net);
        }
    }

    private double cycleChargesFor(Position p) {
        double entryTurnover = p.entryPrice * p.qty;
        double exitTurnover  = currentExitTurnover(p);
        return p.isShort
            ? perCycleCharges(entryTurnover, exitTurnover)
            : perCycleCharges(exitTurnover,  entryTurnover);
    }

    @Override
    public double liveChargesToday() {
        rolloverIfNewDay();
        synchronized (this) {
            double ch = 0;
            for (Map<String, Object> m : state.todayClosedTrades) ch += asDouble(m.get("charges"));
            for (Position p : state.openPositions.values()) ch += cycleChargesFor(p);
            return round2(ch);
        }
    }

    @Override
    public void tick() {
        rolloverIfNewDay();
        entryIfDue();
        watchSquareoff();
        refreshUnresolvedFills();
    }

    @Override
    public void fastSlCheck() {
        if (state.openPositions.isEmpty()) return;
        for (Position p : new java.util.ArrayList<>(state.openPositions.values())) {
            if (p == null) continue;
            if (!p.isShort) continue;                       // hedges (BUY) don't SL
            if (p.setup == ActiveSetup.MANUAL) continue;
            if (p.symbol == null || p.symbol.isBlank()) continue;
            if (p.slLevel <= 0) continue;
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); } catch (Exception ignored) {}
            if (ltp <= 0) continue;
            if (ltp >= p.slLevel) {
                event("[WARNING]", "Exit",
                    shortSym(p.symbol) + " SL_HIT @ " + round2(ltp)
                    + " (sl=" + round2(p.slLevel) + ", role=" + p.role + ")");
                handleSlHit(p);
            }
        }
    }

    // ── Instrument ──────────────────────────────────────────────────────────
    //
    // Hard-coded NIFTY, every trading day. Weekday routing + SENSEX/DISABLED
    // options were removed once the strategy was locked to NIFTY-only.

    // ── Entry ───────────────────────────────────────────────────────────────

    private synchronized void entryIfDue() {
        if (state.entered) return;
        if (!isEnabled()) return;
        if (!state.openPositions.isEmpty()) {
            state.entered = true;    // defensive — restored positions imply entered already
            return;
        }

        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        LocalTime entryAt;
        try { entryAt = LocalTime.parse(riskSettings.getStrangleAdjustEntryTime()); }
        catch (Exception e) { entryAt = LocalTime.of(9, 20); }
        if (now.isBefore(entryAt)) return;

        InstrumentSpec spec = InstrumentSpec.NIFTY;
        double targetPremium = riskSettings.getStrangleAdjustNiftyTargetPremium();
        BalancedAtmSelector.StrikeAtLevel ceRow =
            atmSelector.resolveStrikeByTargetPremium(spec.spotSymbol, "CE", targetPremium);
        BalancedAtmSelector.StrikeAtLevel peRow =
            atmSelector.resolveStrikeByTargetPremium(spec.spotSymbol, "PE", targetPremium);
        if (ceRow == null || peRow == null
            || ceRow.ceSymbol() == null || ceRow.ceSymbol().isBlank()
            || peRow.peSymbol() == null || peRow.peSymbol().isBlank()) {
            log.warn("[StrangleAdjust] entry deferred — chain rows unavailable ({} target={})",
                spec, targetPremium);
            return;
        }

        int qty = riskSettings.getStrangleAdjustLotsPerLeg() * (int) spec.lotSize;
        String productType = riskSettings.getStrangleAdjustOrderType();
        double slMult = Math.max(1.0, riskSettings.getStrangleAdjustSlMultiplier());

        String ceSym = ceRow.ceSymbol();
        String peSym = peRow.peSymbol();

        // Subscribe to tick feed before placing orders so fill-price + SL watcher have data.
        try { marketDataService.subscribeAdditional(java.util.List.of(ceSym, peSym)); }
        catch (Exception ignored) {}

        Position cePos = firePosition(spec, "CE", ceRow.resolvedStrike(), ceSym, qty,
            ActiveSetup.STRANGLE, "ENTRY_CE", productType, slMult, ceRow.ceLtp());
        Position pePos = firePosition(spec, "PE", peRow.resolvedStrike(), peSym, qty,
            ActiveSetup.STRANGLE, "ENTRY_PE", productType, slMult, peRow.peLtp());

        if (cePos == null && pePos == null) {
            event("[ERROR]", "STRANGLE ENTRY", "both legs rejected — no positions opened");
            return;
        }

        state.entered          = true;
        state.todaysInstrument = spec.name();
        state.originalCeStrike = ceRow.resolvedStrike();
        state.originalPeStrike = peRow.resolvedStrike();
        state.tradesToday      = (cePos != null ? 1 : 0) + (pePos != null ? 1 : 0);
        saveToDisk();

        event("[SUCCESS]", "STRANGLE ENTRY",
            spec + " — sold " + ceRow.resolvedStrike() + " CE @ "
            + (cePos != null ? round2(cePos.entryPrice) : "REJECTED")
            + ", " + peRow.resolvedStrike() + " PE @ "
            + (pePos != null ? round2(pePos.entryPrice) : "REJECTED"));
    }

    /** Common leg-placement helper. Places a SELL, records the Position, returns null
     *  on rejection. */
    private Position firePosition(InstrumentSpec spec, String side, long strike, String symbol,
                                  int qty, ActiveSetup setup, String role,
                                  String productType, double slMult, double refLtp) {
        OrderDTO order = orderService.placeOrder(symbol, qty, -1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            event("[ERROR]", "AUTO ENTRY", "SELL rejected for " + shortSym(symbol) + " (" + role + ")");
            return null;
        }
        double entryLtp = 0;
        try { entryLtp = marketDataService.getLtp(symbol); } catch (Exception ignored) {}
        if (entryLtp <= 0) entryLtp = refLtp;
        if (entryLtp <= 0) {
            event("[ERROR]", "AUTO ENTRY", shortSym(symbol) + " (" + role + ") — no entry price");
            return null;
        }
        Position p = new Position();
        p.symbol          = symbol;
        p.setup           = setup;
        p.role            = role;
        p.instrument      = spec.name();
        p.side            = side;
        p.strike          = strike;
        p.qty             = qty;
        p.entryPrice      = entryLtp;
        p.entryOrderId    = order.getId();
        p.openMillis      = System.currentTimeMillis();
        p.slLevel         = entryLtp * slMult;
        p.originalSlLevel = p.slLevel;
        p.targetLevel     = 0;
        p.isShort         = true;
        p.fillResolved    = false;
        p.productType     = productType;
        state.openPositions.put(posKey(p) + "|" + role, p);
        return p;
    }

    // ── SL handling + adjustment ────────────────────────────────────────────

    private synchronized void handleSlHit(Position pos) {
        String role = pos.role == null ? "" : pos.role;
        closePosition(pos, "SL_HIT");

        // Adjustment fires only on the two initial legs, once per side.
        boolean isEntry = "ENTRY_CE".equals(role) || "ENTRY_PE".equals(role);
        if (!isEntry) return;

        String slHitSide = "ENTRY_CE".equals(role) ? "CE" : "PE";
        String adjustSide = "CE".equals(slHitSide) ? "PE" : "CE";
        if ("CE".equals(adjustSide) && state.ceAdjusted) return;
        if ("PE".equals(adjustSide) && state.peAdjusted) return;

        InstrumentSpec spec;
        try { spec = InstrumentSpec.valueOf(state.todaysInstrument); }
        catch (Exception e) {
            event("[ERROR]", "STRANGLE ADJUST", "no instrument recorded for adjustment");
            return;
        }
        double targetPremium = riskSettings.getStrangleAdjustNiftyTargetPremium();

        BalancedAtmSelector.StrikeAtLevel sellRow =
            atmSelector.resolveStrikeByTargetPremium(spec.spotSymbol, adjustSide, targetPremium);
        if (sellRow == null) {
            event("[WARNING]", "STRANGLE ADJUST",
                "no strike near ₹" + targetPremium + " on " + adjustSide + " side — adjust skipped");
            return;
        }
        long sellStrike = sellRow.resolvedStrike();
        String sellSym  = "CE".equals(adjustSide) ? sellRow.ceSymbol() : sellRow.peSymbol();
        double sellRef  = "CE".equals(adjustSide) ? sellRow.ceLtp()    : sellRow.peLtp();
        if (sellSym == null || sellSym.isBlank()) {
            event("[WARNING]", "STRANGLE ADJUST",
                "no " + adjustSide + " symbol at strike " + sellStrike + " — adjust skipped");
            return;
        }

        // Hedge — same side as the new sell, N strikes further OTM.
        int hedgeStrikesAway = riskSettings.getStrangleAdjustHedgeStrikesAway();
        String direction = "CE".equals(adjustSide) ? "UP" : "DOWN";
        BalancedAtmSelector.StrikeAtLevel hedgeRow = atmSelector.resolveStrikeNAway(
            spec.spotSymbol, spec.strikeStep, sellStrike, hedgeStrikesAway, direction);

        int baseQty = riskSettings.getStrangleAdjustLotsPerLeg() * (int) spec.lotSize;
        int hedgeQty = (int) Math.round(baseQty * riskSettings.getStrangleAdjustHedgeQtyMultiplier());
        // Round hedgeQty to whole lots (min 1 lot).
        long lot = spec.lotSize;
        hedgeQty = (int) Math.max(lot, ((long) hedgeQty / lot) * lot);
        String productType = riskSettings.getStrangleAdjustOrderType();

        // Subscribe both symbols before order placement.
        List<String> subs = new ArrayList<>();
        subs.add(sellSym);
        String hedgeSym = null;
        if (hedgeRow != null) {
            hedgeSym = "CE".equals(adjustSide) ? hedgeRow.ceSymbol() : hedgeRow.peSymbol();
            if (hedgeSym != null && !hedgeSym.isBlank()) subs.add(hedgeSym);
        }
        try { marketDataService.subscribeAdditional(subs); } catch (Exception ignored) {}

        // Place hedge FIRST (buy) so margin freed before the new sell lands.
        Position hedgePos = null;
        if (hedgeSym != null && !hedgeSym.isBlank()) {
            OrderDTO hOrder = orderService.placeOrder(hedgeSym, hedgeQty, +1, 0, productType);
            if (hOrder != null && hOrder.getId() != null && !hOrder.getId().isEmpty()) {
                double hLtp = 0;
                try { hLtp = marketDataService.getLtp(hedgeSym); } catch (Exception ignored) {}
                if (hLtp <= 0) hLtp = "CE".equals(adjustSide) ? hedgeRow.ceLtp() : hedgeRow.peLtp();
                hedgePos = new Position();
                hedgePos.symbol          = hedgeSym;
                hedgePos.setup           = ActiveSetup.STRANGLE;
                hedgePos.role            = "HEDGE_" + adjustSide;
                hedgePos.instrument      = spec.name();
                hedgePos.side            = adjustSide;
                hedgePos.strike          = hedgeRow.resolvedStrike();
                hedgePos.qty             = hedgeQty;
                hedgePos.entryPrice      = hLtp > 0 ? hLtp : OPTION_TICK_SIZE;
                hedgePos.entryOrderId    = hOrder.getId();
                hedgePos.openMillis      = System.currentTimeMillis();
                hedgePos.slLevel         = 0;     // hedges have no SL
                hedgePos.targetLevel     = 0;
                hedgePos.isShort         = false;
                hedgePos.fillResolved    = false;
                hedgePos.productType     = productType;
                state.openPositions.put(posKey(hedgePos) + "|" + hedgePos.role, hedgePos);
            } else {
                event("[WARNING]", "STRANGLE ADJUST",
                    "hedge BUY rejected for " + shortSym(hedgeSym) + " — proceeding with naked adjustment sell");
            }
        }

        // New opposite-side sell.
        double slMult = Math.max(1.0, riskSettings.getStrangleAdjustSlMultiplier());
        Position sellPos = firePosition(spec, adjustSide, sellStrike, sellSym, baseQty,
            ActiveSetup.STRANGLE, "ADJUST_" + adjustSide, productType, slMult, sellRef);

        if (sellPos == null) {
            event("[ERROR]", "STRANGLE ADJUST", "adjustment SELL failed — hedge remains open");
            return;
        }

        if ("CE".equals(adjustSide)) state.ceAdjusted = true;
        else state.peAdjusted = true;

        state.tradesToday++;
        saveToDisk();

        event("[SUCCESS]", "STRANGLE ADJUST",
            spec + " — sold " + sellStrike + " " + adjustSide + " @ " + round2(sellPos.entryPrice)
            + (hedgePos != null
                ? ", hedge " + hedgePos.strike + " " + adjustSide + " ×" + hedgeQty
                  + " @ " + round2(hedgePos.entryPrice)
                : " (no hedge)"));
    }

    // ── Fill resolver ──────────────────────────────────────────────────────

    private void refreshUnresolvedFills() {
        if (state.openPositions.isEmpty()) return;
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            if (p.fillResolved) continue;
            if (p.entryOrderId == null || p.entryOrderId.isBlank()) continue;
            try {
                double fillPrice = orderService.getFilledPriceByOrderId(p.entryOrderId);
                if (fillPrice <= 0) continue;
                double oldEntry = p.entryPrice;
                p.entryPrice = round2(fillPrice);
                if (p.isShort && p.slLevel > 0 && p.originalSlLevel > 0) {
                    // Rescale SL to the actual fill price, preserving the SL multiplier.
                    double mult = Math.max(1.0, riskSettings.getStrangleAdjustSlMultiplier());
                    p.slLevel = p.entryPrice * mult;
                    p.originalSlLevel = p.slLevel;
                }
                p.fillResolved = true;
                event("[INFO]", "Fill", shortSym(p.symbol) + " (" + p.role + ") fill resolved — entry "
                    + round2(oldEntry) + " → " + round2(p.entryPrice) + " (SL now " + round2(p.slLevel) + ")");
                saveToDisk();
            } catch (Exception e) {
                log.warn("[StrangleAdjust] fill lookup failed for {}: {}", p.entryOrderId, e.getMessage());
            }
        }
    }

    // ── Time-based squareoff ───────────────────────────────────────────────

    public synchronized void watchSquareoff() {
        if (state.openPositions.isEmpty()) return;
        String hhmm = riskSettings.getStrangleAdjustSquareOffTime();
        if (hhmm == null || hhmm.isBlank()) return;
        LocalTime cutoff;
        try { cutoff = LocalTime.parse(hhmm); }
        catch (Exception e) { return; }
        if (ZonedDateTime.now(IST).toLocalTime().isAfter(cutoff)) {
            event("[INFO]", "Squareoff", "TIMED_EXIT — clock reached " + hhmm
                + ", flattening " + state.openPositions.size() + " position(s)");
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                closePosition(p, "TIMED_EXIT");
            }
        }
    }

    // ── Position close + persistence ────────────────────────────────────────

    private boolean closePosition(Position p, String reason) {
        if (p == null) return false;
        String symbol = p.symbol;
        String productType = (p.productType == null || p.productType.isBlank())
            ? riskSettings.getStrangleAdjustOrderType()
            : p.productType;
        int closeSide = p.isShort ? +1 : -1;
        OrderDTO close = orderService.placeExitOrder(symbol, p.qty, closeSide, productType);
        double exitPrice = 0;
        if (close != null) {
            try { exitPrice = marketDataService.getLtp(symbol); }
            catch (Exception ignored) {}
        }
        double sellTurnover = (p.isShort ? p.entryPrice : exitPrice) * p.qty;
        double buyTurnover  = (p.isShort ? exitPrice    : p.entryPrice) * p.qty;
        double gross   = p.isShort
            ? (p.entryPrice - exitPrice) * p.qty
            : (exitPrice    - p.entryPrice) * p.qty;
        double charges = perCycleCharges(sellTurnover, buyTurnover);
        double net     = gross - charges;

        long closedAtMillis = System.currentTimeMillis();
        String dbStrategyId = (p.setup == ActiveSetup.MANUAL) ? MANUAL_STRATEGY_ID : STRATEGY_ID;
        String setupName    = p.setup == null ? "MANUAL" : p.setup.name();
        persistTradeRow(dbStrategyId, p.symbol, setupName, reason, p.qty,
            gross, charges, net,
            "SL_HIT".equals(reason) ? 1 : 0,
            closedAtMillis, p.openMillis, p.entryPrice, exitPrice, p.instrument);

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("strategyId",     dbStrategyId);
        cycle.put("setup",          setupName);
        cycle.put("role",           p.role == null ? "" : p.role);
        cycle.put("instrument",     p.instrument == null ? "" : p.instrument);
        cycle.put("side",           p.isShort ? "SELL" : "BUY");
        cycle.put("legSide",        p.side == null ? "" : p.side);
        cycle.put("strike",         p.strike);
        cycle.put("symbol",         p.symbol);
        cycle.put("qty",            p.qty);
        cycle.put("entryPrice",     round2(p.entryPrice));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    reason);
        cycle.put("closedAtMillis", closedAtMillis);
        cycle.put("openedAtMillis", p.openMillis);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 200) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Exit",
            shortSym(symbol) + " (" + p.role + ") closed (" + reason + ") net=" + round2(net)
            + " gross=" + round2(gross));

        // Remove from openPositions using the composite key we assigned at fire time.
        String key = posKey(p) + "|" + (p.role == null ? "" : p.role);
        state.openPositions.remove(key);

        saveToDisk();
        return true;
    }

    private void persistTradeRow(String strategyId, String symbol, String setup, String reason,
                                 int qty, double gross, double charges, double net, int slHits,
                                 long closedAtMillis, long openedAtMillis,
                                 double entryPrice, double exitPrice, String instrument) {
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo == null) return;
            LocalDate sessionDate = LocalDate.now(IST);
            StrategyTradeEntity row = new StrategyTradeEntity();
            row.setStrategyId(strategyId == null ? STRATEGY_ID : strategyId);
            row.setSymbol(symbol);
            row.setSetup(setup);
            row.setSessionDate(sessionDate.toString());
            row.setClosedAtMillis(closedAtMillis);
            row.setOpenedAtMillis(openedAtMillis);
            row.setInstrument(instrument != null && !instrument.isBlank() ? instrument
                : instrumentFromSymbol(symbol));
            row.setQty(qty);
            row.setGrossPnl(round2(gross));
            row.setCharges(round2(charges));
            row.setNetPnl(round2(net));
            row.setCloseReason(reason);
            row.setSlHitCount(slHits);
            row.setEntryPrice(entryPrice > 0 ? round2(entryPrice) : null);
            row.setExitPrice(exitPrice   > 0 ? round2(exitPrice)  : null);
            repo.save(row);
        } catch (Exception e) {
            log.warn("[StrangleAdjust] persist trade failed: {}", e.getMessage());
        }
    }

    private static String instrumentFromSymbol(String symbol) {
        if (symbol == null) return null;
        String s = symbol.toUpperCase();
        if (s.contains("SENSEX")) return "SENSEX";
        if (s.contains("BANKNIFTY") || s.contains("NIFTYBANK")) return "BANKNIFTY";
        if (s.contains("NIFTY")) return "NIFTY";
        return null;
    }

    // ── Maintenance actions ─────────────────────────────────────────────────

    public synchronized Map<String, Object> clearAllRecords() {
        int cyclesCleared = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();
        state.tradesToday = 0;
        state.consecutiveLosses = 0;
        int eventsCleared = state.recentEvents.size();
        state.recentEvents.clear();
        saveToDisk();

        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) {
                dbCleared = repo.deleteAllRows();
                log.warn("[StrangleAdjust] clearAllRecords — DB deleteAllRows wiped {} rows", dbCleared);
            }
        } catch (Exception e) {
            log.warn("[StrangleAdjust] clearAllRecords DB wipe failed: {}", e.getMessage());
        }

        event("[WARNING]", "Maintenance",
            "Cleared ALL records — cycles=" + cyclesCleared + " events=" + eventsCleared
            + " dbRows=" + dbCleared + " (open positions preserved)");
        publishStream();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cyclesCleared", cyclesCleared);
        out.put("eventsCleared", eventsCleared);
        out.put("dbCleared",     dbCleared);
        return out;
    }

    public synchronized Map<String, Object> clearTodayRecords() {
        int cyclesCleared = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();
        state.tradesToday = 0;
        state.consecutiveLosses = 0;

        long startOfTodayMillis = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
        int eventsBefore = state.recentEvents.size();
        state.recentEvents.removeIf(e -> {
            Object ts = e.get("ts");
            return ts instanceof Number && ((Number) ts).longValue() >= startOfTodayMillis;
        });
        int eventsCleared = eventsBefore - state.recentEvents.size();
        saveToDisk();

        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) dbCleared = repo.deleteBySessionDate(LocalDate.now(IST).toString());
        } catch (Exception e) {
            log.warn("[StrangleAdjust] clearTodayRecords DB wipe failed: {}", e.getMessage());
        }

        event("[WARNING]", "Maintenance",
            "Cleared today's records — cycles=" + cyclesCleared + " events=" + eventsCleared
            + " dbRows=" + dbCleared);
        publishStream();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cyclesCleared", cyclesCleared);
        out.put("eventsCleared", eventsCleared);
        out.put("dbCleared",     dbCleared);
        return out;
    }

    // ── Day rollover ─────────────────────────────────────────────────────────

    @Scheduled(cron = "0 0 6 * * *", zone = "Asia/Kolkata")
    public synchronized void scheduledDailyReset() {
        String today = LocalDate.now(IST).toString();
        log.info("[StrangleAdjust] 06:00 IST daily reset (was dayKey={})", today, state.dayKey);
        performDailyReset(today);
    }

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        synchronized (this) {
            if (today.equals(state.dayKey)) return;
            performDailyReset(today);
        }
    }

    private void performDailyReset(String today) {
        state.dayKey            = today;
        state.tradesToday       = 0;
        state.consecutiveLosses = 0;
        state.todayClosedTrades.clear();
        if (state.recentEvents != null) state.recentEvents.clear();
        // Session-scoped flags always reset — even if a position was carried across midnight
        // (unusual), the strategy should re-evaluate today's instrument fresh.
        state.entered          = false;
        state.ceAdjusted       = false;
        state.peAdjusted       = false;
        state.originalCeStrike = 0;
        state.originalPeStrike = 0;
        state.todaysInstrument = "";
        // Any lingering openPositions from yesterday get force-closed here so they don't leak.
        for (Position p : new ArrayList<>(state.openPositions.values())) {
            if (p != null) closePosition(p, "DAY_ROLLOVER");
        }
        saveToDisk();
    }

    // ── Charges ──────────────────────────────────────────────────────────────

    private double perCycleCharges(double sellTurnover, double buyTurnover) {
        double broker = riskSettings.getBrokeragePerOrder() * 2;
        double stt = sellTurnover * riskSettings.getSttRate() / 100.0;
        double total = sellTurnover + buyTurnover;
        double exch = total * riskSettings.getExchangeRate() / 100.0;
        double sebi = total / 1e7 * riskSettings.getSebiRate();
        double stamp = buyTurnover * riskSettings.getStampDutyRate() / 100.0;
        double gst = (broker + exch) * riskSettings.getGstRate() / 100.0;
        return round2(broker + stt + exch + sebi + stamp + gst);
    }

    private double currentExitTurnover(Position p) {
        try {
            double ltp = marketDataService.getLtp(p.symbol);
            if (ltp > 0) return ltp * p.qty;
        } catch (Exception ignored) {}
        return p.entryPrice * p.qty;
    }

    private double openPositionMtm(Position p) {
        if (p == null || p.entryPrice <= 0) return 0;
        try {
            double ltp = marketDataService.getLtp(p.symbol);
            if (ltp <= 0) return 0;
            return p.isShort
                ? (p.entryPrice - ltp) * p.qty
                : (ltp - p.entryPrice) * p.qty;
        } catch (Exception e) {
            return 0;
        }
    }

    /** Sum of remaining ₹ at risk across open SHORT positions (hedges = 0). */
    private double exposedRiskNow() {
        double total = 0;
        for (Position p : state.openPositions.values()) {
            if (p == null || !p.isShort || p.slLevel <= 0) continue;
            total += Math.max(0, p.slLevel - p.entryPrice) * p.qty;
        }
        return total;
    }

    // ── Dashboard payload (consumed by StrangleAdjustController + Trade page) ─────

    public synchronized Map<String, Object> dashboardState() {
        rolloverIfNewDay();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("strategy",          STRATEGY_ID);
        m.put("displayName",       displayName());
        m.put("enabled",           isEnabled());
        m.put("lifecycle",         currentState());
        m.put("dayKey",            state.dayKey);
        m.put("tradesToday",       state.tradesToday);
        m.put("consecutiveLosses", state.consecutiveLosses);
        m.put("entered",           state.entered);
        m.put("todaysInstrument",  state.todaysInstrument);

        // StrangleAdjust-specific block: today's original strikes + adjustment flags.
        Map<String, Object> str = new LinkedHashMap<>();
        str.put("originalCeStrike", state.originalCeStrike);
        str.put("originalPeStrike", state.originalPeStrike);
        str.put("ceAdjusted",       state.ceAdjusted);
        str.put("peAdjusted",       state.peAdjusted);
        m.put("strangleAdjust", str);

        // Open positions
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            double ltp = safeLtp(p.symbol);
            double mtm = openPositionMtm(p);
            row.put("symbol",       p.symbol);
            row.put("role",         p.role);
            row.put("instrument",   p.instrument);
            row.put("side",         p.side);
            row.put("strike",       p.strike);
            row.put("qty",          p.qty);
            row.put("entryPrice",   round2(p.entryPrice));
            row.put("ltp",          round2(ltp));
            row.put("mtm",          round2(mtm));
            row.put("slLevel",      round2(p.slLevel));
            row.put("isShort",      p.isShort);
            row.put("openMillis",   p.openMillis);
            rows.add(row);
        }
        m.put("openPositions", rows);

        Map<String, Object> risk = new LinkedHashMap<>();
        risk.put("exposedRisk",     round2(exposedRiskNow()));
        risk.put("initialCapital",  round2(initialCapital()));
        m.put("risk", risk);

        m.put("todayClosedTrades", new ArrayList<>(state.todayClosedTrades));
        m.put("recentEvents",      new ArrayList<>(state.recentEvents));

        // Reference LTPs for header display: whichever instrument runs today.
        String todayInst = state.todaysInstrument;
        if (todayInst != null && !todayInst.isBlank()) {
            try {
                InstrumentSpec sp = InstrumentSpec.valueOf(todayInst);
                m.put("spotLtp",  round2(safeLtp(sp.spotSymbol)));
                m.put("spotSymbol", sp.spotSymbol);
            } catch (Exception ignored) {}
        }
        return m;
    }

    // ── State + persistence ────────────────────────────────────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class State {
        public String dayKey = "";
        public int    tradesToday;
        public int    consecutiveLosses;
        public Map<String, Position> openPositions   = new ConcurrentHashMap<>();
        public List<Map<String, Object>> todayClosedTrades = new ArrayList<>();
        public List<Map<String, Object>> recentEvents      = new ArrayList<>();

        /** Set at 09:20 entry (or the DISABLED short-circuit). Idempotent guard. */
        public boolean entered;
        /** One-shot adjustment flag per side. */
        public boolean ceAdjusted;
        public boolean peAdjusted;
        public long   originalCeStrike;
        public long   originalPeStrike;
        /** Today's active instrument ("NIFTY" / "SENSEX" / ""). Set at entry. */
        public String todaysInstrument = "";
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Position {
        public String     symbol = "";
        public ActiveSetup setup;
        /** Semantic tag: ENTRY_CE / ENTRY_PE / ADJUST_CE / ADJUST_PE / HEDGE_CE / HEDGE_PE. */
        public String     role = "";
        /** "NIFTY" / "SENSEX". */
        public String     instrument = "";
        /** "CE" / "PE". */
        public String     side = "";
        public long       strike;
        public int        qty;
        public double     entryPrice;
        public String     entryOrderId = "";
        public long       openMillis;
        public double     targetLevel;
        public double     slLevel;
        public double     originalSlLevel;
        public boolean    fillResolved;
        public boolean    isShort = true;
        public String     productType = "";
    }

    // ── Event log ────────────────────────────────────────────────────────────

    public void postEvent(String severity, String source, String message) {
        event(severity, source, message);
    }

    private void event(String severity, String source, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts",       System.currentTimeMillis());
        e.put("severity", severity);
        e.put("source",   source);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [strangleAdjust:" + source + "] " + message);
        publishStream();
    }

    // ── Persistence ─────────────────────────────────────────────────────────

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) {
                // One-time migration from the pre-rename state file.
                Path legacy = Path.of(LEGACY_STATE_FILE);
                if (Files.exists(legacy)) {
                    File parent = p.toFile().getParentFile();
                    if (parent != null && !parent.exists()) parent.mkdirs();
                    Files.move(legacy, p);
                    log.info("[StrangleAdjust] migrated legacy state file {} → {}", legacy, p);
                } else {
                    return;
                }
            }
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.openPositions == null)     state.openPositions     = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null)      state.recentEvents      = new ArrayList<>();
            }
        } catch (IOException e) {
            log.warn("[StrangleAdjust] failed to load state: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[StrangleAdjust] failed to save state: {}", e.getMessage());
        }
    }

    // ── Utility ─────────────────────────────────────────────────────────────

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private double safeLtp(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getLtp(sym)); }
        catch (Exception e) { return 0; }
    }

    private static String shortSym(String s) {
        if (s == null || s.isBlank()) return "";
        if (s.endsWith("CE") || s.endsWith("PE")) {
            int len = s.length();
            int strikeEnd = len - 2;
            int strikeStart = strikeEnd;
            while (strikeStart > 0 && Character.isDigit(s.charAt(strikeStart - 1))) strikeStart--;
            if (strikeEnd - strikeStart >= 4) return s.substring(strikeStart);
        }
        return s.length() > 12 ? s.substring(s.length() - 12) : s;
    }

    private static double asDouble(Object o) {
        if (o instanceof Number) return ((Number) o).doubleValue();
        if (o == null) return 0;
        try { return Double.parseDouble(o.toString()); } catch (Exception e) { return 0; }
    }
}
