package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.StrangleStreamBroker;
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
 * SENSEX intraday defined-risk strangle. Every trading day at 09:20:
 * <ul>
 *   <li>SELL CE at target premium (₹50 default) and PE at target premium (₹50).</li>
 *   <li>BUY CE hedge at hedge premium (₹5 default) and PE hedge at hedge premium (₹5).</li>
 *   <li>Per-leg SL = entryPremium × slMultiplier (default 2.0 = 100 % of received premium).</li>
 * </ul>
 * When either short's SL hits, close that leg — <b>no adjustments</b>. Remaining legs
 * ride to 15:15 timed squareoff.
 *
 * <p>Analytics treats each session day as one trade
 * ({@link #aggregatesToDay()} = true).
 */
@Service
public class Strangle implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(Strangle.class);
    private static final String STRATEGY_ID = "strangle";
    public  static final String MANUAL_STRATEGY_ID = "manual";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/strangle-state.json";
    private static final int    RECENT_EVENTS_LIMIT = 60;

    /** Single setup value — every leg (short + hedge) shares this tag. */
    public enum ActiveSetup { STRANGLE, MANUAL }

    private static String posKey(Position p) {
        if (p == null) return "";
        String setup = p.setup == null ? "MANUAL" : p.setup.name();
        return setup + "|" + (p.symbol == null ? "" : p.symbol) + "|"
             + (p.role == null ? "" : p.role);
    }

    private final BalancedAtmSelector   atmSelector;
    private final MarketDataService     marketDataService;
    private final OrderService          orderService;
    private final EventService          eventService;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectProvider<StrangleStreamBroker>    streamBrokerProvider;

    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

    private volatile State state = new State();

    public Strangle(BalancedAtmSelector atmSelector,
                    MarketDataService marketDataService,
                    OrderService orderService,
                    EventService eventService,
                    RiskSettingsStore riskSettings,
                    ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                    ObjectProvider<StrangleStreamBroker> streamBrokerProvider) {
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
            StrangleStreamBroker b = streamBrokerProvider == null ? null : streamBrokerProvider.getIfAvailable();
            if (b != null) b.publish();
        } catch (Exception ignored) {}
    }

    @PostConstruct
    public void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        pruneStaleEventsBeforeToday();
        // Re-subscribe restored open positions so the SL watcher has live LTP.
        for (Position p : state.openPositions.values()) {
            if (p != null && p.symbol != null && !p.symbol.isBlank()) {
                try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(p.symbol)); }
                catch (Exception ignored) {}
            }
        }
        log.info("[Strangle] booted — enabled={}, entryTime={}, sqoff={}, restoredPositions={}",
            riskSettings.isStrangleEnabled(), riskSettings.getStrangleEntryTime(),
            riskSettings.getStrangleSquareOffTime(), state.openPositions.size());
    }

    private void pruneStaleEventsBeforeToday() {
        if (state.recentEvents == null || state.recentEvents.isEmpty()) return;
        long startOfToday = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
        int before = state.recentEvents.size();
        state.recentEvents.removeIf(e -> {
            Object ts = e.get("ts");
            return !(ts instanceof Number) || ((Number) ts).longValue() < startOfToday;
        });
        int removed = before - state.recentEvents.size();
        if (removed > 0) { saveToDisk(); publishStream(); }
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id() { return STRATEGY_ID; }
    @Override public String displayName() { return "Strangle"; }
    @Override public String description() { return "SENSEX intraday defined-risk strangle (no adjustments)"; }
    @Override public String currentState() {
        if (state.openPositions.isEmpty()) return state.entered ? "DONE_FOR_DAY" : "IDLE";
        return "OPEN(" + state.openPositions.size() + ")";
    }
    @Override public boolean isEnabled() { return riskSettings.isStrangleEnabled(); }
    @Override public boolean aggregatesToDay() { return true; }
    @Override public double initialCapital() { return riskSettings.getStrangleInitialCapital(); }

    @Override
    public boolean forceClose(String reason) {
        boolean any = false;
        synchronized (this) {
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                if (closePosition(p, reason == null ? "MANUAL" : reason)) any = true;
            }
        }
        return any;
    }

    public boolean forceCloseSymbol(String symbol, String reason) {
        if (symbol == null || symbol.isBlank()) return false;
        synchronized (this) {
            boolean any = false;
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                if (p != null && symbol.equals(p.symbol)) {
                    if (closePosition(p, reason == null ? "MANUAL" : reason)) any = true;
                }
            }
            return any;
        }
    }

    @Override
    public void resetToIdle(String reason) {
        synchronized (this) {
            state.openPositions.clear();
            state.entered = false;
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
            for (Position p : state.openPositions.values()) net += openPositionMtm(p) - cycleChargesFor(p);
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
            if (!p.isShort) continue;                       // hedges don't SL
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
                closePosition(p, "SL_HIT");
                // No adjustment — this is the whole point of this strategy vs StrangleAdjust.
            }
        }
    }

    // ── Entry ───────────────────────────────────────────────────────────────

    private synchronized void entryIfDue() {
        if (state.entered) return;
        if (!isEnabled()) return;
        if (!state.openPositions.isEmpty()) {
            state.entered = true;   // defensive
            return;
        }
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        LocalTime entryAt;
        try { entryAt = LocalTime.parse(riskSettings.getStrangleEntryTime()); }
        catch (Exception e) { entryAt = LocalTime.of(9, 20); }
        if (now.isBefore(entryAt)) return;

        // Skip weekends (SENSEX doesn't trade Sat/Sun).
        java.time.DayOfWeek dow = ZonedDateTime.now(IST).getDayOfWeek();
        if (dow == java.time.DayOfWeek.SATURDAY || dow == java.time.DayOfWeek.SUNDAY) {
            state.entered = true;
            return;
        }

        StrangleAdjust.InstrumentSpec spec = StrangleAdjust.InstrumentSpec.SENSEX;
        double shortPrem = riskSettings.getStrangleShortPremium();
        double hedgePrem = riskSettings.getStrangleHedgePremium();

        BalancedAtmSelector.StrikeAtLevel ceShortRow =
            atmSelector.resolveStrikeByTargetPremium(spec.spotSymbol, "CE", shortPrem);
        BalancedAtmSelector.StrikeAtLevel peShortRow =
            atmSelector.resolveStrikeByTargetPremium(spec.spotSymbol, "PE", shortPrem);
        BalancedAtmSelector.StrikeAtLevel ceHedgeRow =
            atmSelector.resolveStrikeByTargetPremium(spec.spotSymbol, "CE", hedgePrem);
        BalancedAtmSelector.StrikeAtLevel peHedgeRow =
            atmSelector.resolveStrikeByTargetPremium(spec.spotSymbol, "PE", hedgePrem);

        if (ceShortRow == null || peShortRow == null) {
            log.warn("[Strangle] entry deferred — short strike lookup returned null (short={})", shortPrem);
            return;
        }
        // Hedges are best-effort but strongly recommended for margin relief.
        String ceShortSym = ceShortRow.ceSymbol();
        String peShortSym = peShortRow.peSymbol();
        String ceHedgeSym = ceHedgeRow == null ? null : ceHedgeRow.ceSymbol();
        String peHedgeSym = peHedgeRow == null ? null : peHedgeRow.peSymbol();
        if (ceShortSym == null || ceShortSym.isBlank() || peShortSym == null || peShortSym.isBlank()) {
            log.warn("[Strangle] entry deferred — short symbols blank");
            return;
        }

        int qty = riskSettings.getStrangleLotsPerLeg() * (int) spec.lotSize;
        String productType = riskSettings.getStrangleOrderType();
        double slMult = Math.max(1.0, riskSettings.getStrangleSlMultiplier());

        List<String> subs = new ArrayList<>();
        subs.add(ceShortSym); subs.add(peShortSym);
        if (ceHedgeSym != null && !ceHedgeSym.isBlank()) subs.add(ceHedgeSym);
        if (peHedgeSym != null && !peHedgeSym.isBlank()) subs.add(peHedgeSym);
        try { marketDataService.subscribeAdditional(subs); } catch (Exception ignored) {}

        // Place hedges first (buy) → free margin for the subsequent sells.
        Position ceHedgePos = null;
        if (ceHedgeSym != null && !ceHedgeSym.isBlank()) {
            ceHedgePos = fireBuyHedge(spec, "CE", ceHedgeRow.resolvedStrike(), ceHedgeSym, qty,
                productType, ceHedgeRow.ceLtp());
        }
        Position peHedgePos = null;
        if (peHedgeSym != null && !peHedgeSym.isBlank()) {
            peHedgePos = fireBuyHedge(spec, "PE", peHedgeRow.resolvedStrike(), peHedgeSym, qty,
                productType, peHedgeRow.peLtp());
        }
        Position ceShortPos = fireShort(spec, "CE", ceShortRow.resolvedStrike(), ceShortSym, qty,
            productType, slMult, ceShortRow.ceLtp());
        Position peShortPos = fireShort(spec, "PE", peShortRow.resolvedStrike(), peShortSym, qty,
            productType, slMult, peShortRow.peLtp());

        if (ceShortPos == null && peShortPos == null) {
            event("[ERROR]", "STRANGLE ENTRY", "both shorts rejected — hedges remain open");
            state.entered = true;   // don't retry today
            return;
        }

        state.entered = true;
        state.originalCeStrike = ceShortRow.resolvedStrike();
        state.originalPeStrike = peShortRow.resolvedStrike();
        state.tradesToday = (ceShortPos != null ? 1 : 0) + (peShortPos != null ? 1 : 0);
        saveToDisk();

        event("[SUCCESS]", "STRANGLE ENTRY",
            "SENSEX — sold " + ceShortRow.resolvedStrike() + " CE @ "
            + (ceShortPos != null ? round2(ceShortPos.entryPrice) : "REJ")
            + ", " + peShortRow.resolvedStrike() + " PE @ "
            + (peShortPos != null ? round2(peShortPos.entryPrice) : "REJ")
            + (ceHedgePos != null ? ", hedge " + ceHedgeRow.resolvedStrike() + " CE @ " + round2(ceHedgePos.entryPrice) : "")
            + (peHedgePos != null ? ", hedge " + peHedgeRow.resolvedStrike() + " PE @ " + round2(peHedgePos.entryPrice) : ""));
    }

    private Position fireShort(StrangleAdjust.InstrumentSpec spec, String side, long strike,
                                String symbol, int qty, String productType, double slMult, double refLtp) {
        OrderDTO order = orderService.placeOrder(symbol, qty, -1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            event("[ERROR]", "AUTO ENTRY", "SELL rejected for " + shortSym(symbol));
            return null;
        }
        double entryLtp = 0;
        try { entryLtp = marketDataService.getLtp(symbol); } catch (Exception ignored) {}
        if (entryLtp <= 0) entryLtp = refLtp;
        if (entryLtp <= 0) {
            event("[ERROR]", "AUTO ENTRY", shortSym(symbol) + " — no entry price");
            return null;
        }
        Position p = new Position();
        p.symbol        = symbol;
        p.setup         = ActiveSetup.STRANGLE;
        p.role          = "ENTRY_" + side;
        p.instrument    = spec.name();
        p.side          = side;
        p.strike        = strike;
        p.qty           = qty;
        p.entryPrice    = entryLtp;
        p.entryOrderId  = order.getId();
        p.openMillis    = System.currentTimeMillis();
        p.slLevel       = entryLtp * slMult;
        p.originalSlLevel = p.slLevel;
        p.isShort       = true;
        p.fillResolved  = false;
        p.productType   = productType;
        state.openPositions.put(posKey(p), p);
        return p;
    }

    private Position fireBuyHedge(StrangleAdjust.InstrumentSpec spec, String side, long strike,
                                   String symbol, int qty, String productType, double refLtp) {
        OrderDTO order = orderService.placeOrder(symbol, qty, +1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            event("[WARNING]", "AUTO ENTRY", "hedge BUY rejected for " + shortSym(symbol));
            return null;
        }
        double entryLtp = 0;
        try { entryLtp = marketDataService.getLtp(symbol); } catch (Exception ignored) {}
        if (entryLtp <= 0) entryLtp = refLtp;
        Position p = new Position();
        p.symbol        = symbol;
        p.setup         = ActiveSetup.STRANGLE;
        p.role          = "HEDGE_" + side;
        p.instrument    = spec.name();
        p.side          = side;
        p.strike        = strike;
        p.qty           = qty;
        p.entryPrice    = entryLtp > 0 ? entryLtp : 0.05;
        p.entryOrderId  = order.getId();
        p.openMillis    = System.currentTimeMillis();
        p.slLevel       = 0;         // hedges have no SL
        p.isShort       = false;
        p.fillResolved  = false;
        p.productType   = productType;
        state.openPositions.put(posKey(p), p);
        return p;
    }

    private void refreshUnresolvedFills() {
        if (state.openPositions.isEmpty()) return;
        for (Position p : state.openPositions.values()) {
            if (p == null || p.fillResolved) continue;
            if (p.entryOrderId == null || p.entryOrderId.isBlank()) continue;
            try {
                double fillPrice = orderService.getFilledPriceByOrderId(p.entryOrderId);
                if (fillPrice <= 0) continue;
                double oldEntry = p.entryPrice;
                p.entryPrice = round2(fillPrice);
                if (p.isShort && p.slLevel > 0 && p.originalSlLevel > 0) {
                    double mult = Math.max(1.0, riskSettings.getStrangleSlMultiplier());
                    p.slLevel = p.entryPrice * mult;
                    p.originalSlLevel = p.slLevel;
                }
                p.fillResolved = true;
                event("[INFO]", "Fill", shortSym(p.symbol) + " (" + p.role + ") entry "
                    + round2(oldEntry) + " → " + round2(p.entryPrice) + " (SL " + round2(p.slLevel) + ")");
                saveToDisk();
            } catch (Exception e) {
                log.warn("[Strangle] fill lookup failed for {}: {}", p.entryOrderId, e.getMessage());
            }
        }
    }

    public synchronized void watchSquareoff() {
        if (state.openPositions.isEmpty()) return;
        String hhmm = riskSettings.getStrangleSquareOffTime();
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

    private boolean closePosition(Position p, String reason) {
        if (p == null) return false;
        String symbol = p.symbol;
        String productType = (p.productType == null || p.productType.isBlank())
            ? riskSettings.getStrangleOrderType()
            : p.productType;
        int closeSide = p.isShort ? +1 : -1;
        OrderDTO close = orderService.placeExitOrder(symbol, p.qty, closeSide, productType);
        double exitPrice = 0;
        if (close != null) { try { exitPrice = marketDataService.getLtp(symbol); } catch (Exception ignored) {} }
        double sellTurnover = (p.isShort ? p.entryPrice : exitPrice) * p.qty;
        double buyTurnover  = (p.isShort ? exitPrice    : p.entryPrice) * p.qty;
        double gross   = p.isShort
            ? (p.entryPrice - exitPrice) * p.qty
            : (exitPrice    - p.entryPrice) * p.qty;
        double charges = perCycleCharges(sellTurnover, buyTurnover);
        double net     = gross - charges;

        long closedAt = System.currentTimeMillis();
        String setupName = p.setup == null ? "MANUAL" : p.setup.name();
        String dbStrategyId = (p.setup == ActiveSetup.MANUAL) ? MANUAL_STRATEGY_ID : STRATEGY_ID;
        persistTradeRow(dbStrategyId, symbol, setupName, reason, p.qty,
            gross, charges, net, "SL_HIT".equals(reason) ? 1 : 0,
            closedAt, p.openMillis, p.entryPrice, exitPrice, p.instrument);

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("strategyId",     dbStrategyId);
        cycle.put("setup",          setupName);
        cycle.put("role",           p.role);
        cycle.put("instrument",     p.instrument);
        cycle.put("side",           p.isShort ? "SELL" : "BUY");
        cycle.put("legSide",        p.side);
        cycle.put("strike",         p.strike);
        cycle.put("symbol",         p.symbol);
        cycle.put("qty",            p.qty);
        cycle.put("entryPrice",     round2(p.entryPrice));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    reason);
        cycle.put("closedAtMillis", closedAt);
        cycle.put("openedAtMillis", p.openMillis);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 200) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Exit",
            shortSym(symbol) + " (" + p.role + ") closed (" + reason + ") net="
            + round2(net) + " gross=" + round2(gross));
        state.openPositions.remove(posKey(p));
        saveToDisk();
        return true;
    }

    private void persistTradeRow(String strategyId, String symbol, String setup, String reason,
                                 int qty, double gross, double charges, double net, int slHits,
                                 long closedAt, long openedAt, double entryPrice, double exitPrice,
                                 String instrument) {
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo == null) return;
            StrategyTradeEntity row = new StrategyTradeEntity();
            row.setStrategyId(strategyId == null ? STRATEGY_ID : strategyId);
            row.setSymbol(symbol);
            row.setSetup(setup);
            row.setSessionDate(LocalDate.now(IST).toString());
            row.setClosedAtMillis(closedAt);
            row.setOpenedAtMillis(openedAt);
            row.setInstrument(instrument);
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
            log.warn("[Strangle] persist trade failed: {}", e.getMessage());
        }
    }

    // ── Maintenance ─────────────────────────────────────────────────────────

    public synchronized Map<String, Object> clearAllRecords() {
        int cyc = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();
        state.tradesToday = 0;
        state.consecutiveLosses = 0;
        int ev = state.recentEvents.size();
        state.recentEvents.clear();
        saveToDisk();
        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) dbCleared = repo.deleteAllRows();
        } catch (Exception e) { log.warn("[Strangle] clearAllRecords DB wipe failed: {}", e.getMessage()); }
        event("[WARNING]", "Maintenance",
            "Cleared ALL records — cycles=" + cyc + " events=" + ev + " dbRows=" + dbCleared);
        publishStream();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cyclesCleared", cyc); out.put("eventsCleared", ev); out.put("dbCleared", dbCleared);
        return out;
    }

    public synchronized Map<String, Object> clearTodayRecords() {
        int cyc = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();
        state.tradesToday = 0;
        long startOfToday = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
        int evBefore = state.recentEvents.size();
        state.recentEvents.removeIf(e -> {
            Object ts = e.get("ts");
            return ts instanceof Number && ((Number) ts).longValue() >= startOfToday;
        });
        int ev = evBefore - state.recentEvents.size();
        saveToDisk();
        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) dbCleared = repo.deleteBySessionDate(LocalDate.now(IST).toString());
        } catch (Exception e) { log.warn("[Strangle] clearTodayRecords DB wipe failed: {}", e.getMessage()); }
        event("[WARNING]", "Maintenance",
            "Cleared today — cycles=" + cyc + " events=" + ev + " dbRows=" + dbCleared);
        publishStream();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cyclesCleared", cyc); out.put("eventsCleared", ev); out.put("dbCleared", dbCleared);
        return out;
    }

    // ── Day rollover ─────────────────────────────────────────────────────────

    @Scheduled(cron = "0 0 6 * * *", zone = "Asia/Kolkata")
    public synchronized void scheduledDailyReset() {
        String today = LocalDate.now(IST).toString();
        log.info("[Strangle] 06:00 IST daily reset (was dayKey={})", state.dayKey);
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
        state.entered           = false;
        state.originalCeStrike  = 0;
        state.originalPeStrike  = 0;
        for (Position p : new ArrayList<>(state.openPositions.values())) {
            if (p != null) closePosition(p, "DAY_ROLLOVER");
        }
        saveToDisk();
    }

    // ── Charges + MTM helpers ──────────────────────────────────────────────

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
        } catch (Exception e) { return 0; }
    }

    private double exposedRiskNow() {
        double total = 0;
        for (Position p : state.openPositions.values()) {
            if (p == null || !p.isShort || p.slLevel <= 0) continue;
            total += Math.max(0, p.slLevel - p.entryPrice) * p.qty;
        }
        return total;
    }

    // ── Dashboard state ────────────────────────────────────────────────────

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

        Map<String, Object> str = new LinkedHashMap<>();
        str.put("originalCeStrike", state.originalCeStrike);
        str.put("originalPeStrike", state.originalPeStrike);
        m.put("strangle", str);

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            double ltp = safeLtp(p.symbol);
            double mtm = openPositionMtm(p);
            row.put("symbol",     p.symbol);
            row.put("role",       p.role);
            row.put("instrument", p.instrument);
            row.put("side",       p.side);
            row.put("strike",     p.strike);
            row.put("qty",        p.qty);
            row.put("entryPrice", round2(p.entryPrice));
            row.put("ltp",        round2(ltp));
            row.put("mtm",        round2(mtm));
            row.put("slLevel",    round2(p.slLevel));
            row.put("isShort",    p.isShort);
            row.put("openMillis", p.openMillis);
            rows.add(row);
        }
        m.put("openPositions", rows);

        Map<String, Object> risk = new LinkedHashMap<>();
        risk.put("exposedRisk",     round2(exposedRiskNow()));
        risk.put("initialCapital",  round2(initialCapital()));
        m.put("risk", risk);

        m.put("todayClosedTrades", new ArrayList<>(state.todayClosedTrades));
        m.put("recentEvents",      new ArrayList<>(state.recentEvents));

        try {
            double spot = marketDataService.getLtp("BSE:SENSEX-INDEX");
            m.put("spotLtp",    round2(spot));
            m.put("spotSymbol", "BSE:SENSEX-INDEX");
        } catch (Exception ignored) {}
        return m;
    }

    // ── State ───────────────────────────────────────────────────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class State {
        public String dayKey = "";
        public int    tradesToday;
        public int    consecutiveLosses;
        public Map<String, Position> openPositions = new ConcurrentHashMap<>();
        public List<Map<String, Object>> todayClosedTrades = new ArrayList<>();
        public List<Map<String, Object>> recentEvents      = new ArrayList<>();
        public boolean entered;
        public long   originalCeStrike;
        public long   originalPeStrike;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Position {
        public String     symbol = "";
        public ActiveSetup setup;
        public String     role = "";
        public String     instrument = "";
        public String     side = "";
        public long       strike;
        public int        qty;
        public double     entryPrice;
        public String     entryOrderId = "";
        public long       openMillis;
        public double     slLevel;
        public double     originalSlLevel;
        public boolean    fillResolved;
        public boolean    isShort = true;
        public String     productType = "";
    }

    // ── Event log + persistence + utility ──────────────────────────────────

    public void postEvent(String severity, String source, String message) { event(severity, source, message); }

    private void event(String severity, String source, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts",       System.currentTimeMillis());
        e.put("severity", severity);
        e.put("source",   source);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [strangle:" + source + "] " + message);
        publishStream();
    }

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.openPositions == null)     state.openPositions     = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null)      state.recentEvents      = new ArrayList<>();
            }
        } catch (IOException e) {
            log.warn("[Strangle] failed to load state: {}", e.getMessage());
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
            log.warn("[Strangle] failed to save state: {}", e.getMessage());
        }
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private double safeLtp(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getLtp(sym)); } catch (Exception e) { return 0; }
    }

    private static String shortSym(String s) {
        if (s == null || s.isBlank()) return "";
        if (s.endsWith("CE") || s.endsWith("PE")) {
            int len = s.length(); int strikeEnd = len - 2; int strikeStart = strikeEnd;
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
