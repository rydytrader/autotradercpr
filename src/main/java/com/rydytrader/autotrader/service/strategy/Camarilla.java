package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.dto.CamarillaLevels;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.AtmTracker;
import com.rydytrader.autotrader.service.CamarillaService;
import com.rydytrader.autotrader.service.CamarillaStreamBroker;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Camarilla options-selling strategy — monitors ATM CE/PE option price charts (not NIFTY spot).
 *
 * <p>Two entry setups, evaluated per option symbol on its own 5-min candle close:
 * <ol>
 *   <li><b>H3 Reversal</b> — red candle high ≥ H3 AND close &lt; H3 → sell that option.
 *       Target: premium ≤ L3. SL: premium ≥ H4 (on 5-min candle close).</li>
 *   <li><b>L4 Breakdown</b> — red candle close &lt; L4 → sell that option.
 *       Target: premium ≤ L5. SL: premium ≥ L3 (on 5-min candle close).</li>
 * </ol>
 *
 * <p>Position state is keyed by option symbol. Multiple symbols can hold concurrent shorts
 * (hard cap = 4) — useful when intraday ATM shift creates new candidate symbols while older
 * strikes still have running trades. State persists to {@code ../store/data/camarilla-state.json}.
 */
@Service
public class Camarilla implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(Camarilla.class);
    private static final String STRATEGY_ID = "camarilla";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/data/camarilla-state.json";
    private static final int LOT_SIZE = 75;
    private static final int RECENT_EVENTS_LIMIT = 60;
    /** Number of consecutive fast-scheduler polls (~500 ms each) that LTP must sit at or above
     *  the SL level before the position is squared off. At ~500 ms cadence, 3 polls ≈ 1.5 s of
     *  confirmation — enough to reject single-tick spikes, fast enough that slippage past the
     *  level stays small. */
    private static final int SL_BREACH_CONFIRM_TICKS = 3;

    public enum ActiveSetup { H3_REVERSAL, L4_BREAKDOWN }

    private final CamarillaService      camarillaService;
    private final CandleAggregator      candleAggregator;
    private final AtmTracker            atmTracker;
    private final MarketDataService     marketDataService;
    private final OrderService          orderService;
    private final EventService          eventService;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectProvider<CamarillaStreamBroker>   streamBrokerProvider;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    private volatile State state = new State();
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();

    public Camarilla(CamarillaService camarillaService,
                     CandleAggregator candleAggregator,
                     AtmTracker atmTracker,
                     MarketDataService marketDataService,
                     OrderService orderService,
                     EventService eventService,
                     RiskSettingsStore riskSettings,
                     ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                     ObjectProvider<CamarillaStreamBroker> streamBrokerProvider) {
        this.camarillaService     = camarillaService;
        this.candleAggregator     = candleAggregator;
        this.atmTracker           = atmTracker;
        this.marketDataService    = marketDataService;
        this.orderService         = orderService;
        this.eventService         = eventService;
        this.riskSettings         = riskSettings;
        this.tradeRepoProvider    = tradeRepoProvider;
        this.streamBrokerProvider = streamBrokerProvider;
    }

    /** Push the latest dashboard state to every SSE-connected browser. No-op when no clients. */
    private void publishStream() {
        try {
            CamarillaStreamBroker b = streamBrokerProvider == null ? null : streamBrokerProvider.getIfAvailable();
            if (b != null) b.publish();
        } catch (Exception ignored) {}
    }

    @PostConstruct
    public void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        // Re-subscribe candle listeners for any positions restored from disk so we keep
        // evaluating their exit conditions after a restart.
        for (String sym : state.openPositions.keySet()) {
            candleAggregator.subscribe(sym, c -> onCandleClose(sym, c));
        }
        atmTracker.setListener(this::onAtmChange);
        log.info("[Camarilla] booted — enabled={}, lots={}, squareoff={}, restoredPositions={}",
            riskSettings.isCamarillaEnabled(), riskSettings.getCamarillaLotsPerLeg(),
            riskSettings.getCamarillaSquareOffTime(), state.openPositions.size());
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id() { return STRATEGY_ID; }
    @Override public String displayName() { return "Camarilla"; }
    @Override public String description() { return "ATM CE/PE Camarilla short — H3 reversal + L4 breakdown"; }
    @Override public String currentState() {
        if (state.doneForDay) return "DONE_FOR_DAY";
        return state.openPositions.isEmpty() ? "IDLE" : "OPEN(" + state.openPositions.size() + ")";
    }
    @Override public boolean isEnabled() { return riskSettings.isCamarillaEnabled(); }

    @Override
    public boolean forceClose(String reason) {
        boolean anyClosed = false;
        synchronized (this) {
            if (state.openPositions.isEmpty()) return false;
            List<String> symbols = new ArrayList<>(state.openPositions.keySet());
            for (String sym : symbols) {
                if (closePosition(sym, reason == null ? "MANUAL" : reason)) anyClosed = true;
            }
        }
        return anyClosed;
    }

    @Override
    public void resetToIdle(String reason) {
        synchronized (this) {
            // Drop in-memory positions WITHOUT placing exits (operator recovery flow).
            for (String sym : new ArrayList<>(state.openPositions.keySet())) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            state.doneForDay = false;
            saveToDisk();
            event("[INFO]", "reset — " + (reason == null ? "" : reason));
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
                net += openPositionMtm(p) - perCycleCharges(p.entryPrice * p.qty, currentExitTurnover(p));
            }
            return round2(net);
        }
    }

    @Override
    public double liveChargesToday() {
        rolloverIfNewDay();
        synchronized (this) {
            double ch = 0;
            for (Map<String, Object> m : state.todayClosedTrades) ch += asDouble(m.get("charges"));
            for (Position p : state.openPositions.values()) {
                ch += perCycleCharges(p.entryPrice * p.qty, currentExitTurnover(p));
            }
            return round2(ch);
        }
    }

    @Override
    public void tick() {
        rolloverIfNewDay();
        watchSquareoff();
        // Ensure ATM-around warm-up is at least requested once an ATM is known.
        if (atmTracker.getCurrentAtm() > 0 && camarillaService.snapshot().isEmpty()) {
            camarillaService.warmUpAroundAtm(atmTracker.getCurrentAtm());
        }
    }

    @Override
    public void fastSlCheck() {
        // Fast-tick TARGET + SL watcher — fires on the live LTP, not on candle close.
        //   • TARGET: single-tick. As soon as ltp <= targetLevel, close immediately.
        //   • SL:     confirmed over SL_BREACH_CONFIRM_TICKS consecutive polls (~1.5 s) to
        //             reject single-tick spikes. The breach counter resets the moment ltp
        //             drops back below slLevel.
        if (state.openPositions.isEmpty()) return;
        for (String symbol : new ArrayList<>(state.openPositions.keySet())) {
            Position p = state.openPositions.get(symbol);
            if (p == null) continue;
            double ltp;
            try { ltp = marketDataService.getLtp(symbol); }
            catch (Exception e) { continue; }
            if (ltp <= 0) continue;

            // TARGET first — if both fire on the same tick the win takes precedence.
            if (ltp <= p.targetLevel) {
                Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
                synchronized (lock) {
                    Position p2 = state.openPositions.get(symbol);
                    if (p2 == null) continue;
                    if (ltp > p2.targetLevel) continue;
                    event("[SUCCESS]", symbol + " " + p2.setup + " TARGET_HIT — ltp=" + round2(ltp)
                        + " <= target=" + round2(p2.targetLevel));
                    closePosition(symbol, "TARGET_HIT");
                }
                continue;
            }

            // SL: counted, confirmed.
            if (ltp >= p.slLevel) {
                p.slBreachStreak++;
                if (p.slBreachStreak >= SL_BREACH_CONFIRM_TICKS) {
                    Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
                    synchronized (lock) {
                        Position p2 = state.openPositions.get(symbol);
                        if (p2 == null) continue;
                        event("[WARNING]", symbol + " " + p2.setup + " SL_HIT — ltp=" + round2(ltp)
                            + " >= SL=" + round2(p2.slLevel)
                            + " confirmed over " + SL_BREACH_CONFIRM_TICKS + " ticks");
                        closePosition(symbol, "SL_HIT");
                    }
                }
            } else if (p.slBreachStreak > 0) {
                // Price retreated — reset the streak. Avoids accumulating partial breaches
                // across the whole life of the position.
                p.slBreachStreak = 0;
            }
        }
    }

    // ── ATM change handler ──────────────────────────────────────────────────

    public synchronized void onAtmChange(AtmTracker.AtmChange ev) {
        // Subscribe new ATM CE+PE so we start evaluating entries on those charts.
        if (ev.newCeSym() != null && !ev.newCeSym().isBlank()) {
            candleAggregator.subscribe(ev.newCeSym(), c -> onCandleClose(ev.newCeSym(), c));
        }
        if (ev.newPeSym() != null && !ev.newPeSym().isBlank()) {
            candleAggregator.subscribe(ev.newPeSym(), c -> onCandleClose(ev.newPeSym(), c));
        }
        // Retire old ATM subscriptions IF no open position is parked on them.
        if (ev.oldCeSym() != null && !ev.oldCeSym().isBlank()
            && !state.openPositions.containsKey(ev.oldCeSym())) {
            candleAggregator.unsubscribe(ev.oldCeSym());
        }
        if (ev.oldPeSym() != null && !ev.oldPeSym().isBlank()
            && !state.openPositions.containsKey(ev.oldPeSym())) {
            candleAggregator.unsubscribe(ev.oldPeSym());
        }
        // Trigger level warm-up around the new ATM (no-op if already in-flight).
        camarillaService.warmUpAroundAtm(ev.newAtm());

        String tag = ev.oldAtm() < 0 ? "boot" : String.valueOf(ev.oldAtm());
        event("[INFO]", "ATM " + tag + " → " + ev.newAtm()
            + " (CE=" + ev.newCeSym() + " PE=" + ev.newPeSym() + ")");
        saveToDisk();
    }

    // ── Candle close handler — entries + exits, per symbol ──────────────────

    public void onCandleClose(String symbol, Candle c) {
        if (!isEnabled()) return;
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            rolloverIfNewDay();
            if (state.doneForDay) return;

            // ── Exit check on existing position at this symbol ──
            // SL and target exits now fire from the fast-tick scheduler (fastSlCheck) — single-
            // tick target + 3-tick-confirmed SL. The candle-close path is entry-only when a
            // position is already open: there's nothing for it to do here. Returning early.
            if (state.openPositions.containsKey(symbol)) return;

            // ── Entry check at idle on THIS symbol ──
            if (!canFireNewEntry()) return;
            CamarillaLevels lv = camarillaService.getLevels(symbol);
            if (lv == null) {
                // Warm-up in progress — skip this bar; we'll try again next 5 min.
                return;
            }

            // Both setups are always-on and symmetric — break-from-above on the entry bar:
            //   H3 reversal:  bar touches H3 from above and closes back below.
            //   L4 breakdown: bar touches L4 from above and closes back below.
            // The high() requirement protects against firing trivially when today's premium
            // is structurally below the level (e.g. freshly-shifted strikes with stale yesterday-
            // anchored levels) — the candle must have actually reached the level on this bar.
            if (c.isRed() && c.high() >= lv.h3() && c.close() < lv.h3()) {
                fire(symbol, ActiveSetup.H3_REVERSAL, lv.l3(), lv.h4(), c);
                return;
            }
            if (c.isRed() && c.high() >= lv.l4() && c.close() < lv.l4()) {
                fire(symbol, ActiveSetup.L4_BREAKDOWN, lv.l5(), lv.l3(), c);
            }
        }
    }

    private boolean canFireNewEntry() {
        // Trading-start-time gate: new entries only after this IST clock time.
        String startHhmm = riskSettings.getCamarillaTradingStartTime();
        if (startHhmm != null && !startHhmm.isBlank()) {
            try {
                LocalTime start = LocalTime.parse(startHhmm);
                if (ZonedDateTime.now(IST).toLocalTime().isBefore(start)) return false;
            } catch (Exception ignored) {}
        }
        int maxConcurrent = riskSettings.getCamarillaMaxConcurrentPositions();
        if (maxConcurrent <= 0) maxConcurrent = 4;
        if (state.openPositions.size() >= maxConcurrent) return false;
        return true;
    }

    private void fire(String symbol, ActiveSetup setup, double targetLevel, double slLevel, Candle entryCandle) {
        int qty = riskSettings.getCamarillaLotsPerLeg() * LOT_SIZE;
        String productType = riskSettings.getCamarillaOrderType();

        double entryLtp;
        try { entryLtp = marketDataService.getLtp(symbol); }
        catch (Exception e) { entryLtp = entryCandle.close(); }
        if (entryLtp <= 0) entryLtp = entryCandle.close();

        log.info("[Camarilla] {} fired — selling {} qty={} entryLtp={} target={} sl={}",
            setup, symbol, qty, entryLtp, targetLevel, slLevel);
        event("[INFO]", setup + " fired — sell " + symbol + " qty " + qty
            + " entry≈" + round2(entryLtp) + " target=" + round2(targetLevel) + " SL=" + round2(slLevel));

        // side = -1 → sell (short the option)
        OrderDTO order = orderService.placeOrder(symbol, qty, -1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            log.warn("[Camarilla] entry order rejected for {} — staying idle", symbol);
            event("[ERROR]", "entry order rejected for " + symbol);
            return;
        }

        try { marketDataService.subscribeAdditional(Collections.singletonList(symbol)); }
        catch (Exception ignored) {}

        Position p = new Position();
        p.symbol       = symbol;
        p.setup        = setup;
        p.qty          = qty;
        p.entryPrice   = entryLtp;
        p.entryOrderId = order.getId();
        p.openMillis   = System.currentTimeMillis();
        p.targetLevel  = targetLevel;
        p.slLevel      = slLevel;
        state.openPositions.put(symbol, p);
        state.tradesToday++;
        saveToDisk();
    }

    /** Time-based squareoff — flatten every open position at the configured IST time. */
    public synchronized void watchSquareoff() {
        if (state.openPositions.isEmpty()) return;
        String hhmm = riskSettings.getCamarillaSquareOffTime();
        if (hhmm == null || hhmm.isBlank()) return;
        LocalTime cutoff;
        try { cutoff = LocalTime.parse(hhmm); }
        catch (Exception e) { return; }
        if (ZonedDateTime.now(IST).toLocalTime().isAfter(cutoff)) {
            event("[INFO]", "TIMED_EXIT — clock reached " + hhmm + ", flattening " + state.openPositions.size() + " position(s)");
            for (String sym : new ArrayList<>(state.openPositions.keySet())) {
                closePosition(sym, "TIMED_EXIT");
            }
        }
    }

    /** Close a single open position via market exit. Returns true when the close action was
     *  attempted (the order may still fail at the broker). */
    private boolean closePosition(String symbol, String reason) {
        Position p = state.openPositions.get(symbol);
        if (p == null) return false;
        // side = +1 → buy to close
        OrderDTO close = orderService.placeExitOrder(symbol, p.qty, 1);
        double exitPrice = 0;
        if (close != null) {
            try { exitPrice = marketDataService.getLtp(symbol); }
            catch (Exception ignored) {}
        }
        double sellTurnover = p.entryPrice * p.qty;
        double buyTurnover  = exitPrice * p.qty;
        double gross   = (p.entryPrice - exitPrice) * p.qty;
        double charges = perCycleCharges(sellTurnover, buyTurnover);
        double net     = gross - charges;

        persistTradeRow(reason, p.qty, gross, charges, net, reason.equals("SL_HIT") ? 1 : 0);

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("setup",          p.setup.name());
        cycle.put("symbol",         p.symbol);
        cycle.put("qty",            p.qty);
        cycle.put("entryPrice",     round2(p.entryPrice));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    reason);
        cycle.put("closedAtMillis", System.currentTimeMillis());
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 100) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]",
            symbol + " closed (" + reason + ") net=" + round2(net) + " gross=" + round2(gross));

        state.openPositions.remove(symbol);

        // Stop subscribing to this symbol's candles UNLESS it's still the current ATM.
        if (!symbol.equals(atmTracker.getCurrentCeSym()) && !symbol.equals(atmTracker.getCurrentPeSym())) {
            candleAggregator.unsubscribe(symbol);
        }

        saveToDisk();
        return true;
    }

    private void persistTradeRow(String reason, int qty, double gross, double charges, double net, int slHits) {
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo == null) return;
            StrategyTradeEntity row = new StrategyTradeEntity();
            row.setStrategyId(STRATEGY_ID);
            row.setSessionDate(LocalDate.now(IST).toString());
            row.setClosedAtMillis(System.currentTimeMillis());
            row.setQty(qty);
            row.setGrossPnl(round2(gross));
            row.setCharges(round2(charges));
            row.setNetPnl(round2(net));
            row.setCloseReason(reason);
            row.setSlHitCount(slHits);
            repo.save(row);
        } catch (Exception e) {
            log.warn("[Camarilla] persist trade failed: {}", e.getMessage());
        }
    }

    // ── Day rollover ─────────────────────────────────────────────────────────

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        synchronized (this) {
            if (today.equals(state.dayKey)) return;
            state.dayKey = today;
            state.tradesToday = 0;
            state.consecutiveLosses = 0;
            state.doneForDay = false;
            state.todayClosedTrades.clear();
            // Any positions surviving overnight are dropped (intraday product or operator action).
            for (String sym : new ArrayList<>(state.openPositions.keySet())) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            saveToDisk();
        }
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
            return (p.entryPrice - ltp) * p.qty;
        } catch (Exception e) {
            return 0;
        }
    }

    // ── Dashboard payload (consumed by CamarillaController + Trade page) ─────

    public synchronized Map<String, Object> dashboardState() {
        rolloverIfNewDay();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("strategy",          STRATEGY_ID);
        m.put("enabled",           isEnabled());
        m.put("lifecycle",         currentState());
        m.put("doneForDay",        state.doneForDay);
        m.put("dayKey",            state.dayKey);
        m.put("tradesToday",       state.tradesToday);
        m.put("consecutiveLosses", state.consecutiveLosses);
        m.put("currentAtm",        atmTracker.getCurrentAtm());
        m.put("currentAtmCeSym",   atmTracker.getCurrentCeSym());
        m.put("currentAtmPeSym",   atmTracker.getCurrentPeSym());

        // Open positions list — each row carries its own LTP, MTM, target/SL levels.
        List<Map<String, Object>> rows = new ArrayList<>();
        double exposedRisk = 0;
        for (Position p : state.openPositions.values()) {
            Map<String, Object> row = new LinkedHashMap<>();
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); } catch (Exception ignored) {}
            double mtm = openPositionMtm(p);
            row.put("symbol",      p.symbol);
            row.put("setup",       p.setup.name());
            row.put("qty",         p.qty);
            row.put("entryPrice",  round2(p.entryPrice));
            row.put("ltp",         round2(ltp));
            row.put("mtm",         round2(mtm));
            row.put("targetLevel", round2(p.targetLevel));
            row.put("slLevel",     round2(p.slLevel));
            row.put("openMillis",  p.openMillis);
            rows.add(row);
            exposedRisk += Math.max(0, p.slLevel - p.entryPrice) * p.qty;
        }
        m.put("openPositions", rows);

        // Per-symbol levels for monitored symbols (current ATM + any open-position symbols).
        Map<String, CamarillaLevels> perSymbolLevels = new LinkedHashMap<>();
        if (!atmTracker.getCurrentCeSym().isEmpty()) {
            CamarillaLevels lv = camarillaService.getLevels(atmTracker.getCurrentCeSym());
            if (lv != null) perSymbolLevels.put(atmTracker.getCurrentCeSym(), lv);
        }
        if (!atmTracker.getCurrentPeSym().isEmpty()) {
            CamarillaLevels lv = camarillaService.getLevels(atmTracker.getCurrentPeSym());
            if (lv != null) perSymbolLevels.put(atmTracker.getCurrentPeSym(), lv);
        }
        for (String sym : state.openPositions.keySet()) {
            if (perSymbolLevels.containsKey(sym)) continue;
            CamarillaLevels lv = camarillaService.getLevels(sym);
            if (lv != null) perSymbolLevels.put(sym, lv);
        }
        m.put("perSymbolLevels", perSymbolLevels);

        // Risk block — same shape as equities Positions page badges.
        double consumedRisk = 0;
        for (Map<String, Object> trade : state.todayClosedTrades) {
            double net = asDouble(trade.get("netPnl"));
            if (net < 0) consumedRisk += Math.abs(net);
        }
        Map<String, Object> risk = new LinkedHashMap<>();
        risk.put("exposedRisk",      round2(exposedRisk));
        risk.put("consumedRisk",     round2(consumedRisk));
        // Risk Budget now comes from portfolio-wide settings (Initial Capital × Portfolio Max
        // Daily Risk %) — managed in the PORTFOLIO RISK tab, not per-strategy.
        risk.put("dailyRiskBudget",  round2(riskSettings.getPortfolioMaxDailyLoss()));
        m.put("risk", risk);

        m.put("todayClosedTrades", new ArrayList<>(state.todayClosedTrades));
        m.put("recentEvents",      new ArrayList<>(state.recentEvents));
        try {
            double spot = marketDataService.getLtp(NIFTY_SYMBOL);
            m.put("niftySpot", round2(spot));
        } catch (Exception ignored) {}
        return m;
    }

    // ── State persistence ───────────────────────────────────────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class State {
        public String dayKey = "";
        public int    tradesToday;
        public int    consecutiveLosses;
        public boolean doneForDay;
        public Map<String, Position> openPositions = new ConcurrentHashMap<>();
        public List<Map<String, Object>> todayClosedTrades = new ArrayList<>();
        public List<Map<String, Object>> recentEvents      = new ArrayList<>();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Position {
        public String     symbol = "";
        public ActiveSetup setup;
        public int        qty;
        public double     entryPrice;
        public String     entryOrderId = "";
        public long       openMillis;
        public double     targetLevel;
        public double     slLevel;
        /** Consecutive fast-tick polls observing LTP at or above slLevel. Resets on every poll
         *  where LTP drops back below. SL fires when this reaches SL_BREACH_CONFIRM_TICKS.
         *  Transient — not persisted, repopulated by the runtime after a restart. */
        public transient int slBreachStreak;
    }

    private void event(String severity, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts",       System.currentTimeMillis());
        e.put("severity", severity);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [camarilla] " + message);
        publishStream();
    }

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.openPositions == null) state.openPositions = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null) state.recentEvents = new ArrayList<>();
            }
        } catch (IOException e) {
            log.warn("[Camarilla] failed to load state: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
            Files.move(tmp, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("[Camarilla] failed to save state: {}", e.getMessage());
        }
    }

    // ── Misc utility ────────────────────────────────────────────────────────

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
    private static double asDouble(Object o) {
        if (o instanceof Number) return ((Number) o).doubleValue();
        if (o == null) return 0;
        try { return Double.parseDouble(o.toString()); } catch (Exception e) { return 0; }
    }
}
