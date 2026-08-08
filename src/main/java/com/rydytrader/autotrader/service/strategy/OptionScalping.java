package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.gdfl.GdflSymbolMapper;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OptionScalpingStreamBroker;
import com.rydytrader.autotrader.service.OrderService;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NIFTY current-month FUTURES 5-min bar plumbing.
 *
 * <p><b>Phase 3 strip</b> — options / ATM / CE / PE / pre-warm / trigger-candle-FSM /
 * entry / SL / target logic has been removed. Only the NIFTY current-month futures
 * symbol (via GDFL, {@link GdflSymbolMapper#FYERS_NIFTY_FUTURES}) is subscribed and
 * bar closes are logged. No strike selection, no orders. The Strategy interface impl
 * still exists as stubs so the wider app boots cleanly.
 *
 * <p>Strike selection + entry FSM are deferred and will be added when the strategy
 * layer is specified. Legacy accessors ({@link #getCeSymbol}, {@link #getAtmStrike},
 * etc.) remain and return zeros / empty strings so existing UI polls degrade
 * gracefully to "—" instead of crashing.
 */
@Service
public class OptionScalping implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(OptionScalping.class);
    private static final String STRATEGY_ID = "option-scalping";
    /** Strategy ID written to DB rows for MANUAL-tagged trades. Analytics, calendar
     *  day-modal, and Trade Log filter on this string so manual scalps stay distinguishable
     *  from algorithm trades while still aggregating into the same portfolio totals. */
    public  static final String MANUAL_STRATEGY_ID = "manual";
    /** The one symbol Phase 3 subscribes to — GDFL delivers NIFTY current-month
     *  futures ticks under {@link GdflSymbolMapper#FYERS_NIFTY_FUTURES}. */
    private static final String NIFTY_SYMBOL = GdflSymbolMapper.FYERS_NIFTY_FUTURES;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/option-scalping-state.json";
    /** NIFTY option lot size — 65 (post 2025 revision). Exposed for the Manual Terminal
     *  controller (translates operator "lots" input into a contract count) so it
     *  doesn't duplicate the constant. Unused by the stripped Phase 3 body itself. */
    private static final int    LOT_SIZE = 65;
    private static final int    RECENT_EVENTS_LIMIT = 60;

    public static int lotSize() { return LOT_SIZE; }

    /** Setup enum kept in the shape older DB rows and state files know so their
     *  serialized {@code setup} column deserializes cleanly. Nothing in the
     *  Phase 3 strip emits new values — legacy values retained for round-trip
     *  compatibility with historical rows and manual-terminal fires. */
    public enum ActiveSetup {
        L3_REVERSAL,      // legacy
        H3_REVERSAL,      // legacy
        H4_BREAKOUT,      // legacy
        L4_BREAKDOWN,     // legacy
        VWAP_BREAKDOWN,   // legacy
        CE_SELL,          // legacy (options)
        PE_SELL,          // legacy (options)
        MANUAL            // reserved for the Options Scalper Terminal path
    }

    /** Composite key {@code "setup|symbol"} for {@code state.openPositions}. Kept for
     *  round-trip compatibility with historical state files. */
    private static String posKey(Position p) {
        if (p == null) return "";
        String setup = p.setup == null ? "MANUAL" : p.setup.name();
        return setup + "|" + (p.symbol == null ? "" : p.symbol);
    }

    /** V2 watchlist role slot — kept only because {@link State#symbolRole} is serialized in
     *  historical state files. No live code branches on it. */
    public enum WatchRole { ATM_L4, ITM_L4, OTM_H3 }

    private final CandleAggregator      candleAggregator;
    private final MarketDataService     marketDataService;
    private final OrderService          orderService;
    private final EventService          eventService;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectProvider<OptionScalpingStreamBroker> streamBrokerProvider;

    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

    private volatile State state = new State();

    public OptionScalping(CandleAggregator candleAggregator,
                   MarketDataService marketDataService,
                   OrderService orderService,
                   EventService eventService,
                   RiskSettingsStore riskSettings,
                   ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                   ObjectProvider<OptionScalpingStreamBroker> streamBrokerProvider) {
        this.candleAggregator           = candleAggregator;
        this.marketDataService          = marketDataService;
        this.orderService               = orderService;
        this.eventService               = eventService;
        this.riskSettings               = riskSettings;
        this.tradeRepoProvider          = tradeRepoProvider;
        this.streamBrokerProvider       = streamBrokerProvider;
    }

    /** Push the latest dashboard state to every SSE-connected browser. No-op when no clients. */
    private void publishStream() {
        try {
            OptionScalpingStreamBroker b = streamBrokerProvider == null ? null : streamBrokerProvider.getIfAvailable();
            if (b != null) b.publish();
        } catch (Exception ignored) {}
    }

    // ── Boot / lifecycle ────────────────────────────────────────────────────

    @PostConstruct
    public void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        pruneStaleEventsBeforeToday();

        // Subscribe the NIFTY current-month FUTURES symbol. GdflService already
        // subscribes it on its own poller — this registers the aggregator close
        // listener so 5-min bar closes get logged (and can be reacted to when a
        // strategy layer is added later).
        state.futuresSymbol = NIFTY_SYMBOL;
        candleAggregator.subscribe(NIFTY_SYMBOL, c -> onCandleClose(NIFTY_SYMBOL, c));
        log.info("[OptionScalping] boot — futures subscribed: {} (5-min bars)", NIFTY_SYMBOL);

        log.info("[OptionScalping] booted — enabled={}, squareoff={}, restoredPositions={}",
            riskSettings.isOptionScalpingEnabled(),
            riskSettings.getOptionScalpingSquareOffTime(), state.openPositions.size());
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
            log.info("[OptionScalping] pruned {} stale event(s) from before today's 00:00 IST", removed);
            saveToDisk();
            publishStream();
        }
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id() { return STRATEGY_ID; }
    @Override public String displayName() { return "NIFTY Futures (5-min)"; }
    @Override public String description() { return "NIFTY current-month futures · 5-min bar plumbing (strike selection deferred)"; }

    // Legacy accessors — stripped strategy has no ATM / CE / PE / SL / entry state.
    // Return neutral values so UI callers degrade to "—" instead of NPE.
    public long   getAtmStrike()                             { return 0L; }
    public String getCeSymbol()                              { return ""; }
    public String getPeSymbol()                              { return ""; }
    public java.util.List<String> getPreWarmSymbols()        { return Collections.emptyList(); }
    public double getOpenSlLevel(String fyersSymbol)         { return 0.0; }
    public double getOpenEntryPrice(String fyersSymbol)      { return 0.0; }
    public int    getCeTradesToday()                         { return 0; }
    public int    getPeTradesToday()                         { return 0; }
    public double getCeSideNetPnlToday()                     { return 0.0; }
    public double getPeSideNetPnlToday()                     { return 0.0; }

    @Override public String currentState() {
        if (state.doneForDay) return "DONE_FOR_DAY";
        return state.openPositions.isEmpty() ? "IDLE" : "OPEN(" + state.openPositions.size() + ")";
    }
    @Override public boolean isEnabled() { return riskSettings.isOptionScalpingEnabled(); }

    @Override
    public boolean forceClose(String reason) {
        // No auto-entries in Phase 3, but a MANUAL-terminal position may still be open —
        // route through the existing close path so manual positions can be flattened
        // from the kill-switch / Trade page.
        synchronized (this) {
            if (state.openPositions.isEmpty()) return false;
            boolean anyClosed = false;
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                if (closePosition(p, reason == null ? "MANUAL" : reason)) anyClosed = true;
            }
            return anyClosed;
        }
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
            java.util.Set<String> uniqueSymbols = new java.util.HashSet<>();
            for (Position p : state.openPositions.values()) {
                if (p != null && p.symbol != null) uniqueSymbols.add(p.symbol);
            }
            for (String sym : uniqueSymbols) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            state.doneForDay = false;
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
            for (Position p : state.openPositions.values()) {
                ch += cycleChargesFor(p);
            }
            return round2(ch);
        }
    }

    @Override
    public void tick() {
        rolloverIfNewDay();
        // No auto entries / no timed squareoff of algo positions — MANUAL-terminal
        // positions (if any) are managed by the Options Scalper Terminal itself.
    }

    @Override
    public void fastSlCheck() {
        // No auto SL logic in Phase 3.
    }

    // ── Candle close handler — logs only, no FSM ───────────────────────────

    public void onCandleClose(String symbol, Candle c) {
        if (c == null) return;
        if (!NIFTY_SYMBOL.equals(symbol)) return;   // only the futures leg is subscribed
        log.info("[OptionScalping] futures {}-min bar closed — {} o={} h={} l={} c={} startMs={}",
            CandleAggregator.BUCKET_MINUTES, symbol,
            c.open(), c.high(), c.low(), c.close(), c.startMillis());
    }

    // ── Position close + persistence (kept for MANUAL-terminal fires) ──────

    private boolean closePosition(Position p, String reason) {
        if (p == null) return false;
        String symbol = p.symbol;
        String productType = (p.productType == null || p.productType.isBlank())
            ? riskSettings.getOptionScalpingOrderType()
            : p.productType;
        int closeSide = p.isShort ? +1 : -1;
        var close = orderService.placeExitOrder(symbol, p.qty, closeSide, productType);
        double exitPrice = 0;
        String exitOrderId = close == null ? null : close.getId();
        if (exitOrderId != null && !exitOrderId.isBlank()) {
            for (int attempt = 0; attempt < 5; attempt++) {
                try {
                    orderService.invalidateTradebookCache();
                    double filled = orderService.getFilledPriceByOrderId(exitOrderId);
                    if (filled > 0) { exitPrice = filled; break; }
                    Thread.sleep(300);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ignored) {}
            }
        }
        if (exitPrice <= 0 && close != null) {
            try { exitPrice = marketDataService.getLtp(symbol); }
            catch (Exception ignored) {}
            if (exitPrice > 0) {
                log.warn("[OptionScalping] exit fill not resolved for order {} on {} — persisting LTP {} as fallback",
                    exitOrderId, symbol, round2(exitPrice));
            }
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
            closedAtMillis, p.openMillis, p.entryPrice, exitPrice,
            p.entryCandleMs, 0L);

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("strategyId",     dbStrategyId);
        cycle.put("setup",          setupName);
        cycle.put("symbol",         p.symbol);
        cycle.put("side",           p.isShort ? "SELL" : "BUY");
        cycle.put("qty",            p.qty);
        cycle.put("entryPrice",     round2(p.entryPrice));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    reason);
        cycle.put("closedAtMillis", closedAtMillis);
        cycle.put("openedAtMillis", p.openMillis);
        cycle.put("entryCandleMs",  p.entryCandleMs);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 100) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Exit",
            symbol + " closed (" + reason + ") net=" + round2(net) + " gross=" + round2(gross));

        state.openPositions.remove(posKey(p));

        // Drop candle subscription if not the futures leg and no other open positions use it.
        boolean stillUsed = false;
        for (Position pp : state.openPositions.values()) {
            if (pp != null && symbol.equals(pp.symbol)) { stillUsed = true; break; }
        }
        boolean isFuturesLeg = NIFTY_SYMBOL.equals(symbol);
        if (!stillUsed && !isFuturesLeg) {
            candleAggregator.unsubscribe(symbol);
        }

        saveToDisk();
        return true;
    }

    private void persistTradeRow(String strategyId, String symbol, String setup, String reason, int qty,
                                 double gross, double charges, double net, int slHits,
                                 long closedAtMillis, long openedAtMillis,
                                 double entryPrice, double exitPrice,
                                 long entryCandleMs, long exitCandleMs) {
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
            row.setInstrument(instrumentFromSymbol(symbol));
            row.setQty(qty);
            row.setGrossPnl(round2(gross));
            row.setCharges(round2(charges));
            row.setNetPnl(round2(net));
            row.setCloseReason(reason);
            row.setSlHitCount(slHits);
            row.setEntryPrice(entryPrice > 0 ? round2(entryPrice) : null);
            row.setExitPrice(exitPrice   > 0 ? round2(exitPrice)  : null);
            row.setEntryCandleMs(entryCandleMs > 0 ? entryCandleMs : null);
            row.setExitCandleMs (exitCandleMs  > 0 ? exitCandleMs  : null);
            repo.save(row);
        } catch (Exception e) {
            log.warn("[OptionScalping] persist trade failed: {}", e.getMessage());
        }
    }

    private static String instrumentFromSymbol(String symbol) {
        if (symbol == null) return null;
        String s = symbol.toUpperCase();
        if (s.contains("BANKNIFTY") || s.contains("NIFTYBANK")) return "BANKNIFTY";
        if (s.contains("NIFTY")) return "NIFTY";
        return null;
    }

    // ── Maintenance actions (Trade page / Settings) ────────────────────────

    public synchronized Map<String, Object> clearAllRecords() {
        int cyclesCleared = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();

        state.tradesToday        = 0;
        state.consecutiveLosses  = 0;

        int eventsCleared = state.recentEvents.size();
        state.recentEvents.clear();

        saveToDisk();

        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) {
                dbCleared = repo.deleteAllRows();
                log.warn("[OptionScalping] clearAllRecords — DB deleteAllRows wiped {} rows", dbCleared);
            }
        } catch (Exception e) {
            log.warn("[OptionScalping] clearAllRecords DB wipe failed: {}", e.getMessage());
        }

        event("[WARNING]", "Maintenance",
            "Cleared ALL records — cycles=" + cyclesCleared
            + " events=" + eventsCleared
            + " dbRows=" + dbCleared
            + " (open positions preserved)");
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

        state.tradesToday        = 0;
        state.consecutiveLosses  = 0;

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
            if (repo != null) {
                dbCleared = repo.deleteBySessionDate(LocalDate.now(IST).toString());
            }
        } catch (Exception e) {
            log.warn("[OptionScalping] clearTodayRecords DB wipe failed: {}", e.getMessage());
        }

        event("[WARNING]", "Maintenance",
            "Cleared today's records — cycles=" + cyclesCleared
            + " events=" + eventsCleared
            + " dbRows=" + dbCleared
            + " (open positions preserved)");
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
        log.info("[OptionScalping] 06:00 IST daily reset (was dayKey={})", state.dayKey);
        state.dayKey = today;
        state.tradesToday = 0;
        state.consecutiveLosses = 0;
        state.doneForDay = false;
        state.todayClosedTrades.clear();
        if (state.recentEvents != null) state.recentEvents.clear();
        java.util.Set<String> uniqSymbols = new java.util.HashSet<>();
        for (Position p : state.openPositions.values()) {
            if (p != null && p.symbol != null) uniqSymbols.add(p.symbol);
        }
        for (String sym : uniqSymbols) {
            candleAggregator.unsubscribe(sym);
        }
        state.openPositions.clear();
        if (state.symbolRole != null) state.symbolRole.clear();
        saveToDisk();
        publishStream();
    }

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        synchronized (this) {
            if (today.equals(state.dayKey)) return;
            state.dayKey = today;

            state.tradesToday       = 0;
            state.consecutiveLosses = 0;
            state.doneForDay        = false;
            state.todayClosedTrades.clear();
            if (state.recentEvents != null) state.recentEvents.clear();

            java.util.Set<String> uniqSymbolsRoll = new java.util.HashSet<>();
            for (Position p : state.openPositions.values()) {
                if (p != null && p.symbol != null) uniqSymbolsRoll.add(p.symbol);
            }
            for (String sym : uniqSymbolsRoll) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            if (state.symbolRole != null) state.symbolRole.clear();

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
            return p.isShort
                ? (p.entryPrice - ltp) * p.qty
                : (ltp - p.entryPrice) * p.qty;
        } catch (Exception e) {
            return 0;
        }
    }

    /** Legacy indicator snapshot — returns neutral (unavailable) fields so the /positions
     *  page's "NIFTY Technicals" tile shows "—" instead of crashing. No CE/PE state is
     *  tracked in Phase 3. */
    public Map<String, Object> optionIndicatorsSnapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ceSymbol", "");
        m.put("peSymbol", "");
        m.put("atmStrike", 0);
        m.put("ceStAvailable", false);
        m.put("ceStLine", 0.0);
        m.put("ceStIsUp", false);
        m.put("peStAvailable", false);
        m.put("peStLine", 0.0);
        m.put("peStIsUp", false);
        m.put("ts", System.currentTimeMillis());
        return m;
    }

    // ── Dashboard payload (consumed by OptionScalpingController + Trade page) ─────

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

        // No CE/PE session legs in Phase 3 — return an empty setupLegs list so
        // trade.html degrades to "—" for the ATM CE / PE header slots.
        m.put("setupLegs",      Collections.<Map<String, Object>>emptyList());
        m.put("watchlistSize",  0);
        m.put("watchlistRoles", new LinkedHashMap<>());

        // Header chips: NIFTY current-month FUTURES symbol + LTP + change +
        // % change. Consumed by the ticker cells on the trade page (see trade.html
        // renderFutTickerCells).
        Map<String, Object> vwap = new LinkedHashMap<>();
        String futSym = state.futuresSymbol == null ? "" : state.futuresSymbol;
        double futLtp = 0, futChange = 0, futChangePct = 0;
        if (!futSym.isBlank()) {
            try { futLtp        = marketDataService.getLtp(futSym); }              catch (Exception ignored) {}
            try { futChange     = marketDataService.getDisplayChange(futSym); }    catch (Exception ignored) {}
            try { futChangePct  = marketDataService.getDisplayChangePct(futSym); } catch (Exception ignored) {}
        }
        vwap.put("futSymbol",    futSym);
        vwap.put("futLtp",       round2(futLtp));
        vwap.put("futChange",    round2(futChange));
        vwap.put("futChangePct", round2(futChangePct));
        // Legacy fields — kept as empty / 0 so any older client code that reads
        // them doesn't NPE. Not populated in Phase 3.
        vwap.put("putSymbol",    "");
        vwap.put("putStrike",    0);
        vwap.put("putLtp",       0.0);
        vwap.put("callSymbol",   "");
        vwap.put("callStrike",   0);
        vwap.put("callLtp",      0.0);
        vwap.put("ceSymbol",     "");
        vwap.put("peSymbol",     "");
        vwap.put("ceVwap",       0.0);
        vwap.put("peVwap",       0.0);
        m.put("optionScalping", vwap);

        // Open positions — may still exist for MANUAL-terminal fires.
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); } catch (Exception ignored) {}
            double mtm = openPositionMtm(p);
            row.put("symbol",       p.symbol);
            row.put("setup",        p.setup == null ? "MANUAL" : p.setup.name());
            row.put("qty",          p.qty);
            row.put("entryPrice",   round2(p.entryPrice));
            row.put("ltp",          round2(ltp));
            row.put("mtm",          round2(mtm));
            row.put("targetLevel",  round2(p.targetLevel));
            row.put("slLevel",      round2(p.slLevel));
            row.put("breakevenMoved", p.breakevenMoved);
            row.put("isShort",      p.isShort);
            row.put("openMillis",   p.openMillis);
            row.put("entryCandleMs",p.entryCandleMs);
            rows.add(row);
        }
        m.put("openPositions", rows);

        m.put("perSymbolLevels", new LinkedHashMap<String, Object>());

        Map<String, Object> risk = new LinkedHashMap<>();
        risk.put("exposedRisk",     0.0);
        risk.put("consumedRisk",    0.0);
        risk.put("dailyRiskBudget", round2(riskSettings.getPortfolioMaxDailyLoss()));
        m.put("risk", risk);

        m.put("todayClosedTrades", new ArrayList<>(state.todayClosedTrades));
        m.put("recentEvents",      new ArrayList<>(state.recentEvents));
        // NIFTY spot no longer fed by GDFL in Phase 3 (we subscribe futures only).
        // Report the futures LTP as niftySpot so older UIs that expect a number
        // don't render "—" for the whole session.
        m.put("niftySpot", round2(futLtp));
        try {
            double vix = marketDataService.getLtp("NSE:INDIAVIX-INDEX");
            m.put("indiaVix", round2(vix));
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
        /** Fyers-style symbol of the primary subscribed leg — the NIFTY current-month
         *  futures {@link GdflSymbolMapper#FYERS_NIFTY_FUTURES} in Phase 3. */
        public String futuresSymbol = "";
        /** Legacy watchlist role map — kept for state-file back-compat. */
        public Map<String, WatchRole> symbolRole = new ConcurrentHashMap<>();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Position {
        public String     symbol = "";
        public ActiveSetup setup;
        public int        qty;
        public double     entryPrice;
        public String     entryOrderId = "";
        public long       openMillis;
        public long       entryCandleMs;
        public double     targetLevel;
        public double     slLevel;
        public double     originalSlLevel;
        public boolean    breakevenMoved;
        public boolean    fillResolved;
        public boolean    isShort = true;
        public String     productType = "";
    }

    // ── Event log ────────────────────────────────────────────────────────────

    /** Public event-log wrapper for external callers (e.g. the kill-switch toggle in
     *  OptionScalpingController). */
    public void postEvent(String severity, String source, String message) {
        event(severity, source, message);
    }

    private void event(String severity, String source, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        long wallTs = System.currentTimeMillis();
        e.put("ts",       wallTs);
        e.put("severity", severity);
        e.put("source",   source);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [option-scalping:" + source + "] " + message);
        publishStream();
    }

    // ── Persistence ─────────────────────────────────────────────────────────

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.openPositions == null)    state.openPositions    = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null)     state.recentEvents     = new ArrayList<>();
                if (state.symbolRole == null)       state.symbolRole       = new ConcurrentHashMap<>();
                purgeRetiredEntries();
            }
        } catch (IOException e) {
            log.warn("[OptionScalping] failed to load state: {}", e.getMessage());
        }
    }

    private void purgeRetiredEntries() {
        if (state.openPositions != null && !state.openPositions.isEmpty()) {
            int before = state.openPositions.size();
            state.openPositions.values().removeIf(p -> p == null || p.setup == null);
            int after = state.openPositions.size();
            if (after != before) {
                log.info("[OptionScalping] purged {} retired-setup entries from openPositions",
                    before - after);
            }
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
            log.warn("[OptionScalping] failed to save state: {}", e.getMessage());
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
