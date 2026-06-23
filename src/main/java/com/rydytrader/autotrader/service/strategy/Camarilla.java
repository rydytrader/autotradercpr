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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
    /** Strategy ID written to DB rows for MANUAL-tagged trades. Analytics, calendar
     *  day-modal, and Trade Log filter on this string so manual scalps stay distinguishable
     *  from algorithm trades while still aggregating into the same portfolio totals. */
    public  static final String MANUAL_STRATEGY_ID = "manual";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/data/camarilla-state.json";
    private static final int LOT_SIZE = 65;
    /** NSE NIFTY option premium tick size — the minimum tradable price. Used to
     *  clamp negative Camarilla L5 targets to a meaningful display floor on the
     *  positions card. */
    private static final double OPTION_TICK_SIZE = 0.05;

    /** NIFTY contract lot size — used by the Manual Terminal controller to translate
     *  the operator's "lots" input into a contract count. Public so the controller
     *  doesn't have to duplicate the constant. */
    public static int lotSize() { return LOT_SIZE; }
    private static final int RECENT_EVENTS_LIMIT = 60;
    /** Number of consecutive fast-scheduler polls (~500 ms each) that LTP must sit at or above
     *  the SL level before the position is squared off. At ~500 ms cadence, 3 polls ≈ 1.5 s of
     *  confirmation — enough to reject single-tick spikes, fast enough that slippage past the
     *  level stays small. */
    private static final int SL_BREACH_CONFIRM_TICKS = 3;
    /** NIFTY strike interval — 50 points. */
    private static final long STRIKE_STEP = 50L;
    /** NIFTY futures tick size — 0.05. Used for the "3 ticks" SL margin around
     *  Camarilla levels (e.g. {@code slFutures = L4 − 3 × TICK_FUTURES}). */
    private static final double TICK_FUTURES = 0.05;
    /** Grace window after a bar close before we resolve the buffer for that bar. All six
     *  symbols' candles emit inside a single 1-second sample tick at the boundary; 1.5 s gives
     *  every candidate a chance to land before the tiebreaker runs. */
    private static final long BAR_PROCESSING_GRACE_MS = 1500L;
    /** Bar length sourced from {@link com.rydytrader.autotrader.service.CandleAggregator#BUCKET_MINUTES}
     *  so this constant never drifts from the aggregator's actual cadence. */
    private static final long BAR_LENGTH_MS =
        com.rydytrader.autotrader.service.CandleAggregator.BUCKET_MINUTES * 60_000L;

    public enum ActiveSetup {
        // v2 setups — all triggered on the NIFTY near-month FUTURES 5-min bar close,
        // all sell a SHORT OTM option. The trade leg's symbol is picked at fire time.
        L3_REVERSAL,       // bullish: green futures bar wicks below L3, closes above → sell ATM−50 PUT
        H3_REVERSAL,       // bearish: red futures bar wicks above H3, closes below → sell ATM+50 CALL
        H4_BREAKOUT,       // bullish: futures bar closes above H4 → sell ATM−50 PUT
        L4_BREAKDOWN,      // bearish: futures bar closes below L4 → sell ATM+50 CALL
        VWAP_BREAKDOWN,    // v1 legacy (retired) — kept for old DB row deserialisation only
        MANUAL             // user-placed via Options Scalper Terminal — direction comes from caller
    }

    /** True for setups that take a SHORT (sell) position on entry. MANUAL is intentionally
     *  excluded — its direction is supplied by the caller, not derived from the setup tag. */
    private static boolean isShortSetup(ActiveSetup s) {
        return s == ActiveSetup.L3_REVERSAL
            || s == ActiveSetup.H3_REVERSAL
            || s == ActiveSetup.H4_BREAKOUT
            || s == ActiveSetup.L4_BREAKDOWN
            || s == ActiveSetup.VWAP_BREAKDOWN;
    }

    /** True for the bullish-bet setups (sell PUT). Used by fire() to pick the ATM−50
     *  PUT vs ATM+50 CALL trade leg. */
    private static boolean isBullishBet(ActiveSetup s) {
        return s == ActiveSetup.L3_REVERSAL || s == ActiveSetup.H4_BREAKOUT;
    }

    /** V2 watchlist role for each monitored option contract. ATM and ITM strikes only check
     *  L4 breakdown; OTM strikes only check H3 reversal. The role is stored per-symbol in
     *  {@link State#symbolRole} so re-subscribed positions inherit it across restarts. */
    public enum WatchRole { ATM_L4, ITM_L4, OTM_H3 }

    /** A buffered entry candidate waiting for end-of-bar tiebreaker selection. */
    private record EntryCandidate(String symbol, WatchRole role, ActiveSetup setup,
                                  double targetLevel, double slLevel, Candle candle) {
        boolean isCe() { return symbol != null && symbol.endsWith("CE"); }
        boolean isPe() { return symbol != null && symbol.endsWith("PE"); }
    }

    /** Per-bar candidate buffer. Key = candle.startMillis (the bar's opening tick timestamp).
     *  Drained by the fast-tick scheduler after {@link #BAR_PROCESSING_GRACE_MS}. */
    private final Map<Long, List<EntryCandidate>> pendingByBar = new ConcurrentHashMap<>();

    private final CamarillaService      camarillaService;
    private final CandleAggregator      candleAggregator;
    private final AtmTracker            atmTracker;
    private final BalancedAtmSelector   atmSelector;
    private final MarketDataService     marketDataService;
    private final OrderService          orderService;
    private final EventService          eventService;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectProvider<CamarillaStreamBroker>   streamBrokerProvider;
    private final ObjectProvider<com.rydytrader.autotrader.service.OptionOiTracker> oiTrackerProvider;
    // Tolerate unknown fields on read so a state file written by a different
    // branch (e.g. a future v3 or v1's older shape) doesn't wipe today's
    // in-memory ring on boot. Without this guard Jackson throws
    // UnrecognizedPropertyException, loadFromDisk falls into the catch block,
    // and we silently lose today's recentEvents + todayClosedTrades.
    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private volatile State state = new State();
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();

    private final com.rydytrader.autotrader.service.FuturesSymbolResolver futuresSymbolResolver;

    public Camarilla(CamarillaService camarillaService,
                     CandleAggregator candleAggregator,
                     AtmTracker atmTracker,
                     BalancedAtmSelector atmSelector,
                     MarketDataService marketDataService,
                     OrderService orderService,
                     EventService eventService,
                     RiskSettingsStore riskSettings,
                     com.rydytrader.autotrader.service.FuturesSymbolResolver futuresSymbolResolver,
                     ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                     ObjectProvider<CamarillaStreamBroker> streamBrokerProvider,
                     ObjectProvider<com.rydytrader.autotrader.service.OptionOiTracker> oiTrackerProvider) {
        this.camarillaService     = camarillaService;
        this.candleAggregator     = candleAggregator;
        this.atmTracker           = atmTracker;
        this.atmSelector          = atmSelector;
        this.marketDataService    = marketDataService;
        this.orderService         = orderService;
        this.eventService         = eventService;
        this.riskSettings         = riskSettings;
        this.futuresSymbolResolver = futuresSymbolResolver;
        this.tradeRepoProvider    = tradeRepoProvider;
        this.streamBrokerProvider = streamBrokerProvider;
        this.oiTrackerProvider    = oiTrackerProvider;
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
        // Defensive backfill: if state.json carries entries from a session whose DB rows
        // pre-date the symbol/setup columns, copy that data into the DB rows BEFORE the
        // rollover wipes the in-memory ring. No-op when DB rows already have the data.
        backfillLegacyDbRowsFromState();
        rolloverIfNewDay();
        // Independently drop any recentEvents from before today's 00:00 IST. Day rollover
        // alone isn't sufficient — a partial earlier reset (clears trades but not events)
        // can leave state.dayKey == today with stale event timestamps, so {@code
        // rolloverIfNewDay()} short-circuits and yesterday's events bleed into today's
        // Positions Event Log. This timestamp filter always runs on boot.
        pruneStaleEventsBeforeToday();
        // v2: re-subscribe the futures trigger feed restored from disk. The trigger
        // symbol is the same across all positions (we always trade the near-month
        // future), so a single subscribe is enough.
        if (state.futuresSymbol != null && !state.futuresSymbol.isBlank()) {
            String fut = state.futuresSymbol;
            candleAggregator.subscribe(fut, c -> onCandleClose(fut, c));
            try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(fut)); }
            catch (Exception ignored) {}
            log.info("[Camarilla] restored futures subscription on boot: {}", fut);
        }
        atmTracker.setListener(this::onAtmChange);
        log.info("[Camarilla] booted — enabled={}, lots={}, squareoff={}, restoredPositions={}",
            riskSettings.isCamarillaEnabled(), riskSettings.getCamarillaLotsPerLeg(),
            riskSettings.getCamarillaSquareOffTime(), state.openPositions.size());
    }

    /** Drop any event whose timestamp is before today's IST midnight. Called on boot to
     *  catch state files that have today's dayKey but stale event timestamps from a partial
     *  earlier reset. */
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
            log.info("[Camarilla] pruned {} stale event(s) from before today's 00:00 IST", removed);
            saveToDisk();
            publishStream();
        }
    }

    /** Walk through {@code state.todayClosedTrades} (just loaded from disk) and patch any
     *  DB rows whose {@code symbol} or {@code setup} column is null but whose
     *  {@code closedAtMillis} matches an in-memory entry within ±5 s. Runs once at boot,
     *  before {@link #rolloverIfNewDay()} which would otherwise clear the ring. */
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
                log.info("[Camarilla] backfilled symbol/setup on {} legacy DB row(s) for {}",
                    patched, state.dayKey);
            }
        } catch (Exception e) {
            log.warn("[Camarilla] backfill failed: {}", e.getMessage());
        }
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

    /** Maintenance action — wipes today's closed-trade records WITHOUT touching open
     *  positions. Clears the in-memory cycle ring, today's event entries, the day's
     *  trade-count + consecutive-loss counters, and the DB rows where
     *  {@code session_date = today} (both ALGO and MANUAL strategyIds).
     *
     *  <p>Open positions are deliberately left running — they're still live at the broker;
     *  the bot needs to keep managing their SL / squareoff. Use this only to reset
     *  reporting/analytics state, typically after a test session before going live. */
    public synchronized Map<String, Object> clearTodayRecords() {
        int cyclesCleared = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();

        int prevTradesToday      = state.tradesToday;
        int prevConsecutiveLoss  = state.consecutiveLosses;
        state.tradesToday        = 0;
        state.consecutiveLosses  = 0;

        // Drop any event whose timestamp is today's IST midnight or later.
        long startOfTodayMillis = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
        int eventsBefore = state.recentEvents.size();
        state.recentEvents.removeIf(e -> {
            Object ts = e.get("ts");
            return ts instanceof Number && ((Number) ts).longValue() >= startOfTodayMillis;
        });
        int eventsCleared = eventsBefore - state.recentEvents.size();

        saveToDisk();

        // DB wipe — both ALGO ("camarilla") and MANUAL ("manual") rows for today vanish
        // because deleteBySessionDate doesn't filter on strategyId.
        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) {
                dbCleared = repo.deleteBySessionDate(LocalDate.now(IST).toString());
            }
        } catch (Exception e) {
            log.warn("[Camarilla] clearTodayRecords DB wipe failed: {}", e.getMessage());
        }

        // Record a single event marking the wipe so the operator has an audit trail.
        event("[WARNING]", "Maintenance",
            "Cleared today's records — cycles=" + cyclesCleared
            + " events=" + eventsCleared
            + " dbRows=" + dbCleared
            + " (open positions preserved)");
        log.warn("[Camarilla] clearTodayRecords — cycles={} events={} dbRows={} prevTradesToday={} prevConsLoss={}",
            cyclesCleared, eventsCleared, dbCleared, prevTradesToday, prevConsecutiveLoss);
        publishStream();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cyclesCleared", cyclesCleared);
        out.put("eventsCleared", eventsCleared);
        out.put("dbCleared",     dbCleared);
        return out;
    }

    /** Per-row manual squareoff. Closes only the supplied {@code symbol}, leaves the rest
     *  of the open-positions map untouched. No-op (returns {@code false}) when the symbol
     *  isn't currently open. */
    public boolean forceCloseSymbol(String symbol, String reason) {
        if (symbol == null || symbol.isBlank()) return false;
        synchronized (this) {
            if (!state.openPositions.containsKey(symbol)) return false;
            return closePosition(symbol, reason == null ? "MANUAL" : reason);
        }
    }

    /** Snapshot of all open positions whose {@code setup == MANUAL}. Returned as a fresh
     *  list so the caller can iterate without holding the strategy lock. Used by the
     *  Options Scalper Terminal dashboard endpoint to render only its own positions. */
    public java.util.List<Position> openManualPositions() {
        synchronized (this) {
            java.util.List<Position> out = new ArrayList<>();
            for (Position p : state.openPositions.values()) {
                if (p != null && p.setup == ActiveSetup.MANUAL) out.add(p);
            }
            return out;
        }
    }

    /** Projected charges for currently-open MANUAL positions if the operator squared
     *  off right now at the live LTP. Computed with the same brokerage / STT / GST /
     *  SEBI / stamp formula as a real close cycle, so the Options Terminal header can
     *  display a live cost-to-exit estimate while trades are still open (instead of
     *  showing ₹0 until the first close lands). Skips positions where the entry price
     *  or LTP isn't yet available — a fresh entry whose fill hasn't resolved still
     *  shows ₹0 until both prices are known. */
    public synchronized double projectedManualChargesForOpen() {
        double total = 0;
        for (Position p : state.openPositions.values()) {
            if (p == null || p.setup != ActiveSetup.MANUAL) continue;
            if (p.entryPrice <= 0 || p.qty <= 0) continue;
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); }
            catch (Exception ignored) {}
            if (ltp <= 0) continue;
            double sellTurnover = (p.isShort ? p.entryPrice : ltp) * p.qty;
            double buyTurnover  = (p.isShort ? ltp : p.entryPrice) * p.qty;
            total += perCycleCharges(sellTurnover, buyTurnover);
        }
        return total;
    }

    /** Today's closed-trade ring filtered to {@code setup == MANUAL}. Returns deep copies
     *  of the ring entries so mutations on the caller side don't affect strategy state. */
    public java.util.List<Map<String, Object>> todayManualClosedTrades() {
        synchronized (this) {
            java.util.List<Map<String, Object>> out = new ArrayList<>();
            for (Map<String, Object> row : state.todayClosedTrades) {
                Object s = row == null ? null : row.get("setup");
                if (s != null && ActiveSetup.MANUAL.name().equals(s.toString())) {
                    out.add(new LinkedHashMap<>(row));
                }
            }
            return out;
        }
    }

    /** Look up an open MANUAL position by its current {@code entryOrderId}. Returns
     *  {@code null} when no match exists. Used by the Manual Terminal qty / SL adjust
     *  endpoints which receive the order ID as the per-row handle and need to read
     *  the full {@link Position} to derive the existing direction and SL distance. */
    public Position findOpenManualByOrderId(String orderId) {
        if (orderId == null || orderId.isBlank()) return null;
        synchronized (this) {
            for (Position p : state.openPositions.values()) {
                if (p != null && p.setup == ActiveSetup.MANUAL
                    && orderId.equals(p.entryOrderId)) {
                    return p;
                }
            }
            return null;
        }
    }

    /** Close the MANUAL position whose Fyers {@code entryOrderId} matches. Returns the
     *  symbol that was closed, or {@code null} when no matching open MANUAL position
     *  exists. The Options Scalper Terminal's per-position × button uses the order ID
     *  as its handle (the original Manual Terminal did the same), so this is the lookup
     *  helper that bridges to the strategy's symbol-keyed close path. */
    public String closeManualByOrderId(String orderId, String reason) {
        if (orderId == null || orderId.isBlank()) return null;
        synchronized (this) {
            String found = null;
            for (Position p : state.openPositions.values()) {
                if (p != null && p.setup == ActiveSetup.MANUAL
                    && orderId.equals(p.entryOrderId)) {
                    found = p.symbol;
                    break;
                }
            }
            if (found == null) return null;
            return closePosition(found, reason == null ? "MANUAL_CLOSE" : reason) ? found : null;
        }
    }

    /** Close every open MANUAL position. Strategy positions (L4_BREAKDOWN, H4_BREAKOUT,
     *  etc.) are left untouched. Returns the count of positions closed. */
    public int closeAllManual(String reason) {
        synchronized (this) {
            int closed = 0;
            for (Position p : new ArrayList<>(openManualPositions())) {
                if (closePosition(p.symbol, reason == null ? "MANUAL_CLOSE" : reason)) closed++;
            }
            return closed;
        }
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

    /** Direction-aware turnover ordering for {@link #perCycleCharges}. STT lands on the
     *  sell side, stamp on the buy side — for SHORT the entry is the sell; for LONG the
     *  entry is the buy. */
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
        watchSquareoff();
        refreshUnresolvedFills();
        // Ensure ATM-around warm-up is at least requested once an ATM is known.
        if (atmTracker.getCurrentAtm() > 0 && camarillaService.snapshot().isEmpty()) {
            camarillaService.warmUpAroundAtm(atmTracker.getCurrentAtm());
        }
    }

    @Override
    public void fastSlCheck() {
        // First: drain any bar-candidate buffers whose grace window has elapsed.
        drainPendingBars();

        // Fast-tick TARGET + SL watcher — fires on the live LTP, not on candle close.
        //   • TARGET: single-tick. As soon as triggerLtp crosses targetLevel, close.
        //   • SL:     confirmed over SL_BREACH_CONFIRM_TICKS consecutive polls (~1.5 s)
        //             to reject single-tick spikes.
        //
        // v2: when p.triggerSymbol is set, target/SL comparisons read the FUTURES LTP
        // (the trigger symbol) instead of the option's own premium. Breakeven also uses
        // the futures move from entryFutures. v1 positions (triggerSymbol blank) keep
        // the legacy option-premium-based behaviour.
        if (state.openPositions.isEmpty()) return;

        // ── v2 risk gate: exposed > maxRisk → force-close everything ──
        double maxRisk = riskSettings.getCamarillaMaxRisk();
        if (maxRisk > 0 && !state.dailyLossLockout) {
            double exposed = exposedRiskNow();
            if (exposed > maxRisk) {
                event("[ERROR]", "Risk", "exposed ₹" + round2(exposed)
                    + " > maxRisk ₹" + round2(maxRisk)
                    + " — force-closing all positions and locking session");
                state.dailyLossLockout = true;
                for (String sym : new ArrayList<>(state.openPositions.keySet())) {
                    closePosition(sym, "RISK_BREACH");
                }
                saveToDisk();
                return;
            }
        }

        for (String symbol : new ArrayList<>(state.openPositions.keySet())) {
            Position p = state.openPositions.get(symbol);
            if (p == null) continue;

            // Resolve the LTP that drives target/SL decisions for THIS position.
            // v2 = futures LTP, v1 = option premium LTP.
            boolean v2 = p.triggerSymbol != null && !p.triggerSymbol.isBlank()
                       && !Double.isNaN(p.targetFutures) && !Double.isNaN(p.slFutures);
            String triggerSrc = v2 ? p.triggerSymbol : symbol;
            double triggerLtp;
            try { triggerLtp = marketDataService.getLtp(triggerSrc); }
            catch (Exception e) { continue; }
            if (triggerLtp <= 0) continue;

            // BREAKEVEN trigger.
            // v2: 1R favorable on futures → slFutures = entryFutures (and mirror to slLevel
            //     for downstream display + risk math).
            // v1: 1R favorable on option premium → slLevel = entryPrice (legacy behaviour).
            if (riskSettings.isMoveSlToBreakevenEnabled() && !p.breakevenMoved && p.setup != ActiveSetup.MANUAL) {
                double refSl, refEntry, favorMove, r;
                if (v2) {
                    refSl    = p.slFutures;
                    refEntry = p.entryFutures;
                    r        = Math.abs(refSl - refEntry);
                    favorMove = p.isShort ? (refEntry - triggerLtp) : (triggerLtp - refEntry);
                } else {
                    refSl    = p.originalSlLevel > 0 ? p.originalSlLevel : p.slLevel;
                    refEntry = p.entryPrice;
                    r        = Math.abs(refSl - refEntry);
                    favorMove = p.isShort ? (refEntry - triggerLtp) : (triggerLtp - refEntry);
                }
                if (r > 0 && favorMove >= r) {
                    Object beLock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
                    synchronized (beLock) {
                        Position p2 = state.openPositions.get(symbol);
                        if (p2 != null && !p2.breakevenMoved) {
                            double oldSl = v2 ? p2.slFutures : p2.slLevel;
                            if (v2) {
                                p2.slFutures = p2.entryFutures;
                                p2.slLevel   = p2.entryFutures;   // mirror for display + exposed-risk math
                            } else {
                                p2.slLevel = p2.entryPrice;
                            }
                            p2.breakevenMoved = true;
                            p2.slBreachStreak = 0;
                            event("[INFO]", "AUTO SL", symbol + " " + p2.setup
                                + " SL → breakeven — trigger=" + round2(triggerLtp)
                                + (v2 ? " entryFut=" : " entry=") + round2(refEntry)
                                + " R=" + round2(r)
                                + " (was " + round2(oldSl) + ")");
                            saveToDisk();
                        }
                    }
                }
            }

            // Direction-aware comparisons. SHORT: target BELOW, SL ABOVE. LONG: flipped.
            double targetRef = v2 ? p.targetFutures : p.targetLevel;
            double slRef     = v2 ? p.slFutures     : p.slLevel;
            boolean targetHit = p.isShort ? (triggerLtp <= targetRef) : (triggerLtp >= targetRef);
            boolean slBreach  = p.isShort ? (triggerLtp >= slRef)     : (triggerLtp <= slRef);

            if (targetHit) {
                Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
                synchronized (lock) {
                    Position p2 = state.openPositions.get(symbol);
                    if (p2 == null) continue;
                    double tgt2 = v2 ? p2.targetFutures : p2.targetLevel;
                    boolean stillHit = p2.isShort ? (triggerLtp <= tgt2) : (triggerLtp >= tgt2);
                    if (!stillHit) continue;
                    String cmp = p2.isShort ? " <= target=" : " >= target=";
                    event("[SUCCESS]", "Exit", symbol + " " + p2.setup + " TARGET_HIT — "
                        + (v2 ? "fut=" : "ltp=") + round2(triggerLtp) + cmp + round2(tgt2));
                    closePosition(symbol, "TARGET_HIT");
                }
                continue;
            }

            if (slBreach) {
                p.slBreachStreak++;
                if (p.slBreachStreak >= SL_BREACH_CONFIRM_TICKS) {
                    Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
                    synchronized (lock) {
                        Position p2 = state.openPositions.get(symbol);
                        if (p2 == null) continue;
                        String cmp = p2.isShort ? " >= SL=" : " <= SL=";
                        event("[WARNING]", "Exit", symbol + " " + p2.setup + " SL_HIT — "
                            + (v2 ? "fut=" : "ltp=") + round2(triggerLtp) + cmp + round2(slRef)
                            + " confirmed over " + SL_BREACH_CONFIRM_TICKS + " ticks");
                        closePosition(symbol, "SL_HIT");
                    }
                }
            } else if (p.slBreachStreak > 0) {
                p.slBreachStreak = 0;
            }
        }
    }

    // ── V2 bar-candidate drain + tiebreaker ─────────────────────────────────

    /** Process any bar whose grace window has elapsed. For each bar, group candidates by side
     *  (CE/PE) and setup, apply the L4 tiebreaker (ATM_L4 preferred over ITM_L4), and fire the
     *  selected candidate(s). Fired bar entries are removed from the buffer. */
    private void drainPendingBars() {
        if (pendingByBar.isEmpty()) return;
        long now = System.currentTimeMillis();
        // Iterate via snapshot to allow safe removal.
        for (Long barStart : new ArrayList<>(pendingByBar.keySet())) {
            long barCloseDeadline = barStart + BAR_LENGTH_MS + BAR_PROCESSING_GRACE_MS;
            if (now < barCloseDeadline) continue;
            List<EntryCandidate> bar = pendingByBar.remove(barStart);
            if (bar == null || bar.isEmpty()) continue;
            processBar(bar);
        }
    }

    private void processBar(List<EntryCandidate> bar) {
        // v2: detectFuturesSetup returns at most one candidate per bar, so the buffer
        // typically has a single entry. Walk it and fire the first eligible candidate —
        // no per-side / per-setup dispatch needed (the four setups are mutually
        // exclusive on the futures bar by construction).
        for (EntryCandidate cand : bar) {
            fireIfPresent(cand);
        }
    }

    private void fireIfPresent(EntryCandidate cand) {
        if (cand == null) return;
        // Lock on the FUTURES trigger symbol — the position itself will be keyed on the
        // option leg, but the lock prevents two same-bar candidates from racing.
        Object lock = symbolLocks.computeIfAbsent(cand.symbol(), k -> new Object());
        synchronized (lock) {
            if (!canFireNewEntry()) return;
            fire(cand.symbol(), cand.setup(), cand.targetLevel(), cand.slLevel(), cand.candle());
        }
    }

    // ── ATM change handler — session-open watchlist setup ─────────────────────
    // With drift checks removed, this fires exactly once per session — on the
    // open-price bootstrap from AtmTracker. The heartbeat path (oldAtm == newAtm)
    // is gone with the drift loop.

    public synchronized void onAtmChange(AtmTracker.AtmChange ev) {
        long atm = ev.newAtm();
        if (atm <= 0) return;

        // v2 watchlist —
        //   1. NIFTY near-month FUTURE: subscribed for the 5-min candle trigger feed.
        //   2. ATM−50 PUT + ATM+50 CALL: subscribed for live LTPs so the trade legs
        //      are quote-ready the instant a setup fires. We don't aggregate candles
        //      on them — they're trade targets only.
        // The previous v1 per-strike ATM CE/PE watchlist is fully retired.
        String futuresSym = futuresSymbolResolver.nearMonthNiftyFuture();
        state.futuresSymbol = futuresSym;
        candleAggregator.subscribe(futuresSym, c -> onCandleClose(futuresSym, c));

        Map<String, WatchRole> newRoles = new LinkedHashMap<>();
        newRoles.put(futuresSym, WatchRole.ATM_L4);   // re-uses existing enum value, semantics changed

        // Pre-subscribe the two OTM trade legs so live LTPs are warm before fire().
        BalancedAtmSelector.StrikeAtLevel putRow  = atmSelector.resolveStrikeAtLevel(atm - STRIKE_STEP);
        BalancedAtmSelector.StrikeAtLevel callRow = atmSelector.resolveStrikeAtLevel(atm + STRIKE_STEP);
        java.util.List<String> tradeLegs = new java.util.ArrayList<>();
        if (putRow  != null && putRow.peSymbol()  != null && !putRow.peSymbol().isBlank())  tradeLegs.add(putRow.peSymbol());
        if (callRow != null && callRow.ceSymbol() != null && !callRow.ceSymbol().isBlank()) tradeLegs.add(callRow.ceSymbol());
        if (!tradeLegs.isEmpty()) {
            try { marketDataService.subscribeAdditional(tradeLegs); }
            catch (Exception ignored) {}
        }
        try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(futuresSym)); }
        catch (Exception ignored) {}

        // Retire any contract from a previous v1 watchlist that's not in the new one
        // AND has no open position parked on it.
        java.util.Set<String> toRetire = new HashSet<>(state.symbolRole.keySet());
        toRetire.removeAll(newRoles.keySet());
        for (String oldSym : toRetire) {
            if (state.openPositions.containsKey(oldSym)) continue;
            candleAggregator.unsubscribe(oldSym);
        }
        state.symbolRole.clear();
        state.symbolRole.putAll(newRoles);

        // Fetch Camarilla levels for the futures symbol itself (its own prior-day OHLC).
        // getLevels triggers an async refresh on cache-miss; the first call returns null,
        // but the detector tolerates that (skips bar until levels arrive).
        camarillaService.getLevels(futuresSym);

        String tag = ev.oldAtm() < 0 ? "boot" : String.valueOf(ev.oldAtm());
        event("[INFO]", "ATM", "ATM " + tag + " → " + atm + " | future=" + futuresSym
            + " | PUT leg=" + (tradeLegs.isEmpty() ? "—" : tradeLegs.get(0))
            + " | CALL leg=" + (tradeLegs.size() > 1 ? tradeLegs.get(1) : "—"));
        saveToDisk();
    }

    // ── Candle close handler — entries + exits, per symbol ──────────────────

    public void onCandleClose(String symbol, Candle c) {
        if (!isEnabled()) return;
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            rolloverIfNewDay();
            if (state.doneForDay) return;

            // v2: bar arrives from the NIFTY near-month futures subscription. Entry detection
            // ignores any other symbol — option-leg subscriptions only feed live LTP, not bars.
            String futuresSym = state.futuresSymbol;
            if (futuresSym == null || futuresSym.isBlank()) return;
            if (!futuresSym.equals(symbol)) return;

            // Session lockout from the v2 risk gate — once consumed or exposed risk crosses
            // camarillaMaxRisk, no new entries fire for the rest of the day.
            if (state.dailyLossLockout) return;

            // ── Entry check ──
            if (!canFireNewEntry()) return;
            CamarillaLevels lv = camarillaService.getLevels(futuresSym);
            if (lv == null) {
                // Warm-up in progress — skip; we'll try again next bar.
                return;
            }

            // VWAP for the futures contract. Same Fyers atp source as v1 used for
            // options. When the operator turns the VWAP filter off in settings, we
            // skip the "must have a full-mode tick" gate and pass vwap=0 through so
            // the detector treats setups as direction-agnostic.
            double vwap = 0;
            try { vwap = marketDataService.getVwap(futuresSym); }
            catch (Exception ignored) {}
            boolean vwapFilterOn = riskSettings.isCamarillaVwapFilterEnabled();
            if (vwapFilterOn && vwap <= 0) return;   // fail-safe — pre-tick gap

            EntryCandidate candidate = detectFuturesSetup(futuresSym, c, lv, vwap, vwapFilterOn);
            if (candidate == null) return;

            // Buffer the candidate — fast scheduler drains after BAR_PROCESSING_GRACE_MS.
            pendingByBar
                .computeIfAbsent(c.startMillis(), k -> new CopyOnWriteArrayList<>())
                .add(candidate);
        }
    }

    /** v2 — Four-setup detector on a single NIFTY-futures bar close.
     *
     *  <p>Reversal precedence over breakout: a green bar that wicks below L3 AND
     *  closes above H4 in the same bar will be tagged L3_REVERSAL (the rarer
     *  structural signal). Same for the mirror case on the bearish side.
     *
     *  <p>Returns null when no setup matches OR the VWAP direction gate fails.
     *  {@code targetLevel} and {@code slLevel} on the returned candidate are
     *  FUTURES PRICES — fire() copies them to the Position's
     *  {@code targetFutures} / {@code slFutures} fields. */
    private EntryCandidate detectFuturesSetup(String futuresSym, Candle c,
                                              CamarillaLevels lv, double vwap,
                                              boolean vwapFilterOn) {
        if (lv == null) return null;
        WatchRole role = state.symbolRole.get(futuresSym);
        if (role == null) role = WatchRole.ATM_L4;   // defensive — futures is always the trigger

        // VWAP direction gates. When the operator turned the filter off, treat each
        // half as unconditionally allowed — geometry alone decides which setup fires.
        boolean bullishOk = !vwapFilterOn || (vwap > 0 && c.close() > vwap);
        boolean bearishOk = !vwapFilterOn || (vwap > 0 && c.close() < vwap);

        if (bullishOk) {
            // L3_REVERSAL: green bar wicks below L3, closes back above.
            if (c.isGreen() && c.low() <= lv.l3() && c.close() > lv.l3()) {
                return new EntryCandidate(futuresSym, role, ActiveSetup.L3_REVERSAL,
                    /*target*/ lv.h3(),
                    /*sl*/     lv.l4() - 3 * TICK_FUTURES,
                    c);
            }
            // H4_BREAKOUT: futures closes above H4.
            if (c.close() > lv.h4()) {
                return new EntryCandidate(futuresSym, role, ActiveSetup.H4_BREAKOUT,
                    /*target*/ lv.h5(),
                    /*sl*/     lv.h3() - 3 * TICK_FUTURES,
                    c);
            }
        }

        if (bearishOk) {
            // H3_REVERSAL: red bar wicks above H3, closes back below.
            if (c.isRed() && c.high() >= lv.h3() && c.close() < lv.h3()) {
                return new EntryCandidate(futuresSym, role, ActiveSetup.H3_REVERSAL,
                    /*target*/ lv.l3(),
                    /*sl*/     lv.h4() + 3 * TICK_FUTURES,
                    c);
            }
            // L4_BREAKDOWN: futures closes below L4.
            if (c.close() < lv.l4()) {
                return new EntryCandidate(futuresSym, role, ActiveSetup.L4_BREAKDOWN,
                    /*target*/ lv.l5(),
                    /*sl*/     lv.l3() + 3 * TICK_FUTURES,
                    c);
            }
        }
        return null;
    }

    private boolean canFireNewEntry() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        // Trading-start-time gate: new entries only after this IST clock time.
        String startHhmm = riskSettings.getCamarillaTradingStartTime();
        if (startHhmm != null && !startHhmm.isBlank()) {
            try {
                LocalTime start = LocalTime.parse(startHhmm);
                if (now.isBefore(start)) return false;
            } catch (Exception ignored) {}
        }
        // Trading-end-time gate: no new entries after this IST clock time. Existing positions
        // keep being managed (target/SL/squareoff continue).
        String endHhmm = riskSettings.getCamarillaTradingEndTime();
        if (endHhmm != null && !endHhmm.isBlank()) {
            try {
                LocalTime end = LocalTime.parse(endHhmm);
                if (!now.isBefore(end)) return false;
            } catch (Exception ignored) {}
        }
        // Note: count-based "max concurrent positions" gate was removed in favor of the
        // risk-budget gate in fire() — sizing decisions there reflect actual ₹ at risk, not
        // a raw position count. The camarillaMaxConcurrentPositions setting is retained for
        // saved-JSON compat but no longer consulted at runtime.
        return true;
    }

    /** Sum of remaining ₹ at risk across all currently-open positions. For SHORTs that's
     *  {@code max(0, slLevel − entryPrice) × qty}; for LONGs it's
     *  {@code max(0, entryPrice − slLevel) × qty}. Once breakeven moves slLevel to entry,
     *  the per-position contribution drops to 0 either direction. Single source of truth —
     *  used by both the dashboard badge AND the budget gate at entry time. */
    private double exposedRiskNow() {
        double total = 0;
        for (Position p : state.openPositions.values()) {
            // v2 positions: futures-distance proxy (entryFutures vs slFutures).
            // v1 positions: option-price distance (entryPrice vs slLevel).
            boolean v2 = p.triggerSymbol != null && !p.triggerSymbol.isBlank()
                       && p.entryFutures > 0 && !Double.isNaN(p.slFutures);
            double perShare;
            if (v2) {
                perShare = Math.abs(p.slFutures - p.entryFutures);
            } else {
                perShare = p.isShort
                    ? Math.max(0, p.slLevel - p.entryPrice)
                    : Math.max(0, p.entryPrice - p.slLevel);
            }
            total += perShare * p.qty;
        }
        return total;
    }

    /** Sum of realized losses (absolute value) across today's closed-trade ring. Profitable
     *  cycles contribute 0 — only losses consume budget. */
    private double consumedRiskNow() {
        double total = 0;
        for (Map<String, Object> trade : state.todayClosedTrades) {
            double net = asDouble(trade.get("netPnl"));
            if (net < 0) total += Math.abs(net);
        }
        return total;
    }

    /** v2 — Fire a new SHORT option entry triggered by a futures bar setup.
     *
     *  @param triggerSymbol the NIFTY future the trigger fired on (also the SL/target reference)
     *  @param setup         which of the four v2 setups fired
     *  @param targetFutures futures price for the target trigger
     *  @param slFutures     futures price for the SL trigger
     *  @param entryCandle   the futures bar that fired the setup (used for entry futures price)
     */
    private void fire(String triggerSymbol, ActiveSetup setup,
                      double targetFutures, double slFutures, Candle entryCandle) {
        boolean shortSetup = isShortSetup(setup);
        if (!shortSetup) return;   // v2 is sell-only by design
        boolean bullishBet = isBullishBet(setup);

        // ── Pick the OTM option leg ──
        // bullish bet  → sell ATM−50 PUT
        // bearish bet  → sell ATM+50 CALL
        long atm = atmTracker.getCurrentAtm();
        if (atm <= 0) {
            event("[ERROR]", "AUTO ENTRY", setup + " — ATM not yet locked, skipping");
            return;
        }
        long strike = bullishBet ? (atm - STRIKE_STEP) : (atm + STRIKE_STEP);
        BalancedAtmSelector.StrikeAtLevel row = atmSelector.resolveStrikeAtLevel(strike);
        String optionSym = null;
        if (row != null) {
            optionSym = bullishBet ? row.peSymbol() : row.ceSymbol();
        }
        if (optionSym == null || optionSym.isBlank()) {
            event("[ERROR]", "AUTO ENTRY",
                setup + " — couldn't resolve " + (bullishBet ? "PUT" : "CALL")
                + " symbol at strike " + strike);
            return;
        }
        // Avoid double-entry on the same option leg if a prior trade is still open on it.
        if (state.openPositions.containsKey(optionSym)) return;

        // ── Risk gates: consumed > maxRisk locks out the day ──
        double maxRisk = riskSettings.getCamarillaMaxRisk();
        if (maxRisk > 0 && consumedRiskNow() > maxRisk) {
            event("[ERROR]", "Risk", setup + " skipped — consumed ₹"
                + round2(consumedRiskNow()) + " > maxRisk ₹" + round2(maxRisk)
                + ", locking session");
            state.dailyLossLockout = true;
            saveToDisk();
            return;
        }

        // ── Futures-price-based R:R floor (toggle) ──
        // For futures-driven entries the candle close approximates the entry futures price.
        // reward = |entryFut − targetFut|, risk = |slFut − entryFut|.
        double entryFutures = entryCandle.close();
        try {
            double live = marketDataService.getLtp(triggerSymbol);
            if (live > 0) entryFutures = live;
        } catch (Exception ignored) {}
        if (riskSettings.isCamarillaMinRRCheckEnabled()) {
            double reward = Math.abs(entryFutures - targetFutures);
            double risk   = Math.abs(slFutures - entryFutures);
            if (risk > 0 && reward < risk) {
                event("[WARNING]", "Sizing", setup + " skipped — R:R "
                    + round2(reward / risk) + " < 1.0"
                    + " (reward " + round2(reward) + " < risk " + round2(risk)
                    + ", entryFut " + round2(entryFutures)
                    + ", target " + round2(targetFutures)
                    + ", SL " + round2(slFutures) + ")");
                return;
            }
        }

        // ── Project exposed-risk after this entry; block if it would exceed maxRisk ──
        // Per-position futures-equivalent risk = |entryFut − slFut| × qty.
        int qty = riskSettings.getCamarillaLotsPerLeg() * LOT_SIZE;
        double newExposureDelta = Math.abs(entryFutures - slFutures) * qty;
        if (maxRisk > 0 && (exposedRiskNow() + newExposureDelta) > maxRisk) {
            event("[WARNING]", "Risk", setup + " skipped — projected exposed ₹"
                + round2(exposedRiskNow() + newExposureDelta) + " > maxRisk ₹"
                + round2(maxRisk));
            return;
        }

        // ── Place the SELL (SHORT) order on the option leg ──
        String productType = riskSettings.getCamarillaOrderType();
        int orderSide = -1;
        double optionEntryLtp = 0;
        try { optionEntryLtp = marketDataService.getLtp(optionSym); }
        catch (Exception ignored) {}
        // optionEntryLtp may be 0 if the option hasn't ticked yet — leave it 0, the fill
        // resolver will overwrite with the real broker fill price.

        log.info("[Camarilla v2] {} fired — sell {} qty={} (triggerFut={}, entryFut={}, target={}, sl={})",
            setup, optionSym, qty, triggerSymbol, entryFutures, targetFutures, slFutures);
        event("[INFO]", "AUTO ENTRY", setup + " — sell " + optionSym + " qty " + qty
            + " (futEntry≈" + round2(entryFutures) + ", target=" + round2(targetFutures)
            + ", SL=" + round2(slFutures) + ")");

        OrderDTO order = orderService.placeOrder(optionSym, qty, orderSide, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            log.warn("[Camarilla v2] entry order rejected for {} — staying idle", optionSym);
            event("[ERROR]", "AUTO ENTRY", "entry order rejected for " + optionSym);
            return;
        }
        try { marketDataService.subscribeAdditional(Collections.singletonList(optionSym)); }
        catch (Exception ignored) {}

        Position p = new Position();
        p.symbol        = optionSym;          // the leg being traded
        p.triggerSymbol = triggerSymbol;      // futures — fastSlCheck reads its LTP
        p.setup         = setup;
        p.qty           = qty;
        p.entryPrice    = optionEntryLtp;     // option premium estimate; fill resolver overwrites
        p.entryFutures  = entryFutures;       // futures price at entry
        p.fillResolved  = false;
        p.entryOrderId  = order.getId();
        p.openMillis    = System.currentTimeMillis();
        p.targetFutures = targetFutures;
        p.slFutures     = slFutures;
        // Legacy targetLevel / slLevel kept = futures values too, so dashboard JSON
        // continues to show the right numbers and v1-shaped consumers don't NPE.
        p.targetLevel   = targetFutures;
        p.slLevel       = slFutures;
        p.originalSlLevel = slFutures;
        p.breakevenMoved  = false;
        p.isShort         = true;
        p.productType     = productType;
        try {
            com.rydytrader.autotrader.service.OptionOiTracker oiAtEntry = oiTrackerProvider == null ? null
                : oiTrackerProvider.getIfAvailable();
            if (oiAtEntry != null) {
                String b = oiAtEntry.snapshot().bias();
                p.entryOiBias = b == null ? "" : b;
            }
        } catch (Exception ignored) {}
        state.openPositions.put(optionSym, p);
        state.tradesToday++;
        // Re-subscribe candle listener on the option symbol too — needed so the existing
        // candle-close skip-if-open path correctly short-circuits if any leftover code
        // routes option candles through onCandleClose.
        final String optSym = optionSym;
        candleAggregator.subscribe(optSym, c -> onCandleClose(optSym, c));
        saveToDisk();
    }

    /** Result of a manual order placement via the Options Scalper Terminal. */
    public record ManualPlaceResult(boolean ok, String orderId, String message) {
        public static ManualPlaceResult ok(String orderId)                       { return new ManualPlaceResult(true,  orderId, "placed"); }
        public static ManualPlaceResult ok(String orderId, String message)       { return new ManualPlaceResult(true,  orderId, message); }
        public static ManualPlaceResult err(String reason)                       { return new ManualPlaceResult(false, null,    reason); }
    }

    /**
     * Place a single manual option order via the Options Scalper Terminal. Bypasses the
     * setup-specific gates (OI bias, candle pattern, L4/H4 level math) and risk-budget
     * sizing — the operator picked everything. Still respects: global kill switch,
     * duplicate-symbol guard, and basic input validation.
     *
     * <p>Target is unset ({@link Double#NaN}). Stop-loss is configurable in premium
     * points from entry: when {@code stopLossPts > 0}, the fast-tick SL watcher fires
     * a market exit at {@code entry ± stopLossPts} (sign flipped by direction). When
     * {@code stopLossPts == 0}, no auto-SL is attached and the operator exits manually
     * via the per-row squareoff button (which calls {@link #forceCloseSymbol}).
     *
     * @param symbol       Fyers option symbol (e.g. {@code NSE:NIFTY25W13124100CE})
     * @param side         {@code +1} = BUY, {@code -1} = SELL
     * @param qty          contract count (positive)
     * @param orderType    {@code 2} = MARKET, {@code 1} = LIMIT (Fyers code)
     * @param limitPrice   limit price when {@code orderType == 1}; ignored for market orders
     * @param stopLossPts  auto-SL distance in premium points; {@code 0} disables auto-SL
     */
    public ManualPlaceResult placeManual(String symbol, int side, int qty,
                                         int orderType, double limitPrice, double stopLossPts,
                                         String productType) {
        if (symbol == null || symbol.isBlank()) return ManualPlaceResult.err("symbol required");
        if (side != 1 && side != -1)            return ManualPlaceResult.err("side must be +1 or -1");
        if (qty <= 0)                           return ManualPlaceResult.err("qty must be > 0");
        if (orderType != 1 && orderType != 2)   return ManualPlaceResult.err("orderType must be 1 (LMT) or 2 (MKT)");
        if (orderType == 1 && !(limitPrice > 0)) return ManualPlaceResult.err("limitPrice required for LMT");
        if (!(stopLossPts > 0))                  return ManualPlaceResult.err("SL is required and must be greater than 0");
        if (stopLossPts > 50)                    return ManualPlaceResult.err("SL cannot exceed 50 points");
        if (!isEnabled())                       return ManualPlaceResult.err("trading kill switch is OFF");
        // Normalize the product type from the JS dropdown ("OVERNIGHT"/"INTRADAY")
        // into the Fyers token OrderService understands. Blank/unknown values fall
        // back to the strategy default — never raise here, since the controller has
        // already defaulted; this is a belt-and-braces guard.
        String resolvedProductType = (productType == null || productType.isBlank())
            ? riskSettings.getCamarillaOrderType()
            : productType.trim();

        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            // Merge path: when a MANUAL position already exists on this symbol, the second
            // order is either an ADD (same direction) or a REDUCE (opposite direction). The
            // original "reject duplicate" behaviour blocked legitimate scalper workflows;
            // both flows now route through mergeAdd / mergeReduce. Strategy positions
            // (L4_BREAKDOWN etc) don't merge — they're owned by the algo and reject here.
            Position existing = state.openPositions.get(symbol);
            if (existing != null) {
                if (existing.setup != ActiveSetup.MANUAL) {
                    return ManualPlaceResult.err("strategy position open on " + symbol + " — manual merge blocked");
                }
                boolean sameDirection = (side == -1) == existing.isShort;
                if (sameDirection) {
                    return mergeAdd(existing, side, qty, orderType, limitPrice);
                } else {
                    return mergeReduce(existing, qty, orderType, limitPrice);
                }
                // Note: mergeAdd / mergeReduce intentionally reuse the EXISTING position's
                // productType — Fyers nets legs only when the product matches the entry.
                // The dropdown selection on the second order is ignored to avoid an
                // accidental split into two separate positions at Fyers.
            }

            double entryLtp;
            try { entryLtp = marketDataService.getLtp(symbol); }
            catch (Exception e) { entryLtp = 0; }
            // For LMT, use the user's limit price as the entry estimate.
            // For MKT, use the live LTP. If LTP isn't cached yet (rare — symbol not streaming),
            // reject the order. With a mandatory SL we need a real entry reference to derive
            // slLevel before placement; accepting a zero entry would set slLevel to NaN and
            // silently disable the auto-SL the operator explicitly asked for.
            double entryEstimate = orderType == 1 ? limitPrice : entryLtp;
            if (!(entryEstimate > 0)) {
                return ManualPlaceResult.err("symbol LTP unavailable — wait for the feed and retry");
            }

            // ── Stage 1: max-lots hard cap from settings ──
            // camarillaLotsPerLeg is the single source of truth for "lots per trade"
            // (the algo path already derives qty from it). Cap modal-requested lots
            // to this ceiling — independent of budget. Defensive >0 guard treats a
            // zero/unset value as "no cap" (the setter floors at 1 today, but the
            // guard documents intent).
            int maxLots = riskSettings.getCamarillaLotsPerLeg();
            if (maxLots > 0) {
                int requestedLots = qty / LOT_SIZE;
                if (requestedLots > maxLots) {
                    event("[WARNING]", "MANUAL ENTRY",
                        symbol + " capped " + requestedLots + "→" + maxLots
                        + " lots — maxLots setting (camarillaLotsPerLeg=" + maxLots + ")");
                    qty = maxLots * LOT_SIZE;
                }
            }

            // ── Stage 2: portfolio risk-budget shrink (best-fit) ──
            // If the trade's R-at-risk (stopLossPts × qty) would push consumed+exposed
            // risk past the cap, shrink to the largest whole-lot count that fits.
            // Reject only when even 1 lot doesn't fit. Bypassed when maxRisk=0 (not
            // opted in). The shrink output is bounded by Stage 1's cap by construction
            // (shrink only decreases), so the placed qty is always ≤ maxLots.
            double maxRisk = riskSettings.getPortfolioMaxDailyLoss();
            if (maxRisk > 0) {
                double consumed  = consumedRiskNow();
                double exposed   = exposedRiskNow();
                double headroom  = maxRisk - consumed - exposed;
                double tradeRisk = stopLossPts * qty;
                if (tradeRisk > headroom) {
                    long maxAffordableShares = (long) Math.floor(headroom / stopLossPts);
                    int  bestLots            = (int) (maxAffordableShares / LOT_SIZE);
                    if (bestLots < 1) {
                        event("[ERROR]", "MANUAL ENTRY",
                            symbol + " rejected — 1 lot risk ₹"
                            + round2(stopLossPts * LOT_SIZE) + " > headroom ₹"
                            + round2(headroom) + " (consumed=₹" + round2(consumed)
                            + ", exposed=₹" + round2(exposed) + ", max=₹" + round2(maxRisk) + ")");
                        return ManualPlaceResult.err("even 1 lot (₹"
                            + round2(stopLossPts * LOT_SIZE) + " risk) exceeds portfolio headroom ₹"
                            + round2(headroom) + " — raise the daily loss cap or wait for losses to roll off");
                    }
                    int currentLots = qty / LOT_SIZE;
                    if (bestLots < currentLots) {
                        event("[WARNING]", "MANUAL ENTRY",
                            symbol + " downsized " + currentLots + "→" + bestLots
                            + " lots — full risk ₹" + round2(tradeRisk)
                            + " > headroom ₹" + round2(headroom)
                            + ", new risk ₹" + round2(stopLossPts * bestLots * LOT_SIZE));
                        qty = bestLots * LOT_SIZE;
                    }
                }
            }

            String sideWord = side == -1 ? "sell" : "buy";

            // OrderService.placeOrder treats stoploss=0 + orderType=2 as MARKET. For LMT we
            // need to invoke the limit-order path. The product type came from the modal's
            // INTRADAY / OVERNIGHT dropdown (resolved above) and is the same value persisted
            // onto the Position for downstream add/reduce/close to reuse — required for
            // Fyers to net the legs into one position.
            OrderDTO order;
            if (orderType == 2) {
                order = orderService.placeOrder(symbol, qty, side, 0, resolvedProductType);
            } else {
                order = orderService.placeLimitOrder(symbol, qty, side, limitPrice, resolvedProductType);
            }
            if (order == null || order.getId() == null || order.getId().isEmpty()) {
                event("[ERROR]", "MANUAL ENTRY", "order rejected for " + symbol
                    + " (" + sideWord + " qty " + qty + ", type=" + (orderType == 1 ? "LMT" : "MKT") + ")");
                return ManualPlaceResult.err("broker rejected the order");
            }

            try { marketDataService.subscribeAdditional(Collections.singletonList(symbol)); }
            catch (Exception ignored) {}

            // Direction-aware SL price from premium-point distance. SHORT (sell): SL above
            // entry — stop fires when LTP rises stopLossPts ABOVE the entry price. LONG
            // (buy): SL below entry. stopLossPts=0 leaves slLevel as NaN — fast-tick SL
            // comparison against NaN always returns false, so no auto-SL fires.
            double slPrice = Double.NaN;
            if (stopLossPts > 0 && entryEstimate > 0) {
                slPrice = (side == -1)
                    ? entryEstimate + stopLossPts
                    : entryEstimate - stopLossPts;
            }

            Position p = new Position();
            p.symbol         = symbol;
            p.setup          = ActiveSetup.MANUAL;
            p.qty            = qty;
            p.entryPrice     = entryEstimate;       // refreshUnresolvedFills() overwrites with broker fill
            p.fillResolved   = false;
            p.entryOrderId   = order.getId();
            p.openMillis     = System.currentTimeMillis();
            p.targetLevel    = Double.NaN;          // MANUAL has no auto target — operator exits
            p.slLevel        = slPrice;             // NaN when stopLossPts=0 (no auto-SL)
            p.originalSlLevel = slPrice;
            // breakevenMoved stays false. The BE trigger in fastSlCheck explicitly skips
            // MANUAL setup, so leaving this false means the UI's "SL moved to breakeven"
            // ▲ indicator only renders when BE has actually fired (algo trades), not on
            // every manual entry.
            p.breakevenMoved  = false;
            p.isShort         = (side == -1);
            p.productType     = resolvedProductType;   // reused by mergeAdd/mergeReduce/closePosition

            try {
                com.rydytrader.autotrader.service.OptionOiTracker oiAtEntry = oiTrackerProvider == null ? null
                    : oiTrackerProvider.getIfAvailable();
                if (oiAtEntry != null) {
                    String b = oiAtEntry.snapshot().bias();
                    p.entryOiBias = b == null ? "" : b;
                }
            } catch (Exception ignored) {}

            state.openPositions.put(symbol, p);
            state.tradesToday++;
            saveToDisk();

            log.info("[Camarilla] MANUAL placed — {} {} qty={} entry≈{} orderId={}",
                sideWord, symbol, qty, round2(entryEstimate), order.getId());
            event("[SUCCESS]", "MANUAL ENTRY", sideWord + " " + symbol
                + " qty " + qty + " entry≈" + round2(entryEstimate)
                + " (" + (orderType == 1 ? "LMT @ " + round2(limitPrice) : "MKT") + ")");
            return ManualPlaceResult.ok(order.getId());
        }
    }

    /** Same-direction top-up. Places the order, then on successful submission updates
     *  the existing {@link Position} in place — qty incremented, entryPrice recomputed
     *  as a weighted average across the old fill + the new add's estimated fill. SL
     *  level is intentionally unchanged: the operator's existing SL choice carries
     *  forward; the implicit stopLossPts shifts because entryPrice shifts, but the
     *  trigger price the SL watcher reads stays the same. Caller must hold the symbol
     *  lock — this method must be invoked from inside {@link #placeManual}'s
     *  synchronized block. */
    private ManualPlaceResult mergeAdd(Position existing, int side, int addQty,
                                       int orderType, double limitPrice) {
        double entryLtp;
        try { entryLtp = marketDataService.getLtp(existing.symbol); }
        catch (Exception e) { entryLtp = 0; }
        double addEstimate = orderType == 1 ? limitPrice : entryLtp;
        if (!(addEstimate > 0)) {
            return ManualPlaceResult.err("symbol LTP unavailable — wait for the feed and retry");
        }

        // ── Stage 1: total-position max-lots cap ──
        // The cap is on TOTAL lots after the add (existing + add ≤ maxLots), not just
        // the add itself — otherwise an operator could tap +1 lot indefinitely past
        // the ceiling. When the requested add would exceed the total cap, shrink it
        // to whatever capacity remains. If no capacity remains, reject.
        int maxLots = riskSettings.getCamarillaLotsPerLeg();
        if (maxLots > 0) {
            int existingLots     = existing.qty / LOT_SIZE;
            int requestedAddLots = addQty / LOT_SIZE;
            if (existingLots + requestedAddLots > maxLots) {
                int allowedAddLots = Math.max(0, maxLots - existingLots);
                if (allowedAddLots == 0) {
                    event("[ERROR]", "MANUAL ENTRY",
                        existing.symbol + " add rejected — already at maxLots ("
                        + existingLots + "/" + maxLots + ")");
                    return ManualPlaceResult.err("position already at maxLots ("
                        + existingLots + "/" + maxLots + ") — raise the setting or close first");
                }
                event("[WARNING]", "MANUAL ENTRY",
                    existing.symbol + " add capped " + requestedAddLots + "→" + allowedAddLots
                    + " lots — total would exceed maxLots (" + existingLots + " open + "
                    + requestedAddLots + " requested > " + maxLots + ")");
                addQty = allowedAddLots * LOT_SIZE;
            }
        }

        // ── Stage 2: portfolio risk-budget shrink (best-fit) ──
        // Marginal new risk = existing position's SL-distance × addQty (the new shares
        // face the same SL trigger that's already set). exposedRiskNow already counts
        // the existing position's risk, so the headroom comparison is just the delta.
        // Shrink to best-fit instead of outright reject; reject only when even 1 added
        // lot doesn't fit.
        double existingRPerShare = (!Double.isNaN(existing.slLevel) && existing.entryPrice > 0)
            ? Math.abs(existing.slLevel - existing.entryPrice) : 0;
        double maxRisk = riskSettings.getPortfolioMaxDailyLoss();
        if (maxRisk > 0 && existingRPerShare > 0) {
            double consumed = consumedRiskNow();
            double exposed  = exposedRiskNow();
            double headroom = maxRisk - consumed - exposed;
            double addRisk  = existingRPerShare * addQty;
            if (addRisk > headroom) {
                long maxAffordableShares = (long) Math.floor(headroom / existingRPerShare);
                int  bestLots            = (int) (maxAffordableShares / LOT_SIZE);
                if (bestLots < 1) {
                    event("[ERROR]", "MANUAL ENTRY",
                        existing.symbol + " add rejected — 1 lot add-risk ₹"
                        + round2(existingRPerShare * LOT_SIZE) + " > headroom ₹"
                        + round2(headroom) + " (consumed=₹" + round2(consumed)
                        + ", exposed=₹" + round2(exposed) + ", max=₹" + round2(maxRisk) + ")");
                    return ManualPlaceResult.err("even 1 lot add (₹"
                        + round2(existingRPerShare * LOT_SIZE) + " risk) exceeds portfolio headroom ₹"
                        + round2(headroom) + " — raise the daily loss cap or close existing first");
                }
                int currentAddLots = addQty / LOT_SIZE;
                if (bestLots < currentAddLots) {
                    event("[WARNING]", "MANUAL ENTRY",
                        existing.symbol + " add downsized " + currentAddLots + "→" + bestLots
                        + " lots — full add-risk ₹" + round2(addRisk)
                        + " > headroom ₹" + round2(headroom));
                    addQty = bestLots * LOT_SIZE;
                }
            }
        }

        // Reuse the entry's product type so Fyers nets the add-on into the same position.
        // Falls back to the strategy default for legacy state files where productType
        // is blank (predates the field).
        String productType = (existing.productType == null || existing.productType.isBlank())
            ? riskSettings.getCamarillaOrderType()
            : existing.productType;
        OrderDTO order = (orderType == 2)
            ? orderService.placeOrder     (existing.symbol, addQty, side, 0, productType)
            : orderService.placeLimitOrder(existing.symbol, addQty, side, limitPrice, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            event("[ERROR]", "MANUAL ENTRY", "add-on rejected for " + existing.symbol);
            return ManualPlaceResult.err("broker rejected the add-on order");
        }

        int oldQty = existing.qty;
        double oldEntry = existing.entryPrice;
        int    newQty   = oldQty + addQty;
        double newEntry = (oldQty * oldEntry + addQty * addEstimate) / (double) newQty;

        // Capture pre-add state so refreshUnresolvedFills can recompute the weighted
        // average using the real broker fill for the add (instead of overwriting
        // entryPrice with just the new fill — which would erase the older fills'
        // contribution).
        existing.preAddQty     = oldQty;
        existing.preAddEntry   = oldEntry;
        existing.qty           = newQty;
        existing.entryPrice    = newEntry;       // initial weighted-avg estimate
        existing.entryOrderId  = order.getId();
        existing.fillResolved  = false;          // refreshUnresolvedFills will reconcile
        saveToDisk();

        log.info("[Camarilla] MANUAL add — {} qty {}→{} entry {}→{} orderId={}",
            existing.symbol, oldQty, newQty, round2(oldEntry), round2(newEntry), order.getId());
        String msg = "Added " + (addQty / LOT_SIZE) + " lot → " + (newQty / LOT_SIZE)
            + " lots @ avg " + round2(newEntry);
        event("[SUCCESS]", "MANUAL ENTRY", "add " + existing.symbol + " " + msg);
        return ManualPlaceResult.ok(order.getId(), msg);
    }

    /** Opposite-direction reduce. Places the reduce-side order and either closes the
     *  full position (when {@code reduceQty == existing.qty}) or books a partial-reduce
     *  trade row for the closed portion. Rejects reduces that exceed the open qty —
     *  flipping direction requires explicit close + reopen. Caller must hold the symbol
     *  lock. */
    private ManualPlaceResult mergeReduce(Position existing, int reduceQty,
                                          int orderType, double limitPrice) {
        if (reduceQty > existing.qty) {
            return ManualPlaceResult.err("reduce qty exceeds open qty — close + reopen to flip");
        }
        int closeSide = existing.isShort ? +1 : -1;   // SHORT closes via BUY, LONG via SELL
        // Same product as the entry — required for Fyers to net the reduce against
        // the original position rather than open a fresh opposing leg.
        String productType = (existing.productType == null || existing.productType.isBlank())
            ? riskSettings.getCamarillaOrderType()
            : existing.productType;
        OrderDTO order = (orderType == 2)
            ? orderService.placeOrder     (existing.symbol, reduceQty, closeSide, 0, productType)
            : orderService.placeLimitOrder(existing.symbol, reduceQty, closeSide, limitPrice, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            event("[ERROR]", "MANUAL ENTRY", "reduce rejected for " + existing.symbol);
            return ManualPlaceResult.err("broker rejected the reduce order");
        }

        // Full reduce → route through existing closePosition so the trade row, charges,
        // analytics fold-in, and event log all stay consistent with the × button path.
        if (reduceQty == existing.qty) {
            event("[SUCCESS]", "MANUAL ENTRY", "reduce " + existing.symbol
                + " full close (" + (reduceQty / LOT_SIZE) + " lots)");
            closePosition(existing.symbol, "MANUAL_REDUCE");
            return ManualPlaceResult.ok(order.getId(), "Closed " + (reduceQty / LOT_SIZE) + " lots");
        }

        // Partial reduce — book a closed-trade row for the reduced portion, decrement qty,
        // leave entryPrice unchanged.
        double exitPrice = 0;
        try { exitPrice = marketDataService.getLtp(existing.symbol); }
        catch (Exception ignored) {}
        double sellTurnover = (existing.isShort ? existing.entryPrice : exitPrice) * reduceQty;
        double buyTurnover  = (existing.isShort ? exitPrice  : existing.entryPrice) * reduceQty;
        double gross   = existing.isShort
            ? (existing.entryPrice - exitPrice) * reduceQty
            : (exitPrice  - existing.entryPrice) * reduceQty;
        double charges = perCycleCharges(sellTurnover, buyTurnover);
        double net     = gross - charges;
        long closedAtMillis = System.currentTimeMillis();

        persistTradeRow(MANUAL_STRATEGY_ID, existing.symbol, existing.setup.name(), "MANUAL_REDUCE",
            reduceQty, gross, charges, net, 0, closedAtMillis, existing.openMillis, existing.entryOiBias);

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("strategyId",     MANUAL_STRATEGY_ID);
        cycle.put("setup",          existing.setup.name());
        cycle.put("symbol",         existing.symbol);
        cycle.put("side",           existing.isShort ? "SELL" : "BUY");
        cycle.put("qty",            reduceQty);
        cycle.put("entryPrice",     round2(existing.entryPrice));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    "MANUAL_REDUCE");
        cycle.put("closedAtMillis", closedAtMillis);
        cycle.put("openedAtMillis", existing.openMillis);
        cycle.put("entryOiBias",    existing.entryOiBias);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 100) state.todayClosedTrades.remove(0);

        existing.qty -= reduceQty;
        saveToDisk();

        log.info("[Camarilla] MANUAL partial reduce — {} closed {} qty, {} remaining, net={}",
            existing.symbol, reduceQty, existing.qty, round2(net));
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "MANUAL ENTRY",
            "reduce " + existing.symbol + " −" + (reduceQty / LOT_SIZE)
            + " lot → " + (existing.qty / LOT_SIZE) + " lots remaining, net=" + round2(net));
        return ManualPlaceResult.ok(order.getId());
    }

    /** Adjust an open MANUAL position's stop-loss trigger price by {@code deltaPts}
     *  (price-based, not direction-aware). {@code +1} raises slLevel by 1 point;
     *  {@code −1} lowers it. Rejects when no MANUAL position exists on the symbol or
     *  the position has no SL. The fast-tick SL watcher reads the new value on its
     *  next ~500 ms iteration. */
    public ManualPlaceResult adjustManualSl(String symbol, double deltaPts) {
        if (symbol == null || symbol.isBlank()) return ManualPlaceResult.err("symbol required");
        if (deltaPts == 0) return ManualPlaceResult.err("deltaPts cannot be zero");
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            Position p = state.openPositions.get(symbol);
            if (p == null || p.setup != ActiveSetup.MANUAL) {
                return ManualPlaceResult.err("no open MANUAL position on " + symbol);
            }
            if (Double.isNaN(p.slLevel)) {
                return ManualPlaceResult.err("position has no SL to adjust");
            }
            double newSl = p.slLevel + deltaPts;
            if (newSl <= 0) {
                return ManualPlaceResult.err("SL would go to zero or below");
            }
            double oldSl = p.slLevel;
            p.slLevel = round2(newSl);   // 2-dp round matches the price grid display
            saveToDisk();
            event("[INFO]", "MANUAL SL", symbol + " SL " + round2(oldSl) + " → "
                + round2(p.slLevel) + " (Δ=" + (deltaPts > 0 ? "+" : "") + round2(deltaPts) + ")");
            // No order is placed at the broker — SL is a virtual trigger watched by the
            // fast-tick loop, which fires a MKT exit when LTP crosses slLevel. Return a
            // descriptive message so the modal status banner doesn't say "placed".
            String msg = "SL trigger " + round2(oldSl) + " → " + round2(p.slLevel);
            return ManualPlaceResult.ok(null, msg);
        }
    }

    /** For every open position that hasn't had its broker fill resolved yet, look up the actual
     *  trade price by entryOrderId in the cached tradebook and overwrite the estimate. Runs on
     *  the slow 5 s tick — usually resolves within the first tick after the order fills (Fyers
     *  tradebook is refreshed by OrderService on a similar cadence). */
    private void refreshUnresolvedFills() {
        if (state.openPositions.isEmpty()) return;
        for (Position p : state.openPositions.values()) {
            if (p.fillResolved) continue;
            if (p.entryOrderId == null || p.entryOrderId.isBlank()) continue;
            try {
                double fillPrice = orderService.getFilledPriceByOrderId(p.entryOrderId);
                if (fillPrice <= 0) continue;
                double oldEntry = p.entryPrice;
                double newEntry;
                if (p.preAddQty > 0 && p.qty > p.preAddQty) {
                    // The pending unresolved fill is for the LATEST mergeAdd top-up.
                    // Reconstruct the weighted average from (pre-add fills @ pre-add entry)
                    // and (add fills @ actual broker fill price). This preserves the
                    // contribution of every earlier fill instead of clobbering with just
                    // the latest one.
                    int    addQty       = p.qty - p.preAddQty;
                    double trueWeighted = (p.preAddQty * p.preAddEntry + addQty * fillPrice)
                                          / (double) p.qty;
                    newEntry = round2(trueWeighted);
                } else {
                    // Fresh-entry path — overwrite with the actual fill.
                    newEntry = round2(fillPrice);
                }
                p.entryPrice = newEntry;
                p.fillResolved = true;
                p.preAddQty    = 0;     // reconciliation complete
                p.preAddEntry  = 0;
                event("[INFO]", "Fill", p.symbol + " fill resolved — entry "
                    + round2(oldEntry) + " → " + round2(newEntry)
                    + (p.qty > 0 ? " (qty=" + p.qty + ")" : ""));
                saveToDisk();
            } catch (Exception e) {
                log.warn("[Camarilla] fill lookup failed for {}: {}", p.entryOrderId, e.getMessage());
            }
        }
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
            event("[INFO]", "Squareoff", "TIMED_EXIT — clock reached " + hhmm + ", flattening " + state.openPositions.size() + " position(s)");
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
        // SHORT closes with a BUY (+1); LONG closes with a SELL (-1). CRITICAL: pass the
        // SAME productType used for the entry so Fyers nets the two legs. A MARGIN buy
        // against an INTRADAY short does NOT square off — they're treated as separate
        // positions and the original entry stays open. MANUAL trades carry their own
        // productType (chosen from the modal dropdown); algo trades and legacy positions
        // fall back to the strategy's configured Camarilla order type.
        String productType = (p.productType == null || p.productType.isBlank())
            ? riskSettings.getCamarillaOrderType()
            : p.productType;
        int closeSide = p.isShort ? +1 : -1;
        OrderDTO close = orderService.placeExitOrder(symbol, p.qty, closeSide, productType);
        double exitPrice = 0;
        if (close != null) {
            try { exitPrice = marketDataService.getLtp(symbol); }
            catch (Exception ignored) {}
        }
        // SHORT P&L = (entry − exit) × qty (we sold high, hope to buy low).
        // LONG  P&L = (exit − entry) × qty (we bought low, hope to sell high).
        double sellTurnover = (p.isShort ? p.entryPrice : exitPrice) * p.qty;
        double buyTurnover  = (p.isShort ? exitPrice  : p.entryPrice) * p.qty;
        double gross   = p.isShort
            ? (p.entryPrice - exitPrice) * p.qty
            : (exitPrice  - p.entryPrice) * p.qty;
        double charges = perCycleCharges(sellTurnover, buyTurnover);
        double net     = gross - charges;

        // Single timestamp shared by the persisted DB row AND the in-memory ring entry —
        // AnalyticsService dedups by closedAtMillis to avoid double-counting today's cycles,
        // and that dedup only works if both sources stamp the SAME value.
        long closedAtMillis = System.currentTimeMillis();
        // MANUAL trades persist with strategyId="manual" so analytics, calendar, and
        // strategy-history endpoints surface them as a separate source from the algo.
        String dbStrategyId = (p.setup == ActiveSetup.MANUAL) ? MANUAL_STRATEGY_ID : STRATEGY_ID;
        persistTradeRow(dbStrategyId, p.symbol, p.setup.name(), reason, p.qty, gross, charges, net,
            reason.equals("SL_HIT") ? 1 : 0, closedAtMillis, p.openMillis, p.entryOiBias);

        Map<String, Object> cycle = new LinkedHashMap<>();
        // strategyId persisted on the in-memory cycle so AnalyticsService.appendLiveTodayTrades
        // can route MANUAL cycles to the "manual" filter and algo cycles to "camarilla".
        // Without this, every live cycle would inherit strat.id()="camarilla" and MANUAL
        // trades would show up under the Algo filter / disappear under the Manual filter.
        cycle.put("strategyId",     dbStrategyId);
        cycle.put("setup",          p.setup.name());
        cycle.put("symbol",         p.symbol);
        // Entry side — needed by the recent-trades renderer. Without this stored,
        // sideFromCycle defaults to SELL, which mislabels closed BUY positions.
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
        cycle.put("entryOiBias",    p.entryOiBias);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 100) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Exit",
            symbol + " closed (" + reason + ") net=" + round2(net) + " gross=" + round2(gross));

        state.openPositions.remove(symbol);

        // Stop subscribing to this symbol's candles UNLESS it's still in the V2 watchlist.
        if (!state.symbolRole.containsKey(symbol)) {
            candleAggregator.unsubscribe(symbol);
        }

        saveToDisk();
        return true;
    }

    private void persistTradeRow(String strategyId, String symbol, String setup, String reason, int qty,
                                 double gross, double charges, double net, int slHits,
                                 long closedAtMillis, long openedAtMillis, String entryOiBias) {
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
            row.setEntryOiBias(entryOiBias == null || entryOiBias.isBlank() ? null : entryOiBias);
            // Freeze whether the close happened on the currently-configured weekly expiry
            // day. NSE has changed this day before (Thursday → Tuesday) and may change it
            // again; storing the flag at write time keeps historical bucketing accurate
            // regardless of future setting changes.
            row.setWasExpiryDay(isExpiryDayNow(sessionDate));
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

    /** Compare the session date's day-of-week against the operator-configured weekly expiry
     *  day. Defaults to TUESDAY when the setting is blank or unparseable. */
    private boolean isExpiryDayNow(LocalDate sessionDate) {
        String configured = riskSettings.getWeeklyExpiryDayOfWeek();
        if (configured == null || configured.isBlank()) configured = "TUESDAY";
        try {
            return sessionDate.getDayOfWeek() == java.time.DayOfWeek.valueOf(configured.toUpperCase());
        } catch (Exception e) {
            return sessionDate.getDayOfWeek() == java.time.DayOfWeek.TUESDAY;
        }
    }

    // ── Day rollover ─────────────────────────────────────────────────────────

    /** Hard daily reset cron — fires at 06:00 IST every day. Don't wait for the first
     *  market-hour tick to clear yesterday's state: by 06:00 the operator may already
     *  be looking at the Positions Event Log and expects a clean slate.
     *  Forces a fresh day key + clears the event ring, trades list, and stale watchlist
     *  roles even if {@code state.dayKey} happens to equal today (e.g. after a manual
     *  edit or a partial earlier reset). */
    @Scheduled(cron = "0 0 6 * * *", zone = "Asia/Kolkata")
    public synchronized void scheduledDailyReset() {
        String today = LocalDate.now(IST).toString();
        log.info("[Camarilla] 06:00 IST daily reset — clearing events + today's trades (was dayKey={})", state.dayKey);
        state.dayKey = today;
        state.tradesToday = 0;
        state.consecutiveLosses = 0;
        state.doneForDay = false;
        state.todayClosedTrades.clear();
        if (state.recentEvents != null) state.recentEvents.clear();
        for (String sym : new ArrayList<>(state.openPositions.keySet())) {
            candleAggregator.unsubscribe(sym);
        }
        state.openPositions.clear();
        state.symbolRole.clear();
        pendingByBar.clear();
        saveToDisk();
        publishStream();
    }

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
            // Event ring is per-session — drop yesterday's events so the Positions Event Log
            // starts fresh each morning. Without this the log accumulates indefinitely across
            // restarts and old [Entry] / [Exit] lines from previous days bleed into today's view.
            if (state.recentEvents != null) state.recentEvents.clear();
            // Any positions surviving overnight are dropped (intraday product or operator action).
            for (String sym : new ArrayList<>(state.openPositions.keySet())) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            // V2: reset the watchlist roles so the new day's first ATM resolution rebuilds them.
            state.symbolRole.clear();
            pendingByBar.clear();
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
            // SHORT: profit when LTP drops below entry → (entry − ltp) × qty.
            // LONG:  profit when LTP rises above entry → (ltp − entry) × qty.
            return p.isShort
                ? (p.entryPrice - ltp) * p.qty
                : (ltp - p.entryPrice) * p.qty;
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
        m.put("watchlistSize",     state.symbolRole.size());
        m.put("watchlistRoles",    new LinkedHashMap<>(state.symbolRole));

        // v2 header chips — the v1 VWAP-CE / VWAP-PE option-premium chips are replaced
        // by FUTURES-driven chips that reflect what the triggers actually read:
        //   • futSymbol  / futLtp  / futVwap — near-month NIFTY future feed
        //   • putSymbol  / putLtp           — ATM−50 PUT (bullish trade leg)
        //   • callSymbol / callLtp          — ATM+50 CALL (bearish trade leg)
        // The legacy {ceSymbol, peSymbol, ceVwap, peVwap} fields are still emitted
        // (zero-value) so v1-era frontends that haven't been rebuilt don't NPE on
        // missing keys — they'll just render '—' until the page is updated.
        Map<String, Object> vwap = new LinkedHashMap<>();
        String futSym = state.futuresSymbol == null ? "" : state.futuresSymbol;
        double futLtp = 0, futVwap = 0, futChange = 0, futChangePct = 0;
        if (!futSym.isBlank()) {
            try { futLtp        = marketDataService.getLtp(futSym); }              catch (Exception ignored) {}
            try { futVwap       = marketDataService.getVwap(futSym); }             catch (Exception ignored) {}
            try { futChange     = marketDataService.getDisplayChange(futSym); }    catch (Exception ignored) {}
            try { futChangePct  = marketDataService.getDisplayChangePct(futSym); } catch (Exception ignored) {}
        }
        long atm = atmTracker.getCurrentAtm();
        String putSym = "", callSym = "";
        double putLtp = 0, callLtp = 0;
        if (atm > 0) {
            BalancedAtmSelector.StrikeAtLevel putRow  = atmSelector.resolveStrikeAtLevel(atm - STRIKE_STEP);
            BalancedAtmSelector.StrikeAtLevel callRow = atmSelector.resolveStrikeAtLevel(atm + STRIKE_STEP);
            if (putRow  != null && putRow.peSymbol()  != null)  putSym  = putRow.peSymbol();
            if (callRow != null && callRow.ceSymbol() != null) callSym = callRow.ceSymbol();
            if (!putSym.isBlank())  { try { putLtp  = marketDataService.getLtp(putSym);  } catch (Exception ignored) {} }
            if (!callSym.isBlank()) { try { callLtp = marketDataService.getLtp(callSym); } catch (Exception ignored) {} }
        }
        vwap.put("futSymbol",    futSym);
        vwap.put("futLtp",       round2(futLtp));
        vwap.put("futChange",    round2(futChange));
        vwap.put("futChangePct", round2(futChangePct));
        vwap.put("futVwap",      round2(futVwap));
        vwap.put("putSymbol",    putSym);
        vwap.put("putStrike",    atm > 0 ? (atm - STRIKE_STEP) : 0);
        vwap.put("putLtp",       round2(putLtp));
        vwap.put("callSymbol",   callSym);
        vwap.put("callStrike",   atm > 0 ? (atm + STRIKE_STEP) : 0);
        vwap.put("callLtp",      round2(callLtp));
        // Back-compat shims (v1 keys, always 0 now) so the old chip helpers don't break.
        vwap.put("ceSymbol", "");
        vwap.put("peSymbol", "");
        vwap.put("ceVwap",   0);
        vwap.put("peVwap",   0);
        m.put("atmVwap", vwap);

        // Open positions list — each row carries its own LTP, MTM, target/SL levels.
        List<Map<String, Object>> rows = new ArrayList<>();
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
            // v2 positions report futures-price target/SL. v1 positions report
            // option-premium target/SL with the 0.05 tick floor clamp for negative
            // L5 expiry-day cases.
            boolean v2 = p.triggerSymbol != null && !p.triggerSymbol.isBlank();
            double displayedTarget = p.targetLevel;
            if (!v2 && p.isShort && !Double.isNaN(displayedTarget) && displayedTarget < OPTION_TICK_SIZE) {
                displayedTarget = OPTION_TICK_SIZE;
            }
            row.put("targetLevel",   round2(displayedTarget));
            row.put("slLevel",       round2(p.slLevel));
            row.put("breakevenMoved", p.breakevenMoved);
            row.put("isShort",       p.isShort);
            row.put("openMillis",    p.openMillis);
            // v2 metadata for the Live Positions table: triggerSymbol presence flips
            // the target/SL formatter to render '24580.00 FUT' instead of '₹24580.00'.
            row.put("triggerSymbol", p.triggerSymbol == null ? "" : p.triggerSymbol);
            row.put("entryFutures",  round2(p.entryFutures));
            row.put("targetFutures", round2(p.targetFutures));
            row.put("slFutures",     round2(p.slFutures));
            rows.add(row);
        }
        m.put("openPositions", rows);
        double exposedRisk = exposedRiskNow();

        // Per-symbol levels for the V2 6-contract watchlist + any open-position symbols.
        Map<String, CamarillaLevels> perSymbolLevels = new LinkedHashMap<>();
        for (String sym : state.symbolRole.keySet()) {
            CamarillaLevels lv = camarillaService.getLevels(sym);
            if (lv != null) perSymbolLevels.put(sym, lv);
        }
        for (String sym : state.openPositions.keySet()) {
            if (perSymbolLevels.containsKey(sym)) continue;
            CamarillaLevels lv = camarillaService.getLevels(sym);
            if (lv != null) perSymbolLevels.put(sym, lv);
        }
        m.put("perSymbolLevels", perSymbolLevels);

        // Risk block — same shape as equities Positions page badges.
        double consumedRisk = consumedRiskNow();
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
        /** v2 — set when consumedRisk OR exposedRisk crosses camarillaMaxRisk
         *  during the session. Once true, no new algo entries fire for the
         *  rest of the day. Persisted so a mid-day restart preserves it.
         *  Cleared on the day-key flip (next session). */
        public boolean dailyLossLockout;
        /** v2 — the resolved NIFTY near-month future symbol used as the trigger
         *  feed for this session. Set when the AtmChange bootstrap fires;
         *  persisted so a restart can re-subscribe without re-resolving. */
        public String futuresSymbol = "";

        // ── V2 watchlist ──────────────────────────────────────────────────────
        /** Current monitored 6-contract matrix (ATM, ±1 strike, CE+PE) → role mapping. */
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
        public double     targetLevel;
        public double     slLevel;
        /** Original SL level frozen at {@code fire()} time. Used to compute 1R for the
         *  move-SL-to-breakeven trigger after {@code slLevel} mutates. Default 0 for
         *  legacy state files — readers fall back to current slLevel when this is 0. */
        public double     originalSlLevel;
        /** True once the breakeven trigger has fired and {@code slLevel} has been moved
         *  to entry. Guard so the move runs exactly once per position. */
        public boolean    breakevenMoved;
        /** Set to true once {@code entryPrice} has been replaced with the actual broker fill
         *  price (looked up by entryOrderId in the tradebook). Until then, {@code entryPrice}
         *  is the LTP estimate captured at order-placement time. The strategy's slow tick loop
         *  periodically resolves unresolved fills. */
        public boolean fillResolved;
        /** True if this is a SHORT (sell) position, false if LONG (buy). Derived from setup
         *  at fire() time and persisted so target/SL/breakeven/risk-budget comparisons can
         *  flip cleanly. Defaults to {@code true} on legacy state files (Jackson sets the
         *  field to the Java default for missing JSON properties), which is correct because
         *  all pre-V3 positions were SHORTs. */
        public boolean isShort = true;
        /** OI bias state read at the moment the entry order was placed. Frozen for the life
         *  of the position so the analytics page can compare outcomes between
         *  with-trend (Strong) and against-trend / no-signal (Weak) confirmations.
         *  Empty string when the bias couldn't be read (e.g. tracker not yet baselined). */
        public String entryOiBias = "";
        /** Consecutive fast-tick polls observing LTP at or above slLevel. Resets on every poll
         *  where LTP drops back below. SL fires when this reaches SL_BREACH_CONFIRM_TICKS.
         *  Transient — not persisted, repopulated by the runtime after a restart. */
        public transient int slBreachStreak;
        /** Captured BEFORE the latest mergeAdd top-up, so refreshUnresolvedFills can
         *  reconstruct the correct weighted average once the new order's actual broker
         *  fill is known. {@code preAddQty == 0} signals "no pending add to reconcile" —
         *  refreshUnresolvedFills then falls back to the fresh-entry overwrite path. */
        public int    preAddQty;
        public double preAddEntry;
        /** Fyers product type used for the entry order ("INTRADAY" / "MARGIN" / "CNC").
         *  Persisted so subsequent top-ups, reduces, and the auto-close use the SAME
         *  product — required for Fyers to net the legs into one position. Defaults
         *  to empty (""), and the close/adjust paths fall back to the strategy's
         *  configured Camarilla order type when this is blank (covers algo positions
         *  and legacy state files predating this field). */
        public String productType = "";
        // ── v2 fields (futures-driven triggers, option trade leg) ──
        /** Fyers symbol of the NIFTY future used as the trigger feed (e.g.
         *  {@code NSE:NIFTY26JUNFUT}). {@code symbol} stays the OPTION the trade
         *  was placed on; this field tells fastSlCheck which symbol's LTP drives
         *  SL/target comparison. Empty for v1 positions — fastSlCheck falls
         *  back to {@code symbol} (option premium) when blank. */
        public String triggerSymbol = "";
        /** Futures price at entry. Used for breakeven move math and exposed-risk
         *  proxy. 0 for v1 positions. */
        public double entryFutures;
        /** Target futures price — when futures LTP crosses this, close the option
         *  at market. {@code Double.NaN} disables the auto-target. */
        public double targetFutures = Double.NaN;
        /** SL futures price — when futures LTP crosses this for SL_BREACH_CONFIRM_TICKS
         *  consecutive polls, close the option at market. {@code Double.NaN}
         *  disables the auto-SL. */
        public double slFutures = Double.NaN;
    }

    /** Backward-compat overload — defaults source to "Strategy". */
    private void event(String severity, String message) {
        event(severity, "Strategy", message);
    }

    /** Record an event with severity + source-component tag + message. The source label is
     *  surfaced in the Trade page event log so the operator can see which subsystem produced
     *  each line (ATM / Entry / Exit / Fill / System / Strategy). */
    private void event(String severity, String source, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts",       System.currentTimeMillis());
        e.put("severity", severity);
        e.put("source",   source);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [camarilla:" + source + "] " + message);
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
