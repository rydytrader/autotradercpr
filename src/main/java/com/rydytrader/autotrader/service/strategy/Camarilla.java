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
 * strikes still have running trades. State persists to {@code ../store/cache/camarilla-state.json}.
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

    /** Per-instrument constants captured in one place. Currently NIFTY-only
     *  — BankNifty trading was retired in a later revision. The record type
     *  is retained because the fire path and detection helpers thread
     *  {@code InstrumentConfig ic} through their signatures; simplifying
     *  those signatures further is a separate refactor. */
    public record InstrumentConfig(
        String name,
        String spotSymbol,
        int    lotSize,
        long   strikeStep,
        String expiryCadence
    ) {
        public static final InstrumentConfig NIFTY = new InstrumentConfig(
            "NIFTY", NIFTY_SYMBOL, 65, 50L, "WEEKLY");
    }
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/camarilla-state.json";
    private static final String LEGACY_STATE_FILE = "../store/data/camarilla-state.json";
    private static final int LOT_SIZE = 65;
    /** NSE NIFTY option premium tick size — the minimum tradable price. Used to
     *  clamp negative Camarilla L5 targets to a meaningful display floor on the
     *  positions card. */
    private static final double OPTION_TICK_SIZE = 0.05;
    /** Approximate delta of an ATM NIFTY option used to translate spot SL distance
     *  into projected option-premium loss. ATM options track the underlying at
     *  roughly half pace — a 100-pt NIFTY move on an ATM CE shifts premium by
     *  ~50 pts. The exposed-risk gate uses this to project realistic option loss
     *  rather than the raw 1:1 spot-distance proxy. Empirical 0.5 is good enough
     *  for the gate's purpose — we don't need Black-Scholes precision here. */
    private static final double ATM_DELTA = 0.5;

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
    /** v2 two-candle entry — maximum bars a pending confirmation may sit in its
     *  slot without a trigger or invalidation. Six bars × 5 min ≈ 30 min: by
     *  that point the confirmation candle's geometry no longer reflects current
     *  market structure, so the pending is nullified and we hunt for a fresh
     *  confirmation on subsequent bars. Tuneable via {@link #MAX_PENDING_BARS}
     *  if signal noise warrants a different window. */
    private static final int MAX_PENDING_BARS = 6;

    public enum ActiveSetup {
        // v2 setups — all triggered on the NIFTY near-month FUTURES 5-min bar close,
        // all sell a SHORT OTM option. The trade leg's symbol is picked at fire time.
        L3_REVERSAL,       // bullish: green spot bar wicks below L3, closes above → sell ATM PUT
        H3_REVERSAL,       // bearish: red spot bar wicks above H3, closes below → sell ATM CALL
        H4_BREAKOUT,       // bullish: spot bar closes above H4 → sell ATM PUT
        L4_BREAKDOWN,      // bearish: spot bar closes below L4 → sell ATM CALL
        VWAP_BREAKDOWN,    // v1 legacy (retired) — kept for old DB row deserialisation only
        MANUAL             // legacy (retired) — kept for DB row deserialisation only
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

    /** True for the bullish-bet setups (sell PUT). Used by fire() to pick the ATM
     *  PUT vs ATM CALL trade leg. */
    private static boolean isBullishBet(ActiveSetup s) {
        return s == ActiveSetup.L3_REVERSAL
            || s == ActiveSetup.H4_BREAKOUT;
    }

    /** Composite key {@code "setup|symbol"} for {@code state.openPositions}.
     *  Allows a MANUAL Options-Scalper-Terminal position to coexist with a
     *  bot-managed directional fire on the same Fyers option symbol —
     *  each tracked independently with its own SL. */
    private static String posKey(Position p) {
        if (p == null) return "";
        String setup = p.setup == null ? "MANUAL" : p.setup.name();
        return setup + "|" + (p.symbol == null ? "" : p.symbol);
    }
    private static String posKey(ActiveSetup setup, String symbol) {
        String s = setup == null ? "MANUAL" : setup.name();
        return s + "|" + (symbol == null ? "" : symbol);
    }

    /** V2 watchlist role for each monitored option contract. ATM and ITM strikes only check
     *  L4 breakdown; OTM strikes only check H3 reversal. The role is stored per-symbol in
     *  {@link State#symbolRole} so re-subscribed positions inherit it across restarts. */
    public enum WatchRole { ATM_L4, ITM_L4, OTM_H3 }



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
    private final ObjectProvider<com.rydytrader.autotrader.service.NiftyRsiService> niftyRsiProvider;
    private final ObjectProvider<com.rydytrader.autotrader.service.NiftyAtrService> niftyAtrProvider;
    // Tolerate unknown fields on read so a state file written by a different
    // branch (e.g. a future v3 or v1's older shape) doesn't wipe today's
    // in-memory ring on boot. Without this guard Jackson throws
    // UnrecognizedPropertyException, loadFromDisk falls into the catch block,
    // and we silently lose today's recentEvents + todayClosedTrades.
    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        // Stale state files written before the strangle was removed may carry
        // H4_STRANGLE / L4_STRANGLE setup values in openPositions; parse them as
        // null instead of failing, and let loadFromDisk's migration drop them.
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

    private volatile State state = new State();
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();

    public Camarilla(CamarillaService camarillaService,
                     CandleAggregator candleAggregator,
                     AtmTracker atmTracker,
                     BalancedAtmSelector atmSelector,
                     MarketDataService marketDataService,
                     OrderService orderService,
                     EventService eventService,
                     RiskSettingsStore riskSettings,
                     ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                     ObjectProvider<CamarillaStreamBroker> streamBrokerProvider,
                     ObjectProvider<com.rydytrader.autotrader.service.NiftyRsiService> niftyRsiProvider,
                     ObjectProvider<com.rydytrader.autotrader.service.NiftyAtrService> niftyAtrProvider) {
        this.camarillaService     = camarillaService;
        this.candleAggregator     = candleAggregator;
        this.atmTracker           = atmTracker;
        this.atmSelector          = atmSelector;
        this.marketDataService    = marketDataService;
        this.orderService         = orderService;
        this.eventService         = eventService;
        this.riskSettings         = riskSettings;
        this.tradeRepoProvider    = tradeRepoProvider;
        this.streamBrokerProvider = streamBrokerProvider;
        this.niftyRsiProvider     = niftyRsiProvider;
        this.niftyAtrProvider     = niftyAtrProvider;
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
        // v2: subscribe the NIFTY spot trigger feed at boot — independent of
        // AtmTracker. The trigger symbol is the spot index (constant), so we no
        // longer need an AtmChange event to know which symbol to subscribe.
        // The ATM strike is computed live at fire time from the current spot LTP.
        final String triggerSym = NIFTY_SYMBOL;
        state.futuresSymbol = triggerSym;
        state.symbolRole.put(triggerSym, WatchRole.ATM_L4);
        candleAggregator.subscribe(triggerSym, c -> onCandleClose(triggerSym, c));
        try { marketDataService.subscribeAdditional(java.util.List.of(triggerSym)); }
        catch (Exception ignored) {}
        log.info("[Camarilla] v2 boot — trigger feed subscribed: {}", triggerSym);

        // AtmTracker listener — Camarilla no longer depends on AtmChange for
        // strike resolution (fire() looks up the pre-resolved session leg),
        // but keeping the registration is harmless and lets any future
        // AtmChange consumer hook in cleanly.
        atmTracker.setListener(this::onAtmChange);
        // Best-effort attempt to resolve today's session-static OTM legs.
        // Fails silently when Camarilla levels or the option chain aren't
        // warmed yet; the scheduled retry catches up.
        try { resolveSessionLegs(); }
        catch (Exception e) { log.warn("[Camarilla] NIFTY session-legs boot resolve failed: {}", e.getMessage()); }
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
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                if (closePosition(p, reason == null ? "MANUAL" : reason)) anyClosed = true;
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
    /** Wipe ALL recorded trades + ALL event-log entries + EVERY DB row. Open
     *  positions are preserved (same convention as {@link #clearTodayRecords}).
     *  Used by Settings → Maintenance → Clear ALL Records for a hard reset after
     *  a test run. Irreversible. */
    public synchronized Map<String, Object> clearAllRecords() {
        int cyclesCleared = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();

        int prevTradesToday      = state.tradesToday;
        int prevConsecutiveLoss  = state.consecutiveLosses;
        state.tradesToday        = 0;
        state.consecutiveLosses  = 0;

        int eventsCleared = state.recentEvents.size();
        state.recentEvents.clear();

        // Also drop pending two-candle confirmations — without trades or events
        // backing them up, surviving pendings are misleading.
        state.pendingBullish = null;
        state.pendingBearish = null;

        saveToDisk();

        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) {
                // Use the explicit JPQL @Modifying + @Transactional delete (deleteAllRows)
                // instead of deleteAllInBatch() — the explicit version reliably opens its own
                // transaction when called from this non-@Transactional service method, while
                // deleteAllInBatch was silently no-op'ing without a wrapping tx.
                dbCleared = repo.deleteAllRows();
                log.warn("[Camarilla] clearAllRecords — DB deleteAllRows wiped {} rows", dbCleared);
            }
        } catch (Exception e) {
            log.warn("[Camarilla] clearAllRecords DB wipe failed: {}", e.getMessage());
        }

        event("[WARNING]", "Maintenance",
            "Cleared ALL records — cycles=" + cyclesCleared
            + " events=" + eventsCleared
            + " dbRows=" + dbCleared
            + " (open positions preserved)");
        log.warn("[Camarilla] clearAllRecords — cycles={} events={} dbRows={} prevTradesToday={} prevConsLoss={}",
            cyclesCleared, eventsCleared, dbCleared, prevTradesToday, prevConsecutiveLoss);
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
            // Close EVERY logical position on this symbol (composite-key world:
            // a MANUAL Options-Scalper-Terminal position and a bot-managed
            // directional fire can both be open on the same Fyers symbol).
            boolean anyClosed = false;
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                if (p != null && symbol.equals(p.symbol)) {
                    if (closePosition(p, reason == null ? "MANUAL" : reason)) anyClosed = true;
                }
            }
            return anyClosed;
        }
    }

    /** True when at least one AUTO position (any non-MANUAL setup) is currently
     *  open. Used by Phase 3 confirmation-detection to suspend scanning while
     *  an auto trade is in flight, without suspending it for operator-driven
     *  MANUAL terminal trades that run alongside the auto strategy. */
    private boolean hasOpenAutoPosition() {
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            if (p.setup == ActiveSetup.MANUAL) continue;
            return true;
        }
        return false;
    }

    @Override
    public void resetToIdle(String reason) {
        synchronized (this) {
            // Drop in-memory positions WITHOUT placing exits (operator recovery flow).
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
        // v1 vestigial: warmUpAroundAtm fan-out fetched levels for ~20 option strikes
        // around ATM. v2 only needs NIFTY spot levels (fetched at boot + 08:00 cron),
        // so the call is removed. Leaving this in spammed the levels cache with
        // option entries on every restart.
    }

    @Override
    public void fastSlCheck() {
        // First: drain any bar-candidate buffers whose grace window has elapsed.
        drainPendingBars();

        // ── Tick-cadence Phase 1 trigger ──
        // Checks any pending confirmation against live NIFTY spot and fires
        // the moment spot crosses confirmHigh + (ATR × triggerAtrMult) for
        // bullish or confirmLow − same for bearish. This runs even when
        // there are no open positions (fresh confirmations may need to
        // trigger on a session with no prior trade activity), so it must
        // happen BEFORE the openPositions-empty short-circuit below.
        tryFireTriggeredPending();

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
        double maxRisk = riskSettings.getPortfolioMaxDailyLoss();
        if (maxRisk > 0 && !state.dailyLossLockout) {
            double exposed = exposedRiskNow();
            if (exposed > maxRisk) {
                event("[ERROR]", "Risk", "exposed ₹" + round2(exposed)
                    + " > maxRisk ₹" + round2(maxRisk)
                    + " — force-closing all positions and locking session");
                state.dailyLossLockout = true;
                for (Position p : new ArrayList<>(state.openPositions.values())) {
                    closePosition(p, "RISK_BREACH");
                }
                saveToDisk();
                return;
            }
        }

        // Iterate by Position (not by symbol) so a MANUAL Options-Scalper-
        // Terminal position and a bot-managed directional fire that share a
        // Fyers symbol are each watched independently with their own SL/target.
        for (Position p : new ArrayList<>(state.openPositions.values())) {
            if (p == null) continue;
            String symbol = p.symbol;
            String posKey = posKey(p);

            // Resolve the LTP that drives target/SL decisions for THIS position.
            // v2 = futures LTP, v1 = option premium LTP.
            boolean v2 = p.triggerSymbol != null && !p.triggerSymbol.isBlank()
                       && !Double.isNaN(p.targetFutures) && !Double.isNaN(p.slFutures);
            String triggerSrc = v2 ? p.triggerSymbol : symbol;
            double triggerLtp;
            try { triggerLtp = marketDataService.getLtp(triggerSrc); }
            catch (Exception e) { continue; }
            if (triggerLtp <= 0) continue;

            // Direction-aware comparisons. v2 sells options for BOTH directional
            // bets (sell PUT for bullish, sell CALL for bearish), so p.isShort
            // is always true and can't drive the comparator. Use the bet
            // direction instead — bullish bets (L3_REVERSAL, H4_BREAKOUT) want
            // NIFTY to RISE (target ABOVE entry, SL BELOW); bearish bets
            // (H3_REVERSAL, L4_BREAKDOWN) want NIFTY to FALL (target BELOW,
            // SL ABOVE).
            double targetRef = v2 ? p.targetFutures : p.targetLevel;
            double slRef     = v2 ? p.slFutures     : p.slLevel;
            boolean bullishBet = v2 ? isBullishBet(p.setup) : !p.isShort;
            boolean targetHit = bullishBet ? (triggerLtp >= targetRef) : (triggerLtp <= targetRef);
            boolean slBreach  = bullishBet ? (triggerLtp <= slRef)     : (triggerLtp >= slRef);

            if (targetHit) {
                Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
                synchronized (lock) {
                    Position p2 = state.openPositions.get(posKey);
                    if (p2 == null) continue;
                    double tgt2 = v2 ? p2.targetFutures : p2.targetLevel;
                    boolean stillHit = bullishBet ? (triggerLtp >= tgt2) : (triggerLtp <= tgt2);
                    if (!stillHit) continue;
                    event("[SUCCESS]", "Exit", "TARGET — " + shortSym(symbol) + " @ " + round2(triggerLtp));
                    closePosition(p2, "TARGET_HIT");
                }
                continue;
            }

            if (slBreach) {
                p.slBreachStreak++;
                if (p.slBreachStreak >= SL_BREACH_CONFIRM_TICKS) {
                    Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
                    synchronized (lock) {
                        Position p2 = state.openPositions.get(posKey);
                        if (p2 == null) continue;
                        event("[WARNING]", "Exit", "SL — " + shortSym(symbol) + " @ " + round2(triggerLtp));
                        closePosition(p2, "SL_HIT");
                    }
                }
            } else if (p.slBreachStreak > 0) {
                p.slBreachStreak = 0;
            }
        }
    }

    // ── V2 two-candle entry — no buffering needed ──────────────────────────
    // The new model evaluates trigger/invalidation/detection inline inside
    // onCandleClose. The fast-tick scheduler still calls drainPendingBars()
    // every poll, so this stays as a no-op for back-compat.
    private void drainPendingBars() { /* retired in v2 two-candle entry */ }

    /** Tick-cadence Phase 1 trigger check. Fires the pending confirmation
     *  the moment NIFTY spot crosses confirmHigh + (ATR × mult) for bullish
     *  or confirmLow − (ATR × mult) for bearish. Runs on every fastSlCheck
     *  poll (~1.5 s).
     *
     *  <p>ATR-not-seeded fallback: {@code atrVal = 0} → both trigger delta
     *  AND SL buffer become 0. Trigger fires at exact extreme; SL sits at
     *  exact extreme too. Same failure mode both features already share.
     *  Wilder usually seeds within the first few bars of the session.
     *
     *  <p>Passes {@code null} for entryCandle — fire() has a null-tolerant
     *  entryFutures guard that falls back to live spot LTP. */
    private synchronized void tryFireTriggeredPending() {
        if (!isEnabled()) return;
        if (state.dailyLossLockout) return;
        if (!canFireNewEntry()) return;

        double triggerAtrMult = Math.max(0, riskSettings.getCamarillaTriggerAtrMult());
        double slAtrMult      = Math.max(0, riskSettings.getCamarillaDirectionalSlBufferAtrMult());

        // ── NIFTY leg ──
        if ((state.pendingBullish != null || state.pendingBearish != null)
            && state.futuresSymbol != null && !state.futuresSymbol.isBlank()) {
            double spot;
            try { spot = marketDataService.getLtp(NIFTY_SYMBOL); }
            catch (Exception e) { spot = 0; }
            if (spot > 0) {
                com.rydytrader.autotrader.service.NiftyAtrService atrSvc =
                    niftyAtrProvider == null ? null : niftyAtrProvider.getIfAvailable();
                Double atr = atrSvc == null ? null : atrSvc.currentAtr();
                double atrVal = (atr != null && atr > 0) ? atr : 0;
                double triggerDelta = atrVal * triggerAtrMult;
                double slBuf        = atrVal * slAtrMult;
                if (tryFireOnePending(state.futuresSymbol, spot, triggerDelta, slBuf,
                        state.pendingBullish, state.pendingBearish,
                        InstrumentConfig.NIFTY,
                        p -> state.pendingBullish = p,
                        p -> state.pendingBearish = p)) {
                    saveToDisk();
                    return;
                }
            }
        }

    }

    /** Shared trigger logic for one instrument. Every geometric confirmation
     *  arms a tick trigger at {@code confirmExtreme ± ATR × triggerMult}; the
     *  first spot tick to touch that buffered level fires the trade. Returns
     *  true when a fire happened (either direction). The pending-setter
     *  lambdas let the caller clear the correct instrument's state field. */
    private boolean tryFireOnePending(String triggerSym, double spot,
                                       double triggerDelta, double slBuf,
                                       PendingConfirmation pb, PendingConfirmation pr,
                                       InstrumentConfig ic,
                                       java.util.function.Consumer<PendingConfirmation> setBullish,
                                       java.util.function.Consumer<PendingConfirmation> setBearish) {
        if (pb != null) {
            double triggerPrice = pb.confirmHigh + triggerDelta;
            if (spot >= triggerPrice) {
                double slWithBuffer = pb.confirmLow - slBuf;
                double rr = computeRR(spot, pb.targetLevel, slWithBuffer);
                event("[INFO]", "Setup", "[" + ic.name() + "] " + pb.setup + " trigger @ " + round2(spot)
                    + " (SL " + round2(slWithBuffer) + ", TGT " + round2(pb.targetLevel)
                    + ", R:R " + (Double.isNaN(rr) ? "—" : round2(rr)) + ")");
                fire(triggerSym, pb.setup, pb.targetLevel, slWithBuffer, null, pb.lockedAtm, ic);
                setBullish.accept(null);
                return true;
            }
        }
        if (pr != null) {
            double triggerPrice = pr.confirmLow - triggerDelta;
            if (spot <= triggerPrice) {
                double slWithBuffer = pr.confirmHigh + slBuf;
                double rr = computeRR(spot, pr.targetLevel, slWithBuffer);
                event("[INFO]", "Setup", "[" + ic.name() + "] " + pr.setup + " trigger @ " + round2(spot)
                    + " (SL " + round2(slWithBuffer) + ", TGT " + round2(pr.targetLevel)
                    + ", R:R " + (Double.isNaN(rr) ? "—" : round2(rr)) + ")");
                fire(triggerSym, pr.setup, pr.targetLevel, slWithBuffer, null, pr.lockedAtm, ic);
                setBearish.accept(null);
                return true;
            }
        }
        return false;
    }

    // ── ATM change handler — session-open watchlist setup ─────────────────────
    // With drift checks removed, this fires exactly once per session — on the
    // open-price bootstrap from AtmTracker. The heartbeat path (oldAtm == newAtm)
    // is gone with the drift loop.

    /** v2 — true no-op. The trigger feed (NIFTY spot) is subscribed at boot, and
     *  the ATM strike is computed live from spot LTP at fire() time. AtmTracker's
     *  session-locked baseline is irrelevant to Camarilla; the listener
     *  registration in boot() is retained as a harmless hook for any future
     *  AtmChange consumer. Camarilla emits nothing — posting "resolved → N"
     *  was misleading since the value never gates a trade decision. */
    public synchronized void onAtmChange(AtmTracker.AtmChange ev) {
        // intentionally empty
    }

    // ── Candle close handler — entries + exits, per symbol ──────────────────

    public void onCandleClose(String symbol, Candle c) {
        if (!isEnabled()) return;
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            rolloverIfNewDay();
            if (state.doneForDay) return;

            // Session lockout from the v2 risk gate.
            if (state.dailyLossLockout) return;
            if (!canFireNewEntry()) return;

            // Only route NIFTY candles — the strategy is NIFTY-only after
            // BankNifty was retired.
            String niftySym = state.futuresSymbol;
            if (niftySym != null && !niftySym.isBlank() && niftySym.equals(symbol)) {
                CamarillaLevels lv = camarillaService.getLevels(niftySym);
                if (lv != null) processNiftyBarPhases(c, lv);
            }

            saveToDisk();
        }
    }

    /** NIFTY Phase 2/3/4 walk — expiry, invalidation, and new-confirmation
     *  detection on the NIFTY pending slots. Extracted from the old inline
     *  onCandleClose body so the router can invoke it symmetrically with the
     *  BankNifty variant below. */
    private void processNiftyBarPhases(Candle c, CamarillaLevels lv) {
        // --- Phase 2: STALENESS EXPIRY ---
        long maxPendingAgeMs = MAX_PENDING_BARS * BAR_LENGTH_MS;
        if (state.pendingBullish != null
            && c.startMillis() - state.pendingBullish.barStartMs >= maxPendingAgeMs) {
            event("[INFO]", "Setup", "[NIFTY] " + state.pendingBullish.setup + " expired (" + MAX_PENDING_BARS + " bars)");
            state.pendingBullish = null;
        }
        if (state.pendingBearish != null
            && c.startMillis() - state.pendingBearish.barStartMs >= maxPendingAgeMs) {
            event("[INFO]", "Setup", "[NIFTY] " + state.pendingBearish.setup + " expired (" + MAX_PENDING_BARS + " bars)");
            state.pendingBearish = null;
        }
        // --- Phase 3: INVALIDATION ---
        if (state.pendingBullish != null && c.close() < state.pendingBullish.confirmLow) {
            event("[INFO]", "Setup", "[NIFTY] " + state.pendingBullish.setup + " nullified @ " + round2(c.close()));
            state.pendingBullish = null;
        }
        if (state.pendingBearish != null && c.close() > state.pendingBearish.confirmHigh) {
            event("[INFO]", "Setup", "[NIFTY] " + state.pendingBearish.setup + " nullified @ " + round2(c.close()));
            state.pendingBearish = null;
        }
        // --- Phase 4: NEW CONFIRMATION (with opposite-direction swap) ---
        // The blocking guard is "no open trade" — the swap logic below handles
        // the pending-slot cases.
        // Rules:
        //  - Fresh confirmation OPPOSITE the current pending → discard the
        //    stale pending, seed the fresh one. Applies at any age within
        //    the pending window, not just the next bar.
        //  - Fresh confirmation SAME direction as the current pending →
        //    ignore (do not overwrite). Preserves the existing behaviour
        //    where H4_BREAKOUT stays put when a follow-up L3_REVERSAL prints.
        if (!hasOpenAutoPosition()) {
            PendingConfirmation fresh = detectConfirmation(c, lv);
            if (fresh != null) {
                boolean freshBullish = isBullishBet(fresh.setup);
                if (freshBullish && state.pendingBearish != null) {
                    event("[INFO]", "Setup", "[NIFTY] " + state.pendingBearish.setup
                        + " superseded — fresh bullish " + fresh.setup + " confirmed");
                    state.pendingBearish = null;
                }
                if (!freshBullish && state.pendingBullish != null) {
                    event("[INFO]", "Setup", "[NIFTY] " + state.pendingBullish.setup
                        + " superseded — fresh bearish " + fresh.setup + " confirmed");
                    state.pendingBullish = null;
                }
                boolean sameSlotEmpty = freshBullish
                    ? state.pendingBullish == null
                    : state.pendingBearish == null;
                if (sameSlotEmpty && classifyAndSeed(fresh, c, lv, InstrumentConfig.NIFTY)) {
                    if (freshBullish) state.pendingBullish = fresh;
                    else              state.pendingBearish = fresh;
                }
            }
        }
    }

    /** v2 two-candle entry — detect whether the bar c is a CONFIRMATION candle for
     *  one of the four setups. Returns null if no setup's confirmation geometry
     *  matches. Reversal precedence over breakout (rarer structural signal).
     *
     *  <p>Breakout/breakdown require the bar to actually CROSS the level — bar low
     *  ≤ H4 with close above (or bar high ≥ L4 with close below). A bar that
     *  sits entirely above H4 (low > H4) is not a fresh breakout — the breakout
     *  already happened on a prior bar; firing again would be a duplicate setup. */
    private PendingConfirmation detectConfirmation(Candle c, CamarillaLevels lv) {
        if (lv == null) return null;

        // Geometry-only confirmation. The bar must wick beyond the level AND
        // close back across it — that's the level-test-and-recover pattern.
        // Bar COLOR is intentionally NOT a filter here: the trigger candle
        // (Phase 1) requires a same-direction bar that closes through the
        // confirmation's far extreme, which already enforces a strong
        // second-bar conviction. Requiring a coloured confirmation on top
        // dropped ~25-35% of valid level rejections without measurable edge.
        //
        // Bullish — reversal off L3 or breakout above H4.
        if (c.low() <= lv.l3() && c.close() > lv.l3()) {
            return mkConfirmation(ActiveSetup.L3_REVERSAL, c, lv.h3());
        }
        if (c.low() <= lv.h4() && c.close() > lv.h4()) {
            return mkConfirmation(ActiveSetup.H4_BREAKOUT, c, lv.h5());
        }
        // Bearish — reversal off H3 or breakdown below L4.
        if (c.high() >= lv.h3() && c.close() < lv.h3()) {
            return mkConfirmation(ActiveSetup.H3_REVERSAL, c, lv.l3());
        }
        if (c.high() >= lv.l4() && c.close() < lv.l4()) {
            return mkConfirmation(ActiveSetup.L4_BREAKDOWN, c, lv.l5());
        }
        return null;
    }

    /** Pre-subscribe BOTH the CE and PE leg at the locked ATM strike. The side
     *  fire() will trade (PUT for bullish, CALL for bearish) needs to be warm so
     *  the entry-price tick lands before the trigger bar; the opposite side is
     *  subscribed too so the operator can see both legs' LTP/VWAP in the header
     *  chip alongside ATM. Both symbols are stored on the pending so
     *  releaseOptionLegs() can unsubscribe them when the confirmation invalidates
     *  or expires. */
    /** Resolve the four session-static OTM trade legs from today's Camarilla
     *  levels and subscribe them in one shot. Idempotent — bails immediately
     *  once today's legs are already on file. Returns {@code true} if all four
     *  legs are successfully resolved (either freshly, or already resolved).
     *
     *  <p>Setup → leg mapping:
     *  <pre>
     *    H4_BREAKOUT  (bullish) → sell PE at strike nearest to H3
     *    L3_REVERSAL  (bullish) → sell PE at strike nearest to L4
     *    H3_REVERSAL  (bearish) → sell CE at strike nearest to H4
     *    L4_BREAKDOWN (bearish) → sell CE at strike nearest to L3
     *  </pre>
     *  Each trade leg is one Camarilla level "further" than the
     *  breakout/reversal level, giving the bet OTM cushion in the expected
     *  direction. */
    private synchronized boolean resolveSessionLegs() {
        String today = LocalDate.now(IST).toString();
        // Fetch levels first so the short-circuit can validate the persisted
        // strikes against the current OTM-aware rule (floor for PE-sellers,
        // ceil for CE-sellers). If any strike doesn't match, the persisted
        // state was written with the old round-to-nearest logic and must be
        // re-resolved — skip the short-circuit.
        CamarillaLevels lv = camarillaService.getLevels(NIFTY_SYMBOL);
        boolean persistedStrikesAreOtmAware = lv != null && strikesMatchOtmRule(lv);
        if (today.equals(state.sessionLegsDayKey)
            && !state.h4bSymbol.isBlank() && !state.l3rSymbol.isBlank()
            && !state.h3rSymbol.isBlank() && !state.l4bSymbol.isBlank()
            && persistedStrikesAreOtmAware) {
            // Already resolved earlier today with the current OTM logic.
            // Re-subscribe defensively — on a mid-day JVM restart the
            // symbols persist on State (via disk cache) but the Fyers WS
            // subscription set is empty until we call subscribeAdditional
            // again. Idempotent at the WS layer, cheap insurance against
            // any restart gap.
            ensureSessionLegsSubscribed();
            return true;
        }
        if (lv == null) return false;
        // OTM-aware, per-setup strike resolution. Each setup's strike is one
        // Camarilla level FURTHER OTM than its own trigger level:
        //   H4_BREAKOUT  (bullish, PE)  → floor(H3)  — deep OTM PE below the breakout
        //   L3_REVERSAL  (bullish, PE)  → floor(L4)  — deep OTM PE below the reversal
        //   H3_REVERSAL  (bearish, CE)  → ceil (H4)  — deep OTM CE above the reversal
        //   L4_BREAKDOWN (bearish, CE)  → ceil (L3)  — deep OTM CE above the breakdown
        var h4bRow = atmSelector.resolveOtmStrikeAtLevel(lv.h3(), "PE");
        var l3rRow = atmSelector.resolveOtmStrikeAtLevel(lv.l4(), "PE");
        var h3rRow = atmSelector.resolveOtmStrikeAtLevel(lv.h4(), "CE");
        var l4bRow = atmSelector.resolveOtmStrikeAtLevel(lv.l3(), "CE");
        if (h4bRow == null || l3rRow == null || h3rRow == null || l4bRow == null) {
            log.debug("[Camarilla] session legs deferred — one or more chain rows null "
                + "(H4B={}, L3R={}, H3R={}, L4B={})",
                h4bRow, l3rRow, h3rRow, l4bRow);
            return false;
        }
        state.h4bSymbol = h4bRow.peSymbol(); state.h4bStrike = h4bRow.resolvedStrike(); state.h4bRefLtp = h4bRow.peLtp();
        state.l3rSymbol = l3rRow.peSymbol(); state.l3rStrike = l3rRow.resolvedStrike(); state.l3rRefLtp = l3rRow.peLtp();
        state.h3rSymbol = h3rRow.ceSymbol(); state.h3rStrike = h3rRow.resolvedStrike(); state.h3rRefLtp = h3rRow.ceLtp();
        state.l4bSymbol = l4bRow.ceSymbol(); state.l4bStrike = l4bRow.resolvedStrike(); state.l4bRefLtp = l4bRow.ceLtp();
        // Fyers' option-chain payload commonly serves 0 LTPs on holidays for
        // illiquid OTM strikes. Fall back to /data/quotes which returns the
        // last-quoted price per symbol (Friday's close on a holiday Monday).
        // Triggered when ANY of the four chain LTPs is 0.
        if (state.h4bRefLtp <= 0 || state.l3rRefLtp <= 0
            || state.h3rRefLtp <= 0 || state.l4bRefLtp <= 0) {
            backfillRefLtpsFromQuotes();
        }
        state.sessionLegsDayKey = today;
        ensureSessionLegsSubscribed();
        // Split into two log lines — reversal legs and breakout legs — so
        // the operator can visually compare within-family strikes quickly.
        event("[INFO]", "Session",
            "NIFTY Reversal Legs Resolved — L3R PE " + state.l3rStrike
            + " | H3R CE " + state.h3rStrike);
        event("[INFO]", "Session",
            "NIFTY Breakout Legs Resolved — H4B PE " + state.h4bStrike
            + " | L4B CE " + state.l4bStrike);
        saveToDisk();
        return true;
    }


    /** Verify each persisted session-leg strike matches the current OTM-aware
     *  rule (floor for PE-sellers, ceil for CE-sellers). Used by
     *  {@link #resolveSessionLegs()} to detect state files persisted with the
     *  older round-to-nearest logic and force a re-resolve — otherwise the
     *  short-circuit fires on today's date + all six symbols present and the
     *  stale strikes stay in state forever. Returns false when the levels
     *  cache isn't loaded (defensive — caller will fall through and re-resolve). */
    private boolean strikesMatchOtmRule(CamarillaLevels lv) {
        if (lv == null) return false;
        // Each setup's strike is one Camarilla level FURTHER OTM than its own
        // trigger level (H4B→H3, L3R→L4, H3R→H4, L4B→L3). Keep in sync with
        // resolveSessionLegs() — this validator forces a re-resolve when a
        // persisted state file was written under an older mapping.
        long expH4B = (long) Math.floor(lv.h3() / STRIKE_STEP) * STRIKE_STEP;
        long expL3R = (long) Math.floor(lv.l4() / STRIKE_STEP) * STRIKE_STEP;
        long expH3R = (long) Math.ceil (lv.h4() / STRIKE_STEP) * STRIKE_STEP;
        long expL4B = (long) Math.ceil (lv.l3() / STRIKE_STEP) * STRIKE_STEP;
        return state.h4bStrike == expH4B
            && state.l3rStrike == expL3R
            && state.h3rStrike == expH3R
            && state.l4bStrike == expL4B;
    }

    /** Pull last-quoted prices from Fyers {@code /data/quotes} via
     *  {@link CamarillaService#fetchLastQuotedLtps(String)} for any of the
     *  four session legs whose chain-derived refLtp came back as 0. On a
     *  holiday or pre-market the chain endpoint can serve 0 LTPs even though
     *  the quotes endpoint still returns the prior session's close. */
    private void backfillRefLtpsFromQuotes() {
        java.util.LinkedHashSet<String> needed = new java.util.LinkedHashSet<>();
        if (state.h4bRefLtp <= 0 && !state.h4bSymbol.isBlank()) needed.add(state.h4bSymbol);
        if (state.l3rRefLtp <= 0 && !state.l3rSymbol.isBlank()) needed.add(state.l3rSymbol);
        if (state.h3rRefLtp <= 0 && !state.h3rSymbol.isBlank()) needed.add(state.h3rSymbol);
        if (state.l4bRefLtp <= 0 && !state.l4bSymbol.isBlank()) needed.add(state.l4bSymbol);
        if (needed.isEmpty()) return;
        java.util.Map<String, Double> ltpBySymbol = camarillaService.fetchLastQuotedLtps(String.join(",", needed));
        if (ltpBySymbol.isEmpty()) return;
        if (state.h4bRefLtp <= 0 && ltpBySymbol.containsKey(state.h4bSymbol)) state.h4bRefLtp = ltpBySymbol.get(state.h4bSymbol);
        if (state.l3rRefLtp <= 0 && ltpBySymbol.containsKey(state.l3rSymbol)) state.l3rRefLtp = ltpBySymbol.get(state.l3rSymbol);
        if (state.h3rRefLtp <= 0 && ltpBySymbol.containsKey(state.h3rSymbol)) state.h3rRefLtp = ltpBySymbol.get(state.h3rSymbol);
        if (state.l4bRefLtp <= 0 && ltpBySymbol.containsKey(state.l4bSymbol)) state.l4bRefLtp = ltpBySymbol.get(state.l4bSymbol);
        log.info("[Camarilla] session legs ref LTPs backfilled from /data/quotes — H4B={}, L3R={}, H3R={}, L4B={}",
            state.h4bRefLtp, state.l3rRefLtp, state.h3rRefLtp, state.l4bRefLtp);
    }

    /** Idempotent re-subscribe of the four current session legs. Safe to call
     *  on every resolveSessionLegs() invocation — backed by the Set-add
     *  contract of {@code MarketDataService.subscribeAdditional}. Critical for
     *  closing the mid-day-restart gap where State carries the symbols on
     *  disk but the Fyers WS subscription set boots empty. */
    private void ensureSessionLegsSubscribed() {
        java.util.List<String> legs = new java.util.ArrayList<>(4);
        if (state.h4bSymbol != null && !state.h4bSymbol.isBlank()) legs.add(state.h4bSymbol);
        if (state.l3rSymbol != null && !state.l3rSymbol.isBlank()) legs.add(state.l3rSymbol);
        if (state.h3rSymbol != null && !state.h3rSymbol.isBlank()) legs.add(state.h3rSymbol);
        if (state.l4bSymbol != null && !state.l4bSymbol.isBlank()) legs.add(state.l4bSymbol);
        if (legs.isEmpty()) return;
        try { marketDataService.subscribeAdditional(legs); }
        catch (Exception ignored) {}
    }

    /** Unsubscribe yesterday's four session legs. Safe to call multiple times
     *  — only legs without an open position get released. Triggered by the
     *  daily reset cron so today's resolve can subscribe fresh symbols
     *  (helpful when NIFTY weekly expiry rolls and the chain emits new
     *  symbol names). */
    private synchronized void releaseSessionLegs() {
        // Only release a leg if NO open position references it (composite-key
        // world: walk values() and check each Position.symbol).
        java.util.Set<String> openSymbols = new java.util.HashSet<>();
        for (Position p : state.openPositions.values()) {
            if (p != null && p.symbol != null) openSymbols.add(p.symbol);
        }
        java.util.List<String> legs = new java.util.ArrayList<>(4);
        for (String sym : new String[] {state.h4bSymbol, state.l3rSymbol, state.h3rSymbol, state.l4bSymbol}) {
            if (sym != null && !sym.isBlank() && !openSymbols.contains(sym)) {
                legs.add(sym);
            }
        }
        if (!legs.isEmpty()) {
            try { marketDataService.unsubscribeAdditional(legs); }
            catch (Exception ignored) {}
        }
        state.h4bSymbol = "";
        state.l3rSymbol = "";
        state.h3rSymbol = "";
        state.l4bSymbol = "";
        state.h4bStrike = 0;
        state.l3rStrike = 0;
        state.h3rStrike = 0;
        state.l4bStrike = 0;
        state.sessionLegsDayKey = "";
    }

    /** Look up the pre-resolved option symbol for a given setup. Returns
     *  empty string when the legs haven't been resolved yet (caller should
     *  log + skip). */
    private String legSymbolFor(ActiveSetup setup) {
        return legSymbolFor(setup, InstrumentConfig.NIFTY);
    }

    /** Per-instrument leg lookup. NIFTY-only after BankNifty retirement,
     *  but the {@code ic} parameter is retained for signature stability. */
    private String legSymbolFor(ActiveSetup setup, InstrumentConfig ic) {
        if (setup == null) return "";
        return switch (setup) {
            case H4_BREAKOUT  -> state.h4bSymbol;
            case L3_REVERSAL  -> state.l3rSymbol;
            case H3_REVERSAL  -> state.h3rSymbol;
            case L4_BREAKDOWN -> state.l4bSymbol;
            default            -> "";
        };
    }

    /** Look up the resolved strike for a given setup. */
    private long strikeFor(ActiveSetup setup) {
        return strikeFor(setup, InstrumentConfig.NIFTY);
    }

    /** Per-instrument strike lookup. NIFTY-only after BankNifty retirement. */
    private long strikeFor(ActiveSetup setup, InstrumentConfig ic) {
        if (setup == null) return 0;
        return switch (setup) {
            case H4_BREAKOUT  -> state.h4bStrike;
            case L3_REVERSAL  -> state.l3rStrike;
            case H3_REVERSAL  -> state.h3rStrike;
            case L4_BREAKDOWN -> state.l4bStrike;
            default            -> 0;
        };
    }

    /** Scheduled retry — runs every 30 seconds until today's four legs are
     *  resolved. No clock gate; runs from boot until {@code sessionLegsDayKey
     *  == today}. Idempotent — once resolved, becomes a cheap no-op. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void retrySessionLegsIfNeeded() {
        try { resolveSessionLegs(); }
        catch (Exception e) { log.warn("[Camarilla] NIFTY session-legs retry failed: {}", e.getMessage()); }
    }

    private static PendingConfirmation mkConfirmation(ActiveSetup setup, Candle c, double target) {
        PendingConfirmation pc = new PendingConfirmation();
        pc.setup       = setup;
        pc.barStartMs  = c.startMillis();
        pc.confirmHigh = c.high();
        pc.confirmLow  = c.low();
        pc.targetLevel = target;
        return pc;
    }

    /** Seed a freshly-detected confirmation candle. Every geometric
     *  confirmation arms the tick trigger — no STRONG/WEAK classification.
     *  Applies target buffer, checks the projected R:R floor at the
     *  optimistic entry, and emits a log line naming the buffered trigger
     *  price the next spot tick must reach for {@code fire()}. */
    private boolean classifyAndSeed(PendingConfirmation fresh, Candle c, CamarillaLevels lv, InstrumentConfig ic) {
        boolean bullish = isBullishBet(fresh.setup);

        var atrSvc = niftyAtrProvider == null ? null : niftyAtrProvider.getIfAvailable();
        Double atr = atrSvc == null ? null : atrSvc.currentAtr();
        double atrVal = (atr != null && atr > 0) ? atr : 0;

        // Target buffer — pull the target IN toward the entry by ATR × mult.
        // Bullish: target sits ABOVE entry → subtract (magnetically stop just
        // short of the Camarilla level). Bearish: target sits BELOW entry →
        // add. Muted by ATR unavailability (buffer=0). Applied BEFORE the
        // R:R gate below so the projection uses the actual target the fire
        // will chase. Setting to 0 in Settings disables the buffer.
        double tgtBufMult = Math.max(0, riskSettings.getCamarillaTargetBufferAtrMult());
        if (atrVal > 0 && tgtBufMult > 0) {
            double tgtBuf = atrVal * tgtBufMult;
            double origTarget = fresh.targetLevel;
            fresh.targetLevel = bullish
                ? fresh.targetLevel - tgtBuf
                : fresh.targetLevel + tgtBuf;
            event("[INFO]", "Setup",
                "[" + ic.name() + "] " + fresh.setup + " target buffered "
                + round2(origTarget) + " → " + round2(fresh.targetLevel)
                + " (ATR×" + round2(tgtBufMult) + " = " + round2(tgtBuf) + ")");
        }

        // Projected R:R gate at the OPTIMISTIC entry — the buffered tick-fire
        // price (confirmHigh + ATR×triggerMult for bullish, confirmLow −
        // ATR×triggerMult for bearish). Any actual fire lands at an
        // equal-or-worse entry, so R:R at the projected entry is an upper
        // bound on any real fire R:R.
        double minRR = riskSettings.getCamarillaMinRRRatio();
        if (minRR > 0 && atrVal > 0) {
            double triggerBuf = atrVal * Math.max(0, riskSettings.getCamarillaTriggerAtrMult());
            double slBuf      = atrVal * Math.max(0, riskSettings.getCamarillaDirectionalSlBufferAtrMult());
            double optEntry   = bullish
                ? fresh.confirmHigh + triggerBuf
                : fresh.confirmLow  - triggerBuf;
            double sl         = bullish
                ? fresh.confirmLow  - slBuf
                : fresh.confirmHigh + slBuf;
            double reward = Math.abs(fresh.targetLevel - optEntry);
            double risk   = Math.abs(optEntry - sl);
            if (risk > 0) {
                double rr = reward / risk;
                if (rr < minRR) {
                    event("[WARNING]", "Setup",
                        "[" + ic.name() + "] " + fresh.setup + " rejected — projected R:R "
                        + round2(rr) + " < floor " + round2(minRR)
                        + " (optEntry " + round2(optEntry)
                        + ", TGT " + round2(fresh.targetLevel)
                        + ", SL " + round2(sl) + ")");
                    return false;
                }
            }
        }

        double triggerMult  = Math.max(0, riskSettings.getCamarillaTriggerAtrMult());
        double triggerDelta = atrVal * triggerMult;
        double triggerPrice = bullish
            ? fresh.confirmHigh + triggerDelta
            : fresh.confirmLow  - triggerDelta;
        String op    = bullish ? ">=" : "<=";
        String extLbl = bullish ? "confirmHigh " : "confirmLow ";
        double extVal = bullish ? fresh.confirmHigh : fresh.confirmLow;
        String opSign = bullish ? " + " : " − ";
        event("[INFO]", "Setup",
            "[" + ic.name() + "] " + fresh.setup
            + " — will fire when spot " + op + " " + round2(triggerPrice)
            + " (" + extLbl + round2(extVal) + opSign
            + "ATR×" + round2(triggerMult) + " buffer " + round2(triggerDelta) + ")");
        return true;
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
            // v2 positions: futures-distance proxy (entryFutures vs slFutures),
            // scaled by ATM_DELTA so the projection reflects actual option premium
            // loss rather than the raw 1:1 spot move. v1 positions: option-price
            // distance (entryPrice vs slLevel) — no delta scaling needed since
            // the math is already in option-premium space.
            boolean v2 = p.triggerSymbol != null && !p.triggerSymbol.isBlank()
                       && p.entryFutures > 0 && !Double.isNaN(p.slFutures);
            double perShare;
            if (v2) {
                perShare = Math.abs(p.slFutures - p.entryFutures) * ATM_DELTA;
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
                      double targetFutures, double slFutures, Candle entryCandle,
                      long lockedAtm) {
        // Legacy default: assume NIFTY when no InstrumentConfig is provided.
        fire(triggerSymbol, setup, targetFutures, slFutures, entryCandle, lockedAtm,
             InstrumentConfig.NIFTY);
    }

    private void fire(String triggerSymbol, ActiveSetup setup,
                      double targetFutures, double slFutures, Candle entryCandle,
                      long lockedAtm, InstrumentConfig ic) {
        boolean shortSetup = isShortSetup(setup);
        if (!shortSetup) return;   // v2 is sell-only by design
        boolean bullishBet = isBullishBet(setup);

        // ── Pick the trade leg from the pre-resolved session map ──
        // Each setup has a session-static OTM leg resolved at boot from today's
        // Camarilla levels. No per-trade strike math, no on-demand subscribe —
        // the leg has been live on the WebSocket since boot.
        String optionSym = legSymbolFor(setup, ic);
        long   strike    = strikeFor(setup, ic);
        if (optionSym == null || optionSym.isBlank()) {
            event("[ERROR]", "AUTO ENTRY", setup + " — session leg not resolved yet, skipping");
            return;
        }
        // Avoid double-entry of the SAME setup on the same option leg. A
        // MANUAL position on the same Fyers symbol coexists fine — they live
        // under distinct composite keys.
        if (state.openPositions.containsKey(posKey(setup, optionSym))) return;

        // ── Momentum (NIFTY RSI-14) gate ──
        // Each directional setup requires NIFTY RSI to sit inside a band that
        // matches the setup's thesis:
        //   H4_BREAKOUT  (bullish)  → 50 < RSI < 70   trending up but not exhausted
        //   L4_BREAKDOWN (bearish)  → 30 < RSI < 50   trending down but not exhausted
        //   L3_REVERSAL  (bullish)  → RSI > 40         no bearish weakness ruling out an upside bounce
        //   H3_REVERSAL  (bearish)  → RSI < 60         no bullish strength ruling out a downside fade
        // Breakouts skip when RSI is overbought/oversold (≥ 80 / ≤ 20) since
        // that's typically a late, exhaustion-prone entry. Reversals only need
        // the FAR side of neutral cleared — the near side is fine because the
        // thesis is a mean-reversion move IN that direction.
        // RSI unavailable (Wilder not seeded, NIFTY LTP missing) → pass through.
        // Toggle: camarillaMomentumCheckEnabled (Settings → Camarilla pane).
        if (riskSettings.isCamarillaMomentumCheckEnabled()) {
            try {
                Double v = null;
                var rsi = niftyRsiProvider == null ? null : niftyRsiProvider.getIfAvailable();
                if (rsi != null) v = rsi.currentRsi();
                if (v != null) {
                    double r = v;
                    boolean ok = switch (setup) {
                        case H4_BREAKOUT  -> r > 50 && r < 70;
                        case L4_BREAKDOWN -> r < 50 && r > 30;
                        case L3_REVERSAL  -> r > 40;
                        case H3_REVERSAL  -> r < 60;
                        default            -> true;
                    };
                    if (!ok) {
                        event("[WARNING]", "Momentum", "[" + ic.name() + "] " + setup + " skip — RSI " + round2(r));
                        return;
                    }
                }
            } catch (Exception ignored) {}
        }

        // ── Risk gates: consumed > maxRisk locks out the day ──
        double maxRisk = riskSettings.getPortfolioMaxDailyLoss();
        if (maxRisk > 0 && consumedRiskNow() > maxRisk) {
            event("[ERROR]", "Risk", "lockout — consumed ₹"
                + round2(consumedRiskNow()) + " > ₹" + round2(maxRisk));
            state.dailyLossLockout = true;
            saveToDisk();
            return;
        }

        // ── Futures-price-based R:R floor (toggle) ──
        // For futures-driven entries the candle close approximates the entry futures price.
        // reward = |entryFut − targetFut|, risk = |slFut − entryFut|.
        // Tick-triggered fires (from tryFireTriggeredPending) pass null for
        // entryCandle — the live-LTP fallback below overwrites the 0 with
        // the current spot, so no zero propagates into R:R or exposedRisk.
        double entryFutures = entryCandle != null ? entryCandle.close() : 0;
        try {
            double live = marketDataService.getLtp(triggerSymbol);
            if (live > 0) entryFutures = live;
        } catch (Exception ignored) {}
        double minRatio = riskSettings.getCamarillaMinRRRatio();
        if (minRatio > 0) {
            double reward = Math.abs(entryFutures - targetFutures);
            double risk   = Math.abs(slFutures - entryFutures);
            if (risk > 0 && reward < risk * minRatio) {
                event("[WARNING]", "Sizing", setup + " skip — R:R "
                    + round2(reward / risk) + " < " + round2(minRatio));
                return;
            }
        }

        // ── Project exposed-risk after this entry; block if it would exceed maxRisk ──
        // Per-position futures-equivalent risk = |entryFut − slFut| × qty.
        int fullLots = riskSettings.getCamarillaLotsPerLeg();
        int qty = fullLots * ic.lotSize();
        // Project the proposed trade's option-premium loss at SL: spot SL distance
        // × ATM_DELTA (≈ 0.5) × qty. A raw 1:1 spot×qty projection would overstate
        // option loss by ~2× and gate trades that wouldn't actually breach maxRisk.
        double perShareRisk = Math.abs(entryFutures - slFutures) * ATM_DELTA;
        double newExposureDelta = perShareRisk * qty;
        if (maxRisk > 0 && (exposedRiskNow() + newExposureDelta) > maxRisk) {
            // One retry at 50% capacity (floored to a whole-lot count, min 1 lot).
            // Skip only if the halved size still won't fit.
            int halfLots = Math.max(1, fullLots / 2);
            int halfQty  = halfLots * ic.lotSize();
            double halfExposureDelta = perShareRisk * halfQty;
            if (halfLots < fullLots
                    && (exposedRiskNow() + halfExposureDelta) <= maxRisk) {
                event("[WARNING]", "Risk", "[" + ic.name() + "] " + setup + " downsized "
                    + fullLots + "→" + halfLots + " lots — full exposure ₹"
                    + round2(exposedRiskNow() + newExposureDelta) + " > ₹" + round2(maxRisk));
                qty = halfQty;
                newExposureDelta = halfExposureDelta;
            } else {
                event("[WARNING]", "Risk", "[" + ic.name() + "] " + setup + " skip — exposed ₹"
                    + round2(exposedRiskNow() + newExposureDelta) + " > ₹" + round2(maxRisk));
                return;
            }
        }

        // ── Place the SELL (SHORT) order on the option leg ──
        String productType = riskSettings.getCamarillaOrderType();
        int orderSide = -1;
        double optionEntryLtp = 0;
        try { optionEntryLtp = marketDataService.getLtp(optionSym); }
        catch (Exception ignored) {}
        // optionEntryLtp may be 0 if the option hasn't ticked yet — leave it 0, the fill
        // resolver will overwrite with the real broker fill price.

        log.info("[Camarilla v2] {} [{}] fired — sell {} qty={} (triggerFut={}, entryFut={}, target={}, sl={})",
            setup, ic.name(), optionSym, qty, triggerSymbol, entryFutures, targetFutures, slFutures);
        event("[SUCCESS]", "AUTO ENTRY", "[" + ic.name() + "] sell " + optionSym + " ×" + (qty / ic.lotSize())
            + "L (TGT " + round2(targetFutures) + ", SL " + round2(slFutures) + ")");

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
        // Back-compat: lockedAtm field on Position kept for state-file
        // deserialisation of older runs. Still stored on the position so any
        // legacy consumer that reads it sees the trade's strike.
        p.lockedAtm = strike;
        state.openPositions.put(posKey(p), p);
        state.tradesToday++;
        // Re-subscribe candle listener on the option symbol too — needed so the existing
        // candle-close skip-if-open path correctly short-circuits if any leftover code
        // routes option candles through onCandleClose.
        final String optSym = optionSym;
        candleAggregator.subscribe(optSym, c -> onCandleClose(optSym, c));
        saveToDisk();
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
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                closePosition(p, "TIMED_EXIT");
            }
        }
    }

    /** Close a single open position via market exit. Returns true when the close action was
     *  attempted (the order may still fail at the broker).
     *
     *  <p>Back-compat shim: when only a symbol is known (legacy callers like
     *  the squareoff cron and the Options Scalper Terminal), resolve the first
     *  position whose {@code p.symbol} matches and route through the
     *  Position-aware overload.
     *
     *  <p><strong>WARNING</strong> — this overload is AMBIGUOUS when both a
     *  MANUAL and an AUTO position coexist on the same Fyers option symbol
     *  (composite-key openPositions from Commit HH allows that). The
     *  first-match iteration order may pick the wrong Position. Prefer
     *  {@link #closePosition(Position, String)} with an explicit reference
     *  whenever the caller has one. Only use this overload when the caller
     *  genuinely has only a symbol string and there's no ambiguity risk. */
    private boolean closePosition(String symbol, String reason) {
        if (symbol == null || symbol.isBlank()) return false;
        Position p = null;
        for (Position pp : state.openPositions.values()) {
            if (pp != null && symbol.equals(pp.symbol)) { p = pp; break; }
        }
        return p != null && closePosition(p, reason);
    }

    /** Close a specific Position object. The Position MUST already be present
     *  in {@code state.openPositions} under its composite key {@code posKey(p)} —
     *  a MANUAL position and a bot-managed directional fire can coexist on
     *  the same Fyers symbol, each tracked independently with its own
     *  SL / target / qty. */
    private boolean closePosition(Position p, String reason) {
        if (p == null) return false;
        String symbol = p.symbol;
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
            reason.equals("SL_HIT") ? 1 : 0, closedAtMillis, p.openMillis, p.entryOiBias,
            p.entryPrice, exitPrice);

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

        state.openPositions.remove(posKey(p));

        // Stop subscribing to this symbol's candles ONLY when there are no
        // remaining open positions on this exact symbol AND it's not in the
        // V2 watchlist. A Fyers symbol shared by both a MANUAL position and
        // a bot-managed fire must keep its candle feed alive while ANY of
        // them remains open.
        boolean stillUsed = false;
        for (Position pp : state.openPositions.values()) {
            if (pp != null && symbol.equals(pp.symbol)) { stillUsed = true; break; }
        }
        if (!stillUsed && !state.symbolRole.containsKey(symbol)) {
            candleAggregator.unsubscribe(symbol);
        }

        // v2 two-candle entry — clear any pending confirmations when a trade
        // closes on TARGET_HIT or TIMED_EXIT.
        //   TARGET_HIT — market moved through the thesis; old pendings are stale.
        //   TIMED_EXIT — squareoff fired; the session window for this thesis is
        //                done, the pending shouldn't seed a new trade post-cutoff.
        // SL_HIT, RISK_BREACH, and MANUAL closes do NOT clear pendings — those
        // exits don't validate the prior thesis and the opposite-side setup may
        // still be relevant.
        if ("TARGET_HIT".equals(reason) || "TIMED_EXIT".equals(reason)) {
            boolean had = state.pendingBullish != null || state.pendingBearish != null;
            state.pendingBullish = null;
            state.pendingBearish = null;
            if (had) {
                event("[INFO]", "Setup",
                    "pending confirmations reset — trade closed on " + reason);
            }
        }

        saveToDisk();
        return true;
    }

    private void persistTradeRow(String strategyId, String symbol, String setup, String reason, int qty,
                                 double gross, double charges, double net, int slHits,
                                 long closedAtMillis, long openedAtMillis, String entryOiBias,
                                 double entryPrice, double exitPrice) {
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
            row.setInstrument(instrumentFromSymbol(symbol));
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
            log.warn("[Camarilla] persist trade failed: {}", e.getMessage());
        }
    }

    /** Classify a Fyers option leg symbol into its underlying instrument for analytics.
     *  BANKNIFTY prefix wins because "NIFTY" is a substring of "NIFTYBANK". */
    private static String instrumentFromSymbol(String symbol) {
        if (symbol == null) return null;
        String s = symbol.toUpperCase();
        if (s.contains("BANKNIFTY") || s.contains("NIFTYBANK")) return "BANKNIFTY";
        if (s.contains("NIFTY")) return "NIFTY";
        return null;
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
        java.util.Set<String> uniqSymbols = new java.util.HashSet<>();
        for (Position p : state.openPositions.values()) {
            if (p != null && p.symbol != null) uniqSymbols.add(p.symbol);
        }
        for (String sym : uniqSymbols) {
            candleAggregator.unsubscribe(sym);
        }
        state.openPositions.clear();
        state.symbolRole.clear();
        // v2 two-candle entry — drop any pending confirmations on day rollover / reset.
        state.pendingBullish = null;
        state.pendingBearish = null;
        // Release yesterday's session-static OTM legs so the resolver picks up
        // today's fresh Camarilla levels (and any fresh weekly-expiry chain
        // symbols that rolled overnight) on its next tick.
        releaseSessionLegs();
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
            java.util.Set<String> uniqSymbolsRoll = new java.util.HashSet<>();
            for (Position p : state.openPositions.values()) {
                if (p != null && p.symbol != null) uniqSymbolsRoll.add(p.symbol);
            }
            for (String sym : uniqSymbolsRoll) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            // V2: reset the watchlist roles so the new day's first ATM resolution rebuilds them.
            state.symbolRole.clear();
            // v2 two-candle entry — drop any pending confirmations on day rollover / reset.
        state.pendingBullish = null;
        state.pendingBearish = null;
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
        // Live ATM (round current NIFTY spot to STRIKE_STEP) — still used
        // below by the atmVwap chip block (NIFTY futures + ATM PUT/CALL
        // header chips). The standalone `currentAtm` payload field is gone;
        // the trade page no longer displays it.
        long liveAtm = 0;
        try {
            double spotLtp = marketDataService.getLtp(NIFTY_SYMBOL);
            if (spotLtp > 0) liveAtm = Math.round(spotLtp / (double) STRIKE_STEP) * STRIKE_STEP;
        } catch (Exception ignored) {}
        // v2 session-static OTM legs — one row per setup, all four resolved at
        // boot and subscribed all session. Trade page renders this as a
        // 4-row block (replaces the legacy dynamic ATM chip and the per-pending
        // LOCK chip). Emitted as an empty array until the four legs resolve.
        java.util.List<Map<String, Object>> setupLegs = new java.util.ArrayList<>(8);
        // NIFTY legs
        addSetupLegRow(setupLegs, "NIFTY", ActiveSetup.H4_BREAKOUT,  "BULLISH", "H4",
                       state.h4bStrike, state.h4bSymbol, "PE", state.h4bRefLtp);
        addSetupLegRow(setupLegs, "NIFTY", ActiveSetup.L3_REVERSAL,  "BULLISH", "L3",
                       state.l3rStrike, state.l3rSymbol, "PE", state.l3rRefLtp);
        addSetupLegRow(setupLegs, "NIFTY", ActiveSetup.H3_REVERSAL,  "BEARISH", "H3",
                       state.h3rStrike, state.h3rSymbol, "CE", state.h3rRefLtp);
        addSetupLegRow(setupLegs, "NIFTY", ActiveSetup.L4_BREAKDOWN, "BEARISH", "L4",
                       state.l4bStrike, state.l4bSymbol, "CE", state.l4bRefLtp);
        m.put("setupLegs", setupLegs);
        m.put("watchlistSize",     state.symbolRole.size());
        m.put("watchlistRoles",    new LinkedHashMap<>(state.symbolRole));

        // v2 header chips — the v1 VWAP-CE / VWAP-PE option-premium chips are replaced
        // by FUTURES-driven chips that reflect what the triggers actually read:
        //   • futSymbol  / futLtp  / futVwap — near-month NIFTY future feed
        //   • putSymbol  / putLtp           — ATM PUT (bullish trade leg)
        //   • callSymbol / callLtp          — ATM CALL (bearish trade leg)
        // The legacy {ceSymbol, peSymbol, ceVwap, peVwap} fields are still emitted
        // (zero-value) so v1-era frontends that haven't been rebuilt don't NPE on
        // missing keys — they'll just render '—' until the page is updated.
        Map<String, Object> vwap = new LinkedHashMap<>();
        String futSym = state.futuresSymbol == null ? "" : state.futuresSymbol;
        double futLtp = 0, futChange = 0, futChangePct = 0;
        if (!futSym.isBlank()) {
            try { futLtp        = marketDataService.getLtp(futSym); }              catch (Exception ignored) {}
            try { futChange     = marketDataService.getDisplayChange(futSym); }    catch (Exception ignored) {}
            try { futChangePct  = marketDataService.getDisplayChangePct(futSym); } catch (Exception ignored) {}
        }
        // Header chips for the LIVE ATM PUT + CALL — re-resolve every state event
        // so the chips drift naturally with NIFTY spot through the session.
        long atm = liveAtm;
        String putSym = "", callSym = "";
        double putLtp = 0, callLtp = 0;
        if (atm > 0) {
            BalancedAtmSelector.StrikeAtLevel atmRow = atmSelector.resolveStrikeAtLevel(atm);
            if (atmRow != null && atmRow.peSymbol() != null) putSym  = atmRow.peSymbol();
            if (atmRow != null && atmRow.ceSymbol() != null) callSym = atmRow.ceSymbol();
            if (!putSym.isBlank())  { try { putLtp  = marketDataService.getLtp(putSym);  } catch (Exception ignored) {} }
            if (!callSym.isBlank()) { try { callLtp = marketDataService.getLtp(callSym); } catch (Exception ignored) {} }
        }
        vwap.put("futSymbol",    futSym);
        vwap.put("futLtp",       round2(futLtp));
        vwap.put("futChange",    round2(futChange));
        vwap.put("futChangePct", round2(futChangePct));
        vwap.put("putSymbol",    putSym);
        vwap.put("putStrike",    atm);
        vwap.put("putLtp",       round2(putLtp));
        vwap.put("callSymbol",   callSym);
        vwap.put("callStrike",   atm);
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

        // v2 — only NIFTY spot has meaningful Camarilla levels (option-strike levels
        // were a v1 concept). Open positions are option symbols whose target/SL are
        // ALREADY spot prices, so per-option levels aren't useful here either.
        // Emit only the trigger symbol's levels — the trade.html LTP tooltip uses this.
        Map<String, CamarillaLevels> perSymbolLevels = new LinkedHashMap<>();
        if (futSym != null && !futSym.isBlank()) {
            CamarillaLevels lv = camarillaService.getLevels(futSym);
            if (lv != null) perSymbolLevels.put(futSym, lv);
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
        try {
            com.rydytrader.autotrader.service.NiftyAtrService atrSvc = niftyAtrProvider == null ? null
                : niftyAtrProvider.getIfAvailable();
            Double atr = atrSvc == null ? null : atrSvc.currentAtr();
            if (atr != null) m.put("niftyAtr5m", atr);
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
        /** v2 two-candle entry — pending bullish setup confirmation candle, waiting
         *  for a trigger bar (green close above {@code confirmHigh}) to fire the
         *  trade, OR for an invalidation bar (close below {@code confirmLow}) to
         *  nullify it. Replaced when a new bullish confirmation arrives; cleared
         *  on day rollover. null means no pending bullish setup. */
        public PendingConfirmation pendingBullish;
        /** v2 two-candle entry — pending bearish setup confirmation. Symmetric to
         *  pendingBullish. Trigger = red close below {@code confirmLow}; invalidation =
         *  close above {@code confirmHigh}. */
        public PendingConfirmation pendingBearish;

        // ── V2 watchlist ──────────────────────────────────────────────────────
        /** Current monitored 6-contract matrix (ATM, ±1 strike, CE+PE) → role mapping. */
        public Map<String, WatchRole> symbolRole = new ConcurrentHashMap<>();

        // ── V2 session-static OTM legs (one per setup) ───────────────────────
        // Resolved at boot from today's Camarilla levels and subscribed for the
        // full session — no per-confirmation churn. fire() looks up the right
        // leg by setup type. Empty until the first successful resolveSessionLegs().
        public String h4bSymbol = "";   // H4_BREAKOUT  → sell PE near H4
        public String l3rSymbol = "";   // L3_REVERSAL  → sell PE near L3
        public String h3rSymbol = "";   // H3_REVERSAL  → sell CE near H3
        public String l4bSymbol = "";   // L4_BREAKDOWN → sell CE near L4
        public long   h4bStrike;
        public long   l3rStrike;
        public long   h3rStrike;
        public long   l4bStrike;
        /** Reference LTP per leg — captured from the option chain at resolve
         *  time. Used as a display fallback on holidays / pre-market when the
         *  WS feed isn't streaming (live getLtp returns 0). Reflects the
         *  chain's last-quoted price, i.e. yesterday's close on a holiday. */
        public double h4bRefLtp;
        public double l3rRefLtp;
        public double h3rRefLtp;
        public double l4bRefLtp;
        /** YYYY-MM-DD on which the four legs above were resolved. When this
         *  doesn't match today, the scheduled retry refreshes them. */
        public String sessionLegsDayKey = "";
    }

    /** v2 two-candle entry — a bar that met the confirmation geometry of one of the
     *  four setups, captured so a subsequent trigger bar (same-direction close
     *  through the relevant extreme) can fire the trade. SL is read from the
     *  confirmation bar's far extreme; target is the Camarilla level captured at
     *  confirmation time. */
    public static class PendingConfirmation {
        public ActiveSetup setup;
        public long   barStartMs;
        public double confirmHigh;
        public double confirmLow;
        public double targetLevel;
        /** v2 — ATM strike locked at the moment the confirmation candle was recorded.
         *  fire() uses this at trigger time instead of recomputing from live spot,
         *  so the strike we pre-subscribe is guaranteed to be the strike we trade.
         *  0 on legacy pendings from disk — fire() falls back to live ATM in that case. */
        public long lockedAtm;
        /** v2 — both CE and PE Fyers symbols for the locked-ATM strike, captured at
         *  confirmation time. Stored so the watcher can show LTP/VWAP for both legs
         *  in the header chip and so invalidation/expiry knows what to unsubscribe. */
        public String ceSymbol;
        public String peSymbol;
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
        /** v2 — locked ATM strike captured at confirmation time and carried
         *  through to this open position. Drives the "LOCK NNNN" chip on the
         *  trade page so the header stays visible from confirmation through
         *  position close, not just during the pending window. 0 for v1
         *  positions and any position not seeded from a v2 confirmation. */
        public long   lockedAtm;
        /** v2 — both leg symbols at the locked strike, retained on the open
         *  position so the chip can keep showing CE/PE LTP + VWAP through the
         *  full trade lifetime even after the pending is cleared by trigger. */
        public String ceSymbol = "";
        public String peSymbol = "";
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
            // One-time migration — move legacy ../store/data/ file to ../store/cache/.
            if (!Files.exists(p)) {
                Path legacy = Path.of(LEGACY_STATE_FILE);
                if (Files.exists(legacy)) {
                    java.io.File parent = p.toFile().getParentFile();
                    if (parent != null && !parent.exists()) parent.mkdirs();
                    Files.move(legacy, p);
                    log.info("[Camarilla] migrated {} → {}", legacy, p);
                } else {
                    return;
                }
            }
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.openPositions == null) state.openPositions = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null) state.recentEvents = new ArrayList<>();
                purgeRetiredStrangleEntries();
                migrateOpenPositionsKeyFormat();
            }
        } catch (IOException e) {
            log.warn("[Camarilla] failed to load state: {}", e.getMessage());
        }
    }

    /** State files written while the strangle was live may carry
     *  H4_STRANGLE / L4_STRANGLE setups in openPositions. The mapper is now
     *  configured to read unknown enum values as null, so these positions
     *  arrive with {@code setup == null}. Drop them here — the strangle is
     *  no longer managed and a null-setup entry would survive forever in
     *  the openPositions map otherwise. Idempotent. */
    private void purgeRetiredStrangleEntries() {
        if (state.openPositions == null || state.openPositions.isEmpty()) return;
        int before = state.openPositions.size();
        state.openPositions.values().removeIf(p -> p == null || p.setup == null);
        int after = state.openPositions.size();
        if (after != before) {
            log.info("[Camarilla] purged {} retired-strangle entries from openPositions",
                before - after);
        }
    }

    /** State files written before the openPositions composite-key change
     *  keyed by raw symbol; the new format is composite {@code "setup|symbol"}.
     *  Walk the map once after load — any key without a {@code |} is rebuilt
     *  under its computed posKey. Idempotent. */
    private void migrateOpenPositionsKeyFormat() {
        if (state.openPositions == null || state.openPositions.isEmpty()) return;
        boolean anyOld = false;
        for (String key : state.openPositions.keySet()) {
            if (key == null || key.indexOf('|') < 0) { anyOld = true; break; }
        }
        if (!anyOld) return;
        Map<String, Position> migrated = new ConcurrentHashMap<>();
        for (Map.Entry<String, Position> e : state.openPositions.entrySet()) {
            Position pos = e.getValue();
            if (pos == null) continue;
            migrated.put(posKey(pos), pos);
        }
        state.openPositions = migrated;
        log.info("[Camarilla] migrated {} openPositions entries to composite keys",
            migrated.size());
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

    /** Reward-to-risk ratio for an event-log line — {@code reward / risk} in spot points.
     *  Returns NaN when risk is zero (caller should render as "—"). */
    private static double computeRR(double entry, double target, double sl) {
        double risk = Math.abs(sl - entry);
        if (risk <= 0) return Double.NaN;
        double reward = Math.abs(entry - target);
        return reward / risk;
    }

    /** Defensive LTP lookup — returns 0 on null/blank symbol or any LTP cache miss
     *  so the dashboard payload never throws on pre-tick option legs. */
    private double safeLtp(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getLtp(sym)); }
        catch (Exception e) { return 0; }
    }

    /** Defensive VWAP lookup — same contract as {@link #safeLtp(String)} but reads
     *  the session VWAP cache. Returns 0 until at least one full-mode tick lands
     *  for the symbol in the current trading session. */
    private double safeVwap(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getVwap(sym)); }
        catch (Exception e) { return 0; }
    }

    /** Append one row to the {@code setupLegs} dashboard array — used for the
     *  four session-static OTM legs. Live LTP comes from the WS tick cache;
     *  when that's 0 (holiday, pre-market, fresh boot before first tick), we
     *  fall back to {@code refLtp} — the chain-quoted price captured at
     *  resolve time, which on a non-trading day reflects yesterday's last
     *  close. {@code ltpStale = true} signals the UI to render the value
     *  muted so the operator knows it's not live. */
    private void addSetupLegRow(java.util.List<Map<String, Object>> rows,
                                ActiveSetup setup, String dir, String levelRef,
                                long strike, String symbol, String side,
                                double refLtp) {
        addSetupLegRow(rows, "NIFTY", setup, dir, levelRef, strike, symbol, side, refLtp);
    }

    /** Per-instrument overload — tags the emitted row with the instrument
     *  name so the Levels modal UI can group into NIFTY vs BankNifty
     *  columns. */
    private void addSetupLegRow(java.util.List<Map<String, Object>> rows,
                                String instrument,
                                ActiveSetup setup, String dir, String levelRef,
                                long strike, String symbol, String side,
                                double refLtp) {
        double live = safeLtp(symbol);
        boolean stale = live <= 0 && refLtp > 0;
        double ltp   = live > 0 ? live : round2(refLtp);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("instrument", instrument);
        row.put("setup",    setup == null ? "" : setup.toString());
        row.put("setupTag", setup == null ? "" : shortSetupTag(setup));
        row.put("dir",      dir);
        row.put("levelRef", levelRef);
        row.put("strike",   strike);
        row.put("symbol",   symbol == null ? "" : symbol);
        row.put("side",     side);
        row.put("ltp",      ltp);
        row.put("ltpStale", stale);
        row.put("vwap",     safeVwap(symbol));
        rows.add(row);
    }

    /** Short 3-char tag for the trade-page setupLegs block. */
    private static String shortSetupTag(ActiveSetup s) {
        return switch (s) {
            case H4_BREAKOUT  -> "H4B";
            case L3_REVERSAL  -> "L3R";
            case H3_REVERSAL  -> "H3R";
            case L4_BREAKDOWN -> "L4B";
            default            -> s.toString();
        };
    }

    /** Compact rendering of a Fyers option symbol for the event log:
     *  {@code NSE:NIFTY2562624650CE} → {@code 24650CE}. Falls back to the symbol's
     *  last 8 chars when it doesn't match the expected suffix pattern. */
    private static String shortSym(String s) {
        if (s == null || s.isBlank()) return "";
        if (s.endsWith("CE") || s.endsWith("PE")) {
            int len = s.length();
            // Strike is the last 4–5 digits before CE/PE
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
