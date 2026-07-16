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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Camarilla option-premium strategy.
 *
 * <p>Two NIFTY option legs are picked at session start:
 * <ul>
 *   <li><b>H5 CE</b> — CE at the strike nearest to NIFTY spot's Camarilla H5.</li>
 *   <li><b>L5 PE</b> — PE at the strike nearest to NIFTY spot's Camarilla L5.</li>
 * </ul>
 * Each option is monitored against its OWN Camarilla pivots (computed from that option's
 * 10-day daily OHLC) on 3-min bar closes. Bearish setups only:
 * <ul>
 *   <li><b>L4_BREAKDOWN</b> — red bar wicked through the option's L4 and closed below.
 *       SL on entry = option's OWN L3.</li>
 *   <li><b>H3_REVERSAL</b> — red bar wicked through the option's H3 and closed below.
 *       SL on entry = option's OWN H4.</li>
 * </ul>
 * A confirmation bar arms a pending; the NEXT 3-min close below the confirmation's low
 * fires a SELL on that option leg. Broker-side SL is placed at the level; a 3-min-close
 * SL backup fires if broker miss. Timed squareoff flattens everything at the configured
 * cutoff. No target orders — mean-reversion / decay does the work.
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
    private static final String STATE_FILE = "../store/cache/camarilla-state.json";
    private static final String LEGACY_STATE_FILE = "../store/data/camarilla-state.json";
    /** NIFTY option lot size — 65 (post 2025 revision). */
    private static final int    LOT_SIZE = 65;
    /** NIFTY option premium tick size — the minimum tradable price. */
    private static final double OPTION_TICK_SIZE = 0.05;
    /** NIFTY strike interval — 50 points. */
    private static final long   STRIKE_STEP = 50L;
    private static final int    RECENT_EVENTS_LIMIT = 60;
    /** Bar length sourced from {@link com.rydytrader.autotrader.service.CandleAggregator#BUCKET_MINUTES}
     *  so this constant never drifts from the aggregator's actual cadence. */
    private static final long BAR_LENGTH_MS =
        com.rydytrader.autotrader.service.CandleAggregator.BUCKET_MINUTES * 60_000L;
    /** Max bars a pending confirmation may sit without a trigger. 3 × 3 min = 9 min window. */
    private static final int MAX_PENDING_BARS = 3;

    /** NIFTY contract lot size — exposed for the Manual Terminal controller (translates
     *  operator "lots" input into a contract count) so it doesn't duplicate the constant. */
    public static int lotSize() { return LOT_SIZE; }

    /** Setup enum kept in the shape older DB rows and state files know so their
     *  serialized {@code setup} column deserializes cleanly. Only L4_BREAKDOWN and
     *  H3_REVERSAL fire in the current strategy; the other values (L3_REVERSAL,
     *  H4_BREAKOUT, VWAP_BREAKDOWN, MANUAL) are legacy — never emitted by the new
     *  detection code but retained so historical rows load without exception. */
    public enum ActiveSetup {
        L3_REVERSAL,      // legacy
        H3_REVERSAL,      // active (bearish reversal off option's own H3)
        H4_BREAKOUT,      // legacy
        L4_BREAKDOWN,     // active (bearish breakdown below option's own L4)
        VWAP_BREAKDOWN,   // legacy
        MANUAL            // legacy — reserved for the Options Scalper Terminal path
    }

    /** Composite key {@code "setup|symbol"} for {@code state.openPositions}.
     *  Allows a MANUAL Options-Scalper-Terminal position to coexist with a bot-managed
     *  fire on the same Fyers option symbol — each tracked independently. */
    private static String posKey(Position p) {
        if (p == null) return "";
        String setup = p.setup == null ? "MANUAL" : p.setup.name();
        return setup + "|" + (p.symbol == null ? "" : p.symbol);
    }

    /** V2 watchlist role slot — kept only because {@link State#symbolRole} is serialized in
     *  historical state files. No live code branches on it in the new option-premium
     *  pipeline. */
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

    // Tolerate unknown fields on read so a state file written by a different branch doesn't
    // wipe today's in-memory ring on boot. Unknown enum values become null (loadFromDisk
    // drops them via purgeRetiredEntries).
    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
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
                     ObjectProvider<CamarillaStreamBroker> streamBrokerProvider) {
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
    }

    /** Push the latest dashboard state to every SSE-connected browser. No-op when no clients. */
    private void publishStream() {
        try {
            CamarillaStreamBroker b = streamBrokerProvider == null ? null : streamBrokerProvider.getIfAvailable();
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

        // Subscribe NIFTY spot — kept alive so the dashboard/ticker chip can read its LTP.
        // Its candles route through onCandleClose but are IGNORED by the phase processor
        // (only the two option legs matter for setup detection).
        state.futuresSymbol = NIFTY_SYMBOL;
        candleAggregator.subscribe(NIFTY_SYMBOL, c -> onCandleClose(NIFTY_SYMBOL, c));
        try { marketDataService.subscribeAdditional(java.util.List.of(NIFTY_SYMBOL)); }
        catch (Exception ignored) {}
        log.info("[Camarilla] boot — NIFTY spot subscribed: {}", NIFTY_SYMBOL);

        // AtmTracker listener kept as a harmless hook; the new strategy doesn't gate on ATM.
        atmTracker.setListener(this::onAtmChange);

        // Best-effort attempt to resolve today's two option legs. Fails silently if the
        // NIFTY levels or option chain aren't warm yet — the scheduled retry catches up.
        try { resolveSessionLegs(); }
        catch (Exception e) { log.warn("[Camarilla] session-legs boot resolve failed: {}", e.getMessage()); }

        log.info("[Camarilla] booted — enabled={}, lots={}, squareoff={}, restoredPositions={}",
            riskSettings.isCamarillaEnabled(), riskSettings.getCamarillaLotsPerLeg(),
            riskSettings.getCamarillaSquareOffTime(), state.openPositions.size());
    }

    /** Drop any event whose timestamp is before today's IST midnight. Called on boot to
     *  catch state files with today's dayKey but stale event timestamps from a partial
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

    /** Walk today's in-memory closed-trades ring (just loaded from disk) and patch any DB
     *  rows whose {@code symbol} or {@code setup} column is null but whose {@code
     *  closedAtMillis} matches an in-memory entry within ±5 s. Runs once at boot, before
     *  {@link #rolloverIfNewDay()} which would otherwise clear the ring. */
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
    @Override public String description() { return "Option-premium Camarilla — H5-CE + L5-PE bearish shorts"; }
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

    /** Per-row manual squareoff. Closes only the supplied {@code symbol}, leaves the rest
     *  of the open-positions map untouched. */
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
            state.pendingByOption.clear();
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
     *  sell side, stamp on the buy side. */
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
        // Portfolio risk gate — if consumed daily loss exceeds the cap, flatten everything
        // and lock the session out. Runs on the slow tick because it doesn't need
        // millisecond precision.
        double maxRisk = riskSettings.getPortfolioMaxDailyLoss();
        if (maxRisk > 0 && !state.dailyLossLockout) {
            double consumed = consumedRiskNow();
            if (consumed > maxRisk) {
                event("[ERROR]", "Risk", "consumed ₹" + round2(consumed)
                    + " > maxRisk ₹" + round2(maxRisk) + " — force-closing and locking session");
                state.dailyLossLockout = true;
                for (Position p : new ArrayList<>(state.openPositions.values())) {
                    closePosition(p, "RISK_BREACH");
                }
                saveToDisk();
            }
        }
    }

    @Override
    public void fastSlCheck() {
        // No-op. Entry and SL both fire on 3-min option bar closes now (broker-side SL is
        // placed on entry; the bar-close backup in processOptionBarPhases handles the case
        // where the broker order is rejected/mis-priced). No tick-level watcher.
    }

    /** Best-effort re-resolve trigger for the two session legs. Runs every 30 s until the
     *  legs are on file. Idempotent — becomes a cheap no-op once resolved. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void retrySessionLegsIfNeeded() {
        try { resolveSessionLegs(); }
        catch (Exception e) { log.warn("[Camarilla] session-legs retry failed: {}", e.getMessage()); }
    }

    // ── Session-static leg resolution ──────────────────────────────────────

    /** Resolve today's two option legs from NIFTY's Camarilla H5 / L5, kick off per-option
     *  Camarilla level fetches (each option's own 10-day daily OHLC → its own pivots), and
     *  subscribe both option symbols to the 3-min candle feed. Idempotent — bails once
     *  today's legs are on file. Returns {@code true} on success (freshly resolved or
     *  already resolved). */
    private synchronized boolean resolveSessionLegs() {
        String today = LocalDate.now(IST).toString();
        CamarillaLevels niftyLv = camarillaService.getLevels(NIFTY_SYMBOL);
        if (today.equals(state.sessionLegsDayKey)
            && !state.h5CeSymbol.isBlank() && !state.l5PeSymbol.isBlank()
            && strikesMatchExpected(niftyLv)) {
            ensureSessionLegsSubscribed();
            return true;
        }
        if (niftyLv == null) return false;

        BalancedAtmSelector.StrikeAtLevel h5Row = atmSelector.resolveStrikeAtLevel(niftyLv.h5());
        BalancedAtmSelector.StrikeAtLevel l5Row = atmSelector.resolveStrikeAtLevel(niftyLv.l5());
        if (h5Row == null || l5Row == null) {
            log.debug("[Camarilla] session legs deferred — chain row null (H5={}, L5={})", h5Row, l5Row);
            return false;
        }
        state.h5CeSymbol = h5Row.ceSymbol() == null ? "" : h5Row.ceSymbol();
        state.h5CeStrike = h5Row.resolvedStrike();
        state.h5CeRefLtp = h5Row.ceLtp();
        state.l5PeSymbol = l5Row.peSymbol() == null ? "" : l5Row.peSymbol();
        state.l5PeStrike = l5Row.resolvedStrike();
        state.l5PeRefLtp = l5Row.peLtp();

        if (state.h5CeRefLtp <= 0 || state.l5PeRefLtp <= 0) {
            backfillRefLtpsFromQuotes();
        }
        state.sessionLegsDayKey = today;
        ensureSessionLegsSubscribed();
        // Kick off per-option Camarilla level fetch so getLevels(sym) returns the option's
        // own pivots on the first bar close (or triggers an async refresh if not cached).
        if (!state.h5CeSymbol.isBlank()) camarillaService.getLevels(state.h5CeSymbol);
        if (!state.l5PeSymbol.isBlank()) camarillaService.getLevels(state.l5PeSymbol);

        event("[INFO]", "Session",
            "NIFTY Option Legs Resolved — H5 CE " + state.h5CeStrike
            + " (" + shortSym(state.h5CeSymbol) + ")"
            + " | L5 PE " + state.l5PeStrike
            + " (" + shortSym(state.l5PeSymbol) + ")");
        saveToDisk();
        return true;
    }

    /** Verify each persisted session-leg strike matches today's rounded NIFTY H5/L5. Used
     *  to force a re-resolve when the persisted state file was written under an older
     *  mapping or on a prior session's levels. */
    private boolean strikesMatchExpected(CamarillaLevels niftyLv) {
        if (niftyLv == null) return false;
        long expH5 = Math.round(niftyLv.h5() / (double) STRIKE_STEP) * STRIKE_STEP;
        long expL5 = Math.round(niftyLv.l5() / (double) STRIKE_STEP) * STRIKE_STEP;
        return state.h5CeStrike == expH5 && state.l5PeStrike == expL5;
    }

    /** Pull last-quoted prices from Fyers {@code /data/quotes} for any leg whose
     *  chain-derived refLtp came back as 0 (holiday / pre-market chain response). */
    private void backfillRefLtpsFromQuotes() {
        java.util.LinkedHashSet<String> needed = new java.util.LinkedHashSet<>();
        if (state.h5CeRefLtp <= 0 && !state.h5CeSymbol.isBlank()) needed.add(state.h5CeSymbol);
        if (state.l5PeRefLtp <= 0 && !state.l5PeSymbol.isBlank()) needed.add(state.l5PeSymbol);
        if (needed.isEmpty()) return;
        Map<String, Double> ltpBySymbol = camarillaService.fetchLastQuotedLtps(String.join(",", needed));
        if (ltpBySymbol == null || ltpBySymbol.isEmpty()) return;
        if (state.h5CeRefLtp <= 0 && ltpBySymbol.containsKey(state.h5CeSymbol))
            state.h5CeRefLtp = ltpBySymbol.get(state.h5CeSymbol);
        if (state.l5PeRefLtp <= 0 && ltpBySymbol.containsKey(state.l5PeSymbol))
            state.l5PeRefLtp = ltpBySymbol.get(state.l5PeSymbol);
        log.info("[Camarilla] session legs ref LTPs backfilled from /data/quotes — H5CE={}, L5PE={}",
            state.h5CeRefLtp, state.l5PeRefLtp);
    }

    /** Idempotent re-subscribe of the current session legs (WS + candle aggregator). Safe
     *  to call on every resolveSessionLegs invocation. Closes the mid-day-restart gap
     *  where State carries the symbols on disk but the Fyers WS subscription set boots
     *  empty. */
    private void ensureSessionLegsSubscribed() {
        java.util.List<String> legs = new java.util.ArrayList<>(2);
        if (state.h5CeSymbol != null && !state.h5CeSymbol.isBlank()) legs.add(state.h5CeSymbol);
        if (state.l5PeSymbol != null && !state.l5PeSymbol.isBlank()) legs.add(state.l5PeSymbol);
        if (legs.isEmpty()) return;
        try { marketDataService.subscribeAdditional(legs); }
        catch (Exception ignored) {}
        for (String sym : legs) {
            final String s = sym;
            candleAggregator.subscribe(s, c -> onCandleClose(s, c));
        }
    }

    /** Release yesterday's session legs. Called on daily reset so today's resolve picks up
     *  fresh Camarilla levels (and any fresh weekly-expiry chain symbols) on its next
     *  tick. Legs are only released when NO open position references them. */
    private synchronized void releaseSessionLegs() {
        java.util.Set<String> openSymbols = new java.util.HashSet<>();
        for (Position p : state.openPositions.values()) {
            if (p != null && p.symbol != null) openSymbols.add(p.symbol);
        }
        java.util.List<String> legs = new java.util.ArrayList<>(2);
        for (String sym : new String[] {state.h5CeSymbol, state.l5PeSymbol}) {
            if (sym != null && !sym.isBlank() && !openSymbols.contains(sym)) {
                legs.add(sym);
                candleAggregator.unsubscribe(sym);
            }
        }
        if (!legs.isEmpty()) {
            try { marketDataService.unsubscribeAdditional(legs); }
            catch (Exception ignored) {}
        }
        state.h5CeSymbol = "";
        state.l5PeSymbol = "";
        state.h5CeStrike = 0;
        state.l5PeStrike = 0;
        state.h5CeRefLtp = 0;
        state.l5PeRefLtp = 0;
        state.sessionLegsDayKey = "";
    }

    // ── ATM change handler — no-op (retained as a harmless AtmTracker hook) ─

    public synchronized void onAtmChange(AtmTracker.AtmChange ev) {
        // Camarilla no longer gates trade decisions on the ATM strike. Left as an
        // intentionally-empty listener so the AtmTracker registration doesn't need
        // conditional wiring.
    }

    // ── Candle close handler — dispatches to option phase processor ────────

    public void onCandleClose(String symbol, Candle c) {
        if (!isEnabled()) return;
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            rolloverIfNewDay();
            if (state.doneForDay) return;
            if (state.dailyLossLockout) return;

            // Only the two option legs route into the phase processor. NIFTY spot's own
            // candles are ignored for setup detection — the strategy monitors option
            // premium against each option's OWN Camarilla, not spot's.
            boolean isH5Ce = symbol.equals(state.h5CeSymbol);
            boolean isL5Pe = symbol.equals(state.l5PeSymbol);
            if (!isH5Ce && !isL5Pe) return;

            CamarillaLevels lv = camarillaService.getLevels(symbol);
            if (lv == null) {
                // First bar before per-option pivots have populated — fetch is async, next
                // bar close will see the levels. Nothing to do this bar.
                return;
            }
            processOptionBarPhases(symbol, c, lv);
            saveToDisk();
        }
    }

    /** Per-option 3-min bar walk. Order matters: SL check → target check → entry
     *  trigger → new confirmation → age-out. */
    private void processOptionBarPhases(String symbol, Candle c, CamarillaLevels lv) {
        // (a) SL check first — 3-min close at or above the position's slLevel exits
        //     regardless of any pending state on this symbol.
        Position open = null;
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            if (p.setup == ActiveSetup.MANUAL) continue;   // manual positions are excluded
            if (symbol.equals(p.symbol)) { open = p; break; }
        }
        if (open != null && !Double.isNaN(open.slLevel) && open.slLevel > 0
            && c.close() >= open.slLevel) {
            event("[WARNING]", "Exit",
                shortSym(symbol) + " SL_HIT on 3-min close @ " + round2(c.close())
                + " (sl=" + round2(open.slLevel) + ")");
            closePosition(open, "SL_HIT");
            return;
        }
        // (a2) Target backup — 3-min close at or below the position's targetLevel
        //      also exits (the broker-side target order should catch this first
        //      on any tick, but this covers the case where the target order
        //      didn't fill).
        if (open != null && open.targetLevel > 0
            && c.close() <= open.targetLevel) {
            event("[SUCCESS]", "Exit",
                shortSym(symbol) + " TARGET_HIT on 3-min close @ " + round2(c.close())
                + " (tgt=" + round2(open.targetLevel) + ")");
            closePosition(open, "TARGET_HIT");
            return;
        }

        // (b) Entry trigger — pending exists AND next bar closes below the confirmation low.
        PendingConfirmation pending = state.pendingByOption.get(symbol);
        if (pending != null && c.close() < pending.confirmLow) {
            fire(symbol, pending, c, lv);
            state.pendingByOption.remove(symbol);
            return;
        }

        // (c) Fresh confirmation detection — bearish only.
        if (open == null && pending == null) {
            boolean red = c.close() < c.open();
            if (red) {
                PendingConfirmation fresh = null;
                if (c.high() >= lv.l4() && c.close() < lv.l4()) {
                    fresh = mkPending(ActiveSetup.L4_BREAKDOWN, c);
                } else if (c.high() >= lv.h3() && c.close() < lv.h3()) {
                    fresh = mkPending(ActiveSetup.H3_REVERSAL, c);
                }
                if (fresh != null) {
                    state.pendingByOption.put(symbol, fresh);
                    event("[INFO]", "Setup",
                        "[NIFTY] " + shortSym(symbol) + " " + fresh.setup + " confirmation @ "
                        + round2(c.close()) + " (low=" + round2(c.low())
                        + ", high=" + round2(c.high()) + ")");
                    // fresh pending seeded — no age-out needed this bar.
                    return;
                }
            }
        }

        // (d) Age-out — pending older than MAX_PENDING_BARS × BAR_LENGTH_MS is dropped.
        if (pending != null
            && c.startMillis() - pending.barStartMs >= (long) MAX_PENDING_BARS * BAR_LENGTH_MS) {
            event("[INFO]", "Setup",
                "[NIFTY] " + shortSym(symbol) + " " + pending.setup + " expired (age-out)");
            state.pendingByOption.remove(symbol);
        }
    }

    private static PendingConfirmation mkPending(ActiveSetup setup, Candle c) {
        PendingConfirmation pc = new PendingConfirmation();
        pc.setup       = setup;
        pc.barStartMs  = c.startMillis();
        pc.confirmHigh = c.high();
        pc.confirmLow  = c.low();
        return pc;
    }

    private boolean canFireNewEntry() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        String startHhmm = riskSettings.getCamarillaTradingStartTime();
        if (startHhmm != null && !startHhmm.isBlank()) {
            try {
                LocalTime start = LocalTime.parse(startHhmm);
                if (now.isBefore(start)) return false;
            } catch (Exception ignored) {}
        }
        String endHhmm = riskSettings.getCamarillaTradingEndTime();
        if (endHhmm != null && !endHhmm.isBlank()) {
            try {
                LocalTime end = LocalTime.parse(endHhmm);
                if (!now.isBefore(end)) return false;
            } catch (Exception ignored) {}
        }
        return true;
    }

    /** Sum of remaining ₹ at risk across all currently-open positions. SHORT contribution
     *  is {@code max(0, slLevel − entryPrice) × qty}; LONG uses the mirrored form. */
    private double exposedRiskNow() {
        double total = 0;
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            double perShare = p.isShort
                ? Math.max(0, p.slLevel - p.entryPrice)
                : Math.max(0, p.entryPrice - p.slLevel);
            total += perShare * p.qty;
        }
        return total;
    }

    /** Sum of realized losses (absolute value) across today's closed-trade ring. */
    private double consumedRiskNow() {
        double total = 0;
        for (Map<String, Object> trade : state.todayClosedTrades) {
            double net = asDouble(trade.get("netPnl"));
            if (net < 0) total += Math.abs(net);
        }
        return total;
    }

    /** Fire a SHORT on the option leg. SL comes from the option's OWN Camarilla:
     *  L4_BREAKDOWN → SL at L3; H3_REVERSAL → SL at H4. No target order — the position
     *  closes on SL hit (broker or bar-close backup) or timed squareoff. */
    private void fire(String symbol, PendingConfirmation pending, Candle entryCandle, CamarillaLevels lv) {
        if (!canFireNewEntry()) return;
        if (state.dailyLossLockout) return;
        // Skip if already open on this symbol (any composite key).
        for (Position p : state.openPositions.values()) {
            if (p != null && symbol.equals(p.symbol)) return;
        }
        double slLevel;
        double targetLevel;
        if (pending.setup == ActiveSetup.L4_BREAKDOWN) {
            slLevel     = lv.l3();       // next structural level above L4
            targetLevel = lv.l5();       // extreme downside — premium exhaustion
        } else if (pending.setup == ActiveSetup.H3_REVERSAL) {
            slLevel     = lv.h4();       // next structural level above H3
            targetLevel = lv.l3();       // rejection carries the premium back through L3
        } else {
            event("[ERROR]", "AUTO ENTRY", shortSym(symbol) + " — unknown setup " + pending.setup);
            return;
        }
        if (Double.isNaN(slLevel) || slLevel <= 0) {
            event("[ERROR]", "AUTO ENTRY",
                shortSym(symbol) + " — SL level unavailable for " + pending.setup);
            return;
        }
        // Target may legitimately be <= 0 (L5 can print negative for very-cheap options);
        // clamp to the option tick size so the display + order placement stay sane.
        if (Double.isNaN(targetLevel) || targetLevel < OPTION_TICK_SIZE) {
            targetLevel = OPTION_TICK_SIZE;
        }

        int qty = riskSettings.getCamarillaLotsPerLeg() * LOT_SIZE;

        // Portfolio risk lockout — consumed loss already breached the daily cap.
        double maxRisk = riskSettings.getPortfolioMaxDailyLoss();
        if (maxRisk > 0 && consumedRiskNow() > maxRisk) {
            event("[ERROR]", "Risk", "lockout — consumed ₹"
                + round2(consumedRiskNow()) + " > ₹" + round2(maxRisk));
            state.dailyLossLockout = true;
            saveToDisk();
            return;
        }

        String productType = riskSettings.getCamarillaOrderType();
        double entryLtp = 0;
        try { entryLtp = marketDataService.getLtp(symbol); } catch (Exception ignored) {}
        if (entryLtp <= 0 && entryCandle != null) entryLtp = entryCandle.close();

        // Sell the option leg (side = -1). productType is passed through so the exit
        // orders can net against this entry at Fyers.
        OrderDTO order = orderService.placeOrder(symbol, qty, -1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            event("[ERROR]", "AUTO ENTRY", "entry order rejected for " + shortSym(symbol));
            return;
        }
        try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(symbol)); }
        catch (Exception ignored) {}

        // Broker-side SL — BUY to close the short. Failure is non-fatal (the 3-min close
        // backup in processOptionBarPhases still exits on breach), just logged.
        OrderDTO slOrder = orderService.placeStopLoss(symbol, qty, +1, slLevel);
        if (slOrder == null || slOrder.getId() == null || slOrder.getId().isEmpty()) {
            log.warn("[Camarilla] broker SL placement failed for {} @ {} — falling back to bar-close backup",
                symbol, slLevel);
        }

        // Broker-side TARGET — BUY LIMIT at the target level to close the short at
        // profit. Failure is non-fatal (the 3-min close backup covers it).
        OrderDTO tgtOrder = orderService.placeTarget(symbol, qty, +1, targetLevel);
        if (tgtOrder == null || tgtOrder.getId() == null || tgtOrder.getId().isEmpty()) {
            log.warn("[Camarilla] broker target placement failed for {} @ {} — falling back to bar-close backup",
                symbol, targetLevel);
        }

        Position p = new Position();
        p.symbol          = symbol;
        p.setup           = pending.setup;
        p.qty             = qty;
        p.entryPrice      = entryLtp;
        p.entryOrderId    = order.getId();
        p.openMillis      = System.currentTimeMillis();
        p.slLevel         = slLevel;
        p.originalSlLevel = slLevel;
        p.targetLevel     = targetLevel;
        p.isShort         = true;
        p.fillResolved    = false;
        p.productType     = productType;
        p.breakevenMoved  = false;
        p.lockedAtm       = symbol.equals(state.h5CeSymbol) ? state.h5CeStrike : state.l5PeStrike;

        state.openPositions.put(posKey(p), p);
        state.tradesToday++;
        event("[SUCCESS]", "AUTO ENTRY",
            "sell " + shortSym(symbol) + " ×" + (qty / LOT_SIZE) + "L "
            + pending.setup + " @ " + round2(entryLtp)
            + " (SL " + round2(slLevel) + ", TGT " + round2(targetLevel) + ")");
        saveToDisk();
    }

    // ── Fill resolver ──────────────────────────────────────────────────────

    /** For every open position that hasn't had its broker fill resolved, look up the
     *  actual trade price by entryOrderId in the cached tradebook and overwrite the
     *  estimate. Runs on the slow 5 s tick. */
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
                p.fillResolved = true;
                event("[INFO]", "Fill", shortSym(p.symbol) + " fill resolved — entry "
                    + round2(oldEntry) + " → " + round2(p.entryPrice) + " (qty=" + p.qty + ")");
                saveToDisk();
            } catch (Exception e) {
                log.warn("[Camarilla] fill lookup failed for {}: {}", p.entryOrderId, e.getMessage());
            }
        }
    }

    // ── Time-based squareoff ───────────────────────────────────────────────

    public synchronized void watchSquareoff() {
        if (state.openPositions.isEmpty()) return;
        String hhmm = riskSettings.getCamarillaSquareOffTime();
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

    // ── Position close + persistence to DB / in-memory ring ────────────────

    /** Close a specific Position via market exit and persist the cycle. */
    private boolean closePosition(Position p, String reason) {
        if (p == null) return false;
        String symbol = p.symbol;
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
            closedAtMillis, p.openMillis, p.entryOiBias, p.entryPrice, exitPrice);

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
        cycle.put("entryOiBias",    p.entryOiBias);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 100) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Exit",
            shortSym(symbol) + " closed (" + reason + ") net=" + round2(net) + " gross=" + round2(gross));

        state.openPositions.remove(posKey(p));

        // Drop the candle subscription for this symbol only if it isn't one of the two
        // session legs (those stay subscribed all session) and no other open position
        // references it.
        boolean stillUsed = false;
        for (Position pp : state.openPositions.values()) {
            if (pp != null && symbol.equals(pp.symbol)) { stillUsed = true; break; }
        }
        boolean isSessionLeg = symbol != null
            && (symbol.equals(state.h5CeSymbol) || symbol.equals(state.l5PeSymbol));
        if (!stillUsed && !isSessionLeg) {
            candleAggregator.unsubscribe(symbol);
        }

        // Drop any lingering pending on this symbol — closes on TARGET_HIT / TIMED_EXIT /
        // SL_HIT all invalidate the prior thesis for entry purposes.
        if (symbol != null) state.pendingByOption.remove(symbol);

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

    private static String instrumentFromSymbol(String symbol) {
        if (symbol == null) return null;
        String s = symbol.toUpperCase();
        if (s.contains("BANKNIFTY") || s.contains("NIFTYBANK")) return "BANKNIFTY";
        if (s.contains("NIFTY")) return "NIFTY";
        return null;
    }

    // ── Maintenance actions (Trade page / Settings) ────────────────────────

    /** Wipe ALL recorded trades + ALL event-log entries + EVERY DB row. Open positions
     *  are preserved. */
    public synchronized Map<String, Object> clearAllRecords() {
        int cyclesCleared = state.todayClosedTrades.size();
        state.todayClosedTrades.clear();

        int prevTradesToday      = state.tradesToday;
        int prevConsecutiveLoss  = state.consecutiveLosses;
        state.tradesToday        = 0;
        state.consecutiveLosses  = 0;

        int eventsCleared = state.recentEvents.size();
        state.recentEvents.clear();
        state.pendingByOption.clear();

        saveToDisk();

        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) {
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
            log.warn("[Camarilla] clearTodayRecords DB wipe failed: {}", e.getMessage());
        }

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

    // ── Day rollover ─────────────────────────────────────────────────────────

    @Scheduled(cron = "0 0 6 * * *", zone = "Asia/Kolkata")
    public synchronized void scheduledDailyReset() {
        String today = LocalDate.now(IST).toString();
        log.info("[Camarilla] 06:00 IST daily reset — clearing events + today's trades (was dayKey={})", state.dayKey);
        state.dayKey = today;
        state.tradesToday = 0;
        state.consecutiveLosses = 0;
        state.doneForDay = false;
        state.dailyLossLockout = false;
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
        state.pendingByOption.clear();
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
            state.dailyLossLockout = false;
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
            state.symbolRole.clear();
            state.pendingByOption.clear();
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

        // Live ATM (round current NIFTY spot to STRIKE_STEP) — used by the header chip block
        // below to look up ATM CE/PE symbols for display purposes.
        long liveAtm = 0;
        try {
            double spotLtp = marketDataService.getLtp(NIFTY_SYMBOL);
            if (spotLtp > 0) liveAtm = Math.round(spotLtp / (double) STRIKE_STEP) * STRIKE_STEP;
        } catch (Exception ignored) {}

        // Two session-static option legs (row 0 = H5 CE, row 1 = L5 PE).
        java.util.List<Map<String, Object>> setupLegs = new java.util.ArrayList<>(2);
        addSetupLegRow(setupLegs, "NIFTY", "H5_MONITOR", "BEARISH", "H5",
            state.h5CeStrike, state.h5CeSymbol, "CE", state.h5CeRefLtp);
        addSetupLegRow(setupLegs, "NIFTY", "L5_MONITOR", "BEARISH", "L5",
            state.l5PeStrike, state.l5PeSymbol, "PE", state.l5PeRefLtp);
        m.put("setupLegs",      setupLegs);
        m.put("watchlistSize",  state.symbolRole.size());
        m.put("watchlistRoles", new LinkedHashMap<>(state.symbolRole));

        // Header chips: NIFTY spot (change / change%) + live ATM CE/PE LTP.
        Map<String, Object> vwap = new LinkedHashMap<>();
        String futSym = state.futuresSymbol == null ? "" : state.futuresSymbol;
        double futLtp = 0, futChange = 0, futChangePct = 0;
        if (!futSym.isBlank()) {
            try { futLtp        = marketDataService.getLtp(futSym); }              catch (Exception ignored) {}
            try { futChange     = marketDataService.getDisplayChange(futSym); }    catch (Exception ignored) {}
            try { futChangePct  = marketDataService.getDisplayChangePct(futSym); } catch (Exception ignored) {}
        }
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
        // Back-compat shims (v1 keys, always 0 now) so old chip helpers don't break.
        vwap.put("ceSymbol", "");
        vwap.put("peSymbol", "");
        vwap.put("ceVwap",   0);
        vwap.put("peVwap",   0);
        m.put("atmVwap", vwap);

        // Open positions
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
            double displayedTarget = p.targetLevel;
            if (p.isShort && !Double.isNaN(displayedTarget) && displayedTarget > 0
                && displayedTarget < OPTION_TICK_SIZE) {
                displayedTarget = OPTION_TICK_SIZE;
            }
            row.put("targetLevel",    round2(displayedTarget));
            row.put("slLevel",        round2(p.slLevel));
            row.put("breakevenMoved", p.breakevenMoved);
            row.put("isShort",        p.isShort);
            row.put("openMillis",     p.openMillis);
            // v2 metadata retained for the Live Positions renderer.
            row.put("triggerSymbol", p.triggerSymbol == null ? "" : p.triggerSymbol);
            row.put("entryFutures",  round2(p.entryFutures));
            row.put("targetFutures", round2(p.targetFutures));
            row.put("slFutures",     round2(p.slFutures));
            rows.add(row);
        }
        m.put("openPositions", rows);

        // Per-symbol levels — surface NIFTY spot + each of the two option legs so the trade
        // page tooltip can render their Camarilla pivots on hover.
        Map<String, CamarillaLevels> perSymbolLevels = new LinkedHashMap<>();
        if (futSym != null && !futSym.isBlank()) {
            CamarillaLevels lv = camarillaService.getLevels(futSym);
            if (lv != null) perSymbolLevels.put(futSym, lv);
        }
        for (String sym : new String[]{state.h5CeSymbol, state.l5PeSymbol}) {
            if (sym == null || sym.isBlank()) continue;
            CamarillaLevels lv = camarillaService.getLevels(sym);
            if (lv != null) perSymbolLevels.put(sym, lv);
        }
        m.put("perSymbolLevels", perSymbolLevels);

        // Risk block
        Map<String, Object> risk = new LinkedHashMap<>();
        risk.put("exposedRisk",     round2(exposedRiskNow()));
        risk.put("consumedRisk",    round2(consumedRiskNow()));
        risk.put("dailyRiskBudget", round2(riskSettings.getPortfolioMaxDailyLoss()));
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
        /** Set when consumedRisk crosses the portfolio max daily loss during the session.
         *  Persisted so a mid-day restart preserves the lockout; cleared on day rollover. */
        public boolean dailyLossLockout;
        /** NIFTY spot symbol — subscribed for dashboard LTP display; NOT routed into the
         *  option-premium phase processor (only the two option legs are). */
        public String futuresSymbol = "";
        /** Per-option pending confirmation — keyed by the option's Fyers symbol. A confirmation
         *  bar (red 3-min bar that wicked through L4 or H3 and closed below) sits here waiting
         *  for the NEXT 3-min bar to close below the confirmation's low to fire the sell. */
        public Map<String, PendingConfirmation> pendingByOption = new ConcurrentHashMap<>();
        /** Legacy watchlist role map — kept because old state files serialize it. Not consulted
         *  by the new option-premium detection code. */
        public Map<String, WatchRole> symbolRole = new ConcurrentHashMap<>();

        // ── Option legs resolved at session start from NIFTY H5 / L5 ────────
        public String h5CeSymbol = "";
        public String l5PeSymbol = "";
        public long   h5CeStrike;
        public long   l5PeStrike;
        /** Reference LTP captured at resolve time — display fallback on holidays / pre-market
         *  when the WS feed isn't streaming (live getLtp returns 0). */
        public double h5CeRefLtp;
        public double l5PeRefLtp;
        /** YYYY-MM-DD on which the two legs above were resolved. Mismatch with today forces
         *  a re-resolve on the next retry tick. */
        public String sessionLegsDayKey = "";
    }

    /** A bar that met the confirmation geometry of one of the bearish setups, captured so
     *  the next bar closing below its low can fire the trade. */
    public static class PendingConfirmation {
        public ActiveSetup setup;
        public long   barStartMs;
        public double confirmHigh;
        public double confirmLow;
        /** Retained for state-file back-compat. Not used by the new option-premium fire path
         *  (target orders were removed). */
        public double targetLevel;
        /** Retained for state-file back-compat. */
        public long   lockedAtm;
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
        /** Original SL frozen at fire time. Used to compute 1R for the breakeven trigger
         *  after slLevel mutates. */
        public double     originalSlLevel;
        /** True once the breakeven trigger has fired and slLevel has been moved to entry. */
        public boolean    breakevenMoved;
        /** True once entryPrice has been replaced with the broker's actual fill price. */
        public boolean fillResolved;
        /** True if this is a SHORT (sell) position, false if LONG (buy). Defaults to true —
         *  every algo entry is a short. */
        public boolean isShort = true;
        /** OI bias state at entry — retained for analytics splits. */
        public String entryOiBias = "";
        /** Consecutive fast-tick polls observing LTP at or above slLevel. Legacy transient
         *  field — the fast-tick path is gone, but the field stays so old JSON loads. */
        public transient int slBreachStreak;
        /** Legacy weighted-average reconciliation fields — retained for state-file compat. */
        public int    preAddQty;
        public double preAddEntry;
        /** Fyers product type used for the entry order — required so exits net cleanly. */
        public String productType = "";

        // ── v2 fields retained for state-file / dashboard compatibility ─────
        /** Fyers symbol of the trigger feed (blank for the new option-premium strategy). */
        public String triggerSymbol = "";
        public double entryFutures;
        public double targetFutures = Double.NaN;
        public double slFutures     = Double.NaN;
        public long   lockedAtm;
        public String ceSymbol = "";
        public String peSymbol = "";
    }

    // ── Event log ────────────────────────────────────────────────────────────

    /** Public event-log wrapper for external callers (e.g. the kill-switch toggle in
     *  CamarillaController). Pushes into {@code state.recentEvents} for the Trade page
     *  event-log widget and mirrors the line to {@link EventService}. */
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
        if (eventService != null) eventService.log(severity + " [camarilla:" + source + "] " + message);
        publishStream();
    }

    // ── Persistence ─────────────────────────────────────────────────────────

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) {
                Path legacy = Path.of(LEGACY_STATE_FILE);
                if (Files.exists(legacy)) {
                    File parent = p.toFile().getParentFile();
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
                if (state.openPositions == null)   state.openPositions   = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null)    state.recentEvents    = new ArrayList<>();
                if (state.pendingByOption == null) state.pendingByOption = new ConcurrentHashMap<>();
                if (state.symbolRole == null)      state.symbolRole      = new ConcurrentHashMap<>();
                purgeRetiredEntries();
                migrateOpenPositionsKeyFormat();
            }
        } catch (IOException e) {
            log.warn("[Camarilla] failed to load state: {}", e.getMessage());
        }
    }

    /** State files written under older schemas may carry openPositions or pendingByOption
     *  entries whose setup enum no longer exists (deserialized as null via
     *  READ_UNKNOWN_ENUM_VALUES_AS_NULL). Drop them here — a null setup would break every
     *  downstream check. */
    private void purgeRetiredEntries() {
        if (state.openPositions != null && !state.openPositions.isEmpty()) {
            int before = state.openPositions.size();
            state.openPositions.values().removeIf(p -> p == null || p.setup == null);
            int after = state.openPositions.size();
            if (after != before) {
                log.info("[Camarilla] purged {} retired-setup entries from openPositions",
                    before - after);
            }
        }
        if (state.pendingByOption != null && !state.pendingByOption.isEmpty()) {
            int before = state.pendingByOption.size();
            state.pendingByOption.values().removeIf(pc -> pc == null || pc.setup == null);
            int after = state.pendingByOption.size();
            if (after != before) {
                log.info("[Camarilla] purged {} retired-setup entries from pendingByOption",
                    before - after);
            }
        }
    }

    /** State files predating the composite-key change keyed openPositions by raw symbol.
     *  Rebuild any such entries under the new {@code "setup|symbol"} format. Idempotent. */
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

    /** Defensive LTP lookup — returns 0 on null/blank symbol or any cache miss. */
    private double safeLtp(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getLtp(sym)); }
        catch (Exception e) { return 0; }
    }

    /** Defensive VWAP lookup. */
    private double safeVwap(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getVwap(sym)); }
        catch (Exception e) { return 0; }
    }

    /** Append one row to the {@code setupLegs} dashboard array. Live LTP comes from the WS
     *  tick cache; when that's 0 (holiday, pre-market, fresh boot), we fall back to
     *  {@code refLtp} — the chain-quoted price captured at resolve time. */
    private void addSetupLegRow(java.util.List<Map<String, Object>> rows,
                                String instrument,
                                String setupLabel, String dir, String levelRef,
                                long strike, String symbol, String side,
                                double refLtp) {
        double live = safeLtp(symbol);
        boolean stale = live <= 0 && refLtp > 0;
        double ltp   = live > 0 ? live : round2(refLtp);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("instrument", instrument);
        row.put("setup",      setupLabel == null ? "" : setupLabel);
        row.put("setupTag",   setupLabel == null ? "" : setupLabel);
        row.put("dir",        dir);
        row.put("levelRef",   levelRef);
        row.put("strike",     strike);
        row.put("symbol",     symbol == null ? "" : symbol);
        row.put("side",       side);
        row.put("ltp",        ltp);
        row.put("ltpStale",   stale);
        row.put("vwap",       safeVwap(symbol));
        rows.add(row);
    }

    /** Compact rendering of a Fyers option symbol for the event log:
     *  {@code NSE:NIFTY2562624650CE} → {@code 24650CE}. Falls back to the symbol's
     *  last 8 chars when it doesn't match the expected suffix pattern. */
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
