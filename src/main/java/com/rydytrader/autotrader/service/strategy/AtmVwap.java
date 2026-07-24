package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.AtmTracker;
import com.rydytrader.autotrader.service.AtmVwapStreamBroker;
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
 * NIFTY ATM 2-min VWAP option-selling strategy.
 *
 * <p>First 2-min NIFTY spot bar close (~09:17 IST) captures the close price and locks
 * today's ATM strike (round to nearest 50). The strike's CE and PE symbols become the
 * two option legs for the session. From {@code atmVwapTradingStartTime} onward (default
 * 09:30 IST), each option's 2-min bar closes drive a trigger-candle state machine:
 *
 * <ul>
 *   <li><b>S0 → seed</b>: a bar whose close is &lt; the option's session VWAP becomes the
 *       new trigger candle.</li>
 *   <li><b>S1 → fire</b>: the very next bar whose close is &lt; trigger.low fires a SELL on
 *       that option (VWAP position irrelevant to the fire check).</li>
 *   <li><b>S1 → promote</b>: if it didn't fire but its own close is &lt; VWAP, THAT bar
 *       becomes the new trigger.</li>
 *   <li><b>S1 → invalidate</b>: otherwise the trigger is dropped.</li>
 * </ul>
 *
 * <p>Stop loss = max(trigger.high, {@code atmVwapMinSlPoints}). No target — every open
 * position exits on SL hit or timed squareoff at {@code atmVwapSquareOffTime}. CE and PE
 * can be short simultaneously; the combined (SL − entry) × qty must fit under
 * {@code portfolioMaxDailyLoss}.
 */
@Service
public class AtmVwap implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(AtmVwap.class);
    private static final String STRATEGY_ID = "atmvwap";
    /** Strategy ID written to DB rows for MANUAL-tagged trades. Analytics, calendar
     *  day-modal, and Trade Log filter on this string so manual scalps stay distinguishable
     *  from algorithm trades while still aggregating into the same portfolio totals. */
    public  static final String MANUAL_STRATEGY_ID = "manual";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/atmvwap-state.json";
    private static final String LEGACY_STATE_FILE = "../store/cache/camarilla-state.json";
    /** NIFTY option lot size — 65 (post 2025 revision). */
    private static final int    LOT_SIZE = 65;
    /** NIFTY option premium tick size — the minimum tradable price. */
    private static final double OPTION_TICK_SIZE = 0.05;
    /** NIFTY strike interval — 50 points. */
    private static final long   STRIKE_STEP = 50L;
    private static final int    RECENT_EVENTS_LIMIT = 60;

    /** NIFTY contract lot size — exposed for the Manual Terminal controller (translates
     *  operator "lots" input into a contract count) so it doesn't duplicate the constant. */
    public static int lotSize() { return LOT_SIZE; }

    /** Setup enum kept in the shape older DB rows and state files know so their
     *  serialized {@code setup} column deserializes cleanly. Only CE_SELL and
     *  PE_SELL fire in the current strategy — split by which leg the SELL
     *  targets so analytics can compare CE vs PE performance. The other
     *  values are legacy — never emitted by the new detection code but
     *  retained so historical rows load without exception. */
    public enum ActiveSetup {
        L3_REVERSAL,      // legacy
        H3_REVERSAL,      // legacy
        H4_BREAKOUT,      // legacy
        L4_BREAKDOWN,     // legacy
        VWAP_BREAKDOWN,   // legacy (pre-CE/PE split — short-lived, still in some state files)
        CE_SELL,          // active — SELL fired on the ATM CE leg
        PE_SELL,          // active — SELL fired on the ATM PE leg
        MANUAL            // reserved for the Options Scalper Terminal path
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
     *  historical state files. No live code branches on it. */
    public enum WatchRole { ATM_L4, ITM_L4, OTM_H3 }

    private final CandleAggregator      candleAggregator;
    private final AtmTracker            atmTracker;
    private final BalancedAtmSelector   atmSelector;
    private final MarketDataService     marketDataService;
    private final OrderService          orderService;
    private final EventService          eventService;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectProvider<AtmVwapStreamBroker>     streamBrokerProvider;
    /** OI subscriber. Injected via {@code ObjectProvider} so the AtmVwap bean can boot
     *  even if the OI wiring is disabled or the class hasn't been instantiated yet
     *  (e.g. in a stripped-down test context). */
    private final ObjectProvider<com.rydytrader.autotrader.service.OptionOiSubscriber> optionOiSubscriberProvider;
    /** OI tracker snapshot source for the CE/PE-side entry filter. Same provider
     *  pattern as {@link #optionOiSubscriberProvider} so the strategy tolerates the
     *  tracker being absent. */
    private final ObjectProvider<com.rydytrader.autotrader.service.OptionOiTracker> optionOiTrackerProvider;
    /** Fyers /history reconcile — swaps our WS-aggregated bar OHLC with the exchange-
     *  authoritative bar before the FSM commits to a fire/no-fire decision. Provider so
     *  a stripped-down test context without the Fyers client can still boot AtmVwap. */
    private final ObjectProvider<com.rydytrader.autotrader.service.HistoryReconcileService> historyReconcileProvider;
    /** GDFL config — read to skip Fyers WS + aggregator subscribes for option strikes
     *  when GDFL owns those symbols. Provider so the strategy still boots when the
     *  GDFL bean is absent (test / simulator contexts, or gdfl.enabled=false setups
     *  where the properties bean is present but disabled). */
    private final ObjectProvider<com.rydytrader.autotrader.gdfl.GdflProperties> gdflPropertiesProvider;

    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

    private volatile State state = new State();
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();
    /** Symbols for which we've already registered a {@code candleAggregator.subscribe}
     *  listener this JVM lifetime. Prevents duplicate registrations when
     *  {@link #ensureSessionLegsSubscribed()} is called repeatedly (which happens every
     *  2-min NIFTY candle close via the same-day early-return branch of
     *  {@code resolveAtmFromFirstBar}). Without this guard, each pass added another
     *  listener → {@code processOptionBar} ran N times per bar → trigger promoted /
     *  seeded / invalidated events fired N times. Cleared on day rollover, kill switch,
     *  logout — anywhere the session's option legs are released. */
    private final java.util.Set<String> aggregatorSubscribedSymbols = ConcurrentHashMap.newKeySet();
    public AtmVwap(CandleAggregator candleAggregator,
                   AtmTracker atmTracker,
                   BalancedAtmSelector atmSelector,
                   MarketDataService marketDataService,
                   OrderService orderService,
                   EventService eventService,
                   RiskSettingsStore riskSettings,
                   ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                   ObjectProvider<AtmVwapStreamBroker> streamBrokerProvider,
                   ObjectProvider<com.rydytrader.autotrader.service.OptionOiSubscriber> optionOiSubscriberProvider,
                   ObjectProvider<com.rydytrader.autotrader.service.OptionOiTracker> optionOiTrackerProvider,
                   ObjectProvider<com.rydytrader.autotrader.service.HistoryReconcileService> historyReconcileProvider,
                   ObjectProvider<com.rydytrader.autotrader.gdfl.GdflProperties> gdflPropertiesProvider) {
        this.candleAggregator           = candleAggregator;
        this.atmTracker                 = atmTracker;
        this.atmSelector                = atmSelector;
        this.marketDataService          = marketDataService;
        this.orderService               = orderService;
        this.eventService               = eventService;
        this.riskSettings               = riskSettings;
        this.tradeRepoProvider          = tradeRepoProvider;
        this.streamBrokerProvider       = streamBrokerProvider;
        this.optionOiSubscriberProvider = optionOiSubscriberProvider;
        this.optionOiTrackerProvider    = optionOiTrackerProvider;
        this.historyReconcileProvider   = historyReconcileProvider;
        this.gdflPropertiesProvider     = gdflPropertiesProvider;
    }

    /** Whether an alternate feed (GDFL) is owning tick delivery for the option strikes.
     *  When true, {@link #warmupIfDue} and {@link #ensureSessionLegsSubscribed} skip
     *  the Fyers WS subscription for those symbols — GDFL provides the ticks and any
     *  Fyers-side subscription would be dead weight (ticks would be dropped at
     *  {@link com.rydytrader.autotrader.service.MarketDataService#onTick} anyway). */
    private boolean gdflOwnsOptionTicks() {
        var props = gdflPropertiesProvider == null ? null : gdflPropertiesProvider.getIfAvailable();
        return props != null && props.isEnabled();
    }

    /** Current OI bias — {@code BULLISH} / {@code BEARISH} / {@code NEUTRAL} / {@code STALE} /
     *  {@code UNKNOWN}. Reads {@link com.rydytrader.autotrader.service.OptionOiTracker#snapshot()}
     *  when the tracker bean is available; returns {@code "UNKNOWN"} otherwise so the
     *  filter never fires on a missing dependency. */
    private String currentOiBias() {
        try {
            var t = optionOiTrackerProvider == null ? null : optionOiTrackerProvider.getIfAvailable();
            if (t == null) return "UNKNOWN";
            var snap = t.snapshot();
            String b = snap == null ? null : snap.bias();
            return (b == null || b.isBlank()) ? "UNKNOWN" : b;
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    /** Fires the OI subscriber for the day's ATM. Called from both fast + slow paths of
     *  {@code resolveAtmFromFirstBar} once the strike is locked. No-op when the provider
     *  hasn't materialised (e.g. dependency missing in a test context). */
    private void notifyOiWindow(long atm) {
        try {
            var sub = optionOiSubscriberProvider == null ? null : optionOiSubscriberProvider.getIfAvailable();
            if (sub != null) sub.onAtmSelected(atm);
        } catch (Exception e) {
            log.warn("[AtmVwap] OI subscriber notify failed for ATM={}: {}", atm, e.getMessage());
        }
    }

    /** Fires the OI subscriber at 09:15 pre-warm, handing it the ±15 strike window so
     *  per-strike baselines are taken from the first tick after market open instead of
     *  waiting for the 09:17 ATM lock. See {@code OptionOiSubscriber#onPreWarm}. */
    private void notifyOiPreWarm(long baseAtm, java.util.List<com.rydytrader.autotrader.service.OptionOiTracker.StrikeSymbols> window) {
        try {
            var sub = optionOiSubscriberProvider == null ? null : optionOiSubscriberProvider.getIfAvailable();
            if (sub != null) sub.onPreWarm(baseAtm, window);
        } catch (Exception e) {
            log.warn("[AtmVwap] OI pre-warm notify failed for baseAtm={}: {}", baseAtm, e.getMessage());
        }
    }

    /** Fires on every LTP tick from the {@link MarketDataService} listener chain — most
     *  are filtered out. The one we care about: the very first NIFTY spot tick of the
     *  trading day that arrives with an in-hours (>= 09:15) timestamp. Emits a
     *  once-per-day "Trading started" event anchored to the 09:15 bar OPEN so the
     *  operator's event log has a clear session-start marker. Subsequent ticks are
     *  cheap no-ops via {@link #tradingStartedDayKey}. */
    private void onFirstNiftyTickOfDay(MarketDataService.LtpTick t) {
        if (t == null || !NIFTY_SYMBOL.equals(t.fyersSymbol())) return;
        String today = LocalDate.now(IST).toString();
        // Persisted across restarts — a mid-day boot won't re-fire the event with the
        // now-stale "09:15 candle forming" message on the next NIFTY tick.
        if (today.equals(state.tradingStartedDayKey)) return;

        // Prefer LTT (exchange trade time) when populated; fall back to EFT (Fyers
        // dissemination). NIFTY is an index so LTT is usually 0 → EFT is our primary.
        long tickSec = t.lastTradedTimeSec() > 0 ? t.lastTradedTimeSec() : t.exchFeedTimeSec();
        if (tickSec <= 0) return;
        java.time.Instant instant = java.time.Instant.ofEpochSecond(tickSec);
        ZonedDateTime tickZdt = instant.atZone(IST);
        if (!today.equals(tickZdt.toLocalDate().toString())) return;
        LocalTime tickTime = tickZdt.toLocalTime();
        if (tickTime.isBefore(MARKET_OPEN_IST)) return;

        // Compute today's 09:15 IST epoch — that's the bar open we anchor the display
        // time to (user sees "09:15  INFO  [Session] Trading started …").
        long marketOpenMs = ZonedDateTime.now(IST)
            .withHour(9).withMinute(15).withSecond(0).withNano(0)
            .toInstant().toEpochMilli();

        state.tradingStartedDayKey = today;
        saveToDisk();
        eventAtDisplayTime("[INFO]", "Session",
            "Trading started — 09:15 candle forming (first NIFTY tick @ "
            + tickTime.withNano(0).withSecond(tickTime.getSecond()).toString() + ")",
            marketOpenMs);
    }

    /** Push the latest dashboard state to every SSE-connected browser. No-op when no clients. */
    private void publishStream() {
        try {
            AtmVwapStreamBroker b = streamBrokerProvider == null ? null : streamBrokerProvider.getIfAvailable();
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

        // Subscribe NIFTY spot — first 2-min close resolves today's ATM strike.
        state.futuresSymbol = NIFTY_SYMBOL;
        candleAggregator.subscribe(NIFTY_SYMBOL, c -> onCandleClose(NIFTY_SYMBOL, c));
        try { marketDataService.subscribeAdditional(java.util.List.of(NIFTY_SYMBOL)); }
        catch (Exception ignored) {}
        log.info("[AtmVwap] boot — NIFTY spot subscribed: {}", NIFTY_SYMBOL);

        // Trading-started marker — fires exactly once per day, on the first NIFTY spot
        // tick received after 09:15 IST. Anchors the operator's mental timeline to the
        // 09:15 open of the first 2-min bar.
        marketDataService.addLtpListener(this::onFirstNiftyTickOfDay);

        atmTracker.setListener(this::onAtmChange);

        // If today's ATM is already resolved (mid-day restart), re-subscribe the two option legs.
        try { ensureSessionLegsSubscribed(); }
        catch (Exception e) { log.warn("[AtmVwap] session-legs boot re-subscribe failed: {}", e.getMessage()); }

        // If pre-warm was in progress before the crash (09:15-09:17 window), re-subscribe.
        try { resumeWarmingIfNeeded(); }
        catch (Exception e) { log.warn("[AtmVwap] resume-warming failed: {}", e.getMessage()); }

        // Mid-day restart with an already-resolved ATM: re-fire the OI-window subscribe
        // immediately so the OI feed resumes within seconds of boot instead of waiting
        // ~2-3 min for the next NIFTY 2-min candle close to trigger the usual re-entry
        // path. Idempotent — the subscriber's per-day guard prevents any duplicate work
        // when the 09:17-ish real resolution also fires.
        try {
            String today = LocalDate.now(IST).toString();
            if (state.atmStrike > 0 && today.equals(state.sessionSetupDayKey)) {
                notifyOiWindow(state.atmStrike);
            }
        } catch (Exception e) {
            log.warn("[AtmVwap] OI-window boot re-subscribe failed: {}", e.getMessage());
        }

        log.info("[AtmVwap] booted — enabled={}, lots={}, squareoff={}, restoredPositions={}",
            riskSettings.isAtmVwapEnabled(), riskSettings.getAtmVwapLotsPerLeg(),
            riskSettings.getAtmVwapSquareOffTime(), state.openPositions.size());
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
            log.info("[AtmVwap] pruned {} stale event(s) from before today's 00:00 IST", removed);
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
                log.info("[AtmVwap] backfilled symbol/setup on {} legacy DB row(s) for {}",
                    patched, state.dayKey);
            }
        } catch (Exception e) {
            log.warn("[AtmVwap] backfill failed: {}", e.getMessage());
        }
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id() { return STRATEGY_ID; }
    @Override public String displayName() { return "ATM VWAP"; }
    @Override public String description() { return "NIFTY ATM 2-min · session VWAP · bearish premium sell"; }
    /** Session-locked ATM strike, or 0 before the first 2-min close resolves it. */
    public long getAtmStrike() { return state.atmStrike; }
    /** Selected ATM CE leg Fyers symbol, or "" before ATM resolution. */
    public String getCeSymbol() { return state.ceSymbol == null ? "" : state.ceSymbol; }
    /** Selected ATM PE leg Fyers symbol, or "" before ATM resolution. */
    public String getPeSymbol() { return state.peSymbol == null ? "" : state.peSymbol; }
    /** SL price of the currently-open CE / PE leg position (matched by symbol), or 0
     *  when no such position is active. Used by the Chart page to draw an SL price
     *  line on the corresponding option chart. */
    public double getOpenSlLevel(String fyersSymbol) {
        if (fyersSymbol == null || fyersSymbol.isBlank()) return 0;
        for (Position p : state.openPositions.values()) {
            if (p != null && fyersSymbol.equals(p.symbol) && p.slLevel > 0) return p.slLevel;
        }
        return 0;
    }

    /** Per-side trade counter for today (CE_SELL fires). */
    public int getCeTradesToday() { return state.ceTradesToday; }
    /** Per-side trade counter for today (PE_SELL fires). */
    public int getPeTradesToday() { return state.peTradesToday; }

    /** Realised + open-MTM net P&L of the CE_SELL leg for today. Closed cycles read
     *  from {@code todayClosedTrades} filtered by setup; open positions add live MTM
     *  minus their projected cycle charges. */
    public double getCeSideNetPnlToday() { return sideNetPnlToday(ActiveSetup.CE_SELL); }
    public double getPeSideNetPnlToday() { return sideNetPnlToday(ActiveSetup.PE_SELL); }

    private synchronized double sideNetPnlToday(ActiveSetup side) {
        rolloverIfNewDay();
        double net = 0;
        String sideName = side.name();
        for (Map<String, Object> m : state.todayClosedTrades) {
            if (sideName.equals(String.valueOf(m.getOrDefault("setup", "")))) {
                net += asDouble(m.get("netPnl"));
            }
        }
        for (Position p : state.openPositions.values()) {
            if (p != null && p.setup == side) {
                net += openPositionMtm(p) - cycleChargesFor(p);
            }
        }
        return round2(net);
    }

    @Override public String currentState() {
        if (state.doneForDay) return "DONE_FOR_DAY";
        return state.openPositions.isEmpty() ? "IDLE" : "OPEN(" + state.openPositions.size() + ")";
    }
    @Override public boolean isEnabled() { return riskSettings.isAtmVwapEnabled(); }

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
            java.util.Set<String> uniqueSymbols = new java.util.HashSet<>();
            for (Position p : state.openPositions.values()) {
                if (p != null && p.symbol != null) uniqueSymbols.add(p.symbol);
            }
            for (String sym : uniqueSymbols) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            state.triggerByOption.clear();
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
        watchSquareoff();
        refreshUnresolvedFills();
        warmupIfDue();
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
        // Tick-based SL watcher. No target check — spec says no target level.
        if (state.openPositions.isEmpty()) return;
        for (Position p : new java.util.ArrayList<>(state.openPositions.values())) {
            if (p == null) continue;
            if (p.setup == ActiveSetup.MANUAL) continue;
            if (p.symbol == null || p.symbol.isBlank()) continue;
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); } catch (Exception ignored) {}
            if (ltp <= 0) continue;
            if (p.slLevel > 0 && ltp >= p.slLevel) {
                // Anchor to the CURRENT bar's OPEN (the bar the SL tick fell into).
                // e.g. SL at 09:27:54 → display "09:27" (bar starting 09:27, closing 09:29).
                // Wall-clock time appended in the message for post-mortem precision.
                String wallClock = ZonedDateTime.now(IST).toLocalTime().withNano(0).toString();
                eventAtDisplayTime("[WARNING]", "Exit",
                    shortSym(p.symbol) + " SL_HIT @ " + round2(ltp)
                    + " (sl=" + round2(p.slLevel) + ") @ " + wallClock,
                    currentBarStartMs());
                closePosition(p, "SL_HIT");
            }
        }
    }

    // ── ATM change handler — no-op (retained as a harmless AtmTracker hook) ─

    public synchronized void onAtmChange(AtmTracker.AtmChange ev) {
        // Session ATM is locked at the first 2-min NIFTY bar close; subsequent
        // AtmTracker moves are informational only.
    }

    // ── Candle close handler ──────────────────────────────────────────────

    public void onCandleClose(String symbol, Candle c) {
        // Reconcile FIRST — this replaces the local WS-aggregated candle with the
        // exchange-authoritative /history bar and pushes it back into the aggregator's
        // history ring. Runs regardless of the strategy enable / doneForDay / lockout
        // state because the /chart page needs to render authoritative OHLC even when
        // trading is paused. FSM logic below still gates on those flags.
        Candle authoritative = reconcileBar(symbol, c);

        if (!isEnabled()) return;
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            rolloverIfNewDay();
            if (state.doneForDay) return;
            if (state.dailyLossLockout) return;

            // (1) NIFTY spot: first 2-min close of the day resolves today's ATM strike.
            if (NIFTY_SYMBOL.equals(symbol)) {
                resolveAtmFromFirstBar(authoritative);
                return;
            }

            // (2) Option leg: run the trigger-candle FSM.
            boolean isCe = symbol.equals(state.ceSymbol);
            boolean isPe = symbol.equals(state.peSymbol);
            if (!isCe && !isPe) return;

            processOptionBar(symbol, authoritative);
            saveToDisk();
        }
    }

    // ── Session start — pre-warm at 09:15, resolve ATM CE + PE at 09:17 ────

    /** Pre-warm width — ±10 strikes each side (21 strikes total, 42 option symbols).
     *  Covers ±500 pts of first-2-min NIFTY move so the resolved ATM's CE + PE almost
     *  always fall inside the window and their first 09:15–09:17 candle has full OHLC.
     *  Also feeds the {@code OptionOiSubscriber}'s window, so the OI tracker's
     *  per-strike baseline can be taken from the first WS OI tick at 09:15 rather than
     *  waiting for ATM lock at 09:17. Extreme opens beyond ±500 pts still fall back to
     *  the slow (racy) path with a partial-first-bar warning.
     *
     *  <p>Sized at 10 so the total 42-symbol pre-warm fits under GDFL's 50-symbol
     *  per-key subscription cap when the {@code gdfl-integration} flow subscribes the
     *  same window for exchange-authoritative OI + LTP. */
    private static final int PRE_WARM_STRIKES_EACH_SIDE = 10;
    private static final LocalTime MARKET_OPEN_IST      = LocalTime.of(9, 15);
    private static final LocalTime PRE_WARM_CUTOFF_IST  = LocalTime.of(9, 17);

    /** Called from {@link #tick()} on the 5 s slow loop. Subscribes ±10 strikes of ATM
     *  candidate legs so the aggregator has 2 min of tick history for whichever strike
     *  ends up being the 09:17 ATM. Idempotent — same-day short-circuits. */
    private synchronized void warmupIfDue() {
        if (!isEnabled()) return;
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        if (now.isBefore(MARKET_OPEN_IST)) return;                    // too early
        if (!now.isBefore(PRE_WARM_CUTOFF_IST)) return;               // too late — resolveAtmFromFirstBar will handle it
        if (state.ceSymbol != null && !state.ceSymbol.isBlank()) return;  // ATM already resolved
        if (!state.warmingStrikes.isEmpty()) return;                  // already warming
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.preWarmDayKey)) return;                // already ran today
        double niftyLtp;
        try { niftyLtp = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { return; }
        if (niftyLtp <= 0) return;                                     // WS not warm yet

        long baseAtm = Math.round(niftyLtp / (double) STRIKE_STEP) * STRIKE_STEP;
        long lo = baseAtm - (long) PRE_WARM_STRIKES_EACH_SIDE * STRIKE_STEP;
        long hi = baseAtm + (long) PRE_WARM_STRIKES_EACH_SIDE * STRIKE_STEP;

        java.util.NavigableMap<Long, BalancedAtmSelector.ChainStrike> chain;
        try { chain = atmSelector.fetchChainStrikes(); }
        catch (Exception e) {
            log.warn("[AtmVwap] pre-warm chain fetch failed: {}", e.getMessage());
            return;
        }
        if (chain == null || chain.isEmpty()) {
            log.warn("[AtmVwap] pre-warm skipped — empty chain response");
            return;
        }

        List<String> subs = new ArrayList<>();
        List<com.rydytrader.autotrader.service.OptionOiTracker.StrikeSymbols> oiWindow = new ArrayList<>();
        for (Map.Entry<Long, BalancedAtmSelector.ChainStrike> e : chain.entrySet()) {
            long strike = e.getKey();
            if (strike < lo || strike > hi) continue;
            BalancedAtmSelector.ChainStrike cs = e.getValue();
            String ce = cs.ceSymbol(), pe = cs.peSymbol();
            if (ce == null || ce.isBlank() || pe == null || pe.isBlank()) continue;
            if (cs.ceLtp() <= 0 || cs.peLtp() <= 0) continue;
            state.warmingStrikes.add(strike);
            state.warmingCeByStrike.put(strike, ce);
            state.warmingPeByStrike.put(strike, pe);
            subs.add(ce);
            subs.add(pe);
            oiWindow.add(new com.rydytrader.autotrader.service.OptionOiTracker.StrikeSymbols(strike, ce, pe));
        }

        if (subs.isEmpty()) {
            log.warn("[AtmVwap] pre-warm found no quoted strikes near {} (range {}–{})",
                baseAtm, lo, hi);
            return;
        }

        // When GDFL owns option tick delivery: skip the Fyers WS subscribe (dead weight —
        // Fyers ticks for altFeed-owned symbols get dropped at MarketDataService.onTick).
        // BUT still register the per-strike CandleAggregator listener — CandleAggregator
        // drops ticks for symbols with no registered listener at onLtpTick, so without
        // this the 09:15 → 09:17 bucket for CE/PE never forms (aggregator subscribe
        // otherwise wouldn't happen until 09:17 ATM lock, by which point the 09:15
        // ticks have already been discarded). GDFL pushes LTP via pushLtpTick which
        // fires the same listener chain, so a registered listener is sufficient.
        if (!gdflOwnsOptionTicks()) {
            try { marketDataService.subscribeAdditional(subs); }
            catch (Exception ignored) {}
        }
        for (String sym : subs) {
            if (aggregatorSubscribedSymbols.contains(sym)) continue;
            final String s = sym;
            candleAggregator.subscribe(s, cc -> onCandleClose(s, cc));
            aggregatorSubscribedSymbols.add(sym);
        }
        // Hand the ±10 window to the OI tracker so per-strike baselines are captured on
        // the very first WS OI tick (09:15 IST), not on the 09:17 ATM lock. When
        // resolveAtmFromFirstBar later fires notifyOiWindow(resolvedAtm), the tracker
        // narrows to ±7 and keeps the 09:15 baselines for strikes that stay in the new
        // window; outer strikes are dropped and their contribution un-credited.
        notifyOiPreWarm(baseAtm, oiWindow);
        state.preWarmDayKey = today;
        saveToDisk();

        event("[INFO]", "Setup",
            "pre-warm subscribed " + state.warmingStrikes.size() + " strikes around ATM "
            + baseAtm + " (range " + lo + "–" + hi + ")");
    }

    /** Called on a mid-warm boot restart. Re-subscribes the warming set that was
     *  persisted before the crash so the aggregator continues sampling. Called from
     *  {@link #boot()} after the state is loaded. */
    private synchronized void resumeWarmingIfNeeded() {
        if (state.warmingStrikes.isEmpty()) return;
        if (state.ceSymbol != null && !state.ceSymbol.isBlank()) {
            // ATM already resolved before the crash — warming set is stale.
            trimWarmingSet();
            return;
        }
        String today = LocalDate.now(IST).toString();
        if (!today.equals(state.preWarmDayKey)) {
            // Yesterday's warming set — abandon (no live subscriptions to unsubscribe;
            // the WS was already closed on shutdown).
            state.warmingStrikes.clear();
            state.warmingCeByStrike.clear();
            state.warmingPeByStrike.clear();
            state.preWarmDayKey = "";
            saveToDisk();
            return;
        }
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        if (!now.isBefore(PRE_WARM_CUTOFF_IST)) {
            // Past 09:17 with no ATM resolved and stale warming — likely a bug case.
            // Drop the warming set; resolveAtmFromFirstBar will slow-path when NIFTY closes.
            state.warmingStrikes.clear();
            state.warmingCeByStrike.clear();
            state.warmingPeByStrike.clear();
            state.preWarmDayKey = "";
            saveToDisk();
            return;
        }
        List<String> subs = new ArrayList<>();
        for (Long strike : state.warmingStrikes) {
            String ce = state.warmingCeByStrike.get(strike);
            String pe = state.warmingPeByStrike.get(strike);
            if (ce != null && !ce.isBlank()) subs.add(ce);
            if (pe != null && !pe.isBlank()) subs.add(pe);
        }
        if (subs.isEmpty()) return;
        try { marketDataService.subscribeAdditional(subs); }
        catch (Exception ignored) {}
        for (String sym : subs) {
            if (aggregatorSubscribedSymbols.contains(sym)) continue;
            final String s = sym;
            candleAggregator.subscribe(s, cc -> onCandleClose(s, cc));
            aggregatorSubscribedSymbols.add(sym);
        }
        event("[INFO]", "Setup",
            "resuming pre-warm from persisted state — " + state.warmingStrikes.size() + " strikes");
    }

    /** Unsubscribe every pre-warmed symbol that isn't the resolved ATM CE or PE, and
     *  clear the warming state. Called by {@link #resolveAtmFromFirstBar} at 09:17. */
    private synchronized void trimWarmingSet() {
        if (state.warmingStrikes.isEmpty()) return;
        java.util.Set<String> keep = new java.util.HashSet<>();
        if (state.ceSymbol != null && !state.ceSymbol.isBlank()) keep.add(state.ceSymbol);
        if (state.peSymbol != null && !state.peSymbol.isBlank()) keep.add(state.peSymbol);

        List<String> drop = new ArrayList<>();
        for (Long strike : state.warmingStrikes) {
            String ce = state.warmingCeByStrike.get(strike);
            String pe = state.warmingPeByStrike.get(strike);
            if (ce != null && !ce.isBlank() && !keep.contains(ce)) drop.add(ce);
            if (pe != null && !pe.isBlank() && !keep.contains(pe)) drop.add(pe);
        }

        if (!drop.isEmpty()) {
            try { marketDataService.unsubscribeAdditional(drop); }
            catch (Exception ignored) {}
            for (String sym : drop) candleAggregator.unsubscribe(sym);
        }

        int dropped = drop.size();
        state.warmingStrikes.clear();
        state.warmingCeByStrike.clear();
        state.warmingPeByStrike.clear();
        saveToDisk();

        if (dropped > 0) {
            event("[INFO]", "Setup",
                "pre-warm trimmed — kept ATM legs, unsubscribed " + dropped + " symbol(s)");
        }
    }

    /** First 2-min NIFTY spot bar close of the day. Rounds close to the nearest 50-point
     *  strike, picks the ATM CE + PE symbols. Fast path: pre-warm already subscribed the
     *  target strike (~99% of days). Slow path: the strike fell outside the ±10 pre-warm
     *  window (extreme open) — subscribe fresh and log a warning (that bar's OHLC will be
     *  partial). Idempotent — no-op if today's ATM is already resolved. */
    private synchronized void resolveAtmFromFirstBar(Candle c) {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.sessionSetupDayKey) && state.atmStrike > 0
            && !state.ceSymbol.isBlank() && !state.peSymbol.isBlank()) {
            ensureSessionLegsSubscribed();
            // Re-establish OI subscriptions after a restart — idempotent when the window
            // is already active in the tracker.
            notifyOiWindow(state.atmStrike);
            return;
        }
        double close = c.close();
        if (close <= 0) return;
        long strike = Math.round(close / (double) STRIKE_STEP) * STRIKE_STEP;

        String ce = state.warmingCeByStrike.get(strike);
        String pe = state.warmingPeByStrike.get(strike);

        if (ce != null && !ce.isBlank() && pe != null && !pe.isBlank()) {
            // Fast path — pre-warm hit. The strike was subscribed at 09:15 so the
            // 09:17-09:19 aggregator bucket has been continuously sampling since ~09:15.
            state.firstBarCloseSymbol = NIFTY_SYMBOL;
            state.firstBarClose       = close;
            state.atmStrike           = strike;
            state.ceSymbol            = ce;
            state.peSymbol            = pe;
            state.ceRefLtp            = safeLtp(ce);
            state.peRefLtp            = safeLtp(pe);
            state.sessionSetupDayKey  = today;
            trimWarmingSet();
            // ATM lock is special-cased to display the bar CLOSE (bar start + 2 min)
            // rather than the bar OPEN — the "lock" conceptually happens AT the boundary
            // when the first NIFTY bar closes, not inside the bar itself. Shows "09:17"
            // for the first bar closing, not "09:15".
            eventAtDisplayTime("[INFO]", "Setup",
                "NIFTY ATM Resolved (pre-warm HIT) — CE " + strike + " (" + shortSym(ce)
                + ") | PE " + strike + " (" + shortSym(pe) + ")",
                c.startMillis() + 2 * 60 * 1000L);
            saveToDisk();
            notifyOiWindow(state.atmStrike);
            return;
        }

        // Slow path — extreme move outside the ±10 pre-warm window, OR pre-warm never ran
        // (fresh install, no market-open tick received in time). Warn and subscribe fresh.
        if (!state.warmingStrikes.isEmpty()) {
            event("[WARNING]", "Setup",
                "NIFTY ATM " + strike + " outside pre-warm window — resolving fresh "
                + "(first bar OHLC may be partial)");
        }
        BalancedAtmSelector.StrikeAtLevel row = atmSelector.resolveStrikeAtLevel(close);
        if (row == null || row.ceSymbol() == null || row.peSymbol() == null
            || row.ceSymbol().isBlank() || row.peSymbol().isBlank()) {
            log.debug("[AtmVwap] ATM resolution deferred — chain row null for close {}", close);
            return;
        }

        state.firstBarCloseSymbol = NIFTY_SYMBOL;
        state.firstBarClose       = close;
        state.atmStrike           = row.resolvedStrike() > 0 ? row.resolvedStrike() : strike;
        state.ceSymbol            = row.ceSymbol();
        state.peSymbol            = row.peSymbol();
        state.ceRefLtp            = row.ceLtp();
        state.peRefLtp            = row.peLtp();
        state.sessionSetupDayKey  = today;

        ensureSessionLegsSubscribed();
        trimWarmingSet();  // drop the useless pre-warm (its ATM guess was wrong)
        // Same special-case as the fast path above — display bar CLOSE (09:17), not
        // bar OPEN (09:15), for the ATM lock event.
        eventAtDisplayTime("[INFO]", "Setup",
            "NIFTY ATM Resolved — CE " + state.atmStrike + " (" + shortSym(state.ceSymbol)
            + ") | PE " + state.atmStrike + " (" + shortSym(state.peSymbol) + ")",
            c.startMillis() + 2 * 60 * 1000L);
        saveToDisk();
        notifyOiWindow(state.atmStrike);
    }

    private void ensureSessionLegsSubscribed() {
        java.util.List<String> legs = new java.util.ArrayList<>(2);
        if (state.ceSymbol != null && !state.ceSymbol.isBlank()) legs.add(state.ceSymbol);
        if (state.peSymbol != null && !state.peSymbol.isBlank()) legs.add(state.peSymbol);
        if (legs.isEmpty()) return;
        // WS subscribe is safely idempotent (the underlying subscribedHsmTokens set
        // dedups). Skipped when GDFL owns option ticks — Fyers WS ticks for these
        // symbols would be dropped at MarketDataService.onTick anyway; the WS
        // subscription is dead weight in that mode.
        // Candle-aggregator subscribe is NOT idempotent — every call adds another
        // listener. Guard with aggregatorSubscribedSymbols so we register exactly one
        // listener per session leg per JVM lifetime. Aggregator subscription is
        // ALWAYS needed (regardless of feed) so the aggregator builds candles from
        // whatever LTP source feeds onLtpTick.
        if (!gdflOwnsOptionTicks()) {
            try { marketDataService.subscribeAdditional(legs); }
            catch (Exception ignored) {}
        }
        for (String sym : legs) {
            if (aggregatorSubscribedSymbols.contains(sym)) continue;
            final String s = sym;
            candleAggregator.subscribe(s, cc -> onCandleClose(s, cc));
            aggregatorSubscribedSymbols.add(sym);
        }
    }

    private synchronized void releaseSessionLegs() {
        java.util.Set<String> openSymbols = new java.util.HashSet<>();
        for (Position p : state.openPositions.values()) {
            if (p != null && p.symbol != null) openSymbols.add(p.symbol);
        }
        java.util.List<String> legs = new java.util.ArrayList<>(2);
        for (String sym : new String[] {state.ceSymbol, state.peSymbol}) {
            if (sym != null && !sym.isBlank() && !openSymbols.contains(sym)) {
                legs.add(sym);
                candleAggregator.unsubscribe(sym);
                aggregatorSubscribedSymbols.remove(sym);
            }
        }
        if (!legs.isEmpty()) {
            try { marketDataService.unsubscribeAdditional(legs); }
            catch (Exception ignored) {}
        }
        // Also drop any still-warming symbols (defensive — should be empty by now).
        if (!state.warmingStrikes.isEmpty()) {
            List<String> drop = new ArrayList<>();
            for (Long strike : state.warmingStrikes) {
                String ce = state.warmingCeByStrike.get(strike);
                String pe = state.warmingPeByStrike.get(strike);
                if (ce != null && !ce.isBlank() && !openSymbols.contains(ce)) drop.add(ce);
                if (pe != null && !pe.isBlank() && !openSymbols.contains(pe)) drop.add(pe);
            }
            if (!drop.isEmpty()) {
                try { marketDataService.unsubscribeAdditional(drop); }
                catch (Exception ignored) {}
                for (String sym : drop) {
                    candleAggregator.unsubscribe(sym);
                    aggregatorSubscribedSymbols.remove(sym);
                }
            }
            state.warmingStrikes.clear();
            state.warmingCeByStrike.clear();
            state.warmingPeByStrike.clear();
        }
        state.ceSymbol           = "";
        state.peSymbol           = "";
        state.atmStrike          = 0;
        state.ceRefLtp           = 0;
        state.peRefLtp           = 0;
        state.firstBarClose      = 0;
        state.sessionSetupDayKey = "";
        state.preWarmDayKey      = "";
    }

    // ── Trigger-candle FSM ─────────────────────────────────────────────────

    /** Per-option 2-min bar walk. Trigger-candle FSM described at the class-level Javadoc. */
    private void processOptionBar(String symbol, Candle c) {
        double vwap = 0;
        try { vwap = marketDataService.getVwap(symbol); }
        catch (Exception ignored) {}
        if (vwap <= 0) {
            // Fyers ATP not warm yet — skip this bar; the FSM stays in whatever state it was.
            return;
        }

        // After the configured trading end time, no new entries will fire — so the FSM
        // work (seeding / promoting / invalidating triggers) is dead weight and just
        // spams the event log. Silently skip. Existing open positions keep running via
        // fastSlCheck + watchSquareoff independently of this method.
        if (!canFireNewEntry()) {
            // Also drop any stale trigger so it doesn't linger past squareoff / next day.
            state.triggerByOption.remove(symbol);
            return;
        }

        // Skip if a position is already open on this symbol (no stacking).
        for (Position p : state.openPositions.values()) {
            if (p == null) continue;
            if (p.setup == ActiveSetup.MANUAL) continue;
            if (symbol.equals(p.symbol)) return;
        }

        // Note: {@code c} arrives already reconciled by {@link #onCandleClose}. All FSM
        // decisions below therefore use exchange-authoritative OHLC without any per-branch
        // /history calls.
        TriggerCandle trigger = state.triggerByOption.get(symbol);

        if (trigger != null) {
            // S1 — this bar decides the previous trigger's fate.
            if (c.close() < trigger.low) {
                // (a) Fire — check is UNCONDITIONAL vs VWAP.
                fire(symbol, c, trigger);
                state.triggerByOption.remove(symbol);
                return;
            }
            // (b) No fire — the old trigger is invalidated (immediate follow-through failed).
            //     If THIS bar also closes below VWAP, it's promoted to the new trigger.
            if (c.close() < vwap) {
                state.triggerByOption.put(symbol, TriggerCandle.of(c));
                eventAtDisplayTime("[INFO]", "Setup",
                    shortSym(symbol) + " trigger promoted @ close " + round2(c.close())
                    + " (high=" + round2(c.high()) + ", low=" + round2(c.low())
                    + ", vwap=" + round2(vwap) + ")",
                    c.startMillis());
            } else {
                state.triggerByOption.remove(symbol);
                eventAtDisplayTime("[INFO]", "Setup",
                    shortSym(symbol) + " trigger invalidated (no follow-through, close "
                    + round2(c.close()) + " ≥ VWAP " + round2(vwap) + ")",
                    c.startMillis());
            }
            return;
        }

        // S0 — no active trigger. Seed a fresh one if this bar closes below VWAP.
        if (c.close() < vwap) {
            state.triggerByOption.put(symbol, TriggerCandle.of(c));
            eventAtDisplayTime("[INFO]", "Setup",
                shortSym(symbol) + " trigger seeded @ close " + round2(c.close())
                + " (high=" + round2(c.high()) + ", low=" + round2(c.low())
                + ", vwap=" + round2(vwap) + ")",
                c.startMillis());
        }
    }

    /** Blocking /history reconcile for {@code bar}. Returns the authoritative bar when
     *  Fyers has published it (typical ~2-4 s wait), or falls back to {@code bar} with a
     *  warn event so the FSM still fires when the API is down. Emits an info event when
     *  the authoritative close differs from the local close by more than 0.05 so drift is
     *  visible in the operator's event log. */
    /** Kill-switch for the /history reconcile path. Flip to {@code true} to re-enable
     *  after evaluating the LTT-only bucketing (the LTT swap eliminates the boundary
     *  skew that reconcile was mostly there to correct — testing needed to confirm the
     *  remaining throttle/wick drift is tolerable without reconcile). While disabled,
     *  the FSM runs on the local WS-aggregated bar and the chart ring keeps the local
     *  values. All the reconcile machinery stays in place — this is a one-line toggle. */
    private static final boolean HISTORY_RECONCILE_ENABLED = false;

    private Candle reconcileBar(String symbol, Candle bar) {
        if (!HISTORY_RECONCILE_ENABLED) return bar;
        var svc = historyReconcileProvider == null ? null : historyReconcileProvider.getIfAvailable();
        if (svc == null) return bar;
        Candle auth;
        try { auth = svc.fetchAuthoritative(symbol, bar.startMillis()); }
        catch (Exception e) {
            log.warn("[AtmVwap] reconcile threw for {} bar {}: {}", symbol, bar.startMillis(), e.getMessage());
            return bar;
        }
        if (auth == null) {
            event("[WARNING]", "Setup",
                shortSym(symbol) + " /history reconcile unavailable — using local close "
                + round2(bar.close()));
            return bar;
        }
        double drift = auth.close() - bar.close();
        if (Math.abs(drift) > 0.05) {
            event("[INFO]", "Setup",
                shortSym(symbol) + " reconciled close " + round2(auth.close())
                + " (local " + round2(bar.close()) + ", Δ " + round2(drift) + ")");
        }
        // Push the authoritative bar back into the aggregator's history ring so the
        // /chart page (which polls that ring) renders the same OHLC TradingView shows.
        // Best-effort — a miss just leaves the local bar visible on the chart while the
        // FSM still uses the authoritative value returned below.
        try { candleAggregator.updateHistoryEntry(symbol, auth); }
        catch (Exception e) { log.warn("[AtmVwap] history-ring update failed for {}: {}", symbol, e.getMessage()); }
        return auth;
    }


    private boolean canFireNewEntry() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        String startHhmm = riskSettings.getAtmVwapTradingStartTime();
        if (startHhmm != null && !startHhmm.isBlank()) {
            try {
                LocalTime start = LocalTime.parse(startHhmm);
                if (now.isBefore(start)) return false;
            } catch (Exception ignored) {}
        }
        String endHhmm = riskSettings.getAtmVwapTradingEndTime();
        if (endHhmm != null && !endHhmm.isBlank()) {
            try {
                LocalTime end = LocalTime.parse(endHhmm);
                if (!now.isBefore(end)) return false;
            } catch (Exception ignored) {}
        }
        return true;
    }

    /** Sum of remaining ₹ at risk across all currently-open positions. */
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

    private double consumedRiskNow() {
        double total = 0;
        for (Map<String, Object> trade : state.todayClosedTrades) {
            double net = asDouble(trade.get("netPnl"));
            if (net < 0) total += Math.abs(net);
        }
        return total;
    }

    /** Fire a SHORT on the option leg. SL price is clamped to [entry+minSl, entry+maxSl].
     *  Per-leg CE / PE trade counters cap fires per day. No target order — position exits
     *  on tick-based SL hit or timed squareoff. */
    private void fire(String symbol, Candle entryCandle, TriggerCandle trigger) {
        if (!canFireNewEntry()) return;
        if (state.dailyLossLockout) return;
        for (Position p : state.openPositions.values()) {
            if (p != null && symbol.equals(p.symbol)) return;
        }

        // Per-leg fire count gate — hard cap on CE / PE fires per day.
        boolean isCeLeg = symbol.equals(state.ceSymbol);
        int maxCe = riskSettings.getAtmVwapMaxCeTradesPerDay();
        int maxPe = riskSettings.getAtmVwapMaxPeTradesPerDay();
        if (isCeLeg && maxCe > 0 && state.ceTradesToday >= maxCe) {
            event("[WARNING]", "Risk",
                shortSym(symbol) + " — CE fire cap reached (" + state.ceTradesToday
                + "/" + maxCe + "), skipping");
            return;
        }
        if (!isCeLeg && maxPe > 0 && state.peTradesToday >= maxPe) {
            event("[WARNING]", "Risk",
                shortSym(symbol) + " — PE fire cap reached (" + state.peTradesToday
                + "/" + maxPe + "), skipping");
            return;
        }

        // OI bias trade filter. Opt-in gate — when ON, don't fight a directional flow:
        // BULLISH OI (put writers dominant → market bullish) blocks CE_SELL; BEARISH OI
        // (call writers dominant → market bearish) blocks PE_SELL. NEUTRAL / STALE /
        // UNKNOWN never block (STALE = feed dead > 5 min, UNKNOWN = tracker not present).
        if (riskSettings.isAtmVwapOiBiasFilterEnabled()) {
            String bias = currentOiBias();
            if (isCeLeg && "BULLISH".equals(bias)) {
                event("[WARNING]", "OI Bias",
                    shortSym(symbol) + " — CE_SELL blocked, market is BULLISH per OI flow");
                return;
            }
            if (!isCeLeg && "BEARISH".equals(bias)) {
                event("[WARNING]", "OI Bias",
                    shortSym(symbol) + " — PE_SELL blocked, market is BEARISH per OI flow");
                return;
            }
        }

        // Portfolio realized-loss lockout.
        double maxRisk = riskSettings.getPortfolioMaxDailyLoss();
        if (maxRisk > 0 && consumedRiskNow() > maxRisk) {
            event("[ERROR]", "Risk", "lockout — consumed ₹"
                + round2(consumedRiskNow()) + " > ₹" + round2(maxRisk));
            state.dailyLossLockout = true;
            saveToDisk();
            return;
        }

        double entryLtp = 0;
        try { entryLtp = marketDataService.getLtp(symbol); } catch (Exception ignored) {}
        if (entryLtp <= 0 && entryCandle != null) entryLtp = entryCandle.close();
        if (entryLtp <= 0) {
            event("[ERROR]", "AUTO ENTRY", shortSym(symbol) + " — no entry price available");
            return;
        }

        // SL clamped to [entry + minSl, entry + maxSl]. If trigger.high sits below entry +
        // minSl the floor kicks in; if it sits above entry + maxSl the ceiling caps it.
        double minSl = Math.max(0, riskSettings.getAtmVwapMinSlPoints());
        double maxSl = Math.max(minSl, riskSettings.getAtmVwapMaxSlPoints());
        double slFloor = entryLtp + minSl;
        double slCeil  = entryLtp + maxSl;
        double slLevel = Math.max(trigger.high, slFloor);
        if (slLevel > slCeil) slLevel = slCeil;
        if (slLevel <= 0) {
            event("[ERROR]", "AUTO ENTRY",
                shortSym(symbol) + " — invalid SL level (trigger.high=" + trigger.high
                + ", entry=" + entryLtp + ", minSl=" + minSl + ", maxSl=" + maxSl + ")");
            return;
        }

        int qty = riskSettings.getAtmVwapLotsPerLeg() * LOT_SIZE;

        // Combined-risk gate — projected exposure after this entry must fit under portfolioMaxDailyLoss.
        if (maxRisk > 0) {
            double addedRisk = Math.max(0, slLevel - entryLtp) * qty;
            double projected = exposedRiskNow() + addedRisk;
            if (projected > maxRisk) {
                int halfQty = Math.max(LOT_SIZE, qty / 2);
                double halfProjected = exposedRiskNow() + Math.max(0, slLevel - entryLtp) * halfQty;
                if (halfProjected <= maxRisk && halfQty < qty) {
                    event("[WARNING]", "Risk",
                        shortSym(symbol) + " — projected ₹" + round2(projected)
                        + " > cap ₹" + round2(maxRisk) + "; retrying at half qty (" + halfQty + ")");
                    qty = halfQty;
                } else {
                    event("[WARNING]", "Risk",
                        shortSym(symbol) + " — skipped, projected ₹" + round2(projected)
                        + " > cap ₹" + round2(maxRisk) + " (half-qty also over)");
                    return;
                }
            }
        }

        String productType = riskSettings.getAtmVwapOrderType();
        OrderDTO order = orderService.placeOrder(symbol, qty, -1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            event("[ERROR]", "AUTO ENTRY", "entry order rejected for " + shortSym(symbol));
            return;
        }
        try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(symbol)); }
        catch (Exception ignored) {}

        // SL is tick-based inside fastSlCheck — no broker SL order is placed.
        // Tag the setup by which leg fired so analytics can split CE vs PE performance.
        Position p = new Position();
        p.symbol          = symbol;
        p.setup           = isCeLeg ? ActiveSetup.CE_SELL : ActiveSetup.PE_SELL;
        p.qty             = qty;
        p.entryPrice      = entryLtp;
        p.entryOrderId    = order.getId();
        p.openMillis      = System.currentTimeMillis();
        // Record the confirmation candle's bar start — the bar whose close met the fire
        // gate. UI shows the CLOSE time (start + 2 min) as the "entry candle time".
        p.entryCandleMs   = entryCandle == null ? 0 : entryCandle.startMillis();
        p.slLevel         = slLevel;
        p.originalSlLevel = slLevel;
        p.targetLevel     = 0;              // no target
        p.isShort         = true;
        p.fillResolved    = false;
        p.productType     = productType;
        p.breakevenMoved  = false;
        p.lockedAtm       = state.atmStrike;
        // Record entryOiBias ONLY when the trade is against the current bias — those
        // are the ones the filter would have blocked. With-bias / neutral / stale /
        // unknown trades leave the column NULL so analytics can trivially split
        // "against" from "everything else". Only accumulates while the filter is off
        // (with it on, fire() short-circuits earlier and no row is written at all).
        String biasAtEntry = currentOiBias();
        boolean againstBias =
            (isCeLeg && "BULLISH".equals(biasAtEntry)) ||
            (!isCeLeg && "BEARISH".equals(biasAtEntry));
        if (againstBias) p.entryOiBias = biasAtEntry;

        state.openPositions.put(posKey(p), p);
        state.tradesToday++;
        if (isCeLeg) state.ceTradesToday++; else state.peTradesToday++;
        eventAtDisplayTime("[SUCCESS]", "AUTO ENTRY",
            "sell " + shortSym(symbol) + " ×" + (qty / LOT_SIZE) + "L "
            + "@ " + round2(entryLtp) + " (SL " + round2(slLevel)
            + ", " + (isCeLeg ? "CE " + state.ceTradesToday + "/" + maxCe
                              : "PE " + state.peTradesToday + "/" + maxPe) + ")",
            entryCandle == null ? 0 : entryCandle.startMillis());
        saveToDisk();
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
                p.fillResolved = true;
                event("[INFO]", "Fill", shortSym(p.symbol) + " fill resolved — entry "
                    + round2(oldEntry) + " → " + round2(p.entryPrice) + " (qty=" + p.qty + ")");
                saveToDisk();
            } catch (Exception e) {
                log.warn("[AtmVwap] fill lookup failed for {}: {}", p.entryOrderId, e.getMessage());
            }
        }
    }

    // ── Time-based squareoff ───────────────────────────────────────────────

    public synchronized void watchSquareoff() {
        if (state.openPositions.isEmpty()) return;
        String hhmm = riskSettings.getAtmVwapSquareOffTime();
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

    private boolean closePosition(Position p, String reason) {
        if (p == null) return false;
        String symbol = p.symbol;
        String productType = (p.productType == null || p.productType.isBlank())
            ? riskSettings.getAtmVwapOrderType()
            : p.productType;
        int closeSide = p.isShort ? +1 : -1;
        OrderDTO close = orderService.placeExitOrder(symbol, p.qty, closeSide, productType);
        double exitPrice = 0;
        String exitOrderId = close == null ? null : close.getId();
        // Prefer the actual filled trade price from Fyers tradebook. Market squareoff
        // orders usually fill within a few hundred ms; poll a short window before
        // falling back to LTP so the persisted P&L reflects real execution, not a
        // moving-window LTP snapshot.
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
                log.warn("[AtmVwap] exit fill not resolved for order {} on {} — persisting LTP {} as fallback",
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
        long exitCandleMs   = currentBarStartMs();
        String dbStrategyId = (p.setup == ActiveSetup.MANUAL) ? MANUAL_STRATEGY_ID : STRATEGY_ID;
        String setupName    = p.setup == null ? "MANUAL" : p.setup.name();
        persistTradeRow(dbStrategyId, p.symbol, setupName, reason, p.qty,
            gross, charges, net,
            "SL_HIT".equals(reason) ? 1 : 0,
            closedAtMillis, p.openMillis, p.entryOiBias, p.entryPrice, exitPrice,
            p.entryCandleMs, exitCandleMs);

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
        cycle.put("exitCandleMs",   exitCandleMs);
        cycle.put("entryOiBias",    p.entryOiBias);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 100) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        eventAtDisplayTime(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Exit",
            shortSym(symbol) + " closed (" + reason + ") net=" + round2(net) + " gross=" + round2(gross),
            exitCandleMs);

        state.openPositions.remove(posKey(p));

        // Drop candle subscription for this symbol only if not a session leg + no other open uses.
        boolean stillUsed = false;
        for (Position pp : state.openPositions.values()) {
            if (pp != null && symbol.equals(pp.symbol)) { stillUsed = true; break; }
        }
        boolean isSessionLeg = symbol != null
            && (symbol.equals(state.ceSymbol) || symbol.equals(state.peSymbol));
        if (!stillUsed && !isSessionLeg) {
            candleAggregator.unsubscribe(symbol);
        }

        // Drop any lingering trigger — the FSM resets after any close on this symbol.
        if (symbol != null) state.triggerByOption.remove(symbol);

        saveToDisk();
        return true;
    }

    private void persistTradeRow(String strategyId, String symbol, String setup, String reason, int qty,
                                 double gross, double charges, double net, int slHits,
                                 long closedAtMillis, long openedAtMillis, String entryOiBias,
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
            row.setEntryCandleMs(entryCandleMs > 0 ? entryCandleMs : null);
            row.setExitCandleMs (exitCandleMs  > 0 ? exitCandleMs  : null);
            repo.save(row);
        } catch (Exception e) {
            log.warn("[AtmVwap] persist trade failed: {}", e.getMessage());
        }
    }

    /** Start-of-bar epoch millis for the current wall-clock 2-min bucket, anchored on
     *  09:15 IST. Used at exit time to tag which bar the exit fell into. Off-market
     *  hours the returned bucket is still math-correct — no callers use it then. */
    private long currentBarStartMs() {
        ZonedDateTime nowIst = ZonedDateTime.now(IST);
        LocalTime t = nowIst.toLocalTime();
        int minuteOfDay = t.getHour() * 60 + t.getMinute();
        int marketOpen  = 9 * 60 + 15;
        int minutesSinceOpen = minuteOfDay - marketOpen;
        int bucketMinute = minutesSinceOpen >= 0
            ? marketOpen + (minutesSinceOpen / 2) * 2
            : (minuteOfDay / 2) * 2;
        return nowIst.withHour(bucketMinute / 60).withMinute(bucketMinute % 60)
            .withSecond(0).withNano(0).toInstant().toEpochMilli();
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

        int prevTradesToday      = state.tradesToday;
        int prevConsecutiveLoss  = state.consecutiveLosses;
        state.tradesToday        = 0;
        state.ceTradesToday      = 0;
        state.peTradesToday      = 0;
        state.consecutiveLosses  = 0;

        int eventsCleared = state.recentEvents.size();
        state.recentEvents.clear();
        state.triggerByOption.clear();

        saveToDisk();

        long dbCleared = 0;
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo != null) {
                dbCleared = repo.deleteAllRows();
                log.warn("[AtmVwap] clearAllRecords — DB deleteAllRows wiped {} rows", dbCleared);
            }
        } catch (Exception e) {
            log.warn("[AtmVwap] clearAllRecords DB wipe failed: {}", e.getMessage());
        }

        event("[WARNING]", "Maintenance",
            "Cleared ALL records — cycles=" + cyclesCleared
            + " events=" + eventsCleared
            + " dbRows=" + dbCleared
            + " (open positions preserved)");
        log.warn("[AtmVwap] clearAllRecords — cycles={} events={} dbRows={} prevTradesToday={} prevConsLoss={}",
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
        state.ceTradesToday      = 0;
        state.peTradesToday      = 0;
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
            log.warn("[AtmVwap] clearTodayRecords DB wipe failed: {}", e.getMessage());
        }

        event("[WARNING]", "Maintenance",
            "Cleared today's records — cycles=" + cyclesCleared
            + " events=" + eventsCleared
            + " dbRows=" + dbCleared
            + " (open positions preserved)");
        log.warn("[AtmVwap] clearTodayRecords — cycles={} events={} dbRows={} prevTradesToday={} prevConsLoss={}",
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
        log.info("[AtmVwap] 06:00 IST daily reset — clearing events + today's trades (was dayKey={})", state.dayKey);
        state.dayKey = today;
        state.tradesToday = 0;
        state.ceTradesToday = 0;
        state.peTradesToday = 0;
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
        state.triggerByOption.clear();
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

            // Per-day counters + lockouts + audit lists.
            state.tradesToday       = 0;
            state.ceTradesToday     = 0;
            state.peTradesToday     = 0;
            state.consecutiveLosses = 0;
            state.doneForDay        = false;
            state.dailyLossLockout  = false;
            state.todayClosedTrades.clear();
            if (state.recentEvents != null) state.recentEvents.clear();

            // Unsubscribe symbols behind yesterday's open positions before dropping them
            // — otherwise the aggregator keeps buffering into a ring nobody reads.
            java.util.Set<String> uniqSymbolsRoll = new java.util.HashSet<>();
            for (Position p : state.openPositions.values()) {
                if (p != null && p.symbol != null) uniqSymbolsRoll.add(p.symbol);
            }
            for (String sym : uniqSymbolsRoll) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            state.symbolRole.clear();
            state.triggerByOption.clear();

            // Yesterday's resolved-ATM block — resolveAtmFromFirstBar will re-populate
            // at 09:17 from today's first NIFTY 2-min bar close.
            state.firstBarCloseSymbol = "";
            state.firstBarClose       = 0;
            state.atmStrike           = 0;
            state.ceSymbol            = "";
            state.peSymbol            = "";
            state.ceRefLtp            = 0;
            state.peRefLtp            = 0;
            state.sessionSetupDayKey  = "";

            // Pre-warm carries strike-scoped subscriptions that warmupIfDue will rebuild
            // at 09:15 for the new day's baseAtm estimate.
            state.warmingStrikes.clear();
            state.warmingCeByStrike.clear();
            state.warmingPeByStrike.clear();
            state.preWarmDayKey = "";

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

    // ── Dashboard payload (consumed by AtmVwapController + Trade page) ─────

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

        // Live ATM (round current NIFTY spot to STRIKE_STEP) — used by header chip block.
        long liveAtm = 0;
        try {
            double spotLtp = marketDataService.getLtp(NIFTY_SYMBOL);
            if (spotLtp > 0) liveAtm = Math.round(spotLtp / (double) STRIKE_STEP) * STRIKE_STEP;
        } catch (Exception ignored) {}

        // Two session-static option legs (row 0 = ATM CE, row 1 = ATM PE).
        java.util.List<Map<String, Object>> setupLegs = new java.util.ArrayList<>(2);
        addSetupLegRow(setupLegs, "NIFTY", "ATM_CE", "BEARISH", "ATM",
            state.atmStrike, state.ceSymbol, "CE", state.ceRefLtp);
        addSetupLegRow(setupLegs, "NIFTY", "ATM_PE", "BEARISH", "ATM",
            state.atmStrike, state.peSymbol, "PE", state.peRefLtp);
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
        vwap.put("ceSymbol", "");
        vwap.put("peSymbol", "");
        vwap.put("ceVwap",   round2(safeVwap(state.ceSymbol)));
        vwap.put("peVwap",   round2(safeVwap(state.peSymbol)));
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
            row.put("entryCandleMs",  p.entryCandleMs);
            row.put("triggerSymbol", p.triggerSymbol == null ? "" : p.triggerSymbol);
            row.put("entryFutures",  round2(p.entryFutures));
            row.put("targetFutures", round2(p.targetFutures));
            row.put("slFutures",     round2(p.slFutures));
            rows.add(row);
        }
        m.put("openPositions", rows);

        // Per-symbol levels retired — the new strategy computes no pivots. Emit an empty map
        // so downstream JS that iterates perSymbolLevels doesn't NPE.
        m.put("perSymbolLevels", new LinkedHashMap<String, Object>());

        // Risk block
        Map<String, Object> risk = new LinkedHashMap<>();
        risk.put("exposedRisk",         round2(exposedRiskNow()));
        risk.put("consumedRisk",        round2(consumedRiskNow()));
        risk.put("dailyRiskBudget",     round2(riskSettings.getPortfolioMaxDailyLoss()));
        risk.put("atmVwapMinSlPoints",  round2(riskSettings.getAtmVwapMinSlPoints()));
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
        /** Per-leg fire counters for the per-day CE / PE trade caps. Reset on day rollover. */
        public int    ceTradesToday;
        public int    peTradesToday;
        public int    consecutiveLosses;
        public boolean doneForDay;
        public Map<String, Position> openPositions = new ConcurrentHashMap<>();
        public List<Map<String, Object>> todayClosedTrades = new ArrayList<>();
        public List<Map<String, Object>> recentEvents      = new ArrayList<>();
        public boolean dailyLossLockout;
        /** NIFTY spot symbol — subscribed to trigger first-2-min ATM resolution. */
        public String futuresSymbol = "";
        /** Per-option live trigger candle (VWAP FSM state). */
        public Map<String, TriggerCandle> triggerByOption = new ConcurrentHashMap<>();
        /** Legacy watchlist role map — kept for state-file back-compat. */
        public Map<String, WatchRole> symbolRole = new ConcurrentHashMap<>();

        // ── ATM strike + option legs resolved at first 2-min NIFTY bar close ─
        public String firstBarCloseSymbol = "";
        public double firstBarClose;
        public long   atmStrike;
        public String ceSymbol = "";
        public String peSymbol = "";
        public double ceRefLtp;
        public double peRefLtp;
        /** YYYY-MM-DD on which today's ATM was resolved. Mismatch → force re-resolve. */
        public String sessionSetupDayKey = "";

        // ── Pre-warm (±10 strikes subscribed at 09:15 to eliminate second-candle OHLC race) ─
        /** Strikes we pre-warmed at 09:15. Empty after the 09:17 trim. */
        public List<Long> warmingStrikes = new ArrayList<>();
        /** Per-strike CE / PE Fyers symbols captured from the chain at pre-warm time. */
        public Map<Long, String> warmingCeByStrike = new ConcurrentHashMap<>();
        public Map<Long, String> warmingPeByStrike = new ConcurrentHashMap<>();
        /** YYYY-MM-DD of the last pre-warm run. Same-day short-circuits re-warm; different-day
         *  triggers a fresh warm on the next tick where guards pass. */
        public String preWarmDayKey = "";
        /** YYYY-MM-DD on which the "Trading started — 09:15 candle forming" event has
         *  already fired. Persisted so a mid-day restart doesn't re-fire the event on
         *  the next NIFTY tick with a stale "09:15 candle forming" message. */
        public String tradingStartedDayKey = "";
    }

    /** A 2-min bar that closed below its option's session VWAP. If the very next bar closes
     *  below its low, the fire triggers. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class TriggerCandle {
        public double high;
        public double low;
        public double close;
        public long   barStartMs;

        public static TriggerCandle of(Candle c) {
            TriggerCandle t = new TriggerCandle();
            t.high       = c.high();
            t.low        = c.low();
            t.close      = c.close();
            t.barStartMs = c.startMillis();
            return t;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Position {
        public String     symbol = "";
        public ActiveSetup setup;
        public int        qty;
        public double     entryPrice;
        public String     entryOrderId = "";
        public long       openMillis;
        /** Start-of-bar epoch millis for the 2-min candle that TRIGGERED this fire — the
         *  confirmation bar whose close met {@code close < trigger.low}. UI renders as
         *  the bar CLOSE time (start + 2 min). 0 for MANUAL fires and legacy state-file
         *  positions that predate this field. */
        public long       entryCandleMs;
        public double     targetLevel;
        public double     slLevel;
        public double     originalSlLevel;
        public boolean    breakevenMoved;
        public boolean fillResolved;
        public boolean isShort = true;
        public String entryOiBias = "";
        public transient int slBreachStreak;
        public int    preAddQty;
        public double preAddEntry;
        public String productType = "";

        // v2 fields retained for state-file / dashboard compatibility.
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
     *  AtmVwapController). Pushes into {@code state.recentEvents} for the Trade page
     *  event-log widget and mirrors the line to {@link EventService}. */
    public void postEvent(String severity, String source, String message) {
        event(severity, source, message);
    }

    /** Default event emitter — anchors display to the CURRENT 2-min bar's OPEN.
     *  Every event log entry renders as "HH:MM" (bar-aligned) rather than the
     *  wall-clock "HH:MM:SS". For an event that must be anchored to a SPECIFIC
     *  bar boundary that isn't the current one, call {@link #eventAtDisplayTime}
     *  directly with the desired {@code displayMs}. Pure wall-clock events are
     *  no longer emitted from this class. */
    private void event(String severity, String source, String message) {
        eventAtDisplayTime(severity, source, message, currentBarStartMs());
    }

    /** Emit an event whose displayed time is EXACTLY {@code displayMs}. Used when the
     *  caller wants to anchor to a SPECIFIC bar boundary that isn't the current one
     *  (e.g. "Trading started — 09:15" pinned to 09:15 even if the first tick arrived
     *  a few seconds later). Pass 0 for pure wall-clock events. */
    private void eventAtDisplayTime(String severity, String source, String message, long displayMs) {
        Map<String, Object> e = new LinkedHashMap<>();
        long wallTs = System.currentTimeMillis();
        e.put("ts",       wallTs);
        if (displayMs > 0) e.put("barMs", displayMs);
        e.put("severity", severity);
        e.put("source",   source);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [atmvwap:" + source + "] " + message);
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
                    log.info("[AtmVwap] migrated {} → {}", legacy, p);
                } else {
                    return;
                }
            }
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.openPositions == null)    state.openPositions    = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null)     state.recentEvents     = new ArrayList<>();
                if (state.triggerByOption == null)  state.triggerByOption  = new ConcurrentHashMap<>();
                if (state.symbolRole == null)       state.symbolRole       = new ConcurrentHashMap<>();
                if (state.warmingStrikes == null)     state.warmingStrikes     = new ArrayList<>();
                if (state.warmingCeByStrike == null)  state.warmingCeByStrike  = new ConcurrentHashMap<>();
                if (state.warmingPeByStrike == null)  state.warmingPeByStrike  = new ConcurrentHashMap<>();
                purgeRetiredEntries();
                migrateOpenPositionsKeyFormat();
            }
        } catch (IOException e) {
            log.warn("[AtmVwap] failed to load state: {}", e.getMessage());
        }
    }

    private void purgeRetiredEntries() {
        if (state.openPositions != null && !state.openPositions.isEmpty()) {
            int before = state.openPositions.size();
            state.openPositions.values().removeIf(p -> p == null || p.setup == null);
            int after = state.openPositions.size();
            if (after != before) {
                log.info("[AtmVwap] purged {} retired-setup entries from openPositions",
                    before - after);
            }
        }
    }

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
        log.info("[AtmVwap] migrated {} openPositions entries to composite keys",
            migrated.size());
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
            log.warn("[AtmVwap] failed to save state: {}", e.getMessage());
        }
    }

    // ── Misc utility ────────────────────────────────────────────────────────

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private double safeLtp(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getLtp(sym)); }
        catch (Exception e) { return 0; }
    }

    private double safeVwap(String sym) {
        if (sym == null || sym.isBlank()) return 0;
        try { return round2(marketDataService.getVwap(sym)); }
        catch (Exception e) { return 0; }
    }

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
     *  {@code NSE:NIFTY2562624650CE} → {@code 24650CE}. */
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
