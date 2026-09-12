package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.indicator.SuperTrend;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.util.NiftyExpiryResolver;
import com.rydytrader.autotrader.util.NiftyOptionSymbolBuilder;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * VWAP + Supertrend options-buying strategy.
 *
 * <p>On the FIRST NIFTY spot tick each morning (≥ 09:15:00 IST), captures
 * {@code spotOpen}, computes ATM = round(spotOpen / 50) × 50, subscribes ±N
 * strikes for both CE and PE via {@code MarketDataService.subscribeAdditional}.
 * After a 15 s warm-up, picks the CE and PE nearest to the configured target
 * premium (default ₹250) as the tracked pair.
 *
 * <p>Historical bars for both chosen symbols are pulled via
 * {@code FyersClientRouter.getHistory} REST to prime Supertrend so its output is
 * valid from BAR 1 of today's session.
 *
 * <p>On every N-min bar close for either chosen symbol (default 3-min): enters
 * a MARKET buy when the candle is a VWAP-bounce green bar
 * ({@code low ≤ VWAP AND close > VWAP AND close > open}) AND Supertrend is up.
 * SL = the entry bar's low. Exit trail = Supertrend flip. Unlimited re-entries
 * (each fresh VWAP-bounce green bar re-arms). CE and PE tracked independently;
 * both may be open at the same time.
 */
@Service
public class VwapSupertrendStrategy implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(VwapSupertrendStrategy.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HHMM = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    /** NIFTY spot symbol on Fyers — for the market-open tick. */
    private static final String NIFTY_SPOT_SYM = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_INTERVAL = 50L;
    /** NIFTY lot size — matches OptionBuying constant. Fyers changes this once
     *  per year; hardcoded for now. */
    private static final int    LOT_SIZE = 65;
    /** Time-of-day at which the pre-market strike subscription fires. */
    private static final LocalTime PRE_MARKET_SUB_TIME = LocalTime.of(9, 13);
    /** Persisted state — restored on mid-day restart so the chosen CE/PE
     *  strikes (picked at 09:15 based on ₹250 target) survive without
     *  re-picking against the current premium. Guarded by {@code dayKey} —
     *  a state file from a prior day is discarded on load. */
    private static final String STATE_FILE = "../store/cache/vwap-supertrend-state.json";
    // Tolerate unknown JSON properties so stale state files from prior code
    // versions (fields since removed) don't crash the load path.
    private final ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final CandleAggregator    candleAggregator;
    private final MarketDataService   marketDataService;
    private final OrderService        orderService;
    private final EventService        eventService;
    private final RiskSettingsStore   riskSettings;
    private final FyersClientRouter         fyersClient;
    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final MarketHolidayService holidays;
    private final ObjectProvider<OrderEventService> orderEventServiceProvider;
    private final StrategyTradeRepository tradeRepository;

    enum FsmState { BOOT, STRIKES_SUBSCRIBING, ARMED, DONE_FOR_DAY }
    enum LegState { WAITING, PENDING_ENTRY, IN_POSITION }

    /** Per-leg state — one instance for CE, one for PE. Guarded by the
     *  enclosing {@code VwapSupertrendStrategy}'s intrinsic monitor. */
    private static class Leg {
        volatile String   chosenSymbol;
        volatile LegState state = LegState.WAITING;
        volatile String   entryOrderId;
        volatile double   fillPrice;
        volatile double   entryCandleLow;   // frozen at entry order placement, used to derive slPrice after fill
        volatile double   slPrice;          // = entryCandleLow − slBuffer; final value set on fill
        /** SL price at the moment applyFill computed it — never modified by
         *  the trail. UI reads {@code slPrice > initialSlPrice} to render the
         *  yellow "trailed" marker in Live Positions. */
        volatile double   initialSlPrice;
        volatile double   targetPrice;      // = fillPrice + rr × (fillPrice − slPrice)
        volatile int      qty;
        volatile long     entryBarStartMs;
        /** ST direction on the PREVIOUS confirmed bar close — used to detect
         *  a red→green flip on the current bar (a fresh Supertrend-flip
         *  entry). null = first evaluation, no previous state yet. */
        volatile Boolean  previousStUp;
        /** ATR value at the moment fireEntry was called — captured from the
         *  entry bar so applyFill can derive an ATR-based SL buffer using
         *  the SAME ATR the entry decision was made on (not the current
         *  bar's, which could be different by the time the fill lands). */
        volatile double   atrAtEntry;
        /** Latest Supertrend line at the moment fireEntry was called.
         *  Used when SL Mode = SUPERTREND: applyFill seeds slPrice from
         *  this value, then onBarClose trails it upward on each 3-min close
         *  where ST is still up and its line has risen. */
        volatile double   stLineAtEntry;
        /** Which pathway triggered this leg's current entry — 'VWAP_BOUNCE'
         *  or 'ST_FLIP'. Persisted with the trade row on exit. */
        volatile String   entryReason;
        void reset() {
            state = LegState.WAITING;
            entryOrderId = null;
            fillPrice = 0;
            entryCandleLow = 0;
            slPrice = 0;
            initialSlPrice = 0;
            targetPrice = 0;
            qty = 0;
            entryBarStartMs = 0;
            entryReason = null;
            // previousStUp intentionally NOT reset — it's a running signal
            // tracker across bars, not a per-position field.
        }
    }

    private final Leg ceLeg = new Leg();
    private final Leg peLeg = new Leg();

    private volatile FsmState fsm = FsmState.BOOT;
    private volatile double spotOpen = 0;
    private volatile long   atmStrike = 0;
    private volatile long   strikesSubscribedAtMs = 0;
    private volatile String todayKey = "";
    /** Whether today's pre-market subscription has already fired (idempotency
     *  guard). Reset on day rollover. */
    private volatile boolean preMarketSubscribedToday = false;
    /** ATM strike used for the pre-market subscription (based on prev NIFTY
     *  close). Kept so we can detect when the actual 09:15 ATM falls outside
     *  the pre-subscribed range and top up the subscription. */
    private volatile long   preMarketAtm = 0;
    /** Every option strike subscribed today (pre-market + top-up on 09:15).
     *  Used to trim the subscription to only the chosen CE + PE after pair
     *  pick — dropping the 78 unused strikes frees WS bandwidth and cuts
     *  incoming tick volume by ~95 %. */
    private final java.util.Set<String> subscribedStrikes = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** IST date the strategy last captured spot open on (real trading session
     *  date). Empty until spot-open capture fires today. Persisted; used at
     *  load time to detect state that carried over from a prior trading day
     *  through a periodic save that happened past IST midnight. */
    private volatile String sessionDate = "";
    /** NIFTY daily floor pivot P = (prevH + prevL + prevC) / 3, computed once
     *  at pre-market subscribe. 0 until fetched. Drives the daily-bias filter:
     *  spot > pivot → BULLISH (allow CE, skip PE); spot < pivot → BEARISH
     *  (allow PE, skip CE). Only enforced when vwapStBiasFilterEnabled. */
    private volatile double niftyPivot = 0;
    /** Total closed trades today for liveNetPnlToday accumulation. */
    private final AtomicReference<Double> realisedPnlToday = new AtomicReference<>(0.0);
    private final Map<Long, ClosedTrade> tradesTodayById = new ConcurrentHashMap<>();

    private record ClosedTrade(String side, String symbol, double entry, double exit,
                                int qty, long closedMs, String reason, String setup,
                                long openedMs) {}

    /** Exit orders placed but not yet confirmed by Fyers's fill event. When
     *  the exit fill lands via {@link OrderEventService}, we look up this map
     *  by orderId to find the trade record we need to REFINE with the actual
     *  fill price (fireExit writes the row with LTP-at-placement as a
     *  best-effort placeholder). */
    private final Map<String, PendingExit> pendingExitsByOrderId = new ConcurrentHashMap<>();

    private record PendingExit(long tradeMs, String side, String symbol,
                                int qty, double entry, String reason,
                                double approxExit, Long dbRowId) {}

    public VwapSupertrendStrategy(CandleAggregator candleAggregator,
                                   MarketDataService marketDataService,
                                   OrderService orderService,
                                   EventService eventService,
                                   RiskSettingsStore riskSettings,
                                   FyersClientRouter fyersClient,
                                   TokenStore tokenStore,
                                   FyersProperties fyersProperties,
                                   MarketHolidayService holidays,
                                   ObjectProvider<OrderEventService> orderEventServiceProvider,
                                   StrategyTradeRepository tradeRepository) {
        this.candleAggregator          = candleAggregator;
        this.marketDataService         = marketDataService;
        this.orderService              = orderService;
        this.eventService              = eventService;
        this.riskSettings              = riskSettings;
        this.fyersClient               = fyersClient;
        this.tokenStore                = tokenStore;
        this.fyersProperties           = fyersProperties;
        this.holidays                  = holidays;
        this.orderEventServiceProvider = orderEventServiceProvider;
        this.tradeRepository           = tradeRepository;
    }

    @PostConstruct
    public void boot() {
        todayKey = LocalDate.now(IST).toString();
        marketDataService.subscribeAdditional(Collections.singletonList(NIFTY_SPOT_SYM));
        marketDataService.addLtpListener(this::onTick);
        OrderEventService oes = orderEventServiceProvider.getIfAvailable();
        if (oes != null) oes.addFillListener(this::onOrderFill);

        // Restore state from disk if today's snapshot exists — mid-day restart
        // keeps the chosen CE/PE from 09:15 rather than re-picking against the
        // now-different premium.
        if (loadStateFromDisk()) {
            // Re-subscribe every strike we had subscribed pre-restart so the
            // tick flow to the chart resumes without waiting on the on-tick
            // catch-up cycle.
            if (!subscribedStrikes.isEmpty()) {
                marketDataService.subscribeAdditional(new ArrayList<>(subscribedStrikes));
            }
            // Re-register the bar-close callback on the two chosen symbols —
            // without this the strategy would silently stop firing entry
            // signals after any restart.
            if (ceLeg.chosenSymbol != null) {
                candleAggregator.subscribe(ceLeg.chosenSymbol, c -> onBarClose(ceLeg, "CE", c));
            }
            if (peLeg.chosenSymbol != null) {
                candleAggregator.subscribe(peLeg.chosenSymbol, c -> onBarClose(peLeg, "PE", c));
            }
            // Re-fetch prior-session 1-min bars for both chosen legs so ATR
            // warmup is valid from bar 1 of today. CandleAggregator's on-load
            // filter drops yesterday's bars, so without this the first ~10
            // three-min bars of today (09:15 - 09:42) would have NaN
            // Supertrend and no ST line on the chart after a mid-day restart.
            if (ceLeg.chosenSymbol != null) warmupHistory(ceLeg.chosenSymbol, "CE");
            if (peLeg.chosenSymbol != null) warmupHistory(peLeg.chosenSymbol, "PE");
            // Rehydrate today's closed trades ring from the DB — persistTradeRow
            // wrote each exit to strategy_trades, but tradesTodayById is
            // in-memory and empty on boot, so /positions and P&L totals lose
            // today's history until the next exit. Rebuild from DB rows whose
            // sessionDate == today.
            rehydrateTodayClosedTradesFromDb();
            log.info("[VwapSupertrend] restored state — fsm={} spotOpen={} atm={} CE={} PE={}",
                fsm, spotOpen, atmStrike, ceLeg.chosenSymbol, peLeg.chosenSymbol);
            event("[INFO]", "VwapST",
                "State restored — fsm=" + fsm + " spotOpen=" + fmt(spotOpen)
                    + " atm=" + atmStrike
                    + " CE=" + (ceLeg.chosenSymbol == null ? "—" : ceLeg.chosenSymbol)
                    + " PE=" + (peLeg.chosenSymbol == null ? "—" : peLeg.chosenSymbol));
        } else {
            log.info("[VwapSupertrend] booted — waiting for first {} tick ≥ 09:15 IST", NIFTY_SPOT_SYM);
        }
        // Fallback pivot fetch — a mid-day restart that discards stale state
        // has niftyPivot=0. Compute it directly from Fyers /data/history so
        // the bias filter and header chip stay accurate without waiting for
        // tomorrow's 09:13 pre-market cron. Non-fatal — the tick() poll
        // retries every 5s while pivot stays 0.
        computePivotIfMissing("boot");
    }

    /** Computes {@link #niftyPivot} from Fyers /data/history when it's still 0.
     *  No-op when pivot already set or when today isn't a trading day. Logs
     *  every attempt (success + failure) so the operator can trace why the
     *  bias chip reads NEUTRAL. {@code source} tags the log line — "boot",
     *  "tick-retry", etc. */
    private synchronized void computePivotIfMissing(String source) {
        if (niftyPivot > 0) return;
        if (holidays != null && !holidays.isTradingDay()) return;
        try {
            double[] ohlc = fetchNiftySpotPrevOhlc();
            if (ohlc == null) {
                event("[WARNING]", "VwapST",
                    "Pivot fetch (" + source + ") returned no data — Fyers /data/history empty response");
                return;
            }
            if (ohlc[3] <= 0) {
                event("[WARNING]", "VwapST",
                    "Pivot fetch (" + source + ") returned zero close — H=" + fmt(ohlc[1])
                        + " L=" + fmt(ohlc[2]) + " C=" + fmt(ohlc[3]));
                return;
            }
            niftyPivot = (ohlc[1] + ohlc[2] + ohlc[3]) / 3.0;
            event("[INFO]", "VwapST",
                "NIFTY daily pivot (" + source + ") = " + fmt(niftyPivot)
                    + " (prev H=" + fmt(ohlc[1]) + " L=" + fmt(ohlc[2]) + " C=" + fmt(ohlc[3]) + ")");
        } catch (Exception e) {
            event("[ERROR]", "VwapST",
                "Pivot fetch (" + source + ") threw — " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }
    }

    // ── LTP tick path ───────────────────────────────────────────────────────

    void onTick(MarketDataService.LtpTick t) {
        if (t == null) return;
        String sym = t.fyersSymbol();
        if (sym == null) return;

        // Spot-open capture — arms the strategy for today. Skipped on
        // NSE holidays / weekends — Fyers still delivers stale ticks from
        // the last live session, which would trigger a phantom pair pick
        // and warmup on a non-trading day.
        if (fsm == FsmState.BOOT && NIFTY_SPOT_SYM.equals(sym) && t.ltp() > 0) {
            LocalTime nowIst = ZonedDateTime.now(IST).toLocalTime();
            if (nowIst.isBefore(LocalTime.of(9, 15))) return;
            if (holidays != null && !holidays.isMarketOpen()) return;
            captureSpotOpenAndSubscribeStrikes(t.ltp());
            return;
        }

        // No tick-level exits — SL fires only on 3-min bar close in onBarClose.
    }


    // ── Spot-open + strike subscription ─────────────────────────────────────

    private synchronized void captureSpotOpenAndSubscribeStrikes(double openTickLtp) {
        if (fsm != FsmState.BOOT) return;
        spotOpen  = openTickLtp;
        atmStrike = Math.round(spotOpen / (double) STRIKE_INTERVAL) * STRIKE_INTERVAL;
        // Stamp the actual trading-session date. Anything the periodic save
        // writes past midnight IST tomorrow will carry THIS date; next-day
        // boot then discards the state via the sessionDate check.
        sessionDate = LocalDate.now(IST).toString();
        int range = Math.max(1, riskSettings.getVwapStStrikesRange());

        LocalDate today = LocalDate.now(IST);
        LocalDate expiry = NiftyExpiryResolver.currentWeeklyExpiry(today, holidays);

        // Top-up subscription only if actual ATM differs materially from the
        // pre-market anchor — pre-market ±N covers most cases. If NIFTY gaps
        // >N×50 from prev close, subscribe the missing strikes around the
        // actual ATM.
        List<String> topUp = new ArrayList<>();
        if (preMarketSubscribedToday && preMarketAtm > 0) {
            long lowestPre  = preMarketAtm - range * STRIKE_INTERVAL;
            long highestPre = preMarketAtm + range * STRIKE_INTERVAL;
            long lowestNow  = atmStrike - range * STRIKE_INTERVAL;
            long highestNow = atmStrike + range * STRIKE_INTERVAL;
            for (long strike = lowestNow; strike <= highestNow; strike += STRIKE_INTERVAL) {
                if (strike <= 0) continue;
                if (strike >= lowestPre && strike <= highestPre) continue;   // already subscribed
                topUp.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "CE"));
                topUp.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "PE"));
            }
        } else {
            // No pre-market subscription happened (bot booted after 09:15?).
            // Subscribe the full ±N range now — we'll pay the 15-s LTP wait
            // penalty on this path.
            for (int i = -range; i <= range; i++) {
                long strike = atmStrike + i * STRIKE_INTERVAL;
                if (strike <= 0) continue;
                topUp.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "CE"));
                topUp.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "PE"));
            }
        }
        if (!topUp.isEmpty()) {
            marketDataService.subscribeAdditional(topUp);
            subscribedStrikes.addAll(topUp);
        }
        strikesSubscribedAtMs = System.currentTimeMillis();
        fsm = FsmState.STRIKES_SUBSCRIBING;
        event("[INFO]", "VwapST",
            "Spot open captured — spotOpen=" + fmt(spotOpen)
                + " atmStrike=" + atmStrike
                + " preMarketAtm=" + preMarketAtm
                + " topUpSubs=" + topUp.size());
        // Try pair pick immediately — LTPs may already be flowing from the
        // pre-market subscription. If not, tick() retries every 5 s.
        pickPairAndWarmup();
        saveStateToDisk();
    }

    /** 1-second fast-poll while the strategy is actively trying to pick the
     *  CE + PE pair after spot open. NIFTY ATM ± N option strikes are highly
     *  liquid — LTPs typically populate within 1-3 s of 09:15. This poll
     *  fires pickPairAndWarmup every second so the pick succeeds as soon as
     *  enough LTPs are in the cache instead of waiting on the coarser 5 s
     *  scheduler cycle. Runs no-op when the FSM is anywhere but
     *  STRIKES_SUBSCRIBING. */
    @org.springframework.scheduling.annotation.Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void fastPickPoll() {
        if (holidays != null && !holidays.isMarketOpen()) return;
        if (fsm == FsmState.STRIKES_SUBSCRIBING) {
            pickPairAndWarmup();
        }
    }

    /** Fires once daily at 09:13 IST via @Scheduled cron (right after NSE's
     *  09:00-09:12 pre-open session ends), or from tick() as a catch-up when
     *  the bot boots inside the 09:13-09:15 window. Fetches yesterday's
     *  NIFTY 50 spot close via Fyers /data/history (D bars), computes
     *  ATM = round(prevClose/50)×50, and subscribes ±N strikes for both CE
     *  and PE. When the 09:15 tick fires, subscription is already active —
     *  LTPs stream from the first trade and pair pick can happen in ~3 s
     *  instead of 15. */
    @org.springframework.scheduling.annotation.Scheduled(cron = "0 13 9 * * MON-FRI", zone = "Asia/Kolkata")
    public void preMarketScheduledFire() {
        // Use isTradingDay() — isMarketOpen() also gates on the 09:15-15:40
        // window, which is false at 09:13 on every normal trading day and
        // would falsely tag every session as a holiday.
        if (holidays != null && !holidays.isTradingDay()) {
            log.info("[VwapSupertrend] pre-market subscribe SKIPPED — NSE holiday today");
            return;
        }
        preMarketSubscribe();
    }

    private synchronized void preMarketSubscribe() {
        if (preMarketSubscribedToday) return;
        if (!riskSettings.isVwapStEnabled()) return;
        try {
            double[] ohlc = fetchNiftySpotPrevOhlc();
            if (ohlc == null || ohlc[3] <= 0) {
                event("[WARNING]", "VwapST",
                    "Pre-market subscribe SKIPPED — could not resolve NIFTY spot prev OHLC");
                return;
            }
            double prevClose = ohlc[3];
            // Daily floor pivot P = (H + L + C) / 3 — drives the bias filter.
            niftyPivot = (ohlc[1] + ohlc[2] + ohlc[3]) / 3.0;
            event("[INFO]", "VwapST",
                "NIFTY daily pivot = " + fmt(niftyPivot)
                    + " (prev O=" + fmt(ohlc[0]) + " H=" + fmt(ohlc[1])
                    + " L=" + fmt(ohlc[2]) + " C=" + fmt(ohlc[3]) + ")");
            long anchor = Math.round(prevClose / (double) STRIKE_INTERVAL) * STRIKE_INTERVAL;
            int range = Math.max(1, riskSettings.getVwapStStrikesRange());
            LocalDate today = LocalDate.now(IST);
            LocalDate expiry = NiftyExpiryResolver.currentWeeklyExpiry(today, holidays);
            List<String> allSymbols = new ArrayList<>(range * 2 * 2);
            for (int i = -range; i <= range; i++) {
                long strike = anchor + i * STRIKE_INTERVAL;
                if (strike <= 0) continue;
                allSymbols.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "CE"));
                allSymbols.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "PE"));
            }
            marketDataService.subscribeAdditional(allSymbols);
            subscribedStrikes.addAll(allSymbols);
            preMarketAtm = anchor;
            preMarketSubscribedToday = true;
            event("[INFO]", "VwapST",
                "Pre-market subscribe — prevClose=" + fmt(prevClose)
                    + " anchor=" + anchor + " expiry=" + expiry
                    + " subscribed " + allSymbols.size() + " strikes (±" + range + ")");
        } catch (Exception e) {
            event("[ERROR]", "VwapST", "Pre-market subscribe THREW — " + e.getMessage());
        }
    }

    /** NIFTY 50 spot's most recent daily close from Fyers /data/history.
     *  Returns 0 if the call fails or returns no bars — caller logs and skips. */
    private double fetchNiftySpotPrevClose() {
        double[] ohlc = fetchNiftySpotPrevOhlc();
        return ohlc == null ? 0 : ohlc[3];  // close
    }

    /** NIFTY 50 spot's most recent completed daily bar as {open, high, low, close}
     *  from Fyers /data/history. Returns null if the call fails or returns no bars.
     *  Logs at WARN level on failure so silent auth/network errors surface. */
    private double[] fetchNiftySpotPrevOhlc() {
        try {
            LocalDate today = LocalDate.now(IST);
            String from = today.minusDays(7).format(ISO_DATE);
            String to   = today.format(ISO_DATE);
            JsonNode resp = fyersClient.getHistory("NSE:NIFTY50-INDEX", "D", from, to, authHeader());
            if (resp == null) {
                log.warn("[VwapSupertrend] fetchNiftySpotPrevOhlc: null response from Fyers /data/history");
                return null;
            }
            if ("error".equals(resp.path("s").asText(""))) {
                log.warn("[VwapSupertrend] fetchNiftySpotPrevOhlc: Fyers error s={} code={} message={}",
                    resp.path("s").asText(""),
                    resp.path("code").asInt(0),
                    resp.path("message").asText(""));
                return null;
            }
            JsonNode candles = resp.path("candles");
            if (candles == null || !candles.isArray() || candles.size() == 0) {
                log.warn("[VwapSupertrend] fetchNiftySpotPrevOhlc: empty candles array (from={} to={})", from, to);
                return null;
            }
            // Last row's fields (0=time, 1 open, 2 high, 3 low, 4 close, 5 vol).
            // Skip today's bar if the D endpoint includes it.
            JsonNode last = candles.get(candles.size() - 1);
            long epochSec = last.get(0).asLong(0);
            LocalDate barDate = java.time.Instant.ofEpochSecond(epochSec).atZone(IST).toLocalDate();
            if (barDate.equals(today) && candles.size() >= 2) {
                last = candles.get(candles.size() - 2);
            }
            return new double[] {
                last.get(1).asDouble(0),
                last.get(2).asDouble(0),
                last.get(3).asDouble(0),
                last.get(4).asDouble(0)
            };
        } catch (Exception e) {
            log.warn("[VwapSupertrend] fetchNiftySpotPrevOhlc threw: {} — {}",
                e.getClass().getSimpleName(), e.getMessage());
            return null;
        }
    }

    // ── Scheduler tick — pair pick + squareoff cutoff ──────────────────────

    @Override
    public void tick() {
        // Day rollover MUST run before the enabled gate — otherwise a
        // strategy that was disabled at the weekend crossover keeps yesterday's
        // tradesTodayById ring alive and analytics tags those trades as
        // today's P&L on the calendar/home dashboard.
        String today = LocalDate.now(IST).toString();
        if (!today.equals(todayKey)) rolloverIfNewDay(today);
        if (!riskSettings.isVwapStEnabled()) return;
        // NSE closed today — nothing to do until tomorrow. Uses isTradingDay()
        // (weekend / holiday only) not isMarketOpen() so the 09:13-09:15
        // catch-up pre-market subscribe path below can still run on normal
        // trading days.
        if (holidays != null && !holidays.isTradingDay()) return;

        // Retry pivot fetch if boot fallback failed (e.g. Fyers auth expired
        // at boot then refreshed). No-op when pivot is already set.
        if (niftyPivot <= 0) computePivotIfMissing("tick-retry");

        // Pre-market subscription — fire once daily between 09:13 and 09:15 IST.
        // Cron @Scheduled fires this at 09:13 exactly; the check here is a catch-up
        // path for bots that boot mid-window (or if the cron misses for any reason).
        if (!preMarketSubscribedToday) {
            LocalTime nowIst = ZonedDateTime.now(IST).toLocalTime();
            if (!nowIst.isBefore(PRE_MARKET_SUB_TIME) && nowIst.isBefore(LocalTime.of(9, 15))) {
                preMarketSubscribe();
            }
        }

        // Retry pair pick on every scheduler tick while STRIKES_SUBSCRIBING —
        // pickPairAndWarmup() bails silently when no LTPs are available yet,
        // succeeds and transitions to ARMED the moment enough LTPs land.
        if (fsm == FsmState.STRIKES_SUBSCRIBING) {
            pickPairAndWarmup();
        }

        if (fsm == FsmState.ARMED) {
            String squareoffTime = riskSettings.getVwapStSquareOffTime();
            if (squareoffTime != null && !squareoffTime.isBlank()) {
                LocalTime cutoff = LocalTime.parse(squareoffTime);
                if (ZonedDateTime.now(IST).toLocalTime().isAfter(cutoff)) {
                    forceClose("SQUAREOFF");
                }
            }
        }
    }

    // ── Pair pick + history warmup ──────────────────────────────────────────

    private synchronized void pickPairAndWarmup() {
        if (fsm != FsmState.STRIKES_SUBSCRIBING) return;
        // Fires as soon as atmStrike is known (right after 09:15 spot open
        // capture). Picks the ATM strike directly — no LTP scan, no wait for
        // premium proximity. Fast path so the strategy is ARMED and evaluating
        // the very first 3-min candle close.
        if (atmStrike <= 0) return;
        LocalDate today = LocalDate.now(IST);
        LocalDate expiry = NiftyExpiryResolver.currentWeeklyExpiry(today, holidays);
        String bestCe = NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, atmStrike, "CE");
        String bestPe = NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, atmStrike, "PE");
        double bestCeLtp = marketDataService.getLtp(bestCe);
        double bestPeLtp = marketDataService.getLtp(bestPe);
        ceLeg.chosenSymbol = bestCe;
        peLeg.chosenSymbol = bestPe;
        event("[SUCCESS]", "VwapST",
            "Chosen ATM pair — CE=" + bestCe + " (ltp=" + fmt(bestCeLtp) + ")"
                + "  PE=" + bestPe + " (ltp=" + fmt(bestPeLtp) + ")"
                + "  atmStrike=" + atmStrike);

        // Wire candle-close listeners on chosen symbols.
        candleAggregator.subscribe(bestCe, c -> onBarClose(ceLeg, "CE", c));
        candleAggregator.subscribe(bestPe, c -> onBarClose(peLeg, "PE", c));

        // History warmup — pull 1-min bars for the past 3 days so Supertrend is
        // valid from BAR 1 of today's session. Failure on a leg logs and continues.
        warmupHistory(bestCe, "CE");
        warmupHistory(bestPe, "PE");

        // Trim subscription to only the chosen pair — the other ~78 pre-market
        // strikes are no longer needed. Cuts incoming tick volume by ~95 %.
        pruneSubscriptionsToPair(bestCe, bestPe);

        fsm = FsmState.ARMED;
        event("[INFO]", "VwapST", "ARMED — monitoring 3-min bars on chosen CE and PE");
        saveStateToDisk();
    }

    private void pruneSubscriptionsToPair(String keepCe, String keepPe) {
        try {
            List<String> toDrop = new ArrayList<>();
            for (String sym : subscribedStrikes) {
                if (sym.equals(keepCe) || sym.equals(keepPe)) continue;
                toDrop.add(sym);
            }
            if (toDrop.isEmpty()) return;
            marketDataService.unsubscribeAdditional(toDrop);
            subscribedStrikes.removeAll(toDrop);
            event("[INFO]", "VwapST",
                "Unsubscribed " + toDrop.size() + " unused strikes — retained only CE + PE");
        } catch (Exception e) {
            event("[WARNING]", "VwapST",
                "Prune subscriptions THREW — " + e.getMessage() + " (continuing anyway)");
        }
    }

    private void warmupHistory(String sym, String sideLabel) {
        try {
            LocalDate to   = LocalDate.now(IST);
            LocalDate from = to.minusDays(7);   // widened from 3 to 7 to survive over-weekend restarts
            log.info("[VwapSupertrend] warmupHistory START — sym={} side={} from={} to={}",
                sym, sideLabel, from, to);
            JsonNode resp = fyersClient.getHistory(sym, "1", from.format(ISO_DATE), to.format(ISO_DATE), authHeader());
            log.info("[VwapSupertrend] warmupHistory RESP — sym={} respPresent={} candlesPresent={} candlesSize={} respPreview={}",
                sym,
                resp != null,
                resp != null && resp.has("candles"),
                resp != null && resp.has("candles") ? resp.path("candles").size() : -1,
                resp == null ? "null" : resp.toString().substring(0, Math.min(200, resp.toString().length())));
            // Auth-failure detection (Fyers code -16 = expired/invalid token).
            // Surface it as a prominent event so the user knows to re-login;
            // otherwise the warning below reads like a data-availability issue
            // when the real cause is a stale access token.
            if (resp != null && "error".equals(resp.path("s").asText(""))
                    && resp.path("code").asInt(0) == -16) {
                event("[ERROR]", "VwapST",
                    "Fyers auth expired — cannot warm up Supertrend history. "
                        + "Re-login at /fyers/login to mint a fresh token.");
                return;
            }
            JsonNode candles = resp == null ? null : resp.path("candles");
            if (candles == null || !candles.isArray() || candles.size() == 0) {
                event("[WARNING]", "VwapST",
                    sideLabel + " history warmup returned no bars for " + sym
                        + " — Supertrend will be UNAVAILABLE for the first ~33 min of today's session");
                return;
            }
            List<Candle> bars = new ArrayList<>(candles.size());
            for (JsonNode row : candles) {
                if (!row.isArray() || row.size() < 6) continue;
                long epochSec = row.get(0).asLong(0);
                double o = row.get(1).asDouble(0);
                double h = row.get(2).asDouble(0);
                double l = row.get(3).asDouble(0);
                double c = row.get(4).asDouble(0);
                long   v = row.get(5).asLong(0);
                if (epochSec <= 0 || o <= 0) continue;
                bars.add(new Candle(o, h, l, c, v, epochSec * 1000L, 0.0));
            }
            candleAggregator.prependHistory(sym, bars);
            event("[INFO]", "VwapST",
                sideLabel + " history warmup — prepended " + bars.size() + " 1-min bars for " + sym);
        } catch (Exception e) {
            event("[WARNING]", "VwapST",
                sideLabel + " history warmup FAILED for " + sym + " — " + e.getMessage());
        }
    }

    // ── Bar close signal handler ────────────────────────────────────────────

    /** Fires whenever a new 1-min bar appends via CandleAggregator for the
     *  chosen symbol. We check for the CONFIGURED-timeframe bar close (default
     *  3-min) inside — CandleAggregator's ring stores 1-min bars but
     *  {@code getHistory(sym, N)} aggregates them into N-min buckets. */
    private synchronized void onBarClose(Leg leg, String sideLabel, Candle latestOneMin) {
        if (fsm != FsmState.ARMED || !riskSettings.isVwapStEnabled()) return;
        int tf = Math.max(1, riskSettings.getVwapStCandleMinutes());
        // Trigger check only on bars that ALIGN with the configured timeframe
        // boundary (i.e. the last 1-min bar of an N-min bucket). For 3-min,
        // that's when (istMinuteOfDay - 555 [09:15]) % 3 == 2 → 09:18, 09:21…
        long istMs = latestOneMin.startMillis() + 19_800_000L;   // UTC ms → IST ms
        int minuteOfDay = (int) ((istMs % 86_400_000L) / 60_000L);
        if ((minuteOfDay - (9 * 60 + 15)) % tf != tf - 1) return;

        List<Candle> bars = candleAggregator.getHistory(leg.chosenSymbol, tf);
        if (bars.isEmpty()) return;
        int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
        double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
        if (bars.size() < atrPeriod + 1) {
            // Not enough for Supertrend yet — history warmup may still be
            // prepending or option didn't trade prior sessions. Bail silently.
            return;
        }
        Candle bar = bars.get(bars.size() - 1);
        SuperTrend.State st = SuperTrend.at(bars, atrPeriod, mult);
        // Latest ATR — used both for the ATR-mode SL buffer (captured at
        // entry time so applyFill uses the same value the decision was made
        // on) and reported in downstream diagnostics. Prior-session bars
        // prepended by warmupHistory make this valid from bar 1 of today.
        double[] atrSeries = com.rydytrader.autotrader.indicator.Atr.series(bars, atrPeriod);
        double latestAtr = atrSeries.length > 0 ? atrSeries[atrSeries.length - 1] : 0;

        // Wick-crossover: bar range must STRADDLE VWAP — high above AND low
        // below. Filters out bars sitting entirely on one side of VWAP.
        boolean wickAtVwap      = bar.low()  <= bar.vwap() && bar.high() >= bar.vwap();
        boolean closeBelowVwap  = bar.close() < bar.vwap();
        boolean stUp            = st.available() && st.isUp();
        // ST-flip DOWN detection: green→red on THIS bar close. previousStUp=true
        // AND stUp=false = fresh flip down. Combined with closeBelowVwap so the
        // flip happens while price is under its session anchor.
        boolean stFlipDown = leg.previousStUp != null && leg.previousStUp && !stUp;

        // Log bars that are near-misses or fires — either the wick touched
        // VWAP OR ST just flipped down.
        if (wickAtVwap || stFlipDown) {
            log.info("[VwapSupertrend] {} {} bar close — o={} h={} l={} c={} vwap={} st_line={} st_up={} st_flip_down={} wick_at_vwap={} close_below_vwap={} legState={}",
                sideLabel, leg.chosenSymbol,
                fmt(bar.open()), fmt(bar.high()), fmt(bar.low()), fmt(bar.close()),
                fmt(bar.vwap()), fmt(st.line()), stUp, stFlipDown, wickAtVwap, closeBelowVwap, leg.state);
        }

        // Entry pathways — first match wins when leg is WAITING:
        //   A. VWAP_BREAKDOWN        — wick straddled VWAP AND close below AND ST down
        //   B. SUPER_TREND_FLIP_DOWN — ST just flipped green→red AND close below VWAP
        if (leg.state == LegState.WAITING) {
            if (wickAtVwap && closeBelowVwap && !stUp) {
                leg.entryReason  = "VWAP_BREAKDOWN";
                leg.atrAtEntry   = latestAtr;
                leg.stLineAtEntry = st.available() ? st.line() : 0;
                fireEntry(leg, sideLabel, bar);
            } else if (stFlipDown && closeBelowVwap) {
                leg.entryReason  = "SUPER_TREND_FLIP_DOWN";
                leg.atrAtEntry   = latestAtr;
                leg.stLineAtEntry = st.available() ? st.line() : 0;
                fireEntry(leg, sideLabel, bar);
            } else if (wickAtVwap && closeBelowVwap && stUp) {
                // VWAP-breakdown fired but Supertrend still green — surface a
                // skip event so the operator sees the near-miss.
                event("[WARNING]", "VwapST",
                    sideLabel + " " + leg.chosenSymbol + " entry SKIPPED — VWAP breakdown "
                        + "but Supertrend not aligned (st_up=true, st_line=" + fmt(st.line())
                        + " close=" + fmt(bar.close()) + " vwap=" + fmt(bar.vwap()) + ")");
            }
        }

        // Update ST direction tracker for the NEXT bar's flip check.
        // Must run after entry evaluation so THIS bar's flip fires only once.
        leg.previousStUp = stUp;

        // SL exit — 3-min bar CLOSE above the current slPrice (mirror of the
        // buy case). slPrice starts at ST line at entry (finalUpper, ABOVE
        // fill) and trails DOWN on ST descent. Trigger is bar close, not tick.
        // TRAILING_SL_HIT when the trail moved SL below initial (profit locked);
        // plain SL_HIT when the initial SL was still in force (loss).
        if (leg.state == LegState.IN_POSITION && leg.slPrice > 0
                && bar.close() > leg.slPrice) {
            boolean trailed = leg.initialSlPrice > 0 && leg.slPrice < leg.initialSlPrice;
            String reason = trailed ? "TRAILING_SL_HIT" : "SL_HIT";
            fireExit(leg, sideLabel, reason,
                "3-min close " + fmt(bar.close()) + " > SL " + fmt(leg.slPrice));
            return;
        }

        // Trailing SL — ratchet leg.slPrice DOWN to match the latest ST line
        // whenever ST is still down and the line has moved BELOW current slPrice.
        // Never widens — locked-in profit stays locked.
        if (leg.state == LegState.IN_POSITION && !stUp && st.available()) {
            double newSl = st.line();
            if (newSl < leg.slPrice) {
                double oldSl = leg.slPrice;
                leg.slPrice = newSl;
                if (round1(newSl) < round1(oldSl)) {
                    event("[INFO]", "VwapST",
                        sideLabel + " " + leg.chosenSymbol + " SL trailed — "
                            + fmt(oldSl) + " → " + fmt(newSl) + " (ST line)");
                }
                saveStateToDisk();
            }
        }
    }

    private static double round1(double v) { return Math.round(v * 10.0) / 10.0; }

    // ── Entry / exit ────────────────────────────────────────────────────────

    private void fireEntry(Leg leg, String sideLabel, Candle triggerBar) {
        // Start-time gate — no entries before the configured cutoff. Prep
        // (spot capture, pair pick, warmup) still runs at 09:15; this only
        // suppresses the order placement until the operator's chosen start.
        String startTime = riskSettings.getVwapStStartTime();
        if (startTime != null && !startTime.isBlank()) {
            try {
                LocalTime start = LocalTime.parse(startTime);
                if (ZonedDateTime.now(IST).toLocalTime().isBefore(start)) {
                    log.debug("[VwapSupertrend] {} entry skipped — wall clock < startTime {}",
                        sideLabel, startTime);
                    return;
                }
            } catch (Exception ignored) {}
        }
        // Trading-end gate — no NEW entries after this cutoff. Open positions
        // continue to be managed to SL / target / squareoff as normal; this
        // only blocks fresh entries so the strategy tapers off before the
        // hard squareoff time.
        String tradingEnd = riskSettings.getVwapStTradingEndTime();
        if (tradingEnd != null && !tradingEnd.isBlank()) {
            try {
                LocalTime end = LocalTime.parse(tradingEnd);
                if (!ZonedDateTime.now(IST).toLocalTime().isBefore(end)) {
                    event("[INFO]", "VwapST",
                        sideLabel + " " + leg.chosenSymbol + " entry SKIPPED — past trading end time "
                            + tradingEnd + " (new entries blocked; open positions still managed)");
                    return;
                }
            } catch (Exception ignored) {}
        }
        int lots = Math.max(1, riskSettings.getVwapStLotsPerLeg());
        int qty = lots * LOT_SIZE;
        try {
            // SELL to open — side = -1. Fyers requires MARGIN product for options
            // shorts. Position becomes SHORT; exit will BUY BACK.
            OrderDTO placed = orderService.placeOrder(leg.chosenSymbol, qty, -1, 0.0, "MARGIN");
            if (placed == null || placed.getId() == null || placed.getId().isBlank()) {
                event("[ERROR]", "VwapST",
                    sideLabel + " SELL placeOrder rejected — response=" + (placed == null ? "null" : placed.getMessage()));
                return;
            }
            leg.entryOrderId    = placed.getId();
            leg.entryCandleLow  = triggerBar.high();  // for shorts we track entry candle HIGH (fallback for SL)
            leg.slPrice         = 0;    // computed on fill (ST line at entry, above fill)
            leg.targetPrice     = 0;    // no fixed target — trail on ST
            leg.qty             = qty;
            leg.entryBarStartMs = triggerBar.startMillis();
            leg.state           = LegState.PENDING_ENTRY;
            event("[SUCCESS]", "VwapST",
                sideLabel + " SELL placed — sym=" + leg.chosenSymbol + " qty=" + qty
                    + " triggerBarStart=" + ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(triggerBar.startMillis()), IST).toLocalTime()
                    + " entryCandleHigh=" + fmt(leg.entryCandleLow) + " orderId=" + placed.getId());
            saveStateToDisk();
        } catch (Exception e) {
            event("[ERROR]", "VwapST", sideLabel + " SELL threw — " + e.getMessage());
        }
    }


    private synchronized void fireExit(Leg leg, String sideLabel, String reason, String detail) {
        if (leg.state == LegState.WAITING) return;
        String sym = leg.chosenSymbol;
        int qty = Math.max(1, leg.qty);
        double entry = leg.fillPrice;
        try {
            // BUY BACK to close a short — side = +1.
            OrderDTO placed = orderService.placeExitOrder(sym, qty, 1, "MARGIN");
            String orderId = placed != null ? placed.getId() : "";
            event("[WARNING]", "VwapST",
                sideLabel + " BUY-BACK placed — reason=" + reason
                    + " qty=" + qty + " orderId=" + orderId
                    + " detail=" + detail);
            // Short P&L: (entry - exit) × qty (premium dropped = profit).
            double exitLtp = marketDataService.getLtp(sym);
            if (exitLtp > 0 && entry > 0) {
                double pnl = (entry - exitLtp) * qty;
                realisedPnlToday.updateAndGet(v -> v + pnl);
                long id = System.currentTimeMillis();
                String setup = leg.entryReason == null ? "VWAP+ST" : leg.entryReason;
                tradesTodayById.put(id, new ClosedTrade(sideLabel, sym, entry, exitLtp, qty, id, reason, setup, leg.entryBarStartMs));
                Long dbRowId = persistTradeRow(sideLabel, sym, entry, exitLtp, qty, id, reason, leg);
                // Register the pending exit so onOrderFill can refine the
                // recorded exit price once Fyers confirms the actual trade.
                if (orderId != null && !orderId.isBlank()) {
                    pendingExitsByOrderId.put(orderId,
                        new PendingExit(id, sideLabel, sym, qty, entry, reason, exitLtp, dbRowId));
                }
            }
        } catch (Exception e) {
            event("[ERROR]", "VwapST", sideLabel + " EXIT threw — " + e.getMessage());
        } finally {
            leg.reset();
            saveStateToDisk();
        }
    }

    // ── Fill listener ───────────────────────────────────────────────────────

    void onOrderFill(String orderId, double fillPrice) {
        if (orderId == null) return;
        synchronized (this) {
            if (orderId.equals(ceLeg.entryOrderId) && ceLeg.state == LegState.PENDING_ENTRY) {
                applyFill(ceLeg, "CE", fillPrice);
                return;
            }
            if (orderId.equals(peLeg.entryOrderId) && peLeg.state == LegState.PENDING_ENTRY) {
                applyFill(peLeg, "PE", fillPrice);
                return;
            }
            // Exit-order fill — refine the recorded exit price from the LTP
            // approximation to the actual Fyers-reported trade price.
            PendingExit pending = pendingExitsByOrderId.remove(orderId);
            if (pending != null) {
                refineExitFill(pending, orderId, fillPrice);
            }
        }
    }

    /** Runs when Fyers's fill event lands for a previously-placed exit order.
     *  Corrects the recorded exit price on the in-memory ClosedTrade + the
     *  DB row + realisedPnlToday (delta between LTP approximation and the
     *  actual fill). */
    private void refineExitFill(PendingExit pending, String orderId, double actualFill) {
        double approx = pending.approxExit();
        if (actualFill <= 0 || Math.abs(actualFill - approx) < 0.005) {
            log.debug("[VwapSupertrend] exit fill matches approx — orderId={} price={}", orderId, actualFill);
            return;
        }
        // Short P&L: (entry - exit) × qty.
        double approxPnl = (pending.entry() - approx) * pending.qty();
        double actualPnl = (pending.entry() - actualFill) * pending.qty();
        double delta = actualPnl - approxPnl;
        realisedPnlToday.updateAndGet(v -> v + delta);
        // In-memory ClosedTrade row.
        ClosedTrade old = tradesTodayById.get(pending.tradeMs());
        if (old != null) {
            tradesTodayById.put(pending.tradeMs(), new ClosedTrade(
                old.side(), old.symbol(), old.entry(), actualFill,
                old.qty(), old.closedMs(), old.reason(), old.setup(), old.openedMs()));
        }
        // DB row.
        if (pending.dbRowId() != null) {
            try {
                tradeRepository.findById(pending.dbRowId()).ifPresent(row -> {
                    row.setExitPrice(actualFill);
                    double gross   = (pending.entry() - actualFill) * pending.qty();   // short P&L
                    double charges = computeChargesForTrade(pending.entry(), actualFill, pending.qty());
                    row.setGrossPnl(gross);
                    row.setCharges(charges);
                    row.setNetPnl(gross - charges);
                    tradeRepository.save(row);
                });
            } catch (Exception e) {
                log.warn("[VwapSupertrend] refineExitFill DB update failed: {}", e.getMessage());
            }
        }
        event("[INFO]", "VwapST",
            pending.side() + " EXIT fill refined — approx=" + fmt(approx)
                + " actual=" + fmt(actualFill)
                + " P&L delta=" + fmt(delta)
                + " orderId=" + orderId);
    }

    /** Captures fill price and derives SL. Called once per leg per entry,
     *  inside the class monitor.
     *
     *  <p>Initial SL = ST line at entry. Trail on subsequent 3-min closes
     *  ratchets slPrice up to match the rising ST line. Exit fires when a
     *  3-min bar closes below the current slPrice ({@link #onBarClose}).
     *
     *  <p>No fixed target — trade rides on the trailing SL. */
    private void applyFill(Leg leg, String sideLabel, double fillPrice) {
        leg.fillPrice = fillPrice;
        // Initial SL = ST line at entry (finalUpper — ABOVE fill for shorts).
        // If ST wasn't available (shouldn't happen — entry requires !stUp),
        // fall back to a small buffer above the entry candle high so we
        // never place an SL == fill.
        double sl = leg.stLineAtEntry > 0 && leg.stLineAtEntry > fillPrice
            ? leg.stLineAtEntry
            : leg.entryCandleLow + 1;  // entryCandleLow field carries entry candle HIGH for shorts
        leg.slPrice = sl;
        leg.initialSlPrice = sl;
        double risk = Math.max(0, sl - fillPrice);
        leg.targetPrice = 0;   // RIDE — no fixed target, exit via ST flip.
        leg.state = LegState.IN_POSITION;
        event("[SUCCESS]", "VwapST",
            sideLabel + " SELL FILL — sym=" + leg.chosenSymbol + " @ " + fmt(fillPrice)
                + " slPrice=" + fmt(leg.slPrice) + " (ST line, above fill)"
                + " target=RIDE risk=" + fmt(risk));
        saveStateToDisk();
    }

    // ── Rollover ────────────────────────────────────────────────────────────

    private synchronized void rolloverIfNewDay(String today) {
        todayKey = today;
        sessionDate = "";   // cleared until this day's spot-open capture sets it
        ceLeg.reset();
        peLeg.reset();
        // reset() doesn't clear chosenSymbol (it's the leg's identity, not
        // per-position state). On day rollover we DO want to drop it —
        // otherwise the chart keeps rendering yesterday's strike using
        // yesterday's bars until the new 09:15 pair pick fires.
        ceLeg.chosenSymbol  = null;
        peLeg.chosenSymbol  = null;
        ceLeg.previousStUp  = null;
        peLeg.previousStUp  = null;
        spotOpen = 0;
        atmStrike = 0;
        strikesSubscribedAtMs = 0;
        preMarketSubscribedToday = false;
        preMarketAtm = 0;
        niftyPivot = 0;
        subscribedStrikes.clear();
        fsm = FsmState.BOOT;
        realisedPnlToday.set(0.0);
        tradesTodayById.clear();
        log.info("[VwapSupertrend] rolled over to new day {} — waiting for spot open tick", today);
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id()           { return "vwap-supertrend"; }
    @Override public String displayName()  { return "VWAP + Supertrend"; }
    @Override public String description()  {
        return "Sell ATM CE/PE on NIFTY weekly. Entry: VWAP breakdown or ST flip DOWN on option's chart. SL = ST line (above fill), trails DOWN on 3-min close. Exit = bar close > SL.";
    }
    @Override public String currentState() {
        return fsm.name() + " (CE=" + ceLeg.state.name() + ", PE=" + peLeg.state.name() + ")";
    }
    @Override public boolean isEnabled()   { return riskSettings.isVwapStEnabled(); }
    @Override public double  liveNetPnlToday() {
        double closed = realisedPnlToday.get() == null ? 0 : realisedPnlToday.get();
        // Short position MTM = (fillPrice - ltp) × qty. Premium drop = profit.
        double open = 0;
        if (ceLeg.state == LegState.IN_POSITION && ceLeg.chosenSymbol != null) {
            double ltp = marketDataService.getLtp(ceLeg.chosenSymbol);
            if (ltp > 0 && ceLeg.fillPrice > 0) open += (ceLeg.fillPrice - ltp) * ceLeg.qty;
        }
        if (peLeg.state == LegState.IN_POSITION && peLeg.chosenSymbol != null) {
            double ltp = marketDataService.getLtp(peLeg.chosenSymbol);
            if (ltp > 0 && peLeg.fillPrice > 0) open += (peLeg.fillPrice - ltp) * peLeg.qty;
        }
        // Subtract today's brokerage on closed trades so the header ticker
        // and the positions-page P&L card agree. Both are now:
        //   (realised gross) + (open-position gross MTM) − (brokerage on
        //    closed trades so far). Open positions still contribute gross
        //   MTM — their charges materialise on exit.
        return closed + open - liveChargesToday();
    }
    @Override public double liveChargesToday() {
        double sum = 0;
        for (ClosedTrade t : tradesTodayById.values()) {
            sum += computeChargesForTrade(t.entry(), t.exit(), t.qty());
        }
        return sum;
    }
    /** Current NIFTY daily floor pivot (0 until fetched at pre-market). */
    public double getNiftyPivot() { return niftyPivot; }
    /** Daily-bias tag derived from live NIFTY LTP vs pivot. Returns
     *  BULLISH / BEARISH / NEUTRAL (pivot or LTP unavailable). Independent
     *  of the bias-filter setting — always computed for UI display. */
    public String getBias() {
        if (niftyPivot <= 0) return "NEUTRAL";
        double niftyLtp = marketDataService.getLtp(NIFTY_SPOT_SYM);
        if (niftyLtp <= 0) return "NEUTRAL";
        if (niftyLtp > niftyPivot) return "BULLISH";
        if (niftyLtp < niftyPivot) return "BEARISH";
        return "NEUTRAL";
    }

    /** Per-cycle charges: brokerage (both sides) + STT (sell only) +
     *  exchange transaction (both sides) + SEBI turnover fee + stamp
     *  duty (buy only) + GST on (brokerage + exchange + SEBI). Rates
     *  come from RiskSettingsStore — set them once for your instrument
     *  in the Risk / Charges tab. Percent-valued fields divide by 100;
     *  SEBI's ₹-per-crore divides by 1e7. */
    private double computeChargesForTrade(double entry, double exit, int qty) {
        if (qty <= 0 || entry <= 0 || exit <= 0) return 0;
        double buyNotional  = entry * qty;
        double sellNotional = exit  * qty;
        double turnover     = buyNotional + sellNotional;
        double brokerage    = riskSettings.getBrokeragePerOrder() * 2;   // buy + sell
        double stt          = sellNotional * riskSettings.getSttRate()       / 100.0;
        double exchTxn      = turnover     * riskSettings.getExchangeRate()  / 100.0;
        double sebi         = turnover     * riskSettings.getSebiRate()      / 10_000_000.0;
        double stampDuty    = buyNotional  * riskSettings.getStampDutyRate() / 100.0;
        double gst          = (brokerage + exchTxn + sebi) * riskSettings.getGstRate() / 100.0;
        return brokerage + stt + exchTxn + sebi + stampDuty + gst;
    }
    @Override public List<Map<String, Object>> todayClosedTrades() {
        List<Map<String, Object>> out = new ArrayList<>(tradesTodayById.size());
        for (ClosedTrade t : tradesTodayById.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            double gross   = (t.entry - t.exit) * t.qty;   // short P&L
            double charges = computeChargesForTrade(t.entry, t.exit, t.qty);
            m.put("setup",          t.setup);
            m.put("side",           t.side);
            m.put("symbol",         t.symbol);
            m.put("qty",            t.qty);
            m.put("entryPrice",     t.entry);
            m.put("exitPrice",      t.exit);
            m.put("grossPnl",       gross);
            m.put("charges",        charges);
            m.put("netPnl",         gross - charges);
            m.put("closedAtMillis", t.closedMs);
            m.put("openedAtMillis", t.openedMs);
            m.put("closeReason",    t.reason);
            out.add(m);
        }
        return out;
    }
    @Override public synchronized boolean forceClose(String reason) {
        boolean acted = false;
        if (ceLeg.state != LegState.WAITING) {
            fireExit(ceLeg, "CE", "FORCE_" + reason, reason);
            acted = true;
        }
        if (peLeg.state != LegState.WAITING) {
            fireExit(peLeg, "PE", "FORCE_" + reason, reason);
            acted = true;
        }
        fsm = FsmState.DONE_FOR_DAY;
        return acted;
    }
    @Override public synchronized void resetToIdle(String reason) {
        ceLeg.reset();
        peLeg.reset();
        fsm = spotOpen > 0 ? FsmState.ARMED : FsmState.BOOT;
        event("[INFO]", "VwapST", "Reset to idle — " + reason);
    }

    // ── Public accessors for chart endpoint ─────────────────────────────────

    public String getChosenCeSymbol() { return ceLeg.chosenSymbol; }
    public String getChosenPeSymbol() { return peLeg.chosenSymbol; }
    public double getSpotOpen()       { return spotOpen; }
    public long   getAtmStrike()      { return atmStrike; }

    /** Per-symbol strategy-computed levels for the live positions table.
     *  Returns a snapshot of { entryPrice, slPrice, targetPrice, side } for
     *  the requested Fyers symbol, or empty when the symbol isn't tracked
     *  or the leg isn't in position. */
    public java.util.Map<String, Object> getLegSnapshot(String fyersSymbol) {
        java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
        if (fyersSymbol == null) return m;
        Leg leg = null; String side = null;
        if (fyersSymbol.equals(ceLeg.chosenSymbol)) { leg = ceLeg; side = "CE"; }
        else if (fyersSymbol.equals(peLeg.chosenSymbol)) { leg = peLeg; side = "PE"; }
        if (leg == null) return m;
        m.put("side",        side);
        m.put("entryPrice",  leg.fillPrice);
        m.put("slPrice",     leg.slPrice);
        m.put("initialSlPrice", leg.initialSlPrice);
        // For shorts: trail moves SL DOWN (toward and past fill). Trailed=true
        // once slPrice < initialSlPrice (SL tightened).
        m.put("slTrailed",   leg.initialSlPrice > 0 && leg.slPrice < leg.initialSlPrice);
        m.put("targetPrice", leg.targetPrice);
        m.put("legState",    leg.state.name());
        // Full setup label (pathway + side) matching the persisted trade row,
        // so /positions and /trades show the same '<pathway> CE|PE' string.
        String pathway = leg.entryReason == null ? "VWAP+ST" : leg.entryReason;
        m.put("entryReason", leg.entryReason);
        m.put("setup",       pathway + " " + side);
        return m;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private String authHeader() {
        return fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
    }
    private static String fmt(double v) {
        return String.format("%.1f", v);
    }
    private void event(String level, String tag, String msg) {
        eventService.log(level + " [" + tag + "] " + msg);
    }

    /** Reads today's closed vwap-supertrend rows from strategy_trades and
     *  re-populates the in-memory tradesTodayById + realisedPnlToday.
     *  Called on state restore so a mid-day restart doesn't lose today's
     *  P&L on the positions page (the DB has them; only the in-memory ring
     *  starts empty). Best-effort — a repo error is logged and swallowed. */
    private void rehydrateTodayClosedTradesFromDb() {
        try {
            String today = LocalDate.now(IST).toString();
            List<StrategyTradeEntity> rows = tradeRepository
                .findByStrategyIdAndSessionDateOrderByClosedAtMillisAsc("vwap-supertrend", today);
            if (rows == null || rows.isEmpty()) return;
            double sum = 0;
            for (StrategyTradeEntity e : rows) {
                if (e == null) continue;
                double entry = e.getEntryPrice() == null ? 0 : e.getEntryPrice();
                double exit  = e.getExitPrice()  == null ? 0 : e.getExitPrice();
                int    qty   = e.getQty();
                long   ms    = e.getClosedAtMillis();
                String setup = e.getSetup()       == null ? "VWAP+ST" : e.getSetup();
                // Extract side ('CE' or 'PE') from '<pathway> CE|PE' — fall back
                // to blank if we can't parse.
                String side = "";
                int sp = setup.lastIndexOf(' ');
                if (sp >= 0 && sp < setup.length() - 1) side = setup.substring(sp + 1);
                String reason = e.getCloseReason() == null ? "" : e.getCloseReason();
                long openedMs = e.getOpenedAtMillis() == null ? 0 : e.getOpenedAtMillis();
                tradesTodayById.put(ms,
                    new ClosedTrade(side, e.getSymbol(), entry, exit, qty, ms, reason, setup, openedMs));
                sum += (exit - entry) * qty;
            }
            realisedPnlToday.set(sum);
            log.info("[VwapSupertrend] rehydrated {} closed trades from DB — realisedPnlToday={}",
                rows.size(), sum);
        } catch (Exception e) {
            log.warn("[VwapSupertrend] rehydrateTodayClosedTradesFromDb failed: {}", e.getMessage());
        }
    }

    /** Persists a single-leg closed trade to the strategy_trades table so it
     *  shows up on /trades. Called from fireExit once we have the exit LTP.
     *  Charges include brokerage + STT + exchange + SEBI + stamp duty + GST
     *  via {@link #computeChargesForTrade}. */
    private Long persistTradeRow(String side, String sym, double entry, double exit,
                                  int qty, long closedMs, String reason, Leg leg) {
        try {
            double gross   = (entry - exit) * qty;   // short P&L
            double charges = computeChargesForTrade(entry, exit, qty);
            double net     = gross - charges;
            StrategyTradeEntity e = new StrategyTradeEntity();
            e.setStrategyId("vwap-supertrend");
            e.setSymbol(sym);
            String pathway = leg.entryReason == null ? "VWAP+ST" : leg.entryReason;
            e.setSetup(pathway);
            e.setInstrument("OPT");
            e.setSessionDate(LocalDate.now(IST).toString());
            e.setClosedAtMillis(closedMs);
            e.setOpenedAtMillis(leg.entryBarStartMs > 0 ? leg.entryBarStartMs : closedMs);
            e.setQty(qty);
            e.setEntryPrice(entry);
            e.setExitPrice(exit);
            e.setGrossPnl(gross);
            e.setCharges(charges);
            e.setNetPnl(net);
            e.setCloseReason(reason);
            e.setEntryCandleMs(leg.entryBarStartMs > 0 ? leg.entryBarStartMs : null);
            e.setExitCandleMs(closedMs);
            StrategyTradeEntity saved = tradeRepository.save(e);
            return saved == null ? null : saved.getId();
        } catch (Exception ex) {
            log.warn("[VwapSupertrend] persistTradeRow failed: {}", ex.getMessage());
            return null;
        }
    }

    /** Wipes today's in-memory trade tracking — the ClosedTrade ring backing
     *  {@link #todayClosedTrades()} and the realised-P&L counter. Called by
     *  the Maintenance / Clear All action so /positions and P&L displays
     *  reset alongside the DB wipe. FSM + leg state are NOT touched (open
     *  positions on Fyers continue to be managed). */
    public synchronized void clearInMemoryTradeState() {
        tradesTodayById.clear();
        realisedPnlToday.set(0.0);
    }

    /** Re-runs the history warmup for both currently-chosen legs. Callable
     *  from ViewController after a fresh /fyers/callback so ST can populate
     *  from 09:15 without needing an app restart when the previous warmup
     *  failed on an expired token. Safe to call at any time — pass-through
     *  when no leg has a chosen symbol yet. */
    public synchronized void reWarmupChosenLegs() {
        if (ceLeg.chosenSymbol != null) warmupHistory(ceLeg.chosenSymbol, "CE");
        if (peLeg.chosenSymbol != null) warmupHistory(peLeg.chosenSymbol, "PE");
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /** Persisted snapshot — everything needed to resume a mid-day restart
     *  without re-picking strikes or losing live-leg position state. */
    public static class PersistedState {
        /** IST date of the last save (mostly informational). Not used to
         *  detect stale state — that's what {@link #sessionDate} is for. */
        public String    dayKey = "";
        /** IST date when the strategy last transitioned into an active FSM
         *  (STRIKES_SUBSCRIBING / ARMED) — i.e. the actual trading day the
         *  persisted CE/PE + leg state belongs to. Set only when the bot
         *  captures a real spot open. Used to distinguish "state written by
         *  the periodic save 15 min after IST midnight while the FSM was
         *  DONE_FOR_DAY from yesterday" (stale — sessionDate < today) from
         *  "state written mid-day today" (valid — sessionDate == today). */
        public String    sessionDate = "";
        public String    fsm = "BOOT";
        public double    spotOpen;
        public long      atmStrike;
        public long      strikesSubscribedAtMs;
        public boolean   preMarketSubscribedToday;
        public long      preMarketAtm;
        public double    niftyPivot;
        public java.util.Set<String> subscribedStrikes = new java.util.HashSet<>();
        public double    realisedPnlToday;
        public PersistedLeg ceLeg = new PersistedLeg();
        public PersistedLeg peLeg = new PersistedLeg();
    }
    public static class PersistedLeg {
        public String chosenSymbol;
        public String state = "WAITING";
        public String entryOrderId;
        public double fillPrice;
        public double entryCandleLow;
        public double slPrice;
        public double initialSlPrice;
        public double targetPrice;
        public int    qty;
        public long   entryBarStartMs;
    }

    /** Reads {@link #STATE_FILE} and, if today's dayKey matches, restores every
     *  field of the FSM + both legs. Returns true when state was loaded. */
    private synchronized boolean loadStateFromDisk() {
        try {
            java.nio.file.Path p = java.nio.file.Path.of(STATE_FILE);
            if (!java.nio.file.Files.exists(p)) return false;
            PersistedState s = mapper.readValue(java.nio.file.Files.readString(p), PersistedState.class);
            if (s == null) return false;
            String today = LocalDate.now(IST).toString();
            // Freshness gate uses sessionDate (the day spot-open was captured),
            // NOT dayKey (which is rewritten by every periodic save and thus
            // rolls over silently at IST midnight even when the FSM is stale).
            // Empty sessionDate = state saved before spot-open ever fired
            // today; also discard so we don't restore a half-initialised FSM.
            if (s.sessionDate == null || s.sessionDate.isBlank() || !today.equals(s.sessionDate)) {
                log.info("[VwapSupertrend] discarding stale state — sessionDate={} today={} dayKey={}",
                    s.sessionDate, today, s.dayKey);
                return false;
            }
            sessionDate = s.sessionDate;
            try { fsm = FsmState.valueOf(s.fsm); } catch (Exception e) { fsm = FsmState.BOOT; }
            spotOpen                 = s.spotOpen;
            atmStrike                = s.atmStrike;
            strikesSubscribedAtMs    = s.strikesSubscribedAtMs;
            preMarketSubscribedToday = s.preMarketSubscribedToday;
            preMarketAtm             = s.preMarketAtm;
            niftyPivot               = s.niftyPivot;
            if (s.subscribedStrikes != null) subscribedStrikes.addAll(s.subscribedStrikes);
            realisedPnlToday.set(s.realisedPnlToday);
            restoreLeg(ceLeg, s.ceLeg);
            restoreLeg(peLeg, s.peLeg);
            return true;
        } catch (Exception e) {
            log.warn("[VwapSupertrend] failed to load state: {}", e.getMessage());
            return false;
        }
    }
    private static void restoreLeg(Leg leg, PersistedLeg s) {
        if (s == null) return;
        leg.chosenSymbol    = s.chosenSymbol;
        try { leg.state = LegState.valueOf(s.state); } catch (Exception e) { leg.state = LegState.WAITING; }
        leg.entryOrderId    = s.entryOrderId;
        leg.fillPrice       = s.fillPrice;
        leg.entryCandleLow  = s.entryCandleLow;
        leg.slPrice         = s.slPrice;
        // Older persisted snapshots don't carry initialSlPrice — fall back to
        // current slPrice so the trailed-check treats it as "not yet trailed".
        leg.initialSlPrice  = s.initialSlPrice > 0 ? s.initialSlPrice : s.slPrice;
        leg.targetPrice     = s.targetPrice;
        leg.qty             = s.qty;
        leg.entryBarStartMs = s.entryBarStartMs;
    }

    /** Writes {@link #STATE_FILE} atomically. Called on state-change checkpoints
     *  (spot open capture, pair pick, entry, fill, exit) and by a 30-s scheduled
     *  sweep so a crash between checkpoints loses at most 30 s of drift. */
    private synchronized void saveStateToDisk() {
        try {
            PersistedState s = new PersistedState();
            s.dayKey                   = LocalDate.now(IST).toString();
            s.sessionDate              = sessionDate;
            s.fsm                      = fsm.name();
            s.spotOpen                 = spotOpen;
            s.atmStrike                = atmStrike;
            s.strikesSubscribedAtMs    = strikesSubscribedAtMs;
            s.preMarketSubscribedToday = preMarketSubscribedToday;
            s.preMarketAtm             = preMarketAtm;
            s.niftyPivot               = niftyPivot;
            s.subscribedStrikes        = new java.util.HashSet<>(subscribedStrikes);
            s.realisedPnlToday         = realisedPnlToday.get() == null ? 0 : realisedPnlToday.get();
            s.ceLeg = snapshotLeg(ceLeg);
            s.peLeg = snapshotLeg(peLeg);
            java.nio.file.Path dst = java.nio.file.Path.of(STATE_FILE);
            java.io.File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            java.nio.file.Path tmp = java.nio.file.Path.of(STATE_FILE + ".tmp");
            java.nio.file.Files.writeString(tmp, mapper.writeValueAsString(s));
            try {
                java.nio.file.Files.move(tmp, dst,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception atomicFail) {
                java.nio.file.Files.move(tmp, dst,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception e) {
            log.warn("[VwapSupertrend] failed to save state: {}", e.getMessage());
        }
    }
    private static PersistedLeg snapshotLeg(Leg leg) {
        PersistedLeg s = new PersistedLeg();
        s.chosenSymbol    = leg.chosenSymbol;
        s.state           = leg.state.name();
        s.entryOrderId    = leg.entryOrderId;
        s.fillPrice       = leg.fillPrice;
        s.entryCandleLow  = leg.entryCandleLow;
        s.slPrice         = leg.slPrice;
        s.initialSlPrice  = leg.initialSlPrice;
        s.targetPrice     = leg.targetPrice;
        s.qty             = leg.qty;
        s.entryBarStartMs = leg.entryBarStartMs;
        return s;
    }

    @org.springframework.scheduling.annotation.Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void periodicSave() {
        saveStateToDisk();
    }
}
