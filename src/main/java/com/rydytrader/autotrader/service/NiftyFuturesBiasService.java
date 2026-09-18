package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.indicator.ChoppinessIndex;
import com.rydytrader.autotrader.indicator.SuperTrend;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.util.NiftyExpiryResolver;
import com.rydytrader.autotrader.util.NiftyFuturesSymbolBuilder;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Tracks NIFTY near-month futures Supertrend(10, 3) on a 3-min chart to drive
 * the options-selling directional bias filter.
 *
 * <p>Bias rule:
 * <ul>
 *   <li>Futures ST UP → market bullish → CE premium rising → skip CE sells.</li>
 *   <li>Futures ST DOWN → market bearish → PE premium rising → skip PE sells.</li>
 * </ul>
 *
 * <p>Self-contained lifecycle: resolves near-month futures symbol at boot,
 * subscribes to the tick feed (which the existing pipeline aggregates into
 * 1-min bars via {@link FyersMinuteBarBuilder} → {@link CandleAggregator}),
 * fetches prior-session bars for immediate ST availability at 09:15, and
 * updates its cached ST direction on each 3-min bar close.
 *
 * <p>Symbol auto-rolls: if today is past the current-month expiry, the next
 * month's contract is used. Rollover is re-run on daily wake.
 */
@Service
public class NiftyFuturesBiasService {

    private static final Logger log = LoggerFactory.getLogger(NiftyFuturesBiasService.class);
    private static final ZoneId  IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;

    private final MarketDataService  marketDataService;
    private final CandleAggregator   candleAggregator;
    private final RiskSettingsStore  riskSettings;
    private final FyersClientRouter  fyersClient;
    private final TokenStore         tokenStore;
    private final FyersProperties    fyersProperties;
    private final EventService       eventService;

    @Autowired @Lazy private MarketHolidayService holidays;

    /** Human-readable status of the last computation attempt — surfaced in the
     *  BIAS chip tooltip so the operator can tell WHY bias is NEUTRAL. */
    private volatile String status = "not yet initialized";

    /** Currently tracked futures symbol (e.g. NSE:NIFTY26SEPFUT). Null before init. */
    private volatile String futuresSymbol;

    /** Three-state directional bias derived from BOTH the ST direction AND
     *  the close-vs-VWAP relationship on the most recent 3-min bar:
     *  <ul>
     *    <li>{@link Bias#BULLISH} — ST green AND close &gt; VWAP</li>
     *    <li>{@link Bias#BEARISH} — ST red AND close &lt; VWAP</li>
     *    <li>{@link Bias#NEUTRAL} — everything else (mixed signals, or data
     *        not yet available — treated the same by the filter which blocks
     *        BOTH sides on NEUTRAL)</li>
     *  </ul> */
    public enum Bias { BULLISH, BEARISH, NEUTRAL }

    /** Cached ST direction from the most recent 3-min bar close.
     *  null = data not yet available (subscribe fresh / bars insufficient).
     *  true = ST up (bullish leg), false = ST down (bearish leg). */
    private volatile Boolean stUp;
    private volatile double  stLine;
    private volatile double  lastBarClose;
    private volatile double  lastBarVwap;
    private volatile long    lastBarStartMs;
    /** Cached composite bias — recomputed on every evaluateSt. Defaults to
     *  NEUTRAL until the first successful evaluation. */
    private volatile Bias    bias = Bias.NEUTRAL;

    /** Choppiness Index over the trailing {@link #CHOPPINESS_PERIOD} 3-min bars.
     *  NaN until enough bars are available. Recomputed on every evaluateSt. */
    private volatile double  choppinessIndex = Double.NaN;
    private volatile ChoppinessIndex.Regime choppinessRegime = ChoppinessIndex.Regime.UNAVAILABLE;
    /** 14 is Dreiss's original recommendation and the widely-published default. */
    private static final int CHOPPINESS_PERIOD = 14;
    /** IST date of the last successful ST evaluation. Used to detect stale state
     *  across daily rollover. */
    private volatile String  lastComputedDay = "";

    public NiftyFuturesBiasService(MarketDataService marketDataService,
                                    CandleAggregator candleAggregator,
                                    RiskSettingsStore riskSettings,
                                    FyersClientRouter fyersClient,
                                    TokenStore tokenStore,
                                    FyersProperties fyersProperties,
                                    EventService eventService) {
        this.marketDataService = marketDataService;
        this.candleAggregator  = candleAggregator;
        this.riskSettings      = riskSettings;
        this.fyersClient       = fyersClient;
        this.tokenStore        = tokenStore;
        this.fyersProperties   = fyersProperties;
        this.eventService      = eventService;
    }

    @PostConstruct
    public void boot() {
        try {
            resolveSymbolAndSubscribe();
        } catch (Exception e) {
            status = "boot failed: " + e.getClass().getSimpleName() + ": " + e.getMessage();
            log.warn("[NiftyFuturesBias] boot failed: {}", e.getMessage());
            event("[ERROR]", status);
        }
    }

    /** Called from the Fyers OAuth callback right after a fresh access token is
     *  minted. If the service was stuck at "warming up" because /data/history
     *  was returning code -16 (auth expired), this re-runs subscribe + warmup
     *  with the new token so the ST reading arms immediately — no waiting on
     *  the periodic retry. Idempotent when the service is already armed. */
    public void reWarmupAfterTokenRefresh() {
        try {
            if (futuresSymbol == null) {
                resolveSymbolAndSubscribe();
            } else {
                warmupHistory(futuresSymbol);
            }
        } catch (Exception e) {
            log.warn("[NiftyFuturesBias] reWarmupAfterTokenRefresh threw: {}", e.getMessage());
        }
    }

    /** Retry the warmup every 5 min while the ST reading is unarmed. Safety
     *  net for transient Fyers hiccups; the primary re-warm path is the OAuth
     *  callback in ViewController which fires the moment a fresh token is
     *  minted. Silently no-ops once armed. */
    @Scheduled(fixedDelay = 300_000, initialDelay = 60_000)
    public void periodicRetry() {
        try {
            if (holidays != null && !holidays.isTradingDay()) return;
            if (futuresSymbol == null) {
                resolveSymbolAndSubscribe();
                return;
            }
            // Armed today already — nothing to do.
            String today = LocalDate.now(IST).toString();
            if (today.equals(lastComputedDay) && stUp != null) return;
            warmupHistory(futuresSymbol);
        } catch (Exception e) {
            log.warn("[NiftyFuturesBias] periodicRetry threw: {}", e.getMessage());
        }
    }

    /** Idempotent — safe to call at boot and on daily rollover. Resolves the
     *  near-month futures expiry, subscribes to its tick feed, warms up
     *  history, and wires the bar-close listener. */
    public synchronized void resolveSymbolAndSubscribe() {
        LocalDate today = LocalDate.now(IST);
        LocalDate expiry = NiftyExpiryResolver.currentMonthlyFuturesExpiry(today, holidays);
        String sym = NiftyFuturesSymbolBuilder.buildFyersSymbol(expiry);
        if (sym.equals(futuresSymbol)) return;  // already subscribed
        futuresSymbol = sym;
        stUp = null;   // reset until we get bars
        marketDataService.subscribeAdditional(Collections.singletonList(sym));
        candleAggregator.subscribe(sym, this::onBarClose);
        log.info("[NiftyFuturesBias] tracking {} (expiry {})", sym, expiry);
        event("[INFO]", "Tracking " + sym + " (expiry " + expiry + ") for bias filter");
        // Warmup — pull prior-session 1-min bars so ST is valid from 09:15.
        warmupHistory(sym);
    }

    private synchronized void warmupHistory(String sym) {
        String from = "";
        String to   = "";
        try {
            LocalDate today = LocalDate.now(IST);
            from = today.minusDays(7).format(ISO_DATE);
            to   = today.format(ISO_DATE);
            JsonNode resp = fyersClient.getHistory(sym, "1", from, to, authHeader());
            if (resp == null) {
                status = "warmup null response from Fyers /data/history";
                log.warn("[NiftyFuturesBias] {}", status);
                event("[WARNING]", status);
                return;
            }
            // Fyers error envelope — surface auth failures loudly.
            if ("error".equals(resp.path("s").asText(""))) {
                int    code = resp.path("code").asInt(0);
                String msg  = resp.path("message").asText("");
                status = "Fyers error s=error code=" + code + " message=" + msg + " (sym=" + sym + ")";
                log.warn("[NiftyFuturesBias] {}", status);
                String level = code == -16 ? "[ERROR]" : "[WARNING]";
                String uiMsg = code == -16
                    ? "Fyers auth expired — futures bias UNAVAILABLE. Re-login at /fyers/login."
                    : "Fyers /data/history error code=" + code + " msg=" + msg;
                event(level, uiMsg);
                return;
            }
            JsonNode candles = resp.path("candles");
            if (!candles.isArray() || candles.size() == 0) {
                status = "warmup returned empty candles array (sym=" + sym + " from=" + from + " to=" + to + ")";
                log.warn("[NiftyFuturesBias] {}", status);
                event("[WARNING]", "Futures history empty for " + sym + " — bias UNAVAILABLE");
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
            log.info("[NiftyFuturesBias] warmed up {} bars for {}", bars.size(), sym);
            // Compute initial ST from the warmed-up history so the filter is
            // armed the instant subscription starts, without waiting for the
            // first live 3-min close.
            evaluateSt();
            if (stUp != null) {
                event("[SUCCESS]",
                    "Futures bias ARMED — " + sym + "  close=" + round2(lastBarClose)
                        + "  ST=" + round2(stLine) + "  " + (stUp ? "BULLISH" : "BEARISH"));
            }
        } catch (Exception e) {
            status = "warmup threw: " + e.getClass().getSimpleName() + ": " + e.getMessage();
            log.warn("[NiftyFuturesBias] {} (sym={} from={} to={})", status, sym, from, to);
            event("[ERROR]", "Futures warmup threw for " + sym + " — " + e.getMessage());
        }
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private void event(String level, String msg) {
        try { eventService.log(level + " [FuturesBias] " + msg); } catch (Exception ignored) {}
    }

    /** Fires on every 1-min bar close for the futures symbol. Only re-evaluates
     *  ST at the 3-min bucket boundary to match the options strategy's cadence. */
    private void onBarClose(Candle bar) {
        // Timeframe gate — evaluate only at 3-min bucket close (matching option strategy).
        long istMs = bar.startMillis() + 19_800_000L;
        int minuteOfDay = (int) ((istMs % 86_400_000L) / 60_000L);
        // Aligned on 09:15 → minute 555. 3-min bucket close = last 1-min bar of bucket.
        if ((minuteOfDay - (9 * 60 + 15)) % 3 != 2) return;
        evaluateSt();
    }

    private synchronized void evaluateSt() {
        if (futuresSymbol == null) {
            status = "futures symbol not resolved yet";
            bias = Bias.NEUTRAL;
            choppinessIndex = Double.NaN;
            choppinessRegime = ChoppinessIndex.Regime.UNAVAILABLE;
            return;
        }
        int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
        double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
        List<Candle> bars = candleAggregator.getHistory(futuresSymbol, 3);
        if (bars == null || bars.size() < atrPeriod + 1) {
            status = "insufficient 3-min bars for ST (have " + (bars == null ? 0 : bars.size())
                + ", need " + (atrPeriod + 1) + ")";
            bias = Bias.NEUTRAL;
            return;
        }
        SuperTrend.State st = SuperTrend.at(bars, atrPeriod, mult);
        if (!st.available()) {
            status = "SuperTrend.at returned unavailable despite " + bars.size() + " bars";
            bias = Bias.NEUTRAL;
            return;
        }
        stUp    = st.isUp();
        stLine  = st.line();
        Candle last = bars.get(bars.size() - 1);
        lastBarClose   = last.close();
        lastBarVwap    = last.vwap();
        lastBarStartMs = last.startMillis();
        lastComputedDay = LocalDate.now(IST).toString();

        // Choppiness Index — computed on the SAME 3-min bar list used for ST.
        // Independent from ST direction; describes how much price is
        // zig-zagging within its range over the last CHOPPINESS_PERIOD bars.
        double ci = ChoppinessIndex.at(bars, CHOPPINESS_PERIOD);
        choppinessIndex  = ci;
        choppinessRegime = ChoppinessIndex.classify(ci);

        // Composite bias: requires BOTH ST direction AND close-vs-VWAP alignment.
        //   BULLISH  = ST up AND close > VWAP  → block CE sells
        //   BEARISH  = ST down AND close < VWAP → block PE sells
        //   NEUTRAL  = everything else (close sitting between VWAP and ST line,
        //              or VWAP not yet populated) → block BOTH sides
        // If VWAP hasn't populated yet (warmed-up bars have vwap=0 until the
        // aggregator ingests live ticks), the bias stays NEUTRAL by design.
        Bias next;
        if (last.vwap() <= 0) {
            next = Bias.NEUTRAL;
            status = "armed on " + bars.size() + " bars — VWAP not yet available on latest bar";
        } else if (stUp && last.close() > last.vwap()) {
            next = Bias.BULLISH;
            status = "armed on " + bars.size() + " bars — ST green AND close > VWAP";
        } else if (!stUp && last.close() < last.vwap()) {
            next = Bias.BEARISH;
            status = "armed on " + bars.size() + " bars — ST red AND close < VWAP";
        } else {
            next = Bias.NEUTRAL;
            status = "armed on " + bars.size() + " bars — close between VWAP and ST line ("
                + (stUp ? "ST green" : "ST red") + ", "
                + (last.close() > last.vwap() ? "close > VWAP" : "close < VWAP") + ")";
        }
        bias = next;
    }

    // ── Public API ──────────────────────────────────────────────────────────

    /** Raw ST direction — kept for chart / diagnostics consumers. Prefer
     *  {@link #getBiasEnum()} for the composite bias used by the entry filter. */
    public Optional<Boolean> getStUp() {
        Boolean v = stUp;
        // Stale-day guard — if we haven't computed today, treat as unavailable.
        if (v == null) return Optional.empty();
        String today = LocalDate.now(IST).toString();
        if (!today.equals(lastComputedDay)) return Optional.empty();
        return Optional.of(v);
    }

    /** Composite three-state bias:
     *  <ul>
     *    <li>{@link Bias#BULLISH} — ST green AND close &gt; VWAP → block CE sells</li>
     *    <li>{@link Bias#BEARISH} — ST red AND close &lt; VWAP → block PE sells</li>
     *    <li>{@link Bias#NEUTRAL} — mixed signals OR data not ready → block both</li>
     *  </ul>
     *  Stale-day guard: if the last evaluation was not today, returns NEUTRAL. */
    public Bias getBiasEnum() {
        String today = LocalDate.now(IST).toString();
        if (!today.equals(lastComputedDay)) return Bias.NEUTRAL;
        return bias;
    }

    public String getFuturesSymbol()    { return futuresSymbol; }
    public double getStLine()           { return stLine; }
    public double getLastBarClose()     { return lastBarClose; }
    public double getLastBarVwap()      { return lastBarVwap; }
    /** Choppiness Index of the last 14 3-min futures bars, or NaN when not
     *  yet computed / stale (not today). */
    public double getChoppinessIndex() {
        String today = LocalDate.now(IST).toString();
        if (!today.equals(lastComputedDay)) return Double.NaN;
        return choppinessIndex;
    }
    /** Regime bucket for {@link #getChoppinessIndex()} — CHOPPY / TRENDING /
     *  MIXED / UNAVAILABLE (stale, or bars insufficient). */
    public ChoppinessIndex.Regime getChoppinessRegime() {
        String today = LocalDate.now(IST).toString();
        if (!today.equals(lastComputedDay)) return ChoppinessIndex.Regime.UNAVAILABLE;
        return choppinessRegime;
    }
    /** Human-readable reason the service is or isn't armed — for the BIAS chip
     *  tooltip so the operator can diagnose a stuck NEUTRAL. */
    public String getStatus()           { return status; }
    public String getBias()             { return getBiasEnum().name(); }

    private String authHeader() {
        return fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
    }
}
