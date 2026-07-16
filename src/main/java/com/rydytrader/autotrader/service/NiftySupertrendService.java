package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.util.FileIoUtils;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * NIFTY SuperTrend(10, 2) on 5-minute spot bars.
 *
 * <p>Standard SuperTrend formulation:
 * <ul>
 *   <li>ATR(10) via Wilder smoothing.</li>
 *   <li>Basic upper band = hl2 + multiplier × ATR; basic lower = hl2 − multiplier × ATR.</li>
 *   <li>Final upper/lower bands ratchet — final upper only widens when prior close &gt; prior
 *       final upper; final lower only tightens when prior close &lt; prior final lower.</li>
 *   <li>Trend flips: bullish (SuperTrend = final lower) when close crosses above prior final
 *       upper; bearish (SuperTrend = final upper) when close crosses below prior final lower.</li>
 * </ul>
 *
 * <p>Seeded from Fyers 5-min history at boot (and each day rollover) so the indicator has
 * converged before the first live tick. Wilder + band ratchet state persist to
 * {@code ../store/cache/nifty-supertrend-state.json}.
 */
@Service
public class NiftySupertrendService {

    private static final Logger log = LoggerFactory.getLogger(NiftySupertrendService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/nifty-supertrend-state.json";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final int    PERIOD     = 10;
    private static final double MULTIPLIER = 2.0;
    /** Bars of 5-min history for the cold-boot seed. 500+ bars ≈ 10 trading days
     *  — well above the ~30-bar Wilder convergence horizon for a period of 10. */
    private static final int    SEED_DAYS  = 10;
    private static final LocalTime SESSION_START = LocalTime.of(9, 15);
    private static final LocalTime SESSION_END   = LocalTime.of(15, 30);

    private final CandleAggregator candleAggregator;
    private final FyersClientRouter fyersClient;
    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .findAndRegisterModules();

    private State state = new State();

    public NiftySupertrendService(CandleAggregator candleAggregator,
                                  FyersClientRouter fyersClient,
                                  TokenStore tokenStore,
                                  FyersProperties fyersProperties) {
        this.candleAggregator = candleAggregator;
        this.fyersClient      = fyersClient;
        this.tokenStore       = tokenStore;
        this.fyersProperties  = fyersProperties;
    }

    @PostConstruct
    public synchronized void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        if (needsSeed()) attemptSeed("boot");
        candleAggregator.subscribe(NIFTY_SYMBOL, this::onBarClose);
        log.info("[NiftySupertrend] booted — seeded={} trend={} st={}",
            state.seeded, trendLabel(), round2(state.supertrend));
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public synchronized void retrySeedIfNeeded() {
        if (!needsSeed()) return;
        attemptSeed("scheduled");
    }

    private boolean needsSeed() { return !state.seeded; }

    private void attemptSeed(String trigger) {
        try {
            boolean ok = seedFromHistory();
            if (ok) log.info("[NiftySupertrend] seed succeeded ({}) — trend={} st={}",
                trigger, trendLabel(), round2(state.supertrend));
        } catch (Exception e) {
            log.warn("[NiftySupertrend] seed failed ({}): {}", trigger, e.getMessage());
        }
    }

    private synchronized void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        if (state.dayKey != null && !state.dayKey.isBlank()) {
            log.info("[NiftySupertrend] day rollover {} → {} — forcing re-seed",
                state.dayKey, today);
        }
        state.dayKey = today;
        state.seeded = false;
        saveToDisk();
    }

    /** Bar-close callback — advances Wilder ATR and re-computes SuperTrend. */
    synchronized void onBarClose(Candle c) {
        if (c == null || c.high() <= 0 || c.low() <= 0 || c.close() <= 0) return;
        rolloverIfNewDay();
        if (needsSeed()) {
            attemptSeed("onBarClose");
            return;
        }
        double tr  = trueRange(c.high(), c.low(), state.prevClose);
        double atr = (state.atr * (PERIOD - 1) + tr) / PERIOD;
        advance(c.high(), c.low(), c.close(), atr);
        saveToDisk();
    }

    /** Apply one bar's worth of SuperTrend advance given the pre-computed ATR. */
    private void advance(double high, double low, double close, double atr) {
        double hl2   = (high + low) / 2.0;
        double basicUpper = hl2 + MULTIPLIER * atr;
        double basicLower = hl2 - MULTIPLIER * atr;

        double finalUpper = (basicUpper < state.finalUpper || state.prevClose > state.finalUpper)
            ? basicUpper : state.finalUpper;
        double finalLower = (basicLower > state.finalLower || state.prevClose < state.finalLower)
            ? basicLower : state.finalLower;

        int newTrend;
        double newSt;
        if (state.trend > 0) {
            // was bullish (ST = lower band)
            if (close < finalLower) { newTrend = -1; newSt = finalUpper; }
            else                    { newTrend = +1; newSt = finalLower; }
        } else if (state.trend < 0) {
            // was bearish (ST = upper band)
            if (close > finalUpper) { newTrend = +1; newSt = finalLower; }
            else                    { newTrend = -1; newSt = finalUpper; }
        } else {
            // no prior direction yet — pick from close vs bands
            if (close > finalUpper)      { newTrend = +1; newSt = finalLower; }
            else if (close < finalLower) { newTrend = -1; newSt = finalUpper; }
            else                          { newTrend = +1; newSt = finalLower; }
        }

        state.atr        = atr;
        state.prevClose  = close;
        state.finalUpper = finalUpper;
        state.finalLower = finalLower;
        state.trend      = newTrend;
        state.supertrend = newSt;
    }

    /** True Range = max(H−L, |H − prevClose|, |L − prevClose|). */
    private static double trueRange(double high, double low, double prevClose) {
        double hl = high - low;
        if (prevClose <= 0) return hl;
        double hpc = Math.abs(high - prevClose);
        double lpc = Math.abs(low  - prevClose);
        return Math.max(hl, Math.max(hpc, lpc));
    }

    /** "BULLISH" / "BEARISH" / null (before first seed). */
    public synchronized String currentTrend() {
        if (!state.seeded) return null;
        return state.trend > 0 ? "BULLISH" : state.trend < 0 ? "BEARISH" : null;
    }

    /** Current SuperTrend level (the line on the chart). Null before seed. */
    public synchronized Double currentLevel() {
        return state.seeded ? round2(state.supertrend) : null;
    }

    private String trendLabel() {
        if (state.trend > 0) return "BULLISH";
        if (state.trend < 0) return "BEARISH";
        return "—";
    }

    // ── Cold-boot seed from Fyers history ──────────────────────────────────

    private boolean seedFromHistory() throws Exception {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.info("[NiftySupertrend] seed deferred — access token not loaded yet");
            return false;
        }
        LocalDate today = LocalDate.now(IST);
        LocalDate from  = today.minusDays(SEED_DAYS);
        String auth = fyersProperties.getClientId() + ":" + accessToken;
        JsonNode root = fyersClient.getHistory(NIFTY_SYMBOL, "5", from.toString(), today.toString(), auth);
        if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
            String resp = root == null ? "<null>" : root.toString();
            if (resp.length() > 300) resp = resp.substring(0, 300) + "…";
            log.warn("[NiftySupertrend] seed — Fyers history missing candles[]; response: {}", resp);
            return false;
        }
        JsonNode candles = root.get("candles");
        long nowMs = System.currentTimeMillis();
        long bucketLenMs = 5L * 60_000L;
        List<double[]> fresh = new ArrayList<>();   // [high, low, close]
        for (JsonNode row : candles) {
            if (!row.isArray() || row.size() < 5) continue;
            long epochSec = row.get(0).asLong();
            double high  = row.get(2).asDouble();
            double low   = row.get(3).asDouble();
            double close = row.get(4).asDouble();
            if (close <= 0 || high <= 0 || low <= 0) continue;
            long barStartMs = epochSec * 1000L;
            if (barStartMs + bucketLenMs > nowMs) continue;
            ZonedDateTime z = Instant.ofEpochMilli(barStartMs).atZone(IST);
            LocalTime tt = z.toLocalTime();
            if (tt.isBefore(SESSION_START) || tt.isAfter(SESSION_END)) continue;
            fresh.add(new double[]{high, low, close});
        }
        if (fresh.size() < PERIOD + 2) {
            log.warn("[NiftySupertrend] seed — only {} in-session bars returned, need >= {}",
                fresh.size(), PERIOD + 2);
            return false;
        }

        // Seed Wilder ATR across the first PERIOD bars.
        double sumTr = 0;
        for (int i = 0; i < PERIOD; i++) {
            double[] bar = fresh.get(i);
            double prevClose = i == 0 ? 0 : fresh.get(i - 1)[2];
            sumTr += trueRange(bar[0], bar[1], prevClose);
        }
        double atr = sumTr / PERIOD;
        // First SuperTrend anchor — pick from the PERIOD-th bar's bands.
        double[] anchor = fresh.get(PERIOD - 1);
        state.atr        = atr;
        state.prevClose  = anchor[2];
        double hl2       = (anchor[0] + anchor[1]) / 2.0;
        state.finalUpper = hl2 + MULTIPLIER * atr;
        state.finalLower = hl2 - MULTIPLIER * atr;
        state.trend      = anchor[2] >= (state.finalUpper + state.finalLower) / 2.0 ? +1 : -1;
        state.supertrend = state.trend > 0 ? state.finalLower : state.finalUpper;
        state.seeded     = true;

        // Advance SuperTrend across the remaining historical bars so the state
        // reflects the exact indicator value at the last closed bar.
        for (int i = PERIOD; i < fresh.size(); i++) {
            double[] bar = fresh.get(i);
            double prevClose = fresh.get(i - 1)[2];
            double tr = trueRange(bar[0], bar[1], prevClose);
            double next = (state.atr * (PERIOD - 1) + tr) / PERIOD;
            advance(bar[0], bar[1], bar[2], next);
        }
        log.info("[NiftySupertrend] seed — converged across {} bars, trend={} st={} atr={}",
            fresh.size(), trendLabel(), round2(state.supertrend), round2(state.atr));
        saveToDisk();
        return true;
    }

    private static double round2(double v) {
        if (Double.isInfinite(v) || Double.isNaN(v)) return v;
        return Math.round(v * 100.0) / 100.0;
    }

    // ── Disk persistence ────────────────────────────────────────────────────

    private void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.dayKey == null) state.dayKey = LocalDate.now(IST).toString();
            }
        } catch (IOException e) {
            log.warn("[NiftySupertrend] failed to load state: {}", e.getMessage());
        }
    }

    private void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
            FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[NiftySupertrend] failed to save state: {}", e.getMessage());
        }
    }

    // ── State ───────────────────────────────────────────────────────────────

    public static class State {
        public String  dayKey     = LocalDate.now(IST).toString();
        public boolean seeded     = false;
        /** Wilder's running 10-period ATR in NIFTY index points. */
        public double  atr        = 0;
        /** Most recent closed bar's close — used for the next bar's true range. */
        public double  prevClose  = 0;
        /** Ratcheted final upper band. */
        public double  finalUpper = 0;
        /** Ratcheted final lower band. */
        public double  finalLower = 0;
        /** +1 bullish, −1 bearish, 0 not seeded yet. */
        public int     trend      = 0;
        /** Current SuperTrend line value (either final upper or final lower
         *  depending on trend). */
        public double  supertrend = 0;
    }
}
