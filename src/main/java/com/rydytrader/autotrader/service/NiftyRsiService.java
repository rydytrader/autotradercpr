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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * NIFTY 14-period RSI tracker — display-only indicator surfaced on the trade page
 * next to the OI trend chart.
 *
 * <p>Subscribes to {@link CandleAggregator}'s 5-min close stream for
 * {@code NSE:NIFTY50-INDEX}. Maintains running Wilder averages
 * ({@code avgGain} / {@code avgLoss}) — the standard RSI smoothing used by
 * TradingView (regardless of what the "Smoothing Type" dropdown shows;
 * that field controls the overlay moving-average line, not the RSI itself).
 *
 * <p>Cold boot replays the trailing {@link #SEED_DAYS} calendar days of 5-min
 * candles from Fyers through Wilder's smoothing in a single pass — long
 * enough that the running averages converge to within ~0.01 of the
 * TradingView-displayed value (the (13/14)^N decay of the seed is below
 * 0.01% by N ≈ 100 bars).
 *
 * <p>The running averages CARRY FORWARD across day boundaries — RSI is a
 * continuous indicator and resetting at midnight would break parity with
 * TradingView. Only the per-day chart series resets on rollover.
 *
 * <p>State persists to {@code ../store/cache/nifty-rsi-state.json} after every
 * bar close — atomic temp-file write via {@link FileIoUtils}.
 *
 * <p>This service does not gate trades or feed risk decisions — it is
 * purely an operator-facing indicator.
 */
@Service
public class NiftyRsiService {

    private static final Logger log = LoggerFactory.getLogger(NiftyRsiService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/nifty-rsi-state.json";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final int    PERIOD       = 14;
    /** Rolling buffer of recent closes. Only the most recent close is required
     *  to advance Wilder on the next bar (for the diff calculation); the rest
     *  of the buffer is kept for inspection / diagnostics. */
    private static final int    BUFFER_CAP   = 20;
    /** Calendar days of 5-min history pulled by the cold-boot seed.
     *  10 calendar days ≈ 7 trading days ≈ ~520 5-min bars inside session —
     *  enough for Wilder's smoothing to converge to within ~0.01 of
     *  TradingView's published value. Seed contribution decays as
     *  (13/14)^N: ~26% at N=50, ~7% at N=100, ~0.5% at N=200, ~0.04% at N=300.
     *  After 500 bars the value matches TradingView to the second decimal. */
    private static final int    SEED_DAYS    = 10;
    private static final LocalTime SESSION_START = LocalTime.of(9, 15);
    private static final LocalTime SESSION_END   = LocalTime.of(15, 30);
    private static final DateTimeFormatter HHMM = DateTimeFormatter.ofPattern("HH:mm");

    private final CandleAggregator   candleAggregator;
    private final FyersClientRouter  fyersClient;
    private final TokenStore         tokenStore;
    private final FyersProperties    fyersProperties;
    private final ObjectMapper       mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .findAndRegisterModules();

    private State state = new State();

    public NiftyRsiService(CandleAggregator candleAggregator,
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
        // Try the seed once now. Boot typically beats TokenStore loading the
        // access token, so this call commonly fails — that's expected and
        // recovered by the scheduled retry + onBarClose retry below.
        if (needsSeed()) {
            attemptSeed("boot");
        }
        candleAggregator.subscribe(NIFTY_SYMBOL, this::onBarClose);
        log.info("[NiftyRsi] booted — recentBars={} wilderSeeded={} lastRsi={} todaySamples={}",
            state.recentBars.size(), state.wilderSeeded, state.lastRsi, state.todaySamples.size());
    }

    /** Scheduled retry every 30 s. No clock gate — runs as soon as the bot
     *  boots so the buffer can be warm long before the 09:15 open. Once Fyers
     *  auth loads and history returns candles, the seed succeeds and this
     *  becomes a cheap no-op (needsSeed() returns false). The 30-second
     *  initialDelay gives TokenStore a head start so the very first attempt
     *  usually catches the access token already loaded. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public synchronized void retrySeedIfNeeded() {
        if (!needsSeed()) return;
        attemptSeed("scheduled");
    }

    private boolean needsSeed() {
        return !state.wilderSeeded;
    }

    /** Single seed attempt with consistent logging. Tagged with the trigger so
     *  the operator can tell boot vs scheduled vs onBarClose retries apart in
     *  the log. */
    private void attemptSeed(String trigger) {
        try {
            boolean ok = seedFromHistory();
            if (ok) log.info("[NiftyRsi] seed succeeded ({}) — recentBars={} lastRsi={}",
                trigger, state.recentBars.size(), state.lastRsi);
        } catch (Exception e) {
            log.warn("[NiftyRsi] seed failed ({}): {}", trigger, e.getMessage());
        }
    }

    /** 5-min candle close callback. Maintains the rolling buffer, advances
     *  Wilder's smoothing, appends the new RSI value to today's chart series,
     *  and persists. */
    synchronized void onBarClose(Candle c) {
        if (c == null || c.close() <= 0) return;
        rolloverIfNewDay();
        // Catch-up seed: if a prior @PostConstruct seed failed (typically
        // because Fyers auth wasn't loaded yet) and the scheduled retry hasn't
        // succeeded yet either, try once more now. The appendCloseToBuffer
        // idempotency guard handles any overlap between the seed's last bar
        // and this live close.
        if (needsSeed()) {
            attemptSeed("onBarClose");
        }
        String t = bucketLabel(c.startMillis());
        appendCloseToBuffer(t, c.close());
        Double rsi = computeNextRsi();
        if (rsi != null) {
            state.todaySamples.add(new RsiSample(t, round2(rsi)));
            state.lastRsi = round2(rsi);
        }
        saveToDisk();
    }

    /** Append a new close to the rolling buffer, trimming to {@link #BUFFER_CAP}. */
    private void appendCloseToBuffer(String t, double close) {
        if (!state.recentBars.isEmpty()) {
            Bar last = state.recentBars.get(state.recentBars.size() - 1);
            // Idempotency guard — same bar timestamp arriving twice (rare WS
            // replay or seed overlap) updates the close in place instead of
            // double-counting it as a new bar.
            if (t.equals(last.t())) {
                state.recentBars.set(state.recentBars.size() - 1, new Bar(t, close));
                return;
            }
        }
        state.recentBars.add(new Bar(t, close));
        while (state.recentBars.size() > BUFFER_CAP) {
            state.recentBars.remove(0);
        }
    }

    /** Advance Wilder's RSI by one bar using the last two closes in the buffer.
     *  Returns the new RSI value, or null if the seed hasn't run yet (the
     *  running averages haven't been initialised, so any value computed would
     *  be meaningless). Wilder smoothing:
     *  <pre>
     *    diff      = close_n − close_{n-1}
     *    gain      = max(diff, 0)
     *    loss      = max(-diff, 0)
     *    avgGain_n = (avgGain_{n-1} × (PERIOD-1) + gain) / PERIOD
     *    avgLoss_n = (avgLoss_{n-1} × (PERIOD-1) + loss) / PERIOD
     *    RSI       = 100 − 100 / (1 + avgGain_n / avgLoss_n)
     *  </pre> */
    private Double computeNextRsi() {
        if (!state.wilderSeeded) return null;
        int n = state.recentBars.size();
        if (n < 2) return null;
        double diff = state.recentBars.get(n - 1).close() - state.recentBars.get(n - 2).close();
        double gain = diff >= 0 ?  diff : 0;
        double loss = diff <  0 ? -diff : 0;
        state.avgGain = (state.avgGain * (PERIOD - 1) + gain) / PERIOD;
        state.avgLoss = (state.avgLoss * (PERIOD - 1) + loss) / PERIOD;
        return rsiFromAverages(state.avgGain, state.avgLoss);
    }

    private static double rsiFromAverages(double avgGain, double avgLoss) {
        if (avgLoss <= 0) return 100.0;
        double rs = avgGain / avgLoss;
        return 100.0 - (100.0 / (1.0 + rs));
    }

    /** Day rollover — only clears today's chart series. Wilder's running
     *  averages, the recent-bars buffer, and lastRsi all carry forward
     *  unchanged: RSI is a continuous indicator. Resetting Wilder at midnight
     *  was the root cause of values diverging from TradingView (which carries
     *  the smoothing across day boundaries the same way). The header chip
     *  shows the same RSI value at 09:14:59 and 09:15:01; the chart line
     *  starts fresh but the underlying state does not. */
    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        if (state.dayKey != null && !state.dayKey.isBlank()) {
            log.info("[NiftyRsi] day rollover {} → {} — clearing today's samples (Wilder state carries forward)",
                state.dayKey, today);
        }
        state.dayKey       = today;
        state.todaySamples = new ArrayList<>();
        saveToDisk();
    }

    /** Fetch {@link #SEED_DAYS} calendar days of 5-min NIFTY spot candles and
     *  walk the entire history through Wilder's smoothing in a single pass.
     *  The first {@code PERIOD} changes are averaged simply (Wilder's seed);
     *  every subsequent change advances the running averages by
     *  {@code (prev × 13 + new) / 14}. After ~200 bars the value converges
     *  to TradingView's published RSI to within ~0.01.
     *
     *  <p>The persisted buffer retains only the trailing {@link #BUFFER_CAP}
     *  closes — the running averages are the source of truth, and live bar
     *  advancement only needs the most recent close for the diff
     *  calculation. Returns {@code true} on successful seed. */
    private boolean seedFromHistory() throws Exception {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.info("[NiftyRsi] seed deferred — access token not loaded yet");
            return false;
        }
        LocalDate today = LocalDate.now(IST);
        LocalDate from  = today.minusDays(SEED_DAYS);
        String auth = fyersProperties.getClientId() + ":" + accessToken;
        JsonNode root = fyersClient.getHistory(NIFTY_SYMBOL, "5", from.toString(), today.toString(), auth);
        if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
            String resp = root == null ? "<null>" : root.toString();
            if (resp.length() > 300) resp = resp.substring(0, 300) + "…";
            log.warn("[NiftyRsi] seed — Fyers history missing candles[]; response: {}", resp);
            return false;
        }
        JsonNode candles = root.get("candles");
        long nowMs   = System.currentTimeMillis();
        long bucketLenMs = 5L * 60_000L;
        List<Bar> fresh = new ArrayList<>();
        for (JsonNode row : candles) {
            if (!row.isArray() || row.size() < 5) continue;
            long epochSec = row.get(0).asLong();
            double close = row.get(4).asDouble();
            if (close <= 0) continue;
            long barStartMs = epochSec * 1000L;
            // Drop the live in-progress bar — Fyers returns a partial bar for
            // the slot the wall clock is currently inside.
            if (barStartMs + bucketLenMs > nowMs) continue;
            ZonedDateTime z = Instant.ofEpochMilli(barStartMs).atZone(IST);
            LocalTime tt = z.toLocalTime();
            if (tt.isBefore(SESSION_START) || tt.isAfter(SESSION_END)) continue;
            fresh.add(new Bar(z.format(HHMM), round2(close)));
        }
        if (fresh.size() < PERIOD + 1) {
            log.warn("[NiftyRsi] seed — only {} in-session bars returned, need >= {}",
                fresh.size(), PERIOD + 1);
            return false;
        }
        // ── Wilder's smoothing across the entire fetched history ──
        // Seed avgGain / avgLoss from the first PERIOD changes (simple mean),
        // then advance Wilder's smoothing on every subsequent change. After
        // hundreds of bars the running averages converge to TradingView's
        // values; the initial seed's contribution decays geometrically.
        double sumGain = 0, sumLoss = 0;
        for (int i = 1; i <= PERIOD; i++) {
            double diff = fresh.get(i).close() - fresh.get(i - 1).close();
            if (diff >= 0) sumGain += diff;
            else           sumLoss += -diff;
        }
        double avgGain = sumGain / PERIOD;
        double avgLoss = sumLoss / PERIOD;
        for (int i = PERIOD + 1; i < fresh.size(); i++) {
            double diff = fresh.get(i).close() - fresh.get(i - 1).close();
            double gain = diff >= 0 ?  diff : 0;
            double loss = diff <  0 ? -diff : 0;
            avgGain = (avgGain * (PERIOD - 1) + gain) / PERIOD;
            avgLoss = (avgLoss * (PERIOD - 1) + loss) / PERIOD;
        }
        state.avgGain      = avgGain;
        state.avgLoss      = avgLoss;
        state.wilderSeeded = true;
        state.lastRsi      = round2(rsiFromAverages(avgGain, avgLoss));
        // Retain only the trailing BUFFER_CAP closes — Wilder's running
        // averages are the source of truth; the buffer is only kept to
        // compute diff = close - prevClose on the next live bar.
        if (fresh.size() > BUFFER_CAP) {
            state.recentBars = new ArrayList<>(fresh.subList(fresh.size() - BUFFER_CAP, fresh.size()));
        } else {
            state.recentBars = fresh;
        }
        // Wipe any stale chart samples from earlier cache schemas — chart
        // refills from onBarClose going forward.
        state.todaySamples = new ArrayList<>();
        log.info("[NiftyRsi] seed — Wilder converged across {} bars, kept last {}, lastRsi={} (avgGain={}, avgLoss={})",
            fresh.size(), state.recentBars.size(), state.lastRsi,
            round2(state.avgGain), round2(state.avgLoss));
        saveToDisk();
        return true;
    }

    /** Bucket label "HH:mm" derived from the bar's startMillis in IST. */
    private static String bucketLabel(long startMillis) {
        return Instant.ofEpochMilli(startMillis).atZone(IST).format(HHMM);
    }

    private static double round2(double v) {
        if (Double.isInfinite(v) || Double.isNaN(v)) return v;
        return Math.round(v * 100.0) / 100.0;
    }

    // ── REST payload helpers ────────────────────────────────────────────────

    public synchronized History history() {
        // Run the rollover here too so the operator opening the trade page
        // before the first bar of a new day immediately sees the carryover
        // RSI value from yesterday's buffer (the bar-close path won't fire
        // until 09:20 IST). No-op when dayKey already matches today.
        rolloverIfNewDay();
        return new History(state.dayKey, state.lastRsi,
            new ArrayList<>(state.todaySamples));
    }

    // ── Disk persistence ────────────────────────────────────────────────────

    private void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.recentBars == null)   state.recentBars   = new ArrayList<>();
                if (state.todaySamples == null) state.todaySamples = new ArrayList<>();
                if (state.dayKey == null)       state.dayKey       = LocalDate.now(IST).toString();
            }
        } catch (IOException e) {
            log.warn("[NiftyRsi] failed to load state: {}", e.getMessage());
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
            log.warn("[NiftyRsi] failed to save state: {}", e.getMessage());
        }
    }

    // ── DTOs ────────────────────────────────────────────────────────────────

    public record Bar(String t, double close) {}
    public record RsiSample(String t, double rsi) {}
    public record History(String dayKey, Double lastRsi, List<RsiSample> samples) {}

    public static class State {
        public String       dayKey       = LocalDate.now(IST).toString();
        public List<Bar>    recentBars   = new ArrayList<>();
        /** Wilder's running averages of gains / losses. Updated every bar via
         *  {@code (prev × (PERIOD-1) + new) / PERIOD}. Carry forward across
         *  day boundaries because RSI is a continuous indicator. */
        public double       avgGain      = 0;
        public double       avgLoss      = 0;
        /** True once {@code seedFromHistory} has produced the converged
         *  Wilder averages from at least {@code PERIOD + 1} historical
         *  closes. Until this flips true, {@link #computeNextRsi} returns
         *  null and the chart shows no value. */
        public boolean      wilderSeeded = false;
        public Double       lastRsi      = null;
        public List<RsiSample> todaySamples = new ArrayList<>();
    }
}
