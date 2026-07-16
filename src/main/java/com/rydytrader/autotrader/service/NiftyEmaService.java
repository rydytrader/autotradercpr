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
 * NIFTY 20-period Exponential Moving Average (EMA) on 5-minute spot bars.
 *
 * <p>Standard EMA:
 * <ul>
 *   <li>k = 2 / (PERIOD + 1) — for period 20 that's 2/21 ≈ 0.0952.</li>
 *   <li>EMA_n = close_n × k + EMA_{n-1} × (1 − k).</li>
 *   <li>Seed: simple mean of the first {@link #PERIOD} closes; subsequent bars
 *       advance via the exponential update. Converges within ~40 bars.</li>
 * </ul>
 *
 * <p>Seeded from Fyers 5-min history at boot (and each day rollover) so the
 * indicator is ready before the first live tick. State persists to
 * {@code ../store/cache/nifty-ema-state.json}.
 */
@Service
public class NiftyEmaService {

    private static final Logger log = LoggerFactory.getLogger(NiftyEmaService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/nifty-ema-state.json";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final int    PERIOD = 20;
    private static final double K      = 2.0 / (PERIOD + 1.0);
    /** Bars of 5-min history for the cold-boot seed. 10 days ≈ 750 bars —
     *  way past the ~40-bar convergence horizon for a period-20 EMA. */
    private static final int    SEED_DAYS = 10;
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

    public NiftyEmaService(CandleAggregator candleAggregator,
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
        log.info("[NiftyEma] booted — seeded={} ema={}", state.seeded, round2(state.ema));
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
            if (ok) log.info("[NiftyEma] seed succeeded ({}) — ema={}", trigger, round2(state.ema));
        } catch (Exception e) {
            log.warn("[NiftyEma] seed failed ({}): {}", trigger, e.getMessage());
        }
    }

    private synchronized void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        if (state.dayKey != null && !state.dayKey.isBlank()) {
            log.info("[NiftyEma] day rollover {} → {} — forcing re-seed",
                state.dayKey, today);
        }
        state.dayKey = today;
        state.seeded = false;
        saveToDisk();
    }

    /** Bar-close callback — advances the EMA by one exponential step. */
    synchronized void onBarClose(Candle c) {
        if (c == null || c.close() <= 0) return;
        rolloverIfNewDay();
        if (needsSeed()) {
            attemptSeed("onBarClose");
            return;
        }
        state.ema = c.close() * K + state.ema * (1 - K);
        saveToDisk();
    }

    /** Current 20-period EMA in NIFTY index points, or null before seed. */
    public synchronized Double currentEma() {
        return state.seeded ? round2(state.ema) : null;
    }

    // ── Cold-boot seed from Fyers history ──────────────────────────────────

    private boolean seedFromHistory() throws Exception {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.info("[NiftyEma] seed deferred — access token not loaded yet");
            return false;
        }
        LocalDate today = LocalDate.now(IST);
        LocalDate from  = today.minusDays(SEED_DAYS);
        String auth = fyersProperties.getClientId() + ":" + accessToken;
        JsonNode root = fyersClient.getHistory(NIFTY_SYMBOL, "5", from.toString(), today.toString(), auth);
        if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
            String resp = root == null ? "<null>" : root.toString();
            if (resp.length() > 300) resp = resp.substring(0, 300) + "…";
            log.warn("[NiftyEma] seed — Fyers history missing candles[]; response: {}", resp);
            return false;
        }
        JsonNode candles = root.get("candles");
        long nowMs = System.currentTimeMillis();
        long bucketLenMs = 5L * 60_000L;
        List<Double> closes = new ArrayList<>();
        for (JsonNode row : candles) {
            if (!row.isArray() || row.size() < 5) continue;
            long epochSec = row.get(0).asLong();
            double close = row.get(4).asDouble();
            if (close <= 0) continue;
            long barStartMs = epochSec * 1000L;
            if (barStartMs + bucketLenMs > nowMs) continue;
            ZonedDateTime z = Instant.ofEpochMilli(barStartMs).atZone(IST);
            LocalTime tt = z.toLocalTime();
            if (tt.isBefore(SESSION_START) || tt.isAfter(SESSION_END)) continue;
            closes.add(close);
        }
        if (closes.size() < PERIOD + 1) {
            log.warn("[NiftyEma] seed — only {} in-session bars returned, need >= {}",
                closes.size(), PERIOD + 1);
            return false;
        }
        // SMA seed across the first PERIOD closes, then EMA advance the rest.
        double sum = 0;
        for (int i = 0; i < PERIOD; i++) sum += closes.get(i);
        double ema = sum / PERIOD;
        for (int i = PERIOD; i < closes.size(); i++) {
            ema = closes.get(i) * K + ema * (1 - K);
        }
        state.ema    = ema;
        state.seeded = true;
        log.info("[NiftyEma] seed — converged across {} bars, ema={}",
            closes.size(), round2(ema));
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
            log.warn("[NiftyEma] failed to load state: {}", e.getMessage());
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
            log.warn("[NiftyEma] failed to save state: {}", e.getMessage());
        }
    }

    // ── State ───────────────────────────────────────────────────────────────

    public static class State {
        public String  dayKey = LocalDate.now(IST).toString();
        public boolean seeded = false;
        /** Running 20-period EMA in NIFTY index points. */
        public double  ema    = 0;
    }
}
