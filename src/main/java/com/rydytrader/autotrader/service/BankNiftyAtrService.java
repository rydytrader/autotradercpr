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
 * BANKNIFTY 14-period ATR on 5-min spot bars — mirror of
 * {@link NiftyAtrService} for the second instrument. Same Wilder-14 math,
 * same seed / retry semantics, same on-disk state contract. Only tracked
 * symbol and cache filename differ.
 */
@Service
public class BankNiftyAtrService {

    private static final Logger log = LoggerFactory.getLogger(BankNiftyAtrService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/banknifty-atr-state.json";
    private static final String BANK_NIFTY_SYMBOL = "NSE:NIFTYBANK-INDEX";
    private static final int    PERIOD       = 14;
    private static final int    SEED_DAYS    = 10;
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

    public BankNiftyAtrService(CandleAggregator candleAggregator,
                               FyersClientRouter fyersClient,
                               TokenStore tokenStore,
                               FyersProperties fyersProperties) {
        this.candleAggregator     = candleAggregator;
        this.fyersClient          = fyersClient;
        this.tokenStore           = tokenStore;
        this.fyersProperties      = fyersProperties;
    }

    @PostConstruct
    public synchronized void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        if (needsSeed()) attemptSeed("boot");
        candleAggregator.subscribe(BANK_NIFTY_SYMBOL, this::onBarClose);
        log.info("[BankNiftyAtr] booted — wilderSeeded={} atr={} prevClose={}",
            state.wilderSeeded, state.atr, state.prevClose);
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public synchronized void retrySeedIfNeeded() {
        if (!needsSeed()) return;
        attemptSeed("scheduled");
    }

    private boolean needsSeed() { return !state.wilderSeeded; }

    private void attemptSeed(String trigger) {
        try {
            boolean ok = seedFromHistory();
            if (ok) log.info("[BankNiftyAtr] seed succeeded ({}) — atr={} prevClose={}",
                trigger, state.atr, state.prevClose);
        } catch (Exception e) {
            log.warn("[BankNiftyAtr] seed failed ({}): {}", trigger, e.getMessage());
        }
    }

    private synchronized void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        if (state.dayKey != null && !state.dayKey.isBlank()) {
            log.info("[BankNiftyAtr] day rollover {} → {} — forcing Wilder re-seed",
                state.dayKey, today);
        }
        state.dayKey = today;
        state.wilderSeeded = false;
        saveToDisk();
    }

    synchronized void onBarClose(Candle c) {
        if (c == null || c.high() <= 0 || c.low() <= 0 || c.close() <= 0) return;
        rolloverIfNewDay();
        if (needsSeed()) {
            attemptSeed("onBarClose");
            return;
        }
        double tr = trueRange(c.high(), c.low(), state.prevClose);
        state.atr = (state.atr * (PERIOD - 1) + tr) / PERIOD;
        state.prevClose = c.close();
        saveToDisk();
    }

    private static double trueRange(double high, double low, double prevClose) {
        double hl = high - low;
        if (prevClose <= 0) return hl;
        double hpc = Math.abs(high - prevClose);
        double lpc = Math.abs(low  - prevClose);
        return Math.max(hl, Math.max(hpc, lpc));
    }

    public synchronized Double currentAtr() {
        return state.wilderSeeded ? round2(state.atr) : null;
    }

    private boolean seedFromHistory() throws Exception {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.info("[BankNiftyAtr] seed deferred — access token not loaded yet");
            return false;
        }
        LocalDate today = LocalDate.now(IST);
        LocalDate from  = today.minusDays(SEED_DAYS);
        String auth = fyersProperties.getClientId() + ":" + accessToken;
        JsonNode root = fyersClient.getHistory(BANK_NIFTY_SYMBOL, "5", from.toString(), today.toString(), auth);
        if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
            String resp = root == null ? "<null>" : root.toString();
            if (resp.length() > 300) resp = resp.substring(0, 300) + "…";
            log.warn("[BankNiftyAtr] seed — Fyers history missing candles[]; response: {}", resp);
            return false;
        }
        JsonNode candles = root.get("candles");
        long nowMs = System.currentTimeMillis();
        long bucketLenMs = 5L * 60_000L;
        List<double[]> fresh = new ArrayList<>();
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
        if (fresh.size() < PERIOD + 1) {
            log.warn("[BankNiftyAtr] seed — only {} in-session bars returned, need >= {}",
                fresh.size(), PERIOD + 1);
            return false;
        }
        double sumTr = 0;
        for (int i = 0; i < PERIOD; i++) {
            double[] bar = fresh.get(i);
            double prevClose = i == 0 ? 0 : fresh.get(i - 1)[2];
            sumTr += trueRange(bar[0], bar[1], prevClose);
        }
        double atr = sumTr / PERIOD;
        for (int i = PERIOD; i < fresh.size(); i++) {
            double[] bar = fresh.get(i);
            double prevClose = fresh.get(i - 1)[2];
            double tr = trueRange(bar[0], bar[1], prevClose);
            atr = (atr * (PERIOD - 1) + tr) / PERIOD;
        }
        state.atr            = atr;
        state.prevClose      = fresh.get(fresh.size() - 1)[2];
        state.wilderSeeded   = true;
        log.info("[BankNiftyAtr] seed — Wilder converged across {} bars, atr={} prevClose={}",
            fresh.size(), round2(atr), round2(state.prevClose));
        saveToDisk();
        return true;
    }

    private static double round2(double v) {
        if (Double.isInfinite(v) || Double.isNaN(v)) return v;
        return Math.round(v * 100.0) / 100.0;
    }

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
            log.warn("[BankNiftyAtr] failed to load state: {}", e.getMessage());
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
            log.warn("[BankNiftyAtr] failed to save state: {}", e.getMessage());
        }
    }

    public static class State {
        public String dayKey = LocalDate.now(IST).toString();
        public double atr = 0;
        public double prevClose = 0;
        public boolean wilderSeeded = false;
    }
}
