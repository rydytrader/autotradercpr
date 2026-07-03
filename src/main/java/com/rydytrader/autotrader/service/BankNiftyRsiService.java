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
 * BANKNIFTY 14-period RSI tracker — mirror of {@link NiftyRsiService} for the
 * second instrument. Same Wilder-14 math, same seed / retry semantics, same
 * on-disk state contract. Only the tracked symbol and cache filename differ.
 *
 * <p>Subscribes to {@link CandleAggregator}'s 5-min close stream for
 * {@code NSE:NIFTYBANK-INDEX}. State persists to
 * {@code ../store/cache/banknifty-rsi-state.json}.
 *
 * <p>See {@link NiftyRsiService} for the full commentary on Wilder smoothing,
 * seed strategy, day-rollover carry-forward, and live-tip projection — this
 * class is a near-verbatim copy with symbol constants swapped.
 */
@Service
public class BankNiftyRsiService {

    private static final Logger log = LoggerFactory.getLogger(BankNiftyRsiService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/banknifty-rsi-state.json";
    private static final String BANK_NIFTY_SYMBOL = "NSE:NIFTYBANK-INDEX";
    private static final int    PERIOD       = 14;
    private static final int    BUFFER_CAP   = 20;
    private static final int    SEED_DAYS    = 10;
    private static final LocalTime SESSION_START = LocalTime.of(9, 15);
    private static final LocalTime SESSION_END   = LocalTime.of(15, 30);
    private static final DateTimeFormatter HHMM = DateTimeFormatter.ofPattern("HH:mm");

    private final CandleAggregator   candleAggregator;
    private final FyersClientRouter  fyersClient;
    private final TokenStore         tokenStore;
    private final FyersProperties    fyersProperties;
    private final MarketDataService  marketDataService;
    private final MarketHolidayService marketHolidayService;
    private final ObjectMapper       mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .findAndRegisterModules();

    private State state = new State();

    public BankNiftyRsiService(CandleAggregator candleAggregator,
                               FyersClientRouter fyersClient,
                               TokenStore tokenStore,
                               FyersProperties fyersProperties,
                               MarketDataService marketDataService,
                               MarketHolidayService marketHolidayService) {
        this.candleAggregator      = candleAggregator;
        this.fyersClient           = fyersClient;
        this.tokenStore            = tokenStore;
        this.fyersProperties       = fyersProperties;
        this.marketDataService     = marketDataService;
        this.marketHolidayService  = marketHolidayService;
    }

    @PostConstruct
    public synchronized void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        if (needsSeed()) {
            attemptSeed("boot");
        }
        candleAggregator.subscribe(BANK_NIFTY_SYMBOL, this::onBarClose);
        log.info("[BankNiftyRsi] booted — recentBars={} wilderSeeded={} lastRsi={} todaySamples={}",
            state.recentBars.size(), state.wilderSeeded, state.lastRsi, state.todaySamples.size());
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public synchronized void retrySeedIfNeeded() {
        if (!needsSeed()) return;
        attemptSeed("scheduled");
    }

    private boolean needsSeed() {
        return !state.wilderSeeded;
    }

    private void attemptSeed(String trigger) {
        try {
            boolean ok = seedFromHistory();
            if (ok) log.info("[BankNiftyRsi] seed succeeded ({}) — recentBars={} lastRsi={}",
                trigger, state.recentBars.size(), state.lastRsi);
        } catch (Exception e) {
            log.warn("[BankNiftyRsi] seed failed ({}): {}", trigger, e.getMessage());
        }
    }

    synchronized void onBarClose(Candle c) {
        if (c == null || c.close() <= 0) return;
        rolloverIfNewDay();
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

    private void appendCloseToBuffer(String t, double close) {
        if (!state.recentBars.isEmpty()) {
            Bar last = state.recentBars.get(state.recentBars.size() - 1);
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

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        if (state.dayKey != null && !state.dayKey.isBlank()) {
            log.info("[BankNiftyRsi] day rollover {} → {} — clearing today's samples and forcing Wilder re-seed",
                state.dayKey, today);
        }
        state.dayKey       = today;
        state.todaySamples = new ArrayList<>();
        state.wilderSeeded = false;
        saveToDisk();
    }

    private boolean seedFromHistory() throws Exception {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.info("[BankNiftyRsi] seed deferred — access token not loaded yet");
            return false;
        }
        LocalDate today = LocalDate.now(IST);
        LocalDate from  = today.minusDays(SEED_DAYS);
        String auth = fyersProperties.getClientId() + ":" + accessToken;
        JsonNode root = fyersClient.getHistory(BANK_NIFTY_SYMBOL, "5", from.toString(), today.toString(), auth);
        if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
            String resp = root == null ? "<null>" : root.toString();
            if (resp.length() > 300) resp = resp.substring(0, 300) + "…";
            log.warn("[BankNiftyRsi] seed — Fyers history missing candles[]; response: {}", resp);
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
            if (barStartMs + bucketLenMs > nowMs) continue;
            ZonedDateTime z = Instant.ofEpochMilli(barStartMs).atZone(IST);
            LocalTime tt = z.toLocalTime();
            if (tt.isBefore(SESSION_START) || tt.isAfter(SESSION_END)) continue;
            fresh.add(new Bar(z.format(HHMM), round2(close)));
        }
        if (fresh.size() < PERIOD + 1) {
            log.warn("[BankNiftyRsi] seed — only {} in-session bars returned, need >= {}",
                fresh.size(), PERIOD + 1);
            return false;
        }
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
        if (fresh.size() > BUFFER_CAP) {
            state.recentBars = new ArrayList<>(fresh.subList(fresh.size() - BUFFER_CAP, fresh.size()));
        } else {
            state.recentBars = fresh;
        }
        state.todaySamples = new ArrayList<>();
        log.info("[BankNiftyRsi] seed — Wilder converged across {} bars, kept last {}, lastRsi={} (avgGain={}, avgLoss={})",
            fresh.size(), state.recentBars.size(), state.lastRsi,
            round2(state.avgGain), round2(state.avgLoss));
        saveToDisk();
        return true;
    }

    private static String bucketLabel(long startMillis) {
        return Instant.ofEpochMilli(startMillis).atZone(IST).format(HHMM);
    }

    private static double round2(double v) {
        if (Double.isInfinite(v) || Double.isNaN(v)) return v;
        return Math.round(v * 100.0) / 100.0;
    }

    public synchronized History history() {
        rolloverIfNewDay();
        boolean tradingDay = marketHolidayService.isTradingDay();
        List<RsiSample> samples = new ArrayList<>(state.todaySamples);
        if (!tradingDay) {
            return new History(state.dayKey, state.lastRsi, samples, false);
        }
        Double live = currentLiveRsi();
        if (live != null) {
            String tipLabel = currentBucketLabel();
            boolean alreadyClosed = !samples.isEmpty()
                && tipLabel.equals(samples.get(samples.size() - 1).t());
            if (!alreadyClosed) {
                samples.add(new RsiSample(tipLabel, round2(live)));
            }
            return new History(state.dayKey, round2(live), samples, true);
        }
        return new History(state.dayKey, state.lastRsi, samples, true);
    }

    public synchronized Double currentRsi() {
        Double live = currentLiveRsi();
        if (live != null) return round2(live);
        return state.lastRsi;
    }

    public synchronized LiveTip liveTip() {
        rolloverIfNewDay();
        boolean tradingDay = marketHolidayService.isTradingDay();
        RsiSample lastClosed = state.todaySamples.isEmpty()
            ? null
            : state.todaySamples.get(state.todaySamples.size() - 1);
        if (!tradingDay) {
            return new LiveTip(null, lastClosed, false);
        }
        Double live = currentLiveRsi();
        if (live == null) {
            return new LiveTip(null, lastClosed, true);
        }
        return new LiveTip(new RsiSample(currentBucketLabel(), round2(live)),
                           lastClosed, true);
    }

    private Double currentLiveRsi() {
        if (!state.wilderSeeded || state.recentBars.isEmpty()) return null;
        double ltp;
        try { ltp = marketDataService.getLtp(BANK_NIFTY_SYMBOL); }
        catch (Exception e) { return null; }
        if (ltp <= 0) return null;
        double lastClose = state.recentBars.get(state.recentBars.size() - 1).close();
        double diff = ltp - lastClose;
        double gain = diff >= 0 ?  diff : 0;
        double loss = diff <  0 ? -diff : 0;
        double avgGain = (state.avgGain * (PERIOD - 1) + gain) / PERIOD;
        double avgLoss = (state.avgLoss * (PERIOD - 1) + loss) / PERIOD;
        return rsiFromAverages(avgGain, avgLoss);
    }

    private static String currentBucketLabel() {
        LocalTime now = LocalTime.now(IST);
        int minOfDay = now.getHour() * 60 + now.getMinute();
        int bucketStart = (minOfDay / 5) * 5;
        int h = bucketStart / 60;
        int m = bucketStart - h * 60;
        return String.format("%02d:%02d", h, m);
    }

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
            log.warn("[BankNiftyRsi] failed to load state: {}", e.getMessage());
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
            log.warn("[BankNiftyRsi] failed to save state: {}", e.getMessage());
        }
    }

    public record Bar(String t, double close) {}
    public record RsiSample(String t, double rsi) {}
    public record History(String dayKey, Double lastRsi, List<RsiSample> samples, boolean tradingDay) {}
    public record LiveTip(RsiSample tip, RsiSample lastClosed, boolean tradingDay) {}

    public static class State {
        public String       dayKey       = LocalDate.now(IST).toString();
        public List<Bar>    recentBars   = new ArrayList<>();
        public double       avgGain      = 0;
        public double       avgLoss      = 0;
        public boolean      wilderSeeded = false;
        public Double       lastRsi      = null;
        public List<RsiSample> todaySamples = new ArrayList<>();
    }
}
