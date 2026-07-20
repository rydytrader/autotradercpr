package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.util.FileIoUtils;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Samples LTPs for every subscribed Fyers symbol once per second and rolls samples into 2-minute
 * OHLC buckets per symbol. On bucket close (the first sample in a new 2-min window), the closed
 * candle is emitted to every listener registered for that symbol.
 *
 * <p>Buckets are anchored on the IST wall clock — 09:15, 09:17, 09:19, … 15:29, 15:31 — and only
 * emitted during market hours (09:15 ≤ now ≤ 15:31).
 *
 * <p>{@link #BUCKET_MINUTES} is public so downstream consumers can derive bar length without
 * hardcoding it and risk drifting from the aggregator's actual cadence.
 */
@Service
public class CandleAggregator {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregator.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    public  static final int    BUCKET_MINUTES = 2;
    /** NSE market open in minutes-of-day (IST). Bucket boundaries are computed relative
     *  to this so the first 2-min bar spans 09:15→09:17, not 09:14→09:16. */
    private static final int    MARKET_OPEN_MINUTE_OF_DAY = 9 * 60 + 15;

    private final MarketDataService marketDataService;

    private final Map<String, Bucket> bucketBySymbol = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<Consumer<Candle>>> listenersBySymbol = new ConcurrentHashMap<>();
    /** Closed 2-min candles per symbol, kept in a bounded FIFO ring. Populated by
     *  {@link #emitClosed} so the chart page can render the day's session without a
     *  Fyers-history REST fetch. Cap = 250 = ~8.3 h of 2-min bars, well over the
     *  ~187 bars in a full NSE session. */
    private static final int HISTORY_CAP = 250;
    private final Map<String, Deque<Candle>> historyBySymbol = new ConcurrentHashMap<>();

    /** Where the closed-candle rings + in-progress buckets get persisted. Reloaded on
     *  boot when the file's dayKey matches today so a mid-day JVM restart doesn't wipe
     *  the intraday chart history. Kept under {@code store/cache/} alongside the
     *  strategy state files — {@code store/data/} is reserved for permanent per-day
     *  event / trade artefacts. */
    private static final String STATE_FILE = "../store/cache/candle-aggregator-state.json";
    private final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .findAndRegisterModules();
    private volatile boolean dirty = false;

    public CandleAggregator(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    @PostConstruct
    public void boot() {
        Path p = Path.of(STATE_FILE);
        boolean exists = Files.exists(p);
        log.info("[CandleAggregator] boot — state file {} at {}",
            exists ? "present" : "absent", p.toAbsolutePath());
        loadFromDisk();
        if (!exists) {
            log.info("[CandleAggregator] fresh start — no prior state to restore; history will accumulate from now on");
        }
    }

    @PreDestroy
    public void shutdown() {
        // Best-effort final flush so a graceful shutdown captures the latest in-progress
        // buckets too. Uncaught exceptions get swallowed by Spring's PreDestroy handler
        // anyway; saveToDisk logs internally.
        saveToDisk();
    }

    /** Subscribe to 2-min candle closes on {@code symbol}. The symbol is also added to the
     *  Fyers market-data feed if it isn't already streaming. Multiple subscribers per symbol
     *  are allowed; each gets called on every close. */
    public void subscribe(String symbol, Consumer<Candle> listener) {
        if (symbol == null || symbol.isBlank() || listener == null) return;
        listenersBySymbol
            .computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>())
            .add(listener);
        try { marketDataService.subscribeAdditional(Collections.singletonList(symbol)); }
        catch (Exception ignored) {}
    }

    /** Stop emitting candles for {@code symbol}. Drops every listener and the in-flight bucket.
     *  The Fyers market-data subscription is NOT cancelled here — the caller can rely on the
     *  existing position/strategy logic to manage the underlying WS subscription. */
    public void unsubscribe(String symbol) {
        if (symbol == null || symbol.isBlank()) return;
        listenersBySymbol.remove(symbol);
        bucketBySymbol.remove(symbol);
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void sample() {
        ZonedDateTime nowIst = ZonedDateTime.now(IST);
        LocalTime t = nowIst.toLocalTime();
        if (t.isBefore(LocalTime.of(9, 15)) || t.isAfter(LocalTime.of(15, 31))) {
            // outside market hours — flush any straggler buckets
            for (Map.Entry<String, Bucket> e : bucketBySymbol.entrySet()) {
                Bucket b = e.getValue();
                if (b.currentBucketMinute >= 0) {
                    emitClosed(e.getKey(), b);
                    b.reset();
                }
            }
            return;
        }

        int minuteOfDay = t.getHour() * 60 + t.getMinute();
        // Bucket boundaries anchored on 09:15 IST — the true market-open minute — not on
        // midnight. Otherwise 09:15 (odd) rounds down to the 09:14 even-minute bucket and
        // the "first 2-min bar" closes at 09:16 after only ~1 minute of data. Anchoring
        // on 09:15 gives 09:15→09:17, 09:17→09:19, … as the docs promise.
        int minutesSinceOpen = minuteOfDay - MARKET_OPEN_MINUTE_OF_DAY;
        int bucketStart      = minutesSinceOpen >= 0
            ? MARKET_OPEN_MINUTE_OF_DAY + (minutesSinceOpen / BUCKET_MINUTES) * BUCKET_MINUTES
            : (minuteOfDay / BUCKET_MINUTES) * BUCKET_MINUTES;   // pre-open safety net

        for (String symbol : listenersBySymbol.keySet()) {
            double ltp;
            try { ltp = marketDataService.getLtp(symbol); }
            catch (Exception e) { continue; }
            if (ltp <= 0) continue;

            Bucket b = bucketBySymbol.computeIfAbsent(symbol, k -> new Bucket());

            // Session VWAP (Fyers ATP). Zero for index symbols and pre-first-tick option
            // symbols — held at 0 in that case rather than clobbering a previous positive
            // value with zeros mid-session (WS occasionally re-emits ticks without ATP).
            double vwap = 0;
            try { vwap = marketDataService.getVwap(symbol); } catch (Exception ignored) {}
            if (b.currentBucketMinute < 0) {
                // First sample for this symbol — open a bucket without emitting anything.
                b.start(bucketStart, ltp, nowIst);
                if (vwap > 0) b.vwapLast = vwap;
                dirty = true;
                continue;
            }
            if (bucketStart != b.currentBucketMinute) {
                // Rolled over — close current bucket for THIS symbol, fire listeners, start fresh.
                emitClosed(symbol, b);
                b.start(bucketStart, ltp, nowIst);
                if (vwap > 0) b.vwapLast = vwap;
                dirty = true;
                continue;
            }
            // Same bucket — update OHLC + latest VWAP.
            if (ltp > b.highPx) b.highPx = ltp;
            if (ltp < b.lowPx)  b.lowPx  = ltp;
            b.closePx = ltp;
            if (vwap > 0) b.vwapLast = vwap;
            dirty = true;
        }
    }

    // ── Persistence ────────────────────────────────────────────────────────────

    /** Periodic writer — flushes state to disk if anything changed since the last save.
     *  30-s cadence caps write frequency (one write ≈ 15 symbols × 200 candles ≈ 300 KB)
     *  regardless of how many bucket updates the tick sampler produces per second. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void periodicSave() {
        if (!dirty) return;
        saveToDisk();
    }

    /** Deserialise the on-disk state into the runtime maps. Any prior-day snapshot is
     *  discarded — cross-session persistence for chart history would show stale bars,
     *  and the strategy state is intraday anyway. */
    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s == null) return;
            String today = LocalDate.now(IST).toString();
            if (!today.equals(s.dayKey)) {
                log.info("[CandleAggregator] discarding stale state.json — dayKey={} today={}", s.dayKey, today);
                return;
            }
            int candles = 0;
            if (s.historyBySymbol != null) {
                for (Map.Entry<String, List<Candle>> e : s.historyBySymbol.entrySet()) {
                    if (e.getKey() == null || e.getValue() == null || e.getValue().isEmpty()) continue;
                    historyBySymbol.put(e.getKey(), new ConcurrentLinkedDeque<>(e.getValue()));
                    candles += e.getValue().size();
                }
            }
            int buckets = 0;
            if (s.bucketBySymbol != null) {
                for (Map.Entry<String, SerBucket> e : s.bucketBySymbol.entrySet()) {
                    if (e.getKey() == null || e.getValue() == null) continue;
                    Bucket b = new Bucket();
                    SerBucket sb = e.getValue();
                    b.currentBucketMinute  = sb.currentBucketMinute;
                    b.currentBucketStartMs = sb.currentBucketStartMs;
                    b.openPx               = sb.openPx;
                    b.highPx               = sb.highPx;
                    b.lowPx                = sb.lowPx;
                    b.closePx              = sb.closePx;
                    b.vwapLast             = sb.vwapLast;
                    bucketBySymbol.put(e.getKey(), b);
                    buckets++;
                }
            }
            log.info("[CandleAggregator] restored {} candles across {} symbols + {} in-progress buckets for {}",
                candles, historyBySymbol.size(), buckets, today);
        } catch (IOException e) {
            log.warn("[CandleAggregator] failed to load state: {}", e.getMessage());
        }
    }

    /** Serialise the current state atomically. Rolls a stale dayKey on first save of a
     *  new day so tomorrow's history doesn't inherit yesterday's leftovers on disk. */
    private synchronized void saveToDisk() {
        try {
            String today = LocalDate.now(IST).toString();
            State s = new State();
            s.dayKey = today;
            for (Map.Entry<String, Deque<Candle>> e : historyBySymbol.entrySet()) {
                s.historyBySymbol.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
            for (Map.Entry<String, Bucket> e : bucketBySymbol.entrySet()) {
                Bucket b = e.getValue();
                if (b == null || b.currentBucketMinute < 0) continue;
                SerBucket sb = new SerBucket();
                sb.currentBucketMinute  = b.currentBucketMinute;
                sb.currentBucketStartMs = b.currentBucketStartMs;
                sb.openPx               = b.openPx;
                sb.highPx               = b.highPx;
                sb.lowPx                = b.lowPx;
                sb.closePx              = b.closePx;
                sb.vwapLast             = b.vwapLast;
                s.bucketBySymbol.put(e.getKey(), sb);
            }

            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(s));
            FileIoUtils.atomicMoveWithRetry(tmp, dst);
            dirty = false;
        } catch (IOException e) {
            log.warn("[CandleAggregator] failed to save state: {}", e.getMessage());
        }
    }

    // ── Serialisation POJOs ───────────────────────────────────────────────────

    /** Persisted snapshot of the aggregator. Discarded on load if {@link #dayKey} isn't
     *  today. */
    public static class State {
        public String dayKey = "";
        public Map<String, List<Candle>> historyBySymbol = new LinkedHashMap<>();
        public Map<String, SerBucket>    bucketBySymbol  = new LinkedHashMap<>();
    }

    /** Public mirror of the private {@link Bucket} so Jackson can round-trip it. */
    public static class SerBucket {
        public int    currentBucketMinute   = -1;
        public long   currentBucketStartMs  = 0;
        public double openPx = 0, highPx = 0, lowPx = 0, closePx = 0;
        public double vwapLast = 0;
    }

    private void emitClosed(String symbol, Bucket b) {
        Candle c = new Candle(
            round(b.openPx), round(b.highPx), round(b.lowPx), round(b.closePx),
            0L, b.currentBucketStartMs, round(b.vwapLast));
        // Per-symbol candle-close logging is too chatty (one line per symbol every 5 min).
        // Demoted to debug so it stays available for troubleshooting without spamming INFO.
        log.debug("[CandleAggregator] {} {}-min close — o={} h={} l={} c={} startMs={}",
            symbol, BUCKET_MINUTES, c.open(), c.high(), c.low(), c.close(), c.startMillis());
        // Retain in the per-symbol history ring so the chart page has intraday context.
        Deque<Candle> ring = historyBySymbol.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        ring.addLast(c);
        while (ring.size() > HISTORY_CAP) ring.pollFirst();
        dirty = true;
        CopyOnWriteArrayList<Consumer<Candle>> ls = listenersBySymbol.get(symbol);
        if (ls == null) return;
        for (Consumer<Candle> l : ls) {
            try { l.accept(c); }
            catch (Exception e) { log.warn("[CandleAggregator] {} listener threw: {}", symbol, e.getMessage()); }
        }
    }

    /** Closed 2-min candles for {@code symbol} in chronological order. Empty when the
     *  symbol hasn't rolled a bucket yet (freshly subscribed, or pre-market boot). */
    public List<Candle> getHistory(String symbol) {
        Deque<Candle> ring = historyBySymbol.get(symbol);
        if (ring == null || ring.isEmpty()) return Collections.emptyList();
        return new ArrayList<>(ring);
    }

    /** In-progress 2-min bucket for {@code symbol}, synthesised into a {@code Candle}
     *  using the running OHLC state. {@code null} when the symbol has no open bucket
     *  (never sampled, or reset outside market hours). */
    public Candle getCurrentBucket(String symbol) {
        Bucket b = bucketBySymbol.get(symbol);
        if (b == null || b.currentBucketMinute < 0) return null;
        return new Candle(
            round(b.openPx), round(b.highPx), round(b.lowPx), round(b.closePx),
            0L, b.currentBucketStartMs, round(b.vwapLast));
    }

    /** True when at least one listener is registered for {@code symbol}. Consumers that
     *  need the aggregator to start bucketing a specific symbol (e.g. the chart page
     *  requesting an ad-hoc symbol) can gate a no-op {@link #subscribe} on this. */
    public boolean isSubscribed(String symbol) {
        return symbol != null && listenersBySymbol.containsKey(symbol);
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Per-symbol bucket state. */
    private static class Bucket {
        int currentBucketMinute = -1;
        long currentBucketStartMs = 0;
        double openPx = 0, highPx = 0, lowPx = 0, closePx = 0;
        /** Session VWAP as of the most recent sample. Preserved across bucket rolls so
         *  the overlay curve stays continuous (VWAP is cumulative-since-open, not
         *  per-bar). */
        double vwapLast = 0;

        void start(int bucketStart, double ltp, ZonedDateTime nowIst) {
            currentBucketMinute  = bucketStart;
            ZonedDateTime bucketStartTime = nowIst.withHour(bucketStart / 60)
                .withMinute(bucketStart % 60)
                .withSecond(0)
                .withNano(0);
            currentBucketStartMs = bucketStartTime.toInstant().toEpochMilli();
            openPx  = ltp;
            highPx  = ltp;
            lowPx   = ltp;
            closePx = ltp;
            // vwapLast intentionally NOT reset — VWAP is a running session metric.
        }

        void reset() {
            currentBucketMinute = -1;
            currentBucketStartMs = 0;
            openPx = highPx = lowPx = closePx = 0;
            vwapLast = 0;
        }
    }
}
