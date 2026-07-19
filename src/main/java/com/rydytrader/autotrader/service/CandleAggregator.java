package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.Candle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
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

    private final MarketDataService marketDataService;

    private final Map<String, Bucket> bucketBySymbol = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<Consumer<Candle>>> listenersBySymbol = new ConcurrentHashMap<>();
    /** Closed 2-min candles per symbol, kept in a bounded FIFO ring. Populated by
     *  {@link #emitClosed} so the chart page can render the day's session without a
     *  Fyers-history REST fetch. Cap = 250 = ~8.3 h of 2-min bars, well over the
     *  ~187 bars in a full NSE session. */
    private static final int HISTORY_CAP = 250;
    private final Map<String, Deque<Candle>> historyBySymbol = new ConcurrentHashMap<>();

    public CandleAggregator(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
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
        int bucketStart = (minuteOfDay / BUCKET_MINUTES) * BUCKET_MINUTES;

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
                continue;
            }
            if (bucketStart != b.currentBucketMinute) {
                // Rolled over — close current bucket for THIS symbol, fire listeners, start fresh.
                emitClosed(symbol, b);
                b.start(bucketStart, ltp, nowIst);
                if (vwap > 0) b.vwapLast = vwap;
                continue;
            }
            // Same bucket — update OHLC + latest VWAP.
            if (ltp > b.highPx) b.highPx = ltp;
            if (ltp < b.lowPx)  b.lowPx  = ltp;
            b.closePx = ltp;
            if (vwap > 0) b.vwapLast = vwap;
        }
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
