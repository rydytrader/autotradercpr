package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.Candle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Samples LTPs for every subscribed Fyers symbol once per second and rolls samples into 5-minute
 * OHLC buckets per symbol. On bucket close (the first sample in a new 5-min window), the closed
 * candle is emitted to every listener registered for that symbol.
 *
 * <p>Buckets are anchored on the IST wall clock — 09:15, 09:20, 09:25, … 15:25, 15:30 — and only
 * emitted during market hours (09:15 ≤ now ≤ 15:30).
 */
@Service
public class CandleAggregator {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregator.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int BUCKET_MINUTES = 5;

    private final MarketDataService marketDataService;

    private final Map<String, Bucket> bucketBySymbol = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<Consumer<Candle>>> listenersBySymbol = new ConcurrentHashMap<>();

    public CandleAggregator(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    /** Subscribe to 5-min candle closes on {@code symbol}. The symbol is also added to the
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

            if (b.currentBucketMinute < 0) {
                // First sample for this symbol — open a bucket without emitting anything.
                b.start(bucketStart, ltp, nowIst);
                continue;
            }
            if (bucketStart != b.currentBucketMinute) {
                // Rolled over — close current bucket for THIS symbol, fire listeners, start fresh.
                emitClosed(symbol, b);
                b.start(bucketStart, ltp, nowIst);
                continue;
            }
            // Same bucket — update OHLC.
            if (ltp > b.highPx) b.highPx = ltp;
            if (ltp < b.lowPx)  b.lowPx  = ltp;
            b.closePx = ltp;
        }
    }

    private void emitClosed(String symbol, Bucket b) {
        Candle c = new Candle(
            round(b.openPx), round(b.highPx), round(b.lowPx), round(b.closePx),
            0L, b.currentBucketStartMs);
        // Per-symbol candle-close logging is too chatty (one line per symbol every 5 min).
        // Demoted to debug so it stays available for troubleshooting without spamming INFO.
        log.debug("[CandleAggregator] {} 5-min close — o={} h={} l={} c={} startMs={}",
            symbol, c.open(), c.high(), c.low(), c.close(), c.startMillis());
        CopyOnWriteArrayList<Consumer<Candle>> ls = listenersBySymbol.get(symbol);
        if (ls == null) return;
        for (Consumer<Candle> l : ls) {
            try { l.accept(c); }
            catch (Exception e) { log.warn("[CandleAggregator] {} listener threw: {}", symbol, e.getMessage()); }
        }
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Per-symbol bucket state. */
    private static class Bucket {
        int currentBucketMinute = -1;
        long currentBucketStartMs = 0;
        double openPx = 0, highPx = 0, lowPx = 0, closePx = 0;

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
        }

        void reset() {
            currentBucketMinute = -1;
            currentBucketStartMs = 0;
            openPx = highPx = lowPx = closePx = 0;
        }
    }
}
