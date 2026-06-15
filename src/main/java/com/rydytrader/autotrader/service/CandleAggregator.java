package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.Candle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Samples the NIFTY LTP from {@link MarketDataService} once per second and rolls samples
 * into 5-minute OHLC buckets. On bucket close (the first sample in a new 5-min window),
 * the closed candle is emitted to every registered listener.
 *
 * <p>Sampling-based rather than tick-based to avoid wiring a listener API into the existing
 * {@link MarketDataService}. NIFTY index ticks at sub-second cadence; a 1 s sample loses
 * almost no information at the OHLC granularity that 5-min candles report.
 *
 * <p>Buckets are anchored on the IST wall clock — 09:15, 09:20, 09:25, … 15:25, 15:30.
 * The aggregator only emits during market hours (09:15 ≤ now ≤ 15:30).
 */
@Service
public class CandleAggregator {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregator.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int BUCKET_MINUTES = 5;

    private final MarketDataService marketDataService;
    private final CopyOnWriteArrayList<Consumer<Candle>> listeners = new CopyOnWriteArrayList<>();

    // Current open bucket
    private int currentBucketMinute = -1;  // minutes-of-day for bucket start, -1 = none
    private long currentBucketStartMs = 0;
    private double openPx = 0, highPx = 0, lowPx = 0, closePx = 0;

    public CandleAggregator(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    /** Register a callback fired exactly once per 5-min candle close. */
    public void onCandleClose(Consumer<Candle> listener) {
        if (listener != null) listeners.add(listener);
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void sample() {
        ZonedDateTime nowIst = ZonedDateTime.now(IST);
        LocalTime t = nowIst.toLocalTime();
        if (t.isBefore(LocalTime.of(9, 15)) || t.isAfter(LocalTime.of(15, 31))) {
            // outside market hours — flush any straggler bucket
            if (currentBucketMinute >= 0) closeAndReset();
            return;
        }

        double ltp;
        try { ltp = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { return; }
        if (ltp <= 0) return;

        int minuteOfDay = t.getHour() * 60 + t.getMinute();
        int bucketStart = (minuteOfDay / BUCKET_MINUTES) * BUCKET_MINUTES;

        if (currentBucketMinute < 0) {
            // First sample of the day — open a new bucket without emitting anything.
            startBucket(bucketStart, ltp, nowIst);
            return;
        }
        if (bucketStart != currentBucketMinute) {
            // Rolled over — close the current bucket, fire listeners, start fresh.
            emitClosed();
            startBucket(bucketStart, ltp, nowIst);
            return;
        }
        // Same bucket — update OHLC.
        if (ltp > highPx) highPx = ltp;
        if (ltp < lowPx)  lowPx  = ltp;
        closePx = ltp;
    }

    private void startBucket(int bucketStart, double ltp, ZonedDateTime nowIst) {
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

    private void emitClosed() {
        Candle c = new Candle(
            round(openPx), round(highPx), round(lowPx), round(closePx), 0L, currentBucketStartMs);
        log.info("[CandleAggregator] 5-min close — o={} h={} l={} c={} startMs={}",
            c.open(), c.high(), c.low(), c.close(), c.startMillis());
        for (Consumer<Candle> l : listeners) {
            try { l.accept(c); }
            catch (Exception e) { log.warn("[CandleAggregator] listener threw: {}", e.getMessage()); }
        }
    }

    private void closeAndReset() {
        emitClosed();
        currentBucketMinute = -1;
        currentBucketStartMs = 0;
        openPx = highPx = lowPx = closePx = 0;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
