package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.Candle;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates Fyers HSM tick stream into 1-min OHLC bars per symbol and forwards
 * them to {@link CandleAggregator#appendOneMinBar}. Fyers HSM has no canonical
 * server-side bar push, so bars are built locally from the tick feed.
 *
 * <p>Bucketing key = {@code exchFeedTimeSec / 60} (falls back to
 * {@code lastTradedTimeSec} then wall clock if the tick lacks timestamps).
 * Boundary detection: when a tick's bucket key differs from the current bucket
 * for that symbol, the current bucket is emitted and a new one starts.
 *
 * <p>Volume: bars store {@code (sessionVolumeAtBucketEnd - sessionVolumeAtBucketStart)}
 * so pandas_ta VWAP inside {@link CandleAggregator} weights each bar by its own
 * trade volume, not by session-cumulative.
 *
 * <p>Late-tick guard: ticks whose bucket key is older than the current bucket
 * are dropped (we don't reopen closed bars — same trigger-candle invariant
 * CandleAggregator relied on during the tick-aggregation era).
 *
 * <p>Illiquid safety net: a {@code @Scheduled} 1 Hz sweep detects when wall
 * clock has advanced past a bucket's minute end but no tick has arrived to
 * close it, and emits the stale bucket so history doesn't stall.
 */
@Service
public class FyersMinuteBarBuilder {

    private static final Logger log = LoggerFactory.getLogger(FyersMinuteBarBuilder.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final MarketDataService marketDataService;
    private final CandleAggregator  candleAggregator;

    private static class Bucket {
        long   minuteEpoch;   // eftSec / 60
        long   startMs;       // minuteEpoch * 60_000
        double open;
        double high;
        double low;
        double close;
        long   startSessionVol;
        long   endSessionVol;
        int    tickCount;
        /** Last-observed Fyers ATP (Averaged Traded Price) during this minute.
         *  ATP is the exchange-computed session VWAP delivered on every FULL-
         *  mode tick — we carry the LAST value seen into the emitted 1-min
         *  bar's {@code vwap} field so downstream reads see the exchange's
         *  canonical session VWAP instead of a locally reconstructed one. */
        double lastAtp;
    }
    private final Map<String, Bucket> byBucket = new HashMap<>();
    /** IST calendar day of the most recent bucket built for each symbol.
     *  When {@link #startBucket} sees a bucket whose IST day differs from
     *  this (or has never seen the symbol before today), it treats the new
     *  bucket as the day's first and seeds {@code startSessionVol=0} so the
     *  09:15 opening-auction print is included in the first-minute volume
     *  and VWAP. Without this, the first-tick's cumulative sessionVol —
     *  which already contains the auction print — would be counted as
     *  pre-existing and drop the print from the 09:15 minute. */
    private final Map<String, java.time.LocalDate> lastBucketDayBySymbol = new HashMap<>();

    public FyersMinuteBarBuilder(MarketDataService marketDataService,
                                  CandleAggregator candleAggregator) {
        this.marketDataService = marketDataService;
        this.candleAggregator  = candleAggregator;
    }

    @PostConstruct
    public void boot() {
        marketDataService.addLtpListener(this::onTick);
        log.info("[FyersMinuteBarBuilder] wired to MarketDataService LTP stream — building 1-min bars per symbol");
    }

    /** Every tick from MarketDataService lands here. Runs inline on the WS
     *  callback thread — must not block. All mutation is guarded by the
     *  intrinsic monitor on the per-symbol {@link Bucket}. */
    void onTick(MarketDataService.LtpTick t) {
        if (t == null || t.ltp() <= 0) return;
        String symbol = t.fyersSymbol();
        if (symbol == null || symbol.isBlank()) return;

        long tickSec = t.lastTradedTimeSec() > 0 ? t.lastTradedTimeSec()
                     : t.exchFeedTimeSec()   > 0 ? t.exchFeedTimeSec()
                     : System.currentTimeMillis() / 1000L;
        long minuteEpoch = tickSec / 60L;

        Candle emitted = null;
        synchronized (this) {
            Bucket b = byBucket.get(symbol);
            if (b == null || b.minuteEpoch < 0) {
                b = startBucket(symbol, minuteEpoch, t.ltp(), t.sessionVolume(), isFirstBucketOfDay(symbol, minuteEpoch));
                if (t.atp() > 0) b.lastAtp = t.atp();
                byBucket.put(symbol, b);
                return;
            }
            if (minuteEpoch == b.minuteEpoch) {
                // Within-bucket update.
                if (t.ltp() > b.high) b.high = t.ltp();
                if (t.ltp() < b.low)  b.low  = t.ltp();
                b.close = t.ltp();
                if (t.sessionVolume() > 0) b.endSessionVol = t.sessionVolume();
                if (t.atp() > 0) b.lastAtp = t.atp();
                b.tickCount++;
            } else if (minuteEpoch > b.minuteEpoch) {
                // Boundary crossing — close current bucket, start new one.
                emitted = toCandle(b);
                Bucket next = startBucket(symbol, minuteEpoch, t.ltp(), t.sessionVolume(), isFirstBucketOfDay(symbol, minuteEpoch));
                if (t.atp() > 0) next.lastAtp = t.atp();
                byBucket.put(symbol, next);
            } else {
                // Stale tick (older minute than current bucket) — drop.
                return;
            }
        }
        if (emitted != null) candleAggregator.appendOneMinBar(symbol, emitted);
    }

    private static Bucket startBucket(String symbol, long minuteEpoch, double ltp,
                                       long sessionVol, boolean firstBucketOfDay) {
        Bucket b = new Bucket();
        b.minuteEpoch     = minuteEpoch;
        b.startMs         = minuteEpoch * 60_000L;
        b.open = b.high = b.low = b.close = ltp;
        // For the first bucket of the day per symbol: seed startSessionVol=0
        // so the day's opening auction print (already in the first tick's
        // cumulative sessionVol) counts as part of the 09:15 minute rather
        // than being treated as pre-existing volume.
        b.startSessionVol = firstBucketOfDay ? 0 : Math.max(0, sessionVol);
        b.endSessionVol   = Math.max(0, sessionVol);
        b.tickCount       = 1;
        return b;
    }

    /** Returns true when this bucket's IST calendar day differs from the
     *  last bucket we built for the symbol (or the symbol hasn't been seen
     *  yet). Called under the enclosing monitor. Side effect: updates the
     *  tracker. */
    private boolean isFirstBucketOfDay(String symbol, long minuteEpoch) {
        java.time.LocalDate day = java.time.Instant.ofEpochSecond(minuteEpoch * 60L)
            .atZone(IST).toLocalDate();
        java.time.LocalDate prev = lastBucketDayBySymbol.put(symbol, day);
        return prev == null || !prev.equals(day);
    }

    private static Candle toCandle(Bucket b) {
        // Emit the exchange-computed session VWAP (ATP) as the bar's vwap
        // field. CandleAggregator preserves this value on append (guarded
        // ATP > 0 → skip local recompute) so downstream reads see the
        // exchange's canonical session VWAP instead of a locally
        // reconstructed one.
        long vol = Math.max(0, b.endSessionVol - b.startSessionVol);
        return new Candle(b.open, b.high, b.low, b.close, vol, b.startMs, b.lastAtp);
    }

    /** Illiquid safety net — every second, sweep buckets whose minute has
     *  already passed on the wall clock and emit them, so history doesn't
     *  stall on a symbol that stopped ticking mid-minute. Skipped outside
     *  market hours to avoid emitting stragglers at 9:14. */
    @Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void sweepStaleBuckets() {
        LocalTime nowIst = ZonedDateTime.now(IST).toLocalTime();
        if (nowIst.isBefore(LocalTime.of(9, 15)) || nowIst.isAfter(LocalTime.of(15, 41))) return;
        long nowMinuteEpoch = System.currentTimeMillis() / 60_000L;
        Map<String, Candle> toEmit = new HashMap<>();
        synchronized (this) {
            for (var e : byBucket.entrySet()) {
                Bucket b = e.getValue();
                if (b == null || b.minuteEpoch <= 0) continue;
                if (b.minuteEpoch < nowMinuteEpoch) {
                    toEmit.put(e.getKey(), toCandle(b));
                    // Reset bucket — next tick starts fresh.
                    b.minuteEpoch = -1;
                }
            }
        }
        for (var e : toEmit.entrySet()) {
            candleAggregator.appendOneMinBar(e.getKey(), e.getValue());
        }
    }

    /** Snapshot of the currently-open 1-min bucket for {@code symbol}, or
     *  {@code null} when the symbol has no bucket yet. Read is non-mutating
     *  and takes the enclosing monitor briefly. Volume field carries the
     *  session-volume delta accumulated within the bucket so far. */
    public Candle getInProgressBar(String symbol) {
        if (symbol == null || symbol.isBlank()) return null;
        synchronized (this) {
            Bucket b = byBucket.get(symbol);
            if (b == null || b.minuteEpoch <= 0) return null;
            long vol = Math.max(0, b.endSessionVol - b.startSessionVol);
            // Forming bar carries the latest ATP too so the chart's live
            // VWAP line updates tick-by-tick against exchange-canonical
            // values (matches Fyers's chart VWAP exactly).
            return new Candle(b.open, b.high, b.low, b.close, vol, b.startMs, b.lastAtp);
        }
    }

    /** Diagnostic — human-readable summary of per-symbol bucket state. */
    public String debugSnapshot() {
        StringBuilder sb = new StringBuilder("FyersMinuteBarBuilder buckets: ");
        synchronized (this) {
            byBucket.forEach((sym, b) -> {
                if (b == null || b.minuteEpoch <= 0) return;
                String startTs = Instant.ofEpochMilli(b.startMs).atZone(IST).toLocalTime().toString();
                sb.append("[").append(sym).append(" @ ").append(startTs)
                  .append(" o=").append(b.open).append(" h=").append(b.high)
                  .append(" l=").append(b.low).append(" c=").append(b.close)
                  .append(" v=").append(Math.max(0, b.endSessionVol - b.startSessionVol))
                  .append(" ticks=").append(b.tickCount).append("] ");
            });
        }
        return sb.toString();
    }
}
