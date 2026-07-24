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
import java.time.Instant;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    /** Single-threaded executor for firing user close listeners off the WebSocket thread.
     *  {@link #onLtpTick} runs inline on the WS callback thread — if it also invoked
     *  {@code AtmVwap.onCandleClose} → {@code saveToDisk} inline, WS tick throughput
     *  would stall on file I/O. Single-threaded so bar-close events for a given symbol
     *  fire in order. */
    private final ExecutorService closeExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "candle-close");
        t.setDaemon(true);
        return t;
    });

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
        // Push-based tick ingest: fires on every WS snapshot (~4 Hz per symbol) instead
        // of the 1 Hz sample() poll. sample() stays live as an outside-hours flush + a
        // wall-clock rollover safety net for symbols whose exchange feed has gone quiet.
        marketDataService.addLtpListener(this::onLtpTick);
    }

    @PreDestroy
    public void shutdown() {
        // Best-effort final flush so a graceful shutdown captures the latest in-progress
        // buckets too. Uncaught exceptions get swallowed by Spring's PreDestroy handler
        // anyway; saveToDisk logs internally.
        saveToDisk();
        closeExecutor.shutdown();
        try { closeExecutor.awaitTermination(500, TimeUnit.MILLISECONDS); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
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

    /** Runs every second as a safety net alongside the push-based {@link #onLtpTick} path.
     *  Two remaining jobs:
     *  <ul>
     *    <li>Outside market hours: flush any straggler open bucket (a tick landed at
     *        15:29:58, no more ticks after that; without a poll the last bar never closes).</li>
     *    <li>During market hours: if wall-clock has advanced past a bucket's end but no
     *        new tick has arrived (WS feed hiccup, illiquid symbol going quiet), roll it
     *        forward so the FSM downstream isn't waiting on a stale bar.</li>
     *  </ul>
     *  All the intra-bar OHLC/VWAP updates now happen in {@link #onLtpTick} — no more
     *  1 Hz polling of {@link MarketDataService#getLtp} for high/low tracking. */
    @Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void sample() {
        ZonedDateTime nowIst = ZonedDateTime.now(IST);
        LocalTime t = nowIst.toLocalTime();
        if (t.isBefore(LocalTime.of(9, 15))) {
            // Pre-market — DO NOT flush. A bucket may already exist for the 09:15→09:17
            // slot because Fyers rounds exch_feed_time up to the next second (a print at
            // 09:14:59.9 lands with exch_feed_time = 09:15:00, opening a legit 09:15
            // bucket even though wall-clock is still 09:14:xx). Flushing here would emit
            // that bucket as a "close" event at 09:14:xx, which AtmVwap.onCandleClose
            // interprets as the first-bar close and resolves ATM three minutes early.
            // Wait for wall-clock to catch up — in-market-hours flush path handles it.
            return;
        }
        if (t.isAfter(LocalTime.of(15, 31))) {
            // Post-market — flush the day's last-bar stragglers. Safe to unconditionally
            // close: no legit forward-dated bucket can exist after market close.
            for (Map.Entry<String, Bucket> e : bucketBySymbol.entrySet()) {
                Bucket b = e.getValue();
                synchronized (b) {
                    if (b.currentBucketMinute >= 0) {
                        emitClosed(e.getKey(), b);
                        b.reset();
                    }
                }
            }
            return;
        }

        // In-hours: only close a bucket the wall clock has moved PAST. Ordering matters —
        // buckets can (and do) sit AHEAD of the wall clock when exchFeedTime rounds up to
        // the next second or the local system clock trails exchange time by a second or
        // two. Flushing an ahead-of-wall bucket would emit spurious closes every second
        // until wall catches up, which fires the FSM (and /history reconcile) repeatedly
        // for the same bar. Only close when wall has genuinely passed the bucket end.
        int wallBucketStart = bucketStartMinute(t.getHour() * 60 + t.getMinute());
        for (Map.Entry<String, Bucket> e : bucketBySymbol.entrySet()) {
            String symbol = e.getKey();
            Bucket b = e.getValue();
            synchronized (b) {
                if (b.currentBucketMinute < 0) continue;
                if (wallBucketStart <= b.currentBucketMinute) continue;
                emitClosed(symbol, b);
                b.reset();
                dirty = true;
            }
        }
    }

    /** Anchors 2-min bucket boundaries on 09:15 IST — the true market-open minute — not on
     *  midnight. Otherwise 09:15 (odd) rounds down to the 09:14 even-minute bucket and
     *  the "first 2-min bar" closes at 09:16 after only ~1 minute of data. Anchoring on
     *  09:15 gives 09:15→09:17, 09:17→09:19, … as the docs promise. Pre-open ticks
     *  (before 09:15) fall back to midnight-anchored buckets — harmless because sample()
     *  gates outside market hours anyway. */
    private static int bucketStartMinute(int minuteOfDay) {
        int minutesSinceOpen = minuteOfDay - MARKET_OPEN_MINUTE_OF_DAY;
        return minutesSinceOpen >= 0
            ? MARKET_OPEN_MINUTE_OF_DAY + (minutesSinceOpen / BUCKET_MINUTES) * BUCKET_MINUTES
            : (minuteOfDay / BUCKET_MINUTES) * BUCKET_MINUTES;
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
            // In-progress buckets are intentionally NOT restored — persisting them across
            // JVM lifetimes is what introduced the day-key drift bug (yesterday's H/L
            // resurrected as today's first-bar close). {@code /history} reconcile fixes
            // any lost H/L on the next bar close anyway. The SerBucket class is kept for
            // round-trip compatibility with older state files; if any are present in the
            // JSON they get silently ignored.
            log.info("[CandleAggregator] restored {} candles across {} symbols for {}",
                candles, historyBySymbol.size(), today);
        } catch (IOException e) {
            log.warn("[CandleAggregator] failed to load state: {}", e.getMessage());
        }
    }

    /** Serialise the current state atomically. Only closed candles are persisted —
     *  in-progress buckets are deliberately dropped so a JVM lifetime never inherits an
     *  earlier session's partial bar (that's what caused the pre-market 24150 ATM
     *  misfire). Any lost intra-bar H/L is corrected by the {@code /history} reconcile
     *  on the next bar close. */
    private synchronized void saveToDisk() {
        try {
            String today = LocalDate.now(IST).toString();
            State s = new State();
            s.dayKey = today;
            for (Map.Entry<String, Deque<Candle>> e : historyBySymbol.entrySet()) {
                s.historyBySymbol.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
            // bucketBySymbol intentionally NOT written — see method javadoc.

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
        appendHistoryAndFire(symbol, c, false);
    }

    /** Records a closed candle in the per-symbol ring and fans it out to registered
     *  listeners. When {@code async} is true, listener callbacks run on {@link #closeExecutor}
     *  so the caller (the WS thread inside {@link #onLtpTick}) doesn't block on downstream
     *  {@code saveToDisk} calls; the history-ring update always happens inline so
     *  {@link #getHistory} reflects the close immediately regardless. */
    private void appendHistoryAndFire(String symbol, Candle c, boolean async) {
        log.debug("[CandleAggregator] {} {}-min close — o={} h={} l={} c={} startMs={}",
            symbol, BUCKET_MINUTES, c.open(), c.high(), c.low(), c.close(), c.startMillis());
        Deque<Candle> ring = historyBySymbol.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        ring.addLast(c);
        while (ring.size() > HISTORY_CAP) ring.pollFirst();
        dirty = true;
        CopyOnWriteArrayList<Consumer<Candle>> ls = listenersBySymbol.get(symbol);
        if (ls == null) return;
        Runnable fanout = () -> {
            for (Consumer<Candle> l : ls) {
                try { l.accept(c); }
                catch (Exception e) { log.warn("[CandleAggregator] {} listener threw: {}", symbol, e.getMessage()); }
            }
        };
        if (async) closeExecutor.execute(fanout);
        else       fanout.run();
    }

    /** Registered as an LTP listener on {@link MarketDataService} in {@link #boot()}. Runs
     *  inline on the WS callback thread — MUST NOT block. Bucket manipulation is guarded
     *  by {@code synchronized(b)} so a concurrent {@link #sample()} on a different symbol's
     *  bucket doesn't matter, and the listener-fanout on close is dispatched to
     *  {@link #closeExecutor} to keep WS tick throughput off file-I/O paths. */
    void onLtpTick(MarketDataService.LtpTick t) {
        if (t == null) return;
        String symbol = t.fyersSymbol();
        if (symbol == null) return;
        // Only aggregate symbols that have a registered close-listener — pruning here
        // matches the semantic of sample() (which iterates listenersBySymbol.keySet()).
        if (!listenersBySymbol.containsKey(symbol)) return;
        double ltp = t.ltp();
        if (ltp <= 0) return;

        // Bucket the tick by the EXCHANGE's own last-traded time (LTT) when available —
        // that's the timestamp on the actual trade that produced this LTP, and it's what
        // TradingView aligns bar boundaries on. Fall back to Fyers/GDFL dissemination time
        // (EFT) — which typically trails LTT by 100-800 ms of ingest lag — and to
        // wall-clock only when the parser couldn't extract either. This closes the
        // "boundary skew" gap where a tick TRADED at 09:16:59 arrives locally at
        // 09:17:00.1 and was previously attributed to the wrong bar.
        //
        // Freshness guard — a WS reconnect can replay a tick whose timestamp is from
        // a PREVIOUS trading day (e.g. yesterday 15:29). Its LocalTime alone would pass
        // the market-hours filter and create a today-dated bucket with yesterday's data,
        // which the outside-hours flush would later emit as a "close" pre-market.
        long tickSec = t.lastTradedTimeSec() > 0 ? t.lastTradedTimeSec()
                     : t.exchFeedTimeSec()   > 0 ? t.exchFeedTimeSec()
                     : 0L;
        LocalTime tickTime;
        String today = LocalDate.now(IST).toString();
        if (tickSec > 0) {
            ZonedDateTime tickZdt = Instant.ofEpochSecond(tickSec).atZone(IST);
            String tickDay = tickZdt.toLocalDate().toString();
            if (!today.equals(tickDay)) return;
            tickTime = tickZdt.toLocalTime();
        } else {
            tickTime = ZonedDateTime.now(IST).toLocalTime();
        }
        if (tickTime.isBefore(LocalTime.of(9, 15)) || tickTime.isAfter(LocalTime.of(15, 31))) return;
        int bucketStart = bucketStartMinute(tickTime.getHour() * 60 + tickTime.getMinute());

        Bucket b = bucketBySymbol.computeIfAbsent(symbol, k -> new Bucket());
        Candle closed = null;
        synchronized (b) {
            if (b.currentBucketMinute < 0) {
                b.start(bucketStart, ltp, ZonedDateTime.now(IST));
                if (t.atp() > 0) b.vwapLast = t.atp();
            } else if (bucketStart != b.currentBucketMinute) {
                // Snapshot for async fanout OUTSIDE the sync block below.
                closed = new Candle(
                    round(b.openPx), round(b.highPx), round(b.lowPx), round(b.closePx),
                    0L, b.currentBucketStartMs, round(b.vwapLast));
                b.start(bucketStart, ltp, ZonedDateTime.now(IST));
                if (t.atp() > 0) b.vwapLast = t.atp();
            } else {
                if (ltp > b.highPx) b.highPx = ltp;
                if (ltp < b.lowPx)  b.lowPx  = ltp;
                b.closePx = ltp;
                if (t.atp() > 0) b.vwapLast = t.atp();
            }
        }
        dirty = true;
        if (closed != null) appendHistoryAndFire(symbol, closed, true);
    }

    /** Overwrites an existing history-ring entry with an authoritative version — used
     *  after {@code AtmVwap.reconcileBar} pulls the exchange-published OHLC via Fyers
     *  {@code /history}, so the /chart page (which polls this ring) shows the same
     *  values TradingView shows for closed bars. Matches by {@code startMillis}; when no
     *  entry with that start exists (very recent boot, or ring rolled past the entry),
     *  logs at debug and returns silently — never appends, so we can't corrupt ordering.
     *  Preserves the local {@code vwap} field because /history doesn't publish VWAP and
     *  the aggregator's ATP-derived value is already authoritative session VWAP. */
    public void updateHistoryEntry(String symbol, Candle authoritative) {
        if (symbol == null || authoritative == null) return;
        Deque<Candle> ring = historyBySymbol.get(symbol);
        if (ring == null || ring.isEmpty()) return;
        long targetStart = authoritative.startMillis();
        // Iterate from the tail — the reconciled bar is almost always the most recently
        // appended entry, so this is O(1) in practice.
        List<Candle> snapshot = new ArrayList<>(ring);
        int foundIdx = -1;
        for (int i = snapshot.size() - 1; i >= 0; i--) {
            if (snapshot.get(i).startMillis() == targetStart) { foundIdx = i; break; }
        }
        if (foundIdx < 0) {
            log.warn("[CandleAggregator] {} ring-update MISSED — no entry with startMillis={} (ring size {}, last entry startMillis={})",
                symbol, targetStart, snapshot.size(),
                snapshot.isEmpty() ? -1 : snapshot.get(snapshot.size() - 1).startMillis());
            return;
        }
        Candle existing = snapshot.get(foundIdx);
        // Preserve the aggregator's VWAP (Fyers ATP) — /history returns 0 for that field.
        Candle merged = new Candle(
            authoritative.open(), authoritative.high(), authoritative.low(), authoritative.close(),
            authoritative.volume() > 0 ? authoritative.volume() : existing.volume(),
            existing.startMillis(),
            existing.vwap());
        snapshot.set(foundIdx, merged);
        // Rebuild the ring atomically — ConcurrentLinkedDeque doesn't support in-place
        // set, and re-adding preserves iteration order for concurrent readers.
        ring.clear();
        ring.addAll(snapshot);
        dirty = true;
        log.info("[CandleAggregator] {} ring-update APPLIED at index {}/{} startMillis={} — o {}→{} h {}→{} l {}→{} c {}→{}",
            symbol, foundIdx, snapshot.size() - 1, targetStart,
            existing.open(),  merged.open(),
            existing.high(),  merged.high(),
            existing.low(),   merged.low(),
            existing.close(), merged.close());
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
