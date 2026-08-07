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
 * Samples LTPs for every subscribed Fyers symbol once per second and rolls samples into 3-minute
 * OHLC buckets per symbol. On bucket close (the first sample in a new 3-min window), the closed
 * candle is emitted to every listener registered for that symbol.
 *
 * <p>Buckets are anchored on the IST wall clock — 09:15, 09:18, 09:21, … 15:27, 15:30 — and only
 * emitted during market hours (09:15 ≤ now ≤ 15:31).
 *
 * <p>{@link #BUCKET_MINUTES} is public so downstream consumers can derive bar length without
 * hardcoding it and risk drifting from the aggregator's actual cadence.
 */
@Service
public class CandleAggregator {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregator.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    public  static final int    BUCKET_MINUTES = 1;
    /** NSE market open in minutes-of-day (IST). Bucket boundaries are computed relative
     *  to this so the first bar spans 09:15→09:15+BUCKET_MINUTES (at 3-min:
     *  09:15→09:18). */
    private static final int    MARKET_OPEN_MINUTE_OF_DAY = 9 * 60 + 15;

    private final MarketDataService marketDataService;

    private final Map<String, Bucket> bucketBySymbol = new ConcurrentHashMap<>();
    /** Second bucket per symbol — the just-closed bar kept OPEN for a short
     *  "grace window" so late-arriving LTT ticks (LTT stamped just before the
     *  bar boundary but received by the bot after it) merge into their proper
     *  bar instead of being dropped as stale. Absent when no grace bucket is
     *  active. See {@link #GRACE_MS}. */
    private final Map<String, PendingBucket> pendingBySymbol = new ConcurrentHashMap<>();
    /** How long a pending bucket stays open after its bar boundary to absorb
     *  late-arriving LTT ticks. Set to 0 — bar closes fire the instant a
     *  boundary tick arrives, no wait. The strategy entry gate cares only
     *  about the close price relative to VWAP (and the bar high for SL calc)
     *  — a late LTT that would have shifted OHLC by a fraction of a point
     *  is not worth 3 s of entry latency. If a subsequent bar boundary is
     *  crossed while a pending is still open (only possible when GRACE_MS
     *  > 0), the pending is emitted immediately. */
    private static final long GRACE_MS = 0;
    private final Map<String, CopyOnWriteArrayList<Consumer<Candle>>> listenersBySymbol = new ConcurrentHashMap<>();
    /** Closed 3-min candles per symbol, kept in a bounded FIFO ring. Populated by
     *  {@link #appendHistoryAndFire} so the chart page can render the day's session without a
     *  Fyers-history REST fetch. Cap = 250 = ~8.3 h of 3-min bars, well over the
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
     *  {@code OptionScalping.onCandleClose} → {@code saveToDisk} inline, WS tick throughput
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

    /** Subscribe to 3-min candle closes on {@code symbol}. The symbol is also added to the
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
        pendingBySymbol.remove(symbol);
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
            // Pre-market — DO NOT flush. A bucket may already exist for the 09:15→09:18
            // slot because Fyers rounds exch_feed_time up to the next second (a print at
            // 09:14:59.9 lands with exch_feed_time = 09:15:00, opening a legit 09:15
            // bucket even though wall-clock is still 09:14:xx). Flushing here would emit
            // that bucket as a "close" event at 09:14:xx, which OptionScalping.onCandleClose
            // interprets as the first-bar close and resolves ATM three minutes early.
            // Wait for wall-clock to catch up — in-market-hours flush path handles it.
            return;
        }
        if (t.isAfter(LocalTime.of(15, 31))) {
            // Post-market — flush the day's last-bar stragglers. Safe to unconditionally
            // close both active AND pending: no legit forward-dated bucket can exist
            // after market close, and grace-window buckets should not linger overnight.
            for (Map.Entry<String, Bucket> e : bucketBySymbol.entrySet()) {
                String symbol = e.getKey();
                Bucket b = e.getValue();
                Candle activeCloseSnap = null;
                Candle pendingCloseSnap = null;
                synchronized (b) {
                    PendingBucket pb = pendingBySymbol.remove(symbol);
                    if (pb != null && pb.bucket.currentBucketMinute >= 0) {
                        pendingCloseSnap = snapshot(pb.bucket);
                    }
                    if (b.currentBucketMinute >= 0) {
                        activeCloseSnap = snapshot(b);
                        b.reset();
                    }
                }
                if (pendingCloseSnap != null) appendHistoryAndFire(symbol, pendingCloseSnap, false);
                if (activeCloseSnap  != null) appendHistoryAndFire(symbol, activeCloseSnap,  false);
                if (pendingCloseSnap != null || activeCloseSnap != null) dirty = true;
            }
            return;
        }

        // In-hours: two jobs.
        // (1) Drain grace-expired pending buckets → emit their close now.
        // (2) Promote an active bucket whose wall-clock end has passed and whose
        //     grace window hasn't started yet (illiquid symbol: no boundary tick
        //     arrived to trigger the promote in onLtpTick).
        // NEVER emit an active bucket directly here — the close path now always
        // goes through the grace window so late LTTs can merge.
        int wallBucketStart = bucketStartMinute(t.getHour() * 60 + t.getMinute());
        long nowMs = System.currentTimeMillis();
        for (Map.Entry<String, Bucket> e : bucketBySymbol.entrySet()) {
            String symbol = e.getKey();
            Bucket b = e.getValue();
            Candle graceExpired = null;
            synchronized (b) {
                PendingBucket pb = pendingBySymbol.get(symbol);
                if (pb != null && nowMs >= pb.graceExpireMs) {
                    graceExpired = snapshot(pb.bucket);
                    pendingBySymbol.remove(symbol);
                }
                // Illiquid safety net — bar's wall-clock end has passed but no
                // boundary tick has arrived. Promote active → pending so its
                // close is eventually emitted after the grace expires.
                if (!pendingBySymbol.containsKey(symbol)
                    && b.currentBucketMinute >= 0
                    && wallBucketStart > b.currentBucketMinute) {
                    pendingBySymbol.put(symbol, new PendingBucket(cloneBucket(b), nowMs + GRACE_MS));
                    b.reset();
                }
            }
            if (graceExpired != null) {
                appendHistoryAndFire(symbol, graceExpired, false);
                dirty = true;
            }
        }
    }

    /** Snapshot a bucket's OHLC into an immutable {@link Candle} — used at close
     *  emission time so the fanout runs on a value copy, immune to any subsequent
     *  mutation of the underlying {@link Bucket}. */
    private static Candle snapshot(Bucket b) {
        return new Candle(
            round(b.openPx), round(b.highPx), round(b.lowPx), round(b.closePx),
            0L, b.currentBucketStartMs, round(b.vwapLast));
    }

    /** Value-copy of a {@link Bucket} — used when moving the active bucket into
     *  the pending slot at boundary crossing so the two slots don't share state. */
    private static Bucket cloneBucket(Bucket src) {
        Bucket dst = new Bucket();
        dst.currentBucketMinute  = src.currentBucketMinute;
        dst.currentBucketStartMs = src.currentBucketStartMs;
        dst.openPx  = src.openPx;
        dst.highPx  = src.highPx;
        dst.lowPx   = src.lowPx;
        dst.closePx = src.closePx;
        dst.vwapLast = src.vwapLast;
        dst.tickCount = src.tickCount;
        return dst;
    }

    /** Anchors 3-min bucket boundaries on 09:15 IST — the true market-open minute — not on
     *  midnight. Otherwise 09:15 (odd) rounds down to the 09:14 even-minute bucket and
     *  the "first 3-min bar" closes at 09:16 after only ~1 minute of data. Anchoring on
     *  09:15 gives 09:15→09:18, 09:18→09:21, … as the docs promise. Pre-open ticks
     *  (before 09:15) fall back to midnight-anchored buckets — harmless because sample()
     *  gates outside market hours anyway. */
    private static int bucketStartMinute(int minuteOfDay) {
        int minutesSinceOpen = minuteOfDay - MARKET_OPEN_MINUTE_OF_DAY;
        return minutesSinceOpen >= 0
            ? MARKET_OPEN_MINUTE_OF_DAY + (minutesSinceOpen / BUCKET_MINUTES) * BUCKET_MINUTES
            : (minuteOfDay / BUCKET_MINUTES) * BUCKET_MINUTES;
    }

    /** Formats an epoch-seconds value as IST HH:mm:ss for log lines. Returns
     *  "—" for a zero / missing timestamp so stale-tick logs don't render as
     *  "1970-01-01" garbage when a packet omits LTT or EFT. */
    private static String formatSec(long epochSec) {
        if (epochSec <= 0) return "—";
        return Instant.ofEpochSecond(epochSec).atZone(IST).toLocalTime().toString();
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
            // Bar-size schema check — reject state written under a different
            // BUCKET_MINUTES setting. Boundaries computed at 3-min and 3-min
            // don't align, so mixing them in the same ring corrupts the chart
            // and confuses the FSM. Field is defaulted to 0 in State, so any
            // pre-schema file (missing the field) is also treated as a mismatch
            // and discarded. Loss of one boot's chart history is acceptable.
            if (s.bucketMinutes != BUCKET_MINUTES) {
                log.info("[CandleAggregator] discarding state.json — bucketMinutes={} does not match BUCKET_MINUTES={}",
                    s.bucketMinutes, BUCKET_MINUTES);
                return;
            }
            // Filter + dedupe on load so a file corrupted by a prior stale-tick
            // reopen bug (or a prior-session bar that leaked past the dayKey guard)
            // self-cleans on restart. Rules:
            //   1. Drop bars whose startMillis maps to a different IST date than today.
            //   2. Keep the FIRST occurrence per startMillis; drop later duplicates
            //      (the spurious 1-tick reopen artifacts).
            long todayStartUtcMs = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
            long tomorrowStartUtcMs = todayStartUtcMs + 24L * 60 * 60 * 1000;
            int candles = 0, droppedStale = 0, droppedDupe = 0;
            if (s.historyBySymbol != null) {
                for (Map.Entry<String, List<Candle>> e : s.historyBySymbol.entrySet()) {
                    if (e.getKey() == null || e.getValue() == null || e.getValue().isEmpty()) continue;
                    java.util.Set<Long> seenStartMs = new java.util.HashSet<>();
                    List<Candle> kept = new ArrayList<>(e.getValue().size());
                    for (Candle c : e.getValue()) {
                        long sm = c.startMillis();
                        if (sm < todayStartUtcMs || sm >= tomorrowStartUtcMs) { droppedStale++; continue; }
                        if (!seenStartMs.add(sm)) { droppedDupe++; continue; }
                        kept.add(c);
                    }
                    if (!kept.isEmpty()) {
                        historyBySymbol.put(e.getKey(), new ConcurrentLinkedDeque<>(kept));
                        candles += kept.size();
                    }
                }
            }
            if (droppedStale > 0 || droppedDupe > 0) {
                log.info("[CandleAggregator] load cleanup — dropped {} stale (non-today) + {} duplicate (same startMs) bar(s)",
                    droppedStale, droppedDupe);
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
            s.bucketMinutes = BUCKET_MINUTES;
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
        /** BUCKET_MINUTES value under which this state was written. Discarded on
         *  load if it doesn't match the current constant — prevents mixing
         *  3-min and 3-min bars in the same history ring across a migration.
         *  0 for legacy files that predate this field. */
        public int bucketMinutes = 0;
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
        // "boundary skew" gap where a tick TRADED at 09:17:59 arrives locally at
        // 09:18:00.1 and was previously attributed to the wrong bar.
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

        // Stale-bucket guard — a tick whose bucketStart is BEHIND the current wall
        // bucket (or behind the bucket's own currentBucketMinute) points at a bar that
        // has already been finalised. Root cause: sample() flushes bucket X at wall
        // 10:57:00.089 and resets, then a late tick with LTT/EFT still pointing into
        // bucket X arrives at 10:57:00.772 (Fyers keep-alive with slightly-stale
        // timestamp). Without this guard, the first-branch code below would open a
        // fresh "bucket X" for the SAME startMs, appending a duplicate 1-tick bar to
        // history — chart then renders the collapsed bar.
        //
        // Drop stale ticks entirely rather than merging into the closed ring entry.
        // Merging would silently mutate the H/L of a bar the FSM may have already
        // used as its trigger candle (state.triggerByOption stores a value copy of
        // the trigger's high/low; a merge that changes them leaves the FSM checking
        // the old thresholds while the chart shows the new ones — invariant broken).
        // The small OHLC precision lost on a genuinely late tick is worth keeping
        // the trigger-candle contract simple.
        LocalTime wallNow = ZonedDateTime.now(IST).toLocalTime();
        int wallMin      = wallNow.getHour() * 60 + wallNow.getMinute();
        int wallBucket   = bucketStartMinute(wallMin);
        long nowMs       = System.currentTimeMillis();

        Bucket b = bucketBySymbol.computeIfAbsent(symbol, k -> new Bucket());
        Candle preemptedPending = null;
        synchronized (b) {
            PendingBucket pb = pendingBySymbol.get(symbol);

            // Grace-window absorb — this tick's LTT falls in the JUST-CLOSED bar
            // that's still in its grace window. Merge into pending's OHLC instead
            // of dropping. The pending bar's close is emitted later by sample()
            // when grace expires (or by a subsequent boundary crossing).
            if (pb != null && pb.bucket.currentBucketMinute == bucketStart && nowMs < pb.graceExpireMs) {
                if (ltp > pb.bucket.highPx) pb.bucket.highPx = ltp;
                if (ltp < pb.bucket.lowPx)  pb.bucket.lowPx  = ltp;
                pb.bucket.closePx = ltp;
                if (t.atp() > 0) pb.bucket.vwapLast = t.atp();
                pb.bucket.tickCount++;
                log.debug("[CandleAggregator] {} GRACE ABSORB — bucketStart={} ltp={} lttTime={} eftTime={} localTime={} graceLeftMs={}",
                    symbol, bucketStart, ltp,
                    formatSec(t.lastTradedTimeSec()),
                    formatSec(t.exchFeedTimeSec()),
                    wallNow,
                    pb.graceExpireMs - nowMs);
                dirty = true;
                return;
            }

            // Stale — bucket is behind wall clock and NOT the currently-pending
            // bar and NOT the currently-active bar. Drop rather than reopen a
            // finalised bar (would break the trigger-candle invariant).
            //
            // The bucketStart == active.currentBucketMinute exemption catches the
            // race where the wall clock ticks over to bucket N+1 before sample()
            // has run to promote active from N to pending — a tick still in N
            // arrives in that window and would otherwise be wrongly dropped. Let
            // it append to active; sample() will promote to pending shortly.
            if (bucketStart < wallBucket && bucketStart != b.currentBucketMinute) {
                log.info("[CandleAggregator] {} STALE TICK dropped — bucketStart={} wallBucket={} ltp={} lttTime={} eftTime={} localTime={} pending={} active={}",
                    symbol, bucketStart, wallBucket, ltp,
                    formatSec(t.lastTradedTimeSec()),
                    formatSec(t.exchFeedTimeSec()),
                    wallNow,
                    pb == null ? "—" : (pb.bucket.currentBucketMinute + "(exp-in=" + (pb.graceExpireMs - nowMs) + "ms)"),
                    b.currentBucketMinute);
                return;
            }

            if (b.currentBucketMinute < 0) {
                b.start(bucketStart, ltp, ZonedDateTime.now(IST));
                if (t.atp() > 0) b.vwapLast = t.atp();
            } else if (bucketStart < b.currentBucketMinute) {
                // Backward roll — bucket older than active but not older than wall
                // (rare — happens only when active is briefly ahead of wall clock).
                log.info("[CandleAggregator] {} STALE TICK dropped (backward-roll) — bucketStart={} currentBucketMinute={} ltp={} lttTime={} eftTime={} localTime={}",
                    symbol, bucketStart, b.currentBucketMinute, ltp,
                    formatSec(t.lastTradedTimeSec()),
                    formatSec(t.exchFeedTimeSec()),
                    wallNow);
                return;
            } else if (bucketStart != b.currentBucketMinute) {
                // Boundary crossing. Two paths:
                //   GRACE_MS > 0 → move active INTO the pending slot; late LTTs
                //     can still merge until grace expires. Preempt any older
                //     pending still there (we only hold one).
                //   GRACE_MS == 0 → snapshot + emit immediately. No pending, no
                //     wait. Late ticks with LTT in the just-closed bar are
                //     dropped as stale — acceptable trade-off for entry latency.
                if (GRACE_MS > 0) {
                    if (pb != null) preemptedPending = snapshot(pb.bucket);
                    pendingBySymbol.put(symbol, new PendingBucket(cloneBucket(b), nowMs + GRACE_MS));
                } else {
                    preemptedPending = snapshot(b);
                }
                b.reset();
                b.start(bucketStart, ltp, ZonedDateTime.now(IST));
                if (t.atp() > 0) b.vwapLast = t.atp();
            } else {
                if (ltp > b.highPx) b.highPx = ltp;
                if (ltp < b.lowPx)  b.lowPx  = ltp;
                b.closePx = ltp;
                if (t.atp() > 0) b.vwapLast = t.atp();
                b.tickCount++;
            }
        }
        dirty = true;
        if (preemptedPending != null) {
            log.debug("[CandleAggregator] {} pending grace preempted by new boundary — emitting startMs={}",
                symbol, preemptedPending.startMillis());
            appendHistoryAndFire(symbol, preemptedPending, true);
        }
    }

    /** Overwrites an existing history-ring entry with an authoritative version — used
     *  after {@code OptionScalping.reconcileBar} pulls the exchange-published OHLC via Fyers
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

    /** Closed 3-min candles for {@code symbol} in chronological order. Empty when the
     *  symbol hasn't rolled a bucket yet (freshly subscribed, or pre-market boot). */
    public List<Candle> getHistory(String symbol) {
        Deque<Candle> ring = historyBySymbol.get(symbol);
        if (ring == null || ring.isEmpty()) return Collections.emptyList();
        return new ArrayList<>(ring);
    }

    /** Merge historical bars (typically fetched from Fyers /history for yesterday's
     *  session) into the FRONT of the per-symbol ring. Used to warm up indicators
     *  (SuperTrend needs 11+ closed bars) at strategy boot / ATM lock time before
     *  the live tick stream has accumulated enough history.
     *
     *  <p>De-dupes by {@code startMillis} — an existing (live) bar always wins over
     *  a prepended historical bar for the same timestamp. Ring is capped at
     *  {@link #HISTORY_CAP}; older historical bars are dropped when the ring fills.
     *
     *  <p>No listener fanout, no persistence dirty-flag — this is a silent warm-up
     *  path, not a live close. Safe to call from any thread; the underlying deque
     *  is a {@link ConcurrentLinkedDeque} and the merge is guarded by intrinsic
     *  synchronisation on the ring instance. */
    public void prependHistory(String symbol, List<Candle> bars) {
        if (symbol == null || symbol.isBlank() || bars == null || bars.isEmpty()) return;
        Deque<Candle> ring = historyBySymbol.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        synchronized (ring) {
            // Collect existing startMillis so we can skip duplicates cheaply.
            java.util.Set<Long> present = new java.util.HashSet<>(ring.size());
            for (Candle c : ring) present.add(c.startMillis());
            // Sort input bars ascending and take only those that aren't already in the ring
            // AND are older than the earliest live bar (we never overwrite live data).
            long earliestLive = ring.isEmpty() ? Long.MAX_VALUE : ring.peekFirst().startMillis();
            List<Candle> toAdd = new ArrayList<>();
            for (Candle c : bars) {
                if (c == null || c.startMillis() <= 0) continue;
                if (present.contains(c.startMillis())) continue;
                if (c.startMillis() >= earliestLive) continue;
                toAdd.add(c);
            }
            if (toAdd.isEmpty()) return;
            toAdd.sort((a, b) -> Long.compare(a.startMillis(), b.startMillis()));
            // Prepend in reverse so the earliest bar ends up at index 0.
            for (int i = toAdd.size() - 1; i >= 0; i--) {
                ring.addFirst(toAdd.get(i));
            }
            while (ring.size() > HISTORY_CAP) ring.pollFirst();
        }
        log.info("[CandleAggregator] {} — prepended {} historical bars (ring={})",
            symbol, bars.size(), ring.size());
    }

    /** In-progress 3-min bucket for {@code symbol}, synthesised into a {@code Candle}
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
        /** Diagnostic — number of ticks that landed in this bucket. Logged at close so
         *  operator can spot collapsed O=H=L=C bars caused by low tick rate (GDFL
         *  throttle or genuinely quiet market window). */
        int tickCount = 0;

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
            tickCount = 1;   // start() is called ON the first tick of the bucket
            // vwapLast intentionally NOT reset — VWAP is a running session metric.
        }

        void reset() {
            currentBucketMinute = -1;
            currentBucketStartMs = 0;
            openPx = highPx = lowPx = closePx = 0;
            vwapLast = 0;
            tickCount = 0;
        }
    }

    /** A closed bar in its grace window — still accepting late LTT ticks whose
     *  LTT falls in the bar's range. {@link #graceExpireMs} is the wall-clock
     *  epoch after which {@link #sample} emits the close and clears the slot. */
    private static class PendingBucket {
        final Bucket bucket;
        final long graceExpireMs;
        PendingBucket(Bucket bucket, long graceExpireMs) {
            this.bucket = bucket;
            this.graceExpireMs = graceExpireMs;
        }
    }
}
