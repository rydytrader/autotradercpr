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
import java.time.ZoneId;
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
 * Stores 5-min OHLC candles per symbol. Bars are pushed here by
 * {@code GdflService} from GDFL's {@code SubscribeSnapshot MINUTE 5} stream —
 * server-aggregated from the full exchange tape and delivered ~1.5-2 s after
 * each 5-min close. Bar OHLC matches TradingView / exchange truth by construction.
 *
 * <p>Ticks continue flowing to {@code MarketDataService} for LTP display and
 * position P&amp;L, but bar OHLC is NOT built from ticks.
 *
 * <p>{@link #BUCKET_MINUTES} stays public so downstream loggers can print bar
 * duration without hardcoding it.
 */
@Service
public class CandleAggregator {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregator.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    /** Strategy trigger interval — every 5 min a synthetic aggregate bar fires
     *  to registered listeners. Kept public for downstream loggers that print
     *  "5-min bar closed" text. */
    public static final int BUCKET_MINUTES = 5;
    /** Chart bar duration — history ring stores 1-min bars from GDFL
     *  {@code SubscribeSnapshot MINUTE 1}. Chart renders these directly (livelier
     *  visualization) and every 5th 1-min bar triggers a 5-min aggregate for the
     *  strategy. */
    private static final int BAR_MINUTES = 1;

    /** Closed 1-min bar ring — bounded FIFO. Cap = 500 bars ≈ 8 h 20 min,
     *  comfortably above the ~390 1-min bars in a full 09:15-15:40 session. */
    private static final int HISTORY_CAP = 500;
    private final Map<String, Deque<Candle>> historyBySymbol = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<Consumer<Candle>>> listenersBySymbol = new ConcurrentHashMap<>();

    /** Persistence — closed bars + dayKey, restored on same-day boot. */
    private static final String STATE_FILE = "../store/cache/candle-aggregator-state.json";
    private final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .findAndRegisterModules();
    private volatile boolean dirty = false;

    /** Single-threaded listener fanout so downstream I/O (event save, order calls)
     *  doesn't stall the GDFL WS callback thread. */
    private final ExecutorService closeExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "candle-close");
        t.setDaemon(true);
        return t;
    });

    @PostConstruct
    public void boot() {
        Path p = Path.of(STATE_FILE);
        log.info("[CandleAggregator] boot — state file {} at {}",
            Files.exists(p) ? "present" : "absent", p.toAbsolutePath());
        loadFromDisk();
    }

    @PreDestroy
    public void shutdown() {
        saveToDisk();
        closeExecutor.shutdown();
        try { closeExecutor.awaitTermination(500, TimeUnit.MILLISECONDS); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    /** Register a listener for bar closes on {@code symbol}. Listener fires when
     *  a canonical 5-min bar for that symbol arrives via
     *  {@link #appendFiveMinBar}. */
    public void subscribe(String symbol, Consumer<Candle> listener) {
        if (symbol == null || symbol.isBlank() || listener == null) return;
        listenersBySymbol
            .computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>())
            .add(listener);
    }

    /** Drop all listeners for {@code symbol}. History is preserved. */
    public void unsubscribe(String symbol) {
        if (symbol == null || symbol.isBlank()) return;
        listenersBySymbol.remove(symbol);
    }

    /** Append a canonical 1-min bar from GDFL {@code SubscribeSnapshot MINUTE 1}.
     *  Chart reads these bars directly. Recomputes session VWAP for every bar in
     *  history so the yellow line stays consistent. When the appended bar
     *  completes a 5-min window (its minute is the 5th of that window), builds
     *  a 5-min OHLC+volume aggregate from the five 1-min bars in that window
     *  and fires it to strategy listeners — that's the trade trigger. */
    public void appendOneMinBar(String symbol, Candle rawBar) {
        if (symbol == null || symbol.isBlank() || rawBar == null) return;
        if (rawBar.startMillis() <= 0 || rawBar.open() <= 0) return;

        Candle stagedBar = new Candle(
            round(rawBar.open()), round(rawBar.high()),
            round(rawBar.low()),  round(rawBar.close()),
            rawBar.volume(), rawBar.startMillis(), 0.0);

        Deque<Candle> ring = historyBySymbol.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        for (Candle existing : ring) {
            if (existing.startMillis() == stagedBar.startMillis()) {
                log.debug("[CandleAggregator] {} duplicate 1-min bar startMs={} ignored",
                    symbol, stagedBar.startMillis());
                return;
            }
        }
        ring.addLast(stagedBar);
        while (ring.size() > HISTORY_CAP) ring.pollFirst();
        Candle appended = recomputeVwapsAndReturnLast(symbol);
        dirty = true;
        Candle out = appended != null ? appended : stagedBar;
        log.info("[CandleAggregator] {} 1-min bar appended — o={} h={} l={} c={} v={} vwap={} startMs={}",
            symbol, out.open(), out.high(), out.low(), out.close(),
            out.volume(), out.vwap(), out.startMillis());

        // 5-min aggregation. Windows anchor on the same UTC boundary as the bar's
        // startMillis (IST 09:15 = UTC 03:45 which is a 5-min boundary in UTC too,
        // so `startMillis % 300_000L` aligns correctly).
        long bucketStartMs   = out.startMillis() - (out.startMillis() % (BUCKET_MINUTES * 60_000L));
        int  minuteInBucket  = (int) ((out.startMillis() - bucketStartMs) / (BAR_MINUTES * 60_000L));
        if (minuteInBucket == BUCKET_MINUTES - 1) {
            Candle fiveMinAgg = buildFiveMinAggregate(symbol, bucketStartMs, out.vwap());
            if (fiveMinAgg != null) {
                log.info("[CandleAggregator] {} 5-min aggregate — o={} h={} l={} c={} v={} vwap={} startMs={}",
                    symbol, fiveMinAgg.open(), fiveMinAgg.high(), fiveMinAgg.low(),
                    fiveMinAgg.close(), fiveMinAgg.volume(), fiveMinAgg.vwap(),
                    fiveMinAgg.startMillis());
                fireListeners(symbol, fiveMinAgg);
            }
        }
    }

    /** Build a 5-min aggregate bar from every 1-min bar in
     *  {@code [bucketStartMs, bucketStartMs + 5min)}. Returns {@code null} if no
     *  1-min bars fall in that window (shouldn't happen — we only call this on
     *  the 5th minute's append). {@code vwap} is passed through from the latest
     *  1-min bar so both chart and strategy read the same session-cumulative
     *  value at this moment. */
    private Candle buildFiveMinAggregate(String symbol, long bucketStartMs, double vwap) {
        Deque<Candle> ring = historyBySymbol.get(symbol);
        if (ring == null) return null;
        long bucketEndMs = bucketStartMs + BUCKET_MINUTES * 60_000L;
        Candle first = null, last = null;
        double hi = Double.NEGATIVE_INFINITY, lo = Double.POSITIVE_INFINITY;
        long vol = 0;
        for (Candle b : ring) {
            long sm = b.startMillis();
            if (sm < bucketStartMs || sm >= bucketEndMs) continue;
            if (first == null || sm < first.startMillis()) first = b;
            if (last  == null || sm > last.startMillis())  last  = b;
            if (b.high() > hi) hi = b.high();
            if (b.low()  < lo) lo = b.low();
            vol += b.volume();
        }
        if (first == null) return null;
        return new Candle(first.open(), round(hi), round(lo), last.close(),
            vol, bucketStartMs, round(vwap));
    }

    private void fireListeners(String symbol, Candle bar) {
        CopyOnWriteArrayList<Consumer<Candle>> ls = listenersBySymbol.get(symbol);
        if (ls == null || ls.isEmpty()) return;
        closeExecutor.execute(() -> {
            for (Consumer<Candle> l : ls) {
                try { l.accept(bar); }
                catch (Exception e) {
                    log.warn("[CandleAggregator] {} listener threw: {}",
                        symbol, e.getMessage());
                }
            }
        });
    }

    /** Rebuild every bar's {@code vwap} field from stored OHLC+volume using the
     *  pandas_ta session-cumulative formula. Bars are grouped by IST trading day
     *  so the running sums reset at each day boundary. Returns the newly-appended
     *  (last) bar so callers can fan out its recomputed value. */
    private Candle recomputeVwapsAndReturnLast(String symbol) {
        Deque<Candle> ring = historyBySymbol.get(symbol);
        if (ring == null || ring.isEmpty()) return null;
        List<Candle> snapshot = new ArrayList<>(ring);
        Map<Long, double[]> dayState = new java.util.HashMap<>();
        List<Candle> rebuilt = new ArrayList<>(snapshot.size());
        for (Candle b : snapshot) {
            long istMs = b.startMillis() + 19_800_000L;
            long dayEpochMs = (istMs - (istMs % 86_400_000L)) - 19_800_000L;
            double[] st = dayState.computeIfAbsent(dayEpochMs, k -> new double[]{0.0, 0.0});
            double typical = (b.high() + b.low() + b.close()) / 3.0;
            long v = b.volume();
            if (v > 0 && typical > 0) {
                st[0] += typical * v;
                st[1] += v;
            }
            double vwap = st[1] > 0 ? st[0] / st[1] : b.close();
            rebuilt.add(new Candle(b.open(), b.high(), b.low(), b.close(),
                b.volume(), b.startMillis(), round(vwap)));
        }
        ring.clear();
        ring.addAll(rebuilt);
        return rebuilt.get(rebuilt.size() - 1);
    }

    /** Closed 1-min bars for {@code symbol} in chronological order. */
    public List<Candle> getHistory(String symbol) {
        return getHistory(symbol, 1);
    }

    /** Closed bars for {@code symbol} aggregated to {@code intervalMinutes}
     *  granularity. {@code intervalMinutes == 1} returns the raw 1-min ring;
     *  larger intervals group 1-min bars into buckets whose start aligns on
     *  {@code intervalMinutes * 60_000} (works for 1, 5, 10, 15, 30 — every IST
     *  bar boundary that aligns with a UTC bar boundary too). Each aggregate
     *  bar carries the LAST contributing 1-min bar's VWAP, so the session
     *  VWAP line matches the strategy's SL check exactly regardless of the
     *  timeframe the user is viewing. */
    public List<Candle> getHistory(String symbol, int intervalMinutes) {
        Deque<Candle> ring = historyBySymbol.get(symbol);
        if (ring == null || ring.isEmpty()) return Collections.emptyList();
        if (intervalMinutes <= 1) return new ArrayList<>(ring);
        long bucketMs = intervalMinutes * 60_000L;
        java.util.TreeMap<Long, List<Candle>> buckets = new java.util.TreeMap<>();
        for (Candle m : ring) {
            long bs = m.startMillis() - (m.startMillis() % bucketMs);
            buckets.computeIfAbsent(bs, k -> new ArrayList<>()).add(m);
        }
        List<Candle> out = new ArrayList<>(buckets.size());
        for (Map.Entry<Long, List<Candle>> e : buckets.entrySet()) {
            List<Candle> bars = e.getValue();
            bars.sort((a, b) -> Long.compare(a.startMillis(), b.startMillis()));
            Candle first = bars.get(0);
            Candle last  = bars.get(bars.size() - 1);
            double h = first.high(), l = first.low();
            long v = 0;
            for (Candle b : bars) {
                if (b.high() > h) h = b.high();
                if (b.low()  < l) l = b.low();
                v += b.volume();
            }
            out.add(new Candle(first.open(), round(h), round(l), last.close(),
                v, e.getKey(), last.vwap()));
        }
        return out;
    }

    /** No in-progress bar under the snapshot-only model — chart shows closed bars
     *  only, LTP moves on the header via the tick feed. Kept as a null-returning
     *  method for backward compatibility with callers that already null-check. */
    public Candle getCurrentBucket(String symbol) {
        return null;
    }

    /** True when at least one listener is registered for {@code symbol}. */
    public boolean isSubscribed(String symbol) {
        return symbol != null && listenersBySymbol.containsKey(symbol);
    }

    /** Prepend bars (typically fetched from a history API) to the FRONT of the
     *  ring. De-dupes by {@code startMillis} — a live bar always wins over a
     *  prepended historical bar with the same timestamp. Bars whose timestamp
     *  falls at or after the earliest live bar are skipped so we never overwrite
     *  live data. */
    public void prependHistory(String symbol, List<Candle> bars) {
        if (symbol == null || symbol.isBlank() || bars == null || bars.isEmpty()) return;
        Deque<Candle> ring = historyBySymbol.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        synchronized (ring) {
            java.util.Set<Long> present = new java.util.HashSet<>(ring.size());
            for (Candle c : ring) present.add(c.startMillis());
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
            for (int i = toAdd.size() - 1; i >= 0; i--) {
                ring.addFirst(toAdd.get(i));
            }
            while (ring.size() > HISTORY_CAP) ring.pollFirst();
        }
        log.info("[CandleAggregator] {} prepended {} historical bars (ring={})",
            symbol, bars.size(), ring.size());
    }

    // ── Persistence ────────────────────────────────────────────────────────────

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void periodicSave() {
        if (!dirty) return;
        saveToDisk();
    }

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s == null) return;
            String today = LocalDate.now(IST).toString();
            if (!today.equals(s.dayKey)) {
                log.info("[CandleAggregator] discarding stale state.json — dayKey={} today={}",
                    s.dayKey, today);
                return;
            }
            if (s.barMinutes != 0 && s.barMinutes != BAR_MINUTES) {
                log.info("[CandleAggregator] state.json bar granularity mismatch — barMinutes={} does not match BAR_MINUTES={}. Loading anyway so the chart has data from cache; new 1-min bars will accumulate alongside.",
                    s.barMinutes, BAR_MINUTES);
                // Fall through — don't discard. Bars written at other granularities
                // still render fine on the chart (lightweight-charts doesn't care
                // about spacing); they just mix visually with new 1-min bars until
                // the day rolls over.
            }
            long todayStartUtcMs = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
            long tomorrowStartUtcMs = todayStartUtcMs + 86_400_000L;
            int candles = 0, droppedStale = 0, droppedDupe = 0;
            if (s.historyBySymbol != null) {
                for (Map.Entry<String, List<Candle>> e : s.historyBySymbol.entrySet()) {
                    if (e.getKey() == null || e.getValue() == null || e.getValue().isEmpty()) continue;
                    java.util.Set<Long> seen = new java.util.HashSet<>();
                    List<Candle> kept = new ArrayList<>(e.getValue().size());
                    for (Candle c : e.getValue()) {
                        long sm = c.startMillis();
                        if (sm < todayStartUtcMs || sm >= tomorrowStartUtcMs) { droppedStale++; continue; }
                        if (!seen.add(sm)) { droppedDupe++; continue; }
                        kept.add(c);
                    }
                    if (!kept.isEmpty()) {
                        historyBySymbol.put(e.getKey(), new ConcurrentLinkedDeque<>(kept));
                        candles += kept.size();
                        // Force-recompute all restored bars' VWAP from stored OHLC
                        // so any bars written by earlier aggregator versions get
                        // consistent pandas_ta values on the yellow chart line.
                        recomputeVwapsAndReturnLast(e.getKey());
                    }
                }
            }
            if (droppedStale > 0 || droppedDupe > 0) {
                log.info("[CandleAggregator] load cleanup — dropped {} stale + {} duplicate bar(s)",
                    droppedStale, droppedDupe);
            }
            log.info("[CandleAggregator] restored {} candles across {} symbols for {}",
                candles, historyBySymbol.size(), today);
        } catch (IOException e) {
            log.warn("[CandleAggregator] failed to load state: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        try {
            State s = new State();
            s.dayKey = LocalDate.now(IST).toString();
            s.barMinutes = BAR_MINUTES;
            for (Map.Entry<String, Deque<Candle>> e : historyBySymbol.entrySet()) {
                s.historyBySymbol.put(e.getKey(), new ArrayList<>(e.getValue()));
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

    /** Persisted snapshot. Extra fields in older files (in-progress buckets,
     *  session VWAP seeds, {@code bucketMinutes}, etc.) are silently ignored via
     *  {@code FAIL_ON_UNKNOWN_PROPERTIES=false}. Files written before the 1-min
     *  history migration have {@code bucketMinutes} but no {@code barMinutes},
     *  so {@code barMinutes} defaults to 0 and the load path discards the file —
     *  no risk of mixing 5-min and 1-min bars in the same ring. */
    public static class State {
        public String dayKey = "";
        public int barMinutes = 0;
        public Map<String, List<Candle>> historyBySymbol = new LinkedHashMap<>();
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
