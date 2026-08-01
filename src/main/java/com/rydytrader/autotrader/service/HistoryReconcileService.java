package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Fetches the exchange-authoritative OHLC for a just-closed 3-min bar from Fyers
 * {@code /data/history}. Local {@link CandleAggregator} bars are built from throttled WS
 * snapshots (~4 Hz) and can drift a few points from the true bar (open miss, brief-wick
 * high miss). The trigger-candle FSM in {@code OptionSelling} relies on {@code trigger.low}
 * (SL floor) and the confirmation-candle close for its fire/no-fire decision — both need
 * to be authoritative for the FSM to match a TradingView chart.
 *
 * <p>Two entry points:
 * <ul>
 *   <li>{@link #fetchAuthoritative} — blocking, with retry. Use before firing an entry so
 *       the confirmation candle's close is authoritative before comparison.</li>
 *   <li>{@link #fetchAuthoritativeAsync} — non-blocking; result delivered on a background
 *       thread. Use to backfill a freshly-seeded trigger's low/high without adding entry
 *       latency (the trigger is only compared against ~2 min later).</li>
 * </ul>
 *
 * <p>Retry is needed because NSE takes ~1-2 s to publish a just-closed bar and Fyers
 * takes another ~1-2 s to make it queryable — {@link #MAX_ATTEMPTS} × {@link #RETRY_DELAY_MS}
 * covers ~4 s of end-to-end publish lag before we give up.
 */
@Service
public class HistoryReconcileService {

    private static final Logger log = LoggerFactory.getLogger(HistoryReconcileService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    /** Fyers /history resolution for our 3-min bars. */
    private static final String RESOLUTION = "3";
    /** How many polls to make before giving up. Observed: Fyers publishes just-closed
     *  3-min bars via /history within <100 ms — attempt 1 succeeds essentially every
     *  time. 2 × 500 ms retry gap gives Fyers enough time to publish on the rare miss
     *  without blowing entry latency out (worst case ~500 ms extra when attempt 1 fails). */
    private static final int  MAX_ATTEMPTS   = 2;
    private static final long RETRY_DELAY_MS = 500L;

    private final FyersClientRouter fyersClient;
    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;
    /** Dedicated pool for async backfills so the caller's thread (usually the
     *  CandleAggregator close executor) doesn't stall on I/O. */
    private final ExecutorService   asyncExecutor;

    public HistoryReconcileService(FyersClientRouter fyersClient,
                                   TokenStore tokenStore,
                                   FyersProperties fyersProperties) {
        this.fyersClient     = fyersClient;
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
        this.asyncExecutor   = Executors.newFixedThreadPool(3, r -> {
            Thread t = new Thread(r, "history-reconcile");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        asyncExecutor.shutdown();
        try { asyncExecutor.awaitTermination(1, TimeUnit.SECONDS); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    /** Blocking retry loop. Returns the authoritative candle for the bar starting at
     *  {@code barStartMs}, or {@code null} if the fetch keeps failing (exchange still
     *  publishing, Fyers /history down, network error, wrong day range). Sleeps for up
     *  to {@link #MAX_ATTEMPTS} × {@link #RETRY_DELAY_MS} ms — do NOT call on latency-
     *  sensitive threads other than the strategy's close executor.
     *
     *  <p>Timing is logged at INFO on success and WARN on failure — the operator can
     *  eyeball how long Fyers takes to publish just-closed bars and decide whether the
     *  reconcile path is adding tolerable entry latency. */
    public Candle fetchAuthoritative(String fyersSymbol, long barStartMs) {
        long startNanos = System.nanoTime();
        long msSinceBarEnd = System.currentTimeMillis() - (barStartMs + BUCKET_LENGTH_MS);
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            long attemptStartNanos = System.nanoTime();
            Candle c = fetchOnce(fyersSymbol, barStartMs);
            long attemptMs = (System.nanoTime() - attemptStartNanos) / 1_000_000L;
            if (c != null) {
                long totalMs = (System.nanoTime() - startNanos) / 1_000_000L;
                log.info("[HistoryReconcile] {} bar={} — resolved on attempt {}/{} in {} ms total (bar+{} ms at start, this HTTP {} ms)",
                    fyersSymbol, barStartMs, attempt, MAX_ATTEMPTS, totalMs, msSinceBarEnd, attemptMs);
                return c;
            }
            log.debug("[HistoryReconcile] {} bar={} — attempt {}/{} miss ({} ms HTTP)",
                fyersSymbol, barStartMs, attempt, MAX_ATTEMPTS, attemptMs);
            if (attempt == MAX_ATTEMPTS) break;
            try { Thread.sleep(RETRY_DELAY_MS); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        long totalMs = (System.nanoTime() - startNanos) / 1_000_000L;
        log.warn("[HistoryReconcile] {} bar={} — gave up after {} attempts in {} ms total",
            fyersSymbol, barStartMs, MAX_ATTEMPTS, totalMs);
        return null;
    }

    /** 3-min bars in milliseconds — the bar's end wall-clock time equals
     *  {@code barStartMs + BUCKET_LENGTH_MS}. Used to log how many ms after bar-end the
     *  reconcile call started, so the operator can distinguish "we called too early" from
     *  "Fyers is slow." */
    private static final long BUCKET_LENGTH_MS = 2 * 60 * 1000L;

    /** Fires the blocking fetch on {@link #asyncExecutor} and delivers the result to
     *  {@code onResult} (which may receive {@code null} on failure — always guarded).
     *  Caller must be prepared for {@code onResult} to be invoked on a different thread. */
    public void fetchAuthoritativeAsync(String fyersSymbol, long barStartMs, Consumer<Candle> onResult) {
        if (onResult == null) return;
        asyncExecutor.submit(() -> {
            Candle c = null;
            try { c = fetchAuthoritative(fyersSymbol, barStartMs); }
            catch (Exception e) { log.warn("[HistoryReconcile] async fetch threw: {}", e.getMessage()); }
            try { onResult.accept(c); }
            catch (Exception e) { log.warn("[HistoryReconcile] onResult callback threw: {}", e.getMessage()); }
        });
    }

    /** Blocking multi-day fetch — pulls ALL 3-min bars in the given [from, to] date
     *  range for the symbol and returns them in chronological order. Used to warm up
     *  indicators (e.g. SuperTrend needs 11+ bars) with yesterday's session before the
     *  live tick stream has accumulated enough closed bars.
     *
     *  <p>Empty list is a valid return — never null. Callers should NOT retry on empty;
     *  it usually means the symbol didn't exist on the requested date (weekly option
     *  first-day-of-trading edge case). Auth / HTTP errors also collapse to empty list
     *  after MAX_ATTEMPTS retries — the caller falls back to live-only warm-up. */
    public List<Candle> fetchDayRange(String fyersSymbol, LocalDate from, LocalDate to) {
        if (fyersSymbol == null || fyersSymbol.isBlank() || from == null || to == null) {
            return Collections.emptyList();
        }
        if (!tokenStore.isTokenAvailable()) return Collections.emptyList();

        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
        long startNanos = System.nanoTime();
        JsonNode root = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                root = fyersClient.getHistory(fyersSymbol, RESOLUTION,
                    from.toString(), to.toString(), auth);
                if (root != null && "ok".equalsIgnoreCase(root.path("s").asText(""))) break;
            } catch (Exception e) {
                log.debug("[HistoryReconcile] dayRange fetch failed for {} {}..{}: {}",
                    fyersSymbol, from, to, e.getMessage());
            }
            if (attempt == MAX_ATTEMPTS) break;
            try { Thread.sleep(RETRY_DELAY_MS); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return Collections.emptyList();
            }
        }
        if (root == null || !"ok".equalsIgnoreCase(root.path("s").asText(""))) {
            log.warn("[HistoryReconcile] dayRange {} {}..{} — no data after {} attempts ({} ms)",
                fyersSymbol, from, to, MAX_ATTEMPTS,
                (System.nanoTime() - startNanos) / 1_000_000L);
            return Collections.emptyList();
        }
        JsonNode candles = root.path("candles");
        if (candles == null || !candles.isArray()) return Collections.emptyList();

        List<Candle> out = new ArrayList<>(candles.size());
        for (JsonNode row : candles) {
            if (!row.isArray() || row.size() < 5) continue;
            long   ts    = row.get(0).asLong(0);
            double open  = row.get(1).asDouble(0);
            double high  = row.get(2).asDouble(0);
            double low   = row.get(3).asDouble(0);
            double close = row.get(4).asDouble(0);
            long   vol   = row.size() >= 6 ? row.get(5).asLong(0) : 0L;
            if (ts <= 0) continue;
            // /history doesn't publish VWAP — 0 is fine, live bars carry live ATP.
            out.add(new Candle(open, high, low, close, vol, ts * 1000L, 0.0));
        }
        long totalMs = (System.nanoTime() - startNanos) / 1_000_000L;
        log.info("[HistoryReconcile] dayRange {} {}..{} — {} bars in {} ms",
            fyersSymbol, from, to, out.size(), totalMs);
        return out;
    }

    /** Fires {@link #fetchDayRange} on {@link #asyncExecutor} and delivers the result
     *  (never null; may be empty). Caller must be prepared for the callback to run on
     *  a different thread. */
    public void fetchDayRangeAsync(String fyersSymbol, LocalDate from, LocalDate to,
                                    Consumer<List<Candle>> onResult) {
        if (onResult == null) return;
        asyncExecutor.submit(() -> {
            List<Candle> bars = Collections.emptyList();
            try { bars = fetchDayRange(fyersSymbol, from, to); }
            catch (Exception e) {
                log.warn("[HistoryReconcile] dayRange async threw: {}", e.getMessage());
            }
            try { onResult.accept(bars); }
            catch (Exception e) {
                log.warn("[HistoryReconcile] dayRange onResult callback threw: {}", e.getMessage());
            }
        });
    }

    /** Single /history request; extracts the target bar from the response array.
     *  Returns {@code null} when the bar isn't yet in Fyers's response (still publishing)
     *  or on any HTTP / parse error — caller retries. */
    private Candle fetchOnce(String fyersSymbol, long barStartMs) {
        if (fyersSymbol == null || fyersSymbol.isBlank() || barStartMs <= 0) return null;
        if (!tokenStore.isTokenAvailable()) return null;

        String today = LocalDate.now(IST).toString();
        String auth  = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();

        JsonNode root;
        try {
            root = fyersClient.getHistory(fyersSymbol, RESOLUTION, today, today, auth);
        } catch (Exception e) {
            log.debug("[HistoryReconcile] /history fetch failed for {} at {}: {}",
                fyersSymbol, barStartMs, e.getMessage());
            return null;
        }
        if (root == null) return null;
        String status = root.path("s").asText("");
        if (!"ok".equalsIgnoreCase(status)) return null;
        JsonNode candles = root.path("candles");
        if (candles == null || !candles.isArray()) return null;

        long targetEpochSec = barStartMs / 1000L;
        for (JsonNode row : candles) {
            if (!row.isArray() || row.size() < 5) continue;
            long ts = row.get(0).asLong(0);
            if (ts != targetEpochSec) continue;
            double open  = row.get(1).asDouble(0);
            double high  = row.get(2).asDouble(0);
            double low   = row.get(3).asDouble(0);
            double close = row.get(4).asDouble(0);
            long   vol   = row.size() >= 6 ? row.get(5).asLong(0) : 0L;
            // /history doesn't publish VWAP — keep it at 0. OptionSelling reads VWAP from
            // MarketDataService directly, not from the candle, so this is a no-op.
            return new Candle(open, high, low, close, vol, barStartMs, 0.0);
        }
        return null;
    }
}
