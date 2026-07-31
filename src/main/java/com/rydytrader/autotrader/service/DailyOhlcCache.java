package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches the PREVIOUS trading day's daily OHLC bar for symbols that need it.
 * Sole consumer today: {@link com.rydytrader.autotrader.indicator.FloorPivots} —
 * the OPTION BUYING strategy's structure filter needs yesterday's NIFTY H/L/C
 * to compute today's R1 / S1 pivot levels.
 *
 * <p>Fetch path: {@link FyersClientRouter#getHistory} with resolution {@code "D"}.
 * Skips weekends by walking back to Friday for Monday's baseline. Cache is
 * per-symbol, keyed on the fetch date, so a same-day repeat call is a map
 * lookup and doesn't burn a Fyers API call.
 *
 * <p>Fails soft — returns {@link Optional#empty()} when Fyers is unavailable or
 * the response doesn't contain the requested day. Caller is expected to
 * gate on emptiness and skip the pivots-dependent branch (rather than crash).
 */
@Service
public class DailyOhlcCache {

    private static final Logger log = LoggerFactory.getLogger(DailyOhlcCache.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final FyersClientRouter fyersClient;
    private final FyersProperties fyersProperties;
    private final TokenStore tokenStore;

    /** (symbol, dateKey) → cached DailyBar. dateKey is today's IST date so a
     *  new session forces a re-fetch of the previous-day bar. */
    private final ConcurrentHashMap<String, CachedEntry> cache = new ConcurrentHashMap<>();

    public DailyOhlcCache(FyersClientRouter fyersClient,
                          FyersProperties fyersProperties,
                          TokenStore tokenStore) {
        this.fyersClient     = fyersClient;
        this.fyersProperties = fyersProperties;
        this.tokenStore      = tokenStore;
    }

    /** Previous trading day's OHLC for {@code symbol}, from Fyers {@code /history}
     *  with resolution {@code "D"}. Cached for the current IST date; second call
     *  the same day returns instantly from the map. */
    public Optional<DailyBar> previousDay(String symbol) {
        if (symbol == null || symbol.isBlank()) return Optional.empty();
        String today = LocalDate.now(IST).toString();
        CachedEntry hit = cache.get(symbol);
        if (hit != null && today.equals(hit.forDateKey)) return Optional.of(hit.bar);

        DailyBar bar = fetch(symbol);
        if (bar == null) return Optional.empty();
        cache.put(symbol, new CachedEntry(today, bar));
        return Optional.of(bar);
    }

    /** Fetch the last available daily bar (yesterday, or Friday if today is Monday
     *  / holiday-adjacent). Returns null on any error or empty response. */
    private DailyBar fetch(String symbol) {
        if (!tokenStore.isTokenAvailable()) {
            log.warn("[DailyOhlcCache] Fyers token unavailable — skipping {} daily fetch", symbol);
            return null;
        }
        // Ask for a 7-day window ending yesterday so weekends / holidays don't
        // return an empty response. Fyers returns all trading days in the range;
        // we take the most recent bar.
        LocalDate today     = LocalDate.now(IST);
        LocalDate yesterday = previousTradingDay(today);
        LocalDate weekBack  = yesterday.minusDays(7);
        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
        JsonNode root;
        try {
            root = fyersClient.getHistory(symbol, "D",
                weekBack.toString(), yesterday.toString(), auth);
        } catch (Exception e) {
            log.warn("[DailyOhlcCache] /history fetch failed for {}: {}", symbol, e.getMessage());
            return null;
        }
        if (root == null) return null;
        String status = root.path("s").asText("");
        if (!"ok".equalsIgnoreCase(status)) return null;
        JsonNode candles = root.path("candles");
        if (candles == null || !candles.isArray() || candles.isEmpty()) return null;

        // Pick the LAST entry — that's the most recent trading day in the window.
        JsonNode last = candles.get(candles.size() - 1);
        if (!last.isArray() || last.size() < 5) return null;
        long   ts    = last.get(0).asLong(0);
        double open  = last.get(1).asDouble(0);
        double high  = last.get(2).asDouble(0);
        double low   = last.get(3).asDouble(0);
        double close = last.get(4).asDouble(0);
        LocalDate date = LocalDate.ofInstant(java.time.Instant.ofEpochSecond(ts), IST);
        DailyBar bar = new DailyBar(date, open, high, low, close);
        log.info("[DailyOhlcCache] {} previous day {} — O={} H={} L={} C={}",
            symbol, date, open, high, low, close);
        return bar;
    }

    /** Naive previous-trading-day: skip Sat/Sun. NSE holidays aren't checked
     *  here — the 7-day fetch window will still pull the last real trading day
     *  from Fyers regardless. */
    private static LocalDate previousTradingDay(LocalDate today) {
        LocalDate d = today.minusDays(1);
        while (d.getDayOfWeek() == DayOfWeek.SATURDAY || d.getDayOfWeek() == DayOfWeek.SUNDAY) {
            d = d.minusDays(1);
        }
        return d;
    }

    /** One daily OHLC bar. Kept minimal — FloorPivots only needs H / L / C. */
    public record DailyBar(LocalDate date, double open, double high, double low, double close) {}

    private record CachedEntry(String forDateKey, DailyBar bar) {}
}
