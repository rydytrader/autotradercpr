package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serves NIFTY 50 spot 5-min candles for a given date, used by the calendar
 * day-modal to show the day's price action alongside the session P&L. Fetches
 * from Fyers /data/history on demand and caches per-date in-memory since
 * historical bars never change once the session is closed.
 */
@RestController
public class NiftyChartController {

    private static final Logger log = LoggerFactory.getLogger(NiftyChartController.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";

    private final FyersClientRouter fyersClient;
    private final FyersProperties   fyersProperties;
    private final TokenStore        tokenStore;

    /** {@code date + "|" + resolution → payload}. Historical bars are immutable
     *  once the session closes, so the cache never needs invalidation for past
     *  dates. Today's key is refetched every call — see below. */
    private final Map<String, List<Map<String, Object>>> cache = new ConcurrentHashMap<>();

    public NiftyChartController(FyersClientRouter fyersClient,
                                 FyersProperties fyersProperties,
                                 TokenStore tokenStore) {
        this.fyersClient = fyersClient;
        this.fyersProperties = fyersProperties;
        this.tokenStore = tokenStore;
    }

    /** Returns {@code [{time, open, high, low, close, volume}, ...]} for the
     *  requested date. Default resolution is 5-min. {@code time} is epoch
     *  seconds, matching what TradingView's Lightweight Charts expects. */
    @GetMapping("/api/nifty-chart")
    public ResponseEntity<?> getChart(@RequestParam("date") String date,
                                       @RequestParam(value = "resolution", defaultValue = "5") String resolution) {
        // Basic date validation — must be a real yyyy-MM-dd, not something absurd.
        LocalDate d;
        try { d = LocalDate.parse(date); }
        catch (Exception e) { return ResponseEntity.badRequest().body(Map.of("error", "bad date")); }

        String key = date + "|" + resolution;
        boolean isToday = d.isEqual(LocalDate.now());
        // Today's bars keep landing — bypass cache for that key.
        if (!isToday) {
            List<Map<String, Object>> cached = cache.get(key);
            if (cached != null) return ResponseEntity.ok(Map.of("date", date, "resolution", resolution, "candles", cached));
        }

        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode resp = fyersClient.getHistory(NIFTY_SYMBOL, resolution, date, date, auth);
            if (resp == null) {
                return ResponseEntity.ok(Map.of("date", date, "resolution", resolution, "candles", List.of()));
            }
            if ("error".equals(resp.path("s").asText(""))) {
                log.warn("[NiftyChart] Fyers error for {}: code={} msg={}",
                    date, resp.path("code").asInt(0), resp.path("message").asText(""));
                return ResponseEntity.ok(Map.of("date", date, "resolution", resolution, "candles", List.of(),
                    "error", resp.path("message").asText("")));
            }
            JsonNode candles = resp.path("candles");
            List<Map<String, Object>> out = new ArrayList<>();
            if (candles != null && candles.isArray()) {
                for (JsonNode row : candles) {
                    if (!row.isArray() || row.size() < 6) continue;
                    // Fyers row order: [epochSec, open, high, low, close, volume]
                    Map<String, Object> bar = new LinkedHashMap<>();
                    bar.put("time",   row.get(0).asLong(0));
                    bar.put("open",   row.get(1).asDouble(0));
                    bar.put("high",   row.get(2).asDouble(0));
                    bar.put("low",    row.get(3).asDouble(0));
                    bar.put("close",  row.get(4).asDouble(0));
                    bar.put("volume", row.get(5).asLong(0));
                    out.add(bar);
                }
            }
            if (!isToday) cache.put(key, out);
            return ResponseEntity.ok(Map.of("date", date, "resolution", resolution, "candles", out));
        } catch (Exception e) {
            log.warn("[NiftyChart] fetch failed for {}: {}", date, e.getMessage());
            return ResponseEntity.ok(Map.of("date", date, "resolution", resolution, "candles", List.of(),
                "error", e.getMessage()));
        }
    }
}
