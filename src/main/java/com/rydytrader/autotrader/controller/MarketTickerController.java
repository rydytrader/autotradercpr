package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.BhavcopyService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.store.TokenStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map;
import java.util.Set;

@RestController
public class MarketTickerController {

    private final FyersClientRouter fyersClient;
    private final FyersProperties fyersProperties;
    private final TokenStore tokenStore;
    private final MarketHolidayService marketHolidayService;
    private final BhavcopyService bhavcopyService;

    // Only indices stay hardcoded — used by MarketDataService to ensure the WebSocket
    // always subscribes to them for the NIFTY trend calc. Stocks on the ticker come
    // dynamically from the NIFTY 50 list via bhavcopy.
    private static final String BASE_SYMBOLS = "NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX";

    /** Returns the base index symbols. Used by MarketDataService for WebSocket subscription. */
    public static String[] getBaseSymbols() {
        return BASE_SYMBOLS.split(",");
    }

    private static final long CACHE_TTL_MS = 60_000; // 60 seconds
    private volatile List<Map<String, Object>> cachedTickers = List.of();
    private volatile long lastFetchTime = 0;
    private volatile String lastSymbolsKey = "";

    public MarketTickerController(FyersClientRouter fyersClient,
                                   FyersProperties fyersProperties,
                                   TokenStore tokenStore,
                                   MarketHolidayService marketHolidayService,
                                   BhavcopyService bhavcopyService) {
        this.fyersClient = fyersClient;
        this.fyersProperties = fyersProperties;
        this.tokenStore = tokenStore;
        this.marketHolidayService = marketHolidayService;
        this.bhavcopyService = bhavcopyService;
    }

    @GetMapping("/api/market-ticker")
    public ResponseEntity<?> getMarketTicker() {
        String symbols = buildSymbolList();
        long now = System.currentTimeMillis();

        // Invalidate cache if position symbols changed or TTL expired
        boolean cacheValid = (now - lastFetchTime < CACHE_TTL_MS)
                && !cachedTickers.isEmpty()
                && symbols.equals(lastSymbolsKey);

        if (cacheValid) {
            return ResponseEntity.ok(cachedTickers);
        }

        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode resp = fyersClient.getQuotes(symbols, auth);

            List<Map<String, Object>> tickers = new ArrayList<>();
            Set<String> positionSymbols = PositionManager.getAllSymbols();
            JsonNode data = resp.get("d");
            if (data != null && data.isArray()) {
                for (JsonNode item : data) {
                    JsonNode v = item.get("v");
                    if (v == null) continue;
                    Map<String, Object> tick = new LinkedHashMap<>();
                    String sym = v.get("symbol").asText();
                    tick.put("symbol", v.has("short_name") ? v.get("short_name").asText() : sym);
                    tick.put("lp", v.get("lp").asDouble());
                    tick.put("ch", v.get("ch").asDouble());
                    tick.put("chp", v.get("chp").asDouble());
                    // Mark position symbols so the UI can highlight them
                    tick.put("position", positionSymbols.contains(sym));
                    tickers.add(tick);
                }
            }
            cachedTickers = tickers;
            lastFetchTime = now;
            lastSymbolsKey = symbols;
            return ResponseEntity.ok(tickers);
        } catch (Exception e) {
            return ResponseEntity.ok(cachedTickers.isEmpty() ? List.of() : cachedTickers);
        }
    }

    private String buildSymbolList() {
        Set<String> symbols = new LinkedHashSet<>();
        // Two indices first — NIFTY 50 + NIFTY BANK
        for (String s : BASE_SYMBOLS.split(",")) {
            symbols.add(s);
        }
        // All 50 NIFTY 50 stocks from the bhavcopy CPR cache. The cache is already
        // restricted to NIFTY 50 at the parse stage, so every non-index entry is a
        // NIFTY 50 stock. Defensive: still check isInNifty50 in case the fallback
        // "full FNO" path populated the cache on a NIFTY-50-list-unavailable day.
        for (CprLevels cpr : bhavcopyService.getAllCprLevels().values()) {
            if (bhavcopyService.isIndex(cpr.getSymbol())) continue;
            if (!cpr.isInNifty50()) continue;
            symbols.add("NSE:" + cpr.getSymbol() + "-EQ");
        }
        // Open position symbols — ensures user always sees live prices for what they hold,
        // even if somehow outside NIFTY 50 (manual position, NIFTY 50 rebalance mid-session).
        symbols.addAll(PositionManager.getAllSymbols());
        return String.join(",", symbols);
    }

    @GetMapping("/api/profile")
    public Map<String, Object> getProfile() {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode resp = fyersClient.getProfile(auth);
            if (resp != null && resp.has("data")) {
                JsonNode data = resp.get("data");
                result.put("name", data.has("name") ? data.get("name").asText() : "");
                result.put("fyId", data.has("fy_id") ? data.get("fy_id").asText() : "");
                result.put("email", data.has("email_id") ? data.get("email_id").asText() : "");
            }
        } catch (Exception e) {
            result.put("name", "");
        }
        return result;
    }

    @GetMapping("/api/market-holidays")
    public List<Map<String, String>> getMarketHolidays() {
        return marketHolidayService.getHolidayList();
    }

    @GetMapping("/api/console-log")
    public ResponseEntity<List<String>> getConsoleLog(@org.springframework.web.bind.annotation.RequestParam(defaultValue = "500") int lines) {
        try {
            java.nio.file.Path logFile = java.nio.file.Paths.get("../store/logs/autotrader.log");
            if (!java.nio.file.Files.exists(logFile)) return ResponseEntity.ok(List.of());
            List<String> allLines = java.nio.file.Files.readAllLines(logFile);
            int from = Math.max(0, allLines.size() - lines);
            return ResponseEntity.ok(allLines.subList(from, allLines.size()));
        } catch (Exception e) {
            return ResponseEntity.ok(List.of("Error reading log: " + e.getMessage()));
        }
    }
}
