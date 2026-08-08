package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.MarketDataService;
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
import java.util.Set;

@RestController
public class MarketTickerController {

    private final FyersClientRouter fyersClient;
    private final FyersProperties fyersProperties;
    private final TokenStore tokenStore;
    private final MarketHolidayService marketHolidayService;
    private final MarketDataService marketDataService;

    // Indices stay hardcoded — used by MarketDataService to ensure the WebSocket always
    // subscribes to them for the NIFTY trend calc + sector chips on the scanner page.
    // Stocks on the ticker come dynamically from the NIFTY 50 list via bhavcopy.
    private static final String BASE_SYMBOLS =
        "NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX,NSE:FINNIFTY-INDEX,"
      + "NSE:INDIAVIX-INDEX,"
      + "NSE:NIFTYIT-INDEX,NSE:NIFTYPHARMA-INDEX,NSE:NIFTYAUTO-INDEX,"
      + "NSE:NIFTYFMCG-INDEX,NSE:NIFTYMETAL-INDEX,NSE:NIFTYENERGY-INDEX,"
      + "NSE:NIFTYHEALTHCARE-INDEX,NSE:NIFTYREALTY-INDEX,NSE:NIFTYMEDIA-INDEX,"
      + "NSE:NIFTYOILANDGAS-INDEX,NSE:NIFTYCONSRDURBL-INDEX,"
      + "NSE:NIFTYSERVSECTOR-INDEX,NSE:NIFTYCONSUMPTION-INDEX,"
      + "NSE:NIFTYINFRA-INDEX,NSE:NIFTYCOMMODITIES-INDEX";

    /** Returns the base index symbols. Used by MarketDataService for WebSocket subscription. */
    public static String[] getBaseSymbols() {
        return BASE_SYMBOLS.split(",");
    }

    public MarketTickerController(FyersClientRouter fyersClient,
                                   FyersProperties fyersProperties,
                                   TokenStore tokenStore,
                                   MarketHolidayService marketHolidayService,
                                   MarketDataService marketDataService) {
        this.fyersClient = fyersClient;
        this.fyersProperties = fyersProperties;
        this.tokenStore = tokenStore;
        this.marketHolidayService = marketHolidayService;
        this.marketDataService = marketDataService;
    }

    /** REST snapshot for the top-of-page ticker. Reads from {@link MarketDataService}'s
     *  in-memory tick cache — Fyers /quotes was retired with the strip-Fyers-data
     *  refactor. Only symbols with a live LTP in cache appear; the SSE stream
     *  (/api/market-ticker-sse) is the real-time feed. */
    @GetMapping("/api/market-ticker")
    public ResponseEntity<?> getMarketTicker() {
        Set<String> wanted = new LinkedHashSet<>();
        for (String s : BASE_SYMBOLS.split(",")) wanted.add(s);
        wanted.addAll(PositionManager.getAllSymbols());
        Set<String> positionSymbols = PositionManager.getAllSymbols();

        List<Map<String, Object>> tickers = new ArrayList<>();
        for (String sym : wanted) {
            double lp = marketDataService.getDisplayLtp(sym);
            if (lp <= 0) continue;
            Map<String, Object> tick = new LinkedHashMap<>();
            tick.put("symbol", shortName(sym));
            tick.put("lp",  lp);
            tick.put("ch",  marketDataService.getDisplayChange(sym));
            tick.put("chp", marketDataService.getDisplayChangePct(sym));
            tick.put("position", positionSymbols.contains(sym));
            tickers.add(tick);
        }
        return ResponseEntity.ok(tickers);
    }

    private static String shortName(String fyersSymbol) {
        try {
            String afterColon = fyersSymbol.split(":")[1];
            return afterColon.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        } catch (Exception e) {
            return fyersSymbol;
        }
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
