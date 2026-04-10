package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads intraday margin multipliers from Fyers public endpoint on startup
 * and refreshes daily at market open.
 *
 * Data source: https://public.fyers.in/website/margin-calculator/equity/eq_website_upload.json
 * Each entry has: symbol (e.g. "HDFCBANK"), exchange, intraday_multiplier (1, 4, or 5).
 */
@Service
public class MarginDataService {

    private static final Logger log = LoggerFactory.getLogger(MarginDataService.class);

    private static final String MARGIN_URL =
        "https://public.fyers.in/website/margin-calculator/equity/eq_website_upload.json";

    private final ConcurrentHashMap<String, Integer> leverageMap = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final EventService eventService;

    public MarginDataService(EventService eventService) {
        this.eventService = eventService;
    }

    @PostConstruct
    public void loadOnStartup() {
        load();
    }

    @Scheduled(cron = "0 0 9 * * MON-FRI")
    public void reloadAtMarketOpen() {
        load();
    }

    /**
     * Returns the intraday leverage multiplier for a Fyers symbol.
     * Accepts formats like "NSE:HDFCBANK-EQ" or "HDFCBANK".
     * Returns 1 if symbol not found (safe default — no leverage).
     */
    public int getLeverage(String fyersSymbol) {
        String ticker = extractTicker(fyersSymbol);
        return leverageMap.getOrDefault(ticker, 1);
    }

    public int getLoadedCount() {
        return leverageMap.size();
    }

    /**
     * Extracts the bare ticker from a Fyers symbol.
     * "NSE:HDFCBANK-EQ" → "HDFCBANK"
     * "NSE:RELIANCE-EQ" → "RELIANCE"
     * "NSE:NIFTY26MARFUT" → "NIFTY26MARFUT" (won't match — futures default to leverage 1)
     */
    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        int dash = s.indexOf('-');
        if (dash >= 0) s = s.substring(0, dash);
        return s;
    }

    private void load() {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(MARGIN_URL).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(60_000);

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            }

            JsonNode root = objectMapper.readTree(sb.toString());
            JsonNode data = root.get("data");
            if (data == null || !data.isArray()) {
                log.error("[MarginData] No 'data' array in response");
                return;
            }

            int count = 0;
            for (JsonNode entry : data) {
                String symbol = entry.has("symbol") ? entry.get("symbol").asText() : "";
                int multiplier = entry.has("intraday_multiplier") ? entry.get("intraday_multiplier").asInt(1) : 1;
                if (!symbol.isEmpty() && multiplier > 0) {
                    leverageMap.put(symbol, multiplier);
                    count++;
                }
            }
            String msg = "[MarginData] Loaded " + count + " equity margin entries from Fyers";
            log.info(msg);
            eventService.log(msg);
            eventService.log("[INFO] Waiting for Fyers login to load ATR, EMA and weekly trends...");

        } catch (Exception e) {
            log.error("[MarginData] Failed to load margin data: {}", e.getMessage());
        }
    }
}
