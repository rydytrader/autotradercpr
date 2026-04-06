package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.store.TokenStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Computes Weekly ATR(14) for swing trading SL sizing.
 * Fetches ~20 weeks of daily candles from Fyers, aggregates into weekly bars,
 * computes True Range per week, then ATR(14) using Wilder's smoothing.
 */
@Service
public class WeeklyAtrService {

    private static final Logger log = LoggerFactory.getLogger(WeeklyAtrService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int ATR_PERIOD = 14;

    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;

    // symbol (ticker) → weekly ATR value
    private final ConcurrentHashMap<String, Double> weeklyAtrBySymbol = new ConcurrentHashMap<>();

    public WeeklyAtrService(TokenStore tokenStore, FyersProperties fyersProperties) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
    }

    /**
     * Fetch and compute weekly ATR for a list of symbols.
     * Called by MarketDataService on scanner init for WN/WI stocks.
     */
    public void fetchWeeklyAtr(List<String> fyersSymbols) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[WeeklyATR] No access token, cannot fetch");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;

        log.info("[WeeklyATR] Fetching weekly ATR({}) for {} symbols", ATR_PERIOD, fyersSymbols.size());

        int success = 0;
        for (String symbol : fyersSymbols) {
            try {
                double atr = computeWeeklyAtr(symbol, authHeader);
                if (atr > 0) {
                    String ticker = extractTicker(symbol);
                    weeklyAtrBySymbol.put(ticker, atr);
                    success++;
                }
                Thread.sleep(300);
            } catch (Exception e) {
                log.error("[WeeklyATR] Failed for {}: {}", symbol, e.getMessage());
            }
        }
        log.info("[WeeklyATR] Weekly ATR loaded for {}/{} symbols", success, fyersSymbols.size());
    }

    /** Get weekly ATR for a symbol. Returns 0 if not available. */
    public double getWeeklyAtr(String fyersSymbol) {
        String ticker = extractTicker(fyersSymbol);
        return weeklyAtrBySymbol.getOrDefault(ticker, 0.0);
    }

    private double computeWeeklyAtr(String symbol, String authHeader) throws Exception {
        // Fetch ~120 days of daily candles (covers ~20+ weeks)
        long toEpoch = Instant.now().getEpochSecond();
        long fromEpoch = toEpoch - (140L * 24 * 3600); // 140 days back

        String urlStr = "https://api-t1.fyers.in/data/history?symbol=" + java.net.URLEncoder.encode(symbol, StandardCharsets.UTF_8)
            + "&resolution=1D"
            + "&date_format=0"
            + "&range_from=" + fromEpoch
            + "&range_to=" + toEpoch
            + "&cont_flag=1";

        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", authHeader);
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);

        int status = conn.getResponseCode();
        if (status != 200) throw new IOException("HTTP " + status);

        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }

        JsonNode root = mapper.readTree(sb.toString());
        JsonNode candles = root.get("candles");
        if (candles == null || !candles.isArray() || candles.size() < 20) return 0;

        // Parse daily candles
        List<double[]> dailyBars = new ArrayList<>(); // [epoch, open, high, low, close]
        for (JsonNode c : candles) {
            dailyBars.add(new double[]{
                c.get(0).asDouble(), c.get(1).asDouble(), c.get(2).asDouble(),
                c.get(3).asDouble(), c.get(4).asDouble()
            });
        }

        // Aggregate into weekly bars (Mon-Fri grouping using ISO week)
        List<double[]> weeklyBars = aggregateToWeekly(dailyBars);
        if (weeklyBars.size() < ATR_PERIOD + 1) return 0;

        // Compute ATR using Wilder's smoothing on weekly bars
        return calculateAtr(weeklyBars, ATR_PERIOD);
    }

    private List<double[]> aggregateToWeekly(List<double[]> dailyBars) {
        Map<String, double[]> weekMap = new LinkedHashMap<>(); // "year-week" → [open, high, low, close]

        for (double[] bar : dailyBars) {
            long epochSec = (long) bar[0];
            LocalDate date = Instant.ofEpochSecond(epochSec).atZone(IST).toLocalDate();
            int week = date.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            int year = date.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);
            String key = year + "-" + week;

            double[] agg = weekMap.get(key);
            if (agg == null) {
                agg = new double[]{bar[1], bar[2], bar[3], bar[4]}; // open, high, low, close
                weekMap.put(key, agg);
            } else {
                if (bar[2] > agg[1]) agg[1] = bar[2]; // update high
                if (bar[3] < agg[2]) agg[2] = bar[3]; // update low
                agg[3] = bar[4]; // update close (last day's close)
            }
        }

        return new ArrayList<>(weekMap.values());
    }

    private double calculateAtr(List<double[]> bars, int period) {
        if (bars.size() < period + 1) return 0;

        // Compute true range for each bar
        double[] tr = new double[bars.size()];
        for (int i = 0; i < bars.size(); i++) {
            double high = bars.get(i)[1];
            double low = bars.get(i)[2];
            double prevClose = i > 0 ? bars.get(i - 1)[3] : bars.get(i)[0]; // use open for first bar
            tr[i] = Math.max(high - low, Math.max(Math.abs(high - prevClose), Math.abs(low - prevClose)));
        }

        // Wilder's smoothing: SMA for first period, then exponential
        int start = bars.size() - period;
        double atr = 0;
        for (int i = start; i < bars.size(); i++) {
            if (i == start) {
                atr = tr[i];
            } else {
                atr = (atr * (period - 1) + tr[i]) / period;
            }
        }
        return Math.round(atr * 100.0) / 100.0;
    }

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        int dash = s.indexOf('-');
        if (dash >= 0) s = s.substring(0, dash);
        return s;
    }
}
