package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.store.TokenStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Computes monthly CPR from calendar month boundaries (previous month's OHLC).
 * Used for swing trade trend alignment: weekly breakout direction vs monthly trend = HPT/MPT/LPT.
 * Refreshed once per month (1st trading day) or on startup.
 */
@Service
public class MonthlyCprService {

    private static final Logger log = LoggerFactory.getLogger(MonthlyCprService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final ObjectMapper mapper = new ObjectMapper();

    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;

    // symbol → monthly levels (refreshed once per month)
    private final ConcurrentHashMap<String, MonthlyLevels> monthlyLevels = new ConcurrentHashMap<>();
    private volatile String computedForMonth = ""; // e.g., "2026-03" — the month whose data was used

    public MonthlyCprService(TokenStore tokenStore, FyersProperties fyersProperties) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
    }

    /**
     * Fetch monthly CPR for a list of symbols. Called by MarketDataService on scanner init.
     * Uses previous calendar month's daily candles from Fyers history API.
     */
    public void fetchMonthlyLevels(List<String> fyersSymbols) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[MonthlyCpr] No access token, cannot fetch monthly data");
            return;
        }

        // Determine previous month boundaries
        LocalDate today = LocalDate.now(IST);
        YearMonth prevMonth = YearMonth.from(today).minusMonths(1);
        String prevMonthStr = prevMonth.toString(); // e.g., "2026-03"

        // Skip if already computed for this month
        if (prevMonthStr.equals(computedForMonth) && !monthlyLevels.isEmpty()) {
            log.info("[MonthlyCpr] Already computed for {} ({} symbols)", prevMonthStr, monthlyLevels.size());
            return;
        }

        LocalDate monthStart = prevMonth.atDay(1);
        LocalDate monthEnd = prevMonth.atEndOfMonth();
        long fromEpoch = monthStart.atStartOfDay(IST).toEpochSecond();
        long toEpoch = monthEnd.plusDays(1).atStartOfDay(IST).toEpochSecond(); // inclusive

        String authHeader = fyersProperties.getClientId() + ":" + accessToken;

        log.info("[MonthlyCpr] Fetching monthly CPR for {} symbols (month: {})", fyersSymbols.size(), prevMonthStr);

        int success = 0;
        for (String symbol : fyersSymbols) {
            try {
                double[] ohlc = fetchMonthlyOhlc(symbol, fromEpoch, toEpoch, authHeader);
                if (ohlc != null) {
                    double mH = ohlc[1], mL = ohlc[2], mC = ohlc[3];
                    MonthlyLevels ml = new MonthlyLevels();
                    ml.pivot = (mH + mL + mC) / 3.0;
                    ml.bc = (mH + mL) / 2.0;
                    ml.tc = 2.0 * ml.pivot - ml.bc;
                    ml.top = Math.max(ml.tc, ml.bc);
                    ml.bot = Math.min(ml.tc, ml.bc);
                    double range = mH - mL;
                    ml.r1 = 2.0 * ml.pivot - mL;
                    ml.s1 = 2.0 * ml.pivot - mH;
                    ml.r2 = ml.pivot + range;
                    ml.s2 = ml.pivot - range;
                    ml.ph = mH;
                    ml.pl = mL;

                    String ticker = extractTicker(symbol);
                    monthlyLevels.put(ticker, ml);
                    success++;
                }
                Thread.sleep(200); // throttle
            } catch (Exception e) {
                log.error("[MonthlyCpr] Failed for {}: {}", symbol, e.getMessage());
            }
        }

        computedForMonth = prevMonthStr;
        log.info("[MonthlyCpr] Monthly levels loaded for {}/{} symbols (month: {})", success, fyersSymbols.size(), prevMonthStr);
    }

    /**
     * Get monthly trend for a symbol based on LTP vs monthly CPR.
     * Returns: BULLISH, BEARISH, NEUTRAL
     */
    public String getMonthlyTrend(String fyersSymbol) {
        String ticker = extractTicker(fyersSymbol);
        MonthlyLevels ml = monthlyLevels.get(ticker);
        if (ml == null) return "NEUTRAL";

        // We don't have LTP here — caller should provide it
        // For now, return based on stored levels (caller can override)
        return "NEUTRAL";
    }

    /**
     * Get monthly trend based on LTP position vs monthly CPR levels.
     */
    public String getMonthlyTrend(String fyersSymbol, double ltp) {
        String ticker = extractTicker(fyersSymbol);
        MonthlyLevels ml = monthlyLevels.get(ticker);
        if (ml == null || ltp <= 0) return "NEUTRAL";

        if (ltp > ml.top) return "BULLISH";
        if (ltp < ml.bot) return "BEARISH";
        return "NEUTRAL";
    }

    /**
     * Get swing probability: weekly breakout direction vs monthly trend.
     */
    public String getSwingProbability(String fyersSymbol, double ltp, String breakoutSide) {
        String monthlyTrend = getMonthlyTrend(fyersSymbol, ltp);
        if ("BUY".equals(breakoutSide)) {
            if ("BULLISH".equals(monthlyTrend)) return "HPT";
            if ("BEARISH".equals(monthlyTrend)) return "LPT";
            return "MPT";
        } else if ("SELL".equals(breakoutSide)) {
            if ("BEARISH".equals(monthlyTrend)) return "HPT";
            if ("BULLISH".equals(monthlyTrend)) return "LPT";
            return "MPT";
        }
        return "MPT";
    }

    /** Get monthly levels map for API/UI. */
    public Map<String, Double> getMonthlyLevelsMap(String fyersSymbol) {
        String ticker = extractTicker(fyersSymbol);
        MonthlyLevels ml = monthlyLevels.get(ticker);
        if (ml == null) return Collections.emptyMap();
        Map<String, Double> m = new LinkedHashMap<>();
        m.put("pivot", Math.round(ml.pivot * 100.0) / 100.0);
        m.put("tc", Math.round(ml.tc * 100.0) / 100.0);
        m.put("bc", Math.round(ml.bc * 100.0) / 100.0);
        m.put("r1", Math.round(ml.r1 * 100.0) / 100.0);
        m.put("r2", Math.round(ml.r2 * 100.0) / 100.0);
        m.put("s1", Math.round(ml.s1 * 100.0) / 100.0);
        m.put("s2", Math.round(ml.s2 * 100.0) / 100.0);
        m.put("ph", Math.round(ml.ph * 100.0) / 100.0);
        m.put("pl", Math.round(ml.pl * 100.0) / 100.0);
        return m;
    }

    public String getComputedForMonth() { return computedForMonth; }

    // ── Fyers history fetch ──────────────────────────────────────────────────

    private double[] fetchMonthlyOhlc(String symbol, long fromEpoch, long toEpoch, String authHeader) throws Exception {
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
        if (candles == null || !candles.isArray() || candles.isEmpty()) return null;

        // Aggregate all daily candles into monthly OHLC
        double open = candles.get(0).get(1).asDouble();
        double high = Double.MIN_VALUE, low = Double.MAX_VALUE;
        double close = candles.get(candles.size() - 1).get(4).asDouble();

        for (int i = 0; i < candles.size(); i++) {
            JsonNode c = candles.get(i);
            double h = c.get(2).asDouble();
            double l = c.get(3).asDouble();
            if (h > high) high = h;
            if (l < low) low = l;
        }

        return new double[]{open, high, low, close};
    }

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        int dash = s.indexOf('-');
        if (dash >= 0) s = s.substring(0, dash);
        return s;
    }

    // ── Inner class ─────────────────────────────────────────────────────────

    public static class MonthlyLevels {
        public double pivot, tc, bc, top, bot;
        public double r1, r2, s1, s2;
        public double ph, pl;
    }
}
