package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Calculates weekly and daily CPR trends in real-time using current LTP.
 *
 * Weekly CPR levels are fetched once from Fyers history API at 9:00 AM.
 * Trends are recalculated on every candle close using LTP vs CPR levels
 * (matching Pine Script logic: close > wTop = Bullish, etc.).
 */
@Service
public class WeeklyCprService {

    private static final Logger log = LoggerFactory.getLogger(WeeklyCprService.class);
    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final BhavcopyService bhavcopyService;
    private final CandleAggregator candleAggregator;
    private final ObjectMapper mapper = new ObjectMapper();

    // Weekly CPR levels per symbol (fixed for the week, fetched once)
    private final ConcurrentHashMap<String, WeeklyLevels> weeklyLevels = new ConcurrentHashMap<>();

    public WeeklyCprService(TokenStore tokenStore,
                            FyersProperties fyersProperties,
                            BhavcopyService bhavcopyService,
                            CandleAggregator candleAggregator) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.bhavcopyService = bhavcopyService;
        this.candleAggregator = candleAggregator;
    }

    /**
     * Fetch weekly data for all watchlist symbols (called at 9:00 AM or on restart).
     */
    public void fetchWeeklyTrends(List<String> fyersSymbols) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[WeeklyCpr] No access token, cannot fetch weekly data");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;

        log.info("[WeeklyCpr] Fetching weekly CPR levels for {} symbols", fyersSymbols.size());

        int success = 0;
        for (String symbol : fyersSymbols) {
            try {
                double[] weeklyOhlc = fetchPreviousWeekOhlc(symbol, authHeader);
                if (weeklyOhlc != null) {
                    WeeklyLevels wl = new WeeklyLevels();
                    double wH = weeklyOhlc[1], wL = weeklyOhlc[2], wC = weeklyOhlc[3];
                    wl.pivot = (wH + wL + wC) / 3.0;
                    wl.bc = (wH + wL) / 2.0;
                    wl.tc = 2.0 * wl.pivot - wl.bc;
                    wl.top = Math.max(wl.tc, wl.bc);
                    wl.bot = Math.min(wl.tc, wl.bc);
                    wl.r1 = 2.0 * wl.pivot - wL;
                    wl.s1 = 2.0 * wl.pivot - wH;
                    wl.ph = wH;
                    wl.pl = wL;
                    weeklyLevels.put(symbol, wl);
                    success++;
                }
                Thread.sleep(300); // throttle
            } catch (Exception e) {
                log.error("[WeeklyCpr] Failed for {}: {}", symbol, e.getMessage());
            }
        }
        log.info("[WeeklyCpr] Weekly levels loaded for {}/{} symbols", success, fyersSymbols.size());
    }

    // ── Real-time trend calculation using LTP ────────────────────────────────

    /**
     * Get weekly trend based on current LTP vs weekly CPR levels.
     * Matches Pine Script: close > wTop = Bullish, close < wBot = Bearish.
     * Strong Bullish = close > weekly R1 AND close > previous week high.
     */
    public String getWeeklyTrend(String symbol) {
        WeeklyLevels wl = weeklyLevels.get(symbol);
        if (wl == null) return "NEUTRAL";

        double ltp = getPrice(symbol);
        if (ltp <= 0) return "NEUTRAL";

        if (ltp > wl.r1 && ltp > wl.ph) return "STRONG_BULLISH";
        if (ltp < wl.s1 && ltp < wl.pl) return "STRONG_BEARISH";
        if (ltp > wl.top) return "BULLISH";
        if (ltp < wl.bot) return "BEARISH";
        return "NEUTRAL";
    }

    /**
     * Get daily trend based on current LTP vs daily CPR levels.
     * Matches Pine Script: close > dTop = Bullish, close < dBot = Bearish.
     * Strong Bullish = close > R1 AND close > PDH.
     */
    public String getDailyTrend(String symbol) {
        String ticker = extractTicker(symbol);
        CprLevels cpr = bhavcopyService.getCprLevels(ticker);
        if (cpr == null) return "NEUTRAL";

        double ltp = getPrice(symbol);
        if (ltp <= 0) return "NEUTRAL";

        double dTop = Math.max(cpr.getTc(), cpr.getBc());
        double dBot = Math.min(cpr.getTc(), cpr.getBc());

        if (ltp > cpr.getR1() && ltp > cpr.getPh()) return "STRONG_BULLISH";
        if (ltp < cpr.getS1() && ltp < cpr.getPl()) return "STRONG_BEARISH";
        if (ltp > dTop) return "BULLISH";
        if (ltp < dBot) return "BEARISH";
        return "NEUTRAL";
    }

    /**
     * Get probability based on real-time weekly + daily trends (for scanner dashboard display).
     * Returns the probability assuming current daily trend direction.
     * For actual signal generation, use getProbabilityForDirection() instead.
     */
    public String getProbability(String symbol) {
        String daily = getDailyTrend(symbol);
        boolean dBull = daily.contains("BULLISH");
        boolean dBear = daily.contains("BEARISH");
        if (dBull) return getProbabilityForDirection(symbol, true);
        if (dBear) return getProbabilityForDirection(symbol, false);
        // Daily neutral — no classification until breakout direction is known
        return "--";
    }

    /**
     * Get probability based on breakout direction + weekly trend.
     * HPT: weekly aligned with direction. MPT: weekly neutral. LPT: weekly opposed.
     * @param isBuy true for buy breakout, false for sell breakout
     */
    public String getProbabilityForDirection(String symbol, boolean isBuy) {
        String weekly = getWeeklyTrend(symbol);
        boolean wBull = weekly.contains("BULLISH");
        boolean wBear = weekly.contains("BEARISH");
        boolean wNeutral = "NEUTRAL".equals(weekly);

        if (isBuy) {
            if (wBull) return "HPT";
            if (wNeutral) return "MPT";
            return "LPT"; // weekly bearish, buying
        } else {
            if (wBear) return "HPT";
            if (wNeutral) return "MPT";
            return "LPT"; // weekly bullish, selling
        }
    }

    // ── Fyers history API ────────────────────────────────────────────────────

    private double[] fetchPreviousWeekOhlc(String symbol, String authHeader) throws Exception {
        // Fetch daily candles for last 3 weeks and aggregate into weekly OHLC ourselves,
        // because Fyers weekly candle API may not include the most recent completed week on weekends.
        long toEpoch = Instant.now().getEpochSecond();
        long fromEpoch = toEpoch - (21 * 24 * 3600); // 3 weeks back

        String urlStr = "https://api-t1.fyers.in/data/history?symbol=" + symbol
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
        if (status != 200) {
            throw new IOException("HTTP " + status);
        }

        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }

        JsonNode root = mapper.readTree(sb.toString());
        JsonNode candles = root.get("candles");
        if (candles == null || !candles.isArray() || candles.isEmpty()) {
            return null;
        }

        // Group daily candles by ISO week number
        java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
        Map<Integer, double[]> weeklyOhlc = new LinkedHashMap<>(); // weekNum → [open, high, low, close]
        List<Integer> weekOrder = new ArrayList<>();

        for (int i = 0; i < candles.size(); i++) {
            JsonNode c = candles.get(i);
            long epoch = c.get(0).asLong();
            java.time.LocalDate date = java.time.Instant.ofEpochSecond(epoch).atZone(ist).toLocalDate();
            int weekNum = date.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);

            double o = c.get(1).asDouble(), h = c.get(2).asDouble();
            double l = c.get(3).asDouble(), cl = c.get(4).asDouble();

            if (!weeklyOhlc.containsKey(weekNum)) {
                weeklyOhlc.put(weekNum, new double[]{o, h, l, cl});
                weekOrder.add(weekNum);
            } else {
                double[] w = weeklyOhlc.get(weekNum);
                w[1] = Math.max(w[1], h);  // high
                w[2] = Math.min(w[2], l);  // low
                w[3] = cl;                  // close (last day's close)
            }
        }

        if (weekOrder.size() < 2) return null;

        // On weekdays, current week is incomplete → use second-to-last completed week.
        // On weekends, current week is done → use last week.
        java.time.DayOfWeek today = java.time.LocalDate.now().getDayOfWeek();
        boolean weekend = (today == java.time.DayOfWeek.SATURDAY || today == java.time.DayOfWeek.SUNDAY);
        int targetWeekIdx = weekend ? weekOrder.size() - 1 : weekOrder.size() - 2;
        int targetWeekNum = weekOrder.get(targetWeekIdx);
        double[] result = weeklyOhlc.get(targetWeekNum);

        return result;
    }

    /**
     * Get current price: live LTP if available, fallback to previous close from bhavcopy
     * (for weekends/pre-market when no ticks are flowing).
     */
    private double getPrice(String symbol) {
        double ltp = candleAggregator.getLtp(symbol);
        if (ltp > 0) return ltp;
        // Fallback to previous close from BhavcopyService
        String ticker = extractTicker(symbol);
        CprLevels cpr = bhavcopyService.getCprLevels(ticker);
        return cpr != null ? cpr.getClose() : 0;
    }

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        int dash = s.indexOf('-');
        if (dash >= 0) s = s.substring(0, dash);
        return s;
    }

    /** Debug: return weekly levels for a symbol as a map. */
    public Map<String, Double> getWeeklyLevelsMap(String symbol) {
        WeeklyLevels wl = weeklyLevels.get(symbol);
        if (wl == null) return Collections.emptyMap();
        Map<String, Double> m = new LinkedHashMap<>();
        m.put("pivot", Math.round(wl.pivot * 100.0) / 100.0);
        m.put("tc", Math.round(wl.tc * 100.0) / 100.0);
        m.put("bc", Math.round(wl.bc * 100.0) / 100.0);
        m.put("top", Math.round(wl.top * 100.0) / 100.0);
        m.put("bot", Math.round(wl.bot * 100.0) / 100.0);
        m.put("r1", Math.round(wl.r1 * 100.0) / 100.0);
        m.put("s1", Math.round(wl.s1 * 100.0) / 100.0);
        m.put("ph", Math.round(wl.ph * 100.0) / 100.0);
        m.put("pl", Math.round(wl.pl * 100.0) / 100.0);
        return m;
    }

    // ── Inner class for weekly CPR levels ────────────────────────────────────

    private static class WeeklyLevels {
        double pivot, tc, bc, top, bot;
        double r1, s1, ph, pl;
    }
}
