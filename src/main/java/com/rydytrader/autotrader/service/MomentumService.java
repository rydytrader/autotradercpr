package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.dto.MomentumMetrics;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Computes momentum metrics for NFO stocks using BhavcopyService daily history.
 * Detects stocks that broke previous week/month high/low or 52-week high/low
 * with volume confirmation (volume >= configurable multiple of 20-day avg).
 */
@Service
public class MomentumService {

    private static final Logger log = LoggerFactory.getLogger(MomentumService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final BhavcopyService bhavcopyService;
    private final RiskSettingsStore riskSettings;

    private final ConcurrentHashMap<String, MomentumMetrics> metricsCache = new ConcurrentHashMap<>();

    public MomentumService(BhavcopyService bhavcopyService, RiskSettingsStore riskSettings) {
        this.bhavcopyService = bhavcopyService;
        this.riskSettings = riskSettings;
    }

    @PostConstruct
    public void init() {
        // Run after BhavcopyService loads data
        if (!bhavcopyService.getTodayCache().isEmpty()) {
            fetchMarketCap();
            compute();
        }
    }

    @Scheduled(cron = "0 46 8 * * MON-FRI") // 1 minute after BhavcopyService (8:45)
    public void scheduledCompute() {
        fetchMarketCap();
        compute();
    }

    /**
     * Compute momentum metrics for all NFO stocks.
     * Called after BhavcopyService loads data (via @PostConstruct or scheduled).
     */
    public void compute() {
        Map<String, CprLevels> today = bhavcopyService.getTodayCache();
        List<Map<String, CprLevels>> history = bhavcopyService.getDailyHistoryMaps();
        List<String> historyDates = bhavcopyService.getDailyHistoryDates();

        if (today.isEmpty()) {
            log.info("[MomentumService] No bhavcopy data available, skipping");
            return;
        }

        double volumeMultiple = riskSettings.getMomentumVolumeMultiple();
        int count = 0;
        int noVolume = 0, noWeekData = 0, noMonthData = 0, volumeFailed = 0;

        log.info("[MomentumService] Computing: {} stocks, {} history days, volumeMultiple={}",
            today.size(), history.size(), volumeMultiple);

        // Log first stock's data for debugging
        if (!today.isEmpty()) {
            var first = today.values().iterator().next();
            log.info("[MomentumService] Sample: {} close={} vol={} 52wH={} 52wL={}",
                first.getSymbol(), first.getClose(), first.getVolume(),
                first.getFiftyTwoWeekHigh(), first.getFiftyTwoWeekLow());
        }

        for (Map.Entry<String, CprLevels> entry : today.entrySet()) {
            String symbol = entry.getKey();
            CprLevels todayCpr = entry.getValue();

            MomentumMetrics m = new MomentumMetrics(symbol);
            m.setLastClose(todayCpr.getClose());
            m.setLastVolume(todayCpr.getVolume());
            m.setFiftyTwoWeekHigh(todayCpr.getFiftyTwoWeekHigh());
            m.setFiftyTwoWeekLow(todayCpr.getFiftyTwoWeekLow());
            m.setMarketCapCr(todayCpr.getMarketCapCr());

            // Compute 20-day average volume from history
            double totalVol = 0;
            int volDays = 0;
            for (Map<String, CprLevels> day : history) {
                CprLevels d = day.get(symbol);
                if (d != null && d.getVolume() > 0) {
                    totalVol += d.getVolume();
                    volDays++;
                }
                if (volDays >= 20) break;
            }
            double avgVol = volDays > 0 ? totalVol / volDays : 0;
            m.setAvgVolume20(avgVol);
            m.setVolumeRatio(avgVol > 0 ? todayCpr.getVolume() / avgVol : 0);

            // Volume gate — skip if below threshold
            boolean volumeOk = avgVol > 0 && m.getVolumeRatio() >= volumeMultiple;
            if (todayCpr.getVolume() == 0) noVolume++;

            // Previous week high/low (aggregate Mon-Fri of last complete week)
            double[] weekHL = computePreviousWeekHL(symbol, history, historyDates);
            m.setPrevWeekHigh(weekHL[0]);
            m.setPrevWeekLow(weekHL[1]);

            // Previous month high/low (aggregate last ~20 trading days before this week)
            double[] monthHL = computePreviousMonthHL(symbol, history);
            m.setPrevMonthHigh(monthHL[0]);
            m.setPrevMonthLow(monthHL[1]);

            if (weekHL[0] == 0 && weekHL[1] == 0) noWeekData++;
            if (monthHL[0] == 0 && monthHL[1] == 0) noMonthData++;
            if (!volumeOk && todayCpr.getVolume() > 0) volumeFailed++;

            // Check momentum breaks (all require volume confirmation)
            if (volumeOk && riskSettings.isMomentumWeekBreak()) {
                if (weekHL[0] > 0 && todayCpr.getClose() > weekHL[0]) m.addTag("WEEK_HIGH_BREAK");
                if (weekHL[1] > 0 && todayCpr.getClose() < weekHL[1]) m.addTag("WEEK_LOW_BREAK");
            }
            if (volumeOk && riskSettings.isMomentumMonthBreak()) {
                if (monthHL[0] > 0 && todayCpr.getClose() > monthHL[0]) m.addTag("MONTH_HIGH_BREAK");
                if (monthHL[1] > 0 && todayCpr.getClose() < monthHL[1]) m.addTag("MONTH_LOW_BREAK");
            }
            if (volumeOk && riskSettings.isMomentum52Week()) {
                double w52h = todayCpr.getFiftyTwoWeekHigh();
                double w52l = todayCpr.getFiftyTwoWeekLow();
                if (w52h > 0 && todayCpr.getClose() >= w52h) m.addTag("52W_HIGH_BREAK");
                if (w52l > 0 && todayCpr.getClose() <= w52l) m.addTag("52W_LOW_BREAK");
            }

            metricsCache.put(symbol, m);
            if (m.hasMomentumTag()) count++;
        }

        // Count how many are near 52W extremes (for debugging)
        int near52wHigh = 0, near52wLow = 0;
        for (MomentumMetrics mm : metricsCache.values()) {
            if (mm.getFiftyTwoWeekHigh() > 0 && mm.getLastClose() > 0) {
                double pctFromHigh = (mm.getFiftyTwoWeekHigh() - mm.getLastClose()) / mm.getFiftyTwoWeekHigh() * 100;
                if (pctFromHigh <= 5) near52wHigh++;
            }
            if (mm.getFiftyTwoWeekLow() > 0 && mm.getLastClose() > 0) {
                double pctFromLow = (mm.getLastClose() - mm.getFiftyTwoWeekLow()) / mm.getFiftyTwoWeekLow() * 100;
                if (pctFromLow <= 5) near52wLow++;
            }
        }
        log.info("[MomentumService] Computed: {} stocks, {} momentum | noVol={} noWeek={} noMonth={} volFailed={} near52wH={} near52wL={}",
            today.size(), count, noVolume, noWeekData, noMonthData, volumeFailed, near52wHigh, near52wLow);
    }

    /** Get all stocks with at least one momentum tag. */
    public List<MomentumMetrics> getMomentumStocks() {
        List<MomentumMetrics> result = new ArrayList<>();
        for (MomentumMetrics m : metricsCache.values()) {
            if (m.hasMomentumTag()) result.add(m);
        }
        return result;
    }

    /** Get metrics for a specific symbol. */
    public MomentumMetrics getMetrics(String symbol) {
        return metricsCache.get(symbol);
    }

    /** Get all metrics (including non-momentum stocks). */
    public Map<String, MomentumMetrics> getAllMetrics() {
        return Collections.unmodifiableMap(metricsCache);
    }

    // ── Aggregation helpers ──────────────────────────────────────────────────

    /**
     * Compute previous complete week's high/low from history.
     * Returns double[]{high, low}. Returns {0,0} if insufficient data.
     */
    private double[] computePreviousWeekHL(String symbol, List<Map<String, CprLevels>> history, List<String> dates) {
        double high = 0, low = Double.MAX_VALUE;
        boolean found = false;

        // Find days belonging to the previous complete week
        LocalDate today = LocalDate.now(IST);
        LocalDate mondayThisWeek = today.with(DayOfWeek.MONDAY);
        LocalDate mondayLastWeek = mondayThisWeek.minusWeeks(1);
        LocalDate fridayLastWeek = mondayLastWeek.plusDays(4);

        for (int i = 0; i < history.size() && i < dates.size(); i++) {
            LocalDate d = LocalDate.parse(dates.get(i));
            if (!d.isBefore(mondayLastWeek) && !d.isAfter(fridayLastWeek)) {
                CprLevels cpr = history.get(i).get(symbol);
                if (cpr != null) {
                    if (cpr.getHigh() > high) high = cpr.getHigh();
                    if (cpr.getLow() < low) low = cpr.getLow();
                    found = true;
                }
            }
        }
        return found ? new double[]{high, low} : new double[]{0, 0};
    }

    /**
     * Compute previous month's high/low from history (~20 trading days before current week).
     * Returns double[]{high, low}. Returns {0,0} if insufficient data.
     */
    private double[] computePreviousMonthHL(String symbol, List<Map<String, CprLevels>> history) {
        double high = 0, low = Double.MAX_VALUE;
        boolean found = false;

        // Use days 5-25 in history (skip this week's days at 0-4, use ~20 previous days)
        for (int i = 5; i < history.size() && i < 25; i++) {
            CprLevels cpr = history.get(i).get(symbol);
            if (cpr != null) {
                if (cpr.getHigh() > high) high = cpr.getHigh();
                if (cpr.getLow() < low) low = cpr.getLow();
                found = true;
            }
        }
        return found ? new double[]{high, low} : new double[]{0, 0};
    }

    // ── Market Cap from NSE API ──────────────────────────────────────────────

    private final ConcurrentHashMap<String, Double> marketCapCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, double[]> fiftyTwoWeekCache = new ConcurrentHashMap<>(); // symbol → {high, low}

    /**
     * Fetch market cap for Nifty 500 stocks from NSE API (single call per index).
     * Applies to BhavcopyService cache and MomentumMetrics.
     */
    public void fetchMarketCap() {
        try {
            String cookies = bhavcopyService.getNseCookies();
            if (cookies == null || cookies.isEmpty()) {
                log.warn("[MomentumService] Cannot fetch market cap — no NSE cookies");
                return;
            }

            String[] indices = {"NIFTY 500"};
            int total = 0;

            for (String index : indices) {
                try {
                    String url = "https://www.nseindia.com/api/equity-stockIndices?index="
                        + java.net.URLEncoder.encode(index, "UTF-8");
                    java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(url).openConnection();
                    conn.setRequestMethod("GET");
                    conn.setRequestProperty("User-Agent", "Mozilla/5.0");
                    conn.setRequestProperty("Accept", "application/json");
                    conn.setRequestProperty("Cookie", cookies);
                    conn.setConnectTimeout(10000);
                    conn.setReadTimeout(15000);

                    if (conn.getResponseCode() != 200) {
                        log.warn("[MomentumService] NSE market cap API returned {}", conn.getResponseCode());
                        continue;
                    }

                    String body;
                    try (var is = conn.getInputStream()) {
                        body = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                    }

                    com.fasterxml.jackson.databind.JsonNode root = new com.fasterxml.jackson.databind.ObjectMapper().readTree(body);
                    com.fasterxml.jackson.databind.JsonNode data = root.get("data");
                    if (data == null || !data.isArray()) continue;

                    // Log first node's field names for debugging
                    if (data.size() > 0) {
                        var firstNode = data.get(0);
                        var fields = new java.util.ArrayList<String>();
                        firstNode.fieldNames().forEachRemaining(fields::add);
                        log.info("[MomentumService] NSE API fields: {}", fields);
                    }

                    for (var node : data) {
                        String symbol = node.has("symbol") ? node.get("symbol").asText() : "";
                        if (symbol.isEmpty()) continue;
                        // Market cap
                        double mcap = 0;
                        if (node.has("ffmc")) mcap = node.get("ffmc").asDouble(0);
                        else if (node.has("marketCap")) mcap = node.get("marketCap").asDouble(0);
                        else if (node.has("totalTradedValue")) mcap = node.get("totalTradedValue").asDouble(0);

                        if (mcap > 0) {
                            double mcapCr = mcap / 100.0;
                            marketCapCache.put(symbol, mcapCr);
                            total++;
                        }

                        // 52-week high/low from same API response
                        double yHigh = node.has("yearHigh") ? node.get("yearHigh").asDouble(0) : 0;
                        double yLow = node.has("yearLow") ? node.get("yearLow").asDouble(0) : 0;
                        if (yHigh > 0 || yLow > 0) {
                            fiftyTwoWeekCache.put(symbol, new double[]{yHigh, yLow});
                        }
                    }
                } catch (Exception e) {
                    log.warn("[MomentumService] Error fetching market cap for {}: {}", index, e.getMessage());
                }
            }

            // Apply market cap + 52W data to bhavcopy CprLevels cache
            Map<String, CprLevels> todayCache = bhavcopyService.getTodayCache();
            int applied = 0;
            for (Map.Entry<String, Double> entry : marketCapCache.entrySet()) {
                CprLevels cpr = todayCache.get(entry.getKey());
                if (cpr != null) { cpr.setMarketCapCr(entry.getValue()); applied++; }
            }
            for (Map.Entry<String, double[]> entry : fiftyTwoWeekCache.entrySet()) {
                CprLevels cpr = todayCache.get(entry.getKey());
                if (cpr != null) {
                    cpr.setFiftyTwoWeekHigh(entry.getValue()[0]);
                    cpr.setFiftyTwoWeekLow(entry.getValue()[1]);
                }
            }

            // Apply to momentum metrics cache
            for (Map.Entry<String, Double> entry : marketCapCache.entrySet()) {
                MomentumMetrics m = metricsCache.get(entry.getKey());
                if (m != null) m.setMarketCapCr(entry.getValue());
            }

            log.info("[MomentumService] Market cap: {} stocks from NSE, {} applied to NFO cache, 52W data: {} stocks",
                total, applied, fiftyTwoWeekCache.size());
            // Log sample 52W data
            if (!fiftyTwoWeekCache.isEmpty()) {
                var sample = fiftyTwoWeekCache.entrySet().iterator().next();
                log.info("[MomentumService] 52W sample: {} high={} low={}", sample.getKey(), sample.getValue()[0], sample.getValue()[1]);
            } else {
                // Log first node's fields to see what's available
                log.warn("[MomentumService] No 52W data found — check API field names");
            }
        } catch (Exception e) {
            log.error("[MomentumService] Error fetching market cap: {}", e.getMessage());
        }
    }

    /** Get market cap for a symbol (in crores). Returns 0 if not available. */
    public double getMarketCap(String symbol) {
        return marketCapCache.getOrDefault(symbol, 0.0);
    }

    /** Get market cap category: "LARGE", "MID", "SMALL", or "" if unknown. */
    public String getMarketCapCategory(String symbol) {
        double mcap = getMarketCap(symbol);
        if (mcap <= 0) return "";
        if (mcap >= 20000) return "LARGE";
        if (mcap >= 5000) return "MID";
        return "SMALL";
    }

    /** Check if a symbol passes the market cap filter from settings. */
    public boolean passesMarketCapFilter(String symbol) {
        double mcap = getMarketCap(symbol);
        if (mcap <= 0) return true; // no data = allow through
        if (mcap >= 20000) return riskSettings.isMarketCapLarge();
        if (mcap >= 5000) return riskSettings.isMarketCapMid();
        return riskSettings.isMarketCapSmall();
    }
}
