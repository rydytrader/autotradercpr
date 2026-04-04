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
        if (!bhavcopyService.getTodayCache().isEmpty()) {
            fetch52WeekData();
            compute();
        }
    }

    @Scheduled(cron = "0 46 8 * * MON-FRI")
    public void scheduledCompute() {
        fetch52WeekData();
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

            // Get the day-before-yesterday's close (to check freshness of break)
            double prevDayClose = 0;
            if (!history.isEmpty()) {
                CprLevels prevDay = history.get(0).get(symbol);
                if (prevDay != null) prevDayClose = prevDay.getClose();
            }

            // Check momentum breaks (all require volume confirmation + fresh break on last trading day)
            if (volumeOk && riskSettings.isMomentumWeekBreak()) {
                if (weekHL[0] > 0 && todayCpr.getClose() > weekHL[0] && prevDayClose <= weekHL[0]) m.addTag("WEEK_HIGH_BREAK");
                if (weekHL[1] > 0 && todayCpr.getClose() < weekHL[1] && prevDayClose >= weekHL[1]) m.addTag("WEEK_LOW_BREAK");
            }
            if (volumeOk && riskSettings.isMomentumMonthBreak()) {
                if (monthHL[0] > 0 && todayCpr.getClose() > monthHL[0] && prevDayClose <= monthHL[0]) m.addTag("MONTH_HIGH_BREAK");
                if (monthHL[1] > 0 && todayCpr.getClose() < monthHL[1] && prevDayClose >= monthHL[1]) m.addTag("MONTH_LOW_BREAK");
            }
            if (volumeOk && riskSettings.isMomentum52Week()) {
                double w52h = todayCpr.getFiftyTwoWeekHigh();
                double w52l = todayCpr.getFiftyTwoWeekLow();
                if (w52h > 0 && todayCpr.getClose() >= w52h && prevDayClose < w52h) m.addTag("52W_HIGH_BREAK");
                if (w52l > 0 && todayCpr.getClose() <= w52l && prevDayClose > w52l) m.addTag("52W_LOW_BREAK");
            }

            // High Momentum candle: >3% change, 2x volume, closing range >85%
            if (volumeOk && riskSettings.isEnableHighMomentum() && prevDayClose > 0) {
                double changePct = (todayCpr.getClose() - prevDayClose) / prevDayClose * 100;
                double range = todayCpr.getHigh() - todayCpr.getLow();
                if (range > 0) {
                    double closingRangePct = (todayCpr.getClose() - todayCpr.getLow()) / range * 100; // 100% = closed at high
                    double hmThreshold = riskSettings.getHighMomentumPct();
                    double hmClosingRange = riskSettings.getHighMomentumClosingRange();

                    if (changePct >= hmThreshold && closingRangePct >= hmClosingRange) {
                        m.addTag("HMB+"); // High Momentum Bullish
                    } else if (changePct <= -hmThreshold && (100 - closingRangePct) >= hmClosingRange) {
                        m.addTag("HMB-"); // High Momentum Bearish
                    }
                }
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

    private final ConcurrentHashMap<String, double[]> fiftyTwoWeekCache = new ConcurrentHashMap<>(); // symbol → {high, low}

    /**
     * Fetch 52-week high/low from NSE Nifty 500 API.
     * Applied to BhavcopyService CprLevels cache before compute() runs.
     */
    public void fetch52WeekData() {
        try {
            String cookies = bhavcopyService.getNseCookies();
            if (cookies == null || cookies.isEmpty()) {
                log.warn("[MomentumService] Cannot fetch 52W data — no NSE cookies");
                return;
            }

            String url = "https://www.nseindia.com/api/equity-stockIndices?index="
                + java.net.URLEncoder.encode("NIFTY 500", "UTF-8");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Cookie", cookies);
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(15000);

            if (conn.getResponseCode() != 200) {
                log.warn("[MomentumService] NSE 52W API returned {}", conn.getResponseCode());
                return;
            }

            String body;
            try (var is = conn.getInputStream()) {
                body = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            }

            com.fasterxml.jackson.databind.JsonNode root = new com.fasterxml.jackson.databind.ObjectMapper().readTree(body);
            com.fasterxml.jackson.databind.JsonNode data = root.get("data");
            if (data == null || !data.isArray()) return;

            for (var node : data) {
                String symbol = node.has("symbol") ? node.get("symbol").asText() : "";
                if (symbol.isEmpty()) continue;
                double yHigh = node.has("yearHigh") ? node.get("yearHigh").asDouble(0) : 0;
                double yLow = node.has("yearLow") ? node.get("yearLow").asDouble(0) : 0;
                if (yHigh > 0 || yLow > 0) {
                    fiftyTwoWeekCache.put(symbol, new double[]{yHigh, yLow});
                }
            }

            // Apply 52W data to bhavcopy CprLevels cache
            Map<String, CprLevels> todayCache = bhavcopyService.getTodayCache();
            int applied = 0;
            for (Map.Entry<String, double[]> entry : fiftyTwoWeekCache.entrySet()) {
                CprLevels cpr = todayCache.get(entry.getKey());
                if (cpr != null) {
                    cpr.setFiftyTwoWeekHigh(entry.getValue()[0]);
                    cpr.setFiftyTwoWeekLow(entry.getValue()[1]);
                    applied++;
                }
            }

            log.info("[MomentumService] 52W data: {} stocks from NSE, {} applied to NFO cache", fiftyTwoWeekCache.size(), applied);
        } catch (Exception e) {
            log.error("[MomentumService] Error fetching 52W data: {}", e.getMessage());
        }
    }
}
