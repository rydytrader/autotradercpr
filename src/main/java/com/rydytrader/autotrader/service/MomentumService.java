package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.dto.MomentumMetrics;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

            // Previous week high/low (aggregate Mon-Fri of last complete week)
            double[] weekHL = computePreviousWeekHL(symbol, history, historyDates);
            m.setPrevWeekHigh(weekHL[0]);
            m.setPrevWeekLow(weekHL[1]);

            // Previous month high/low (aggregate last ~20 trading days before this week)
            double[] monthHL = computePreviousMonthHL(symbol, history);
            m.setPrevMonthHigh(monthHL[0]);
            m.setPrevMonthLow(monthHL[1]);

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

        log.info("[MomentumService] Computed metrics for {} stocks, {} momentum candidates (vol >= {}x)",
            today.size(), count, volumeMultiple);
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
}
