package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.*;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * REST endpoints for the scanner dashboard.
 */
@RestController
public class ScannerController {

    private final MarketDataService marketDataService;
    private final BhavcopyService bhavcopyService;
    private final AtrService atrService;
    private final WeeklyCprService weeklyCprService;
    private final CandleAggregator candleAggregator;
    private final BreakoutScanner breakoutScanner;
    private final RiskSettingsStore riskSettings;

    public ScannerController(MarketDataService marketDataService,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             WeeklyCprService weeklyCprService,
                             CandleAggregator candleAggregator,
                             BreakoutScanner breakoutScanner,
                             RiskSettingsStore riskSettings) {
        this.marketDataService = marketDataService;
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.candleAggregator = candleAggregator;
        this.breakoutScanner = breakoutScanner;
        this.riskSettings = riskSettings;
    }

    @GetMapping("/api/scanner/watchlist")
    public List<Map<String, Object>> getWatchlist() {
        List<Map<String, Object>> result = new ArrayList<>();
        Set<String> positionSymbols = PositionManager.getAllSymbols();

        // Build set of weekly narrow CPR symbols for cross-referencing
        Set<String> weeklyNarrowSymbols = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getWeeklyNarrowCprStocks()) {
            weeklyNarrowSymbols.add(cpr.getSymbol());
        }

        // Collect narrow CPR stocks
        for (CprLevels cpr : bhavcopyService.getNarrowCprStocks()) {
            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            Map<String, Object> card = buildCard(fyers, cpr, "NARROW", positionSymbols);
            card.put("weeklyNarrow", weeklyNarrowSymbols.contains(cpr.getSymbol()));
            result.add(card);
        }

        // Collect inside CPR stocks (avoid duplicates)
        Set<String> seen = new HashSet<>();
        for (var m : result) seen.add(m.get("symbol").toString());
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            if (!seen.contains(fyers)) {
                Map<String, Object> card = buildCard(fyers, cpr, "INSIDE", positionSymbols);
                card.put("weeklyNarrow", weeklyNarrowSymbols.contains(cpr.getSymbol()));
                result.add(card);
            }
        }

        return result;
    }

    private Map<String, Object> buildCard(String fyersSymbol, CprLevels levels, String cprType, Set<String> positionSymbols) {
        Map<String, Object> card = new LinkedHashMap<>();
        card.put("symbol", fyersSymbol);
        card.put("shortName", levels.getSymbol());
        card.put("cprType", cprType);

        double ltp = candleAggregator.getLtp(fyersSymbol);
        if (ltp <= 0) ltp = levels.getClose(); // fallback to previous close (weekends/pre-market)
        double changePct = candleAggregator.getChangePct(fyersSymbol);
        card.put("ltp", Math.round(ltp * 100.0) / 100.0);
        card.put("changePercent", Math.round(changePct * 100.0) / 100.0);

        card.put("vwap", Math.round(candleAggregator.getVwap(fyersSymbol) * 100.0) / 100.0);
        card.put("atr", Math.round(atrService.getAtr(fyersSymbol) * 100.0) / 100.0);
        card.put("dayOpen", Math.round(candleAggregator.getDayOpen(fyersSymbol) * 100.0) / 100.0);
        card.put("candleVolume", candleAggregator.getCurrentCandleVolume(fyersSymbol));
        card.put("avgVolume", Math.round(candleAggregator.getAvgVolume(fyersSymbol, riskSettings.getVolumeLookback())));

        card.put("weeklyTrend", weeklyCprService.getWeeklyTrend(fyersSymbol));
        card.put("dailyTrend", weeklyCprService.getDailyTrend(fyersSymbol));
        card.put("probability", weeklyCprService.getProbability(fyersSymbol));

        // CPR levels
        Map<String, Object> lvls = new LinkedHashMap<>();
        lvls.put("r4", r(levels.getR4())); lvls.put("r3", r(levels.getR3()));
        lvls.put("r2", r(levels.getR2())); lvls.put("r1", r(levels.getR1()));
        lvls.put("ph", r(levels.getPh())); lvls.put("pivot", r(levels.getPivot()));
        lvls.put("tc", r(levels.getTc())); lvls.put("bc", r(levels.getBc()));
        lvls.put("s1", r(levels.getS1())); lvls.put("pl", r(levels.getPl()));
        lvls.put("s2", r(levels.getS2())); lvls.put("s3", r(levels.getS3()));
        lvls.put("s4", r(levels.getS4()));
        card.put("levels", lvls);

        // Broken levels
        Set<String> broken = breakoutScanner.getBrokenLevels(fyersSymbol);
        card.put("brokenLevels", broken != null ? new ArrayList<>(broken) : Collections.emptyList());

        // Last signal
        BreakoutScanner.SignalInfo sig = breakoutScanner.getLastSignal(fyersSymbol);
        if (sig != null) {
            Map<String, String> sigMap = new LinkedHashMap<>();
            sigMap.put("setup", sig.setup);
            sigMap.put("time", sig.time);
            sigMap.put("status", sig.status);
            card.put("lastSignal", sigMap);
        } else {
            card.put("lastSignal", null);
        }

        card.put("weeklyLevels", weeklyCprService.getWeeklyLevelsMap(fyersSymbol));
        card.put("hasPosition", positionSymbols.contains(fyersSymbol));
        card.put("cprWidthPct", Math.round(levels.getCprWidthPct() * 1000.0) / 1000.0);

        return card;
    }

    private double r(double v) { return Math.round(v * 100.0) / 100.0; }

    @GetMapping("/api/scanner/status")
    public Map<String, Object> getScannerStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("signalSource", riskSettings.getSignalSource());
        status.put("watchlistCount", marketDataService.getWatchlist().size());
        status.put("atrLoaded", atrService.getAllAtr().size());
        status.put("enableHpt", riskSettings.isEnableHpt());
        status.put("enableMpt", riskSettings.isEnableMpt());
        status.put("enableLpt", riskSettings.isEnableLpt());
        status.put("enableVwap", riskSettings.isEnableVwapCheck());
        status.put("timeframe", riskSettings.getScannerTimeframe());
        return status;
    }

    @GetMapping("/api/weekly-narrow-cpr")
    public Map<String, Object> getWeeklyNarrowCpr() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("weekDates", bhavcopyService.getWeekDateRange());
        result.put("historyDays", bhavcopyService.getHistoryDays());
        result.put("totalNfoStocks", bhavcopyService.getLoadedCount());

        var stocks = bhavcopyService.getWeeklyNarrowCprStocks();
        result.put("narrowCount", stocks.size());

        List<Map<String, Object>> stockList = new ArrayList<>();
        for (var cpr : stocks) {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("symbol", cpr.getSymbol());
            s.put("close", r(cpr.getClose()));
            s.put("cprWidthPct", Math.round(cpr.getCprWidthPct() * 1000.0) / 1000.0);
            s.put("pivot", r(cpr.getPivot()));
            s.put("tc", r(cpr.getTc()));
            s.put("bc", r(cpr.getBc()));
            s.put("r1", r(cpr.getR1()));
            s.put("s1", r(cpr.getS1()));
            stockList.add(s);
        }
        result.put("stocks", stockList);
        return result;
    }

    @GetMapping("/api/scanner/tv-watchlist")
    public ResponseEntity<String> getTvWatchlist() {
        StringBuilder csv = new StringBuilder();
        Set<String> added = new HashSet<>();

        // Narrow CPR stocks first
        for (CprLevels cpr : bhavcopyService.getNarrowCprStocks()) {
            csv.append("NSE:").append(cpr.getSymbol()).append(",");
            added.add(cpr.getSymbol());
        }
        // Inside CPR stocks (skip duplicates)
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            if (!added.contains(cpr.getSymbol())) {
                csv.append("NSE:").append(cpr.getSymbol()).append(",");
            }
        }

        String filename = "cpr-watchlist-" + bhavcopyService.getCachedDate() + ".txt";
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
            .contentType(MediaType.TEXT_PLAIN)
            .body(csv.toString());
    }
}
