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
    private final MarginDataService marginDataService;
    private final MomentumService momentumService;

    public ScannerController(MarketDataService marketDataService,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             WeeklyCprService weeklyCprService,
                             CandleAggregator candleAggregator,
                             BreakoutScanner breakoutScanner,
                             RiskSettingsStore riskSettings,
                             MarginDataService marginDataService,
                             MomentumService momentumService) {
        this.marketDataService = marketDataService;
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.candleAggregator = candleAggregator;
        this.breakoutScanner = breakoutScanner;
        this.riskSettings = riskSettings;
        this.marginDataService = marginDataService;
        this.momentumService = momentumService;
    }

    @GetMapping("/api/scanner/watchlist")
    public List<Map<String, Object>> getWatchlist() {
        List<Map<String, Object>> result = new ArrayList<>();
        Set<String> positionSymbols = PositionManager.getAllSymbols();

        // Build sets for cross-referencing
        Set<String> weeklyNarrowSymbols = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getWeeklyNarrowCprStocks()) {
            weeklyNarrowSymbols.add(cpr.getSymbol());
        }
        Set<String> narrowSymbols = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getNarrowCprStocks()) {
            narrowSymbols.add(cpr.getSymbol());
        }
        Set<String> insideSymbols = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            insideSymbols.add(cpr.getSymbol());
        }

        // Collect narrow CPR stocks (mark if also inside)
        Set<String> seen = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getNarrowCprStocks()) {
            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            List<String> types = new ArrayList<>();
            types.add("NARROW");
            if (insideSymbols.contains(cpr.getSymbol())) types.add("INSIDE");
            Map<String, Object> card = buildCard(fyers, cpr, "NARROW", positionSymbols);
            card.put("cprTypes", types);
            card.put("weeklyNarrow", weeklyNarrowSymbols.contains(cpr.getSymbol()));
            result.add(card);
            seen.add(fyers);
        }

        // Collect inside-only CPR stocks
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            if (!seen.contains(fyers)) {
                List<String> types = new ArrayList<>();
                types.add("INSIDE");
                Map<String, Object> card = buildCard(fyers, cpr, "INSIDE", positionSymbols);
                card.put("cprTypes", types);
                card.put("weeklyNarrow", weeklyNarrowSymbols.contains(cpr.getSymbol()));
                result.add(card);
                seen.add(fyers);
            }
        }

        // Add momentum tags to existing cards + add momentum-only stocks
        for (var m : momentumService.getMomentumStocks()) {
            String fyers = "NSE:" + m.getSymbol() + "-EQ";
            if (!momentumService.passesMarketCapFilter(m.getSymbol())) continue;

            // Find existing card or create new one
            Map<String, Object> existingCard = null;
            for (var card : result) {
                if (fyers.equals(card.get("symbol"))) { existingCard = card; break; }
            }

            if (existingCard != null) {
                // Merge momentum tags into existing CPR card
                @SuppressWarnings("unchecked")
                List<String> types = (List<String>) existingCard.get("cprTypes");
                types.addAll(m.getTags());
                existingCard.put("momentumTags", m.getTags());
                existingCard.put("volumeRatio", Math.round(m.getVolumeRatio() * 10.0) / 10.0);
                existingCard.put("marketCapCr", Math.round(m.getMarketCapCr()));
                existingCard.put("marketCapCategory", momentumService.getMarketCapCategory(m.getSymbol()));
            } else {
                // New momentum-only stock — build card from bhavcopy data
                CprLevels cpr = bhavcopyService.getCprLevels(m.getSymbol());
                if (cpr != null) {
                    Map<String, Object> card = buildCard(fyers, cpr, "MOMENTUM", positionSymbols);
                    List<String> types = new ArrayList<>(m.getTags());
                    card.put("cprTypes", types);
                    card.put("weeklyNarrow", false);
                    card.put("momentumTags", m.getTags());
                    card.put("volumeRatio", Math.round(m.getVolumeRatio() * 10.0) / 10.0);
                    card.put("marketCapCr", Math.round(m.getMarketCapCr()));
                    card.put("marketCapCategory", momentumService.getMarketCapCategory(m.getSymbol()));
                    result.add(card);
                    seen.add(fyers);
                }
            }
        }

        // Add market cap to all cards that don't have it yet
        for (var card : result) {
            if (!card.containsKey("marketCapCr")) {
                String fyers = card.get("symbol").toString();
                String ticker = fyers.replace("NSE:", "").replace("-EQ", "");
                card.put("marketCapCr", Math.round(momentumService.getMarketCap(ticker)));
                card.put("marketCapCategory", momentumService.getMarketCapCategory(ticker));
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

        // Current candle OHLC
        CandleAggregator.CandleBar currentCandle = candleAggregator.getCurrentCandle(fyersSymbol);
        if (currentCandle != null) {
            card.put("candleOpen", r(currentCandle.open));
            card.put("candleHigh", r(currentCandle.high));
            card.put("candleLow", r(currentCandle.low));
        }

        card.put("atp", Math.round(candleAggregator.getAtp(fyersSymbol) * 100.0) / 100.0);
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

        // Signal history
        List<BreakoutScanner.SignalInfo> history = breakoutScanner.getSignalHistory(fyersSymbol);
        List<Map<String, String>> histList = new ArrayList<>();
        for (BreakoutScanner.SignalInfo h : history) {
            Map<String, String> hm = new LinkedHashMap<>();
            hm.put("setup", h.setup);
            hm.put("time", h.time);
            hm.put("status", h.status);
            histList.add(hm);
        }
        card.put("signalHistory", histList);

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
        status.put("enableAtp", riskSettings.isEnableAtpCheck());
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

    @GetMapping("/api/scanner/simulate-qty")
    public Map<String, Object> simulateQty() {
        Map<String, Object> result = new LinkedHashMap<>();
        double riskPerTrade = riskSettings.getRiskPerTrade();
        double capitalPerTrade = riskSettings.getCapitalPerTrade();
        double atrMultiplier = riskSettings.getAtrMultiplier();
        int fixedQty = riskSettings.getFixedQuantity();

        result.put("riskPerTrade", riskPerTrade);
        result.put("capitalPerTrade", capitalPerTrade);
        result.put("atrMultiplier", atrMultiplier);
        result.put("fixedQuantity", fixedQty);

        List<Map<String, Object>> stocks = new ArrayList<>();
        for (String fyersSymbol : marketDataService.getWatchlist()) {
            String ticker = fyersSymbol.replaceAll("^(NSE|BSE|MCX):", "").replaceAll("-(EQ|INDEX)$", "");
            double ltp = candleAggregator.getLtp(fyersSymbol);
            if (ltp <= 0) {
                CprLevels cpr = bhavcopyService.getCprLevels(ticker);
                if (cpr != null) ltp = cpr.getClose();
            }
            double atr = atrService.getAtr(fyersSymbol);
            int leverage = marginDataService.getLeverage(fyersSymbol);

            Map<String, Object> s = new LinkedHashMap<>();
            s.put("symbol", ticker);
            s.put("ltp", r(ltp));
            s.put("atr", r(atr));
            s.put("leverage", leverage);

            if (fixedQty != -1) {
                int qty = Math.max(2, fixedQty % 2 != 0 ? fixedQty + 1 : fixedQty);
                s.put("qty", qty);
                s.put("mode", "FIXED");
                s.put("slDist", r(atr * atrMultiplier));
                s.put("riskQty", "--");
                s.put("capitalCapQty", "--");
                s.put("capitalUsed", r(ltp * qty));
                s.put("riskAmount", r(atr * atrMultiplier * qty));
            } else if (atr > 0 && ltp > 0) {
                double slDist = atr * atrMultiplier;
                int riskQty = (int) (riskPerTrade / slDist);
                double effectiveCapital = (capitalPerTrade * leverage) / 2.0;
                int capitalCapQty = (int) (effectiveCapital / ltp);
                int rawQty = Math.min(riskQty, capitalCapQty);
                int qty = Math.max(2, (rawQty / 2) * 2);
                boolean capped = riskQty > capitalCapQty;

                s.put("qty", qty);
                s.put("mode", capped ? "CAPITAL-CAPPED" : "RISK-BASED");
                s.put("slDist", r(slDist));
                s.put("riskQty", riskQty);
                s.put("capitalCapQty", capitalCapQty);
                s.put("capitalUsed", r(ltp * qty));
                s.put("riskAmount", r(slDist * qty));
            } else {
                s.put("qty", 2);
                s.put("mode", "MIN (no ATR)");
                s.put("slDist", 0);
                s.put("riskQty", 0);
                s.put("capitalCapQty", 0);
                s.put("capitalUsed", r(ltp * 2));
                s.put("riskAmount", 0);
            }
            stocks.add(s);
        }
        result.put("stocks", stocks);
        return result;
    }

    // ── MOMENTUM STOCKS ──────────────────────────────────────────────────────
    @GetMapping("/api/momentum-stocks")
    public Map<String, Object> getMomentumStocks() {
        Map<String, Object> result = new LinkedHashMap<>();
        var stocks = momentumService.getMomentumStocks();
        result.put("count", stocks.size());
        result.put("volumeThreshold", riskSettings.getMomentumVolumeMultiple());

        List<Map<String, Object>> list = new ArrayList<>();
        for (var m : stocks) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("symbol", m.getSymbol());
            item.put("close", m.getLastClose());
            item.put("volume", m.getLastVolume());
            item.put("avgVolume20", Math.round(m.getAvgVolume20()));
            item.put("volumeRatio", Math.round(m.getVolumeRatio() * 10.0) / 10.0);
            item.put("prevWeekHigh", m.getPrevWeekHigh());
            item.put("prevWeekLow", m.getPrevWeekLow());
            item.put("prevMonthHigh", m.getPrevMonthHigh());
            item.put("prevMonthLow", m.getPrevMonthLow());
            item.put("fiftyTwoWeekHigh", m.getFiftyTwoWeekHigh());
            item.put("fiftyTwoWeekLow", m.getFiftyTwoWeekLow());
            item.put("marketCapCr", Math.round(m.getMarketCapCr()));
            item.put("marketCapCategory", momentumService.getMarketCapCategory(m.getSymbol()));
            item.put("tags", m.getTags());
            list.add(item);
        }
        // Sort by volume ratio descending
        list.sort((a, b) -> Double.compare((double) b.get("volumeRatio"), (double) a.get("volumeRatio")));
        result.put("stocks", list);
        return result;
    }
}
