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
    private final TradeHistoryService tradeHistoryService;
    private final EmaService emaService;

    public ScannerController(MarketDataService marketDataService,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             WeeklyCprService weeklyCprService,
                             CandleAggregator candleAggregator,
                             BreakoutScanner breakoutScanner,
                             RiskSettingsStore riskSettings,
                             MarginDataService marginDataService,
                             TradeHistoryService tradeHistoryService,
                             EmaService emaService) {
        this.marketDataService = marketDataService;
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.candleAggregator = candleAggregator;
        this.breakoutScanner = breakoutScanner;
        this.riskSettings = riskSettings;
        this.marginDataService = marginDataService;
        this.tradeHistoryService = tradeHistoryService;
        this.emaService = emaService;
    }

    @GetMapping("/api/scanner/watchlist")
    public List<Map<String, Object>> getWatchlist() {
        List<Map<String, Object>> result = new ArrayList<>();
        Set<String> positionSymbols = PositionManager.getAllSymbols();

        // Build set of inside CPR symbols for cross-referencing
        Set<String> insideSymbols = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            insideSymbols.add(cpr.getSymbol());
        }

        // Collect narrow CPR stocks — use configurable width threshold + price filter + NS/NL toggles
        double narrowMaxWidth = riskSettings.getNarrowCprMaxWidth();
        double minPrice = riskSettings.getScanMinPrice();
        Set<String> seen = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getAllCprLevels().values()) {
            if (cpr.getCprWidthPct() >= narrowMaxWidth) continue; // not narrow enough
            if (minPrice > 0 && cpr.getClose() < minPrice) continue; // price too low
            String nrt = cpr.getNarrowRangeType();
            boolean rangeMatches = ("SMALL".equals(nrt) && riskSettings.isScanIncludeNS())
                                || ("LARGE".equals(nrt) && riskSettings.isScanIncludeNL())
                                || (nrt == null && (riskSettings.isScanIncludeNS() || riskSettings.isScanIncludeNL()));
            if (!rangeMatches) continue;

            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            List<String> types = new ArrayList<>();
            types.add("NARROW");
            if (insideSymbols.contains(cpr.getSymbol())) types.add("INSIDE");
            Map<String, Object> card = buildCard(fyers, cpr, "NARROW", positionSymbols);
            card.put("cprTypes", types);
            card.put("narrowRangeType", nrt);
            card.put("rangeZScore", cpr.getRangeZScore());
            result.add(card);
            seen.add(fyers);
        }

        // Collect inside-only CPR stocks — filtered by IS/IL toggles + width filter + price filter
        double insideMaxWidth = riskSettings.getInsideCprMaxWidth();
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            if (seen.contains(fyers)) continue;
            if (minPrice > 0 && cpr.getClose() < minPrice) continue; // price too low
            // Width filter: skip if CPR width exceeds threshold (0 = no filter)
            if (insideMaxWidth > 0 && cpr.getCprWidthPct() > insideMaxWidth) continue;
            String nrt = cpr.getNarrowRangeType();
            boolean rangeMatches = ("SMALL".equals(nrt) && riskSettings.isScanIncludeIS())
                                || ("LARGE".equals(nrt) && riskSettings.isScanIncludeIL())
                                || (nrt == null && (riskSettings.isScanIncludeIS() || riskSettings.isScanIncludeIL()));
            if (!rangeMatches) continue;

            List<String> types = new ArrayList<>();
            types.add("INSIDE");
            Map<String, Object> card = buildCard(fyers, cpr, "INSIDE", positionSymbols);
            card.put("cprTypes", types);
            card.put("narrowRangeType", nrt);
            card.put("rangeZScore", cpr.getRangeZScore());
            result.add(card);
            seen.add(fyers);
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
        card.put("ema20", Math.round(emaService.getEma(fyersSymbol) * 100.0) / 100.0);
        card.put("dayOpen", Math.round(candleAggregator.getDayOpen(fyersSymbol) * 100.0) / 100.0);

        // Open classification: IV (Inside Value), OV (Outside Value), EV (Extended Value)
        double firstClose = candleAggregator.getFirstCandleClose(fyersSymbol);
        String openClass = null;
        if (firstClose > 0) {
            double r1 = levels.getR1(), r2 = levels.getR2();
            double s1 = levels.getS1(), s2 = levels.getS2();
            double pdh = levels.getPh(), pdl = levels.getPl();
            double upperBound = Math.max(r1, pdh);
            double lowerBound = Math.min(s1, pdl);
            if (firstClose >= r2 || firstClose <= s2) {
                openClass = "EV";
            } else if (firstClose > upperBound || firstClose < lowerBound) {
                openClass = "OV";
            } else {
                openClass = "IV";
            }
        }
        card.put("openClass", openClass);

        card.put("candleVolume", candleAggregator.getCurrentCandleVolume(fyersSymbol));
        card.put("avgVolume", Math.round(candleAggregator.getAvgVolume(fyersSymbol, riskSettings.getVolumeLookback())));
        card.put("weeklyTrend", weeklyCprService.getWeeklyTrend(fyersSymbol));
        card.put("dailyTrend", weeklyCprService.getDailyTrend(fyersSymbol));
        card.put("probability", weeklyCprService.getProbability(fyersSymbol));

        // Opening Range status
        String orStatus = null;
        if (riskSettings.getOpeningRangeMinutes() > 0) {
            double orHigh = candleAggregator.getOpeningRangeHigh(fyersSymbol);
            double orLow  = candleAggregator.getOpeningRangeLow(fyersSymbol);
            boolean orLocked = candleAggregator.isOpeningRangeLocked(fyersSymbol);
            if (!orLocked) {
                orStatus = "FORMING";
            } else if (orHigh > 0 && orLow > 0) {
                if (ltp > orHigh) orStatus = "BULLISH";
                else if (ltp < orLow) orStatus = "BEARISH";
                else orStatus = "NEUTRAL";
            }
            card.put("orHigh", r(orHigh));
            card.put("orLow", r(orLow));
        }
        card.put("orStatus", orStatus);

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

    @GetMapping("/api/scanner/tv-watchlist")
    public ResponseEntity<String> getTvWatchlist() {
        // Export exactly what's shown on the Watchlist page (same filters applied)
        StringBuilder csv = new StringBuilder();
        for (Map<String, Object> card : getWatchlist()) {
            Object sym = card.get("symbol");
            if (sym != null) {
                String s = sym.toString().replaceAll("-EQ$", "").replace("-", "_");
                csv.append(s).append(",");
            }
        }
        String filename = "watchlist-" + java.time.LocalDate.now() + ".txt";
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

}
