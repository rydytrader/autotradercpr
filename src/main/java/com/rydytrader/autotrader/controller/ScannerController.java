package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.*;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
    private final HtfEmaService htfEmaService;
    private final IndexTrendService indexTrendService;
    private final MarketHolidayService marketHolidayService;

    public ScannerController(MarketDataService marketDataService,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             WeeklyCprService weeklyCprService,
                             CandleAggregator candleAggregator,
                             BreakoutScanner breakoutScanner,
                             RiskSettingsStore riskSettings,
                             MarginDataService marginDataService,
                             TradeHistoryService tradeHistoryService,
                             EmaService emaService,
                             HtfEmaService htfEmaService,
                             IndexTrendService indexTrendService,
                             MarketHolidayService marketHolidayService) {
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
        this.htfEmaService = htfEmaService;
        this.indexTrendService = indexTrendService;
        this.marketHolidayService = marketHolidayService;
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

        // Collect narrow CPR stocks — use configurable width threshold + filters + NS/NL toggles
        double narrowMaxWidth = riskSettings.getNarrowCprMaxWidth();
        Set<String> seen = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getAllCprLevels().values()) {
            if (cpr.getCprWidthPct() >= narrowMaxWidth) continue;
            if (!marketDataService.passesWatchlistFilters(cpr)) continue;
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
            card.put("rangeAdrPct", cpr.getRangeAdrPct());
            result.add(card);
            seen.add(fyers);
        }

        // Collect inside-only CPR stocks — filtered by IS/IL toggles + width filter + price filter
        double insideMaxWidth = riskSettings.getInsideCprMaxWidth();
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            if (seen.contains(fyers)) continue;
            if (insideMaxWidth > 0 && cpr.getCprWidthPct() > insideMaxWidth) continue;
            if (!marketDataService.passesWatchlistFilters(cpr)) continue;
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
            card.put("rangeAdrPct", cpr.getRangeAdrPct());
            result.add(card);
            seen.add(fyers);
        }

        // Default sort alphabetically by symbol for stable scanner ordering
        result.sort((a, b) -> String.valueOf(a.get("symbol")).compareTo(String.valueOf(b.get("symbol"))));
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
        card.put("ema50", Math.round(emaService.getEma50(fyersSymbol) * 100.0) / 100.0);
        card.put("ema200", Math.round(emaService.getEma200(fyersSymbol) * 100.0) / 100.0);
        // Classify EMA 20/50 pattern over recent candles: BRAIDED (zigzag/choppy), RAILWAY (parallel/trending), or ""
        double atrVal = atrService.getAtr(fyersSymbol);
        String emaPattern = emaService.getEmaPattern(fyersSymbol,
            riskSettings.getEmaPatternLookback(),
            atrVal,
            riskSettings.getBraidedMinCrossovers(),
            riskSettings.getBraidedMaxSpreadAtr(),
            riskSettings.getRailwayMaxCv(),
            riskSettings.getRailwayMinSpreadAtr());
        card.put("emaPattern", emaPattern);
        // HTF (75-min) EMAs — display only, long-term trend
        double htfEma20 = htfEmaService.getEma(fyersSymbol);
        double htfEma50 = htfEmaService.getEma50(fyersSymbol);
        double htfEma200 = htfEmaService.getEma200(fyersSymbol);
        card.put("htfEma20", Math.round(htfEma20 * 100.0) / 100.0);
        card.put("htfEma50", Math.round(htfEma50 * 100.0) / 100.0);
        card.put("htfEma200", Math.round(htfEma200 * 100.0) / 100.0);
        String htfPattern = (htfEma20 > 0 && htfEma50 > 0)
            ? htfEmaService.getEmaPattern(fyersSymbol,
                riskSettings.getEmaPatternLookback(),
                atrVal,
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr())
            : "";
        card.put("htfEmaPattern", htfPattern);
        // EMA-trend reference price: last completed candle close, LTP fallback for the
        // first candle of the day. Mirrors how WeeklyCprService computes daily/weekly trend.
        card.put("price5m",  Math.round(weeklyCprService.getDailyPrice(fyersSymbol)  * 100.0) / 100.0);
        card.put("price60m", Math.round(weeklyCprService.getWeeklyPrice(fyersSymbol) * 100.0) / 100.0);
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
        card.put("weeklyReversalActive", !"NONE".equals(weeklyCprService.getWeeklyRejection(fyersSymbol)));
        card.put("dailyTrend", weeklyCprService.getDailyTrend(fyersSymbol));
        card.put("probability", computeCardProbability(fyersSymbol, ltp));

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
            sigMap.put("detail", sig.detail != null ? sig.detail : "");
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
            hm.put("detail", h.detail != null ? h.detail : "");
            histList.add(hm);
        }
        card.put("signalHistory", histList);

        card.put("hasPosition", positionSymbols.contains(fyersSymbol));
        card.put("cprWidthPct", Math.round(levels.getCprWidthPct() * 1000.0) / 1000.0);

        return card;
    }

    private double r(double v) { return Math.round(v * 100.0) / 100.0; }

    private Map<String, Object> barToMap(CandleAggregator.CandleBar c, boolean forming) {
        Map<String, Object> bar = new LinkedHashMap<>();
        bar.put("t", c.epochSec * 1000L);
        bar.put("o", r(c.open));
        bar.put("h", r(c.high));
        bar.put("l", r(c.low));
        bar.put("c", r(c.close));
        bar.put("v", c.volume);
        if (forming) bar.put("forming", true);
        return bar;
    }

    private Map<String, Object> point(long tMs, double value) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("t", tMs);
        p.put("v", r(value));
        return p;
    }

    private void addIndicatorPoints(CandleAggregator.CandleBar c,
                                    List<Map<String, Object>> vwapSeries,
                                    List<Map<String, Object>> ema20Series,
                                    List<Map<String, Object>> ema200Series) {
        long tMs = c.epochSec * 1000L;
        if (c.vwap > 0) vwapSeries.add(point(tMs, c.vwap));
        if (c.ema20 > 0) ema20Series.add(point(tMs, c.ema20));
        if (c.ema200 > 0) ema200Series.add(point(tMs, c.ema200));
    }

    private void computeIndicatorsAndBuild(List<CandleAggregator.CandleBar> history,
                                           java.time.LocalDate displayDate,
                                           java.time.ZoneId ist,
                                           List<Map<String, Object>> candleList,
                                           List<Map<String, Object>> vwapSeries,
                                           List<Map<String, Object>> ema20Series,
                                           List<Map<String, Object>> ema200Series) {
        final double k20 = 2.0 / 21.0;
        final double k200 = 2.0 / 201.0;
        double ema20 = 0, ema200 = 0;
        boolean firstBar = true;
        double dayCumPV = 0, dayCumV = 0;
        java.time.LocalDate curDay = null;
        for (CandleAggregator.CandleBar c : history) {
            java.time.LocalDate d = java.time.Instant.ofEpochSecond(c.epochSec).atZone(ist).toLocalDate();
            if (!d.equals(curDay)) {
                dayCumPV = 0;
                dayCumV = 0;
                curDay = d;
            }
            double tp = (c.high + c.low + c.close) / 3.0;
            if (c.volume > 0) {
                dayCumPV += tp * c.volume;
                dayCumV += c.volume;
            }
            double vwap = dayCumV > 0 ? dayCumPV / dayCumV : c.close;
            if (firstBar) {
                ema20 = c.close;
                ema200 = c.close;
                firstBar = false;
            } else {
                ema20 = c.close * k20 + ema20 * (1 - k20);
                ema200 = c.close * k200 + ema200 * (1 - k200);
            }
            if (d.equals(displayDate)) {
                candleList.add(barToMap(c, false));
                long tMs = c.epochSec * 1000L;
                vwapSeries.add(point(tMs, vwap));
                ema20Series.add(point(tMs, ema20));
                ema200Series.add(point(tMs, ema200));
            }
        }
    }

    /**
     * Compute card-level probability preview using all pre-checkable conditions (8 of 10).
     * More accurate than the basic weekly+daily check — includes EMA, VWAP, crossover, NIFTY.
     * Direction derived from daily trend (bullish → buy preview, bearish → sell preview).
     */
    private String computeCardProbability(String symbol, double ltp) {
        String daily = weeklyCprService.getDailyTrend(symbol);
        boolean isBuy = daily.contains("BULLISH");
        boolean isSell = daily.contains("BEARISH");
        if (!isBuy && !isSell) return "--"; // daily neutral — direction unknown

        // 1-2. Weekly + daily alignment
        String baseProb = weeklyCprService.getProbabilityForDirection(symbol, isBuy);
        if (!"HPT".equals(baseProb)) return baseProb; // already LPT or SKIP from weekly

        // 3. 5-min EMA trend alignment (price above/below all of EMA 20/50/200)
        double ema    = emaService.getEma(symbol);
        double ema50  = emaService.getEma50(symbol);
        double ema200 = emaService.getEma200(symbol);
        if (riskSettings.isEnableEmaTrendCheck() && ema > 0 && ema50 > 0 && ema200 > 0) {
            boolean bullishAligned = ltp > ema && ltp > ema50 && ltp > ema200;
            boolean bearishAligned = ltp < ema && ltp < ema50 && ltp < ema200;
            if (isBuy  && !bullishAligned) return "LPT";
            if (isSell && !bearishAligned) return "LPT";
        }

        // 4. VWAP/ATP
        double atp = candleAggregator.getAtp(symbol);
        if (riskSettings.isEnableAtpCheck() && atp > 0) {
            if ((isBuy && ltp < atp) || (isSell && ltp > atp)) return "LPT";
        }

        // 5. NIFTY alignment — opposition no longer downgrades probability. The trade
        // fires at its original probability with a qty factor applied by SignalProcessor
        // (indexOpposedQtyFactor, default 0.75). Hard-skip mode still blocks at the
        // scanner level, but that's handled in BreakoutScanner — not surfaced here
        // since classifyProbability is a card-display forecast that assumes the trade fires.

        return "HPT"; // all pre-checkable gates passed
    }

    @GetMapping("/api/scanner/status")
    public Map<String, Object> getScannerStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("signalSource", riskSettings.getSignalSource());
        status.put("watchlistCount", marketDataService.getWatchlist().size());
        status.put("atrLoaded", atrService.getLoadedCount());
        status.put("emaLoaded", emaService.getLoadedCount());
        status.put("ema200Loaded", emaService.getEma200LoadedCount());
        status.put("firstCandleLoaded", candleAggregator.getFirstCandleCloseCount());
        status.put("validationPass", marketDataService.getValidationPass());
        status.put("validationFail", marketDataService.getValidationFail());
        status.put("validationTotal", marketDataService.getValidationTotal());
        status.put("timeframe", riskSettings.getScannerTimeframe());
        status.put("higherTimeframe", riskSettings.getHigherTimeframe());
        status.put("enableHpt", riskSettings.isEnableHpt());
        status.put("enableLpt", riskSettings.isEnableLpt());
        status.put("enableAtp", riskSettings.isEnableAtpCheck());
        status.put("enableEmaTrend", riskSettings.isEnableEmaTrendCheck());
        status.put("minPrice", riskSettings.getScanMinPrice());
        status.put("maxPrice", riskSettings.getScanMaxPrice());
        status.put("minTurnover", riskSettings.getScanMinTurnover());
        status.put("minVolume", riskSettings.getScanMinVolume());
        status.put("minBeta", riskSettings.getScanMinBeta());
        status.put("maxBeta", riskSettings.getScanMaxBeta());
        status.put("capFilter", riskSettings.getScanCapFilter());
        status.put("narrowMaxWidth", riskSettings.getNarrowCprMaxWidth());
        status.put("insideMaxWidth", riskSettings.getInsideCprMaxWidth());
        return status;
    }

    @GetMapping("/api/scanner/chart")
    public Map<String, Object> getChartData(@RequestParam String symbol) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("symbol", symbol);

        boolean tradingDay = marketHolidayService.isTradingDay();
        List<Map<String, Object>> candleList = new ArrayList<>();
        List<Map<String, Object>> vwapSeries = new ArrayList<>();
        List<Map<String, Object>> ema20Series = new ArrayList<>();
        List<Map<String, Object>> ema200Series = new ArrayList<>();

        if (tradingDay) {
            // Live path: compute indicators progressively over prior-day warmup + today so
            // every today's bar has a fully-warmed-up EMA (bar 1 included). Relying on the
            // per-bar snapshots left 0s on bars seeded before EmaService.onCandleClose fired
            // (e.g. cold-start full-fetch bars, mid-day restart catch-up).
            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            java.time.LocalDate today = java.time.LocalDate.now(ist);
            List<CandleAggregator.CandleBar> priors = candleAggregator.getPriorDayCandles(symbol);
            List<CandleAggregator.CandleBar> todays  = candleAggregator.getCompletedCandles(symbol);
            List<CandleAggregator.CandleBar> merged = new ArrayList<>();
            if (priors != null) merged.addAll(priors);
            if (todays != null) merged.addAll(todays);
            computeIndicatorsAndBuild(merged, today, ist, candleList, vwapSeries, ema20Series, ema200Series);

            // Forming (current, still-open) candle — append with live indicator values
            CandleAggregator.CandleBar current = candleAggregator.getCurrentCandle(symbol);
            if (current != null && current.open > 0) {
                candleList.add(barToMap(current, true));
                long tMs = current.epochSec * 1000L;
                double liveVwap = candleAggregator.getAtp(symbol);
                double liveEma20 = emaService.getEma(symbol);
                double liveEma200 = emaService.getEma200(symbol);
                if (liveVwap > 0) vwapSeries.add(point(tMs, liveVwap));
                if (liveEma20 > 0) ema20Series.add(point(tMs, liveEma20));
                if (liveEma200 > 0) ema200Series.add(point(tMs, liveEma200));
            }
            result.put("dataSource", "live");
        } else {
            // Non-trading day: fetch multi-day historical, compute indicators progressively, show most recent trading day
            try {
                List<CandleAggregator.CandleBar> hist = atrService.fetchTodayCandles(symbol);
                if (!hist.isEmpty()) {
                    java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
                    java.time.LocalDate latestDate = null;
                    for (CandleAggregator.CandleBar c : hist) {
                        java.time.LocalDate d = java.time.Instant.ofEpochSecond(c.epochSec).atZone(ist).toLocalDate();
                        if (latestDate == null || d.isAfter(latestDate)) latestDate = d;
                    }
                    if (latestDate != null) {
                        // Compute indicators progressively (Fyers API doesn't return them)
                        computeIndicatorsAndBuild(hist, latestDate, ist, candleList, vwapSeries, ema20Series, ema200Series);
                        result.put("dataDate", latestDate.toString());
                    }
                }
                result.put("dataSource", "historical");
            } catch (Exception e) {
                result.put("dataSource", "error");
                result.put("error", e.getMessage());
            }
        }
        result.put("candles", candleList);
        result.put("vwapSeries", vwapSeries);
        result.put("ema20Series", ema20Series);
        result.put("ema200Series", ema200Series);
        result.put("tradingDay", tradingDay);

        // CPR levels:
        //   Live (trading day): use cache — it's the CPR active for today (computed from yesterday's OHLC)
        //   Historical: use getPreviousCpr — cache is "next day's CPR", we want the CPR that was active
        //   on the historical trading day we're displaying.
        CprLevels lv = tradingDay ? bhavcopyService.getCprLevels(symbol) : bhavcopyService.getPreviousCpr(symbol);
        if (lv != null) {
            Map<String, Object> cpr = new LinkedHashMap<>();
            cpr.put("top", r(Math.max(lv.getTc(), lv.getBc())));
            cpr.put("pivot", r(lv.getPivot()));
            cpr.put("bottom", r(Math.min(lv.getTc(), lv.getBc())));
            cpr.put("r1", r(lv.getR1()));
            cpr.put("r2", r(lv.getR2()));
            cpr.put("r3", r(lv.getR3()));
            cpr.put("r4", r(lv.getR4()));
            cpr.put("s1", r(lv.getS1()));
            cpr.put("s2", r(lv.getS2()));
            cpr.put("s3", r(lv.getS3()));
            cpr.put("s4", r(lv.getS4()));
            cpr.put("pdh", r(lv.getPh()));
            cpr.put("pdl", r(lv.getPl()));
            result.put("cpr", cpr);
        }

        // Indicators (current values)
        result.put("ltp", r(candleAggregator.getLtp(symbol)));
        result.put("vwap", r(candleAggregator.getAtp(symbol)));
        result.put("ema20", r(emaService.getEma(symbol)));
        result.put("ema200", r(emaService.getEma200(symbol)));

        // Opening Range (high/low for the OR window, plus time bounds)
        Map<String, Object> or = new LinkedHashMap<>();
        int orMinutes = riskSettings.getOpeningRangeMinutes();
        double orHigh = 0, orLow = 0;
        java.time.ZoneId orIst = java.time.ZoneId.of("Asia/Kolkata");
        java.time.LocalDate orDate = null;
        if (tradingDay) {
            orHigh = candleAggregator.getOpeningRangeHigh(symbol);
            orLow = candleAggregator.getOpeningRangeLow(symbol);
            orDate = java.time.LocalDate.now(orIst);
        } else {
            // Historical: compute OR from display day's first N candles
            if (!candleList.isEmpty()) {
                try {
                    long firstTs = (long) candleList.get(0).get("t");
                    orDate = java.time.Instant.ofEpochMilli(firstTs).atZone(orIst).toLocalDate();
                    int orEndMin = com.rydytrader.autotrader.service.MarketHolidayService.MARKET_OPEN_MINUTE + orMinutes;
                    double hi = 0, lo = Double.MAX_VALUE;
                    for (Map<String, Object> bar : candleList) {
                        long tMs = (long) bar.get("t");
                        java.time.LocalTime lt = java.time.Instant.ofEpochMilli(tMs).atZone(orIst).toLocalTime();
                        int barMin = lt.getHour() * 60 + lt.getMinute();
                        if (barMin >= com.rydytrader.autotrader.service.MarketHolidayService.MARKET_OPEN_MINUTE
                            && barMin < orEndMin) {
                            double h = ((Number) bar.get("h")).doubleValue();
                            double l = ((Number) bar.get("l")).doubleValue();
                            if (h > hi) hi = h;
                            if (l < lo) lo = l;
                        }
                    }
                    if (hi > 0 && lo < Double.MAX_VALUE) { orHigh = hi; orLow = lo; }
                } catch (Exception ignored) {}
            }
        }
        if (orHigh > 0 && orLow > 0 && orDate != null && orMinutes > 0) {
            long orStartSec = orDate.atTime(9, 15).atZone(orIst).toEpochSecond();
            long orEndSec = orStartSec + (orMinutes * 60L);
            or.put("high", r(orHigh));
            or.put("low", r(orLow));
            or.put("startMs", orStartSec * 1000L);
            or.put("endMs", orEndSec * 1000L);
            result.put("or", or);
        }

        // Trades for this symbol (today's trades only, for live mode)
        List<Map<String, Object>> trades = new ArrayList<>();
        if (tradingDay) {
            for (com.rydytrader.autotrader.dto.TradeRecord tr : tradeHistoryService.getTrades()) {
                if (!symbol.equals(tr.getSymbol())) continue;
                Map<String, Object> t = new LinkedHashMap<>();
                t.put("setup", tr.getSetup());
                t.put("side", tr.getSide());
                t.put("entryPrice", tr.getEntryPrice());
                t.put("exitPrice", tr.getExitPrice());
                t.put("exitReason", tr.getExitReason());
                t.put("netPnl", tr.getNetPnl());
                t.put("qty", tr.getQty());
                // exit time from timestamp (format "HH:mm:ss")
                t.put("exitTime", timeToEpochMs(tr.getTimestamp()));
                // entry time — try to parse from description first line like "HH:mm:ss [ENTRY]"
                t.put("entryTime", extractEntryTimeMs(tr.getDescription(), tr.getTimestamp()));
                trades.add(t);
            }
        }
        result.put("trades", trades);

        // Timeframe minutes (for client to know candle duration)
        result.put("timeframeMinutes", riskSettings.getScannerTimeframe());
        return result;
    }

    private long timeToEpochMs(String hms) {
        if (hms == null || hms.isEmpty()) return 0L;
        try {
            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            java.time.LocalDate today = java.time.LocalDate.now(ist);
            java.time.LocalTime t = java.time.LocalTime.parse(hms);
            return today.atTime(t).atZone(ist).toEpochSecond() * 1000L;
        } catch (Exception e) {
            return 0L;
        }
    }

    private long extractEntryTimeMs(String description, String fallbackTimestamp) {
        if (description != null) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\d{2}:\\d{2}:\\d{2})\\s+\\[ENTRY\\]").matcher(description);
            if (m.find()) return timeToEpochMs(m.group(1));
        }
        return timeToEpochMs(fallbackTimestamp);
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

    @GetMapping("/api/scanner/fyers-watchlist")
    public ResponseEntity<String> getFyersWatchlist() {
        // Export in Fyers import format: one symbol per line, e.g. "NSE:RELIANCE-EQ"
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> card : getWatchlist()) {
            Object sym = card.get("symbol");
            if (sym != null) {
                sb.append(sym.toString()).append("\n");
            }
        }
        String filename = "fyers-watchlist-" + java.time.LocalDate.now() + ".txt";
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
            .contentType(MediaType.TEXT_PLAIN)
            .body(sb.toString());
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

    @PostMapping("/api/scanner/rebuild")
    public Map<String, Object> rebuildWatchlist() {
        int count = marketDataService.rebuildWatchlist();
        return Map.of("ok", true, "watchlistCount", count);
    }

}
