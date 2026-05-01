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
    private final SmaService smaService;
    private final HtfSmaService htfSmaService;
    private final IndexTrendService indexTrendService;
    private final MarketHolidayService marketHolidayService;
    @org.springframework.beans.factory.annotation.Autowired
    private SymbolMasterService symbolMasterService;

    public ScannerController(MarketDataService marketDataService,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             WeeklyCprService weeklyCprService,
                             CandleAggregator candleAggregator,
                             BreakoutScanner breakoutScanner,
                             RiskSettingsStore riskSettings,
                             MarginDataService marginDataService,
                             TradeHistoryService tradeHistoryService,
                             SmaService smaService,
                             HtfSmaService htfSmaService,
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
        this.smaService = smaService;
        this.htfSmaService = htfSmaService;
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

        // Universe is restricted to NIFTY 50 at the bhavcopy parse stage — cache already
        // contains only NIFTY 50 stocks (full FNO fallback if NIFTY 50 list was unavailable).

        // Collect narrow CPR stocks — use configurable width range + filters + NS/NL toggles
        double narrowMaxWidth = riskSettings.getNarrowCprMaxWidth();
        double narrowMinWidth = riskSettings.getNarrowCprMinWidth();
        Set<String> seen = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getAllCprLevels().values()) {
            if (bhavcopyService.isIndex(cpr.getSymbol())) continue; // NIFTY50/NIFTYBANK etc.
            if (cpr.getCprWidthPct() < narrowMinWidth || cpr.getCprWidthPct() >= narrowMaxWidth) continue;
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
        // Universe membership flags drive the scanner-page client-side N50 vs All filter.
        card.put("inNifty50", levels.isInNifty50());
        card.put("inNifty100", levels.isInNifty100());

        // LTP separated into two values:
        //   liveTickLtp — the LTP from today's WS ticks (0 if none, e.g. pre-market new day).
        //                 Both source maps already apply a stale-day guard so yesterday's
        //                 cached LTP doesn't bleed in.
        //   ltp         — internal value used downstream (probability, OR status). Falls back
        //                 to bhavcopy prev close so HPT/LPT logic has a non-zero reference.
        // The card displays liveTickLtp directly: pre-market on a new day → "0.00", once
        // ticks start arriving → today's actual price.
        double liveTickLtp = candleAggregator.getLtp(fyersSymbol);
        if (liveTickLtp <= 0) liveTickLtp = marketDataService.getLtp(fyersSymbol);
        double ltp = liveTickLtp > 0 ? liveTickLtp : levels.getClose();
        double changePct = candleAggregator.getChangePct(fyersSymbol);
        if (changePct == 0) changePct = marketDataService.getChangePercent(fyersSymbol);
        card.put("ltp", Math.round(liveTickLtp * 100.0) / 100.0);
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
        double tickSize = symbolMasterService != null ? symbolMasterService.getTickSize(fyersSymbol) : 0;
        card.put("sma20",  roundToTick(smaService.getSma(fyersSymbol),    tickSize));
        card.put("sma50",  roundToTick(smaService.getSma50(fyersSymbol),  tickSize));
        card.put("sma200", roundToTick(smaService.getSma200(fyersSymbol), tickSize));
        // Classify SMA 20/50 pattern over recent candles: BRAIDED (zigzag/choppy), RAILWAY (parallel/trending), or ""
        double atrVal = atrService.getAtr(fyersSymbol);
        String smaPattern = smaService.getSmaPattern(fyersSymbol,
            riskSettings.getSmaPatternLookback(),
            atrVal,
            riskSettings.getBraidedMinCrossovers(),
            riskSettings.getBraidedMaxSpreadAtr(),
            riskSettings.getRailwayMaxCv(),
            riskSettings.getRailwayMinSpreadAtr());
        card.put("smaPattern", smaPattern);
        // HTF (60-min) SMAs — long-term trend
        double htfSma20 = htfSmaService.getSma(fyersSymbol);
        double htfSma50 = htfSmaService.getSma50(fyersSymbol);
        double htfSma200 = htfSmaService.getSma200(fyersSymbol);
        card.put("htfSma20",  roundToTick(htfSma20,  tickSize));
        card.put("htfSma50",  roundToTick(htfSma50,  tickSize));
        card.put("htfSma200", roundToTick(htfSma200, tickSize));
        String htfPattern = (htfSma20 > 0 && htfSma50 > 0)
            ? htfSmaService.getSmaPattern(fyersSymbol,
                riskSettings.getSmaPatternLookback(),
                atrVal,
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr())
            : "";
        card.put("htfSmaPattern", htfPattern);
        // SMA-trend reference price: last completed candle close, LTP fallback for the
        // first candle of the day. Mirrors how WeeklyCprService computes daily/weekly trend.
        card.put("price5m",  Math.round(weeklyCprService.getDailyPrice(fyersSymbol)  * 100.0) / 100.0);
        card.put("price60m", Math.round(weeklyCprService.getWeeklyPrice(fyersSymbol) * 100.0) / 100.0);
        // "Day Open" on stock card = the open print (close of first 5-min candle), NOT the
        // 9:15 auction open. Open print is the price the day "settles" into and drives the
        // IV/OV/EV classification, so it's the more meaningful reference for traders.
        // Falls back to actual day open before 9:20 (firstCandleClose not yet available).
        double openPrint = candleAggregator.getFirstCandleClose(fyersSymbol);
        if (openPrint <= 0) openPrint = candleAggregator.getDayOpen(fyersSymbol);
        card.put("dayOpen", Math.round(openPrint * 100.0) / 100.0);

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

    /** Round derived prices (SMAs etc.) to the symbol's tick size for display.
     *  Filter math still uses the raw value via SmaService / HtfSmaService. */
    private static double roundToTick(double value, double tick) {
        if (tick <= 0) return Math.round(value * 100.0) / 100.0;
        return Math.round(value / tick) * tick;
    }

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
                                    List<Map<String, Object>> sma20Series,
                                    List<Map<String, Object>> sma200Series) {
        long tMs = c.epochSec * 1000L;
        if (c.vwap > 0) vwapSeries.add(point(tMs, c.vwap));
        if (c.sma20 > 0) sma20Series.add(point(tMs, c.sma20));
        if (c.sma200 > 0) sma200Series.add(point(tMs, c.sma200));
    }

    private void computeIndicatorsAndBuild(List<CandleAggregator.CandleBar> history,
                                           java.time.LocalDate displayDate,
                                           java.time.ZoneId ist,
                                           List<Map<String, Object>> candleList,
                                           List<Map<String, Object>> vwapSeries,
                                           List<Map<String, Object>> sma20Series,
                                           List<Map<String, Object>> sma50Series,
                                           List<Map<String, Object>> sma200Series) {
        // Rolling-window SMA: keep last N closes and average. Walks the full merged history
        // (prior-day warmup + today) so each plotted point has a correctly-warmed-up SMA.
        java.util.ArrayDeque<Double> closes20 = new java.util.ArrayDeque<>();
        java.util.ArrayDeque<Double> closes50 = new java.util.ArrayDeque<>();
        java.util.ArrayDeque<Double> closes200 = new java.util.ArrayDeque<>();
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
            closes20.addLast(c.close);
            closes50.addLast(c.close);
            closes200.addLast(c.close);
            while (closes20.size() > 20) closes20.removeFirst();
            while (closes50.size() > 50) closes50.removeFirst();
            while (closes200.size() > 200) closes200.removeFirst();
            double sma20  = closes20.size()  == 20  ? avg(closes20)  : 0;
            double sma50  = closes50.size()  == 50  ? avg(closes50)  : 0;
            double sma200 = closes200.size() == 200 ? avg(closes200) : 0;
            if (d.equals(displayDate)) {
                candleList.add(barToMap(c, false));
                long tMs = c.epochSec * 1000L;
                vwapSeries.add(point(tMs, vwap));
                if (sma20  > 0) sma20Series.add(point(tMs, sma20));
                if (sma50  > 0) sma50Series.add(point(tMs, sma50));
                if (sma200 > 0) sma200Series.add(point(tMs, sma200));
            }
        }
    }

    private static double avg(java.util.Deque<Double> d) {
        double sum = 0;
        for (Double v : d) sum += v;
        return sum / d.size();
    }

    /**
     * Compute card-level probability preview using all pre-checkable conditions (8 of 10).
     * More accurate than the basic weekly+daily check — includes SMA, VWAP, crossover, NIFTY.
     * Direction derived from daily trend (bullish → buy preview, bearish → sell preview).
     */
    private String computeCardProbability(String symbol, double ltp) {
        String daily = weeklyCprService.getDailyTrend(symbol);
        boolean isBuy = daily.contains("BULLISH");
        boolean isSell = daily.contains("BEARISH");
        if (!isBuy && !isSell) return "--"; // daily neutral — direction unknown

        // Pure CPR alignment: LTF (5-min/LTP vs daily CPR) + HTF (weekly state).
        // HPT = both aligned, MPT = only one aligned, null = LTF opposed → "--".
        // SMA / VWAP / NIFTY filters intentionally NOT applied here — those are
        // execution-time gates in the real pipeline; the card forecast reflects
        // CPR alignment only.
        String baseProb = weeklyCprService.getProbabilityForDirection(symbol, isBuy);
        return baseProb != null ? baseProb : "--";
    }

    /**
     * EOD Analysis: post-mortem audit of every breakout signal today.
     * Returns the full signalHistory (TRADED + every FILTERED rejection with structured filterName)
     * either for one symbol or aggregated across all symbols, plus summary counts and a top-blockers map.
     */
    @GetMapping("/api/signal-trail")
    public Map<String, Object> getEodAnalysis(@RequestParam(required = false) String symbol) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("asOfDate", java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
            .toLocalDate().toString());

        Map<String, List<BreakoutScanner.SignalInfo>> all = breakoutScanner.getSignalHistoryAll();

        // Distinct list of symbols with at least one signal today (for the dropdown).
        List<String> symbols = new ArrayList<>(all.keySet());
        Collections.sort(symbols);
        result.put("symbols", symbols);
        result.put("selected", symbol != null && !symbol.isEmpty() ? symbol : "ALL");

        // Build the row list — either filtered to one symbol or flattened across all.
        List<Map<String, Object>> rows = new ArrayList<>();
        if (symbol != null && !symbol.isEmpty() && !"ALL".equalsIgnoreCase(symbol)) {
            for (BreakoutScanner.SignalInfo si : all.getOrDefault(symbol, Collections.emptyList())) {
                rows.add(buildEodRow(symbol, si));
            }
        } else {
            for (var entry : all.entrySet()) {
                String sym = entry.getKey();
                for (BreakoutScanner.SignalInfo si : entry.getValue()) {
                    rows.add(buildEodRow(sym, si));
                }
            }
        }
        // Sort by time ascending (HH:mm:ss strings sort lexicographically same as chronologically).
        rows.sort((a, b) -> String.valueOf(a.get("time")).compareTo(String.valueOf(b.get("time"))));
        // Stamp serial numbers post-sort.
        for (int i = 0; i < rows.size(); i++) rows.get(i).put("srNo", i + 1);
        result.put("rows", rows);

        // Summary: counts by status + map of filterName → count for the top-blockers chips.
        int traded = 0, filtered = 0, errors = 0;
        Map<String, Integer> byFilter = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            String st = String.valueOf(r.get("status"));
            if ("TRADED".equals(st)) traded++;
            else if ("FILTERED".equals(st)) filtered++;
            else if ("ERROR".equals(st)) errors++;
            String fn = String.valueOf(r.getOrDefault("filterName", ""));
            String detail = String.valueOf(r.getOrDefault("detail", ""));
            // Persisted entries from before the filterName capture work shipped have an empty
            // filterName but still status=FILTERED. Backfill from the detail text so they show
            // a real category (TRADING_HOURS / RISK_LIMIT / etc.) instead of UNKNOWN.
            if (fn.isEmpty() && ("FILTERED".equals(st) || "ERROR".equals(st))) {
                fn = classifyByDetail(detail);
                r.put("filterName", fn);
            }
            // SMA_TREND was overloaded before the split — entries whose detail says "not aligned"
            // were really alignment failures. Reclassify by the source-of-truth detail text.
            if ("SMA_TREND".equals(fn) && detail != null && detail.toLowerCase().contains("not aligned")) {
                fn = "SMA_ALIGNMENT";
                r.put("filterName", fn);
            }
            // LEVEL_COUNT was renamed to SMA_20_DISTANCE — reclassify legacy entries so they
            // show under the new chip name without needing a state-file rewrite.
            if ("LEVEL_COUNT".equals(fn)) {
                fn = "SMA_20_DISTANCE";
                r.put("filterName", fn);
            }
            // HTF filters renamed to mirror the 5-min naming (SMA_TREND / SMA_ALIGNMENT):
            //   HTF_SMA_ORDER → HTF_SMA_ALIGNMENT (stack ordering)
            //   HTF_ALIGNMENT → HTF_SMA_ALIGNMENT (intermediate rename)
            //   HTF_PRICE     → HTF_SMA_TREND     (LTP vs HTF SMAs)
            if ("HTF_SMA_ORDER".equals(fn) || "HTF_ALIGNMENT".equals(fn)) {
                fn = "HTF_SMA_ALIGNMENT";
                r.put("filterName", fn);
            }
            if ("HTF_PRICE".equals(fn)) {
                fn = "HTF_SMA_TREND";
                r.put("filterName", fn);
            }
            if (!fn.isEmpty()) byFilter.merge(fn, 1, Integer::sum);
        }
        // Sort byFilter descending by count (LinkedHashMap rebuild).
        Map<String, Integer> byFilterSorted = byFilter.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue,
                (a, b) -> a, LinkedHashMap::new));

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalSignals", rows.size());
        summary.put("traded", traded);
        summary.put("filtered", filtered);
        summary.put("errors", errors);
        summary.put("byFilter", byFilterSorted);
        result.put("summary", summary);
        return result;
    }

    /** Backfill heuristic for legacy SignalHistory entries without a filterName field, plus
     *  classifier for the full set of SignalProcessor rejection reasons. Order matters —
     *  composite "downgraded to LPT (X)" patterns inspect the inner X to surface the actual
     *  filter that triggered the downgrade chain. */
    private String classifyByDetail(String detail) {
        if (detail == null || detail.isEmpty()) return "UNKNOWN";
        String s = detail.toLowerCase();

        // Composite: "Probability downgraded to LPT (X) — LPT trades disabled". Drill into X.
        // Order matters: HTF SMA order is checked before HTF SMA not aligned because the order
        // string contains "not aligned" too — specificity ordering avoids false matches.
        if (s.contains("probability downgraded to lpt") || s.contains("→ lpt")) {
            if (s.contains("htf hurdle"))              return "HTF_HURDLE";
            if (s.contains("htf sma order"))           return "HTF_SMA_ALIGNMENT";
            if (s.contains("htf sma not aligned"))     return "HTF_SMA_TREND";
            if (s.contains("nifty opposed"))           return "NIFTY_OPPOSED";
            if (s.contains("inside-or"))               return "INSIDE_OR";
            if (s.contains("ev reversal"))             return "EV_REVERSAL";
            return "LPT_DISABLED";
        }

        // Order-layer (TradingController) gates
        if (s.contains("outside trading hours"))                  return "TRADING_HOURS";
        if (s.contains("risk exposure") || s.contains("daily loss")) return "RISK_LIMIT";
        if (s.contains("kill switch"))                             return "KILL_SWITCH";

        // Pre-trade structural gates
        if (s.contains("extended-level"))                          return "EXTENDED_LEVEL";
        if (s.contains("is inside") && s.contains("zone"))         return "DH_DL_ZONE";
        if (s.contains("invalid atr"))                             return "INVALID_ATR";
        if (s.contains("wrong side of entry"))                     return "WRONG_SIDE_TARGET";

        // Candle-shape & volume rejections
        if (s.contains("small candle"))                            return "SMALL_CANDLE";
        if (s.contains("opposite wick pressure"))                  return "OPPOSITE_WICK";
        if (s.contains("large candle body"))                       return "LARGE_CANDLE";
        if (s.contains("low volume"))                              return "LOW_VOLUME";

        // EV / OR gates
        if (s.contains("mean-reversion setup only allowed"))       return "MEAN_REVERSION_DAY";
        if (s.contains("opposes or break"))                        return "EV_OR_OPPOSED";
        if (s.contains("inside or range"))                         return "EV_OR_INSIDE";
        if (s.contains("ev ") && s.contains("detected"))           return "EV_GAP_OPPOSED";

        // Risk / reward / profit gates
        if (s.contains("risk/reward") || s.contains("risk\\reward")) return "RISK_REWARD";
        if (s.contains("absolute profit too low"))                 return "MIN_PROFIT";

        // Order placement
        if (s.contains("order failed") || s.contains("rejected by broker")) return "ORDER_FAILED";

        // Probability disable
        if (s.contains("hpt not enabled") || s.contains("lpt not enabled") || s.contains("mpt not enabled"))
            return "PROB_DISABLED";

        // BreakoutScanner-side filters
        if (s.contains("zone(s) away"))                            return "SMA_20_DISTANCE";
        if (s.contains("too far from broken zone"))                return "LEVEL_PROXIMITY";
        // Distinguish price-vs-SMA (SMA_TREND) from stack-ordering (SMA_ALIGNMENT) within
        // the unified "blocked by 5-min SMA trend" log line.
        if (s.contains("blocked by 5-min sma trend") && s.contains("not aligned")) return "SMA_ALIGNMENT";
        if (s.contains("not aligned (need 20"))                    return "SMA_ALIGNMENT";
        if (s.contains("blocked by 5-min sma trend"))              return "SMA_TREND";
        if (s.contains("close not above all smas") || s.contains("close not below all smas")) return "SMA_TREND";
        if (s.contains("below atp") || s.contains("above atp"))    return "ATP";
        if (s.contains("nifty") && s.contains("opposes"))          return "NIFTY_OPPOSED";

        // Pre-scanner gates that early-return before any setup detection
        if (s.contains("position") && s.contains("already open"))  return "POSITION_OPEN";
        if (s.contains("already traded today") || s.contains("level already traded")) return "LEVEL_BROKEN";

        return "UNKNOWN";
    }

    private Map<String, Object> buildEodRow(String symbol, BreakoutScanner.SignalInfo si) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("symbol", symbol);
        row.put("time", si.time != null ? si.time : "");
        row.put("setup", si.setup != null ? si.setup : "");
        row.put("status", si.status != null ? si.status : "");
        row.put("filterName", si.filterName != null ? si.filterName : "");
        row.put("price", Math.round(si.price * 100.0) / 100.0);
        row.put("detail", si.detail != null ? si.detail : "");
        return row;
    }

    @GetMapping("/api/scanner/status")
    public Map<String, Object> getScannerStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("tradingDay", marketHolidayService.isTradingDay());
        status.put("signalSource", riskSettings.getSignalSource());
        status.put("watchlistCount", marketDataService.getWatchlist().size());
        status.put("universeSize", bhavcopyService.getScanUniverseCount());
        status.put("scanUniverse", riskSettings.getScanUniverse());
        status.put("atrLoaded", atrService.getLoadedCountFor(marketDataService.getWatchlist()));
        status.put("smaLoaded", smaService.getLoadedCountFor(marketDataService.getWatchlist()));
        status.put("sma200Loaded", smaService.getSma200LoadedCountFor(marketDataService.getWatchlist()));
        status.put("firstCandleLoaded", candleAggregator.getFirstCandleCloseCountFor(marketDataService.getWatchlist()));
        status.put("validationPass", marketDataService.getValidationPass());
        status.put("validationFail", marketDataService.getValidationFail());
        status.put("validationTotal", marketDataService.getValidationTotal());
        status.put("timeframe", riskSettings.getScannerTimeframe());
        status.put("higherTimeframe", riskSettings.getHigherTimeframe());
        status.put("enableHpt", riskSettings.isEnableHpt());
        status.put("enableLpt", riskSettings.isEnableLpt());
        status.put("enableAtp", riskSettings.isEnableAtpCheck());
        status.put("enableSmaTrend", riskSettings.isEnableSmaTrendCheck());
        status.put("minPrice", riskSettings.getScanMinPrice());
        status.put("maxPrice", riskSettings.getScanMaxPrice());
        status.put("minTurnover", riskSettings.getScanMinTurnover());
        status.put("minVolume", riskSettings.getScanMinVolume());
        status.put("minBeta", riskSettings.getScanMinBeta());
        status.put("maxBeta", riskSettings.getScanMaxBeta());
        status.put("narrowMaxWidth", riskSettings.getNarrowCprMaxWidth());
        status.put("narrowMinWidth", riskSettings.getNarrowCprMinWidth());
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
        List<Map<String, Object>> sma20Series = new ArrayList<>();
        List<Map<String, Object>> sma50Series = new ArrayList<>();
        List<Map<String, Object>> sma200Series = new ArrayList<>();

        if (tradingDay) {
            // Live path: compute indicators progressively over prior-day warmup + today so
            // every today's bar has a fully-warmed-up SMA (bar 1 included).
            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            java.time.LocalDate today = java.time.LocalDate.now(ist);
            List<CandleAggregator.CandleBar> priors = candleAggregator.getPriorDayCandles(symbol);
            List<CandleAggregator.CandleBar> todays  = candleAggregator.getCompletedCandles(symbol);
            List<CandleAggregator.CandleBar> merged = new ArrayList<>();
            if (priors != null) merged.addAll(priors);
            if (todays != null) merged.addAll(todays);
            computeIndicatorsAndBuild(merged, today, ist, candleList, vwapSeries, sma20Series, sma50Series, sma200Series);

            // Forming (current, still-open) candle — append with live indicator values
            CandleAggregator.CandleBar current = candleAggregator.getCurrentCandle(symbol);
            if (current != null && current.open > 0) {
                candleList.add(barToMap(current, true));
                long tMs = current.epochSec * 1000L;
                double liveVwap = candleAggregator.getAtp(symbol);
                double liveSma20 = smaService.getSma(symbol);
                double liveSma50 = smaService.getSma50(symbol);
                double liveSma200 = smaService.getSma200(symbol);
                if (liveVwap > 0) vwapSeries.add(point(tMs, liveVwap));
                if (liveSma20 > 0) sma20Series.add(point(tMs, liveSma20));
                if (liveSma50 > 0) sma50Series.add(point(tMs, liveSma50));
                if (liveSma200 > 0) sma200Series.add(point(tMs, liveSma200));
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
                        computeIndicatorsAndBuild(hist, latestDate, ist, candleList, vwapSeries, sma20Series, sma50Series, sma200Series);
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
        result.put("sma20Series", sma20Series);
        result.put("sma50Series", sma50Series);
        result.put("sma200Series", sma200Series);
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
        result.put("sma20", r(smaService.getSma(symbol)));
        result.put("sma200", r(smaService.getSma200(symbol)));

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
