package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.indicator.SuperTrend;

import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.HistoricalChartStore;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.strategy.VwapSupertrendStrategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only endpoints powering the Chart page. Under the VWAP+Supertrend
 * strategy, the page shows the chosen CE and PE 3-min charts side-by-side
 * with a session VWAP overlay and a Supertrend step line. Chosen symbols come
 * from {@link VwapSupertrendStrategy#getChosenCeSymbol}/PeSymbol — populated
 * after 09:15 spot open + 15 s warm-up.
 */
@RestController
@RequestMapping("/api/chart")
public class ChartController {

    private final CandleAggregator      candleAggregator;
    private final MarketDataService     marketDataService;
    private final HistoricalChartStore  historicalChartStore;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<VwapSupertrendStrategy> strategyProvider;

    public ChartController(CandleAggregator candleAggregator,
                           MarketDataService marketDataService,
                           HistoricalChartStore historicalChartStore,
                           RiskSettingsStore riskSettings,
                           ObjectProvider<VwapSupertrendStrategy> strategyProvider) {
        this.candleAggregator     = candleAggregator;
        this.marketDataService    = marketDataService;
        this.historicalChartStore = historicalChartStore;
        this.riskSettings         = riskSettings;
        this.strategyProvider     = strategyProvider;
    }

    /** Chosen CE + PE symbols and their header tick blocks. Frontend polls this
     *  every ~2 s and renders both panels. Symbols are null until strategy
     *  picks them (~09:15:15 IST). */
    @GetMapping("/symbols")
    public Map<String, Object> symbols() {
        Map<String, Object> out = new LinkedHashMap<>();
        VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
        String ceSym = s == null ? null : s.getChosenCeSymbol();
        String peSym = s == null ? null : s.getChosenPeSymbol();
        out.put("ceSymbol",  ceSym);
        out.put("peSymbol",  peSym);
        out.put("ceTick",    tickBlock(ceSym));
        out.put("peTick",    tickBlock(peSym));
        out.put("spotOpen",  s == null ? 0 : round2(s.getSpotOpen()));
        out.put("atmStrike", s == null ? 0 : s.getAtmStrike());
        // Live NIFTY spot LTP + change since prev close — for the header chip.
        String spotSym = "NSE:NIFTY50-INDEX";
        out.put("spotLtp",     round2(marketDataService.getDisplayLtp(spotSym)));
        out.put("spotChange",  round2(marketDataService.getDisplayChange(spotSym)));
        out.put("spotChangePct", round2(marketDataService.getDisplayChangePct(spotSym)));
        return out;
    }

    /** Compact tick block for a single symbol — LTP + change + VWAP + Supertrend.
     *  VWAP reads the last N-min bar's pandas_ta value (matches chart yellow line).
     *  Supertrend runs on the same N-min bars using the configured atrPeriod +
     *  multiplier and reports both {@code stLine} (numeric level) and
     *  {@code stIsUp} (boolean — green when true, red when false). */
    private Map<String, Object> tickBlock(String fyersSymbol) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (fyersSymbol == null || fyersSymbol.isBlank()) return m;
        m.put("ltp",  round2(marketDataService.getDisplayLtp(fyersSymbol)));
        m.put("ch",   round2(marketDataService.getDisplayChange(fyersSymbol)));
        m.put("chp",  round2(marketDataService.getDisplayChangePct(fyersSymbol)));
        double vwap = 0.0;
        Double stLine = null;
        Boolean stIsUp = null;
        int tf = Math.max(1, riskSettings.getVwapStCandleMinutes());
        var history = candleAggregator.getHistory(fyersSymbol, tf);
        if (!history.isEmpty()) {
            double lastVwap = history.get(history.size() - 1).vwap();
            if (lastVwap > 0) vwap = lastVwap;
            int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
            double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
            var st = SuperTrend.at(history, atrPeriod, mult);
            if (st.available()) {
                stLine = round2(st.line());
                stIsUp = st.isUp();
            }
        }
        if (vwap == 0.0) vwap = marketDataService.getVwap(fyersSymbol);
        m.put("vwap",   round2(vwap));
        m.put("stLine", stLine);
        m.put("stIsUp", stIsUp);
        return m;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Bars at the configured strategy timeframe (default 3-min) plus a
     *  Supertrend series aligned index-for-index. Response:
     *  {@code { symbol, interval, history: [Candle], stSeries: [{t, line, isUp}] }}. */
    @GetMapping("/candles")
    public Map<String, Object> candles(
            @RequestParam String symbol,
            @RequestParam(required = false, defaultValue = "0") int interval) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("symbol", symbol);
        int tf = interval > 0 ? interval : Math.max(1, riskSettings.getVwapStCandleMinutes());
        out.put("interval", tf);
        if (symbol == null || symbol.isBlank()) {
            out.put("history",  List.of());
            out.put("stSeries", List.of());
            return out;
        }
        if (!candleAggregator.isSubscribed(symbol)) {
            candleAggregator.subscribe(symbol, c -> {});
        }
        List<Candle> bars = candleAggregator.getHistory(symbol, tf);
        out.put("history", bars);
        out.put("stSeries", buildStSeries(bars));
        long latestExchSec = marketDataService.getLatestExchFeedTimeSec();
        out.put("exchangeNowMs", latestExchSec > 0 ? latestExchSec * 1000L : 0L);
        return out;
    }

    /** Per-bar Supertrend points aligned index-for-index with {@code bars}.
     *  Skips indexes where ST is not yet defined (NaN). */
    private List<Map<String, Object>> buildStSeries(List<Candle> bars) {
        if (bars == null || bars.isEmpty()) return List.of();
        int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
        double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
        SuperTrend.Series ser = SuperTrend.series(bars, atrPeriod, mult);
        List<Map<String, Object>> out = new ArrayList<>(bars.size());
        for (int i = 0; i < bars.size(); i++) {
            double line = ser.line()[i];
            if (Double.isNaN(line)) continue;
            Map<String, Object> pt = new LinkedHashMap<>();
            pt.put("t",    bars.get(i).startMillis());
            pt.put("line", round2(line));
            pt.put("isUp", ser.isUp()[i]);
            out.add(pt);
        }
        return out;
    }

    /** Dates for which a historical chart snapshot exists (newest first). */
    @GetMapping("/historical/dates")
    public Map<String, Object> historicalDates() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("dates", historicalChartStore.listAvailableDates());
        return out;
    }

    /** Full snapshot for a given date. */
    @GetMapping("/historical")
    public ResponseEntity<HistoricalChartStore.DailySnapshot> historicalSnapshot(
            @RequestParam String date) {
        return historicalChartStore.loadDailySnapshot(date)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
