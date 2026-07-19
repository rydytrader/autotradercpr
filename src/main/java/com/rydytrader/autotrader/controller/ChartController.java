package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.strategy.AtmVwap;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only endpoints powering the Chart page. Exposes the 2-min OHLC buffer maintained
 * by {@link CandleAggregator} plus the currently-selected ATM CE / PE symbols so the
 * page can render NIFTY spot + both option legs side-by-side without hitting Fyers.
 */
@RestController
@RequestMapping("/api/chart")
public class ChartController {

    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";

    private final CandleAggregator candleAggregator;
    private final ObjectProvider<AtmVwap> atmVwapProvider;

    public ChartController(CandleAggregator candleAggregator,
                           ObjectProvider<AtmVwap> atmVwapProvider) {
        this.candleAggregator = candleAggregator;
        this.atmVwapProvider  = atmVwapProvider;
    }

    /** Which symbols the chart page should render. NIFTY is fixed; CE / PE come from
     *  AtmVwap.state and populate after the day's first-2-min close (~09:17 IST). */
    @GetMapping("/symbols")
    public Map<String, Object> symbols() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nifty", NIFTY_SYMBOL);
        AtmVwap strat = atmVwapProvider == null ? null : atmVwapProvider.getIfAvailable();
        Map<String, Object> dash = strat == null ? Map.of() : strat.dashboardState();
        out.put("atmStrike", dash.getOrDefault("atmStrike", 0));
        out.put("ceSymbol",  dash.getOrDefault("ceSymbol", ""));
        out.put("peSymbol",  dash.getOrDefault("peSymbol", ""));
        return out;
    }

    /** Closed 2-min candles for {@code symbol} plus the in-progress bucket. Polling this
     *  every couple of seconds gives a live-updating rightmost candle without SSE. If the
     *  aggregator isn't yet buffering {@code symbol}, this endpoint registers a no-op
     *  listener to kick off subscription — subsequent polls will see the ring populate. */
    @GetMapping("/candles")
    public Map<String, Object> candles(@RequestParam String symbol) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("symbol", symbol);
        if (symbol == null || symbol.isBlank()) {
            out.put("history", List.of());
            out.put("current", null);
            return out;
        }
        if (!candleAggregator.isSubscribed(symbol)) {
            // No-op listener — enough to make the aggregator start bucketing this symbol
            // from now on. History will be empty on this first response.
            candleAggregator.subscribe(symbol, c -> {});
        }
        List<Candle> hist = candleAggregator.getHistory(symbol);
        out.put("history", hist);
        out.put("current", candleAggregator.getCurrentBucket(symbol));
        return out;
    }
}
