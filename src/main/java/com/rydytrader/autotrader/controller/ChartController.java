package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.HistoricalChartStore;
import com.rydytrader.autotrader.service.MarketDataService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only endpoints powering the Chart page. Phase 3: the page shows the NIFTY
 * current-month FUTURES leg only — no option strikes. The aggregator's 5-min OHLC
 * ring for the futures symbol drives the panel.
 */
@RestController
@RequestMapping("/api/chart")
public class ChartController {

    // Legacy synthetic futures symbol from the GDFL era. Kept as an inline
    // constant until the chart page is rewritten for CE + PE 3-min panels.
    private static final String FUTURES_SYMBOL = "NSE:NIFTY-I-FUT";

    private final CandleAggregator  candleAggregator;
    private final MarketDataService marketDataService;
    private final HistoricalChartStore historicalChartStore;

    public ChartController(CandleAggregator candleAggregator,
                           MarketDataService marketDataService,
                           HistoricalChartStore historicalChartStore) {
        this.candleAggregator     = candleAggregator;
        this.marketDataService    = marketDataService;
        this.historicalChartStore = historicalChartStore;
    }

    /** Compact tick block for a single symbol. Reads directly from the in-memory tick
     *  cache — always returns the last-known value even off-market-hours, unlike the
     *  SSE stream which is only push-on-change.
     *
     *  <p>Change / %change are populated on every tick because GDFL RealtimeResult
     *  frames carry {@code Close} (previous day's close) as a first-class field —
     *  GdflService pulls that into {@link MarketDataService.LtpTick#prevClose()} and
     *  pushLtpTick seeds it into the TickData. So getDisplayChange returns the
     *  correct value from the very first tick. */
    private Map<String, Object> tickBlock(String fyersSymbol) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (fyersSymbol == null || fyersSymbol.isBlank()) return m;
        m.put("ltp",  round2(marketDataService.getDisplayLtp(fyersSymbol)));
        m.put("ch",   round2(marketDataService.getDisplayChange(fyersSymbol)));
        m.put("chp",  round2(marketDataService.getDisplayChangePct(fyersSymbol)));
        // Header VWAP = pandas_ta value from the last closed 5-min bar, which is
        // exactly what the chart's yellow line reads. Same source, same value,
        // no drift. Falls back to exchange ATP when no bar exists yet (first
        // 5 min of the session before the initial snapshot arrives).
        double vwap = 0.0;
        var history = candleAggregator.getHistory(fyersSymbol);
        if (!history.isEmpty()) {
            double lastVwap = history.get(history.size() - 1).vwap();
            if (lastVwap > 0) vwap = lastVwap;
        }
        if (vwap == 0.0) vwap = marketDataService.getVwap(fyersSymbol);
        m.put("vwap", round2(vwap));
        return m;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Symbols block for the chart page. Phase 3 exposes only the NIFTY futures leg —
     *  no ATM strikes / no CE-PE panels. Kept as a single endpoint so the client can
     *  keep polling one URL for both the symbol identity and the header tick block. */
    @GetMapping("/symbols")
    public Map<String, Object> symbols() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("futuresSymbol", FUTURES_SYMBOL);
        out.put("futuresTick",   tickBlock(FUTURES_SYMBOL));
        return out;
    }

    /** Closed candles for {@code symbol} at {@code interval} granularity.
     *  Aggregator stores 1-min bars from GDFL {@code SubscribeSnapshot MINUTE 1};
     *  {@code interval=1} returns them raw, {@code interval=5} groups them into
     *  5-min aggregates. Session VWAP is carried from the last contributing 1-min
     *  bar so the yellow line matches the strategy SL check regardless of
     *  timeframe. */
    @GetMapping("/candles")
    public Map<String, Object> candles(
            @RequestParam String symbol,
            @RequestParam(required = false, defaultValue = "1") int interval) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("symbol", symbol);
        out.put("interval", interval);
        if (symbol == null || symbol.isBlank() || !FUTURES_SYMBOL.equals(symbol)) {
            out.put("history", List.of());
            out.put("current", null);
            return out;
        }
        if (!candleAggregator.isSubscribed(symbol)) {
            // No-op listener — enough to make the aggregator start bucketing this symbol
            // from now on. History will be empty on this first response.
            candleAggregator.subscribe(symbol, c -> {});
        }
        int safeInterval = (interval == 5) ? 5 : 1;
        out.put("history", candleAggregator.getHistory(symbol, safeInterval));
        out.put("current", candleAggregator.getCurrentBucket(symbol));
        // Exchange "now" — max exchFeedTime across subscribed symbols. Chart uses this
        // for the bar countdown so it ticks in sync with TradingView (which also runs
        // on exchange time) rather than local wall clock. 0 when no ticks have arrived.
        long latestExchSec = marketDataService.getLatestExchFeedTimeSec();
        out.put("exchangeNowMs", latestExchSec > 0 ? latestExchSec * 1000L : 0L);
        return out;
    }

    /** Dates for which a historical chart snapshot exists (newest first). Populated by
     *  {@link HistoricalChartStore}'s scheduled daily save. The calendar page uses this
     *  to decide which day cells get a "chart" icon. */
    @GetMapping("/historical/dates")
    public Map<String, Object> historicalDates() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("dates", historicalChartStore.listAvailableDates());
        return out;
    }

    /** Full snapshot for a given date (NIFTY futures candles). Returns 404 when no
     *  snapshot exists for the requested date. */
    @GetMapping("/historical")
    public ResponseEntity<HistoricalChartStore.DailySnapshot> historicalSnapshot(
            @RequestParam String date) {
        return historicalChartStore.loadDailySnapshot(date)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
