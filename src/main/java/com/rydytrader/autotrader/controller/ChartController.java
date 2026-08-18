package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.gdfl.GdflSymbolMapper;
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

    private static final String FUTURES_SYMBOL = GdflSymbolMapper.FYERS_NIFTY_FUTURES;

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
        // VWAP stays session-guarded (it's meaningful only for today's session;
        // last session's VWAP shouldn't display as if it were current).
        m.put("vwap", round2(marketDataService.getVwap(fyersSymbol)));
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

    /** Closed 5-min candles for {@code symbol} plus the in-progress bucket. Polling this
     *  every couple of seconds gives a live-updating rightmost candle without SSE. Phase 3
     *  only serves the futures symbol; any other request returns an empty payload so a
     *  stale client can't accidentally start bucketing an unrelated symbol. */
    @GetMapping("/candles")
    public Map<String, Object> candles(@RequestParam String symbol) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("symbol", symbol);
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
        // Chart's VWAP line = exchange ATP (Bucket.vwapLast), pushed into each
        // Candle.vwap on close. Zero approximation — reflects every actual trade
        // per NSE. Deliberately does NOT match TradingView's yellow line, which
        // is a bar-based (H+L+C)/3 × barVol approximation with a few points of
        // error on top of the true value.
        out.put("history", candleAggregator.getHistory(symbol));
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
