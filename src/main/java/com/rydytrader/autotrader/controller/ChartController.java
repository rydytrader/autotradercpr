package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.HistoricalChartStore;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.strategy.OptionSelling;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only endpoints powering the Chart page. Exposes the 3-min OHLC buffer maintained
 * by {@link CandleAggregator} plus the currently-selected ATM CE / PE symbols so the
 * page can render NIFTY spot + both option legs side-by-side without hitting Fyers.
 */
@RestController
@RequestMapping("/api/chart")
public class ChartController {

    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";

    private final CandleAggregator  candleAggregator;
    private final MarketDataService marketDataService;
    private final RiskSettingsStore riskSettings;
    private final ObjectProvider<OptionSelling> optionSellingProvider;
    private final HistoricalChartStore historicalChartStore;

    public ChartController(CandleAggregator candleAggregator,
                           MarketDataService marketDataService,
                           RiskSettingsStore riskSettings,
                           ObjectProvider<OptionSelling> optionSellingProvider,
                           HistoricalChartStore historicalChartStore) {
        this.candleAggregator     = candleAggregator;
        this.marketDataService    = marketDataService;
        this.riskSettings         = riskSettings;
        this.optionSellingProvider      = optionSellingProvider;
        this.historicalChartStore = historicalChartStore;
    }

    /** Compact tick block for a single symbol. Reads directly from the in-memory tick
     *  cache — always returns the last-known value even off-market-hours, unlike the
     *  SSE stream which is only push-on-change. */
    private Map<String, Object> tickBlock(String fyersSymbol) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (fyersSymbol == null || fyersSymbol.isBlank()) return m;
        m.put("ltp", round2(marketDataService.getLtp(fyersSymbol)));
        m.put("ch",  round2(marketDataService.getChange(fyersSymbol)));
        m.put("chp", round2(marketDataService.getChangePercent(fyersSymbol)));
        return m;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Which symbols the chart page should render. NIFTY is fixed; CE / PE come from
     *  OptionSelling.state and populate after the day's first-3-min close (~09:18 IST). */
    @GetMapping("/symbols")
    public Map<String, Object> symbols() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nifty", NIFTY_SYMBOL);
        // Read the session-locked ATM state DIRECTLY from OptionSelling rather than fishing
        // through dashboardState — the top-level dashboardState map has no ceSymbol /
        // peSymbol keys (they live nested under `optionSelling` and are hardcoded to "" there),
        // which was returning empty strings and starving the CE / PE chart panels.
        OptionSelling strat = optionSellingProvider == null ? null : optionSellingProvider.getIfAvailable();
        long   atmStrike = strat == null ? 0  : strat.getAtmStrike();
        String ceSymbol  = strat == null ? "" : strat.getCeSymbol();
        String peSymbol  = strat == null ? "" : strat.getPeSymbol();
        out.put("atmStrike", atmStrike);
        out.put("ceSymbol",  ceSymbol);
        out.put("peSymbol",  peSymbol);
        // Prime the header cells with the last-known WS-cached tick per symbol so the
        // chart page shows NIFTY / CE / PE values instantly even when the ticker SSE
        // hasn't pushed anything yet (initial load, off-hours, or paused feed).
        out.put("niftyTick", tickBlock(NIFTY_SYMBOL));
        out.put("ceTick",    tickBlock(ceSymbol));
        out.put("peTick",    tickBlock(peSymbol));
        // Active SL levels for the CE / PE legs — drives a horizontal SL price line
        // on the corresponding chart. Zero when no position is open on that side.
        out.put("ceSl", strat == null ? 0.0 : round2(strat.getOpenSlLevel(ceSymbol)));
        out.put("peSl", strat == null ? 0.0 : round2(strat.getOpenSlLevel(peSymbol)));
        // Entry price of the open CE / PE position — drives a horizontal entry
        // price line alongside the SL line. Zero when no position is open on
        // that side (the line is removed).
        out.put("ceEntry", strat == null ? 0.0 : round2(strat.getOpenEntryPrice(ceSymbol)));
        out.put("peEntry", strat == null ? 0.0 : round2(strat.getOpenEntryPrice(peSymbol)));
        // Per-side session stats — trade count vs configured cap + realised+open P&L.
        // Drives the "(n/max) · P&L +X" chip inside the CE / PE panel headers.
        Map<String, Object> ceStats = new LinkedHashMap<>();
        Map<String, Object> peStats = new LinkedHashMap<>();
        ceStats.put("count",  strat == null ? 0 : strat.getCeTradesToday());
        ceStats.put("max",    riskSettings.getOptionSellingMaxCeTradesPerDay());
        ceStats.put("pnl",    strat == null ? 0.0 : round2(strat.getCeSideNetPnlToday()));
        peStats.put("count",  strat == null ? 0 : strat.getPeTradesToday());
        peStats.put("max",    riskSettings.getOptionSellingMaxPeTradesPerDay());
        peStats.put("pnl",    strat == null ? 0.0 : round2(strat.getPeSideNetPnlToday()));
        out.put("ceStats", ceStats);
        out.put("peStats", peStats);
        return out;
    }

    /** Closed 3-min candles for {@code symbol} plus the in-progress bucket. Polling this
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
        // Exchange "now" — max exchFeedTime across subscribed symbols. Chart uses this
        // for the 3-min bar countdown so it ticks in sync with TradingView (which also
        // runs on exchange time) rather than local wall clock (which typically trails
        // exchange time by 2-3 seconds). 0 when no ticks have arrived yet.
        long latestExchSec = marketDataService.getLatestExchFeedTimeSec();
        out.put("exchangeNowMs", latestExchSec > 0 ? latestExchSec * 1000L : 0L);
        return out;
    }

    /** Dates for which a historical chart snapshot exists (newest first). Populated by
     *  {@link HistoricalChartStore}'s 15:32 scheduled save. The calendar page uses this
     *  to decide which day cells get a "chart" icon. */
    @GetMapping("/historical/dates")
    public Map<String, Object> historicalDates() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("dates", historicalChartStore.listAvailableDates());
        return out;
    }

    /** Full snapshot for a given date (NIFTY spot + ATM CE + ATM PE candles). Returns
     *  404 when no snapshot exists for the requested date. */
    @GetMapping("/historical")
    public ResponseEntity<HistoricalChartStore.DailySnapshot> historicalSnapshot(
            @RequestParam String date) {
        return historicalChartStore.loadDailySnapshot(date)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
