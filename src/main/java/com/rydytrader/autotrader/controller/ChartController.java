package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.indicator.SuperTrend;

import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.FyersMinuteBarBuilder;
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
    private final FyersMinuteBarBuilder minuteBarBuilder;
    private final ObjectProvider<VwapSupertrendStrategy> strategyProvider;

    public ChartController(CandleAggregator candleAggregator,
                           MarketDataService marketDataService,
                           HistoricalChartStore historicalChartStore,
                           RiskSettingsStore riskSettings,
                           FyersMinuteBarBuilder minuteBarBuilder,
                           ObjectProvider<VwapSupertrendStrategy> strategyProvider) {
        this.candleAggregator     = candleAggregator;
        this.marketDataService    = marketDataService;
        this.historicalChartStore = historicalChartStore;
        this.riskSettings         = riskSettings;
        this.minuteBarBuilder     = minuteBarBuilder;
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
        // Live per-leg strategy levels — chart draws horizontal lines when
        // entry/SL/target > 0 (i.e. the leg is IN_POSITION).
        out.put("ceLeg",     s == null || ceSym == null ? Map.of() : s.getLegSnapshot(ceSym));
        out.put("peLeg",     s == null || peSym == null ? Map.of() : s.getLegSnapshot(peSym));
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
        // ST series must include prior-session bars so ATR is valid from
        // bar 1 today; but the chart itself only shows TODAY'S session, so
        // the first visible bar is 09:15 IST, not yesterday's close.
        List<Candle> todayBars = filterToToday(bars);
        Candle forming = buildFormingBar(symbol, todayBars, tf);
        out.put("history",  todayBars);
        out.put("current",  forming);   // in-progress N-min bar with live-tick close
        out.put("stSeries", buildStSeries(bars, todayBars));
        long latestExchSec = marketDataService.getLatestExchFeedTimeSec();
        out.put("exchangeNowMs", latestExchSec > 0 ? latestExchSec * 1000L : 0L);
        return out;
    }

    /** Builds the in-progress N-min bar by combining any closed 1-min bars
     *  inside the current N-min window (already in {@code bars} if their
     *  bucket start matches) with the live 1-min bucket from
     *  {@link FyersMinuteBarBuilder}. Also computes a LIVE session VWAP
     *  that includes the in-progress 1-min bucket's contribution — so the
     *  yellow VWAP line updates on every tick, not once per minute.
     *
     *  <p>Returns {@code null} when we can't determine a window (no bar
     *  builder state for the symbol) — chart JS treats null as no live bar. */
    private Candle buildFormingBar(String symbol, List<Candle> bars, int tf) {
        Candle live1m = minuteBarBuilder.getInProgressBar(symbol);
        if (live1m == null) return null;
        long bucketMs      = tf * 60_000L;
        long windowStartMs = live1m.startMillis() - (live1m.startMillis() % bucketMs);
        double open = live1m.open(), high = live1m.high(), low = live1m.low(), close = live1m.close();
        long vol = Math.max(0, live1m.volume());
        Candle latestAgg = bars.isEmpty() ? null : bars.get(bars.size() - 1);
        if (latestAgg != null && latestAgg.startMillis() == windowStartMs) {
            open = latestAgg.open();
            high = Math.max(latestAgg.high(), high);
            low  = latestAgg.low() > 0 ? Math.min(latestAgg.low(), low) : low;
            vol  = latestAgg.volume() + vol;
        }
        double liveVwap = computeLiveSessionVwap(symbol, live1m);
        return new Candle(round2(open), round2(high), round2(low), round2(close),
            vol, windowStartMs, round2(liveVwap));
    }

    /** Session-cumulative pandas_ta VWAP that includes the in-progress 1-min
     *  bucket's contribution — matches how each closed bar's VWAP is computed
     *  in {@code CandleAggregator.recomputeVwapsAndReturnLast}. Iterates all
     *  today's 1-min bars in the ring, sums (H+L+C)/3 × volume, adds the
     *  in-progress bucket, divides. */
    private double computeLiveSessionVwap(String symbol, Candle live1m) {
        java.util.List<Candle> bars1m = candleAggregator.getHistory(symbol, 1);
        long istMs = live1m.startMillis() + 19_800_000L;
        long dayEpochMs = (istMs - (istMs % 86_400_000L)) - 19_800_000L;
        double cumTypVol = 0.0, cumVol = 0.0;
        for (Candle b : bars1m) {
            long ist = b.startMillis() + 19_800_000L;
            long day = (ist - (ist % 86_400_000L)) - 19_800_000L;
            if (day != dayEpochMs) continue;
            double typ = (b.high() + b.low() + b.close()) / 3.0;
            long v = b.volume();
            if (v > 0 && typ > 0) { cumTypVol += typ * v; cumVol += v; }
        }
        double typLive = (live1m.high() + live1m.low() + live1m.close()) / 3.0;
        long vLive = live1m.volume();
        if (vLive > 0 && typLive > 0) { cumTypVol += typLive * vLive; cumVol += vLive; }
        return cumVol > 0 ? cumTypVol / cumVol : 0.0;
    }

    /** Keeps only bars whose IST calendar day equals today. Prior-session
     *  bars stay in the aggregator (Supertrend ATR warmup needs them) but
     *  the chart shows only today's session so the first bar reads 09:15. */
    private List<Candle> filterToToday(List<Candle> bars) {
        if (bars == null || bars.isEmpty()) return List.of();
        long todayStartUtcMs   = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata"))
            .atStartOfDay(java.time.ZoneId.of("Asia/Kolkata")).toInstant().toEpochMilli();
        long tomorrowStartUtcMs = todayStartUtcMs + 86_400_000L;
        List<Candle> out = new ArrayList<>(bars.size());
        for (Candle c : bars) {
            long sm = c.startMillis();
            if (sm >= todayStartUtcMs && sm < tomorrowStartUtcMs) out.add(c);
        }
        return out;
    }

    /** Per-bar Supertrend points. Runs on {@code allBars} so ATR warmup uses
     *  the prior session, then emits only points whose bar timestamp is in
     *  {@code todayBars} so the ST line aligns with the visible candles. */
    private List<Map<String, Object>> buildStSeries(List<Candle> allBars, List<Candle> todayBars) {
        if (allBars == null || allBars.isEmpty()) return List.of();
        int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
        double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
        SuperTrend.Series ser = SuperTrend.series(allBars, atrPeriod, mult);
        java.util.Set<Long> todayStarts = new java.util.HashSet<>();
        for (Candle c : todayBars) todayStarts.add(c.startMillis());
        List<Map<String, Object>> out = new ArrayList<>(allBars.size());
        for (int i = 0; i < allBars.size(); i++) {
            double line = ser.line()[i];
            if (Double.isNaN(line)) continue;
            long ts = allBars.get(i).startMillis();
            if (!todayStarts.contains(ts)) continue;
            Map<String, Object> pt = new LinkedHashMap<>();
            pt.put("t",    ts);
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
