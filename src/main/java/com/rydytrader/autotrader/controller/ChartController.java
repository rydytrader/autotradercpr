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
     *  VWAP reads the current forming bar (live tick) if present, else the last
     *  closed bar. Supertrend runs on closed bars PLUS the in-progress forming
     *  bar so the returned {@code stLine} / {@code stIsUp} update mid-bar on
     *  every tick (matches the chart's live ST chip). */
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
        // Append the in-progress N-min bar so ST sees the live-tick close.
        Candle forming = buildFormingBar(fyersSymbol, history, tf);
        List<Candle> live = new ArrayList<>(history);
        if (forming != null) {
            if (!live.isEmpty() && live.get(live.size() - 1).startMillis() == forming.startMillis()) {
                live.set(live.size() - 1, forming);
            } else {
                live.add(forming);
            }
        }
        Double atrVal = null;
        if (!live.isEmpty()) {
            double lastVwap = live.get(live.size() - 1).vwap();
            if (lastVwap > 0) vwap = lastVwap;
            int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
            double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
            var st = SuperTrend.at(live, atrPeriod, mult);
            if (st.available()) {
                stLine = round2(st.line());
                stIsUp = st.isUp();
            }
            double atr = com.rydytrader.autotrader.indicator.Atr.at(live, atrPeriod);
            if (atr > 0 && !Double.isNaN(atr)) atrVal = round2(atr);
        }
        if (vwap == 0.0) vwap = marketDataService.getVwap(fyersSymbol);
        m.put("vwap",   round2(vwap));
        m.put("stLine", stLine);
        m.put("stIsUp", stIsUp);
        m.put("atr",    atrVal);
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
        // Chart shows today + last 20 prior-session N-min bars (~1 h on a
        // 3-min chart). Enough prior context to see how yesterday closed
        // without dominating today's view once the session progresses.
        List<Candle> visible = trimToRecentContext(bars, 20);
        Candle forming = buildFormingBar(symbol, visible, tf);
        List<Candle> visibleWithForming = new ArrayList<>(visible);
        if (forming != null) {
            if (!visibleWithForming.isEmpty()
                    && visibleWithForming.get(visibleWithForming.size() - 1).startMillis() == forming.startMillis()) {
                visibleWithForming.set(visibleWithForming.size() - 1, forming);
            } else {
                visibleWithForming.add(forming);
            }
        }
        // ST computed on the FULL ring (so ATR warmup is valid) but the
        // series output is filtered to only visible bars.
        List<Candle> allBarsWithForming = new ArrayList<>(bars);
        if (forming != null) {
            if (!allBarsWithForming.isEmpty()
                    && allBarsWithForming.get(allBarsWithForming.size() - 1).startMillis() == forming.startMillis()) {
                allBarsWithForming.set(allBarsWithForming.size() - 1, forming);
            } else {
                allBarsWithForming.add(forming);
            }
        }
        out.put("history",  visible);
        out.put("current",  forming);   // in-progress N-min bar with live-tick close
        out.put("stSeries", buildStSeries(allBarsWithForming, visibleWithForming));
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
        // Prefer the ATP carried in the in-progress bucket (exchange-computed
        // session VWAP updated every tick). Falls back to a local pandas_ta
        // recompute only when ATP wasn't available (rare — pre-market ticks
        // or a symbol whose first FULL-mode tick hasn't landed yet).
        double liveVwap = live1m.vwap() > 0
            ? live1m.vwap()
            : computeLiveSessionVwapFallback(symbol, live1m);
        return new Candle(round2(open), round2(high), round2(low), round2(close),
            vol, windowStartMs, round2(liveVwap));
    }

    /** Local pandas_ta fallback — only used when the forming bar has no
     *  ATP yet. Session-cumulative Σ(HLC/3 × V) / ΣV across today's 1-min
     *  ring, plus the in-progress bucket. */
    private double computeLiveSessionVwapFallback(String symbol, Candle live1m) {
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

    /** Chart window rule: last {@code priorContext} prior-session bars +
     *  all of today's bars. Prior tail gives visual continuity from
     *  yesterday's close; today dominates once the session progresses.
     *  When today has NO bars yet (pre-open, or strategy hasn't captured
     *  the spot tick) the pane still shows the prior tail so it isn't
     *  empty. {@code priorContext = 0} → today only. */
    private List<Candle> trimToRecentContext(List<Candle> bars, int priorContext) {
        if (bars == null || bars.isEmpty()) return List.of();
        long todayStartUtcMs = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata"))
            .atStartOfDay(java.time.ZoneId.of("Asia/Kolkata")).toInstant().toEpochMilli();
        List<Candle> prior = new ArrayList<>();
        List<Candle> today = new ArrayList<>();
        for (Candle c : bars) {
            if (c.startMillis() >= todayStartUtcMs) today.add(c);
            else prior.add(c);
        }
        int from = Math.max(0, prior.size() - Math.max(0, priorContext));
        List<Candle> out = new ArrayList<>(prior.size() - from + today.size());
        out.addAll(prior.subList(from, prior.size()));
        out.addAll(today);
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
