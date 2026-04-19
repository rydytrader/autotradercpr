package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Higher-timeframe (75-min) EMAs for long-term trend display.
 * Mirrors EmaService but listens to the separate htfAggregator.
 * Display-only — not used for trade filtering or scoring.
 */
@Service
public class HtfEmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(HtfEmaService.class);
    private static final int EMA_PERIOD = 20;
    private static final int EMA_MID_PERIOD = 50;
    private static final int EMA_LONG_PERIOD = 200;
    private static final int HISTORY_SIZE = 20;

    private final RiskSettingsStore riskSettings;

    private final ConcurrentHashMap<String, Double> emaBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<Double>> emaHistoryBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> ema50BySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<Double>> ema50HistoryBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> ema200BySymbol = new ConcurrentHashMap<>();

    public HtfEmaService(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
    }

    public double getEma(String symbol)    { return emaBySymbol.getOrDefault(symbol, 0.0); }
    public double getEma50(String symbol)  { return ema50BySymbol.getOrDefault(symbol, 0.0); }
    public double getEma200(String symbol) { return ema200BySymbol.getOrDefault(symbol, 0.0); }

    public int getLoadedCount()     { return (int) emaBySymbol.values().stream().filter(v -> v > 0).count(); }
    public int getEma50LoadedCount(){ return (int) ema50BySymbol.values().stream().filter(v -> v > 0).count(); }
    public int getEma200LoadedCount(){ return (int) ema200BySymbol.values().stream().filter(v -> v > 0).count(); }

    /**
     * Seed HTF EMAs from historical 75-min candles.
     * Needs ≥ EMA_LONG_PERIOD (200) bars for EMA(200) convergence.
     */
    public void seedFromCandles(String symbol, List<CandleAggregator.CandleBar> candles) {
        if (candles == null || candles.size() < EMA_PERIOD) return;

        Deque<Double> h20 = emaHistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (h20) { h20.clear(); }
        Deque<Double> h50 = ema50HistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (h50) { h50.clear(); }

        // 20 EMA
        double k20 = 2.0 / (EMA_PERIOD + 1);
        double sum20 = 0;
        for (int i = 0; i < EMA_PERIOD; i++) sum20 += candles.get(i).close;
        double ema = sum20 / EMA_PERIOD;
        storeEma(symbol, ema);
        for (int i = EMA_PERIOD; i < candles.size(); i++) {
            ema = candles.get(i).close * k20 + ema * (1 - k20);
            storeEma(symbol, ema);
        }

        // 50 EMA
        if (candles.size() >= EMA_MID_PERIOD) {
            double k50 = 2.0 / (EMA_MID_PERIOD + 1);
            double sum50 = 0;
            for (int i = 0; i < EMA_MID_PERIOD; i++) sum50 += candles.get(i).close;
            double ema50 = sum50 / EMA_MID_PERIOD;
            storeEma50(symbol, ema50);
            for (int i = EMA_MID_PERIOD; i < candles.size(); i++) {
                ema50 = candles.get(i).close * k50 + ema50 * (1 - k50);
                storeEma50(symbol, ema50);
            }
        }

        // 200 EMA
        if (candles.size() >= EMA_LONG_PERIOD) {
            double k200 = 2.0 / (EMA_LONG_PERIOD + 1);
            double sum200 = 0;
            for (int i = 0; i < EMA_LONG_PERIOD; i++) sum200 += candles.get(i).close;
            double ema200 = sum200 / EMA_LONG_PERIOD;
            for (int i = EMA_LONG_PERIOD; i < candles.size(); i++) {
                ema200 = candles.get(i).close * k200 + ema200 * (1 - k200);
            }
            ema200BySymbol.put(symbol, ema200);
        }
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (completedCandle == null || completedCandle.close <= 0) return;

        Double prev20 = emaBySymbol.get(fyersSymbol);
        if (prev20 != null && prev20 > 0) {
            double k = 2.0 / (EMA_PERIOD + 1);
            double ema = completedCandle.close * k + prev20 * (1 - k);
            storeEma(fyersSymbol, ema);
        }

        Double prev50 = ema50BySymbol.get(fyersSymbol);
        if (prev50 != null && prev50 > 0) {
            double k50 = 2.0 / (EMA_MID_PERIOD + 1);
            double ema50 = completedCandle.close * k50 + prev50 * (1 - k50);
            storeEma50(fyersSymbol, ema50);
        }

        Double prev200 = ema200BySymbol.get(fyersSymbol);
        if (prev200 != null && prev200 > 0) {
            double k200 = 2.0 / (EMA_LONG_PERIOD + 1);
            double ema200 = completedCandle.close * k200 + prev200 * (1 - k200);
            ema200BySymbol.put(fyersSymbol, ema200);
        }
    }

    private void storeEma(String symbol, double ema) {
        emaBySymbol.put(symbol, ema);
        Deque<Double> h = emaHistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (h) {
            h.addLast(ema);
            while (h.size() > HISTORY_SIZE) h.removeFirst();
        }
    }

    private void storeEma50(String symbol, double ema50) {
        ema50BySymbol.put(symbol, ema50);
        Deque<Double> h = ema50HistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (h) {
            h.addLast(ema50);
            while (h.size() > HISTORY_SIZE) h.removeFirst();
        }
    }

    /**
     * Classify the HTF EMA(20) / EMA(50) relationship.
     * Uses same thresholds as 5-min from RiskSettingsStore.
     */
    public String getEmaPattern(String symbol, int lookback, double atr,
                                int braidedMinCrossovers, double braidedMaxSpreadAtr,
                                double railwayMaxCv, double railwayMinSpreadAtr) {
        if (atr <= 0 || lookback < 3) return "";
        Deque<Double> h20 = emaHistoryBySymbol.get(symbol);
        Deque<Double> h50 = ema50HistoryBySymbol.get(symbol);
        if (h20 == null || h50 == null) return "";

        double[] ema20Arr, ema50Arr;
        synchronized (h20) {
            if (h20.size() < lookback) return "";
            ema20Arr = lastN(h20, lookback);
        }
        synchronized (h50) {
            if (h50.size() < lookback) return "";
            ema50Arr = lastN(h50, lookback);
        }

        double[] spread = new double[lookback];
        double sumAbs = 0;
        for (int i = 0; i < lookback; i++) {
            spread[i] = ema20Arr[i] - ema50Arr[i];
            sumAbs += Math.abs(spread[i]);
        }
        double meanAbs = sumAbs / lookback;

        int crossovers = 0;
        for (int i = 1; i < lookback; i++) {
            if ((spread[i - 1] > 0 && spread[i] < 0) || (spread[i - 1] < 0 && spread[i] > 0)) {
                crossovers++;
            }
        }

        if (crossovers >= braidedMinCrossovers) return "BRAIDED";
        if (meanAbs <= braidedMaxSpreadAtr * atr) return "BRAIDED";

        if (crossovers <= 1 && meanAbs >= railwayMinSpreadAtr * atr) {
            double sumSqDev = 0;
            for (int i = 0; i < lookback; i++) {
                double dev = Math.abs(spread[i]) - meanAbs;
                sumSqDev += dev * dev;
            }
            double std = Math.sqrt(sumSqDev / lookback);
            double cv = meanAbs > 0 ? std / meanAbs : Double.POSITIVE_INFINITY;
            if (cv <= railwayMaxCv) {
                // 20 must also be on the right side of 200 to confirm long-term direction.
                double latestSpread = spread[lookback - 1];
                double ema20Latest = ema20Arr[lookback - 1];
                double ema200Latest = ema200BySymbol.getOrDefault(symbol, 0.0);
                if (ema200Latest <= 0) return "";
                if (latestSpread > 0 && ema20Latest > ema200Latest) return "RAILWAY_UP";
                if (latestSpread < 0 && ema20Latest < ema200Latest) return "RAILWAY_DOWN";
            }
        }
        return "";
    }

    private static double[] lastN(Deque<Double> deque, int n) {
        double[] out = new double[n];
        int size = deque.size();
        int skip = size - n;
        int i = 0;
        int outIdx = 0;
        for (Double v : deque) {
            if (i++ < skip) continue;
            out[outIdx++] = v;
        }
        return out;
    }
}
