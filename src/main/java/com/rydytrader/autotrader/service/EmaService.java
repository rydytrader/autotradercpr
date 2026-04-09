package com.rydytrader.autotrader.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Calculates 20-period EMA on each candle close.
 * Used by BreakoutScanner for EMA distance filter.
 */
@Service
public class EmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(EmaService.class);
    private static final int EMA_PERIOD = 20;

    private final CandleAggregator candleAggregator;
    private final ConcurrentHashMap<String, Double> emaBySymbol = new ConcurrentHashMap<>();

    public EmaService(CandleAggregator candleAggregator) {
        this.candleAggregator = candleAggregator;
    }

    /** Get current EMA(20) for a symbol. Returns 0 if not enough data. */
    public double getEma(String symbol) {
        return emaBySymbol.getOrDefault(symbol, 0.0);
    }

    /** Get all EMA values (for monitoring/debugging). */
    public Map<String, Double> getAllEma() {
        return emaBySymbol;
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        List<CandleAggregator.CandleBar> candles = candleAggregator.getCompletedCandles(fyersSymbol);
        if (candles.size() >= EMA_PERIOD) {
            double ema = calculateEma(candles, EMA_PERIOD);
            emaBySymbol.put(fyersSymbol, ema);
        }
    }

    /**
     * Calculate EMA for given candle history.
     * Seed with SMA of first N closes, then apply exponential smoothing.
     */
    static double calculateEma(List<CandleAggregator.CandleBar> candles, int period) {
        if (candles.size() < period) return 0;

        double k = 2.0 / (period + 1);

        // Seed: SMA of first 'period' closes
        double sum = 0;
        for (int i = 0; i < period; i++) {
            sum += candles.get(i).close;
        }
        double ema = sum / period;

        // Apply exponential smoothing for remaining candles
        for (int i = period; i < candles.size(); i++) {
            ema = candles.get(i).close * k + ema * (1 - k);
        }

        return ema;
    }
}
