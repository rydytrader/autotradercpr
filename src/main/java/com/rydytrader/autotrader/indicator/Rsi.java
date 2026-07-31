package com.rydytrader.autotrader.indicator;

import com.rydytrader.autotrader.dto.Candle;

import java.util.List;

/**
 * Relative Strength Index — Wilder smoothing, 14-period default. Bounded 0-100.
 *
 * <pre>
 * gain[i] = max(0,  close[i] - close[i-1])
 * loss[i] = max(0, -close[i] + close[i-1])
 *
 * // Seed: simple mean over the first `period` gains / losses.
 * avgGain[period] = mean(gain[1..period])
 * avgLoss[period] = mean(loss[1..period])
 *
 * // Wilder recurrence forward:
 * avgGain[i] = (avgGain[i-1] × (period - 1) + gain[i]) / period
 * avgLoss[i] = (avgLoss[i-1] × (period - 1) + loss[i]) / period
 *
 * RS  = avgGain / avgLoss
 * RSI = 100 - (100 / (1 + RS))
 * </pre>
 *
 * <p>Framework rule (Ganesan playbook): RSI > 70 confirms strong up-momentum
 * for a bullish setup; RSI < 30 for bearish. Not treated as a mean-reversion
 * signal — a strong trend can ride the extreme for many bars.
 */
public final class Rsi {

    private Rsi() {}

    /** RSI value at the LAST bar. Returns 0 if insufficient data. */
    public static double at(List<Candle> bars, int period) {
        if (bars == null || period <= 0) return 0;
        // Need period+1 bars: one prior close for the seed's first delta.
        if (bars.size() < period + 1) return 0;
        int n = bars.size();
        double avgGain = 0, avgLoss = 0;
        // Seed = mean of first `period` deltas (indices 1..period).
        for (int i = 1; i <= period; i++) {
            double delta = bars.get(i).close() - bars.get(i - 1).close();
            if (delta > 0) avgGain += delta;
            else           avgLoss -= delta;   // loss is positive
        }
        avgGain /= period;
        avgLoss /= period;
        // Wilder recurrence forward through the rest.
        for (int i = period + 1; i < n; i++) {
            double delta = bars.get(i).close() - bars.get(i - 1).close();
            double gain = delta > 0 ?  delta : 0;
            double loss = delta < 0 ? -delta : 0;
            avgGain = (avgGain * (period - 1) + gain) / period;
            avgLoss = (avgLoss * (period - 1) + loss) / period;
        }
        if (avgLoss == 0) return avgGain == 0 ? 50 : 100;
        double rs = avgGain / avgLoss;
        return 100.0 - (100.0 / (1.0 + rs));
    }
}
