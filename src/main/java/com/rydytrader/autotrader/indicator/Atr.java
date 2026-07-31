package com.rydytrader.autotrader.indicator;

import com.rydytrader.autotrader.dto.Candle;

import java.util.List;

/**
 * Average True Range using Wilder smoothing (RMA — the standard formulation).
 *
 * <p>Seed = simple mean of the first {@code period} true ranges. Every subsequent
 * bar applies the Wilder recurrence:
 *
 * <pre>
 * ATR_i = (ATR_{i-1} × (period - 1) + TR_i) / period
 * </pre>
 *
 * <p>Returns 0 when there are fewer than {@code period} bars — caller should
 * gate on this. All bars must be in chronological order.
 */
public final class Atr {

    private Atr() {}

    /** ATR value at the LAST bar of {@code bars}, computed with {@code period}
     *  bars of Wilder smoothing. Returns 0 if insufficient data. */
    public static double at(List<Candle> bars, int period) {
        if (bars == null || period <= 0) return 0;
        if (bars.size() < period) return 0;
        Candle[] arr = bars.toArray(new Candle[0]);
        return computeSeries(arr, period)[arr.length - 1];
    }

    /** Full ATR series over {@code bars}. Entries before index {@code period-1}
     *  are 0 (insufficient data for the initial seed). Exposed for backtesting
     *  and for SuperTrend which needs ATR at every bar, not just the last. */
    public static double[] series(List<Candle> bars, int period) {
        if (bars == null || period <= 0) return new double[0];
        Candle[] arr = bars.toArray(new Candle[0]);
        return computeSeries(arr, period);
    }

    private static double[] computeSeries(Candle[] bars, int period) {
        double[] out = new double[bars.length];
        if (bars.length < period) return out;
        // Seed: simple mean of first `period` true ranges.
        double sum = 0;
        for (int i = 0; i < period; i++) sum += TrueRange.at(bars, i);
        out[period - 1] = sum / period;
        // Wilder recurrence forward.
        for (int i = period; i < bars.length; i++) {
            out[i] = (out[i - 1] * (period - 1) + TrueRange.at(bars, i)) / period;
        }
        return out;
    }
}
