package com.rydytrader.autotrader.indicator;

import com.rydytrader.autotrader.dto.Candle;

import java.util.List;

/**
 * Choppiness Index — a range-vs-directional-move oscillator that classifies a
 * market as trending or ranging. Introduced by E.W. Dreiss.
 *
 * <pre>
 * CI(n) = 100 × log10( sum(TR, n) / (max(high, n) - min(low, n)) ) / log10(n)
 * </pre>
 *
 * <p>The ratio compares "how much distance price actually travelled" (sum of
 * true ranges) against "how much net range it covered" (high-low of the
 * window). Zig-zagging price runs up big TR sums inside a narrow window →
 * high CI. Trending price stretches the window without inflating TR relative
 * to it → low CI.
 *
 * <p>Threshold conventions (61.8 / 38.2 are Fibonacci retracement levels):
 * <ul>
 *   <li>{@code CI > 61.8} — choppy / ranging</li>
 *   <li>{@code CI < 38.2} — trending</li>
 *   <li>otherwise — mixed</li>
 * </ul>
 *
 * <p>Bounded between 0 and 100 by construction (any monotonic advance would
 * put sum(TR) equal to the window range, giving log10(1)=0 → CI=0).
 */
public final class ChoppinessIndex {

    private ChoppinessIndex() {}

    /** Fibonacci threshold above which CI reads as choppy. */
    public static final double CHOPPY_THRESHOLD = 61.8;
    /** Fibonacci threshold below which CI reads as trending. */
    public static final double TRENDING_THRESHOLD = 38.2;

    /** Latest CI value on {@code bars} over the trailing {@code period} bars.
     *  Returns {@link Double#NaN} when there are fewer than {@code period + 1}
     *  bars (need one prior bar for the first TR). */
    public static double at(List<Candle> bars, int period) {
        if (bars == null || period < 2) return Double.NaN;
        int n = bars.size();
        if (n < period + 1) return Double.NaN;
        Candle[] arr = bars.toArray(new Candle[0]);
        // Sum of TR over the last `period` bars.
        double sumTr = 0;
        double hi = Double.NEGATIVE_INFINITY;
        double lo = Double.POSITIVE_INFINITY;
        for (int i = n - period; i < n; i++) {
            sumTr += TrueRange.at(arr, i);
            if (arr[i].high() > hi) hi = arr[i].high();
            if (arr[i].low()  < lo) lo = arr[i].low();
        }
        double range = hi - lo;
        if (range <= 0 || sumTr <= 0) return Double.NaN;
        return 100.0 * Math.log10(sumTr / range) / Math.log10(period);
    }

    /** Categorical read of a CI value: CHOPPY / TRENDING / MIXED / UNAVAILABLE. */
    public enum Regime { CHOPPY, TRENDING, MIXED, UNAVAILABLE }

    public static Regime classify(double ci) {
        if (Double.isNaN(ci)) return Regime.UNAVAILABLE;
        if (ci > CHOPPY_THRESHOLD)   return Regime.CHOPPY;
        if (ci < TRENDING_THRESHOLD) return Regime.TRENDING;
        return Regime.MIXED;
    }
}
