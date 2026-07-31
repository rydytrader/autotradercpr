package com.rydytrader.autotrader.indicator;

import com.rydytrader.autotrader.dto.Candle;

import java.util.List;

/**
 * SuperTrend indicator — the canonical formulation from Appendix A of the
 * Dharanidharan Ganesan option-buying playbook.
 *
 * <pre>
 * hl2       = (H + L) / 2
 * upperBand = hl2 + (multiplier × ATR)
 * lowerBand = hl2 - (multiplier × ATR)
 *
 * // Recurrence tightens each band so it never loosens while the trend holds:
 * finalUpper[i] = (upperBand[i] < finalUpper[i-1] || prevClose > finalUpper[i-1])
 *                   ? upperBand[i] : finalUpper[i-1];
 * finalLower[i] = (lowerBand[i] > finalLower[i-1] || prevClose < finalLower[i-1])
 *                   ? lowerBand[i] : finalLower[i-1];
 *
 * // Direction flips only when price closes THROUGH the band you're following.
 * // In an uptrend, SuperTrend = finalLower (line below price). In a downtrend,
 * // SuperTrend = finalUpper (line above price).
 * </pre>
 *
 * <p>System default is (10, 2) — 10-period ATR × multiplier 2. Sensitive enough
 * to catch a real move early, stable enough to filter the chop that whipsaws
 * scalpers.
 */
public final class SuperTrend {

    private SuperTrend() {}

    /** SuperTrend state at the LAST bar. */
    public static State at(List<Candle> bars, int atrPeriod, double multiplier) {
        if (bars == null || bars.size() < atrPeriod + 1) return State.UNAVAILABLE;
        Candle[] arr = bars.toArray(new Candle[0]);
        double[] atr = Atr.series(bars, atrPeriod);
        double[] finalUpper = new double[arr.length];
        double[] finalLower = new double[arr.length];
        boolean[] isUp = new boolean[arr.length];

        // Seed at atrPeriod-1 with raw bands + arbitrary direction (up); the
        // recurrence self-corrects on the first bar that closes through one.
        int seed = atrPeriod - 1;
        double hl2Seed = (arr[seed].high() + arr[seed].low()) / 2.0;
        finalUpper[seed] = hl2Seed + multiplier * atr[seed];
        finalLower[seed] = hl2Seed - multiplier * atr[seed];
        isUp[seed] = true;

        for (int i = seed + 1; i < arr.length; i++) {
            double hl2 = (arr[i].high() + arr[i].low()) / 2.0;
            double upper = hl2 + multiplier * atr[i];
            double lower = hl2 - multiplier * atr[i];
            double prevClose = arr[i - 1].close();

            finalUpper[i] = (upper < finalUpper[i - 1] || prevClose > finalUpper[i - 1])
                ? upper : finalUpper[i - 1];
            finalLower[i] = (lower > finalLower[i - 1] || prevClose < finalLower[i - 1])
                ? lower : finalLower[i - 1];

            // Direction flip: if we were in an uptrend and this close breaks
            // finalLower, flip to downtrend. Symmetric on the other side.
            boolean prevUp = isUp[i - 1];
            double close = arr[i].close();
            if (prevUp) {
                isUp[i] = close >= finalLower[i];  // stay up until close < lower
            } else {
                isUp[i] = close > finalUpper[i];   // flip up when close > upper
            }
        }
        int last = arr.length - 1;
        double line = isUp[last] ? finalLower[last] : finalUpper[last];
        return new State(line, isUp[last], true);
    }

    /** Result payload. {@code available=false} for insufficient data. */
    public record State(double line, boolean isUp, boolean available) {
        public static final State UNAVAILABLE = new State(0, false, false);
    }
}
