package com.rydytrader.autotrader.indicator;

import com.rydytrader.autotrader.dto.Candle;

import java.util.List;

/**
 * Bollinger Bands — 20-period SMA of close, ± 2 population standard deviations.
 *
 * <pre>
 * middle = SMA(close, period)
 * stddev = sqrt( Σ(close_i - middle)² / period )    // population stddev
 * upper  = middle + (k × stddev)
 * lower  = middle - (k × stddev)
 * </pre>
 *
 * <p>Framework rule (Ganesan playbook): a close BEYOND the 2σ upper band is a
 * 1-in-20 statistical event — the trigger for a long CE entry. Symmetric for
 * PE on a close below the lower band. NOT treated as mean-reversion; the four
 * co-confirming filters upstream ensure we only fire on a directional breakout.
 */
public final class BollingerBands {

    private BollingerBands() {}

    /** Band values at the LAST bar of {@code bars}. */
    public static State at(List<Candle> bars, int period, double stdMultiplier) {
        if (bars == null || period <= 0) return State.UNAVAILABLE;
        int n = bars.size();
        if (n < period) return State.UNAVAILABLE;
        // SMA over the last `period` closes.
        double sum = 0;
        for (int i = n - period; i < n; i++) sum += bars.get(i).close();
        double mid = sum / period;
        // Population stddev over the same window.
        double sqSum = 0;
        for (int i = n - period; i < n; i++) {
            double d = bars.get(i).close() - mid;
            sqSum += d * d;
        }
        double stddev = Math.sqrt(sqSum / period);
        return new State(mid, mid + stdMultiplier * stddev, mid - stdMultiplier * stddev, true);
    }

    public record State(double mid, double upper, double lower, boolean available) {
        public static final State UNAVAILABLE = new State(0, 0, 0, false);
    }
}
