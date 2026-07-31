package com.rydytrader.autotrader.indicator;

import com.rydytrader.autotrader.dto.Candle;

/**
 * True Range — the building block for ATR and SuperTrend.
 *
 * <pre>
 * TR = max( H - L,
 *           |H - prevC|,
 *           |L - prevC| )
 * </pre>
 *
 * <p>For the very first bar of a series (no previous close), TR reduces to
 * {@code H - L}. Pure static function — no state.
 */
public final class TrueRange {

    private TrueRange() {}

    /** True range of the bar at index {@code i} in {@code bars}. */
    public static double at(Candle[] bars, int i) {
        Candle c = bars[i];
        double hl = c.high() - c.low();
        if (i == 0) return hl;
        double prevClose = bars[i - 1].close();
        double hpc = Math.abs(c.high() - prevClose);
        double lpc = Math.abs(c.low()  - prevClose);
        return Math.max(hl, Math.max(hpc, lpc));
    }
}
