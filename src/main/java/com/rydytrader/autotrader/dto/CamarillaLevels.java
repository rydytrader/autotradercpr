package com.rydytrader.autotrader.dto;

import java.time.LocalDate;

/**
 * Camarilla pivot levels computed from a prior trading day's OHLC.
 *
 * <p>Formulas (with {@code R = priorHigh − priorLow}):
 * <pre>
 *   H4 = priorClose + R × 1.1 / 2
 *   H3 = priorClose + R × 1.1 / 4
 *   H2 = priorClose + R × 1.1 / 6
 *   H1 = priorClose + R × 1.1 / 12
 *   H5 = (priorHigh / priorLow) × priorClose       (trend-day extension)
 *   H6 = priorClose + (H5 − priorClose) × 1.168    (exhaustion)
 *   L1..L4 mirror H1..H4 with subtraction
 *   L5 = priorClose − (H5 − priorClose)
 *   L6 = priorClose − (H5 − priorClose) × 1.168
 *   PP = (priorHigh + priorLow + priorClose) / 3
 * </pre>
 */
public record CamarillaLevels(
    LocalDate sessionDate,    // today's IST date — the session these levels apply to
    LocalDate priorDate,      // the prior trading day whose OHLC was used as input
    double    priorHigh,
    double    priorLow,
    double    priorClose,
    double    pp,
    double    h1, double h2, double h3, double h4, double h5, double h6,
    double    l1, double l2, double l3, double l4, double l5, double l6
) {

    /** Compute from prior-day OHLC. */
    public static CamarillaLevels compute(LocalDate sessionDate, LocalDate priorDate,
                                          double high, double low, double close) {
        double range = high - low;
        double k = range * 1.1;
        double h1 = close + k / 12.0;
        double h2 = close + k / 6.0;
        double h3 = close + k / 4.0;
        double h4 = close + k / 2.0;
        double h5 = low > 0 ? (high / low) * close : close;
        double h6 = close + (h5 - close) * 1.168;
        double l1 = close - k / 12.0;
        double l2 = close - k / 6.0;
        double l3 = close - k / 4.0;
        double l4 = close - k / 2.0;
        double l5 = close - (h5 - close);
        double l6 = close - (h5 - close) * 1.168;
        double pp = (high + low + close) / 3.0;
        return new CamarillaLevels(sessionDate, priorDate, high, low, close, pp,
            round(h1), round(h2), round(h3), round(h4), round(h5), round(h6),
            round(l1), round(l2), round(l3), round(l4), round(l5), round(l6));
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
