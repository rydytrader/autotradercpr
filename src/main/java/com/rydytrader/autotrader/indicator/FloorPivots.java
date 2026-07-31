package com.rydytrader.autotrader.indicator;

/**
 * Standard Floor Pivot Points — computed once at session start from the
 * previous day's H / L / C. Fixed for the entire day.
 *
 * <pre>
 * P  = (H + L + C) / 3
 * R1 = 2P - L                 S1 = 2P - H
 * R2 = P + (H - L)            S2 = P - (H - L)
 * R3 = R1 + (H - L)           S3 = S1 - (H - L)
 * </pre>
 *
 * <p>Framework rule (Ganesan playbook): a close ABOVE R1 confirms the market
 * has broken through its first structural ceiling for a bullish entry.
 * Symmetric — close below S1 confirms breakdown for a bearish entry. Deeper
 * levels (R2/R3, S2/S3) are exposed for future refinements but the current
 * fire gate only checks R1 / S1.
 */
public record FloorPivots(
    double p,
    double r1, double r2, double r3,
    double s1, double s2, double s3
) {

    /** Compute floor pivots from yesterday's high / low / close. */
    public static FloorPivots from(double high, double low, double close) {
        double p  = (high + low + close) / 3.0;
        double r1 = 2 * p - low;
        double s1 = 2 * p - high;
        double range = high - low;
        double r2 = p + range;
        double s2 = p - range;
        double r3 = r1 + range;
        double s3 = s1 - range;
        return new FloorPivots(p, r1, r2, r3, s1, s2, s3);
    }
}
