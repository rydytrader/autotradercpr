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
 *   BC = (priorHigh + priorLow) / 2                (bottom central)
 *   TC = 2 × PP − BC                                (top central)
 *   cprWidth    = |TC − BC|
 *   cprWidthPct = cprWidth / PP × 100
 * </pre>
 *
 * <p>{@link #widthClass} / {@link #valueClass} / {@link #cprWidthPercentile}
 * are the daily context signals used for probability-based lot sizing.
 * Populated by CamarillaService via {@link #withClassification} once the
 * 100-day CPR-width history for this instrument is available. Null on
 * bare-boot before history has loaded.
 */
public record CamarillaLevels(
    LocalDate sessionDate,    // today's IST date — the session these levels apply to
    LocalDate priorDate,      // the prior trading day whose OHLC was used as input
    double    priorHigh,
    double    priorLow,
    double    priorClose,
    double    pp,
    double    h1, double h2, double h3, double h4, double h5, double h6,
    double    l1, double l2, double l3, double l4, double l5, double l6,
    // Central Pivot Range boundaries
    double    bc,
    double    tc,
    double    cprWidth,
    double    cprWidthPct,
    // Daily classification (nullable — populated by CamarillaService once
    // the 100-day history is loaded; null before that)
    Double    cprWidthPercentile,   // 0..100 rank of today's cprWidth vs last 100 days
    String    widthClass,           // "WIDE" / "NORMAL" / "NARROW"
    String    valueClass,           // "INSIDE_VALUE" / "OVERLAP" / "OUTSIDE_VALUE"
    /** Consolidated CPR classification — one of NARROW+INSIDE / NARROW /
     *  WIDE+OUTSIDE / WIDE / NORMAL. Derived from {@link #widthClass} +
     *  {@link #valueClass}. This is the label the modal + sizing bias
     *  actually consume; widthClass/valueClass/percentile are kept for
     *  the hover tooltip and log lines. */
    String    cprClass
) {

    /** Compute from prior-day OHLC. Classification fields start null and
     *  are set later via {@link #withClassification}. */
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
        double bc = (high + low) / 2.0;
        double tc = 2.0 * pp - bc;
        double cprWidth = Math.abs(tc - bc);
        double cprWidthPct = pp != 0 ? (cprWidth / pp) * 100.0 : 0.0;
        return new CamarillaLevels(sessionDate, priorDate, high, low, close, pp,
            round(h1), round(h2), round(h3), round(h4), round(h5), round(h6),
            round(l1), round(l2), round(l3), round(l4), round(l5), round(l6),
            round(bc), round(tc), cprWidth, cprWidthPct,
            null, null, null, null);
    }

    /** Return a copy with the classification fields set + the consolidated
     *  {@code cprClass} label derived from width + value. Called by
     *  CamarillaService after loading the 100-day CPR-width history. */
    public CamarillaLevels withClassification(Double cprWidthPercentile,
                                              String widthClass, String valueClass) {
        return new CamarillaLevels(sessionDate, priorDate, priorHigh, priorLow, priorClose, pp,
            h1, h2, h3, h4, h5, h6,
            l1, l2, l3, l4, l5, l6,
            bc, tc, cprWidth, cprWidthPct,
            cprWidthPercentile, widthClass, valueClass,
            consolidatedClass(widthClass, valueClass));
    }

    /** Collapse the two-axis width × value classification to a single label.
     *  <pre>
     *    NARROW + INSIDE_VALUE  → "NARROW+INSIDE"
     *    NARROW                 → "NARROW"
     *    WIDE   + OUTSIDE_VALUE → "WIDE+OUTSIDE"
     *    WIDE                   → "WIDE"
     *    anything else          → "NORMAL"
     *  </pre>
     *  Sizing bias in Camarilla.biasedLotsForCpr reads this label directly:
     *  NARROW* → breakouts favored (full lots, reversals half);
     *  WIDE* → reversals favored; NORMAL → all setups full. */
    private static String consolidatedClass(String widthClass, String valueClass) {
        if (widthClass == null) return null;
        boolean isInside  = "INSIDE_VALUE".equals(valueClass);
        boolean isOutside = "OUTSIDE_VALUE".equals(valueClass);
        if ("NARROW".equals(widthClass)) return isInside  ? "NARROW+INSIDE"  : "NARROW";
        if ("WIDE".equals(widthClass))   return isOutside ? "WIDE+OUTSIDE"   : "WIDE";
        return "NORMAL";
    }

    /** NIFTY tick size is ₹0.05 — round each computed pivot to the nearest tick so the
     *  displayed value matches a tradeable price step (24656.78 → 24656.80). */
    private static double round(double v) {
        return Math.round(v * 20.0) / 20.0;
    }
}
