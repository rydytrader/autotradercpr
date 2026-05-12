package com.rydytrader.autotrader.service;

/**
 * Stateless candle-pattern helpers used by {@link BreakoutScanner} for the CPR-level
 * retest-only entry model.
 *
 * <p>All thresholds are passed in by the caller so they can be tuned via the Candle
 * settings tab (see {@code RiskSettingsStore}).
 *
 * <ul>
 *   <li><b>Hammer / shooting star (pin bar)</b> — rejection wick ≥
 *       {@code rejectionWickBodyMult} × body, opposite wick ≤
 *       {@code oppositeWickBodyMult} × body, real body present (not a doji).</li>
 *   <li><b>Engulfing</b> — current body ≥ {@code minBodyMultiple} × prev body, current
 *       body ≥ {@code minBodyAtrMult} × ATR (absolute size floor — prevents firing on
 *       two tiny consecutive bars), and current body fully engulfs prior body.</li>
 *   <li><b>Piercing line / Dark cloud cover</b> — 2-bar partial reversal that doesn't
 *       fully engulf. Bar 1 body ≥ {@code prevBodyAtrMult} × ATR; bar 2 opens past
 *       bar 1's close in the reversal direction; bar 2 closes at least
 *       {@code penetrationPct} of the way into bar 1's body but does NOT engulf.</li>
 *   <li><b>Tweezer top / bottom</b> — 2-bar matched-extreme reversal. Bar 1 strong
 *       directional (body ≥ {@code prevBodyAtrMult} × ATR) with strict color flip into
 *       bar 2. Matching extremes (highs for top, lows for bottom) within
 *       {@code matchAtr} × ATR tolerance. Bar 2 body unconstrained — the matched
 *       extreme is the signature.</li>
 *   <li><b>Three Inside Up / Down</b> — 3-bar harami + confirmation. Bar 1 large
 *       directional (body ≥ {@code bodyAtrMult} × ATR), bar 2 opposite color with body
 *       FULLY INSIDE bar 1's body and body ≤ {@code innerBodyMaxRatio} × bar 1 body
 *       (harami constraint), bar 3 closes past bar 1's open in the reversal direction
 *       (full confirmation).</li>
 *   <li><b>Doji reversal</b> — current candle's body ≤
 *       {@code dojiBodyMaxRangeRatio} × range; prior candle is a meaningful
 *       (≥ {@code prevBodyAtrMult} × ATR body) opposite-direction bar.</li>
 *   <li><b>Morning / evening star</b> — 3-bar reversal. Bars 1 and 3 strong
 *       (body ≥ {@code outerBodyAtrMult} × ATR), bar 2 small
 *       (body ≤ {@code middleBodyMaxMultOfOuter} × bar1 body), bar 3 closes past
 *       bar 1's midpoint.</li>
 * </ul>
 */
final class CandlePatternDetector {

    private CandlePatternDetector() {}

    // ── Pin bar (hammer / shooting star) ─────────────────────────────────────

    public static boolean isBullishHammer(double open, double high, double low, double close,
                                          double rejectionWickBodyMult, double oppositeWickBodyMult) {
        double body = Math.abs(close - open);
        if (body <= 0) return false;
        double upperWick = high - Math.max(open, close);
        double lowerWick = Math.min(open, close) - low;
        return lowerWick >= rejectionWickBodyMult * body
            && upperWick <= oppositeWickBodyMult * body;
    }

    public static boolean isShootingStar(double open, double high, double low, double close,
                                         double rejectionWickBodyMult, double oppositeWickBodyMult) {
        double body = Math.abs(close - open);
        if (body <= 0) return false;
        double upperWick = high - Math.max(open, close);
        double lowerWick = Math.min(open, close) - low;
        return upperWick >= rejectionWickBodyMult * body
            && lowerWick <= oppositeWickBodyMult * body;
    }

    // ── Engulfing ─────────────────────────────────────────────────────────────

    public static boolean isBullishEngulfing(CandleAggregator.CandleBar prev,
                                             CandleAggregator.CandleBar curr,
                                             double minBodyMultiple, double atr,
                                             double minBodyAtrMult) {
        if (prev == null || curr == null) return false;
        if (!(prev.close < prev.open)) return false;
        if (!(curr.close > curr.open)) return false;
        double prevBody = prev.open - prev.close;
        double currBody = curr.close - curr.open;
        if (currBody < minBodyMultiple * prevBody) return false;
        if (minBodyAtrMult > 0 && atr > 0 && currBody < minBodyAtrMult * atr) return false;
        return curr.open <= prev.close && curr.close >= prev.open;
    }

    public static boolean isBearishEngulfing(CandleAggregator.CandleBar prev,
                                             CandleAggregator.CandleBar curr,
                                             double minBodyMultiple, double atr,
                                             double minBodyAtrMult) {
        if (prev == null || curr == null) return false;
        if (!(prev.close > prev.open)) return false;
        if (!(curr.close < curr.open)) return false;
        double prevBody = prev.close - prev.open;
        double currBody = curr.open - curr.close;
        if (currBody < minBodyMultiple * prevBody) return false;
        if (minBodyAtrMult > 0 && atr > 0 && currBody < minBodyAtrMult * atr) return false;
        return curr.open >= prev.close && curr.close <= prev.open;
    }

    // ── Piercing line / Dark cloud cover (2-bar partial reversal) ────────────

    public static boolean isPiercingLine(CandleAggregator.CandleBar prev,
                                         CandleAggregator.CandleBar curr, double atr,
                                         double prevBodyAtrMult, double penetrationPct) {
        if (prev == null || curr == null || atr <= 0) return false;
        if (!(prev.close < prev.open)) return false;                // bar 1 red
        double prevBody = prev.open - prev.close;
        if (prevBody < prevBodyAtrMult * atr) return false;         // bar 1 meaningful
        if (!(curr.close > curr.open)) return false;                // bar 2 green
        if (!(curr.open < prev.close)) return false;                // opens past prev close
        double penetrationLevel = prev.close + penetrationPct * prevBody;
        if (curr.close < penetrationLevel) return false;            // closes ≥ N% into bar 1
        return curr.close < prev.open;                              // does NOT engulf
    }

    public static boolean isDarkCloudCover(CandleAggregator.CandleBar prev,
                                           CandleAggregator.CandleBar curr, double atr,
                                           double prevBodyAtrMult, double penetrationPct) {
        if (prev == null || curr == null || atr <= 0) return false;
        if (!(prev.close > prev.open)) return false;                // bar 1 green
        double prevBody = prev.close - prev.open;
        if (prevBody < prevBodyAtrMult * atr) return false;
        if (!(curr.close < curr.open)) return false;                // bar 2 red
        if (!(curr.open > prev.close)) return false;                // opens past prev close
        double penetrationLevel = prev.close - penetrationPct * prevBody;
        if (curr.close > penetrationLevel) return false;            // closes ≥ N% into bar 1
        return curr.close > prev.open;                              // does NOT engulf
    }

    // ── Tweezer top / bottom (2-bar matched-extreme reversal) ───────────────

    public static boolean isTweezerBottom(CandleAggregator.CandleBar prev,
                                          CandleAggregator.CandleBar curr, double atr,
                                          double prevBodyAtrMult, double matchAtr) {
        if (prev == null || curr == null || atr <= 0) return false;
        if (!(prev.close < prev.open)) return false;                // bar 1 red
        double prevBody = prev.open - prev.close;
        if (prevBody < prevBodyAtrMult * atr) return false;         // bar 1 strong
        if (!(curr.close > curr.open)) return false;                // bar 2 green (color flip)
        double tolerance = matchAtr * atr;
        return Math.abs(prev.low - curr.low) <= tolerance;          // matching lows
    }

    public static boolean isTweezerTop(CandleAggregator.CandleBar prev,
                                       CandleAggregator.CandleBar curr, double atr,
                                       double prevBodyAtrMult, double matchAtr) {
        if (prev == null || curr == null || atr <= 0) return false;
        if (!(prev.close > prev.open)) return false;                // bar 1 green
        double prevBody = prev.close - prev.open;
        if (prevBody < prevBodyAtrMult * atr) return false;
        if (!(curr.close < curr.open)) return false;                // bar 2 red (color flip)
        double tolerance = matchAtr * atr;
        return Math.abs(prev.high - curr.high) <= tolerance;        // matching highs
    }

    // ── Doji reversal (2-bar) ────────────────────────────────────────────────

    public static boolean isBullishDojiReversal(CandleAggregator.CandleBar prev,
                                                CandleAggregator.CandleBar curr, double atr,
                                                double dojiBodyMaxRangeRatio,
                                                double prevBodyAtrMult) {
        if (prev == null || curr == null || atr <= 0) return false;
        double currRange = curr.high - curr.low;
        if (currRange <= 0) return false;
        double currBody = Math.abs(curr.close - curr.open);
        if (currBody > dojiBodyMaxRangeRatio * currRange) return false;
        if (!(prev.close < prev.open)) return false;
        double prevBody = prev.open - prev.close;
        return prevBody >= prevBodyAtrMult * atr;
    }

    public static boolean isBearishDojiReversal(CandleAggregator.CandleBar prev,
                                                CandleAggregator.CandleBar curr, double atr,
                                                double dojiBodyMaxRangeRatio,
                                                double prevBodyAtrMult) {
        if (prev == null || curr == null || atr <= 0) return false;
        double currRange = curr.high - curr.low;
        if (currRange <= 0) return false;
        double currBody = Math.abs(curr.close - curr.open);
        if (currBody > dojiBodyMaxRangeRatio * currRange) return false;
        if (!(prev.close > prev.open)) return false;
        double prevBody = prev.close - prev.open;
        return prevBody >= prevBodyAtrMult * atr;
    }

    // ── Three Inside Up / Down (3-bar harami + confirmation) ────────────────

    public static boolean isThreeInsideUp(CandleAggregator.CandleBar bar1,
                                          CandleAggregator.CandleBar bar2,
                                          CandleAggregator.CandleBar bar3, double atr,
                                          double bodyAtrMult, double innerBodyMaxRatio) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        // Bar 1: large red.
        if (!(bar1.close < bar1.open)) return false;
        double bar1Body = bar1.open - bar1.close;
        if (bar1Body < bodyAtrMult * atr) return false;
        // Bar 2: green, body fully inside bar 1's body, and small relative to bar 1.
        if (!(bar2.close > bar2.open)) return false;
        double bar2Body = bar2.close - bar2.open;
        if (bar2Body > innerBodyMaxRatio * bar1Body) return false;
        if (bar2.open < bar1.close || bar2.close > bar1.open) return false;
        // Bar 3: green, closes past bar 1's open (full confirmation of reversal).
        if (!(bar3.close > bar3.open)) return false;
        return bar3.close > bar1.open;
    }

    public static boolean isThreeInsideDown(CandleAggregator.CandleBar bar1,
                                            CandleAggregator.CandleBar bar2,
                                            CandleAggregator.CandleBar bar3, double atr,
                                            double bodyAtrMult, double innerBodyMaxRatio) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        if (!(bar1.close > bar1.open)) return false;
        double bar1Body = bar1.close - bar1.open;
        if (bar1Body < bodyAtrMult * atr) return false;
        if (!(bar2.close < bar2.open)) return false;
        double bar2Body = bar2.open - bar2.close;
        if (bar2Body > innerBodyMaxRatio * bar1Body) return false;
        if (bar2.open > bar1.close || bar2.close < bar1.open) return false;
        if (!(bar3.close < bar3.open)) return false;
        return bar3.close < bar1.open;
    }

    // ── Morning / Evening star (3-bar) ───────────────────────────────────────

    public static boolean isMorningStar(CandleAggregator.CandleBar bar1,
                                        CandleAggregator.CandleBar bar2,
                                        CandleAggregator.CandleBar bar3, double atr,
                                        double outerBodyAtrMult,
                                        double middleBodyMaxMultOfOuter) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        if (!(bar1.close < bar1.open)) return false;
        double bar1Body = bar1.open - bar1.close;
        if (bar1Body < outerBodyAtrMult * atr) return false;
        double bar2Body = Math.abs(bar2.close - bar2.open);
        if (bar2Body > middleBodyMaxMultOfOuter * bar1Body) return false;
        if (!(bar3.close > bar3.open)) return false;
        double bar3Body = bar3.close - bar3.open;
        if (bar3Body < outerBodyAtrMult * atr) return false;
        double bar1Mid = (bar1.open + bar1.close) / 2.0;
        return bar3.close > bar1Mid;
    }

    public static boolean isEveningStar(CandleAggregator.CandleBar bar1,
                                        CandleAggregator.CandleBar bar2,
                                        CandleAggregator.CandleBar bar3, double atr,
                                        double outerBodyAtrMult,
                                        double middleBodyMaxMultOfOuter) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        if (!(bar1.close > bar1.open)) return false;
        double bar1Body = bar1.close - bar1.open;
        if (bar1Body < outerBodyAtrMult * atr) return false;
        double bar2Body = Math.abs(bar2.close - bar2.open);
        if (bar2Body > middleBodyMaxMultOfOuter * bar1Body) return false;
        if (!(bar3.close < bar3.open)) return false;
        double bar3Body = bar3.open - bar3.close;
        if (bar3Body < outerBodyAtrMult * atr) return false;
        double bar1Mid = (bar1.open + bar1.close) / 2.0;
        return bar3.close < bar1Mid;
    }
}
