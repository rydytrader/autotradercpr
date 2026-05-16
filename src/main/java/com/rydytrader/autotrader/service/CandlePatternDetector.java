package com.rydytrader.autotrader.service;

/**
 * Stateless candle-pattern helpers used by {@link BreakoutScanner} for the CPR-level
 * retest-only entry model.
 *
 * <p>All thresholds are passed in by the caller so they can be tuned via the Candle
 * settings tab (see {@code RiskSettingsStore}).
 *
 * <ul>
 *   <li><b>Marubozu</b> — full-body conviction candle. body ≥ {@code bodyAtrMult} × ATR,
 *       total wicks (upper + lower) ≤ {@code maxWicksPctOfBody} × body. Used by the
 *       two-bar MARUBOZU_RETEST: prev bar carries the retest dip, current marubozu is
 *       the strong continuation.</li>
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

    // Pin bar test — two-path so very small bodies (where the body-relative multiplicative
    // test loses meaning) get evaluated geometrically against the bar's total range instead.
    //   • Normal-body path: original wick:body ratio test.
    //   • Small-body fallback: when body ≤ smallBodyMaxRangeRatio × range, require the
    //     rejection wick to dominate the range and the opposite wick to stay capped.
    //   • smallBodyMaxRangeRatio = 0 disables the fallback (and body=0 still returns false).

    public static boolean isBullishHammer(double open, double high, double low, double close,
                                          double rejectionWickBodyMult, double oppositeWickBodyMult,
                                          double smallBodyMaxRangeRatio,
                                          double dominantWickMinRangeRatio,
                                          double oppositeWickMaxRangeRatio) {
        double range = high - low;
        if (range <= 0) return false;
        double body = Math.abs(close - open);
        double upperWick = high - Math.max(open, close);
        double lowerWick = Math.min(open, close) - low;
        if (smallBodyMaxRangeRatio > 0 && body <= smallBodyMaxRangeRatio * range) {
            return lowerWick >= dominantWickMinRangeRatio * range
                && upperWick <= oppositeWickMaxRangeRatio * range;
        }
        if (body <= 0) return false;
        return lowerWick >= rejectionWickBodyMult * body
            && upperWick <= oppositeWickBodyMult * body;
    }

    public static boolean isShootingStar(double open, double high, double low, double close,
                                         double rejectionWickBodyMult, double oppositeWickBodyMult,
                                         double smallBodyMaxRangeRatio,
                                         double dominantWickMinRangeRatio,
                                         double oppositeWickMaxRangeRatio) {
        double range = high - low;
        if (range <= 0) return false;
        double body = Math.abs(close - open);
        double upperWick = high - Math.max(open, close);
        double lowerWick = Math.min(open, close) - low;
        if (smallBodyMaxRangeRatio > 0 && body <= smallBodyMaxRangeRatio * range) {
            return upperWick >= dominantWickMinRangeRatio * range
                && lowerWick <= oppositeWickMaxRangeRatio * range;
        }
        if (body <= 0) return false;
        return upperWick >= rejectionWickBodyMult * body
            && lowerWick <= oppositeWickBodyMult * body;
    }

    // ── Outside Reversal (unified 2-bar body reversal) ──────────────────────
    // Replaces Engulfing + Piercing/Dark Cloud + Tweezer Top/Bottom. Strict color flip,
    // shared body band on both bars, configurable penetration into bar 1's body:
    //   • penetration 1.0 = strict classical engulfing (close past bar 1's open)
    //   • penetration 0.5 = covers piercing AND engulfing (close past bar 1's midpoint)
    //   • penetration 0.0 = any directional recovery past bar 1's close

    public static boolean isBullishOutsideReversal(CandleAggregator.CandleBar prev,
                                                   CandleAggregator.CandleBar curr, double atr,
                                                   double minBodyAtrMult, double maxBodyAtrMult,
                                                   double penetrationPct) {
        if (prev == null || curr == null || atr <= 0) return false;
        if (!(prev.close < prev.open)) return false;                // bar 1 red
        if (!(curr.close > curr.open)) return false;                // bar 2 green (color flip)
        double prevBody = prev.open - prev.close;
        double currBody = curr.close - curr.open;
        if (minBodyAtrMult > 0) {
            if (prevBody < minBodyAtrMult * atr) return false;
            if (currBody < minBodyAtrMult * atr) return false;
        }
        if (maxBodyAtrMult > 0) {
            if (prevBody > maxBodyAtrMult * atr) return false;
            if (currBody > maxBodyAtrMult * atr) return false;
        }
        if (!(curr.open <= prev.close)) return false;               // bar 2 opens at/below bar 1's close
        double threshold = prev.close + penetrationPct * prevBody;
        return curr.close >= threshold;                             // closes ≥ N% into bar 1's body
    }

    public static boolean isBearishOutsideReversal(CandleAggregator.CandleBar prev,
                                                   CandleAggregator.CandleBar curr, double atr,
                                                   double minBodyAtrMult, double maxBodyAtrMult,
                                                   double penetrationPct) {
        if (prev == null || curr == null || atr <= 0) return false;
        if (!(prev.close > prev.open)) return false;                // bar 1 green
        if (!(curr.close < curr.open)) return false;                // bar 2 red (color flip)
        double prevBody = prev.close - prev.open;
        double currBody = curr.open - curr.close;
        if (minBodyAtrMult > 0) {
            if (prevBody < minBodyAtrMult * atr) return false;
            if (currBody < minBodyAtrMult * atr) return false;
        }
        if (maxBodyAtrMult > 0) {
            if (prevBody > maxBodyAtrMult * atr) return false;
            if (currBody > maxBodyAtrMult * atr) return false;
        }
        if (!(curr.open >= prev.close)) return false;               // bar 2 opens at/above bar 1's close
        double threshold = prev.close - penetrationPct * prevBody;
        return curr.close <= threshold;
    }

    // ── Doji reversal (2-bar) ────────────────────────────────────────────────

    // bar 1 (prev) = doji (indecision at the level); bar 2 (curr) = strong directional
    // confirmation. Bullish reversal: doji → strong green. Bearish reversal: doji → strong red.
    // confirmBodyAtrMult sizes the confirmation bar (legacy setting: dojiPrevBodyAtrMult).

    public static boolean isBullishDojiReversal(CandleAggregator.CandleBar prev,
                                                CandleAggregator.CandleBar curr, double atr,
                                                double dojiBodyMaxRangeRatio,
                                                double confirmBodyAtrMult,
                                                double confirmMaxBodyAtrMult) {
        if (prev == null || curr == null || atr <= 0) return false;
        double prevRange = prev.high - prev.low;
        if (prevRange <= 0) return false;
        double prevBody = Math.abs(prev.close - prev.open);
        if (prevBody > dojiBodyMaxRangeRatio * prevRange) return false;   // prev is doji
        if (!(curr.close > curr.open)) return false;                       // curr is green
        double currBody = curr.close - curr.open;
        if (currBody < confirmBodyAtrMult * atr) return false;             // curr is strong
        if (confirmMaxBodyAtrMult > 0 && currBody > confirmMaxBodyAtrMult * atr) return false; // not exhaustion
        // Confirmation must close past the doji's HIGH (not just the doji body) — hardcoded
        // rule. Closes above the entire prior bar's range, including any upper wick.
        return curr.close > prev.high;
    }

    public static boolean isBearishDojiReversal(CandleAggregator.CandleBar prev,
                                                CandleAggregator.CandleBar curr, double atr,
                                                double dojiBodyMaxRangeRatio,
                                                double confirmBodyAtrMult,
                                                double confirmMaxBodyAtrMult) {
        if (prev == null || curr == null || atr <= 0) return false;
        double prevRange = prev.high - prev.low;
        if (prevRange <= 0) return false;
        double prevBody = Math.abs(prev.close - prev.open);
        if (prevBody > dojiBodyMaxRangeRatio * prevRange) return false;   // prev is doji
        if (!(curr.close < curr.open)) return false;                       // curr is red
        double currBody = curr.open - curr.close;
        if (currBody < confirmBodyAtrMult * atr) return false;             // curr is strong
        if (confirmMaxBodyAtrMult > 0 && currBody > confirmMaxBodyAtrMult * atr) return false; // not exhaustion
        // Confirmation must close past the doji's LOW (not just the doji body) — hardcoded
        // rule. Closes below the entire prior bar's range, including any lower wick.
        return curr.close < prev.low;
    }

    // ── Three Inside Up / Down (3-bar harami + confirmation) ────────────────

    public static boolean isThreeInsideUp(CandleAggregator.CandleBar bar1,
                                          CandleAggregator.CandleBar bar2,
                                          CandleAggregator.CandleBar bar3, double atr,
                                          double bodyAtrMult, double bodyMaxAtrMult,
                                          double bar3PenetrationPct) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        if (!(bar1.close < bar1.open)) return false;
        if (!(bar2.close > bar2.open)) return false;
        if (!(bar3.close > bar3.open)) return false;
        double bar1Body = bar1.open - bar1.close;
        double bar3Body = bar3.close - bar3.open;
        // Shared body band applied to BOTH bar 1 AND bar 3 (mirrors Morning Star's
        // symmetric outer-bar band).
        if (bodyAtrMult > 0) {
            if (bar1Body < bodyAtrMult * atr) return false;
            if (bar3Body < bodyAtrMult * atr) return false;
        }
        if (bodyMaxAtrMult > 0) {
            if (bar1Body > bodyMaxAtrMult * atr) return false;
            if (bar3Body > bodyMaxAtrMult * atr) return false;
        }
        // Bar 2: body fully INSIDE bar 1's body (classical harami containment).
        if (bar2.open < bar1.close || bar2.close > bar1.open) return false;
        // Bar 3 closes past a configurable penetration point into bar 1's body.
        // 0.0 = past bar 1's close; 0.5 = midpoint; 1.0 = past bar 1's open.
        double threshold = bar1.close + bar3PenetrationPct * (bar1.open - bar1.close);
        return bar3.close >= threshold;
    }

    public static boolean isThreeInsideDown(CandleAggregator.CandleBar bar1,
                                            CandleAggregator.CandleBar bar2,
                                            CandleAggregator.CandleBar bar3, double atr,
                                            double bodyAtrMult, double bodyMaxAtrMult,
                                            double bar3PenetrationPct) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        if (!(bar1.close > bar1.open)) return false;
        if (!(bar2.close < bar2.open)) return false;
        if (!(bar3.close < bar3.open)) return false;
        double bar1Body = bar1.close - bar1.open;
        double bar3Body = bar3.open - bar3.close;
        if (bodyAtrMult > 0) {
            if (bar1Body < bodyAtrMult * atr) return false;
            if (bar3Body < bodyAtrMult * atr) return false;
        }
        if (bodyMaxAtrMult > 0) {
            if (bar1Body > bodyMaxAtrMult * atr) return false;
            if (bar3Body > bodyMaxAtrMult * atr) return false;
        }
        if (bar2.open > bar1.close || bar2.close < bar1.open) return false;
        double threshold = bar1.close - bar3PenetrationPct * (bar1.close - bar1.open);
        return bar3.close <= threshold;
    }

    // ── Morning / Evening star (3-bar) ───────────────────────────────────────

    public static boolean isMorningStar(CandleAggregator.CandleBar bar1,
                                        CandleAggregator.CandleBar bar2,
                                        CandleAggregator.CandleBar bar3, double atr,
                                        double outerBodyAtrMult,
                                        double outerMaxBodyAtrMult,
                                        double middleBodyMaxMultOfOuter,
                                        double bar3PenetrationPct) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        if (!(bar1.close < bar1.open)) return false;
        double bar1Body = bar1.open - bar1.close;
        if (bar1Body < outerBodyAtrMult * atr) return false;
        if (outerMaxBodyAtrMult > 0 && bar1Body > outerMaxBodyAtrMult * atr) return false;
        double bar2Body = Math.abs(bar2.close - bar2.open);
        if (bar2Body > middleBodyMaxMultOfOuter * bar1Body) return false;
        if (!(bar3.close > bar3.open)) return false;
        double bar3Body = bar3.close - bar3.open;
        if (bar3Body < outerBodyAtrMult * atr) return false;
        if (outerMaxBodyAtrMult > 0 && bar3Body > outerMaxBodyAtrMult * atr) return false;
        // Bar 3 close must reach a configurable penetration point into bar 1's red body.
        // 0.0 = past bar 1's close; 0.5 = past midpoint (Nison default); 1.0 = past bar 1's open.
        double threshold = bar1.close + bar3PenetrationPct * (bar1.open - bar1.close);
        return bar3.close >= threshold;
    }

    public static boolean isEveningStar(CandleAggregator.CandleBar bar1,
                                        CandleAggregator.CandleBar bar2,
                                        CandleAggregator.CandleBar bar3, double atr,
                                        double outerBodyAtrMult,
                                        double outerMaxBodyAtrMult,
                                        double middleBodyMaxMultOfOuter,
                                        double bar3PenetrationPct) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        if (!(bar1.close > bar1.open)) return false;
        double bar1Body = bar1.close - bar1.open;
        if (bar1Body < outerBodyAtrMult * atr) return false;
        if (outerMaxBodyAtrMult > 0 && bar1Body > outerMaxBodyAtrMult * atr) return false;
        double bar2Body = Math.abs(bar2.close - bar2.open);
        if (bar2Body > middleBodyMaxMultOfOuter * bar1Body) return false;
        if (!(bar3.close < bar3.open)) return false;
        double bar3Body = bar3.open - bar3.close;
        if (bar3Body < outerBodyAtrMult * atr) return false;
        if (outerMaxBodyAtrMult > 0 && bar3Body > outerMaxBodyAtrMult * atr) return false;
        // Mirror of Morning Star — penetrate down into bar 1's green body. 0 = past close,
        // 0.5 = midpoint, 1.0 = past open.
        double threshold = bar1.close - bar3PenetrationPct * (bar1.close - bar1.open);
        return bar3.close <= threshold;
    }
}
