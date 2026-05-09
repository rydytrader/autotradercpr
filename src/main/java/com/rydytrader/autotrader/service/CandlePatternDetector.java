package com.rydytrader.autotrader.service;

/**
 * Stateless candle-pattern helpers used by {@link BreakoutScanner} for the CPR-level
 * marubozu-breakout + pattern-retest entry model.
 *
 * <p>All thresholds are hard-wired (no settings). Each method takes primitive doubles
 * (or {@link CandleAggregator.CandleBar}) — no Spring deps, easy to unit-test.
 *
 * <p>Sizing references:
 * <ul>
 *   <li><b>Marubozu</b> — full-body conviction candle. body ≥ 1 × ATR, total wicks
 *       ≤ 10% of body.</li>
 *   <li><b>Hammer / shooting star</b> — pin-bar reversal. Rejection wick ≥ 2 × body,
 *       opposite wick ≤ 0.3 × body, real body present (not a doji).</li>
 *   <li><b>Engulfing</b> — current body engulfs prior body and is at least as large.</li>
 *   <li><b>Doji reversal</b> — current candle's body ≤ 10% of range, prior candle is
 *       a meaningful (≥ 0.5 × ATR body) opposite-direction bar.</li>
 *   <li><b>Morning / evening star</b> — 3-bar reversal. Bars 1 and 3 strong (≥ 0.5 × ATR
 *       body), bar 2 small (≤ 0.3 × bar1 body), bar 3 closes past bar 1's midpoint.</li>
 * </ul>
 */
final class CandlePatternDetector {

    private CandlePatternDetector() {}

    // ── Marubozu ──────────────────────────────────────────────────────────────

    public static boolean isBullishMarubozu(double open, double high, double low, double close, double atr) {
        if (atr <= 0) return false;
        if (close <= open) return false;
        double body = close - open;
        if (body < atr) return false;                           // body ≥ 1 × ATR
        double wicks = (high - close) + (open - low);
        return wicks <= 0.10 * body;                            // wicks ≤ 10% of body
    }

    public static boolean isBearishMarubozu(double open, double high, double low, double close, double atr) {
        if (atr <= 0) return false;
        if (close >= open) return false;
        double body = open - close;
        if (body < atr) return false;
        double wicks = (high - open) + (close - low);
        return wicks <= 0.10 * body;
    }

    // ── Pin bar (hammer / shooting star) ─────────────────────────────────────

    public static boolean isBullishHammer(double open, double high, double low, double close) {
        double body = Math.abs(close - open);
        if (body <= 0) return false;                            // not a doji
        double upperWick = high - Math.max(open, close);
        double lowerWick = Math.min(open, close) - low;
        return lowerWick >= 2.0 * body && upperWick <= 0.3 * body;
    }

    public static boolean isShootingStar(double open, double high, double low, double close) {
        double body = Math.abs(close - open);
        if (body <= 0) return false;
        double upperWick = high - Math.max(open, close);
        double lowerWick = Math.min(open, close) - low;
        return upperWick >= 2.0 * body && lowerWick <= 0.3 * body;
    }

    // ── Engulfing ─────────────────────────────────────────────────────────────

    public static boolean isBullishEngulfing(CandleAggregator.CandleBar prev, CandleAggregator.CandleBar curr) {
        if (prev == null || curr == null) return false;
        if (!(prev.close < prev.open)) return false;            // prev red
        if (!(curr.close > curr.open)) return false;            // current green
        double prevBody = prev.open - prev.close;
        double currBody = curr.close - curr.open;
        if (currBody < prevBody) return false;
        return curr.open <= prev.close && curr.close >= prev.open;
    }

    public static boolean isBearishEngulfing(CandleAggregator.CandleBar prev, CandleAggregator.CandleBar curr) {
        if (prev == null || curr == null) return false;
        if (!(prev.close > prev.open)) return false;            // prev green
        if (!(curr.close < curr.open)) return false;            // current red
        double prevBody = prev.close - prev.open;
        double currBody = curr.open - curr.close;
        if (currBody < prevBody) return false;
        return curr.open >= prev.close && curr.close <= prev.open;
    }

    // ── Doji reversal (2-bar) ────────────────────────────────────────────────

    public static boolean isBullishDojiReversal(CandleAggregator.CandleBar prev, CandleAggregator.CandleBar curr, double atr) {
        if (prev == null || curr == null || atr <= 0) return false;
        double currRange = curr.high - curr.low;
        if (currRange <= 0) return false;
        double currBody = Math.abs(curr.close - curr.open);
        if (currBody > 0.10 * currRange) return false;          // current is a doji
        if (!(prev.close < prev.open)) return false;            // prev red
        double prevBody = prev.open - prev.close;
        return prevBody >= 0.5 * atr;                           // prev was meaningful
    }

    public static boolean isBearishDojiReversal(CandleAggregator.CandleBar prev, CandleAggregator.CandleBar curr, double atr) {
        if (prev == null || curr == null || atr <= 0) return false;
        double currRange = curr.high - curr.low;
        if (currRange <= 0) return false;
        double currBody = Math.abs(curr.close - curr.open);
        if (currBody > 0.10 * currRange) return false;
        if (!(prev.close > prev.open)) return false;            // prev green
        double prevBody = prev.close - prev.open;
        return prevBody >= 0.5 * atr;
    }

    // ── Morning / Evening star (3-bar) ───────────────────────────────────────

    public static boolean isMorningStar(CandleAggregator.CandleBar bar1, CandleAggregator.CandleBar bar2,
                                        CandleAggregator.CandleBar bar3, double atr) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        // bar1 strong red
        if (!(bar1.close < bar1.open)) return false;
        double bar1Body = bar1.open - bar1.close;
        if (bar1Body < 0.5 * atr) return false;
        // bar2 small body (any color)
        double bar2Body = Math.abs(bar2.close - bar2.open);
        if (bar2Body > 0.3 * bar1Body) return false;
        // bar3 strong green closing past bar1's midpoint
        if (!(bar3.close > bar3.open)) return false;
        double bar3Body = bar3.close - bar3.open;
        if (bar3Body < 0.5 * atr) return false;
        double bar1Mid = (bar1.open + bar1.close) / 2.0;
        return bar3.close > bar1Mid;
    }

    public static boolean isEveningStar(CandleAggregator.CandleBar bar1, CandleAggregator.CandleBar bar2,
                                        CandleAggregator.CandleBar bar3, double atr) {
        if (bar1 == null || bar2 == null || bar3 == null || atr <= 0) return false;
        // bar1 strong green
        if (!(bar1.close > bar1.open)) return false;
        double bar1Body = bar1.close - bar1.open;
        if (bar1Body < 0.5 * atr) return false;
        // bar2 small body
        double bar2Body = Math.abs(bar2.close - bar2.open);
        if (bar2Body > 0.3 * bar1Body) return false;
        // bar3 strong red closing past bar1's midpoint
        if (!(bar3.close < bar3.open)) return false;
        double bar3Body = bar3.open - bar3.close;
        if (bar3Body < 0.5 * atr) return false;
        double bar1Mid = (bar1.open + bar1.close) / 2.0;
        return bar3.close < bar1Mid;
    }
}
