package com.rydytrader.autotrader.util;

/**
 * Black-Scholes pricing helpers for European-style options. Used wherever the bot needs
 * greeks Fyers doesn't ship — currently the option-chain modal's delta column and the
 * strangle's delta-driven strike selection.
 *
 * <p>Conventions:
 * <ul>
 *   <li>{@code S} — underlying spot price (NIFTY index level)</li>
 *   <li>{@code K} — strike price</li>
 *   <li>{@code T} — time to expiry in YEARS (e.g. 7 days = 7/365)</li>
 *   <li>{@code r} — annualised continuously-compounded risk-free rate</li>
 *   <li>{@code sigma} — annualised volatility (decimal, e.g. 0.15 = 15 %)</li>
 *   <li>{@code isCall} — true for call options, false for puts</li>
 * </ul>
 *
 * <p>{@link #impliedVol} inverts the market price into a vol using bisection; pair it with
 * {@link #delta} to get the market-implied delta when greeks aren't directly available.
 */
public final class BlackScholes {

    /** India 10Y G-sec yield used as the default risk-free rate. NIFTY weekly delta is
     *  fairly insensitive to this — a 1 % rate swing barely moves delta. */
    public static final double DEFAULT_RISK_FREE_RATE = 0.065;

    private BlackScholes() {}

    /** BSM delta — {@code N(d1)} for calls, {@code N(d1) − 1} for puts. Returns the
     *  intrinsic-only sentinel when the inputs are degenerate. */
    public static double delta(double S, double K, double T, double r, double sigma, boolean isCall) {
        if (S <= 0 || K <= 0 || T <= 0 || sigma <= 0) {
            return isCall ? (S > K ? 1.0 : 0.0) : (S < K ? -1.0 : 0.0);
        }
        double d1 = (Math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
        return isCall ? normCdf(d1) : normCdf(d1) - 1.0;
    }

    /** BSM theoretical price — Black-Scholes formula for European calls/puts. */
    public static double price(double S, double K, double T, double r, double sigma, boolean isCall) {
        if (S <= 0 || K <= 0 || T <= 0 || sigma <= 0) {
            return Math.max(0, isCall ? S - K : K - S);
        }
        double d1 = (Math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
        double d2 = d1 - sigma * Math.sqrt(T);
        return isCall ? S * normCdf(d1) - K * Math.exp(-r * T) * normCdf(d2)
                      : K * Math.exp(-r * T) * normCdf(-d2) - S * normCdf(-d1);
    }

    /** Implied volatility from a market option price via 50-step bisection. Returns 0 when
     *  the market price sits at or below intrinsic (BSM can't price below intrinsic — typical
     *  for deeply ITM strikes in thin markets). */
    public static double impliedVol(double S, double K, double T, double r, double marketPrice, boolean isCall) {
        if (T <= 0 || marketPrice <= 0) return 0;
        double intrinsic = Math.max(0, isCall ? S - K * Math.exp(-r * T) : K * Math.exp(-r * T) - S);
        if (marketPrice <= intrinsic) return 0;
        double lo = 0.001, hi = 5.0;
        for (int i = 0; i < 50; i++) {
            double mid = (lo + hi) / 2;
            double p = price(S, K, T, r, mid, isCall);
            if (p > marketPrice) hi = mid; else lo = mid;
            if (hi - lo < 1e-4) break;
        }
        return (lo + hi) / 2;
    }

    /** Standard normal CDF via the Abramowitz-Stegun approximation (~1e-7 error). */
    public static double normCdf(double x) {
        double a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741,
               a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
        int sign = x < 0 ? -1 : 1;
        double ax = Math.abs(x) / Math.sqrt(2);
        double t = 1.0 / (1.0 + p * ax);
        double y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-ax * ax);
        return 0.5 * (1.0 + sign * y);
    }
}
