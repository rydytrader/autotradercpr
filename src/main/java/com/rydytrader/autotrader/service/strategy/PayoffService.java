package com.rydytrader.autotrader.service.strategy;

import com.rydytrader.autotrader.util.BlackScholes;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Pure payoff maths for the SIMULATE modal on the POSITIONS page. Takes a basket of option
 * legs (CE / PE, BUY / SELL, any expiry) and returns the expiry payoff curve, a T+n curve priced
 * with Black-Scholes, breakevens, max profit / loss, net credit, probability of profit, expected
 * P&amp;L, net greeks and the ±1σ / ±2σ expected range. No broker calls, no side effects.
 *
 * <p>Conventions: {@code sign} = +1 for BUY, −1 for SELL; P&amp;L of a leg = sign × (value − price)
 * × qty. The expiry curve is drawn at the <b>nearest</b> expiry in the basket; legs expiring later
 * are valued with Black-Scholes at that date (same as Opstra). POP uses a lognormal terminal
 * distribution with drift r and σ = qty-weighted implied vol of the legs.
 */
@Service
public class PayoffService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime SETTLE = LocalTime.of(15, 30);
    private static final int GRID_POINTS = 240;
    private static final int POP_POINTS = 1600;

    public static class SimLeg {
        public String side = "SELL";     // BUY | SELL
        public String type = "CE";       // CE | PE
        public double strike;
        public String expiry;            // ISO yyyy-MM-dd
        public int lots = 1;
        public double price;             // premium per unit the P&L is measured from
        public double iv;                // annualised decimal (0.14 = 14 %); 0 = unknown
        public boolean include = true;
        public String tag;               // LIVE | SIM (display only)
        public String symbol;
        public String legId;
    }

    public static class Params {
        public String targetDate;        // ISO; null / today = T+0
        public double spotShiftPct;      // what-if spot move, %
        public double ivShiftPts;        // what-if IV change, vol points (1 = +1 %)
        public Double riskFree;
    }

    public static class Request {
        public List<SimLeg> legs = new ArrayList<>();
        public Params params = new Params();
    }

    /** Internal normalised leg. */
    private static final class Lg {
        int sign; boolean call; double k; LocalDate exp; double qty; double price; double iv;
    }

    public Map<String, Object> compute(Request req, double spot, int lotSize) {
        Map<String, Object> out = new LinkedHashMap<>();
        Params p = req.params == null ? new Params() : req.params;
        double r = p.riskFree != null && p.riskFree > 0 ? p.riskFree : BlackScholes.DEFAULT_RISK_FREE_RATE;
        ZonedDateTime now = ZonedDateTime.now(IST);
        LocalDate today = now.toLocalDate();
        double spotEval = spot * (1.0 + p.spotShiftPct / 100.0);

        List<Lg> legs = new ArrayList<>();
        for (SimLeg s : req.legs == null ? List.<SimLeg>of() : req.legs) {
            if (s == null || !s.include || s.lots <= 0 || !(s.strike > 0) || s.expiry == null) continue;
            LocalDate exp;
            try { exp = LocalDate.parse(s.expiry.trim()); } catch (Exception e) { continue; }
            Lg l = new Lg();
            l.sign = "BUY".equalsIgnoreCase(s.side) ? 1 : -1;
            l.call = !"PE".equalsIgnoreCase(s.type);
            l.k = s.strike; l.exp = exp; l.qty = (double) s.lots * Math.max(1, lotSize);
            l.price = Math.max(0, s.price); l.iv = Math.max(0, s.iv);
            legs.add(l);
        }
        out.put("spot", round2(spot));
        out.put("spotEval", round2(spotEval));
        out.put("lotSize", lotSize);
        if (legs.isEmpty() || !(spot > 0)) { out.put("empty", true); return out; }

        LocalDate nearest = legs.get(0).exp;
        for (Lg l : legs) if (l.exp.isBefore(nearest)) nearest = l.exp;
        double tExp = Math.max(0, years(now, nearest.atTime(SETTLE).atZone(IST)));
        double sigma = weightedIv(legs);
        double sigmaEval = Math.max(0, sigma + p.ivShiftPts / 100.0);

        // Target date for the T+n curve: today → "now"; any later date → that day's close.
        ZonedDateTime target = now;
        LocalDate targetDate = today;
        if (p.targetDate != null && !p.targetDate.trim().isEmpty()) {
            try { targetDate = LocalDate.parse(p.targetDate.trim()); } catch (Exception ignored) {}
            if (targetDate.isAfter(today)) target = targetDate.atTime(SETTLE).atZone(IST);
            else targetDate = today;
        }
        final ZonedDateTime tgt = target;
        final LocalDate near = nearest;
        final double sig = sigma;
        final double sigEval = sigmaEval;

        // Price grid: union of a linear sweep and every strike, so kinks land exactly on knots.
        double kMin = Double.MAX_VALUE, kMax = 0;
        for (Lg l : legs) { kMin = Math.min(kMin, l.k); kMax = Math.max(kMax, l.k); }
        double lo = Math.min(kMin, spotEval) * 0.93, hi = Math.max(kMax, spotEval) * 1.07;
        TreeSet<Double> xs = new TreeSet<>();
        for (int i = 0; i <= GRID_POINTS; i++) xs.add(lo + (hi - lo) * i / GRID_POINTS);
        for (Lg l : legs) xs.add(l.k);
        xs.add(spotEval);

        List<Map<String, Object>> curve = new ArrayList<>();
        double gridMax = -Double.MAX_VALUE, gridMin = Double.MAX_VALUE;
        double prevX = 0, prevE = 0; boolean first = true;
        List<Double> breakevens = new ArrayList<>();
        for (double x : xs) {
            double e = expiryPnl(legs, x, near, r, sig);
            double t = targetPnl(legs, x, tgt, r, sigEval);
            Map<String, Object> pt = new LinkedHashMap<>();
            pt.put("s", round2(x)); pt.put("e", round2(e)); pt.put("t", round2(t));
            curve.add(pt);
            gridMax = Math.max(gridMax, e); gridMin = Math.min(gridMin, e);
            if (!first && ((prevE < 0 && e > 0) || (prevE > 0 && e < 0))) {
                breakevens.add(round2(prevX + (x - prevX) * (prevE / (prevE - e))));
            } else if (!first && e == 0 && prevE != 0) {
                breakevens.add(round2(x));
            }
            prevX = x; prevE = e; first = false;
        }

        // Tails: net long calls → unlimited profit; net short calls → unlimited loss. Puts are
        // bounded by S ≥ 0 but reported the way traders read them (naked short put = "unlimited").
        double netCalls = 0, netPuts = 0;
        for (Lg l : legs) { if (l.call) netCalls += l.sign * l.qty; else netPuts += l.sign * l.qty; }
        // Only count the tail as unlimited if the curve is actually still heading that way at the edge.
        double eHiFar = expiryPnl(legs, hi * 1.5, near, r, sig), eHi = expiryPnl(legs, hi, near, r, sig);
        double eLoFar = expiryPnl(legs, lo * 0.5, near, r, sig), eLo = expiryPnl(legs, lo, near, r, sig);
        boolean rightUp = eHiFar > eHi + 1e-6, rightDown = eHiFar < eHi - 1e-6;
        boolean leftUp = eLoFar > eLo + 1e-6, leftDown = eLoFar < eLo - 1e-6;
        boolean profitUnlimited = (netCalls > 0.5 && rightUp) || (netPuts > 0.5 && leftUp);
        boolean lossUnlimited = (netCalls < -0.5 && rightDown) || (netPuts < -0.5 && leftDown);

        double netCredit = 0;
        for (Lg l : legs) netCredit += (l.sign < 0 ? l.price : -l.price) * l.qty;

        // POP + expected P&L under a lognormal terminal distribution at the nearest expiry.
        Double pop = null, expected = null;
        if (tExp <= 0) {
            double eNow = expiryPnl(legs, spotEval, near, r, sig);
            pop = eNow > 0 ? 100.0 : 0.0; expected = eNow;
        } else if (sigEval > 0) {
            double sT = sigEval * Math.sqrt(tExp);
            double mu = Math.log(spotEval) + (r - 0.5 * sigEval * sigEval) * tExp;
            double a = Math.exp(mu - 5 * sT), b = Math.exp(mu + 5 * sT);
            double popAcc = 0, evAcc = 0, prevCdf = cdf(a, mu, sT);
            for (int i = 1; i <= POP_POINTS; i++) {
                double x1 = Math.exp(Math.log(a) + (Math.log(b) - Math.log(a)) * i / POP_POINTS);
                double c = cdf(x1, mu, sT);
                double dP = c - prevCdf;
                double mid = Math.sqrt(x1 * Math.exp(Math.log(a) + (Math.log(b) - Math.log(a)) * (i - 1) / POP_POINTS));
                double pnl = expiryPnl(legs, mid, near, r, sig);
                if (pnl > 0) popAcc += dP;
                evAcc += pnl * dP;
                prevCdf = c;
            }
            pop = round2(popAcc * 100.0); expected = round2(evAcc);
        }

        // Net greeks at the evaluated spot, today's time to each leg's expiry.
        double dDelta = 0, dGamma = 0, dTheta = 0, dVega = 0;
        List<Map<String, Object>> legOut = new ArrayList<>();
        for (Lg l : legs) {
            double T = Math.max(0, years(now, l.exp.atTime(SETTLE).atZone(IST)));
            double v = (l.iv > 0 ? l.iv : sig) + p.ivShiftPts / 100.0;
            double de = BlackScholes.delta(spotEval, l.k, T, r, v, l.call);
            double ga = BlackScholes.gamma(spotEval, l.k, T, r, v);
            double th = BlackScholes.theta(spotEval, l.k, T, r, v, l.call);
            double ve = BlackScholes.vega(spotEval, l.k, T, r, v);
            dDelta += l.sign * l.qty * de; dGamma += l.sign * l.qty * ga;
            dTheta += l.sign * l.qty * th; dVega += l.sign * l.qty * ve;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("strike", l.k); m.put("type", l.call ? "CE" : "PE"); m.put("side", l.sign > 0 ? "BUY" : "SELL");
            m.put("iv", round4(v)); m.put("delta", round4(de)); m.put("theta", round2(th * l.qty * l.sign));
            m.put("value", round2(v > 0 ? BlackScholes.price(spotEval, l.k, T, r, v, l.call) : intrinsic(spotEval, l.k, l.call)));
            legOut.add(m);
        }
        Map<String, Object> greeks = new LinkedHashMap<>();
        greeks.put("delta", round2(dDelta)); greeks.put("gamma", round4(dGamma));
        greeks.put("theta", round2(dTheta)); greeks.put("vega", round2(dVega));

        Map<String, Object> bands = new LinkedHashMap<>();
        // Worst / best expiry P&L inside the ±2σ band: the practical risk / reward figure when a
        // side of the payoff is unlimited (naked short or long options).
        Double loss2s = null, profit2s = null;
        if (sigEval > 0 && tExp > 0) {
            double sT = sigEval * Math.sqrt(tExp);
            double lo2 = spotEval * Math.exp(-2 * sT), hi2 = spotEval * Math.exp(2 * sT);
            bands.put("lo1", round2(spotEval * Math.exp(-sT))); bands.put("hi1", round2(spotEval * Math.exp(sT)));
            bands.put("lo2", round2(lo2)); bands.put("hi2", round2(hi2));
            double worst = Double.MAX_VALUE, best = -Double.MAX_VALUE;
            TreeSet<Double> inBand = new TreeSet<>(xs.subSet(lo2, true, hi2, true));
            inBand.add(lo2); inBand.add(hi2);
            for (double x : inBand) {
                double e = expiryPnl(legs, x, near, r, sig);
                worst = Math.min(worst, e); best = Math.max(best, e);
            }
            loss2s = round2(worst); profit2s = round2(best);
        }

        out.put("empty", false);
        out.put("nearestExpiry", nearest.toString());
        out.put("daysToExpiry", ChronoUnit.DAYS.between(today, nearest));
        out.put("targetDate", targetDate.toString());
        out.put("sigma", round4(sigma));
        out.put("sigmaEval", round4(sigmaEval));
        out.put("curve", curve);
        out.put("breakevens", breakevens);
        out.put("maxProfit", round2(gridMax));
        out.put("maxProfitUnlimited", profitUnlimited);
        out.put("maxLoss", round2(gridMin));
        out.put("maxLossUnlimited", lossUnlimited);
        out.put("loss2Sigma", loss2s);
        out.put("profit2Sigma", profit2s);
        out.put("loss2Sigma", loss2s);
        out.put("profit2Sigma", profit2s);
        out.put("netCredit", round2(netCredit));
        out.put("pop", pop);
        out.put("expectedPnl", expected);
        out.put("greeks", greeks);
        out.put("bands", bands);
        out.put("legs", legOut);
        out.put("gridLo", round2(lo)); out.put("gridHi", round2(hi));
        return out;
    }

    // ── maths ─────────────────────────────────────────────────────────────────
    private static double expiryPnl(List<Lg> legs, double s, LocalDate nearest, double r, double sigma) {
        double pnl = 0;
        for (Lg l : legs) {
            double value;
            if (!l.exp.isAfter(nearest)) value = intrinsic(s, l.k, l.call);
            else {
                double T = Math.max(0, years(nearest.atTime(SETTLE).atZone(IST), l.exp.atTime(SETTLE).atZone(IST)));
                double v = l.iv > 0 ? l.iv : sigma;
                value = v > 0 && T > 0 ? BlackScholes.price(s, l.k, T, r, v, l.call) : intrinsic(s, l.k, l.call);
            }
            pnl += l.sign * (value - l.price) * l.qty;
        }
        return pnl;
    }

    private static double targetPnl(List<Lg> legs, double s, ZonedDateTime at, double r, double sigmaEval) {
        double pnl = 0;
        for (Lg l : legs) {
            double T = years(at, l.exp.atTime(SETTLE).atZone(IST));
            double v = l.iv > 0 ? l.iv : sigmaEval;   // sigmaEval already carries the IV shift when iv unknown
            double value = (T > 0 && v > 0) ? BlackScholes.price(s, l.k, T, r, v, l.call) : intrinsic(s, l.k, l.call);
            pnl += l.sign * (value - l.price) * l.qty;
        }
        return pnl;
    }

    private static double intrinsic(double s, double k, boolean call) { return Math.max(0, call ? s - k : k - s); }

    private static double weightedIv(List<Lg> legs) {
        double num = 0, den = 0;
        for (Lg l : legs) if (l.iv > 0) { num += l.iv * l.qty; den += l.qty; }
        return den > 0 ? num / den : 0;
    }

    private static double cdf(double x, double mu, double sT) { return BlackScholes.normCdf((Math.log(x) - mu) / sT); }

    private static double years(ZonedDateTime from, ZonedDateTime to) {
        return ChronoUnit.SECONDS.between(from, to) / (365.0 * 24 * 3600);
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
    private static double round4(double v) { return Math.round(v * 10000.0) / 10000.0; }
}
