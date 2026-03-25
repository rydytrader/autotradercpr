package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.ProcessedSignal;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class SignalProcessor {

    private final RiskSettingsStore riskSettings;
    private final EventService     eventService;
    private final QuantityService  quantityService;

    public SignalProcessor(RiskSettingsStore riskSettings, EventService eventService,
                           QuantityService quantityService) {
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.quantityService = quantityService;
    }

    public ProcessedSignal process(Map<String, Object> alert) {

        // ── 4a. Parse alert fields ──────────────────────────────────────────────
        String setup       = str(alert, "setup");
        String symbol      = str(alert, "symbol");
        String probability = str(alert, "probability");
        double close       = dbl(alert, "close");
        double atr         = dbl(alert, "atr");
        double dayOpen     = dbl(alert, "dayOpen");
        double r1 = dbl(alert, "r1"), r2 = dbl(alert, "r2"), r3 = dbl(alert, "r3"), r4 = dbl(alert, "r4");
        double s1 = dbl(alert, "s1"), s2 = dbl(alert, "s2"), s3 = dbl(alert, "s3"), s4 = dbl(alert, "s4");
        double ph = dbl(alert, "ph"), pl = dbl(alert, "pl");
        double tc = dbl(alert, "tc"), bc = dbl(alert, "bc");

        // ── Reject if ATR is invalid (NaN or zero — Pine Script may send NaN for insufficient bars)
        if (Double.isNaN(atr) || atr <= 0) {
            return ProcessedSignal.rejected(setup, symbol, "Invalid ATR (" + atr + ") — cannot compute stop loss");
        }

        // ── 4b. Derive direction ────────────────────────────────────────────────
        boolean isBuy = setup.startsWith("BUY_") || "DAY_HIGH_BO".equals(setup);
        String signal = isBuy ? "BUY" : "SELL";

        // ── R4/S4 gate ────────────────────────────────────────────────────────
        boolean isR4S4 = "BUY_ABOVE_R4".equals(setup) || "SELL_BELOW_S4".equals(setup);
        if (isR4S4 && !riskSettings.isEnableR4S4()) {
            return ProcessedSignal.rejected(setup, symbol, "R4/S4 signals disabled in settings");
        }

        // ── 4c. Compute breakout level ──────────────────────────────────────────
        double breakoutLevel = computeBreakoutLevel(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);

        // ── 4c2. Compute stop loss (needed for risk-based qty) ─────────────────
        double atrMultiplier = riskSettings.getAtrMultiplier();
        double sl = isBuy ? (close - atr * atrMultiplier) : (close + atr * atrMultiplier);

        // ── 4c3. Compute base quantity (uses SL for risk-based sizing) ─────────
        int baseQty = quantityService.computeBaseQty(symbol, close, sl);

        // ── 4d. Large candle filter ─────────────────────────────────────────────
        boolean isDayBO = "DAY_HIGH_BO".equals(setup) || "DAY_LOW_BO".equals(setup);
        if (!isDayBO && riskSettings.isEnableLargeCandleFilter()) {
            double largeThreshold = riskSettings.getLargeCandleAtrThreshold();
            if (isBuy && (close - breakoutLevel) > atr * largeThreshold) {
                return ProcessedSignal.rejected(setup, symbol, "Large candle — move > " + largeThreshold + " ATR (" + fmt(atr * largeThreshold) + ") from breakout level");
            }
            if (!isBuy && (breakoutLevel - close) > atr * largeThreshold) {
                return ProcessedSignal.rejected(setup, symbol, "Large candle — move > " + largeThreshold + " ATR (" + fmt(atr * largeThreshold) + ") from breakout level");
            }
        }

        // ── 4d2. Small candle filter ────────────────────────────────────────────
        if (!isDayBO && riskSettings.isEnableSmallCandleFilter()) {
            double smallThreshold = riskSettings.getSmallCandleAtrThreshold();
            double moveFromLevel = isBuy ? (close - breakoutLevel) : (breakoutLevel - close);
            if (moveFromLevel < atr * smallThreshold) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Small candle — move from breakout level (" + fmt(moveFromLevel) + ") < " + smallThreshold + " ATR (" + fmt(atr * smallThreshold) + ")");
            }
        }

        // ── 4f. Compute target ──────────────────────────────────────────────────
        double[] targets = computeTargets(setup, close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
        double defaultTarget = targets[0];
        double shiftTarget   = targets[1];
        double target = defaultTarget;

        boolean shifted = false;
        if (Math.abs(close - defaultTarget) < atr) {
            if (riskSettings.isEnableTargetShift()) {
                target = shiftTarget;
                shifted = true;
            } else {
                return ProcessedSignal.rejected(setup, symbol,
                    "Target too close (< 1 ATR) and target shift disabled — default: " + fmt(defaultTarget) + ", distance: " + fmt(Math.abs(close - defaultTarget)) + ", ATR: " + fmt(atr));
            }
        }

        boolean sessionCapped = false; // session target cap removed — use CPR levels only

        // ── 4i. Quantity ────────────────────────────────────────────────────────
        int qty = baseQty;
        String qtyLog = null;
        boolean isExtreme = "BUY_ABOVE_R3".equals(setup) || "SELL_BELOW_S3".equals(setup) || isR4S4;
        if (isExtreme) {
            qty = Math.max(1, qty / 2);
            qtyLog = "[INFO] " + symbol + " " + setup + " qty halved (extreme level): " + baseQty + " -> " + qty;
        }
        // ── Session move limit: reduce qty if price moved too far from day open or PDC ──
        double sessionMoveLimit = riskSettings.getSessionMoveLimit() / 100.0; // e.g. 2.0 → 0.02
        if (!isExtreme && !isDayBO && sessionMoveLimit > 0) {
            double pivot = (tc + bc) / 2.0;
            double pdc = pivot * 3 - ph - pl;
            double moveFromOpen = dayOpen > 0 ? Math.abs(close - dayOpen) / dayOpen : 0;
            double moveFromPdc  = pdc > 0 ? Math.abs(close - pdc) / pdc : 0;
            double movePct = Math.max(moveFromOpen, moveFromPdc) * 100;
            String moveSource = moveFromOpen >= moveFromPdc ? "day open " + fmt(dayOpen) : "PDC " + fmt(pdc);
            if (Math.max(moveFromOpen, moveFromPdc) > sessionMoveLimit) {
                int reduced = Math.max(1, qty / 2);
                qtyLog = "[INFO] " + symbol + " " + setup + " qty halved (moved " + fmt(movePct)
                    + "% from " + moveSource + " > " + fmt(riskSettings.getSessionMoveLimit()) + "% limit): " + qty + " -> " + reduced;
                qty = reduced;
            }
        }

        // ── 4j. Log the decision ────────────────────────────────────────────────
        eventService.log("[SUCCESS] " + signal + " signal received for " + symbol + " | " + setup + " | Entry: " + fmt(close)
            + " | Tgt: " + fmt(target) + "(" + fmt(Math.abs(target - close)) + ")"
            + " | SL: " + fmt(sl) + "(" + fmt(Math.abs(close - sl)) + ")"
            + " | Qty: " + qty);
        if (qtyLog != null) {
            eventService.log(qtyLog);
        }
        if (shifted) {
            eventService.log("[INFO] " + symbol + " " + setup + " target shifted: " + fmt(defaultTarget) + " -> " + fmt(shiftTarget)
                + " (default was < 1 ATR from entry)");
        }

        return new ProcessedSignal.Builder()
            .signal(signal)
            .symbol(symbol)
            .quantity(qty)
            .target(target)
            .stoploss(sl)
            .setup(setup)
            .probability(probability)
            .rejected(false)
            .build();
    }

    // ── Breakout level per setup ────────────────────────────────────────────────
    private double computeBreakoutLevel(String setup,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"    -> Math.max(tc, bc);
            case "BUY_ABOVE_R1_PDH" -> Math.max(r1, ph);
            case "BUY_ABOVE_R2"     -> r2;
            case "BUY_ABOVE_R3"     -> r3;
            case "BUY_ABOVE_R4"     -> r4;
            case "BUY_ABOVE_S1_PDL" -> Math.max(s1, pl);
            case "SELL_BELOW_CPR"    -> Math.min(tc, bc);
            case "SELL_BELOW_S1_PDL" -> Math.min(s1, pl);
            case "SELL_BELOW_S2"     -> s2;
            case "SELL_BELOW_S3"     -> s3;
            case "SELL_BELOW_S4"     -> s4;
            case "SELL_BELOW_R1_PDH" -> Math.min(r1, ph);
            case "DAY_HIGH_BO"       -> 0;  // no fixed breakout level; skip large candle filter
            case "DAY_LOW_BO"        -> 0;
            default -> 0;
        };
    }

    // ── Default + shift target per setup ────────────────────────────────────────
    private double[] computeTargets(String setup, double close,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"     -> new double[]{ Math.min(r1, ph), r2 };
            case "BUY_ABOVE_R1_PDH"  -> new double[]{ r2, r3 };
            case "BUY_ABOVE_R2"      -> new double[]{ r3, r4 };
            case "BUY_ABOVE_R3"      -> new double[]{ r4, r4 };
            case "BUY_ABOVE_R4"      -> { double r5 = r4 + (r4 - r3); yield new double[]{ r5, r5 }; }
            case "BUY_ABOVE_S1_PDL"  -> new double[]{ Math.min(tc, bc), Math.min(r1, ph) };
            case "SELL_BELOW_CPR"     -> new double[]{ Math.max(s1, pl), s2 };
            case "SELL_BELOW_S1_PDL"  -> new double[]{ s2, s3 };
            case "SELL_BELOW_S2"      -> new double[]{ s3, s4 };
            case "SELL_BELOW_S3"      -> new double[]{ s4, s4 };
            case "SELL_BELOW_S4"      -> { double s5 = s4 - (s3 - s4); yield new double[]{ s5, s5 }; }
            case "SELL_BELOW_R1_PDH"  -> new double[]{ Math.max(tc, bc), Math.max(s1, pl) };
            case "DAY_HIGH_BO"       -> nextResistanceTargets(close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
            case "DAY_LOW_BO"        -> nextSupportTargets(close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
            default -> new double[]{ 0, 0 };
        };
    }

    // ── Find next pivot level above close for DAY_HIGH_BO ─────────────────────
    private double[] nextResistanceTargets(double close,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc) {
        double[] all = { r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc };
        java.util.Arrays.sort(all);
        // Find the two nearest levels above close
        double target = 0, shift = 0;
        for (int i = 0; i < all.length; i++) {
            if (all[i] > close) {
                target = all[i];
                shift = (i + 1 < all.length) ? all[i + 1] : target + (target - all[i - 1]);
                return new double[]{ target, shift };
            }
        }
        // Above all levels — use synthetic level
        double top = all[all.length - 1];
        double gap = all.length >= 2 ? top - all[all.length - 2] : top * 0.01;
        double synthetic = top + gap;
        return new double[]{ synthetic, synthetic };
    }

    // ── Find next pivot level below close for DAY_LOW_BO ────────────────────────
    private double[] nextSupportTargets(double close,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc) {
        double[] all = { r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc };
        java.util.Arrays.sort(all);
        // Find the two nearest levels below close (scan from high to low)
        double target = 0, shift = 0;
        for (int i = all.length - 1; i >= 0; i--) {
            if (all[i] < close) {
                target = all[i];
                shift = (i - 1 >= 0) ? all[i - 1] : target - (all[i + 1] - target);
                return new double[]{ target, shift };
            }
        }
        // Below all levels — use synthetic level
        double bottom = all[0];
        double gap = all.length >= 2 ? all[1] - bottom : bottom * 0.01;
        double synthetic = bottom - gap;
        return new double[]{ synthetic, synthetic };
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────
    private static String str(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v != null ? v.toString() : "";
    }

    private static double dbl(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) return 0;
        return Double.parseDouble(v.toString());
    }

    private static int intVal(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) return 0;
        return (int) Double.parseDouble(v.toString());
    }

    private static String fmt(double v) {
        return String.format("%.2f", v);
    }
}
