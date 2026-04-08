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
    private final MarketDataService marketDataService;

    public SignalProcessor(RiskSettingsStore riskSettings, EventService eventService,
                           QuantityService quantityService, MarketDataService marketDataService) {
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.quantityService = quantityService;
        this.marketDataService = marketDataService;
    }

    public ProcessedSignal process(Map<String, Object> alert) {

        // ── 4a. Parse alert fields ──────────────────────────────────────────────
        String setup       = str(alert, "setup");
        String rawSymbol   = str(alert, "symbol");
        // TradingView uses _ in symbols (e.g. BAJAJ_AUTO), Fyers uses - (BAJAJ-AUTO)
        String symbol      = rawSymbol.replace("_", "-");
        String probability = str(alert, "probability");
        double close       = dbl(alert, "close");
        double atr         = dbl(alert, "atr");
        double dayOpen     = dbl(alert, "dayOpen");
        double r1 = dbl(alert, "r1"), r2 = dbl(alert, "r2"), r3 = dbl(alert, "r3"), r4 = dbl(alert, "r4");
        double s1 = dbl(alert, "s1"), s2 = dbl(alert, "s2"), s3 = dbl(alert, "s3"), s4 = dbl(alert, "s4");
        double ph = dbl(alert, "ph"), pl = dbl(alert, "pl");
        double tc = dbl(alert, "tc"), bc = dbl(alert, "bc");
        double candleOpen = dbl(alert, "candleOpen");
        double candleHigh = dbl(alert, "candleHigh");
        double candleLow  = dbl(alert, "candleLow");

        // ── Reject if ATR is invalid (NaN or zero — Pine Script may send NaN for insufficient bars)
        if (Double.isNaN(atr) || atr <= 0) {
            return ProcessedSignal.rejected(setup, symbol, "Invalid ATR (" + atr + ") — cannot compute stop loss");
        }

        // ── 4b. Derive direction ────────────────────────────────────────────────
        boolean isBuy = setup.startsWith("BUY_");
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
        if (riskSettings.isEnableLargeCandleFilter()) {
            double largeThreshold = riskSettings.getLargeCandleAtrThreshold();
            if (isBuy && (close - breakoutLevel) > atr * largeThreshold) {
                return ProcessedSignal.rejected(setup, symbol, "Large candle — move > " + largeThreshold + " ATR (" + fmt(atr * largeThreshold) + ") from breakout level");
            }
            if (!isBuy && (breakoutLevel - close) > atr * largeThreshold) {
                return ProcessedSignal.rejected(setup, symbol, "Large candle — move > " + largeThreshold + " ATR (" + fmt(atr * largeThreshold) + ") from breakout level");
            }
        }

        // ── 4d2. Small candle filter ────────────────────────────────────────────
        if (riskSettings.isEnableSmallCandleFilter()) {
            double smallThreshold = riskSettings.getSmallCandleAtrThreshold();
            double wickRatio = riskSettings.getWickRejectionRatio();
            double oppWickRatio = riskSettings.getOppositeWickRatio();
            double moveFromLevel = isBuy ? (close - breakoutLevel) : (breakoutLevel - close);

            // Check 1: close too near breakout level
            if (moveFromLevel < atr * smallThreshold) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Small candle — move from breakout level (" + fmt(moveFromLevel) + ") < " + smallThreshold + " ATR (" + fmt(atr * smallThreshold) + ")");
            }

            if (candleOpen > 0) {
                double candleBody = Math.abs(close - candleOpen);

                // Check 2 + 3: candle body too small, unless wick rejection overrides
                if (candleBody < atr * smallThreshold) {
                    boolean wickRejection = false;
                    if (candleHigh > 0 && candleLow > 0) {
                        // Buy: long lower wick = buyers defended. Sell: long upper wick = sellers defended.
                        double breakoutWick = isBuy
                            ? (Math.min(close, candleOpen) - candleLow)
                            : (candleHigh - Math.max(close, candleOpen));
                        wickRejection = breakoutWick >= wickRatio * candleBody
                            && breakoutWick >= atr * smallThreshold;
                    }
                    if (!wickRejection) {
                        return ProcessedSignal.rejected(setup, symbol,
                            "Small candle body (" + fmt(candleBody) + ") < " + smallThreshold + " ATR (" + fmt(atr * smallThreshold) + ")");
                    }
                }

                // Check 4: opposite wick pressure (always checked, even for large bodies)
                // Buy: long upper wick = sellers pushing down. Sell: long lower wick = buyers pushing up.
                if (candleHigh > 0 && candleLow > 0) {
                    double oppositeWick = isBuy
                        ? (candleHigh - Math.max(close, candleOpen))
                        : (Math.min(close, candleOpen) - candleLow);
                    if (oppositeWick >= oppWickRatio * candleBody && oppositeWick >= atr * smallThreshold) {
                        return ProcessedSignal.rejected(setup, symbol,
                            "Opposite wick pressure (" + fmt(oppositeWick) + ") — counter-pressure against breakout");
                    }
                }
            }
        }

        // ── 4e. Volume filter ─────────────────────────────────────────────────
        double candleVolume = dbl(alert, "candleVolume");
        double avgVolume = dbl(alert, "avgVolume");
        if (riskSettings.isEnableVolumeFilter() && candleVolume > 0 && avgVolume > 0) {
            double volumeMultiple = riskSettings.getVolumeMultiple();
            if (candleVolume < avgVolume * volumeMultiple) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Low volume — candle vol (" + (long) candleVolume + ") < " + volumeMultiple + "x avg (" + (long) avgVolume + ")");
            }
        }

        // ── 4f. Compute target ──────────────────────────────────────────────────
        double[] targets = computeTargets(setup, close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
        double defaultTarget = targets[0];
        double shiftTarget   = targets[1];
        double target = defaultTarget;

        boolean shifted = false;
        double targetShiftThreshold = riskSettings.getTargetShiftAtrThreshold();
        if (Math.abs(close - defaultTarget) < atr * targetShiftThreshold) {
            if (riskSettings.isEnableTargetShift()) {
                target = shiftTarget;
                shifted = true;
            } else {
                // Target shift disabled — use the small target as-is
                eventService.log("[INFO] " + symbol + " " + setup + " target close (< " + targetShiftThreshold + " ATR) but shift disabled — using default: " + fmt(defaultTarget));
            }
        }


        // ── 4g. Day high/low target shift ──────────────────────────────────────
        // If today's session high (BUY) or low (SELL) is between entry and target,
        // shift target to day high/low — it acts as intraday resistance/support
        if (isBuy) {
            double dayHigh = marketDataService.getDayHigh(symbol);
            if (dayHigh > 0 && dayHigh > close && dayHigh < target) {
                eventService.log("[INFO] " + symbol + " " + setup + " target shifted to day high: "
                    + fmt(target) + " → " + fmt(dayHigh) + " (session high between entry and target)");
                target = dayHigh;
            }
        } else {
            double dayLow = marketDataService.getDayLow(symbol);
            if (dayLow > 0 && dayLow < close && dayLow > target) {
                eventService.log("[INFO] " + symbol + " " + setup + " target shifted to day low: "
                    + fmt(target) + " → " + fmt(dayLow) + " (session low between entry and target)");
                target = dayLow;
            }
        }

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
        if (!isExtreme && sessionMoveLimit > 0) {
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

        // ── 4i2. Probability-based qty adjustment (MPT = half, LPT = quarter, configurable) ──
        if ("MPT".equals(probability)) {
            double factor = riskSettings.getMptQtyFactor();
            int reduced = Math.max(2, ((int)(qty * factor) / 2) * 2); // apply factor, round to even
            eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (MPT ×" + factor + "): " + qty + " -> " + reduced);
            qty = reduced;
        } else if ("LPT".equals(probability)) {
            double factor = riskSettings.getLptQtyFactor();
            int reduced = Math.max(2, ((int)(qty * factor) / 2) * 2);
            eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (LPT ×" + factor + "): " + qty + " -> " + reduced);
            qty = reduced;
        }

        // ── 4i3. Level-based qty adjustment (R3/S3 and R4/S4 extended levels) ──
        if (setup.contains("R3") || setup.contains("S3")) {
            double factor = riskSettings.getR3s3QtyFactor();
            if (factor < 1.0) {
                int reduced = Math.max(2, ((int)(qty * factor) / 2) * 2);
                eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (R3/S3 ×" + factor + "): " + qty + " -> " + reduced);
                qty = reduced;
            }
        } else if (setup.contains("R4") || setup.contains("S4")) {
            double factor = riskSettings.getR4s4QtyFactor();
            if (factor < 1.0) {
                int reduced = Math.max(2, ((int)(qty * factor) / 2) * 2);
                eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (R4/S4 ×" + factor + "): " + qty + " -> " + reduced);
                qty = reduced;
            }
        }

        // ── 4j. Build description ─────────────────────────────────────────────
        StringBuilder desc = new StringBuilder();

        // [ENTRY] line
        String levelName = levelNameForSetup(setup);
        desc.append("[ENTRY] ").append(setup).append(" | ").append(probability)
            .append(" — Close ").append(fmt(close))
            .append(" broke ").append(levelName)
            .append(" (").append(fmt(breakoutLevel)).append(").");

        // [QTY] line
        int fixedQty = riskSettings.getFixedQuantity();
        if (fixedQty != -1) {
            desc.append("\n[QTY] Fixed: ").append(baseQty).append(".");
        } else {
            double riskPerTrade = riskSettings.getRiskPerTrade();
            double riskPerShare = Math.abs(close - sl);
            int riskQty = riskPerShare > 0 ? (int)(riskPerTrade / riskPerShare) : 0;
            desc.append("\n[QTY] Risk: ₹").append(fmt(riskPerTrade))
                .append(" / ₹").append(fmt(riskPerShare))
                .append(" = ").append(riskQty)
                .append(". Final: ").append(baseQty).append(".");
        }
        if (isExtreme) {
            desc.append(" Halved → ").append(qty).append(" (extreme level).");
        } else if (qtyLog != null && qty != baseQty) {
            desc.append(" Halved → ").append(qty).append(" (session move limit).");
        }

        // [TARGET] line (only if shifted)
        if (shifted) {
            desc.append("\n[TARGET] Shifted ").append(fmt(defaultTarget))
                .append(" → ").append(fmt(shiftTarget))
                .append(" (default < ").append(targetShiftThreshold).append(" ATR from entry).");
        }

        String description = desc.toString();

        // ── 4k. Log the decision ────────────────────────────────────────────────
        eventService.log("[SUCCESS] " + signal + " signal received for " + symbol + " | " + setup + " | Entry: " + fmt(close)
            + " | Tgt: " + fmt(target) + "(" + fmt(Math.abs(target - close)) + ")"
            + " | SL: " + fmt(sl) + "(" + fmt(Math.abs(close - sl)) + ")"
            + " | Qty: " + qty);
        if (qtyLog != null) {
            eventService.log(qtyLog);
        }
        if (shifted) {
            eventService.log("[INFO] " + symbol + " " + setup + " target shifted: " + fmt(defaultTarget) + " -> " + fmt(shiftTarget)
                + " (default was < " + targetShiftThreshold + " ATR from entry)");
        }

        return new ProcessedSignal.Builder()
            .signal(signal)
            .symbol(symbol)
            .quantity(qty)
            .target(target)
            .stoploss(sl)
            .setup(setup)
            .probability(probability)
            .atr(atr)
            .atrMultiplier(riskSettings.getAtrMultiplier())
            .rejected(false)
            .description(description)
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
            default -> new double[]{ 0, 0 };
        };
    }

    // ── Level name for description ──────────────────────────────────────────────
    private static String levelNameForSetup(String setup) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"     -> "CPR top";
            case "BUY_ABOVE_R1_PDH"  -> "R1+PDH";
            case "BUY_ABOVE_R2"      -> "R2";
            case "BUY_ABOVE_R3"      -> "R3";
            case "BUY_ABOVE_R4"      -> "R4";
            case "BUY_ABOVE_S1_PDL"  -> "S1+PDL";
            case "SELL_BELOW_CPR"    -> "CPR bottom";
            case "SELL_BELOW_S1_PDL" -> "S1+PDL";
            case "SELL_BELOW_S2"     -> "S2";
            case "SELL_BELOW_S3"     -> "S3";
            case "SELL_BELOW_S4"     -> "S4";
            case "SELL_BELOW_R1_PDH" -> "R1+PDH";
            default -> setup;
        };
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
