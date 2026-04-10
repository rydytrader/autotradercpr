package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.ProcessedSignal;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Service
public class SignalProcessor {

    private final RiskSettingsStore riskSettings;
    private final EventService     eventService;
    private final QuantityService  quantityService;
    private final MarketDataService marketDataService;
    private final CandleAggregator candleAggregator;
    private final WeeklyCprService weeklyCprService;

    public SignalProcessor(RiskSettingsStore riskSettings, EventService eventService,
                           QuantityService quantityService, MarketDataService marketDataService,
                           CandleAggregator candleAggregator, WeeklyCprService weeklyCprService) {
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.quantityService = quantityService;
        this.marketDataService = marketDataService;
        this.candleAggregator = candleAggregator;
        this.weeklyCprService = weeklyCprService;
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
        double dayHigh = dbl(alert, "dayHigh");
        double dayLow  = dbl(alert, "dayLow");

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
        double breakoutLevel = computeBreakoutLevel(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, dayHigh, dayLow);

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

        // ── 4e2. Opening Range EV filter — on Extended Value days, only trade OR-aligned or mean-reversion ──
        int orMinutes = riskSettings.getOpeningRangeMinutes();
        boolean isReversal = false;
        if (orMinutes > 0) {
            double firstClose = candleAggregator.getFirstCandleClose(symbol);
            boolean gapUp   = firstClose > 0 && r2 > 0 && firstClose >= r2;
            boolean gapDown = firstClose > 0 && s2 > 0 && firstClose <= s2;
            if (gapUp || gapDown) {
                if (!candleAggregator.isOpeningRangeLocked(symbol)) {
                    return ProcessedSignal.rejected(setup, symbol,
                        "EV " + (gapUp ? "up" : "down") + " detected (1st candle close " + fmt(firstClose)
                        + (gapUp ? " >= R2 " + fmt(r2) : " <= S2 " + fmt(s2))
                        + ") but Opening Range still forming — skipped");
                }
                double orHigh = candleAggregator.getOpeningRangeHigh(symbol);
                double orLow  = candleAggregator.getOpeningRangeLow(symbol);
                boolean orBullish = close > orHigh;
                boolean orBearish = close < orLow;
                boolean aligned = (isBuy && orBullish) || (!isBuy && orBearish);
                boolean counter = (isBuy && orBearish) || (!isBuy && orBullish);

                if (!aligned && !counter) {
                    // Inside OR range — skip
                    return ProcessedSignal.rejected(setup, symbol,
                        "EV " + (gapUp ? "up" : "down") + " (1st close " + fmt(firstClose) + ") — price inside OR range"
                        + " (H:" + fmt(orHigh) + " L:" + fmt(orLow) + ") — skipped");
                }
                if (counter) {
                    // Counter-OR on EV day = mean-reversion trade
                    isReversal = true;
                    // Recalculate probability: reversals use weekly-only (like magnets — counter-daily is expected)
                    String oldProb = probability;
                    probability = weeklyCprService.getProbabilityForDirection(symbol, isBuy, true);
                    if (!oldProb.equals(probability)) {
                        eventService.log("[INFO] " + symbol + " " + setup + " probability recalculated for reversal: " + oldProb + " → " + probability);
                    }
                    eventService.log("[INFO] " + symbol + " " + setup + " EV " + (gapUp ? "up" : "down")
                        + " — OR " + (orBullish ? "Bullish" : "Bearish") + " reversal, mean-reversion targets");
                } else {
                    eventService.log("[INFO] " + symbol + " " + setup + " EV " + (gapUp ? "up" : "down")
                        + " — OR " + (orBullish ? "Bullish" : "Bearish") + " aligned, proceeding");
                }
            } else {
                // ── IV/OV day: if breakout candle is inside OR range, downgrade prob to LPT ──
                // (still trading inside the opening range — weak breakout)
                if (candleAggregator.isOpeningRangeLocked(symbol)) {
                    double orHigh = candleAggregator.getOpeningRangeHigh(symbol);
                    double orLow  = candleAggregator.getOpeningRangeLow(symbol);
                    if (orHigh > 0 && orLow > 0 && close >= orLow && close <= orHigh) {
                        if ("HPT".equals(probability) || "MPT".equals(probability)) {
                            String oldProb = probability;
                            probability = "LPT";
                            eventService.log("[INFO] " + symbol + " " + setup
                                + " probability downgraded " + oldProb + " → LPT — close " + fmt(close)
                                + " inside OR range (H:" + fmt(orHigh) + " L:" + fmt(orLow) + ") — still trading in range");
                        }
                    }
                }
            }
        }

        // ── 4f. Compute target ──────────────────────────────────────────────────
        double[] targets;
        if (isReversal) {
            targets = computeReversalTargets(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
        } else {
            targets = computeTargets(setup, close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, dayHigh, dayLow);
        }
        double defaultTarget = targets[0];
        double target = defaultTarget;

        // ── 4g. Day high/low target shift (always on, except DH/DL setups) ────
        // If today's session high (BUY) or low (SELL) is between entry and target,
        // shift target to day high/low — it acts as intraday resistance/support
        // Skip for DH/DL setups: breakout candle creates new day high/low, would shift target to near entry
        boolean isDhDl = "BUY_ABOVE_DH".equals(setup) || "SELL_BELOW_DL".equals(setup);
        boolean dayHighLowShifted = false;
        if (!isDhDl && isBuy) {
            double sessionHigh = marketDataService.getDayHigh(symbol);
            if (sessionHigh > 0 && sessionHigh > close && sessionHigh < target) {
                eventService.log("[INFO] " + symbol + " " + setup + " target shifted to day high: "
                    + fmt(target) + " → " + fmt(sessionHigh) + " (session high between entry and target)");
                target = sessionHigh;
                dayHighLowShifted = true;
            }
        } else if (!isDhDl) {
            double sessionLow = marketDataService.getDayLow(symbol);
            if (sessionLow > 0 && sessionLow < close && sessionLow > target) {
                eventService.log("[INFO] " + symbol + " " + setup + " target shifted to day low: "
                    + fmt(target) + " → " + fmt(sessionLow) + " (session low between entry and target)");
                target = sessionLow;
                dayHighLowShifted = true;
            }
        }

        // ── 4g2. Small target filter — skip if target < N ATR from entry ──
        double minTargetAtr = riskSettings.getTargetShiftAtrThreshold();
        if (riskSettings.isEnableSmallTargetFilter() && minTargetAtr > 0 && atr > 0) {
            double targetDist = Math.abs(target - close);
            if (targetDist < atr * minTargetAtr) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Target " + fmt(target) + " too close to entry " + fmt(close)
                    + " (" + fmt(targetDist) + " < " + fmt(atr * minTargetAtr) + " = " + minTargetAtr + " ATR) — insufficient profit room");
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
        // ── Session move limit: reduce qty if price moved too far from day open/PDC ──
        double sessionMoveLimit = riskSettings.getSessionMoveLimit() / 100.0; // e.g. 2.0 → 0.02
        if (riskSettings.isEnableSessionMoveLimit() && sessionMoveLimit > 0) {
            double pivot = (tc + bc) / 2.0;
            double pdc = pivot * 3 - ph - pl;
            double moveFromOpen = dayOpen > 0 ? Math.abs(close - dayOpen) / dayOpen : 0;
            double moveFromPdc  = pdc > 0 ? Math.abs(close - pdc) / pdc : 0;
            double movePct = Math.max(moveFromOpen, moveFromPdc) * 100;
            String moveSource = moveFromOpen >= moveFromPdc ? "day open " + fmt(dayOpen) : "PDC " + fmt(pdc);
            boolean sessionMoveExceeded = Math.max(moveFromOpen, moveFromPdc) > sessionMoveLimit;

            if (sessionMoveExceeded) {
                int reduced = Math.max(1, qty / 2);
                String reason = "moved " + fmt(movePct) + "% from " + moveSource + " > " + fmt(riskSettings.getSessionMoveLimit()) + "% limit";
                qtyLog = "[INFO] " + symbol + " " + setup + " qty halved (" + reason + "): " + qty + " -> " + reduced;
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

        // ── 4i4. Mean-reversion qty reduction — half qty for all counter-trend trades ──
        boolean isMagnet = "BUY_ABOVE_S1_PDL".equals(setup) || "SELL_BELOW_R1_PDH".equals(setup);
        if (isReversal || (isMagnet && !isReversal)) {
            // Check if OV magnet (not EV — EV magnets are already isReversal)
            boolean shouldHalve = isReversal; // all EV reversals get halved
            if (!shouldHalve && isMagnet) {
                double firstCloseOv = candleAggregator.getFirstCandleClose(symbol);
                if (firstCloseOv > 0) {
                    double upperBound = Math.max(r1, ph);
                    double lowerBound = Math.min(s1, pl);
                    shouldHalve = firstCloseOv > upperBound || firstCloseOv < lowerBound;
                }
            }
            if (shouldHalve) {
                int reduced = Math.max(2, (qty / 4) * 2); // halve and round to even
                String reason = isReversal ? "EV mean-reversion (counter daily trend)" : "OV magnet (outside value)";
                eventService.log("[INFO] " + symbol + " " + setup + " qty halved (" + reason + "): " + qty + " -> " + reduced);
                qty = reduced;
            }
        }

        // ── 4j. Build description ─────────────────────────────────────────────
        String ts = LocalTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        StringBuilder desc = new StringBuilder();

        // Header
        desc.append("═══ TRADE DETAILS ═══");
        if (isReversal) desc.append("\n").append(ts).append(" [REVERSAL] EV mean-reversion trade — OR counter-trend");

        // [ENTRY] line
        String levelName = levelNameForSetup(setup);
        desc.append("\n").append(ts).append(" [ENTRY] ").append(setup).append(" | ").append(probability)
            .append(" — Close ").append(fmt(close))
            .append(" broke ").append(levelName)
            .append(" (").append(fmt(breakoutLevel)).append(").");

        // [SL] line
        desc.append("\n").append(ts).append(" [SL] ").append(fmt(sl))
            .append(" (ATR ").append(fmt(atr)).append(" × ").append(riskSettings.getAtrMultiplier()).append(").");

        // [TARGET] line
        desc.append("\n").append(ts).append(" [TARGET] ").append(fmt(target));
        if (target != defaultTarget) {
            desc.append(" (shifted from ").append(fmt(defaultTarget)).append(" to day high/low).");
        }

        // [QTY] line
        int fixedQty = riskSettings.getFixedQuantity();
        if (fixedQty != -1) {
            desc.append("\n").append(ts).append(" [QTY] Fixed: ").append(baseQty).append(".");
        } else {
            double riskPerTrade = riskSettings.getRiskPerTrade();
            double riskPerShare = Math.abs(close - sl);
            int riskQty = riskPerShare > 0 ? (int)(riskPerTrade / riskPerShare) : 0;
            desc.append("\n").append(ts).append(" [QTY] Risk: ₹").append(fmt(riskPerTrade))
                .append(" / ₹").append(fmt(riskPerShare))
                .append(" = ").append(riskQty)
                .append(". Final: ").append(qty).append(".");
        }
        if (qtyLog != null && qty != baseQty) {
            desc.append(" (reduced from ").append(baseQty).append(")");
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
        if (target != defaultTarget) {
            eventService.log("[INFO] " + symbol + " " + setup + " target shifted to day high/low: " + fmt(defaultTarget) + " -> " + fmt(target));
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
            .dayHighLowShifted(dayHighLowShifted)
            .build();
    }

    // ── Breakout level per setup ────────────────────────────────────────────────
    private double computeBreakoutLevel(String setup,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc,
            double dayHigh, double dayLow) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"    -> Math.max(tc, bc);
            case "BUY_ABOVE_R1_PDH" -> Math.max(r1, ph);
            case "BUY_ABOVE_R2"     -> r2;
            case "BUY_ABOVE_R3"     -> r3;
            case "BUY_ABOVE_R4"     -> r4;
            case "BUY_ABOVE_S1_PDL" -> Math.max(s1, pl);
            case "BUY_ABOVE_DH"     -> dayHigh;
            case "SELL_BELOW_CPR"    -> Math.min(tc, bc);
            case "SELL_BELOW_S1_PDL" -> Math.min(s1, pl);
            case "SELL_BELOW_S2"     -> s2;
            case "SELL_BELOW_S3"     -> s3;
            case "SELL_BELOW_S4"     -> s4;
            case "SELL_BELOW_R1_PDH" -> Math.min(r1, ph);
            case "SELL_BELOW_DL"     -> dayLow;
            default -> 0;
        };
    }

    // ── Default + shift target per setup ────────────────────────────────────────
    private double[] computeTargets(String setup, double close,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc,
            double dayHigh, double dayLow) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"     -> new double[]{ Math.min(r1, ph), r2 };
            case "BUY_ABOVE_R1_PDH"  -> new double[]{ r2, r3 };
            case "BUY_ABOVE_R2"      -> new double[]{ r3, r4 };
            case "BUY_ABOVE_R3"      -> new double[]{ r4, r4 };
            case "BUY_ABOVE_R4"      -> { double r5 = r4 + (r4 - r3); yield new double[]{ r5, r5 }; }
            case "BUY_ABOVE_S1_PDL"  -> new double[]{ Math.min(tc, bc), Math.min(r1, ph) };
            case "BUY_ABOVE_DH"      -> { yield new double[]{ nextLevelAbove(dayHigh, r1, r2, r3, r4, ph, tc, bc), 0 }; }
            case "SELL_BELOW_CPR"     -> new double[]{ Math.max(s1, pl), s2 };
            case "SELL_BELOW_S1_PDL"  -> new double[]{ s2, s3 };
            case "SELL_BELOW_S2"      -> new double[]{ s3, s4 };
            case "SELL_BELOW_S3"      -> new double[]{ s4, s4 };
            case "SELL_BELOW_S4"      -> { double s5 = s4 - (s3 - s4); yield new double[]{ s5, s5 }; }
            case "SELL_BELOW_R1_PDH"  -> new double[]{ Math.max(tc, bc), Math.max(s1, pl) };
            case "SELL_BELOW_DL"      -> { yield new double[]{ nextLevelBelow(dayLow, s1, s2, s3, s4, pl, tc, bc), 0 }; }
            default -> new double[]{ 0, 0 };
        };
    }

    /** Find the next CPR level above dayHigh for BUY_ABOVE_DH target. */
    private double nextLevelAbove(double dh, double r1, double r2, double r3, double r4,
                                   double ph, double tc, double bc) {
        double cprTop = Math.max(tc, bc);
        double r1ph = Math.max(r1, ph);
        if (dh >= r4) return r4 + (r4 - r3); // projected R5
        if (dh >= r3) return r4;
        if (dh >= r2) return r3;
        if (dh >= r1ph) return r2;
        if (dh >= cprTop) return r1ph;
        return r1ph; // fallback
    }

    /** Find the next CPR level below dayLow for SELL_BELOW_DL target. */
    private double nextLevelBelow(double dl, double s1, double s2, double s3, double s4,
                                   double pl, double tc, double bc) {
        double cprBot = Math.min(tc, bc);
        double s1pl = Math.min(s1, pl);
        if (dl <= s4) return s4 - (s3 - s4); // projected S5
        if (dl <= s3) return s4;
        if (dl <= s2) return s3;
        if (dl <= s1pl) return s2;
        if (dl <= cprBot) return s1pl;
        return s1pl; // fallback
    }

    /**
     * Mean-reversion targets for EV reversal trades.
     * Gap up + OR bearish: sell breakdowns target the next level back toward value.
     * Gap down + OR bullish: buy breakouts target the next level back toward value.
     */
    private double[] computeReversalTargets(String setup,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc) {
        return switch (setup) {
            // Gap up reversal (selling back down)
            case "SELL_BELOW_R4"      -> new double[]{ r3, r3 };
            case "SELL_BELOW_R3"      -> new double[]{ r2, r2 };
            case "SELL_BELOW_R2"      -> new double[]{ Math.max(r1, ph), Math.max(r1, ph) };
            case "SELL_BELOW_R1_PDH"  -> new double[]{ Math.max(tc, bc), Math.max(s1, pl) }; // magnet target
            // Gap down reversal (buying back up)
            case "BUY_ABOVE_S4"       -> new double[]{ s3, s3 };
            case "BUY_ABOVE_S3"       -> new double[]{ s2, s2 };
            case "BUY_ABOVE_S2"       -> new double[]{ Math.min(s1, pl), Math.min(s1, pl) };
            case "BUY_ABOVE_S1_PDL"   -> new double[]{ Math.min(tc, bc), Math.min(r1, ph) }; // magnet target
            // Other setups — fall back to normal targets (shouldn't reach here in EV reversal)
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
            case "BUY_ABOVE_DH"     -> "Day High";
            case "SELL_BELOW_DL"    -> "Day Low";
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
