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
    private final EmaService       emaService;
    private final HtfEmaService    htfEmaService;

    public SignalProcessor(RiskSettingsStore riskSettings, EventService eventService,
                           QuantityService quantityService, MarketDataService marketDataService,
                           CandleAggregator candleAggregator, WeeklyCprService weeklyCprService,
                           EmaService emaService, HtfEmaService htfEmaService) {
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.quantityService = quantityService;
        this.marketDataService = marketDataService;
        this.candleAggregator = candleAggregator;
        this.emaService = emaService;
        this.htfEmaService = htfEmaService;
        this.weeklyCprService = weeklyCprService;
    }

    public ProcessedSignal process(Map<String, Object> alert) {

        // ── 4a. Parse alert fields ──────────────────────────────────────────────
        String setup       = str(alert, "setup");
        String rawSymbol   = str(alert, "symbol");
        // TradingView uses _ in symbols (e.g. BAJAJ_AUTO), Fyers uses - (BAJAJ-AUTO)
        String symbol      = rawSymbol.replace("_", "-");
        String probability = str(alert, "probability");
        // Accumulates every probability/qty adjustment for inclusion in the trade description.
        java.util.List<String> adjustments = new java.util.ArrayList<>();
        // Scanner may have already downgraded probability (e.g. NIFTY index alignment) — surface that note.
        String scannerNote = str(alert, "scannerNote");
        if (scannerNote != null && !scannerNote.isEmpty()) adjustments.add(scannerNote);
        // Capture EMA 20/50 pattern at time of signal (informational — for post-trade analysis).
        // ATR comes from the alert payload below, so we defer pattern check until after atr is parsed.
        double close       = dbl(alert, "close");
        double atr         = dbl(alert, "atr");
        double dayOpen     = dbl(alert, "dayOpen");
        // EMA 20/50 pattern at time of signal (for post-trade analysis)
        try {
            if (atr > 0 && emaService != null) {
                String emaPattern = emaService.getEmaPattern(symbol,
                    riskSettings.getEmaPatternLookback(), atr,
                    riskSettings.getBraidedMinCrossovers(), riskSettings.getBraidedMaxSpreadAtr(),
                    riskSettings.getRailwayMaxCv(), riskSettings.getRailwayMinSpreadAtr());
                if ("RAILWAY_UP".equals(emaPattern)) adjustments.add("EMA 20/50: R-RTP (rising railway track — bullish parallel trend)");
                else if ("RAILWAY_DOWN".equals(emaPattern)) adjustments.add("EMA 20/50: F-RTP (falling railway track — bearish parallel trend)");
                else if ("BRAIDED".equals(emaPattern)) adjustments.add("EMA 20/50: ZIG ZAG (braided — choppy, whipsaw risk)");
            }
        } catch (Exception ignored) {}
        // Volume snapshot at signal time: breakout candle volume vs 20-candle average
        try {
            CandleAggregator.CandleBar breakoutCandle = candleAggregator.getLastCompletedCandle(symbol);
            long breakoutVol = breakoutCandle != null ? breakoutCandle.volume : 0;
            double avgVol = candleAggregator.getAvgVolume(symbol, 20);
            if (breakoutVol > 0 && avgVol > 0) {
                double multiple = breakoutVol / avgVol;
                adjustments.add(String.format("Volume: %s vs 20-avg %s (%.2fx)",
                    fmtVol(breakoutVol), fmtVol((long) avgVol), multiple));
            } else if (breakoutVol > 0) {
                adjustments.add("Volume: " + fmtVol(breakoutVol) + " (no 20-avg baseline yet)");
            }
        } catch (Exception ignored) {}
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
        // Always compute the close-based (default) SL. If structural SL is enabled for this
        // setup, also compute the level-anchored SL and pick whichever is TIGHTER (smaller
        // distance from entry). Structural SL is skipped for DH/DL — no real level to anchor to.
        boolean isDhDl = "BUY_ABOVE_DH".equals(setup) || "SELL_BELOW_DL".equals(setup);
        double atrMultiplier = riskSettings.getAtrMultiplier();
        double defaultSl = isBuy ? (close - atr * atrMultiplier) : (close + atr * atrMultiplier);
        double sl = defaultSl;
        boolean useStructuralSl = false;
        if (riskSettings.isEnableStructuralSl()) {
            // For DH/DL setups, the structural anchor is the 20 EMA (trend support/resistance)
            // For all other setups, the anchor is the CPR/R/S level that was broken
            double anchor = isDhDl
                ? emaService.getEma(symbol)
                : computeStructuralAnchor(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
            if (anchor > 0) {
                double buffer = riskSettings.getStructuralSlBufferAtr();
                double structSl = isBuy ? (anchor - atr * buffer) : (anchor + atr * buffer);
                // Always use structural SL when enabled — it anchors to the broken level
                // and guarantees the SL sits on the correct side of the support/resistance.
                // The old "pick tighter" rule could place the default ATR-based SL inside
                // the level's zone (e.g. below CPR top for a sell), causing premature stops
                // on normal level retests.
                sl = structSl;
                useStructuralSl = true;
                double defaultDist = Math.abs(close - defaultSl);
                double structDist = Math.abs(close - structSl);
                eventService.log("[INFO] " + symbol + " " + setup + " using structural SL " + fmt(structSl)
                    + " (dist " + fmt(structDist) + ", anchor " + fmt(anchor) + " ± " + buffer + "×ATR)"
                    + " — default would be " + fmt(defaultSl) + " (dist " + fmt(defaultDist) + ")");
            }
        }

        // ── 4c3. Compute base quantity (uses SL for risk-based sizing) ─────────
        int baseQty = quantityService.computeBaseQty(symbol, close, sl);

        // ── 4d. Small candle filter ─────────────────────────────────────────────
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

        // ── 4d2. Large candle body filter ──────────────────────────────────────
        if (riskSettings.isEnableLargeCandleBodyFilter() && candleOpen > 0 && atr > 0) {
            double candleBody = Math.abs(close - candleOpen);
            double largeThreshold = riskSettings.getLargeCandleBodyAtrThreshold();
            if (candleBody > atr * largeThreshold) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Large candle body (" + fmt(candleBody) + ") > " + largeThreshold + " ATR (" + fmt(atr * largeThreshold) + ") — exhaustion risk");
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

        // ── 4e2. Opening Range filter ──
        // On EV days: if OR is broken, trade direction must match OR break. Gap-fade (counter-gap) still fires
        //   as LPT mean-reversion (isReversal=true) to use reversal targets.
        // Any day type (EV/IV/OV): if breakout close is INSIDE the OR range, downgrade HPT→LPT and proceed
        //   (no skip, no qty reduction — just LPT).
        int orMinutes = riskSettings.getOpeningRangeMinutes();
        boolean isReversal = false;
        if (orMinutes > 0) {
            double firstClose = candleAggregator.getFirstCandleClose(symbol);
            boolean gapUp   = firstClose > 0 && r2 > 0 && firstClose >= r2;
            boolean gapDown = firstClose > 0 && s2 > 0 && firstClose <= s2;
            boolean isEv = gapUp || gapDown;

            // IV/OV days: mean-reversion setups not allowed (only fire on EV days)
            if (!isEv && isMeanReversionSetup(setup)) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Mean-reversion setup only allowed on EV days (gap up/down) — today is IV/OV");
            }

            if (candleAggregator.isOpeningRangeLocked(symbol)) {
                double orHigh = candleAggregator.getOpeningRangeHigh(symbol);
                double orLow  = candleAggregator.getOpeningRangeLow(symbol);

                if (isEv) {
                    boolean orBullish = close > orHigh;
                    boolean orBearish = close < orLow;

                    // EV + OR broken: direction must match the break
                    if (orBullish || orBearish) {
                        boolean directionMatchesOR = (isBuy && orBullish) || (!isBuy && orBearish);
                        if (!directionMatchesOR) {
                            return ProcessedSignal.rejected(setup, symbol,
                                "EV " + (gapUp ? "up" : "down") + " — trade direction opposes OR break ("
                                + (orBullish ? "OR Bullish" : "OR Bearish") + ") — skipped");
                        }
                        // Gap-fade (counter-gap direction): mean-reversion (LPT + reversal targets)
                        boolean isGapFade = (gapUp && !isBuy) || (gapDown && isBuy);
                        if (isGapFade) {
                            isReversal = true;
                            if (!"LPT".equals(probability)) {
                                adjustments.add("Probability " + probability + " → LPT (EV "
                                    + (gapUp ? "gap-up" : "gap-down") + " reversal — mean-reversion)");
                                probability = "LPT";
                            }
                        }
                    }
                }

                // Any day type: inside OR range → LPT, proceed as-is (no skip, no qty reduction)
                if (orHigh > 0 && orLow > 0 && close >= orLow && close <= orHigh) {
                    if ("HPT".equals(probability)) {
                        probability = "LPT";
                        adjustments.add("Probability HPT → LPT (inside OR range)");
                    }
                }
            } else if (isEv) {
                // EV detected but OR still forming — skip trade
                return ProcessedSignal.rejected(setup, symbol,
                    "EV " + (gapUp ? "up" : "down") + " detected (1st candle close " + fmt(firstClose)
                    + (gapUp ? " >= R2 " + fmt(r2) : " <= S2 " + fmt(s2))
                    + ") but Opening Range still forming — skipped");
            }
        }

        // ── 4e3. Re-check probability after all downgrades ─────────────────────
        // BreakoutScanner checked isProbabilityEnabled at signal time, but SignalProcessor
        // may have downgraded HPT → LPT (inside-OR, EV reversal, etc.). If LPT is disabled
        // and the probability was just downgraded, reject the trade here.
        if ("LPT".equals(probability) && !riskSettings.isEnableLpt()) {
            return ProcessedSignal.rejected(setup, symbol,
                "Probability downgraded to LPT after scanner — LPT trades disabled");
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

        // ── 4g. Target shift to nearest resistance/support ──────────────────────
        // Candidates: session H/L + weekly CPR levels. Pick the nearest level strictly
        // between entry (close) and the structural target. Single target (no T1/T2 split)
        // when a shift occurs. DH/DL setups skip session H/L (breakout just created it)
        // but still consider weekly levels.
        boolean dayHighLowShifted = false;
        {
            java.util.Map<Double, String> shiftCandidates = new java.util.LinkedHashMap<>();
            if (!isDhDl && riskSettings.isEnableDayHighLowTargetShift()) {
                double minDist = riskSettings.getDayHighLowShiftMinDistAtr() * atr;
                if (isBuy) {
                    double sh = candleAggregator.getDayHighBeforeLast(symbol);
                    if (sh > 0 && (sh - close) >= minDist) shiftCandidates.put(sh, "session high");
                } else {
                    double sessionLow = candleAggregator.getDayLowBeforeLast(symbol);
                    if (sessionLow > 0 && (close - sessionLow) >= minDist) shiftCandidates.put(sessionLow, "session low");
                }
            }
            WeeklyCprService.WeeklyLevels wl = riskSettings.isEnableWeeklyLevelTargetShift()
                ? weeklyCprService.getWeeklyLevels(symbol) : null;
            if (wl != null) {
                shiftCandidates.putIfAbsent(wl.r1,    "weekly R1");
                shiftCandidates.putIfAbsent(wl.s1,    "weekly S1");
                shiftCandidates.putIfAbsent(wl.ph,    "weekly PH");
                shiftCandidates.putIfAbsent(wl.pl,    "weekly PL");
                shiftCandidates.putIfAbsent(wl.tc,    "weekly TC");
                shiftCandidates.putIfAbsent(wl.bc,    "weekly BC");
                shiftCandidates.putIfAbsent(wl.pivot, "weekly Pivot");
            }
            // 200 EMA as target shift candidate — only when the EMA trend gate is disabled
            // (trade fires into the 200 EMA rather than being blocked by it).
            // The 200 EMA acts as resistance (buys) or support (sells) for target capping.
            if (!riskSettings.isEnableEmaTrendCheck()) {
                double ema200 = emaService.getEma200(symbol);
                if (ema200 > 0) {
                    shiftCandidates.putIfAbsent(ema200, "200 EMA");
                }
            }
            // Weekly (HTF 60-min) EMA levels — any of EMA(20/50/200) sitting between close and
            // the structural target becomes a resistance/support candidate. These are the same
            // values shown as the "1h" row under the EMA Levels section on the scanner card.
            if (htfEmaService != null) {
                double htfE20  = htfEmaService.getEma(symbol);
                double htfE50  = htfEmaService.getEma50(symbol);
                double htfE200 = htfEmaService.getEma200(symbol);
                if (htfE20  > 0) shiftCandidates.putIfAbsent(htfE20,  "weekly EMA 20");
                if (htfE50  > 0) shiftCandidates.putIfAbsent(htfE50,  "weekly EMA 50");
                if (htfE200 > 0) shiftCandidates.putIfAbsent(htfE200, "weekly EMA 200");
            }
            Double bestLevel = null;
            String bestName = null;
            double bestDist = Double.MAX_VALUE;
            for (java.util.Map.Entry<Double, String> e : shiftCandidates.entrySet()) {
                double lvl = e.getKey();
                if (lvl <= 0) continue;
                boolean valid = isBuy
                    ? (lvl > close && lvl < target)
                    : (lvl < close && lvl > target);
                if (!valid) continue;
                double dist = Math.abs(lvl - close);
                if (dist < bestDist) {
                    bestDist = dist;
                    bestLevel = lvl;
                    bestName = e.getValue();
                }
            }
            if (bestLevel != null) {
                eventService.log("[INFO] " + symbol + " " + setup + " target shifted to "
                    + bestName + ": " + fmt(target) + " → " + fmt(bestLevel)
                    + " (nearest resistance/support between entry and target)");
                target = bestLevel;
                dayHighLowShifted = true;
            }
        }

        // ── 4g2. Risk/Reward filter — reject if reward < minRiskRewardRatio × risk ──
        if (riskSettings.isEnableRiskRewardFilter()) {
            double minRR = riskSettings.getMinRiskRewardRatio();
            double risk   = Math.abs(close - sl);
            double reward = Math.abs(target - close);
            double rr = risk > 0 ? reward / risk : 0;
            if (rr < minRR) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Risk/Reward " + fmt(rr) + ":1 < " + minRR + ":1 — "
                    + "entry=" + fmt(close) + " SL=" + fmt(sl) + " target=" + fmt(target)
                    + " risk=" + fmt(risk) + " reward=" + fmt(reward));
            }
        }

        // ── 4i. Quantity ────────────────────────────────────────────────────────
        int qty = baseQty;
        String qtyLog = null;
        boolean isExtreme = "BUY_ABOVE_R3".equals(setup) || "SELL_BELOW_S3".equals(setup) || isR4S4;
        if (isExtreme) {
            qty = Math.max(1, qty / 2);
            qtyLog = "[INFO] " + symbol + " " + setup + " qty halved (extreme level): " + baseQty + " -> " + qty;
            adjustments.add("Qty " + baseQty + " → " + qty + " (halved — extreme level R3/S3/R4/S4)");
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
                adjustments.add("Qty " + qty + " → " + reduced + " (halved — session move " + fmt(movePct) + "% > " + fmt(riskSettings.getSessionMoveLimit()) + "% limit from " + moveSource + ")");
                qty = reduced;
            }
        }

        // ── 4i2. Probability-based qty adjustment (LPT = configurable, default 0.50) ──
        if ("LPT".equals(probability)) {
            double factor = riskSettings.getLptQtyFactor();
            int reduced = Math.max(1, (int)(qty * factor)); // apply factor, round to even
            eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (LPT ×" + factor + "): " + qty + " -> " + reduced);
            adjustments.add("Qty " + qty + " → " + reduced + " (×" + factor + " — LPT probability)");
            qty = reduced;
        }

        // Weekly NEUTRAL trades are always LPT (downgraded in BreakoutScanner) — no additional qty reduction.

        // ── 4i3. Level-based qty adjustment (R3/S3 and R4/S4 extended levels) ──
        if (setup.contains("R3") || setup.contains("S3")) {
            double factor = riskSettings.getR3s3QtyFactor();
            if (factor < 1.0) {
                int reduced = Math.max(1, (int)(qty * factor));
                eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (R3/S3 ×" + factor + "): " + qty + " -> " + reduced);
                adjustments.add("Qty " + qty + " → " + reduced + " (×" + factor + " — R3/S3 extended level)");
                qty = reduced;
            }
        } else if (setup.contains("R4") || setup.contains("S4")) {
            double factor = riskSettings.getR4s4QtyFactor();
            if (factor < 1.0) {
                int reduced = Math.max(1, (int)(qty * factor));
                eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (R4/S4 ×" + factor + "): " + qty + " -> " + reduced);
                adjustments.add("Qty " + qty + " → " + reduced + " (×" + factor + " — R4/S4 extended level)");
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
                int reduced = Math.max(1, qty / 2);
                String reason = isReversal ? "EV mean-reversion (counter daily trend)" : "OV magnet (outside value)";
                eventService.log("[INFO] " + symbol + " " + setup + " qty halved (" + reason + "): " + qty + " -> " + reduced);
                adjustments.add("Qty " + qty + " → " + reduced + " (halved — " + reason + ")");
                qty = reduced;
            }
        }

        // ── 4i6. Minimum absolute profit filter ────────────────────────────────
        double minProfit = riskSettings.getMinAbsoluteProfit();
        if (minProfit > 0) {
            double expectedProfit = qty * Math.abs(target - close);
            if (expectedProfit < minProfit) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Absolute profit too low — ₹" + fmt(expectedProfit) + " < ₹" + fmt(minProfit)
                    + " (qty=" + qty + " × dist=" + fmt(Math.abs(target - close)) + ")");
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
        desc.append("\n").append(ts).append(" [SL] ").append(fmt(sl));
        if (useStructuralSl) {
            double anchor = computeStructuralAnchor(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
            desc.append(" (structural anchor ").append(fmt(anchor))
                .append(" ").append(isBuy ? "−" : "+").append(" ").append(riskSettings.getStructuralSlBufferAtr())
                .append(" × ATR ").append(fmt(atr)).append(").");
        } else {
            desc.append(" (ATR ").append(fmt(atr)).append(" × ").append(riskSettings.getAtrMultiplier()).append(").");
        }

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

        // [ADJUST] lines — every probability/qty change applied during signal processing
        for (String note : adjustments) {
            desc.append("\n").append(ts).append(" [ADJUST] ").append(note);
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
            .useStructuralSl(useStructuralSl)
            .build();
    }

    // ── Structural SL anchor per setup (outer edge of zone for pairs) ──────────
    private static boolean isDhDlSetup(String setup) {
        return "BUY_ABOVE_DH".equals(setup) || "SELL_BELOW_DL".equals(setup);
    }

    private static double computeStructuralAnchor(String setup,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"     -> Math.min(tc, bc);
            case "SELL_BELOW_CPR"    -> Math.max(tc, bc);
            case "BUY_ABOVE_R1_PDH"  -> Math.min(r1, ph);
            case "SELL_BELOW_S1_PDL" -> Math.max(s1, pl);
            case "BUY_ABOVE_S1_PDL"  -> Math.min(s1, pl); // magnet bounce — outer edge of support zone
            case "SELL_BELOW_R1_PDH" -> Math.max(r1, ph); // magnet rejection — outer edge of resistance zone
            case "BUY_ABOVE_R2", "SELL_BELOW_R2" -> r2;
            case "BUY_ABOVE_R3", "SELL_BELOW_R3" -> r3;
            case "BUY_ABOVE_R4", "SELL_BELOW_R4" -> r4;
            case "BUY_ABOVE_S2", "SELL_BELOW_S2" -> s2;
            case "BUY_ABOVE_S3", "SELL_BELOW_S3" -> s3;
            case "BUY_ABOVE_S4", "SELL_BELOW_S4" -> s4;
            default -> 0;
        };
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
            case "BUY_ABOVE_S2"     -> s2;
            case "BUY_ABOVE_S3"     -> s3;
            case "BUY_ABOVE_S4"     -> s4;
            case "BUY_ABOVE_DH"     -> dayHigh;
            case "SELL_BELOW_CPR"    -> Math.min(tc, bc);
            case "SELL_BELOW_S1_PDL" -> Math.min(s1, pl);
            case "SELL_BELOW_S2"     -> s2;
            case "SELL_BELOW_S3"     -> s3;
            case "SELL_BELOW_S4"     -> s4;
            case "SELL_BELOW_R1_PDH" -> Math.min(r1, ph);
            case "SELL_BELOW_R2"     -> r2;
            case "SELL_BELOW_R3"     -> r3;
            case "SELL_BELOW_R4"     -> r4;
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
            case "BUY_ABOVE_DH"      -> { yield new double[]{ nextLevelAbove(Math.max(close, dayHigh), r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc), 0 }; }
            case "SELL_BELOW_CPR"     -> new double[]{ Math.max(s1, pl), s2 };
            case "SELL_BELOW_S1_PDL"  -> new double[]{ s2, s3 };
            case "SELL_BELOW_S2"      -> new double[]{ s3, s4 };
            case "SELL_BELOW_S3"      -> new double[]{ s4, s4 };
            case "SELL_BELOW_S4"      -> { double s5 = s4 - (s3 - s4); yield new double[]{ s5, s5 }; }
            case "SELL_BELOW_R1_PDH"  -> new double[]{ Math.max(tc, bc), Math.max(s1, pl) };
            // Mean-reversion fades from above R-levels (gap-up reversal pattern, also valid intraday)
            case "SELL_BELOW_R4"      -> new double[]{ r3, r3 };
            case "SELL_BELOW_R3"      -> new double[]{ r2, r2 };
            case "SELL_BELOW_R2"      -> new double[]{ Math.max(r1, ph), Math.max(r1, ph) };
            // Mean-reversion bounces from below S-levels (gap-down reversal pattern, also valid intraday)
            case "BUY_ABOVE_S4"       -> new double[]{ s3, s3 };
            case "BUY_ABOVE_S3"       -> new double[]{ s2, s2 };
            case "BUY_ABOVE_S2"       -> new double[]{ Math.min(s1, pl), Math.min(s1, pl) };
            case "SELL_BELOW_DL"      -> { yield new double[]{ nextLevelBelow(Math.min(close, dayLow), r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc), 0 }; }
            default -> new double[]{ 0, 0 };
        };
    }

    /** Find the nearest CPR/PDH/PDL level strictly above `from`. Includes all individual levels
     *  (R1 and PDH separate, S1 and PDL separate) so the nearest one is picked as target. */
    private double nextLevelAbove(double from, double r1, double r2, double r3, double r4,
                                   double s1, double s2, double s3, double s4,
                                   double ph, double pl, double tc, double bc) {
        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);
        double pivot = (cprTop + cprBot) / 2.0;
        double[] levels = { s4, s3, s2, s1, pl, cprBot, pivot, cprTop, r1, ph, r2, r3, r4 };
        double best = Double.MAX_VALUE;
        for (double lvl : levels) {
            if (lvl > from && lvl < best) best = lvl;
        }
        if (best == Double.MAX_VALUE) {
            return r4 > 0 && r3 > 0 ? r4 + (r4 - r3) : from; // projected R5 if above all known levels
        }
        return best;
    }

    /** Find the nearest CPR/PDH/PDL level strictly below `from`. Includes all individual levels
     *  (R1 and PDH separate, S1 and PDL separate) so the nearest one is picked as target. */
    private double nextLevelBelow(double from, double r1, double r2, double r3, double r4,
                                   double s1, double s2, double s3, double s4,
                                   double ph, double pl, double tc, double bc) {
        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);
        double pivot = (cprTop + cprBot) / 2.0;
        double[] levels = { r4, r3, r2, r1, ph, cprTop, pivot, cprBot, s1, pl, s2, s3, s4 };
        double best = -1;
        for (double lvl : levels) {
            if (lvl > 0 && lvl < from && lvl > best) best = lvl;
        }
        if (best < 0) {
            return s4 > 0 && s3 > 0 ? s4 - (s3 - s4) : from; // projected S5 if below all known levels
        }
        return best;
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

    /**
     * R2+/S2+ mean-reversion fades — only valid on EV (gap) days.
     * Magnets (SELL_BELOW_R1_PDH, BUY_ABOVE_S1_PDL) are excluded — they fire on all day types.
     */
    private static boolean isMeanReversionSetup(String setup) {
        return "SELL_BELOW_R2".equals(setup)
            || "SELL_BELOW_R3".equals(setup)
            || "SELL_BELOW_R4".equals(setup)
            || "BUY_ABOVE_S2".equals(setup)
            || "BUY_ABOVE_S3".equals(setup)
            || "BUY_ABOVE_S4".equals(setup);
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
            case "SELL_BELOW_R2"     -> "R2";
            case "SELL_BELOW_R3"     -> "R3";
            case "SELL_BELOW_R4"     -> "R4";
            case "BUY_ABOVE_S2"      -> "S2";
            case "BUY_ABOVE_S3"      -> "S3";
            case "BUY_ABOVE_S4"      -> "S4";
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

    private static String fmtVol(long v) {
        if (v <= 0) return "--";
        if (v >= 10_000_000L) return String.format("%.2fCr", v / 10_000_000.0);
        if (v >= 100_000L)    return String.format("%.2fL", v / 100_000.0);
        if (v >= 1_000L)      return String.format("%.1fK", v / 1_000.0);
        return String.valueOf(v);
    }
}
