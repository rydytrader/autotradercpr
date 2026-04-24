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
    private final SmaService       smaService;
    private final HtfSmaService    htfSmaService;
    private final MarketHolidayService marketHolidayService;

    public SignalProcessor(RiskSettingsStore riskSettings, EventService eventService,
                           QuantityService quantityService, MarketDataService marketDataService,
                           CandleAggregator candleAggregator, WeeklyCprService weeklyCprService,
                           SmaService smaService, HtfSmaService htfSmaService,
                           MarketHolidayService marketHolidayService) {
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.quantityService = quantityService;
        this.marketDataService = marketDataService;
        this.candleAggregator = candleAggregator;
        this.smaService = smaService;
        this.htfSmaService = htfSmaService;
        this.weeklyCprService = weeklyCprService;
        this.marketHolidayService = marketHolidayService;
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
        // Capture SMA 20/50 pattern at time of signal (informational — for post-trade analysis).
        // ATR comes from the alert payload below, so we defer pattern check until after atr is parsed.
        double close       = dbl(alert, "close");
        double atr         = dbl(alert, "atr");
        double dayOpen     = dbl(alert, "dayOpen");
        // SMA 20/50 pattern at time of signal (for post-trade analysis)
        try {
            if (atr > 0 && smaService != null) {
                String smaPattern = smaService.getSmaPattern(symbol,
                    riskSettings.getSmaPatternLookback(), atr,
                    riskSettings.getBraidedMinCrossovers(), riskSettings.getBraidedMaxSpreadAtr(),
                    riskSettings.getRailwayMaxCv(), riskSettings.getRailwayMinSpreadAtr());
                if ("RAILWAY_UP".equals(smaPattern)) adjustments.add("SMA 20/50: R-RTP (rising railway track — bullish parallel trend)");
                else if ("RAILWAY_DOWN".equals(smaPattern)) adjustments.add("SMA 20/50: F-RTP (falling railway track — bearish parallel trend)");
                else if ("BRAIDED".equals(smaPattern)) adjustments.add("SMA 20/50: ZIG ZAG (braided — choppy, whipsaw risk)");
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

        // ── Extended-level skip gate ─────────────────────────────────────────
        // R3/S3 and R4/S4 breakouts skipped on ALL day types when their respective toggle is on.
        // DH/DL breakouts that have already reached or passed an R3/S3 or R4/S4 level are
        // treated identically — a DH break above R3 IS effectively an R3 trade.
        boolean isR3S3Setup = "BUY_ABOVE_R3".equals(setup) || "SELL_BELOW_S3".equals(setup)
                           || "BUY_ABOVE_S3".equals(setup) || "SELL_BELOW_R3".equals(setup);
        boolean isR4S4Setup = "BUY_ABOVE_R4".equals(setup) || "SELL_BELOW_S4".equals(setup)
                           || "BUY_ABOVE_S4".equals(setup) || "SELL_BELOW_R4".equals(setup);
        boolean isDhAboveR3 = "BUY_ABOVE_DH".equals(setup)  && r3 > 0 && dayHigh > r3;
        boolean isDhAboveR4 = "BUY_ABOVE_DH".equals(setup)  && r4 > 0 && dayHigh > r4;
        boolean isDlBelowS3 = "SELL_BELOW_DL".equals(setup) && s3 > 0 && dayLow  < s3;
        boolean isDlBelowS4 = "SELL_BELOW_DL".equals(setup) && s4 > 0 && dayLow  < s4;

        if (riskSettings.isSkipR3S3NormalDays()) {
            if (isR3S3Setup) {
                return ProcessedSignal.rejected(setup, symbol,
                    "R3/S3 breakout skipped (extended-level skip enabled)");
            }
            if (isDhAboveR3 || isDlBelowS3) {
                return ProcessedSignal.rejected(setup, symbol,
                    (isDhAboveR3 ? "DH above R3" : "DL below S3")
                    + " skipped (extended-level skip enabled — DH/DL beyond R3/S3 treated as R3/S3)");
            }
        }
        if (riskSettings.isSkipR4S4NormalDays()) {
            if (isR4S4Setup) {
                return ProcessedSignal.rejected(setup, symbol,
                    "R4/S4 breakout skipped (extended-level skip enabled)");
            }
            if (isDhAboveR4 || isDlBelowS4) {
                return ProcessedSignal.rejected(setup, symbol,
                    (isDhAboveR4 ? "DH above R4" : "DL below S4")
                    + " skipped (extended-level skip enabled — DH/DL beyond R4/S4 treated as R4/S4)");
            }
        }

        // ── DH/DL inside-zone gate ─────────────────────────────────────────────
        // DH/DL breakouts are only meaningful when the broken level sits in clear air —
        // not inside an existing CPR / R1+PDH / S1+PDL zone. A DH that's actually within
        // the CPR (or R1+PDH) zone is just a level retest, not a fresh structural break.
        if ("BUY_ABOVE_DH".equals(setup) || "SELL_BELOW_DL".equals(setup)) {
            boolean isDh = "BUY_ABOVE_DH".equals(setup);
            double level = isDh ? dayHigh : dayLow;
            if (level > 0) {
                double cprLo  = Math.min(tc, bc), cprHi  = Math.max(tc, bc);
                double r1pLo  = Math.min(r1, ph), r1pHi  = Math.max(r1, ph);
                double s1pLo  = Math.min(s1, pl), s1pHi  = Math.max(s1, pl);
                String inZone = null;
                if      (cprLo > 0 && cprHi > 0 && level >= cprLo && level <= cprHi) inZone = "CPR";
                else if (r1pLo > 0 && r1pHi > 0 && level >= r1pLo && level <= r1pHi) inZone = "R1+PDH";
                else if (s1pLo > 0 && s1pHi > 0 && level >= s1pLo && level <= s1pHi) inZone = "S1+PDL";
                if (inZone != null) {
                    return ProcessedSignal.rejected(setup, symbol,
                        (isDh ? "DH" : "DL") + " (" + fmt(level) + ") is inside " + inZone
                        + " zone — not a clean structural breakout");
                }
            }
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
            // Structural anchor:
            //  - DH/DL breakouts: the breakout candle's own low (for buys) or high (for sells).
            //    The candle's extreme is the structural level the breakout defended — if price
            //    falls back through it, the breakout has failed. Falls back to candle close
            //    if low/high aren't in the payload.
            //  - All other setups: the CPR/R/S level that was broken (computeStructuralAnchor).
            double anchor;
            if (isDhDl) {
                anchor = isBuy
                    ? (candleLow  > 0 ? candleLow  : close)
                    : (candleHigh > 0 ? candleHigh : close);
            } else {
                anchor = computeStructuralAnchor(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
            }
            if (anchor > 0) {
                double buffer = riskSettings.getStructuralSlBufferAtr();
                // Single-level setups (R2/R3/R4, S2/S3/S4, DH/DL) lack the zone-width cushion
                // that zone setups (CPR, R1+PDH, S1+PDL, magnets) get for free, so add an
                // extra ATR buffer to push the SL further from the anchor.
                double extra = isSingleLevelSetup(setup) ? riskSettings.getSingleLevelSlBufferAtr() : 0;
                double totalBufferAtr = buffer + extra;
                double structSl = isBuy ? (anchor - atr * totalBufferAtr) : (anchor + atr * totalBufferAtr);
                // Always use structural SL when enabled — it anchors to the broken level
                // and guarantees the SL sits on the correct side of the support/resistance.
                // The old "pick tighter" rule could place the default ATR-based SL inside
                // the level's zone (e.g. below CPR top for a sell), causing premature stops
                // on normal level retests.
                sl = structSl;
                useStructuralSl = true;
                double defaultDist = Math.abs(close - defaultSl);
                double structDist = Math.abs(close - structSl);
                String anchorLabel = isDhDl ? (isBuy ? "candle low" : "candle high") : "level";
                String bufferLabel = extra > 0
                    ? (totalBufferAtr + "×ATR = " + buffer + " base + " + extra + " single-level")
                    : (buffer + "×ATR");
                eventService.log("[INFO] " + symbol + " " + setup + " using structural SL " + fmt(structSl)
                    + " (dist " + fmt(structDist) + ", " + anchorLabel + " " + fmt(anchor) + " ± " + bufferLabel + ")"
                    + " — default would be " + fmt(defaultSl) + " (dist " + fmt(defaultDist) + ")");
            }
        }

        // ── 4c3. Compute base quantity (uses SL for risk-based sizing) ─────────
        int baseQty = quantityService.computeBaseQty(symbol, close, sl);

        // ── 4d. Small candle filter ─────────────────────────────────────────────
        // Collapsed "meaningful size" check: reject only when BOTH moveFromLevel and
        // candle body are below the threshold AND the candle shows no wick defense.
        // A wide-body candle that broke a level near its high now qualifies via body;
        // a narrow-body candle that pushed well past the level qualifies via move.
        // Opposite-wick penalty (Check 4) still runs separately — always applied.
        if (riskSettings.isEnableSmallCandleFilter()) {
            double smallThreshold = riskSettings.getSmallCandleAtrThreshold();
            double wickRatio      = riskSettings.getWickRejectionRatio();
            double oppWickRatio   = riskSettings.getOppositeWickRatio();
            double moveFromLevel  = isBuy ? (close - breakoutLevel) : (breakoutLevel - close);
            double candleBody     = candleOpen > 0 ? Math.abs(close - candleOpen) : 0;
            double sizeFloor      = atr * smallThreshold;

            // Meaningful size: either the move from the breakout level OR the overall
            // candle body is at least smallCandleAtrThreshold × ATR.
            boolean meaningfulSize = Math.max(moveFromLevel, candleBody) >= sizeFloor;

            // Wick defense: tiny-body doji with a long wick into the breakout direction.
            // Buyers/sellers pushed the price to the wick extreme but the close
            // recovered — committed directional intent even if the body is small.
            boolean wickDefense = false;
            if (candleOpen > 0 && candleHigh > 0 && candleLow > 0 && candleBody < sizeFloor) {
                double breakoutWick = isBuy
                    ? (Math.min(close, candleOpen) - candleLow)
                    : (candleHigh - Math.max(close, candleOpen));
                wickDefense = breakoutWick >= wickRatio * candleBody
                    && breakoutWick >= sizeFloor;
            }

            if (!meaningfulSize && !wickDefense) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Small candle — move from level (" + fmt(moveFromLevel) + ") and body ("
                    + fmt(candleBody) + ") both < " + smallThreshold + " ATR ("
                    + fmt(sizeFloor) + "), no wick defense");
            }

            // Check 4: opposite wick pressure — always checked, even for large bodies.
            // Buy: long upper wick = sellers pushing down. Sell: long lower wick = buyers pushing up.
            if (candleOpen > 0 && candleHigh > 0 && candleLow > 0) {
                double oppositeWick = isBuy
                    ? (candleHigh - Math.max(close, candleOpen))
                    : (Math.min(close, candleOpen) - candleLow);
                if (oppositeWick >= oppWickRatio * candleBody && oppositeWick >= sizeFloor) {
                    return ProcessedSignal.rejected(setup, symbol,
                        "Opposite wick pressure (" + fmt(oppositeWick) + ") — counter-pressure against breakout");
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

                // Inside-OR handling — day-type specific, magnet-aware.
                //   EV gap-OPPOSED                        → reject (fighting gap with no OR break).
                //   EV gap-ALIGNED                        → HPT retained (gap = conviction).
                //   IV/OV + TREND trade + daily aligned   → HPT retained.
                //   IV/OV + TREND trade + daily NOT aligned → HPT → LPT (no full trend support).
                //   IV/OV + MAGNET trade                  → HPT retained unconditionally
                //                                           (daily is expected to be opposite by
                //                                            the nature of a bounce-off-support /
                //                                            rejection-at-resistance setup — the
                //                                            HPT rule for magnets already uses
                //                                            weekly-only and doesn't require daily).
                boolean isMagnetSetup = "BUY_ABOVE_S1_PDL".equals(setup) || "SELL_BELOW_R1_PDH".equals(setup);
                if (orHigh > 0 && orLow > 0 && close >= orLow && close <= orHigh) {
                    if (isEv) {
                        boolean opposesGap = (gapUp && !isBuy) || (gapDown && isBuy);
                        if (opposesGap) {
                            return ProcessedSignal.rejected(setup, symbol,
                                "EV " + (gapUp ? "up" : "down") + " — inside OR range [" + fmt(orLow) + "-" + fmt(orHigh)
                                + "] and trade direction opposes gap (no OR break yet) — skipped");
                        }
                        // EV gap-aligned inside OR → HPT retained (no downgrade)
                    } else if ("HPT".equals(probability) && !isMagnetSetup) {
                        // IV/OV trend trade inside-OR — HPT retained only if daily aligned.
                        String daily = weeklyCprService.getDailyTrend(symbol);
                        boolean dailyAligned = isBuy
                            ? (daily != null && daily.contains("BULLISH"))
                            : (daily != null && daily.contains("BEARISH"));
                        if (!dailyAligned) {
                            probability = "LPT";
                            adjustments.add("Probability HPT → LPT (inside OR on IV/OV day, daily not aligned: "
                                + (daily != null ? daily : "UNKNOWN") + ")");
                        }
                        // else daily aligned → HPT retained
                    }
                    // Magnets inside OR → HPT retained (already classified via weekly-only rule)
                }
            } else if (isEv) {
                // EV detected but OR still forming — skip trade
                return ProcessedSignal.rejected(setup, symbol,
                    "EV " + (gapUp ? "up" : "down") + " detected (1st candle close " + fmt(firstClose)
                    + (gapUp ? " >= R2 " + fmt(r2) : " <= S2 " + fmt(s2))
                    + ") but Opening Range still forming — skipped");
            }
        }

        // ── 4e2a. NIFTY alignment: HPT → LPT for NIFTY-opposed trades ──
        // When NIFTY opposes the trade direction, downgrade probability to LPT regardless
        // of the stock's own alignment. No qty factor — the LPT downgrade itself routes
        // through the LPT qty factor and the LPT-enable gate.
        if (Boolean.TRUE.equals(alert.get("niftyOpposed")) && "HPT".equals(probability)) {
            probability = "LPT";
            adjustments.add("Probability HPT → LPT (NIFTY opposed)");
        }

        // ── 4e2a2. 2-Day CPR relationship: HPT → LPT when CPR doesn't favor the direction ──
        // Buys want today's CPR completely above yesterday's (HV); sells want completely below (LV).
        // Overlapping (NC) or wrong-side relation downgrades HPT → LPT. Fail-open if no relation
        // computed (e.g. fresh boot before bhavcopy classification).
        if (riskSettings.isEnableCprDayRelationFilter() && "HPT".equals(probability)) {
            String rel = str(alert, "cprDayRelation");
            if (rel != null && !rel.isEmpty()) {
                boolean wanted = isBuy ? "HV".equals(rel) : "LV".equals(rel);
                if (!wanted) {
                    probability = "LPT";
                    adjustments.add("Probability HPT → LPT (2D CPR relation 2D-" + rel
                        + ", need 2D-" + (isBuy ? "HV" : "LV") + ")");
                }
            }
        }

        // ── 4e2b. HTF Hurdle: nearest-weekly-level HPT→LPT downgrade ──
        // When a 5-min breakout closes above (for buys) the nearest weekly R1/PWH, the previous
        // 1h HTF candle must have closed above that level too — otherwise the HTF hasn't
        // confirmed the move and we downgrade HPT → LPT. Mirror for sells against S1/PWL.
        // Only R1/PWH and S1/PWL are hurdles — R2+/S2+ are far-out projections (not a wall
        // the HTF needs to clear first), and weekly CPR (TC/BC/Pivot) already falls under the
        // weekly-NEUTRAL → LPT rule.
        if (riskSettings.isEnableHtfHurdleFilter() && "HPT".equals(probability)) {
            WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(symbol);
            if (wl != null) {
                double[] candidates;
                String[] names;
                if (isBuy) {
                    candidates = new double[]{ wl.r1, wl.ph };
                    names      = new String[]{ "R1", "PWH" };
                } else {
                    candidates = new double[]{ wl.s1, wl.pl };
                    names      = new String[]{ "S1", "PWL" };
                }
                // Find the nearest weekly level in the relevant direction from the 5-min close.
                // Buy: max level strictly below close. Sell: min level strictly above close.
                double chosenLevel = 0;
                String chosenName = null;
                for (int i = 0; i < candidates.length; i++) {
                    double lv = candidates[i];
                    if (lv <= 0) continue;
                    if (isBuy) {
                        if (lv < close && lv > chosenLevel) { chosenLevel = lv; chosenName = names[i]; }
                    } else {
                        if (lv > close && (chosenName == null || lv < chosenLevel)) { chosenLevel = lv; chosenName = names[i]; }
                    }
                }
                if (chosenName != null) {
                    Double priorHtfClose = weeklyCprService.getLastHigherTfClose(symbol);
                    boolean usedPrevSession = false;
                    boolean firstTradingDay = marketHolidayService.isFirstTradingDayOfWeek();

                    // First-hour fallback: before today's first 1h candle closes, there's no
                    // today's HTF close to check against. On Tue-Fri (not first trading day),
                    // use the prior session's final 1h close — weekly CPR levels are stable
                    // across the week, so yesterday's close vs this week's reversal level is a
                    // meaningful comparison. On Monday/first-day, skip the fallback — weekly
                    // levels just went live and last Friday's close was vs last week's levels.
                    if ((priorHtfClose == null || priorHtfClose <= 0) && !firstTradingDay) {
                        Double prev = htfSmaService.getLastClose(symbol);
                        if (prev != null && prev > 0) {
                            priorHtfClose = prev;
                            usedPrevSession = true;
                        }
                    }

                    if (priorHtfClose == null || priorHtfClose <= 0) {
                        probability = "LPT";
                        adjustments.add("Probability HPT → LPT (HTF hurdle at weekly " + chosenName
                            + ": close=" + fmt(close) + ", level=" + fmt(chosenLevel)
                            + ", no prior 1h close available"
                            + (firstTradingDay ? " — first trading day of week" : "")
                            + ")");
                    } else if (isBuy ? priorHtfClose <= chosenLevel : priorHtfClose >= chosenLevel) {
                        probability = "LPT";
                        adjustments.add("Probability HPT → LPT (HTF hurdle at weekly " + chosenName
                            + ": close=" + fmt(close)
                            + (usedPrevSession ? ", prev session 1h close=" : ", prior 1h close=") + fmt(priorHtfClose)
                            + ", level=" + fmt(chosenLevel) + ")");
                    }
                    // else: prior 1h (today's or previous session's) has cleared the level → HPT retained
                }
                // else: no weekly level in the relevant direction → filter no-op (e.g. close below all buy candidates)
            }
        }

        // ── 4e2c. HTF SMA alignment — additional HPT→LPT check ──
        // Live LTP must be above (buys) / below (sells) both HTF SMA 20 and HTF SMA 50.
        // LTP-based, not candle-close: the hourly trend structure has to be on the trade's
        // side right now, regardless of whether the last 1h candle has closed. SMA 200 is
        // excluded — it's too slow a reference to be a near-term HTF alignment gate and
        // would block too many valid recoveries. Independent toggle from the weekly-level
        // hurdle — either can trigger a downgrade.
        if (riskSettings.isEnableHtfSmaAlignment() && "HPT".equals(probability)) {
            double htfS20  = htfSmaService != null ? htfSmaService.getSma(symbol)   : 0;
            double htfS50  = htfSmaService != null ? htfSmaService.getSma50(symbol) : 0;
            double liveLtp = candleAggregator.getLtp(symbol);
            if (liveLtp > 0 && htfS20 > 0 && htfS50 > 0) {
                boolean alignedBuy  = liveLtp > htfS20 && liveLtp > htfS50;
                boolean alignedSell = liveLtp < htfS20 && liveLtp < htfS50;
                if (isBuy ? !alignedBuy : !alignedSell) {
                    probability = "LPT";
                    adjustments.add("Probability HPT → LPT (HTF SMA not aligned: LTP=" + fmt(liveLtp)
                        + " vs 1h SMA 20=" + fmt(htfS20) + ", 50=" + fmt(htfS50)
                        + " — need " + (isBuy ? "LTP above both" : "LTP below both") + ")");
                }
            }
        }

        // ── 4e2d. HTF SMA order — stricter alignment check ──
        // 1h SMA 20 must be above 1h SMA 50 (buys) or below it (sells). This is the
        // SMA-to-SMA ordering check — stricter than the price-vs-SMAs gate above.
        // Mirrors the 5-min SMA alignment check (20>50>200) but using only 20/50 since
        // 200 is excluded from HTF structure rules here.
        if (riskSettings.isEnableHtfSmaAlignmentCheck() && "HPT".equals(probability)) {
            double htfS20 = htfSmaService != null ? htfSmaService.getSma(symbol)   : 0;
            double htfS50 = htfSmaService != null ? htfSmaService.getSma50(symbol) : 0;
            if (htfS20 > 0 && htfS50 > 0) {
                boolean orderedBuy  = htfS20 > htfS50;
                boolean orderedSell = htfS20 < htfS50;
                if (isBuy ? !orderedBuy : !orderedSell) {
                    probability = "LPT";
                    adjustments.add("Probability HPT → LPT (HTF SMA order not aligned: 1h SMA 20="
                        + fmt(htfS20) + ", 50=" + fmt(htfS50)
                        + " — need " + (isBuy ? "20 > 50" : "20 < 50") + ")");
                }
            }
        }

        // ── 4e3. Re-check probability after all downgrades ─────────────────────
        // Only fires when an HPT trade was actually downgraded to LPT inside SignalProcessor
        // (inside-OR, EV reversal, HTF hurdle, etc.) AND LPT is disabled. Natively-LPT trades
        // (magnets, counter-trend bounces) are silently rejected so the event log only
        // surfaces the surprising case: a would-have-been HPT blocked by the LPT setting.
        if ("LPT".equals(probability) && !riskSettings.isEnableLpt()) {
            String downgradeReason = "";
            for (String adj : adjustments) {
                if (adj != null && adj.startsWith("Probability") && adj.contains("→ LPT")) {
                    int parenOpen  = adj.indexOf('(');
                    int parenClose = adj.lastIndexOf(')');
                    if (parenOpen >= 0 && parenClose > parenOpen) {
                        if (!downgradeReason.isEmpty()) downgradeReason += "; ";
                        downgradeReason += adj.substring(parenOpen + 1, parenClose);
                    }
                }
            }
            if (downgradeReason.isEmpty()) {
                // Native LPT arrived (no in-processor downgrade). Silent rejection — user
                // already disabled LPT at the setting, no need to announce every magnet skip.
                return ProcessedSignal.silentlyRejected(setup, symbol);
            }
            return ProcessedSignal.rejected(setup, symbol,
                "Probability downgraded to LPT (" + downgradeReason + ") — LPT trades disabled");
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
            // Session H/L shift only applies when the breakout close is OUTSIDE the Opening Range.
            // Inside-OR breakouts (EV-day LPT or IV/OV-day HPT) shouldn't use session H/L as a
            // target cap — the OR itself is the immediate overhead structure, not today's session H/L.
            boolean insideOr = false;
            if (candleAggregator.isOpeningRangeLocked(symbol)) {
                double orHi = candleAggregator.getOpeningRangeHigh(symbol);
                double orLo = candleAggregator.getOpeningRangeLow(symbol);
                if (orHi > 0 && orLo > 0 && close >= orLo && close <= orHi) insideOr = true;
            }
            if (!isDhDl && !insideOr && riskSettings.isEnableDayHighLowTargetShift()) {
                double minDist = riskSettings.getDayHighLowShiftMinDistAtr() * atr;
                // Session body high/low — ignores wicks. A wick rejection isn't treated as
                // a confirmed resistance/support for target-capping; only the price level the
                // candle actually held at (body extreme) qualifies.
                if (isBuy) {
                    double sh = candleAggregator.getDayBodyHighBeforeLast(symbol);
                    if (sh > 0 && (sh - close) >= minDist) {
                        shiftCandidates.put(sh, "session body high");
                    } else if (sh > 0) {
                        eventService.log("[INFO] " + symbol + " " + setup + " session body high " + fmt(sh)
                            + " too close to close " + fmt(close) + " — shift skipped (dist "
                            + fmt(sh - close) + " < " + riskSettings.getDayHighLowShiftMinDistAtr()
                            + "×ATR = " + fmt(minDist) + ")");
                    }
                } else {
                    double sessionLow = candleAggregator.getDayBodyLowBeforeLast(symbol);
                    if (sessionLow > 0 && (close - sessionLow) >= minDist) {
                        shiftCandidates.put(sessionLow, "session body low");
                    } else if (sessionLow > 0) {
                        eventService.log("[INFO] " + symbol + " " + setup + " session body low " + fmt(sessionLow)
                            + " too close to close " + fmt(close) + " — shift skipped (dist "
                            + fmt(close - sessionLow) + " < " + riskSettings.getDayHighLowShiftMinDistAtr()
                            + "×ATR = " + fmt(minDist) + ")");
                    }
                }
            }
            WeeklyCprService.WeeklyLevels wl = riskSettings.isEnableWeeklyLevelTargetShift()
                ? weeklyCprService.getWeeklyLevels(symbol) : null;
            if (wl != null) {
                // Weekly S/R levels only — TC / BC / Pivot excluded because they're part of the
                // pivot range (derived midpoints), not real support/resistance lines. Shifting
                // to them compresses R/R unnecessarily, especially for trades firing inside
                // weekly CPR where TC/BC become the first obstacle.
                shiftCandidates.putIfAbsent(wl.r1, "weekly R1");
                shiftCandidates.putIfAbsent(wl.r2, "weekly R2");
                shiftCandidates.putIfAbsent(wl.r3, "weekly R3");
                shiftCandidates.putIfAbsent(wl.r4, "weekly R4");
                shiftCandidates.putIfAbsent(wl.s1, "weekly S1");
                shiftCandidates.putIfAbsent(wl.s2, "weekly S2");
                shiftCandidates.putIfAbsent(wl.s3, "weekly S3");
                shiftCandidates.putIfAbsent(wl.s4, "weekly S4");
                shiftCandidates.putIfAbsent(wl.ph, "weekly PH");
                shiftCandidates.putIfAbsent(wl.pl, "weekly PL");
            }
            // 200 SMA as target shift candidate — only when the SMA trend gate is disabled
            // (trade fires into the 200 SMA rather than being blocked by it).
            // The 200 SMA acts as resistance (buys) or support (sells) for target capping.
            if (!riskSettings.isEnableSmaTrendCheck()) {
                double sma200 = smaService.getSma200(symbol);
                if (sma200 > 0) {
                    shiftCandidates.putIfAbsent(sma200, "200 SMA");
                }
            }
            // Weekly (HTF 60-min) SMA levels — any of SMA(20/50/200) sitting between close and
            // the structural target becomes a resistance/support candidate. These are the same
            // values shown as the "1h" row under the SMA Levels section on the scanner card.
            if (riskSettings.isEnableWeeklySmaTargetShift() && htfSmaService != null) {
                double htfS20  = htfSmaService.getSma(symbol);
                double htfS50  = htfSmaService.getSma50(symbol);
                double htfS200 = htfSmaService.getSma200(symbol);
                if (htfS20  > 0) shiftCandidates.putIfAbsent(htfS20,  "weekly SMA 20");
                if (htfS50  > 0) shiftCandidates.putIfAbsent(htfS50,  "weekly SMA 50");
                if (htfS200 > 0) shiftCandidates.putIfAbsent(htfS200, "weekly SMA 200");
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

        // ── 4g1. Defensive wrong-side target check ──
        // After target computation + shift, target must sit on the correct side of entry
        // (above for BUY, below for SELL). Guards against degenerate target projection
        // (e.g. projected R5 below current close when price has run far past R4).
        if ((isBuy && target <= close) || (!isBuy && target >= close)) {
            return ProcessedSignal.rejected(setup, symbol,
                "Target " + fmt(target) + " on wrong side of entry " + fmt(close)
                + " (" + (isBuy ? "BUY target must be above" : "SELL target must be below") + ")");
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
        // Extended-level (R3/S3/R4/S4) qty reduction removed — these setups are either
        // skipped outright on IV/OV days (via skipR3S3NormalDays / skipR4S4NormalDays)
        // or allowed at full size on EV days. No per-level factor.
        int qty = baseQty;
        String qtyLog = null;
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

        // ── 4i3. Mean-reversion qty reduction — half qty for all counter-trend trades ──
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
            double base = riskSettings.getStructuralSlBufferAtr();
            double extra = isSingleLevelSetup(setup) ? riskSettings.getSingleLevelSlBufferAtr() : 0;
            desc.append(" (structural anchor ").append(fmt(anchor))
                .append(" ").append(isBuy ? "−" : "+").append(" ").append(base + extra)
                .append(" × ATR ").append(fmt(atr));
            if (extra > 0) desc.append(" [").append(base).append(" base + ").append(extra).append(" single-level]");
            desc.append(").");
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

    /** Single-level breakouts that lack a zone-width cushion — get the extra SL buffer. */
    private static boolean isSingleLevelSetup(String setup) {
        return switch (setup) {
            case "BUY_ABOVE_R2", "BUY_ABOVE_R3", "BUY_ABOVE_R4",
                 "BUY_ABOVE_S2", "BUY_ABOVE_S3", "BUY_ABOVE_S4",
                 "SELL_BELOW_R2", "SELL_BELOW_R3", "SELL_BELOW_R4",
                 "SELL_BELOW_S2", "SELL_BELOW_S3", "SELL_BELOW_S4",
                 "BUY_ABOVE_DH", "SELL_BELOW_DL" -> true;
            default -> false;
        };
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
    public static double computeBreakoutLevel(String setup,
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
            if (r4 > 0 && r3 > 0 && r4 > r3) {
                double step = r4 - r3;
                double projected = r4 + step;
                while (projected <= from) projected += step;
                return projected;
            }
            return from;
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
            if (s4 > 0 && s3 > 0 && s3 > s4) {
                double step = s3 - s4;
                double projected = s4 - step;
                while (projected >= from) projected -= step;
                return projected;
            }
            return from;
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
