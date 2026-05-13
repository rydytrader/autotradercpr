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
    private final MarketHolidayService marketHolidayService;

    public SignalProcessor(RiskSettingsStore riskSettings, EventService eventService,
                           QuantityService quantityService, MarketDataService marketDataService,
                           CandleAggregator candleAggregator, WeeklyCprService weeklyCprService,
                           SmaService smaService,
                           MarketHolidayService marketHolidayService) {
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.quantityService = quantityService;
        this.marketDataService = marketDataService;
        this.candleAggregator = candleAggregator;
        this.smaService = smaService;
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
        // VWAP — present only for BUY_ABOVE_VWAP / SELL_BELOW_VWAP setups, captured at signal-fire
        // time by BreakoutScanner. 0 for all CPR-level setups (those switch cases default to 0).
        double vwap = dbl(alert, "vwap");
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

        // ── Extended-level skip gate (daily) ─────────────────────────────────
        // Daily R3/S3 and R4/S4 breakouts skipped per day type. The toggle splits into two:
        //   *IvOvDays — skip on IV (open inside CPR) or OV (open between CPR and R2/S2) days
        //   *EvDays   — skip on EV (open above R2 or below S2 — extreme-value gap) days
        boolean isR3S3Setup = "BUY_ABOVE_R3".equals(setup) || "SELL_BELOW_S3".equals(setup);
        boolean isR4S4Setup = "BUY_ABOVE_R4".equals(setup) || "SELL_BELOW_S4".equals(setup);

        // Classify today's day type from the open print (close of first 5-min candle).
        double openPrint = candleAggregator.getFirstCandleClose(symbol);
        String dayType = classifyDayType(openPrint, tc, bc, r2, s2);
        boolean isEvDay   = "EV".equals(dayType);
        // Pre-9:20 IST or missing CPR → UNKNOWN treated as IV/OV (conservative: applies the
        // IV/OV toggle so trades aren't silently let through before the open print confirms).
        boolean isIvOvDay = "IV".equals(dayType) || "OV".equals(dayType) || "UNKNOWN".equals(dayType);

        boolean shouldSkipR3S3 =
               (isEvDay   && riskSettings.isSkipR3S3EvDays())
            || (isIvOvDay && riskSettings.isSkipR3S3IvOvDays());
        boolean shouldSkipR4S4 =
               (isEvDay   && riskSettings.isSkipR4S4EvDays())
            || (isIvOvDay && riskSettings.isSkipR4S4IvOvDays());

        if (shouldSkipR3S3 && isR3S3Setup) {
            return ProcessedSignal.rejected(setup, symbol,
                "R3/S3 breakout skipped (" + dayType + " day)");
        }
        if (shouldSkipR4S4 && isR4S4Setup) {
            return ProcessedSignal.rejected(setup, symbol,
                "R4/S4 breakout skipped (" + dayType + " day)");
        }

        // ── HTF Extended-level skip gate (weekly) ───────────────────────────
        // Independent of the daily checks above. Skip when the breakout candle close is
        // beyond the weekly R3/R4 (buys) or weekly S3/S4 (sells) in the trade direction.
        // Even an R1 setup is "HTF-extended" if the entry sits past weekly R3 — same
        // exhaustion concern as a daily-R3 trade. Each toggle is independent so users
        // can enable e.g. daily R3/S3 only, or weekly R3/S3 only, or both.
        WeeklyCprService.WeeklyLevels weeklyLv = weeklyCprService.getWeeklyLevels(symbol);
        if (riskSettings.isSkipHtfR3S3NormalDays() && weeklyLv != null) {
            boolean aboveWeeklyR3 = isBuy  && weeklyLv.r3 > 0 && close > weeklyLv.r3;
            boolean belowWeeklyS3 = !isBuy && weeklyLv.s3 > 0 && close < weeklyLv.s3;
            if (aboveWeeklyR3 || belowWeeklyS3) {
                String levelName = aboveWeeklyR3 ? "weekly R3=" + fmt(weeklyLv.r3)
                                                 : "weekly S3=" + fmt(weeklyLv.s3);
                return ProcessedSignal.rejected(setup, symbol,
                    setup + " entry close=" + fmt(close) + " beyond " + levelName
                    + " (HTF extended-level skip enabled)");
            }
        }
        if (riskSettings.isSkipHtfR4S4NormalDays() && weeklyLv != null) {
            boolean aboveWeeklyR4 = isBuy  && weeklyLv.r4 > 0 && close > weeklyLv.r4;
            boolean belowWeeklyS4 = !isBuy && weeklyLv.s4 > 0 && close < weeklyLv.s4;
            if (aboveWeeklyR4 || belowWeeklyS4) {
                String levelName = aboveWeeklyR4 ? "weekly R4=" + fmt(weeklyLv.r4)
                                                 : "weekly S4=" + fmt(weeklyLv.s4);
                return ProcessedSignal.rejected(setup, symbol,
                    setup + " entry close=" + fmt(close) + " beyond " + levelName
                    + " (HTF extended-level skip enabled)");
            }
        }

        // ── 4c. Compute breakout level ──────────────────────────────────────────
        double breakoutLevel = computeBreakoutLevel(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, vwap);

        // ── 4c2. Compute stop loss (needed for risk-based qty) ─────────────────
        // Always compute the close-based (default) SL. If structural SL is enabled, anchor
        // on the broken CPR/R/S level (every remaining setup is a static-level setup).
        double atrMultiplier = riskSettings.getAtrMultiplier();
        double defaultSl = isBuy ? (close - atr * atrMultiplier) : (close + atr * atrMultiplier);
        double sl = defaultSl;
        boolean useStructuralSl = false;
        if (riskSettings.isEnableStructuralSl()) {
            double anchor = computeStructuralAnchor(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, vwap);
            if (anchor > 0) {
                double buffer = riskSettings.getStructuralSlBufferAtr();
                // Single-level setups (R2/R3/R4, S2/S3/S4) lack the zone-width cushion that
                // zone setups (CPR, R1+PDH, S1+PDL, magnets) get for free, so add an extra
                // ATR buffer to push the SL further from the anchor.
                double extra = isSingleLevelSetup(setup) ? riskSettings.getSingleLevelSlBufferAtr() : 0;
                double totalBufferAtr = buffer + extra;
                double structSl = isBuy ? (anchor - atr * totalBufferAtr) : (anchor + atr * totalBufferAtr);
                sl = structSl;
                useStructuralSl = true;
                double defaultDist = Math.abs(close - defaultSl);
                double structDist = Math.abs(close - structSl);
                String bufferLabel = extra > 0
                    ? (totalBufferAtr + "×ATR = " + buffer + " base + " + extra + " single-level")
                    : (buffer + "×ATR");
                eventService.log("[INFO] " + symbol + " " + setup + " using structural SL " + fmt(structSl)
                    + " (dist " + fmt(structDist) + ", level " + fmt(anchor) + " ± " + bufferLabel + ")"
                    + " — default would be " + fmt(defaultSl) + " (dist " + fmt(defaultDist) + ")");
            }
        }

        // ── 4c3. Compute base quantity (uses SL for risk-based sizing) ─────────
        int baseQty = quantityService.computeBaseQty(symbol, close, sl, setup);

        // ── 4d. Small candle filter ─────────────────────────────────────────────
        // Two independent ATR floors:
        //   bodyFloor — minimum candle body required to claim "meaningful conviction"
        //   moveFloor — minimum push past the breakout level required to claim "level
        //               actually cleared" (defaults much smaller than bodyFloor — a
        //               strong-body candle that closes only just past the level is fine)
        // OR-logic: pass if EITHER body ≥ bodyFloor OR moveFromLevel ≥ moveFloor.
        // Wick defense and opposite-wick checks are body-based, so both use bodyFloor.
        if (riskSettings.isEnableSmallCandleFilter()) {
            double bodyAtrMult    = riskSettings.getSmallCandleBodyAtrThreshold();
            double moveAtrMult    = riskSettings.getSmallCandleMoveAtrThreshold();
            double wickRatio      = riskSettings.getWickRejectionRatio();
            double moveFromLevel  = isBuy ? (close - breakoutLevel) : (breakoutLevel - close);
            double candleBody     = candleOpen > 0 ? Math.abs(close - candleOpen) : 0;
            double bodyFloor      = atr * bodyAtrMult;
            double moveFloor      = atr * moveAtrMult;

            boolean bodyOk = candleBody    >= bodyFloor;
            boolean moveOk = moveFromLevel >= moveFloor;
            boolean meaningfulSize = bodyOk || moveOk;

            // Wick defense: tiny-body doji with a long wick into the breakout direction.
            // Buyers/sellers pushed the price to the wick extreme but the close
            // recovered — committed directional intent even if the body is small.
            boolean wickDefense = false;
            if (candleOpen > 0 && candleHigh > 0 && candleLow > 0 && candleBody < bodyFloor) {
                double breakoutWick = isBuy
                    ? (Math.min(close, candleOpen) - candleLow)
                    : (candleHigh - Math.max(close, candleOpen));
                wickDefense = breakoutWick >= wickRatio * candleBody
                    && breakoutWick >= bodyFloor;
            }

            if (!meaningfulSize && !wickDefense) {
                return ProcessedSignal.rejected(setup, symbol,
                    "Small candle — body (" + fmt(candleBody) + ") < " + bodyAtrMult + " ATR ("
                    + fmt(bodyFloor) + ") AND move from level (" + fmt(moveFromLevel) + ") < "
                    + moveAtrMult + " ATR (" + fmt(moveFloor) + "), no wick defense");
            }
            // Opposite-wick rejection is handled upstream by each retest pattern matcher
            // (hammer / engulfing / piercing / tweezer / doji / star / harami) in
            // BreakoutScanner. No longer enforced here so the trade log doesn't show
            // "fired then rejected".
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

            // Mean-reversion setups (S2/S3/S4 buys, R2/R3/R4 sells) fire on any day type.
            // Master gate is the BreakoutScanner's enableMeanReversionTrades toggle, applied
            // before this point. Earlier "EV-only" restriction was removed.

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
                        // (EV gap-fade rejection removed — already caught by the LTF-priority gate
                        //  in WeeklyCprService.getProbabilityForDirection upstream.)
                    }
                }

                // Inside-OR handling on EV days only — IV/OV inside-OR check removed
                // (redundant under LTF-priority model; LTF gate already enforces direction match).
                if (orHigh > 0 && orLow > 0 && close >= orLow && close <= orHigh && isEv) {
                    boolean opposesGap = (gapUp && !isBuy) || (gapDown && isBuy);
                    if (opposesGap) {
                        return ProcessedSignal.rejected(setup, symbol,
                            "EV " + (gapUp ? "up" : "down") + " — inside OR range [" + fmt(orLow) + "-" + fmt(orHigh)
                            + "] and trade direction opposes gap (no OR break yet) — skipped");
                    }
                    // EV gap-aligned inside OR → trade allowed
                }
            } else if (isEv) {
                // EV detected but OR still forming — skip trade
                return ProcessedSignal.rejected(setup, symbol,
                    "EV " + (gapUp ? "up" : "down") + " detected (1st candle close " + fmt(firstClose)
                    + (gapUp ? " >= R2 " + fmt(r2) : " <= S2 " + fmt(s2))
                    + ") but Opening Range still forming — skipped");
            }
        }

        // ── 4e2b. HTF Hurdle: nearest-weekly-level HPT→LPT downgrade ──
        // When a 5-min breakout closes above (for buys) the nearest weekly hurdle, the previous
        // 1h HTF candle must have closed above that level too — otherwise the HTF hasn't
        // confirmed the move and we downgrade HPT → LPT. Mirror for sells.
        // Hurdle candidates: R1, PWH, weekly TC, weekly Pivot, weekly BC for buys; S1, PWL,
        // weekly TC, weekly Pivot, weekly BC for sells. Including weekly CPR levels (TC/Pivot/BC)
        // catches breakouts that fired just above (or below) weekly CPR while the prior 1h
        // hadn't yet committed past that boundary — e.g. a buy at R1 in a stock that's BULLISH
        // by the state machine but currently still inside weekly CPR. R2+/S2+ are excluded —
        // far-out projections, not walls the HTF needs to clear first.
        if (riskSettings.isEnableHtfHurdleFilter()
                && ("HPT".equals(probability) || "MPT".equals(probability))) {
            WeeklyCprService.WeeklyLevels wl = weeklyLv; // reuse cached from line 141
            if (wl != null) {
                double[] candidates;
                String[] names;
                if (isBuy) {
                    candidates = new double[]{ wl.r1, wl.ph, wl.tc, wl.pivot, wl.bc };
                    names      = new String[]{ "R1", "PWH", "weekly TC", "weekly Pivot", "weekly BC" };
                } else {
                    candidates = new double[]{ wl.s1, wl.pl, wl.tc, wl.pivot, wl.bc };
                    names      = new String[]{ "S1", "PWL", "weekly TC", "weekly Pivot", "weekly BC" };
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
                    // Per-stock HTF Hurdle now uses 15-min boundaries (mirrors the NIFTY HTF
                    // Hurdle). Pre-9:30 IST no 15-min close exists yet → silent fail-open.
                    // Trades only fire from 9:30 onwards anyway, so by the time this gate runs
                    // the 9:25-9:30 5-min bar HAS finalized and IS the day's first 15-min close.
                    Double priorHtfClose = candleAggregator != null
                        ? candleAggregator.getLast15MinClose(symbol) : null;
                    if (priorHtfClose == null || priorHtfClose <= 0) {
                        // Silent fail-open — no rejection log pre-9:30.
                    } else if (isBuy ? priorHtfClose <= chosenLevel : priorHtfClose >= chosenLevel) {
                        return ProcessedSignal.rejected(setup, symbol,
                            "HTF hurdle at weekly " + chosenName
                            + ": close=" + fmt(close)
                            + ", prior 15-min close=" + fmt(priorHtfClose)
                            + ", level=" + fmt(chosenLevel));
                    }
                    // else: prior 15-min close has cleared the level → trade allowed
                }
                // else: no weekly level in the relevant direction → filter no-op (e.g. close below all buy candidates)
            }
        }

        // (Removed: a legacy safety-net block here used to silently reject any signal whose
        // probability was "LPT", on the assumption that SignalProcessor never produced LPT
        // anymore. That assumption is wrong — BreakoutScanner's checkIndexAlignment still
        // downgrades NIFTY-misaligned trades to LPT, and those legitimate LPT signals were
        // being silently dropped here with no event-log breadcrumb. The upstream downgrade
        // path already checks enableLpt before assigning LPT, so no re-gate is needed.)

        // ── 4f. Compute target ──────────────────────────────────────────────────
        double[] targets;
        if (isReversal) {
            targets = computeReversalTargets(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
        } else {
            targets = computeTargets(setup, close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, vwap, atr);
        }
        double defaultTarget = targets[0];
        double target = defaultTarget;

        // ── 4g1. Defensive wrong-side target check ──
        // Target must sit on the correct side of entry (above for BUY, below for SELL).
        // Guards against degenerate target projection (e.g. projected R5 below current close
        // when price has run far past R4).
        if ((isBuy && target <= close) || (!isBuy && target >= close)) {
            return ProcessedSignal.rejected(setup, symbol,
                "Target " + fmt(target) + " on wrong side of entry " + fmt(close)
                + " (" + (isBuy ? "BUY target must be above" : "SELL target must be below") + ")");
        }

        // ── 4g2. Walk-and-shift target picker ──────────────────────────────────
        // Single target only — no splits. Algorithm per user spec:
        //   1. Walk daily R/S levels (closest to entry first, beyond entry in trade direction)
        //   2. Find first daily level whose R/R clears minRR
        //   3. Check if any weekly CPR level lies strictly between entry and that daily level
        //   4. If a weekly intervenes, shift target to the weekly closest to entry
        //   5. Re-check R/R after the shift — reject if shifted R/R falls below minRR
        //   6. If no daily level satisfies minRR → reject
        boolean rescueShifted = false;
        if (riskSettings.isEnableRiskRewardFilter()) {
            double minRR = riskSettings.getMinRiskRewardRatio();
            double risk  = Math.abs(close - sl);
            TargetPick pick = pickTarget(isBuy, close, risk, minRR,
                r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, weeklyLv);
            if (pick == null) {
                return ProcessedSignal.rejected(setup, symbol,
                    "No daily CPR level satisfies R/R " + minRR + ":1 (walk-and-shift) — "
                    + "entry=" + fmt(close) + " SL=" + fmt(sl) + " risk=" + fmt(risk));
            }
            double finalRR = Math.abs(pick.target - close) / risk;
            if (pick.shifted) {
                eventService.log("[INFO] " + symbol + " " + setup
                    + " target: daily " + pick.dailyName + " = " + fmt(pick.dailyValue)
                    + " shifted to weekly " + pick.weeklyName + " = " + fmt(pick.target)
                    + " (between entry and daily) | R/R " + fmt(finalRR) + ":1");
                rescueShifted = true;
            } else {
                eventService.log("[INFO] " + symbol + " " + setup
                    + " target: daily " + pick.dailyName + " = " + fmt(pick.target)
                    + " | R/R " + fmt(finalRR) + ":1");
            }
            target = pick.target;
        }

        // ── 4i. Quantity ────────────────────────────────────────────────────────
        // Extended-level (R3/S3/R4/S4) qty reduction removed — these setups are either
        // skipped outright on IV/OV days (via skipR3S3NormalDays / skipR4S4NormalDays)
        // or allowed at full size on EV days. No per-level factor.
        int qty = baseQty;

        // ── 4i2. Probability-based qty adjustment ─────────────────────────────
        // MPT (medium): qty × mptQtyFactor (default 0.75). LPT: legacy path, no longer
        // assigned by new classification but kept for backward compat with any old payload.
        if ("MPT".equals(probability)) {
            double factor = riskSettings.getMptQtyFactor();
            int reduced = Math.max(1, (int)(qty * factor));
            eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (MPT ×" + factor + "): " + qty + " -> " + reduced);
            adjustments.add("Qty " + qty + " → " + reduced + " (×" + factor + " — MPT probability)");
            qty = reduced;
        } else if ("LPT".equals(probability)) {
            double factor = riskSettings.getLptQtyFactor();
            int reduced = Math.max(1, (int)(qty * factor));
            eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (LPT ×" + factor + "): " + qty + " -> " + reduced);
            adjustments.add("Qty " + qty + " → " + reduced + " (×" + factor + " — LPT probability)");
            qty = reduced;
        }

        // ── 4i3. Counter-trend qty reductions removed ──
        // Both EV-reversal halving and OV-magnet halving were removed under the LTF-priority
        // model. Counter-trend setups (8 magnets + deep mean-rev) are now sized purely by the
        // MPT factor (mptQtyFactor, default 0.75) applied above. The setup-quality / day-type
        // reductions added too much compounding when the user already has explicit toggles for
        // the same conditions (enableMeanReversionTrades, OR rules, etc.).

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
            double anchor = computeStructuralAnchor(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, vwap);
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

        // [TARGET] line — always single target (no splits).
        desc.append("\n").append(ts).append(" [TARGET] ").append(fmt(target));
        if (rescueShifted) {
            desc.append(" (walk-and-shift: shifted to weekly between entry and chosen daily)");
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
        if (qty != baseQty) {
            desc.append(" (reduced from ").append(baseQty).append(")");
        }

        // [ADJUST] lines — every probability/qty change applied during signal processing
        for (String note : adjustments) {
            desc.append("\n").append(ts).append(" [ADJUST] ").append(note);
        }

        String description = desc.toString();

        // ── 4k. Log the decision ────────────────────────────────────────────────
        String tgtSummary = "Tgt: " + fmt(target) + "(" + fmt(Math.abs(target - close)) + ")";
        String routeSuffix = (scannerNote != null && !scannerNote.isEmpty()) ? " [" + scannerNote + "]" : "";
        eventService.log("[SUCCESS] " + signal + " signal received for " + symbol + " | " + setup + routeSuffix + " | Entry: " + fmt(close)
            + " | " + tgtSummary
            + " | SL: " + fmt(sl) + "(" + fmt(Math.abs(close - sl)) + ")"
            + " | Qty: " + qty);

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
            .rescueShifted(rescueShifted)
            .useStructuralSl(useStructuralSl)
            .build();
    }

    /** Result of pickTarget: the chosen target + names for logging. */
    private static class TargetPick {
        final double target;
        final double dailyValue;
        final String dailyName;
        final String weeklyName;   // null when no shift happened
        final boolean shifted;
        TargetPick(double target, double dailyValue, String dailyName, String weeklyName, boolean shifted) {
            this.target = target;
            this.dailyValue = dailyValue;
            this.dailyName = dailyName;
            this.weeklyName = weeklyName;
            this.shifted = shifted;
        }
    }

    /**
     * Walk-and-shift target picker — single target, no splits.
     * <ol>
     *   <li>Walk daily R-side (buy) or S-side (sell) levels closest-to-entry first.</li>
     *   <li>Find the first daily level whose R/R clears {@code minRR}.</li>
     *   <li>Check if any weekly CPR level sits strictly between entry and that daily level.</li>
     *   <li>If a weekly intervenes, shift target to the weekly closest to entry.</li>
     *   <li>Re-check R/R after shift. If the shift drops R/R below minRR, return null (reject).</li>
     *   <li>If no daily level satisfies minRR, return null (reject).</li>
     * </ol>
     * Daily walk is the FULL daily CPR ladder, low to high:
     *     S4 - S3 - S2 - S1+PDL - Daily CPR - R1+PDH - R2 - R3 - R4
     * Mid-points are inserted between every adjacent pair. The chain candidates above entry
     * are kept for buys (sorted ascending); the ones below entry for sells (descending).
     *
     * This handles every setup — BUY_ABOVE_S4/S3/S2 walk up through their nearby S-levels
     * first, BUY_ABOVE_S1_PDL starts at Daily CPR, BUY_ABOVE_R1_PDH starts at R2, and so on.
     * Mirror for sells.
     *
     * Zone levels (S1+PDL, Daily CPR, R1+PDH) use the close-to-entry edge as their walk
     * value: lower edge (= min) for buys going up, upper edge (= max) for sells going down.
     * Mid-points are the geometric midpoint of the gap between two adjacent items — using
     * the UPPER edge of the lower item and the LOWER edge of the upper item. Single-line
     * items have upper = lower = the line itself.
     *
     * Weekly candidates for the intercept check: Pivot, TC, BC, R1-R4, S1-S4, PWH, PWL.
     */
    private TargetPick pickTarget(boolean isBuy, double entry, double risk, double minRR,
                                   double r1, double r2, double r3, double r4,
                                   double s1, double s2, double s3, double s4,
                                   double ph, double pl, double tc, double bc,
                                   WeeklyCprService.WeeklyLevels wl) {
        if (risk <= 0) return null;
        // Zone edges (low / high). For single-line levels, low == high == level value.
        double s1pl_lo = Math.min(s1, pl), s1pl_hi = Math.max(s1, pl);
        double cpr_lo  = Math.min(tc, bc), cpr_hi  = Math.max(tc, bc);
        double r1ph_lo = Math.min(r1, ph), r1ph_hi = Math.max(r1, ph);

        // Build full chain low-to-high. Each entry: name, value-for-walk.
        // Zone close-edge = lower for buys (going up), upper for sells (going down).
        double s1plVal = isBuy ? s1pl_lo : s1pl_hi;
        double cprVal  = isBuy ? cpr_lo  : cpr_hi;
        double r1phVal = isBuy ? r1ph_lo : r1ph_hi;

        // Mid-points: (upper-of-low-item + lower-of-high-item) / 2. Direction-independent.
        double midS4S3   = (s4 > 0 && s3 > 0) ? (s4 + s3) / 2 : 0;
        double midS3S2   = (s3 > 0 && s2 > 0) ? (s3 + s2) / 2 : 0;
        double midS2S1   = (s2 > 0 && s1pl_lo > 0) ? (s2 + s1pl_lo) / 2 : 0;
        double midS1CPR  = (s1pl_hi > 0 && cpr_lo > 0) ? (s1pl_hi + cpr_lo) / 2 : 0;
        double midCPRR1  = (cpr_hi > 0 && r1ph_lo > 0) ? (cpr_hi + r1ph_lo) / 2 : 0;
        double midR1R2   = (r1ph_hi > 0 && r2 > 0) ? (r1ph_hi + r2) / 2 : 0;
        double midR2R3   = (r2 > 0 && r3 > 0) ? (r2 + r3) / 2 : 0;
        double midR3R4   = (r3 > 0 && r4 > 0) ? (r3 + r4) / 2 : 0;

        String[] names = {
            "S4", "MID S4-S3",
            "S3", "MID S3-S2",
            "S2", "MID S2-S1+PDL",
            "S1+PDL", "MID S1+PDL-CPR",
            "Daily CPR", "MID CPR-R1+PDH",
            "R1+PDH", "MID R1+PDH-R2",
            "R2", "MID R2-R3",
            "R3", "MID R3-R4",
            "R4"
        };
        double[] vals = {
            s4, midS4S3,
            s3, midS3S2,
            s2, midS2S1,
            s1plVal, midS1CPR,
            cprVal, midCPRR1,
            r1phVal, midR1R2,
            r2, midR2R3,
            r3, midR3R4,
            r4
        };

        // Filter by trade direction (above entry for buys, below for sells) and sort by proximity.
        java.util.List<double[]> walk = new java.util.ArrayList<>();
        for (int i = 0; i < vals.length; i++) {
            if (vals[i] <= 0) continue;
            if (isBuy  && vals[i] > entry) walk.add(new double[]{ vals[i], i });
            if (!isBuy && vals[i] < entry) walk.add(new double[]{ vals[i], i });
        }
        if (isBuy) walk.sort((a, b) -> Double.compare(a[0], b[0])); // closest above entry first
        else       walk.sort((a, b) -> Double.compare(b[0], a[0])); // closest below entry first

        // Step 2: find first daily level satisfying minRR
        boolean checkWeekly = riskSettings.isEnableWeeklyLevelTargetShift();
        for (double[] d : walk) {
            double dailyValue = d[0];
            String dailyName  = names[(int) d[1]];
            double dailyRR = Math.abs(dailyValue - entry) / risk;
            if (dailyRR < minRR) continue;

            // Step 3: optional weekly intercept (toggled via enableWeeklyLevelTargetShift).
            // When OFF, the chosen daily becomes the final target as-is.
            if (!checkWeekly) {
                return new TargetPick(dailyValue, dailyValue, dailyName, null, false);
            }

            // Closest weekly strictly between entry and this daily level
            String[] weeklyName = { null };
            Double weeklyValue = findClosestWeeklyBetween(isBuy, entry, dailyValue, wl, weeklyName);
            if (weeklyValue == null) {
                return new TargetPick(dailyValue, dailyValue, dailyName, null, false);
            }

            // Step 4: re-check R/R with weekly-shifted target. Reject if shift drops below minRR.
            double weeklyRR = Math.abs(weeklyValue - entry) / risk;
            if (weeklyRR >= minRR) {
                return new TargetPick(weeklyValue, dailyValue, dailyName, weeklyName[0], true);
            }
            return null; // weekly shift failed R/R — reject per spec
        }
        return null; // no daily level satisfies minRR
    }

    /** Closest weekly CPR level strictly between entry and dailyTarget (in trade direction). */
    private Double findClosestWeeklyBetween(boolean isBuy, double entry, double dailyTarget,
                                             WeeklyCprService.WeeklyLevels wl, String[] outName) {
        if (wl == null) return null;
        double[] vals = { wl.pivot, wl.tc, wl.bc, wl.r1, wl.r2, wl.r3, wl.r4,
                          wl.s1, wl.s2, wl.s3, wl.s4, wl.ph, wl.pl };
        String[] names = { "WK Pivot", "WK TC", "WK BC", "WK R1", "WK R2", "WK R3", "WK R4",
                           "WK S1", "WK S2", "WK S3", "WK S4", "WK PWH", "WK PWL" };
        Double best = null;
        double bestDist = Double.MAX_VALUE;
        for (int i = 0; i < vals.length; i++) {
            double v = vals[i];
            if (v <= 0) continue;
            boolean between = isBuy ? (v > entry && v < dailyTarget)
                                    : (v < entry && v > dailyTarget);
            if (!between) continue;
            double dist = Math.abs(v - entry);
            if (dist < bestDist) {
                bestDist = dist;
                best = v;
                outName[0] = names[i];
            }
        }
        return best;
    }

    /** Single-level breakouts that lack a zone-width cushion — get the extra SL buffer. */
    private static boolean isSingleLevelSetup(String setup) {
        return switch (setup) {
            case "BUY_ABOVE_R2", "BUY_ABOVE_R3", "BUY_ABOVE_R4",
                 "BUY_ABOVE_S2", "BUY_ABOVE_S3", "BUY_ABOVE_S4",
                 "SELL_BELOW_R2", "SELL_BELOW_R3", "SELL_BELOW_R4",
                 "SELL_BELOW_S2", "SELL_BELOW_S3", "SELL_BELOW_S4",
                 "BUY_ABOVE_VWAP", "SELL_BELOW_VWAP" -> true;
            default -> false;
        };
    }

    private static double computeStructuralAnchor(String setup,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc, double vwap) {
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
            // VWAP single-line setups — SL anchors to VWAP itself with single-level extra buffer
            // applied via isSingleLevelSetup so the buffer-addition machinery kicks in.
            case "BUY_ABOVE_VWAP", "SELL_BELOW_VWAP" -> vwap;
            default -> 0;
        };
    }

    // ── Breakout level per setup ────────────────────────────────────────────────
    public static double computeBreakoutLevel(String setup,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc, double vwap) {
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
            case "SELL_BELOW_CPR"    -> Math.min(tc, bc);
            case "SELL_BELOW_S1_PDL" -> Math.min(s1, pl);
            case "SELL_BELOW_S2"     -> s2;
            case "SELL_BELOW_S3"     -> s3;
            case "SELL_BELOW_S4"     -> s4;
            case "SELL_BELOW_R1_PDH" -> Math.min(r1, ph);
            case "SELL_BELOW_R2"     -> r2;
            case "SELL_BELOW_R3"     -> r3;
            case "SELL_BELOW_R4"     -> r4;
            case "BUY_ABOVE_VWAP", "SELL_BELOW_VWAP" -> vwap;
            default -> 0;
        };
    }

    // ── Default + shift target per setup ────────────────────────────────────────
    private double[] computeTargets(String setup, double close,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc, double vwap, double atr) {
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
            // Mean-reversion fades from above R-levels (gap-up reversal pattern, also valid intraday)
            case "SELL_BELOW_R4"      -> new double[]{ r3, r3 };
            case "SELL_BELOW_R3"      -> new double[]{ r2, r2 };
            case "SELL_BELOW_R2"      -> new double[]{ Math.max(r1, ph), Math.max(r1, ph) };
            // Mean-reversion bounces from below S-levels (gap-down reversal pattern, also valid intraday)
            case "BUY_ABOVE_S4"       -> new double[]{ s3, s3 };
            case "BUY_ABOVE_S3"       -> new double[]{ s2, s2 };
            case "BUY_ABOVE_S2"       -> new double[]{ Math.min(s1, pl), Math.min(s1, pl) };
            // VWAP setups — nearest CPR level beyond entry in trade direction, with ATR fallback.
            case "BUY_ABOVE_VWAP"     -> vwapTargets(true,  close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, atr);
            case "SELL_BELOW_VWAP"    -> vwapTargets(false, close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, atr);
            default -> new double[]{ 0, 0 };
        };
    }

    /**
     * Targets for BUY_ABOVE_VWAP / SELL_BELOW_VWAP — VWAP is a single line, no "next VWAP"
     * above/below, so we borrow from the CPR chain. Primary target = nearest CPR level
     * strictly beyond entry in trade direction. Secondary target = next CPR after primary.
     * If no CPR level lies in direction, fall back to entry ± (2 × atrMultiplier × ATR)
     * so the target-shift / min-R-R machinery still has a measurable distance to evaluate.
     */
    private double[] vwapTargets(boolean isBuy, double entry,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc, double atr) {
        // Build the full CPR-level set for the trade direction beyond entry.
        // For BUY: levels strictly above entry, sorted ascending → nearest first.
        // For SELL: levels strictly below entry, sorted descending → nearest first.
        java.util.List<Double> cands = new java.util.ArrayList<>();
        double[] allLevels = { r4, r3, r2, Math.max(r1, ph), Math.min(r1, ph),
                               Math.max(tc, bc), Math.min(tc, bc),
                               Math.max(s1, pl), Math.min(s1, pl), s2, s3, s4 };
        for (double lv : allLevels) {
            if (lv <= 0) continue;
            if (isBuy && lv > entry)   cands.add(lv);
            if (!isBuy && lv < entry)  cands.add(lv);
        }
        if (isBuy) cands.sort(java.util.Comparator.naturalOrder());
        else       cands.sort(java.util.Comparator.reverseOrder());

        if (cands.isEmpty()) {
            // No CPR level in direction — ATR fallback. 2 × atrMultiplier matches the rough
            // distance of an R2/R3 step, giving the min-R-R check a sensible number.
            double atrMult = riskSettings.getAtrMultiplier();
            double dist = atr * atrMult * 2;
            double t = isBuy ? entry + dist : entry - dist;
            return new double[]{ t, t };
        }
        double t1 = cands.get(0);
        double t2 = cands.size() > 1 ? cands.get(1) : t1;
        return new double[]{ t1, t2 };
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
            case "SELL_BELOW_R2"     -> "R2";
            case "SELL_BELOW_R3"     -> "R3";
            case "SELL_BELOW_R4"     -> "R4";
            case "BUY_ABOVE_S2"      -> "S2";
            case "BUY_ABOVE_S3"      -> "S3";
            case "BUY_ABOVE_S4"      -> "S4";
            case "BUY_ABOVE_VWAP", "SELL_BELOW_VWAP" -> "VWAP";
            default -> setup;
        };
    }

    /**
     * Classify today's day type from the open print (first 5-min candle close) vs daily CPR.
     * Used by the daily extended-level skip toggles to decide which day-type-specific toggle
     * applies.
     * <ul>
     *   <li>{@code IV}  — open print inside CPR (between BC and TC)</li>
     *   <li>{@code OV}  — open print between CPR and R2/S2</li>
     *   <li>{@code EV}  — open print outside R2/S2 (extreme-value gap)</li>
     *   <li>{@code UNKNOWN} — open print not yet established (pre-9:20 IST) or CPR missing</li>
     * </ul>
     */
    static String classifyDayType(double openPrint, double tc, double bc, double r2, double s2) {
        if (openPrint <= 0) return "UNKNOWN";
        if (tc <= 0 || bc <= 0) return "UNKNOWN";
        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);
        if (openPrint > cprBot && openPrint < cprTop) return "IV";
        // Inclusive of the boundary itself (open == TC or BC) → still IV.
        if (openPrint == cprTop || openPrint == cprBot) return "IV";
        if (r2 > 0 && openPrint > r2) return "EV";
        if (s2 > 0 && openPrint < s2) return "EV";
        return "OV";
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
