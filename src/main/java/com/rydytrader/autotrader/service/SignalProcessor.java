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
    private final MarketHolidayService marketHolidayService;
    private final BhavcopyService  bhavcopyService;

    public SignalProcessor(RiskSettingsStore riskSettings, EventService eventService,
                           QuantityService quantityService, MarketDataService marketDataService,
                           CandleAggregator candleAggregator, WeeklyCprService weeklyCprService,
                           EmaService emaService,
                           MarketHolidayService marketHolidayService,
                           BhavcopyService bhavcopyService) {
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.quantityService = quantityService;
        this.marketDataService = marketDataService;
        this.candleAggregator = candleAggregator;
        this.emaService = emaService;
        this.weeklyCprService = weeklyCprService;
        this.marketHolidayService = marketHolidayService;
        this.bhavcopyService = bhavcopyService;
    }

    public ProcessedSignal process(Map<String, Object> alert) {

        // ── 4a. Parse alert fields ──────────────────────────────────────────────
        String setup       = str(alert, "setup");
        String rawSymbol   = str(alert, "symbol");
        // TradingView uses _ in symbols (e.g. BAJAJ_AUTO), Fyers uses - (BAJAJ-AUTO)
        String symbol      = rawSymbol.replace("_", "-");

        // ── Non-trading-day gate ────────────────────────────────────────────────
        // Block weekends / NSE holidays. WebSocket ticks still flow on NSE mock-session
        // Saturdays and trades would otherwise execute against the user's real account.
        // Both TradingView webhook and internal-scanner signals funnel through this method,
        // so this single check covers every entry path. MarketDataService and CandleAggregator
        // remain day-agnostic by design (data feed layer); the gate belongs here.
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) {
            return ProcessedSignal.rejected(setup, symbol, "Non-trading day (weekend / NSE holiday)");
        }

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

        // ── Reject if ATR is invalid (NaN or zero — Pine Script may send NaN for insufficient bars)
        if (Double.isNaN(atr) || atr <= 0) {
            return ProcessedSignal.rejected(setup, symbol, "Invalid ATR (" + atr + ") — cannot compute stop loss");
        }

        // ── 4b. Derive direction ────────────────────────────────────────────────
        boolean isBuy = setup.startsWith("BUY_");
        String signal = isBuy ? "BUY" : "SELL";

        // ── Extended-level skip gate ─────────────────────────────────────────
        // Setups at each successive level pair have their own opt-out toggle. R1+PDH /
        // S1+PDL are zone setups (first structural breakout); R2/S2, R3/S3, R4/S4 sit
        // progressively further from the daily CPR. R1+PDH/S1+PDL and R2/S2 default OFF
        // (allowed); R3/S3 and R4/S4 default ON (skipped) since they're least reliable.
        boolean isR1PdhS1PdlSetup = "BUY_ABOVE_R1_PDH".equals(setup) || "SELL_BELOW_S1_PDL".equals(setup);
        boolean isR2S2Setup = "BUY_ABOVE_R2".equals(setup) || "SELL_BELOW_S2".equals(setup);
        boolean isR3S3Setup = "BUY_ABOVE_R3".equals(setup) || "SELL_BELOW_S3".equals(setup);
        boolean isR4S4Setup = "BUY_ABOVE_R4".equals(setup) || "SELL_BELOW_S4".equals(setup);

        if (isR1PdhS1PdlSetup && riskSettings.isSkipR1PdhS1Pdl()) {
            return ProcessedSignal.rejected(setup, symbol, "R1+PDH/S1+PDL breakout/retest skipped (toggle off)");
        }
        if (isR2S2Setup && riskSettings.isSkipR2S2()) {
            return ProcessedSignal.rejected(setup, symbol, "R2/S2 breakout/retest skipped (toggle off)");
        }
        if (isR3S3Setup && riskSettings.isSkipR3S3()) {
            return ProcessedSignal.rejected(setup, symbol, "R3/S3 breakout/retest skipped (toggle off)");
        }
        if (isR4S4Setup && riskSettings.isSkipR4S4()) {
            return ProcessedSignal.rejected(setup, symbol, "R4/S4 breakout/retest skipped (toggle off)");
        }

        // Weekly levels lookup — used by the HTF Hurdle filter and computeTargets below.
        WeeklyCprService.WeeklyLevels weeklyLv = weeklyCprService.getWeeklyLevels(symbol);

        // ── 4c. Compute breakout level ──────────────────────────────────────────
        double breakoutLevel = computeBreakoutLevel(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);

        // ── 4c2. Compute stop loss (needed for risk-based qty) ─────────────────
        // Always compute the close-based (default) SL. If structural SL is enabled, anchor
        // on the broken CPR/R/S level (every remaining setup is a static-level setup).
        double atrMultiplier = riskSettings.getAtrMultiplier();
        double defaultSl = isBuy ? (close - atr * atrMultiplier) : (close + atr * atrMultiplier);
        double sl = defaultSl;
        // Adaptive-CPR midpoint anchor: when the stock is classified AVERAGE (B band —
        // widthRatio between narrow and wide thresholds), the zone is wide enough that
        // anchoring on the entry-side outer edge places the SL too far from price. Switch
        // to the zone midpoint ((TC+BC)/2, (R1+PH)/2, (S1+PL)/2) so the SL is tighter and
        // more proportionate. NARROW and WIDE states keep the outer-edge anchor.
        boolean useMidpointAnchor = false;
        String adaptiveStateLabel = "UNKNOWN";
        if (bhavcopyService != null) {
            String stockTicker = symbol.replaceFirst("^NSE:", "").replaceAll("-(EQ|INDEX)$", "");
            BhavcopyService.AdaptiveCprResult adaptive = bhavcopyService.getAdaptiveCpr(stockTicker);
            if (adaptive != null) {
                adaptiveStateLabel = adaptive.state().name();
                useMidpointAnchor = adaptive.state() == BhavcopyService.CprState.AVERAGE;
            }
        }
        boolean useStructuralSl = false;
        // Mode label captured here so both the event log AND the trade description can show
        // exactly which SL-calculation path fired. Reset per signal.
        String slModeLabel = "Default ATR-based SL (structural SL disabled)";
        if (riskSettings.isEnableStructuralSl()) {
            double anchor = computeStructuralAnchor(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, useMidpointAnchor);
            if (anchor > 0) {
                double buffer = riskSettings.getStructuralSlBufferAtr();
                // Single-level setups (R2/R3/R4, S2/S3/S4) lack the zone-width cushion that
                // zone setups (CPR, R1+PDH, S1+PDL, magnets) get for free, so add an extra
                // ATR buffer to push the SL further from the anchor. Zone setups also receive
                // the same extra cushion when CPR width is below narrowCprZoneCollapseWidthPct
                // — their zone is too tight to absorb a normal pullback.
                boolean isSingleLvl   = isSingleLevelSetup(setup);
                boolean appliesExtra  = appliesSingleLevelSlBuffer(setup, tc, bc, ph, pl);
                double extra = appliesExtra ? riskSettings.getSingleLevelSlBufferAtr() : 0;
                double totalBufferAtr = buffer + extra;
                double structSl = isBuy ? (anchor - atr * totalBufferAtr) : (anchor + atr * totalBufferAtr);
                sl = structSl;
                useStructuralSl = true;

                // ── SL-mode label — human-readable narrative of which structural path fired ──
                if (isSingleLvl) {
                    slModeLabel = "Single-level setup (" + setup.replaceFirst("^(BUY_ABOVE_|SELL_BELOW_)", "") + ") + buffer";
                } else if (useMidpointAnchor && appliesExtra) {
                    // Rare: AVERAGE state AND zone width below collapse threshold.
                    slModeLabel = "Zone midpoint anchor (AVERAGE CPR) + narrow-zone buffer (CPR width < threshold)";
                } else if (useMidpointAnchor) {
                    slModeLabel = "Zone midpoint anchor (AVERAGE CPR)";
                } else if (appliesExtra) {
                    slModeLabel = "Zone edge anchor + narrow-zone buffer (CPR width < " + riskSettings.getNarrowCprZoneCollapseWidthPct() + "%)";
                } else {
                    slModeLabel = "Zone edge anchor (" + adaptiveStateLabel + " CPR)";
                }

                double defaultDist = Math.abs(close - defaultSl);
                double structDist = Math.abs(close - structSl);
                String bufferLabel = extra > 0
                    ? (totalBufferAtr + "×ATR = " + buffer + " base + " + extra + " single-level")
                    : (buffer + "×ATR");
                eventService.log("[INFO] " + symbol + " " + setup + " SL mode: " + slModeLabel
                    + " — SL " + fmt(structSl) + " (dist " + fmt(structDist)
                    + ", anchor " + fmt(anchor) + " ± " + bufferLabel + ")"
                    + " — default would be " + fmt(defaultSl) + " (dist " + fmt(defaultDist) + ")");
            }
        }

        // ── 4c3. Compute base quantity (uses SL for risk-based sizing) ─────────
        int baseQty = quantityService.computeBaseQty(symbol, close, sl, setup);

        // Global large-candle filter retired — each named pattern (Outside Reversal,
        // Doji reversal, Morning/Evening star, Three Inside) now enforces its own
        // per-pattern body cap inside CandlePatternDetector. Hammer relies on its
        // structural pin-bar signature instead.

        // HTF Hurdle filter moved to BreakoutScanner.checkStockHtfHurdle so all four
        // hurdle filters (Index HTF, Index 5m, Virgin CPR, Stock HTF) live in one place.

        // ── 4f. Compute target ──────────────────────────────────────────────────
        double[] targets = computeTargets(setup, close, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, atr);
        double defaultTarget = targets[0];
        double target = defaultTarget;

        // Open Target mode — trade fires with SL only. No target leg is placed on
        // Fyers; the position closes via SL hit or auto-squareoff. Both the wrong-side
        // check and the walk-and-shift R/R filter are silently bypassed because there
        // is no target to grade.
        boolean rescueShifted = false;
        if (riskSettings.isEnableOpenTargetMode()) {
            target = 0.0;
            eventService.log("[INFO] " + symbol + " " + setup
                + " target: OPEN — closes only on SL hit or auto-squareoff");
        } else {
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
        }

        // ── 4i. Quantity ────────────────────────────────────────────────────────
        // Extended-level (R3/S3/R4/S4) qty reduction removed — these setups are either
        // skipped outright on IV/OV days (via skipR3S3NormalDays / skipR4S4NormalDays)
        // or allowed at full size on EV days. No per-level factor.
        int qty = baseQty;

        // ── 4i2. Probability-based qty adjustment ─────────────────────────────
        // MPT (medium): qty × per-family factor. Setup type picks the factor:
        //   • Magnet (S1+PDL buys, R1+PDH sells)      → magnetTradesQtyFactor  (default 0.75)
        //   • Deep mean-rev (S2/S3/S4, R2/R3/R4)      → meanReversionQtyFactor (default 0.50)
        //   • Any other MPT trade (currently none)    → mptQtyFactor           (legacy fallback)
        // LPT classification has been removed; the bot hard-rejects NIFTY-misaligned trades.
        if ("MPT".equals(probability)) {
            double factor;
            String tag;
            if ("BUY_ABOVE_S1_PDL".equals(setup) || "SELL_BELOW_R1_PDH".equals(setup)) {
                factor = riskSettings.getMagnetTradesQtyFactor();
                tag = "MAGNET";
            } else if ("BUY_ABOVE_S2".equals(setup) || "BUY_ABOVE_S3".equals(setup) || "BUY_ABOVE_S4".equals(setup)
                    || "SELL_BELOW_R2".equals(setup) || "SELL_BELOW_R3".equals(setup) || "SELL_BELOW_R4".equals(setup)) {
                factor = riskSettings.getMeanReversionQtyFactor();
                tag = "MEAN_REVERSION";
            } else {
                factor = riskSettings.getMptQtyFactor();
                tag = "MPT";
            }
            int reduced = Math.max(1, (int)(qty * factor));
            eventService.log("[INFO] " + symbol + " " + setup + " qty reduced (" + tag + " ×" + factor + "): " + qty + " -> " + reduced);
            adjustments.add("Qty " + qty + " → " + reduced + " (×" + factor + " — " + tag + ")");
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

        // [ENTRY] line
        String levelName = levelNameForSetup(setup);
        desc.append("\n").append(ts).append(" [ENTRY] ").append(setup).append(" | ").append(probability)
            .append(" — Close ").append(fmt(close))
            .append(" broke ").append(levelName)
            .append(" (").append(fmt(breakoutLevel)).append(").");

        // [SL_MODE] line — surfaces which calculation path fired (structural variant or default
        // ATR). Lets the trader verify the SL was sized by the intended rule for the stock's
        // CPR state (NARROW/AVERAGE/WIDE), setup type (zone vs single-level), and any
        // narrow-zone buffer add-on.
        desc.append("\n").append(ts).append(" [SL_MODE] ").append(slModeLabel);

        // [SL] line
        desc.append("\n").append(ts).append(" [SL] ").append(fmt(sl));
        if (useStructuralSl) {
            double anchor = computeStructuralAnchor(setup, r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, useMidpointAnchor);
            double base = riskSettings.getStructuralSlBufferAtr();
            double extra = appliesSingleLevelSlBuffer(setup, tc, bc, ph, pl) ? riskSettings.getSingleLevelSlBufferAtr() : 0;
            desc.append(" (anchor ")
                .append(useMidpointAnchor ? "(midpoint) " : "")
                .append(fmt(anchor))
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
    static boolean isSingleLevelSetup(String setup) {
        return switch (setup) {
            case "BUY_ABOVE_R2", "BUY_ABOVE_R3", "BUY_ABOVE_R4",
                 "BUY_ABOVE_S2", "BUY_ABOVE_S3", "BUY_ABOVE_S4",
                 "SELL_BELOW_R2", "SELL_BELOW_R3", "SELL_BELOW_R4",
                 "SELL_BELOW_S2", "SELL_BELOW_S3", "SELL_BELOW_S4" -> true;
            default -> false;
        };
    }

    /** Two-line zone setups (CPR, R1+PDH, S1+PDL incl. magnets). When the underlying CPR
     *  is very narrow, these effectively behave like a single line for SL purposes — the
     *  zone-width cushion is too tight to absorb a normal pullback — and they get the
     *  extra singleLevelSlBufferAtr cushion. Breakout detection still uses the zone. */
    private static boolean isZoneSetup(String setup) {
        return switch (setup) {
            case "BUY_ABOVE_CPR", "BUY_ABOVE_R1_PDH", "BUY_ABOVE_S1_PDL",
                 "SELL_BELOW_CPR", "SELL_BELOW_R1_PDH", "SELL_BELOW_S1_PDL" -> true;
            default -> false;
        };
    }

    /** CPR width % using previous day's close as the denominator (matches
     *  CprLevels.cprWidthPct semantics). PDC is recovered from CPR fields: since
     *  pivot = (TC+BC)/2 = (PH+PL+PDC)/3, PDC = 1.5(TC+BC) − PH − PL. */
    private static double cprWidthPct(double tc, double bc, double ph, double pl) {
        if (tc <= 0 || bc <= 0) return 0;
        double prevClose = 1.5 * (tc + bc) - ph - pl;
        if (prevClose <= 0) return 0;
        return Math.abs(tc - bc) / prevClose * 100.0;
    }

    /** True when the setup needs the extra single-level SL buffer — either it's a pure
     *  single-level setup OR it's a zone setup with CPR width below the collapse threshold. */
    boolean appliesSingleLevelSlBuffer(String setup, double tc, double bc, double ph, double pl) {
        if (isSingleLevelSetup(setup)) return true;
        if (!isZoneSetup(setup)) return false;
        double threshold = riskSettings.getNarrowCprZoneCollapseWidthPct();
        if (threshold <= 0) return false;
        return cprWidthPct(tc, bc, ph, pl) < threshold;
    }

    /** When {@code useMidpoint} is true (stock is adaptive-classified AVERAGE), the 6
     *  zone setups anchor on the zone's midpoint instead of the entry-side outer edge.
     *  Single-level setups (R2/R3/R4, S2/S3/S4) are unaffected — no midpoint concept. */
    /** Reused by {@link LevelWalkTrailingService} so trailing SL anchors match
     *  exactly what an entry on that level would have computed. */
    static double computeStructuralAnchor(String setup,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc,
            boolean useMidpoint) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"     -> useMidpoint ? (tc + bc) / 2.0 : Math.min(tc, bc);
            case "SELL_BELOW_CPR"    -> useMidpoint ? (tc + bc) / 2.0 : Math.max(tc, bc);
            case "BUY_ABOVE_R1_PDH"  -> useMidpoint ? (r1 + ph) / 2.0 : Math.min(r1, ph);
            case "SELL_BELOW_S1_PDL" -> useMidpoint ? (s1 + pl) / 2.0 : Math.max(s1, pl);
            case "BUY_ABOVE_S1_PDL"  -> useMidpoint ? (s1 + pl) / 2.0 : Math.min(s1, pl); // magnet bounce
            case "SELL_BELOW_R1_PDH" -> useMidpoint ? (r1 + ph) / 2.0 : Math.max(r1, ph); // magnet rejection
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
    /** Used by {@link LevelWalkTrailingService} to detect when a bar close has
     *  cleared the next CPR level in the trade direction. */
    public static double computeBreakoutLevel(String setup,
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
            default -> 0;
        };
    }

    // ── Default + shift target per setup ────────────────────────────────────────
    private double[] computeTargets(String setup, double close,
            double r1, double r2, double r3, double r4,
            double s1, double s2, double s3, double s4,
            double ph, double pl, double tc, double bc, double atr) {
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
