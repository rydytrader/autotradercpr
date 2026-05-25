package com.rydytrader.autotrader.store;

import com.rydytrader.autotrader.entity.SettingEntity;
import com.rydytrader.autotrader.repository.SettingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Persists risk management settings to SQLite so they survive server restarts.
 */
@Component
public class RiskSettingsStore {

    private static final Logger log = LoggerFactory.getLogger(RiskSettingsStore.class);

    @Autowired
    private SettingRepository settingRepo;

    // ── settings container ───────────────────────────────────────────────────
    static class Cfg {
        volatile String tradingStartTime  = "09:15";
        volatile String tradingEndTime    = "15:25";
        volatile double totalCapital      = 0;     // total trading capital in ₹
        volatile double maxRiskPerDayPct  = 1.0;  // max risk per day as % of totalCapital
        volatile double riskPerTrade      = 1000;  // max ₹ loss per trade if SL hits
        volatile String autoSquareOffTime = "";  // empty = disabled, e.g. "15:15"
        volatile double atrMultiplier     = 1.5; // SL = close ± (ATR × this)
        volatile double brokeragePerOrder = 20.0;  // flat brokerage per order in ₹ (Fyers default)
        // Charges rates (regulatory — rarely change)
        volatile double sttRate           = 0.025;   // STT % on sell side
        volatile double exchangeRate      = 0.00345;  // Exchange transaction % (NSE cash)
        volatile double gstRate           = 18.0;     // GST % on brokerage + exchange
        volatile double sebiRate          = 10.0;     // SEBI charges ₹ per crore
        volatile double stampDutyRate     = 0.003;    // Stamp duty % on buy side
        volatile double brokeragePct      = 0.03;     // Brokerage % per order (cap)
        volatile int    fixedQuantity    = 2;     // -1 = use capital-based calculation
        volatile double capitalPerTrade  = 0;     // ₹ per trade (used when fixedQuantity == -1)
        volatile int    telegramAlertFrequency = 60; // seconds between Telegram portfolio updates (0 = disabled)
        // Global large-candle filter retired — each named pattern (Marubozu, Engulfing,
        // Doji reversal, Morning/Evening star) now enforces its own per-pattern max body
        // cap (xxxMaxBodyAtrMult). See the candle-pattern block below.
        // ── Candle pattern thresholds (BreakoutScanner / CandlePatternDetector) ──
        // Good-size candle (used in two-bar GOOD_SIZE_CANDLE_RETEST): decent body without
        // marubozu-strict wick rules. body ≥ N × ATR; opposing wick ≤ N × body so a green
        // bar with a long upper wick (shooting-star-shape) or red bar with long lower wick
        // (hammer-shape) doesn't qualify even though the body is big enough.
        volatile double goodSizeCandleBodyAtrMult           = 0.6;
        volatile double goodSizeCandleMaxBodyAtrMult        = 2.0;
        // Shared opposing-wick cap applied to the confirmation bar of every pattern:
        // Outside Reversal (bar 2), Doji Reversal (bar 2), Morning/Evening Star (bar 3),
        // Three Inside Up/Down (bar 3), AND the single-bar Good Size catch-all. For bullish
        // patterns this caps the upper wick; for bearish, the lower wick. 0.5 = wick can be
        // at most half the body. 0 disables the check.
        volatile double confirmationMaxOppositeWickRatio    = 0.5;
        // Pin bar (hammer / shooting star) — single range-relative test. Rejection wick must
        // be ≥ dominantWickMinRangeRatio × range, opposite wick ≤ oppositeWickMaxRangeRatio
        // × range. Body size is implicitly capped at (1 − dom − opp) × range.
        // 0.50 admits hammer-like bars with a meaningful body (up to 50% of range); raise
        // to 0.60 for the classical "wick dominates" pin bar definition or 0.67 for strict
        // Pivot Boss (upper/lower third).
        volatile double pinBarDominantWickMinRangeRatio = 0.50;
        volatile double pinBarOppositeWickMaxRangeRatio = 0.30;
        // Outside Reversal (Engulfing) — classical 2-bar bullish/bearish engulfing.
        // Strict color flip on both bars + shared body band + bar 2 closes past bar 1's
        // open (penetration 1.0). Lowering penetration accepts weaker reversal cousins
        // (0.5 = closes past bar 1's midpoint), but the default is strict engulfing.
        volatile double outsideReversalMinBodyAtrMult = 0.5;
        volatile double outsideReversalMaxBodyAtrMult = 2.0;
        volatile double outsideReversalPenetrationPct = 1.0;
        // Three Inside Up / Three Inside Down (3-bar harami + confirmation). bar1 large
        // directional (≥ N × ATR), bar2 opposite color with body fully INSIDE bar1's body
        // and body ≤ N × bar1 body (small inside bar — harami constraint), bar3 closes past
        // bar1's open in reversal direction (confirmation).
        // Shared body band for bar 1 and bar 3 (mirrors Morning/Evening Star's symmetric
        // outer-bar band). Bar 2 has no body-size cap — classical harami containment
        // (body fully inside bar 1's body) is the only constraint, hardcoded in
        // CandlePatternDetector. Floor keeps both outer bars meaningful; ceiling skips
        // climactic exhaustion outer bars. Set either side to 0 to disable.
        volatile double haramiBodyAtrMult           = 0.5;
        volatile double haramiBodyMaxAtrMult        = 2.0;
        // Bar 3 penetration into bar 1's body — parallel to starBar3PenetrationPct. 0.0 =
        // past bar 1's close (any recovery); 0.5 = past midpoint; 1.0 = past bar 1's open
        // (full body reclaim — the Nison classical Three Inside Up/Down requirement).
        // Default 1.0 keeps current behavior.
        volatile double haramiBar3PenetrationPct    = 1.0;
        // Doji reversal (2-bar): bar 1 (prev) is the doji — body ≤ dojiBodyMaxRangeRatio × range.
        // Bar 2 (curr) is the strong directional confirmation —
        // body in [dojiConfirmBodyAtrMult, dojiConfirmMaxBodyAtrMult] × ATR.
        volatile double dojiBodyMaxRangeRatio       = 0.10;
        volatile double dojiConfirmBodyAtrMult      = 0.5;
        volatile double dojiConfirmMaxBodyAtrMult   = 2.0;
        // Morning / evening star: bar1 + bar3 body in [outerBodyAtrMult, outerMaxBodyAtrMult] × ATR;
        // bar2 body ≤ N × bar1 body.
        volatile double starOuterBodyAtrMult        = 0.5;
        volatile double starOuterMaxBodyAtrMult     = 2.0;
        volatile double starMiddleBodyMaxMultOfOuter = 0.3;
        // Bar 3 penetration into bar 1's body. 0.0 = bar 3 close past bar 1's close (any
        // recovery); 0.5 = past midpoint (Nison classical); 1.0 = past bar 1's open (full
        // reclaim, same as Three Inside Up). Default 0.5 keeps current behavior.
        volatile double starBar3PenetrationPct      = 0.5;
        // Retest touch rule slack — bar's extreme can fall short of the level by up to
        // (toleranceAtr × ATR) and still count as a touch. Absorbs tick-level whisker misses.
        volatile double levelTouchToleranceAtr      = 0.2;
        // Entry-proximity cap — the confirmation bar's close must be within
        // (entryProximityAtrMult × ATR) of the near edge of the retested level (upper edge
        // for buy zones, lower edge for sell zones, the level itself for single-line levels).
        // Bounds SL distance and prevents patterns that touched the level via tolerance but
        // then ran far away from it. Set to 0 to disable.
        volatile double entryProximityAtrMult        = 1.0;
        volatile boolean enableTargetShift = true; // shift target to next level if default target < threshold ATR. If false, skip the entry.
        volatile boolean enableGapCheck = true;     // halve qty if day open or first candle beyond R2/S2
        // Weekly CPR intercept inside the walk-and-shift target picker. When ON: after the
        // walk picks a daily level satisfying minRR, the picker checks whether any weekly
        // CPR level (Pivot, TC, BC, R1-R4, S1-S4, PWH, PWL) sits strictly between entry and
        // that daily level. If yes, shift target to the closest weekly to entry; if the
        // shifted R/R falls below minRR, the trade is rejected. When OFF: skip the weekly
        // check entirely — the chosen daily level becomes the final target as-is.
        volatile boolean enableWeeklyLevelTargetShift = true;
        volatile boolean enableHtfHurdleFilter = true; // HPT→LPT when 5-min close lands inside R1/PWH (buy) or S1/PWL (sell) zone
        // Headroom check for the per-stock HTF Hurdle filter — mirror of niftyHurdleMinHeadroomAtr
        // but applied to the per-stock weekly candidate set. When > 0, trades are also rejected
        // when the nearest hurdle in the OPPOSITE direction (above stock LTP for buys / below
        // for sells) is closer than this many stock ATRs. 0 = headroom check off.
        volatile double htfHurdleMinHeadroomAtr = 1.0;
        // Daily ATR exhaustion filter — rejects new entries (both buy and sell) when today's
        // True Range (gap-inclusive) has consumed too much of the stock's 14-day daily ATR.
        // Opt-in. Threshold = 0 disables the check inline even when the toggle is on.
        volatile boolean enableDailyAtrExhaustionFilter = false;
        volatile double  dailyAtrExhaustionMult         = 0.85;
        // Open Range filter (strict ORB) — during the first {@code openRangeMinutes} after
        // market open the OR is "forming" and all signals are rejected. Post-formation,
        // buys require close > OR high and sells require close < OR low.
        volatile boolean enableOpenRangeFilter = false;
        volatile int     openRangeMinutes      = 15;
        // Primary-Index Open Range filter — same ORB rules applied to the stock's primary
        // index (NIFTY 50 OR the mapped sector index). Shares {@link #openRangeMinutes}
        // so both stock-side and index-side ORs use the same window.
        volatile boolean enableIndexOpenRangeFilter = false;
        // Marubozu Breakout — strict fresh-break entry on a conviction bar. Fires when
        // (a) this bar's close crosses a CPR/R/S level the prior bar's close didn't,
        // (b) the bar is a Marubozu (body in [floor, ceiling] × ATR; opposite-direction
        //     wick ≤ maxOppositeWickPctOfBody × body), and
        // (c) the bar's volume exceeds its 20-bar average (mandatory — no exceptions).
        // Coexists with the 6 retest patterns; level-broken state machine + brokenLevels
        // gate continue to apply. Marubozu Breakout is always on (no enable toggle) —
        // only the shape parameters below tune its detection.
        volatile double  marubozuBreakoutBodyAtrMult           = 0.5;
        volatile double  marubozuBreakoutMaxBodyAtrMult        = 1.0;
        volatile double  marubozuBreakoutMaxOppositeWickPctOfBody = 0.10;
        // Index HTF Hurdle filter. When on, every stock signal is gated against its primary
        // index's 1-hour close vs that index's nearest weekly hurdle in trade direction. Default
        // off — opt-in. Replaces the old NIFTY-only macro filter; now runs per primary index.
        volatile boolean enableIndexHtfHurdleFilter = false;
        // Index HTF Trend Alignment filter — strict 2-factor on the stock's primary index
        // 1-hour timeframe: index 1h close vs index weekly CPR + index 1h EMA20. Buys need
        // index HTF state = BULLISH; sells need BEARISH. Mirrors checkStockHtfAlignment but
        // on the index. Default off — opt-in.
        volatile boolean enableIndexHtfAlignment = false;
        // Headroom check for the Index HTF Hurdle filter. When > 0, trades are also rejected
        // when the nearest hurdle in the OPPOSITE direction is closer than this many ATRs of
        // the primary index. 0 = headroom check off.
        volatile double indexHtfHurdleMinHeadroomAtr = 1.0;
        // 5-min variant of the Index HTF Hurdle filter, against the primary index's daily CPR
        // levels. When on, every stock breakout requires the primary index's same-bucket 5-min
        // close to have cleared its nearest daily-CPR hurdle in trade direction. Default off.
        volatile boolean enableIndex5mHurdleFilter = false;
        // Headroom check for the Index 5m Hurdle filter. 0 = headroom check off.
        volatile double index5mHurdleMinHeadroomAtr = 1.0;
        // Structural SL — opt-in, anchors SL to the S/R level the trade is testing (per setup family)
        // When on, we compute both structural and default SL and pick the TIGHTER one.
        volatile boolean enableStructuralSl = false;   // when false, always use close ± atrMultiplier × ATR
        volatile double  structuralSlBufferAtr = 1.0;  // ATR multiplier added below/above the structural anchor
        // Extra ATR cushion applied ONLY to single-level setups (R2/R3/R4, S2/S3/S4, DH/DL).
        // Zone setups (CPR, R1+PDH, S1+PDL, magnets) already get a built-in cushion from the
        // zone width itself, so they don't need this. Active only when enableStructuralSl is on.
        volatile double  singleLevelSlBufferAtr = 0.5;
        volatile double dayHighLowMinAtr = 0.5; // min distance in ATR for day high/low shifted target (0 = no check)
        // Risk/Reward filter — skip trade if |target−entry| / |entry−SL| < minRiskRewardRatio
        volatile boolean enableRiskRewardFilter = true;
        volatile double  minRiskRewardRatio     = 1.0;
        // EMA filters
        // 5-min EMA trend gate: buy requires close above EMA 20, sell requires close below EMA 20.
        // (enableEmaTrendCheck removed — 5-min EMA factor is now hard-baked into the
        // strict trend state machine in BreakoutScanner. The top-level direction gate
        // checks state per setup category; the standalone EMA Price Filter no longer exists.)
        // EMA level-count filter — counts CPR zones strictly between EMA and the broken level.
        // Allow only when count == 0 (EMA is in the zone immediately adjacent to the broken level).
        volatile boolean enableEmaLevelCountFilter = true;
        // Secondary proximity constraint on top of the level-count filter. Requires EMA to sit
        // within (100 - emaLevelMinRangePct)% of the range from the broken level to the nearest
        // non-broken zone edge on the other side. 50 = EMA must be in the upper half of that
        // range (buy) / lower half (sell). 0 = proximity check disabled.
        volatile int emaLevelMinRangePct = 50;
        // Morning skip: when enabled, the EMA level-count filter is bypassed before this time.
        // Rationale: in the first hour of the session price can run hard while EMA(20) lags
        // behind, making the filter reject otherwise valid breakouts.
        volatile boolean emaLevelFilterMorningSkip = false;
        volatile String  emaLevelFilterMorningSkipUntil = "10:15"; // HH:mm IST
        volatile boolean enableTrailingSl = true; // enable breakeven SL — moves SL to entry ± buffer once price reaches breakevenTriggerPct of (entry→target) range
        // Defensive Price-vs-EMA exit. At every 5-min candle close, if the just-closed bar's
        // close is against the trade direction relative to the 5-min EMA 20 (LONG: close < EMA 20;
        // SHORT: close > EMA 20), squareoff the position before SL hits. Default off — material
        // behavior change.
        volatile boolean enablePriceEmaExit = false;
        // Virgin CPR — when NIFTY's session range never overlapped today's daily CPR (BC..TC)
        // by 15:30 IST, that day's CPR levels (TC, Pivot, BC) are cached as a "virgin CPR" and
        // become available to the dedicated Virgin CPR Hurdle filter for the next N trading
        // days. A new virgin CPR replaces any existing active one. 0 = feature disabled.
        volatile int virginCprExpiryDays = 10;
        // Breakeven SL — single-stage move. When the peak (long) / trough (short) reaches
        // {@code breakevenTriggerPct}% of the range from entry to target, the SL is moved to
        // {@code entry ± breakevenSlAtrMult × ATR}. Once moved, it stays put (never widens).
        volatile double breakevenTriggerPct = 61.8;   // peak/trough hits this % of (entry→target) range
        volatile double breakevenSlAtrMult  = 1.0;    // new SL = entry ± N × ATR
        // Extended-level breakouts (R3/S3, R4/S4) are skipped on normal IV/OV days but allowed
        // on EV (gap up/down) days regardless. Toggles default ON (skip on normal days).
        // Daily extended-level skip — split by day type. IV/OV = open print inside CPR or
        // between CPR and R2/S2; EV = open print outside R2/S2 (gap). Defaults preserve the
        // Skip R3/S3 and R4/S4 breakouts + retest signals (the setup name covers both fresh
        // breakouts and retest variants). Single toggle per level pair — no day-type split.
        // Legacy keys (skipR3S3IvOvDays / skipR3S3EvDays / skipR4S4IvOvDays / skipR4S4EvDays
        // / skipR3S3NormalDays / skipR4S4NormalDays) silently fold into the new toggle on
        // load: any legacy-true value flips the new toggle to true.
        volatile boolean skipR1PdhS1Pdl = false;
        volatile boolean skipR2S2 = false;
        volatile boolean skipR3S3 = true;
        volatile boolean skipR4S4 = true;
        // Counter-trend setups split into two independent families. Each has its own
        // enable toggle and qty factor.
        //   • Magnets        — BUY_ABOVE_S1_PDL, SELL_BELOW_R1_PDH (first structural pair).
        //   • Mean-reversion — BUY_ABOVE_S2/S3/S4, SELL_BELOW_R2/R3/R4 (deep fades).
        // All 8 are day-type agnostic — fire on IV/OV/EV alike.
        volatile boolean enableMeanReversionTrades = true;
        volatile boolean enableMagnetTrades        = true;
        // Per-family qty factors override the legacy mptQtyFactor for these specific setups.
        // Magnets are higher-probability than deep mean-rev, so default magnet at 0.75 (same
        // as the prior shared MPT factor) and mean-rev at 0.5 (riskier — half size).
        volatile double magnetTradesQtyFactor      = 0.75;
        volatile double meanReversionQtyFactor     = 0.5;
        volatile int    atrPeriod = 14;        // ATR lookback period for initial SL
        // Scanner settings
        volatile String signalSource    = "TRADINGVIEW"; // TRADINGVIEW or INTERNAL
        volatile int    scannerTimeframe = 15;  // candle timeframe in minutes
        volatile int    higherTimeframe  = 60;  // higher TF for weekly trend (minutes) — Fyers native resolution
        volatile boolean enableHpt      = true;  // High Probable Trade signals (weekly+daily aligned)
        // Medium Probable Trade — produced by static counter-trend setups (S1+PDL/S2-S4 buys,
        // R1+PDH/R2-R4 sells). Trades at reduced qty via mptQtyFactor.
        volatile boolean enableMpt      = true;
        volatile double mptQtyFactor    = 0.75;
        volatile double minAbsoluteProfit = 500; // skip if qty × target_distance < this amount (₹)
        // Narrow-CPR single-level SL buffer threshold. When CPR width % is below this
        // value, the three zone setups (CPR, R1+PDH, S1+PDL) get the same extra
        // singleLevelSlBufferAtr cushion that pure single-level setups (R2/R3/R4, S2/S3/S4)
        // already receive — narrow CPR means the zone-width cushion is too small to absorb
        // a normal pullback, so we widen the SL the same way as for a pure single level.
        // Breakout detection and zone-broken state tracking are NOT affected — only the SL.
        // 0 = feature disabled. Default 0.1% matches typical "very tight" CPR.
        volatile double narrowCprZoneCollapseWidthPct = 0.1;
        // narrowRangeRatioThreshold removed — z-score of PDH-PDL/CPR ratio is self-calibrating
        volatile double insideCprMaxWidth = 0.5;  // max CPR width % for inside CPR stocks (0 = no filter)
        // Adaptive CPR State Classifier — replaces the legacy narrowCprMin/MaxWidth band.
        // Two multipliers anchor today's CPR width vs the stock's own 20-day SMA: narrow
        // upper bound and wide lower bound. Stocks in between fall into AVERAGE.
        volatile double cprWidthSqueezeMult = 0.70;  // today width / 20d avg ≤ this → NARROW
        volatile double cprWidthWideMult    = 1.00;  // today width / 20d avg > this  → WIDE
        // Watchlist inclusion toggles per state (Dynamic Squeeze / Standard Expansion /
        // Volatility Exhaustion). Default all on — every stock with ≥ 20d history appears.
        volatile boolean enableCprStateA = true;   // Dynamic Squeeze (true breakout candidate)
        volatile boolean enableCprStateB = true;   // Standard Expansion (orderly grind / pullback)
        volatile boolean enableCprStateC = true;   // Volatility Exhaustion (Doji trap / mean-reversion fade)
        // Watchlist universe is fully driven by the DB Stock Universe (Settings → Stock
        // Universe). Legacy fields (`scanUniverse`, `scanOnlyNifty50`) have been removed —
        // the bhavcopy fetcher hardcodes NIFTY 100 cap-flag seeding.
        volatile double scanMinPrice = 300;      // min stock price filter (0 = no filter)
        volatile double scanMaxPrice = 0;        // max stock price filter (0 = no max)
        // Opening refresh — re-fetches today's candles from Fyers /data/history after
        // 9:20 to correct any wrong live-tick-built first candle (Fyers' live WS data is
        // unreliable during 9:15-9:25 per their own docs). Re-seeds completedCandles, SMA, ATR,
        // firstCandleClose, dayOpen. Configurable HH:mm time (IST).
        volatile boolean enableOpeningRefresh = true;
        volatile String  openingRefreshTime   = "09:25"; // IST, HH:mm
        // Target Tolerance — discount structural target by ATR fraction so near-miss reversals fill
        volatile boolean enableTargetTolerance = true;
        volatile double targetToleranceAtr = 0.10; // discount structural target by this fraction of ATR
        // Index Trend Alignment — when on, every stock signal is gated against the stock's
        // primary index trend state (NIFTY 50 OR the sector index it maps to via the Stock
        // Universe table). Buys require BULLISH (or BULLISH_REVERSAL for HPT); sells require
        // BEARISH (or BEARISH_REVERSAL for HPT); SIDEWAYS / NEUTRAL / opposite-direction
        // skip the trade outright. Replaces the old separate NIFTY + sector alignment checks.
        volatile boolean enableIndexAlignment = false;        // master toggle, opt-in
        // Stock HTF Trend Alignment — confirms the stock's own 1-hour HTF trend agrees
        // with the trade direction (BULLISH/BULLISH_REVERSAL for buy, BEARISH/BEARISH_REVERSAL
        // for sell). Computed from 1-hour close vs weekly CPR + 1-hour EMA20. Opt-in.
        volatile boolean enableStockHtfAlignment = false;
        // Optional 2nd factor for Stock HTF Alignment. When TRUE (default) the filter
        // runs the strict 2-factor state machine (1h close vs weekly CPR AND 1h close vs
        // 1h EMA20). When FALSE the EMA factor is skipped — state is determined purely
        // by where the 1h close sits relative to the weekly CPR zone.
        volatile boolean enableStockHtf1hEma20Check = true;
    }

    private final Cfg live = new Cfg();

    @jakarta.annotation.PostConstruct
    public void init() {
        load("live");
    }

    // ── always returns live config ───────────────────────────────────────────
    private Cfg cfg() {
        return live;
    }

    public Cfg cfgFor(String mode) {
        return live;
    }

    // ── parameterless getters/setters (used by TradingController etc.) ────────
    public String getTradingStartTime()  { return cfg().tradingStartTime; }
    public String getTradingEndTime()    { return cfg().tradingEndTime; }
    public double getTotalCapital()       { return cfg().totalCapital; }
    public double getMaxRiskPerDayPct()  { return cfg().maxRiskPerDayPct; }
    public double getRiskPerTrade()      { return cfg().riskPerTrade; }
    public double getMaxDailyLoss()      { return cfg().totalCapital * cfg().maxRiskPerDayPct / 100.0; }
    public String getAutoSquareOffTime() { return cfg().autoSquareOffTime; }
    public double getAtrMultiplier()     { return cfg().atrMultiplier; }
    public double getBrokeragePerOrder() { return cfg().brokeragePerOrder; }
    public double getSttRate()         { return cfg().sttRate; }
    public double getExchangeRate()    { return cfg().exchangeRate; }
    public double getGstRate()         { return cfg().gstRate; }
    public double getSebiRate()        { return cfg().sebiRate; }
    public double getStampDutyRate()   { return cfg().stampDutyRate; }
    public double getBrokeragePct()    { return cfg().brokeragePct; }
    public int    getFixedQuantity()   { return cfg().fixedQuantity; }
    public double getCapitalPerTrade() { return cfg().capitalPerTrade; }
    public int    getTelegramAlertFrequency() { return cfg().telegramAlertFrequency; }
    public boolean isEnableGapCheck() { return cfg().enableGapCheck; }
    public boolean isEnableWeeklyLevelTargetShift() { return cfg().enableWeeklyLevelTargetShift; }
    public boolean isEnableHtfHurdleFilter()    { return cfg().enableHtfHurdleFilter; }
    public double  getHtfHurdleMinHeadroomAtr() { return cfg().htfHurdleMinHeadroomAtr; }
    public boolean isEnableDailyAtrExhaustionFilter() { return cfg().enableDailyAtrExhaustionFilter; }
    public double  getDailyAtrExhaustionMult()        { return cfg().dailyAtrExhaustionMult; }
    public boolean isEnableOpenRangeFilter() { return cfg().enableOpenRangeFilter; }
    public int     getOpenRangeMinutes()     { return cfg().openRangeMinutes; }
    public boolean isEnableIndexOpenRangeFilter() { return cfg().enableIndexOpenRangeFilter; }
    public double  getMarubozuBreakoutBodyAtrMult()            { return cfg().marubozuBreakoutBodyAtrMult; }
    public double  getMarubozuBreakoutMaxBodyAtrMult()         { return cfg().marubozuBreakoutMaxBodyAtrMult; }
    public double  getMarubozuBreakoutMaxOppositeWickPctOfBody() { return cfg().marubozuBreakoutMaxOppositeWickPctOfBody; }
    public boolean isEnableIndexHtfHurdleFilter() { return cfg().enableIndexHtfHurdleFilter; }
    public boolean isEnableIndexHtfAlignment()    { return cfg().enableIndexHtfAlignment; }
    public double  getIndexHtfHurdleMinHeadroomAtr() { return cfg().indexHtfHurdleMinHeadroomAtr; }
    public boolean isEnableIndex5mHurdleFilter()  { return cfg().enableIndex5mHurdleFilter; }
    public double  getIndex5mHurdleMinHeadroomAtr() { return cfg().index5mHurdleMinHeadroomAtr; }
    public boolean isEnableStructuralSl()    { return cfg().enableStructuralSl; }
    public double  getStructuralSlBufferAtr(){ return cfg().structuralSlBufferAtr; }
    public double  getSingleLevelSlBufferAtr(){ return cfg().singleLevelSlBufferAtr; }
    public double getDayHighLowMinAtr()            { return cfg().dayHighLowMinAtr; }
    public boolean isEnableRiskRewardFilter()      { return cfg().enableRiskRewardFilter; }
    public double  getMinRiskRewardRatio()         { return cfg().minRiskRewardRatio; }
    public boolean isEnableEmaLevelCountFilter()   { return cfg().enableEmaLevelCountFilter; }
    public int getEmaLevelMinRangePct()            { return cfg().emaLevelMinRangePct; }
    public boolean isEmaLevelFilterMorningSkip()       { return cfg().emaLevelFilterMorningSkip; }
    public String  getEmaLevelFilterMorningSkipUntil() { return cfg().emaLevelFilterMorningSkipUntil; }
    public boolean isEnableTargetShift() { return cfg().enableTargetShift; }
    public double getGoodSizeCandleBodyAtrMult()          { return cfg().goodSizeCandleBodyAtrMult; }
    public double getGoodSizeCandleMaxBodyAtrMult()       { return cfg().goodSizeCandleMaxBodyAtrMult; }
    public double getConfirmationMaxOppositeWickRatio()   { return cfg().confirmationMaxOppositeWickRatio; }
    public double getPinBarDominantWickMinRangeRatio() { return cfg().pinBarDominantWickMinRangeRatio; }
    public double getPinBarOppositeWickMaxRangeRatio() { return cfg().pinBarOppositeWickMaxRangeRatio; }
    public double getOutsideReversalMinBodyAtrMult() { return cfg().outsideReversalMinBodyAtrMult; }
    public double getOutsideReversalMaxBodyAtrMult() { return cfg().outsideReversalMaxBodyAtrMult; }
    public double getOutsideReversalPenetrationPct() { return cfg().outsideReversalPenetrationPct; }
    public double getHaramiBodyAtrMult()           { return cfg().haramiBodyAtrMult; }
    public double getHaramiBodyMaxAtrMult()        { return cfg().haramiBodyMaxAtrMult; }
    public double getHaramiBar3PenetrationPct()    { return cfg().haramiBar3PenetrationPct; }
    public double getDojiBodyMaxRangeRatio()       { return cfg().dojiBodyMaxRangeRatio; }
    public double getDojiConfirmBodyAtrMult()      { return cfg().dojiConfirmBodyAtrMult; }
    public double getDojiConfirmMaxBodyAtrMult()   { return cfg().dojiConfirmMaxBodyAtrMult; }
    public double getStarOuterBodyAtrMult()        { return cfg().starOuterBodyAtrMult; }
    public double getStarOuterMaxBodyAtrMult()     { return cfg().starOuterMaxBodyAtrMult; }
    public double getStarMiddleBodyMaxMultOfOuter() { return cfg().starMiddleBodyMaxMultOfOuter; }
    public double getStarBar3PenetrationPct()       { return cfg().starBar3PenetrationPct; }
    public double getLevelTouchToleranceAtr()      { return cfg().levelTouchToleranceAtr; }
    public double getEntryProximityAtrMult()       { return cfg().entryProximityAtrMult; }
    public boolean isEnableTrailingSl() { return cfg().enableTrailingSl; }
    public boolean isEnablePriceEmaExit() { return cfg().enablePriceEmaExit; }
    public int getVirginCprExpiryDays() { return cfg().virginCprExpiryDays; }
    public double getBreakevenTriggerPct() { return cfg().breakevenTriggerPct; }
    public double getBreakevenSlAtrMult()  { return cfg().breakevenSlAtrMult; }
    public boolean isSkipR1PdhS1Pdl() { return cfg().skipR1PdhS1Pdl; }
    public boolean isSkipR2S2() { return cfg().skipR2S2; }
    public boolean isSkipR3S3() { return cfg().skipR3S3; }
    public boolean isSkipR4S4() { return cfg().skipR4S4; }
    public boolean isEnableMeanReversionTrades() { return cfg().enableMeanReversionTrades; }
    public boolean isEnableMagnetTrades()        { return cfg().enableMagnetTrades; }
    public double  getMagnetTradesQtyFactor()    { return cfg().magnetTradesQtyFactor; }
    public double  getMeanReversionQtyFactor()   { return cfg().meanReversionQtyFactor; }
    public int getAtrPeriod() { return cfg().atrPeriod; }

    public String  getSignalSource()      { return cfg().signalSource; }
    public int     getScannerTimeframe()  { return cfg().scannerTimeframe; }
    public int     getHigherTimeframe()   { return cfg().higherTimeframe; }
    public boolean isEnableHpt()          { return cfg().enableHpt; }
    public boolean isEnableMpt()          { return cfg().enableMpt; }
    public double getMptQtyFactor()       { return cfg().mptQtyFactor; }
    public double getMinAbsoluteProfit() { return cfg().minAbsoluteProfit; }
    public double getInsideCprMaxWidth()    { return cfg().insideCprMaxWidth; }
    public double getCprWidthSqueezeMult()  { return cfg().cprWidthSqueezeMult; }
    public double getCprWidthWideMult()     { return cfg().cprWidthWideMult; }
    public boolean isEnableCprStateA()      { return cfg().enableCprStateA; }
    public boolean isEnableCprStateB()      { return cfg().enableCprStateB; }
    public boolean isEnableCprStateC()      { return cfg().enableCprStateC; }
    public double getNarrowCprZoneCollapseWidthPct() { return cfg().narrowCprZoneCollapseWidthPct; }
    public double getScanMinPrice() { return cfg().scanMinPrice; }
    public double getScanMaxPrice() { return cfg().scanMaxPrice; }
    public boolean isEnableOpeningRefresh()    { return cfg().enableOpeningRefresh; }
    public String  getOpeningRefreshTime()     { return cfg().openingRefreshTime; }
    public boolean isEnableTargetTolerance()   { return cfg().enableTargetTolerance; }
    public double getTargetToleranceAtr()      { return cfg().targetToleranceAtr; }
    public boolean isEnableIndexAlignment()    { return cfg().enableIndexAlignment; }
    public boolean isEnableStockHtfAlignment() { return cfg().enableStockHtfAlignment; }
    public boolean isEnableStockHtf1hEma20Check() { return cfg().enableStockHtf1hEma20Check; }
    public void setSignalSource(String v)      { cfg().signalSource = v; }
    public void setScannerTimeframe(int v)     { cfg().scannerTimeframe = v; }
    public void setHigherTimeframe(int v)      { cfg().higherTimeframe = v; }
    public void setEnableHpt(boolean v)        { cfg().enableHpt = v; }
    public void setEnableMpt(boolean v)        { cfg().enableMpt = v; }
    public void setMptQtyFactor(double v)      { cfg().mptQtyFactor = v; }
    public void setMinAbsoluteProfit(double v) { cfg().minAbsoluteProfit = v; }
    public void setCprWidthSqueezeMult(double v)  { cfg().cprWidthSqueezeMult  = Math.max(0, v); }
    public void setCprWidthWideMult(double v)     { cfg().cprWidthWideMult     = Math.max(0, v); }
    public void setEnableCprStateA(boolean v) { cfg().enableCprStateA = v; }
    public void setEnableCprStateB(boolean v) { cfg().enableCprStateB = v; }
    public void setEnableCprStateC(boolean v) { cfg().enableCprStateC = v; }
    public void setInsideCprMaxWidth(double v) { cfg().insideCprMaxWidth = v; }
    public void setNarrowCprZoneCollapseWidthPct(double v) { cfg().narrowCprZoneCollapseWidthPct = v; }
    public void setScanMinPrice(double v) { cfg().scanMinPrice = v; }
    public void setScanMaxPrice(double v) { cfg().scanMaxPrice = v; }
    public void setEnableOpeningRefresh(boolean v) { cfg().enableOpeningRefresh = v; }
    public void setOpeningRefreshTime(String v)    { cfg().openingRefreshTime = v; }
    public void setEnableTargetTolerance(boolean v) { cfg().enableTargetTolerance = v; }
    public void setTargetToleranceAtr(double v) { cfg().targetToleranceAtr = v; }
    public void setEnableIndexAlignment(boolean v)        { cfg().enableIndexAlignment = v; }
    public void setEnableStockHtfAlignment(boolean v)     { cfg().enableStockHtfAlignment = v; }
    public void setEnableStockHtf1hEma20Check(boolean v)  { cfg().enableStockHtf1hEma20Check = v; }
    public void setTradingStartTime(String v)  { cfg().tradingStartTime = v; }
    public void setTradingEndTime(String v)    { cfg().tradingEndTime = v; }
    public void setTotalCapital(double v)       { cfg().totalCapital = v; }
    public void setMaxRiskPerDayPct(double v)  { cfg().maxRiskPerDayPct = v; }
    public void setRiskPerTrade(double v)      { cfg().riskPerTrade = v; }
    public void setAutoSquareOffTime(String v) { cfg().autoSquareOffTime = v; }
    public void setAtrMultiplier(double v)     { cfg().atrMultiplier = v; }
    public void setBrokeragePerOrder(double v) { cfg().brokeragePerOrder = v; }
    public void setSttRate(double v)         { cfg().sttRate = v; }
    public void setExchangeRate(double v)    { cfg().exchangeRate = v; }
    public void setGstRate(double v)         { cfg().gstRate = v; }
    public void setSebiRate(double v)        { cfg().sebiRate = v; }
    public void setStampDutyRate(double v)   { cfg().stampDutyRate = v; }
    public void setBrokeragePct(double v)    { cfg().brokeragePct = v; }
    public void setFixedQuantity(int v)      { cfg().fixedQuantity = v; }
    public void setCapitalPerTrade(double v) { cfg().capitalPerTrade = v; }
    public void setTelegramAlertFrequency(int v) { cfg().telegramAlertFrequency = v; }
    public void setEnableGapCheck(boolean v) { cfg().enableGapCheck = v; }
    public void setEnableWeeklyLevelTargetShift(boolean v) { cfg().enableWeeklyLevelTargetShift = v; }
    public void setEnableHtfHurdleFilter(boolean v)    { cfg().enableHtfHurdleFilter = v; }
    public void setHtfHurdleMinHeadroomAtr(double v)   { cfg().htfHurdleMinHeadroomAtr = Math.max(0, v); }
    public void setEnableDailyAtrExhaustionFilter(boolean v) { cfg().enableDailyAtrExhaustionFilter = v; }
    public void setDailyAtrExhaustionMult(double v)          { cfg().dailyAtrExhaustionMult = Math.max(0, v); }
    public void setEnableOpenRangeFilter(boolean v) { cfg().enableOpenRangeFilter = v; }
    public void setOpenRangeMinutes(int v)          { cfg().openRangeMinutes = Math.max(5, Math.min(240, v)); }
    public void setEnableIndexOpenRangeFilter(boolean v) { cfg().enableIndexOpenRangeFilter = v; }
    public void setMarubozuBreakoutBodyAtrMult(double v)              { cfg().marubozuBreakoutBodyAtrMult = Math.max(0, v); }
    public void setMarubozuBreakoutMaxBodyAtrMult(double v)           { cfg().marubozuBreakoutMaxBodyAtrMult = Math.max(0, v); }
    public void setMarubozuBreakoutMaxOppositeWickPctOfBody(double v) { cfg().marubozuBreakoutMaxOppositeWickPctOfBody = Math.max(0, v); }
    public void setEnableIndexHtfHurdleFilter(boolean v) { cfg().enableIndexHtfHurdleFilter = v; }
    public void setEnableIndexHtfAlignment(boolean v)    { cfg().enableIndexHtfAlignment = v; }
    public void setIndexHtfHurdleMinHeadroomAtr(double v) { cfg().indexHtfHurdleMinHeadroomAtr = Math.max(0, v); }
    public void setEnableIndex5mHurdleFilter(boolean v)  { cfg().enableIndex5mHurdleFilter = v; }
    public void setIndex5mHurdleMinHeadroomAtr(double v) { cfg().index5mHurdleMinHeadroomAtr = Math.max(0, v); }
    public void setEnableStructuralSl(boolean v)    { cfg().enableStructuralSl = v; }
    public void setStructuralSlBufferAtr(double v)  { cfg().structuralSlBufferAtr = v; }
    public void setSingleLevelSlBufferAtr(double v) { cfg().singleLevelSlBufferAtr = v; }
    public void setDayHighLowMinAtr(double v)              { cfg().dayHighLowMinAtr = v; }
    public void setEnableRiskRewardFilter(boolean v)       { cfg().enableRiskRewardFilter = v; }
    public void setMinRiskRewardRatio(double v)            { cfg().minRiskRewardRatio = v; }
    public void setEnableEmaLevelCountFilter(boolean v)    { cfg().enableEmaLevelCountFilter = v; }
    public void setEmaLevelMinRangePct(int v)               { cfg().emaLevelMinRangePct = Math.max(0, Math.min(100, v)); }
    public void setEmaLevelFilterMorningSkip(boolean v)     { cfg().emaLevelFilterMorningSkip = v; }
    public void setEmaLevelFilterMorningSkipUntil(String v) { if (v != null && !v.isEmpty()) cfg().emaLevelFilterMorningSkipUntil = v; }
    public void setEnableTargetShift(boolean v) { cfg().enableTargetShift = v; }
    public void setGoodSizeCandleBodyAtrMult(double v)          { cfg().goodSizeCandleBodyAtrMult = v; }
    public void setGoodSizeCandleMaxBodyAtrMult(double v)       { cfg().goodSizeCandleMaxBodyAtrMult = v; }
    public void setConfirmationMaxOppositeWickRatio(double v)   { cfg().confirmationMaxOppositeWickRatio = v; }
    public void setPinBarDominantWickMinRangeRatio(double v) { cfg().pinBarDominantWickMinRangeRatio = v; }
    public void setPinBarOppositeWickMaxRangeRatio(double v) { cfg().pinBarOppositeWickMaxRangeRatio = v; }
    public void setOutsideReversalMinBodyAtrMult(double v) { cfg().outsideReversalMinBodyAtrMult = v; }
    public void setOutsideReversalMaxBodyAtrMult(double v) { cfg().outsideReversalMaxBodyAtrMult = v; }
    public void setOutsideReversalPenetrationPct(double v) { cfg().outsideReversalPenetrationPct = v; }
    public void setHaramiBodyAtrMult(double v)           { cfg().haramiBodyAtrMult = v; }
    public void setHaramiBodyMaxAtrMult(double v)        { cfg().haramiBodyMaxAtrMult = v; }
    public void setHaramiBar3PenetrationPct(double v)    { cfg().haramiBar3PenetrationPct = v; }
    public void setDojiBodyMaxRangeRatio(double v)       { cfg().dojiBodyMaxRangeRatio = v; }
    public void setDojiConfirmBodyAtrMult(double v)      { cfg().dojiConfirmBodyAtrMult = v; }
    public void setDojiConfirmMaxBodyAtrMult(double v)   { cfg().dojiConfirmMaxBodyAtrMult = v; }
    public void setStarOuterBodyAtrMult(double v)        { cfg().starOuterBodyAtrMult = v; }
    public void setStarOuterMaxBodyAtrMult(double v)     { cfg().starOuterMaxBodyAtrMult = v; }
    public void setStarMiddleBodyMaxMultOfOuter(double v) { cfg().starMiddleBodyMaxMultOfOuter = v; }
    public void setStarBar3PenetrationPct(double v)       { cfg().starBar3PenetrationPct = v; }
    public void setLevelTouchToleranceAtr(double v)      { cfg().levelTouchToleranceAtr = Math.max(0, v); }
    public void setEntryProximityAtrMult(double v)       { cfg().entryProximityAtrMult = Math.max(0, v); }
    public void setEnableTrailingSl(boolean v) { cfg().enableTrailingSl = v; }
    public void setEnablePriceEmaExit(boolean v) { cfg().enablePriceEmaExit = v; }
    public void setVirginCprExpiryDays(int v) { cfg().virginCprExpiryDays = Math.max(0, v); }
    public void setBreakevenTriggerPct(double v) { cfg().breakevenTriggerPct = v; }
    public void setBreakevenSlAtrMult(double v)  { cfg().breakevenSlAtrMult = v; }
    public void setSkipR1PdhS1Pdl(boolean v) { cfg().skipR1PdhS1Pdl = v; }
    public void setSkipR2S2(boolean v) { cfg().skipR2S2 = v; }
    public void setSkipR3S3(boolean v) { cfg().skipR3S3 = v; }
    public void setSkipR4S4(boolean v) { cfg().skipR4S4 = v; }
    public void setEnableMeanReversionTrades(boolean v) {
        cfg().enableMeanReversionTrades = v;
        // Mean-reversion setups classify as MPT downstream — turning on the master toggle
        // also turns on the MPT tier gate so the trades aren't silently blocked by a stale
        // enableMpt=false value. One-directional: turning mean-rev OFF leaves enableMpt alone.
        if (v) cfg().enableMpt = true;
    }
    public void setEnableMagnetTrades(boolean v) {
        cfg().enableMagnetTrades = v;
        // Magnets also classify as MPT downstream. Same auto-on-MPT behavior as mean-rev.
        if (v) cfg().enableMpt = true;
    }
    public void setMagnetTradesQtyFactor(double v)  { cfg().magnetTradesQtyFactor  = v; }
    public void setMeanReversionQtyFactor(double v) { cfg().meanReversionQtyFactor = v; }
    public void setAtrPeriod(int v) { cfg().atrPeriod = v; }

    // ── mode-specific getters/setters (used by SettingsController) ────────────
    public String getTradingStartTime(String mode)  { return cfgFor(mode).tradingStartTime; }
    public String getTradingEndTime(String mode)    { return cfgFor(mode).tradingEndTime; }
    public double getTotalCapital(String mode)       { return cfgFor(mode).totalCapital; }
    public double getMaxRiskPerDayPct(String mode)  { return cfgFor(mode).maxRiskPerDayPct; }
    public double getRiskPerTrade(String mode)      { return cfgFor(mode).riskPerTrade; }
    public double getMaxDailyLoss(String mode)      { return cfgFor(mode).totalCapital * cfgFor(mode).maxRiskPerDayPct / 100.0; }
    public String getAutoSquareOffTime(String mode) { return cfgFor(mode).autoSquareOffTime; }
    public double getAtrMultiplier(String mode)     { return cfgFor(mode).atrMultiplier; }
    public double getBrokeragePerOrder(String mode) { return cfgFor(mode).brokeragePerOrder; }
    public double getSttRate(String mode)         { return cfgFor(mode).sttRate; }
    public double getExchangeRate(String mode)    { return cfgFor(mode).exchangeRate; }
    public double getGstRate(String mode)         { return cfgFor(mode).gstRate; }
    public double getSebiRate(String mode)        { return cfgFor(mode).sebiRate; }
    public double getStampDutyRate(String mode)   { return cfgFor(mode).stampDutyRate; }
    public double getBrokeragePct(String mode)    { return cfgFor(mode).brokeragePct; }
    public int    getFixedQuantity(String mode)   { return cfgFor(mode).fixedQuantity; }
    public double getCapitalPerTrade(String mode) { return cfgFor(mode).capitalPerTrade; }
    public int    getTelegramAlertFrequency(String mode) { return cfgFor(mode).telegramAlertFrequency; }
    public boolean isEnableTargetShift(String mode) { return cfgFor(mode).enableTargetShift; }
    public boolean isEnableTrailingSl(String mode) { return cfgFor(mode).enableTrailingSl; }

    public void setTradingStartTime(String mode, String v)  { cfgFor(mode).tradingStartTime = v; }
    public void setTradingEndTime(String mode, String v)    { cfgFor(mode).tradingEndTime = v; }
    public void setTotalCapital(String mode, double v)       { cfgFor(mode).totalCapital = v; }
    public void setMaxRiskPerDayPct(String mode, double v)  { cfgFor(mode).maxRiskPerDayPct = v; }
    public void setRiskPerTrade(String mode, double v)      { cfgFor(mode).riskPerTrade = v; }
    public void setAutoSquareOffTime(String mode, String v) { cfgFor(mode).autoSquareOffTime = v; }
    public void setAtrMultiplier(String mode, double v)     { cfgFor(mode).atrMultiplier = v; }
    public void setBrokeragePerOrder(String mode, double v) { cfgFor(mode).brokeragePerOrder = v; }
    public void setSttRate(String mode, double v)         { cfgFor(mode).sttRate = v; }
    public void setExchangeRate(String mode, double v)    { cfgFor(mode).exchangeRate = v; }
    public void setGstRate(String mode, double v)         { cfgFor(mode).gstRate = v; }
    public void setSebiRate(String mode, double v)        { cfgFor(mode).sebiRate = v; }
    public void setStampDutyRate(String mode, double v)   { cfgFor(mode).stampDutyRate = v; }
    public void setBrokeragePct(String mode, double v)    { cfgFor(mode).brokeragePct = v; }
    public void setFixedQuantity(String mode, int v)      { cfgFor(mode).fixedQuantity = v; }
    public void setCapitalPerTrade(String mode, double v) { cfgFor(mode).capitalPerTrade = v; }
    public void setTelegramAlertFrequency(String mode, int v) { cfgFor(mode).telegramAlertFrequency = v; }
    public void setEnableTargetShift(String mode, boolean v) { cfgFor(mode).enableTargetShift = v; }
    public void setEnableTrailingSl(String mode, boolean v) { cfgFor(mode).enableTrailingSl = v; }

    // ── save ──────────────────────────────────────────────────────────────────
    /** Saves the current settings. */
    public void save() {
        saveFor("live");
    }

    /** Saves the specified mode's settings. */
    public void saveFor(String mode) {
        Cfg c = cfgFor(mode);
        try {
            upsert("tradingStartTime", c.tradingStartTime);
            upsert("tradingEndTime", c.tradingEndTime);
            upsert("totalCapital", String.valueOf(c.totalCapital));
            upsert("maxRiskPerDayPct", String.valueOf(c.maxRiskPerDayPct));
            upsert("riskPerTrade", String.valueOf(c.riskPerTrade));
            upsert("autoSquareOffTime", c.autoSquareOffTime);
            upsert("atrMultiplier", String.valueOf(c.atrMultiplier));
            upsert("brokeragePerOrder", String.valueOf(c.brokeragePerOrder));
            upsert("sttRate", String.valueOf(c.sttRate));
            upsert("exchangeRate", String.valueOf(c.exchangeRate));
            upsert("gstRate", String.valueOf(c.gstRate));
            upsert("sebiRate", String.valueOf(c.sebiRate));
            upsert("stampDutyRate", String.valueOf(c.stampDutyRate));
            upsert("brokeragePct", String.valueOf(c.brokeragePct));
            upsert("fixedQuantity", String.valueOf(c.fixedQuantity));
            upsert("capitalPerTrade", String.valueOf(c.capitalPerTrade));
            upsert("telegramAlertFrequency", String.valueOf(c.telegramAlertFrequency));
            upsert("enableGapCheck", String.valueOf(c.enableGapCheck));
            upsert("enableWeeklyLevelTargetShift", String.valueOf(c.enableWeeklyLevelTargetShift));
            upsert("enableHtfHurdleFilter", String.valueOf(c.enableHtfHurdleFilter));
            upsert("htfHurdleMinHeadroomAtr", String.valueOf(c.htfHurdleMinHeadroomAtr));
            upsert("enableDailyAtrExhaustionFilter", String.valueOf(c.enableDailyAtrExhaustionFilter));
            upsert("dailyAtrExhaustionMult",         String.valueOf(c.dailyAtrExhaustionMult));
            upsert("enableOpenRangeFilter", String.valueOf(c.enableOpenRangeFilter));
            upsert("openRangeMinutes",      String.valueOf(c.openRangeMinutes));
            upsert("enableIndexOpenRangeFilter", String.valueOf(c.enableIndexOpenRangeFilter));
            upsert("marubozuBreakoutBodyAtrMult",             String.valueOf(c.marubozuBreakoutBodyAtrMult));
            upsert("marubozuBreakoutMaxBodyAtrMult",          String.valueOf(c.marubozuBreakoutMaxBodyAtrMult));
            upsert("marubozuBreakoutMaxOppositeWickPctOfBody", String.valueOf(c.marubozuBreakoutMaxOppositeWickPctOfBody));
            upsert("enableIndexHtfHurdleFilter", String.valueOf(c.enableIndexHtfHurdleFilter));
            upsert("enableIndexHtfAlignment",    String.valueOf(c.enableIndexHtfAlignment));
            upsert("indexHtfHurdleMinHeadroomAtr", String.valueOf(c.indexHtfHurdleMinHeadroomAtr));
            upsert("enableIndex5mHurdleFilter", String.valueOf(c.enableIndex5mHurdleFilter));
            upsert("index5mHurdleMinHeadroomAtr", String.valueOf(c.index5mHurdleMinHeadroomAtr));
            upsert("enableStructuralSl", String.valueOf(c.enableStructuralSl));
            upsert("structuralSlBufferAtr", String.valueOf(c.structuralSlBufferAtr));
            upsert("singleLevelSlBufferAtr", String.valueOf(c.singleLevelSlBufferAtr));
            upsert("dayHighLowMinAtr", String.valueOf(c.dayHighLowMinAtr));
            upsert("enableRiskRewardFilter", String.valueOf(c.enableRiskRewardFilter));
            upsert("minRiskRewardRatio", String.valueOf(c.minRiskRewardRatio));
            upsert("enableEmaLevelCountFilter", String.valueOf(c.enableEmaLevelCountFilter));
            upsert("emaLevelMinRangePct", String.valueOf(c.emaLevelMinRangePct));
            upsert("emaLevelFilterMorningSkip", String.valueOf(c.emaLevelFilterMorningSkip));
            upsert("emaLevelFilterMorningSkipUntil", c.emaLevelFilterMorningSkipUntil);
            upsert("enableTargetShift", String.valueOf(c.enableTargetShift));
            upsert("goodSizeCandleBodyAtrMult",    String.valueOf(c.goodSizeCandleBodyAtrMult));
            upsert("goodSizeCandleMaxBodyAtrMult", String.valueOf(c.goodSizeCandleMaxBodyAtrMult));
            upsert("confirmationMaxOppositeWickRatio", String.valueOf(c.confirmationMaxOppositeWickRatio));
            upsert("pinBarDominantWickMinRangeRatio", String.valueOf(c.pinBarDominantWickMinRangeRatio));
            upsert("pinBarOppositeWickMaxRangeRatio", String.valueOf(c.pinBarOppositeWickMaxRangeRatio));
            upsert("outsideReversalMinBodyAtrMult", String.valueOf(c.outsideReversalMinBodyAtrMult));
            upsert("outsideReversalMaxBodyAtrMult", String.valueOf(c.outsideReversalMaxBodyAtrMult));
            upsert("outsideReversalPenetrationPct", String.valueOf(c.outsideReversalPenetrationPct));
            upsert("haramiBodyAtrMult",        String.valueOf(c.haramiBodyAtrMult));
            upsert("haramiBodyMaxAtrMult",     String.valueOf(c.haramiBodyMaxAtrMult));
            upsert("haramiBar3PenetrationPct", String.valueOf(c.haramiBar3PenetrationPct));
            upsert("dojiBodyMaxRangeRatio", String.valueOf(c.dojiBodyMaxRangeRatio));
            upsert("dojiConfirmBodyAtrMult",    String.valueOf(c.dojiConfirmBodyAtrMult));
            upsert("dojiConfirmMaxBodyAtrMult", String.valueOf(c.dojiConfirmMaxBodyAtrMult));
            upsert("starOuterBodyAtrMult",      String.valueOf(c.starOuterBodyAtrMult));
            upsert("starOuterMaxBodyAtrMult",   String.valueOf(c.starOuterMaxBodyAtrMult));
            upsert("starMiddleBodyMaxMultOfOuter", String.valueOf(c.starMiddleBodyMaxMultOfOuter));
            upsert("starBar3PenetrationPct",       String.valueOf(c.starBar3PenetrationPct));
            upsert("levelTouchToleranceAtr", String.valueOf(c.levelTouchToleranceAtr));
            upsert("entryProximityAtrMult", String.valueOf(c.entryProximityAtrMult));
            upsert("enableTrailingSl", String.valueOf(c.enableTrailingSl));
            upsert("enablePriceEmaExit", String.valueOf(c.enablePriceEmaExit));
            upsert("virginCprExpiryDays", String.valueOf(c.virginCprExpiryDays));
            upsert("breakevenTriggerPct", String.valueOf(c.breakevenTriggerPct));
            upsert("breakevenSlAtrMult",  String.valueOf(c.breakevenSlAtrMult));
            upsert("skipR1PdhS1Pdl", String.valueOf(c.skipR1PdhS1Pdl));
            upsert("skipR2S2", String.valueOf(c.skipR2S2));
            upsert("skipR3S3", String.valueOf(c.skipR3S3));
            upsert("skipR4S4", String.valueOf(c.skipR4S4));
            upsert("enableMeanReversionTrades", String.valueOf(c.enableMeanReversionTrades));
            upsert("enableMagnetTrades",        String.valueOf(c.enableMagnetTrades));
            upsert("magnetTradesQtyFactor",     String.valueOf(c.magnetTradesQtyFactor));
            upsert("meanReversionQtyFactor",    String.valueOf(c.meanReversionQtyFactor));
            upsert("atrPeriod", String.valueOf(c.atrPeriod));
            upsert("signalSource", c.signalSource);
            upsert("scannerTimeframe", String.valueOf(c.scannerTimeframe));
            upsert("higherTimeframe", String.valueOf(c.higherTimeframe));
            upsert("enableHpt", String.valueOf(c.enableHpt));
            upsert("enableMpt", String.valueOf(c.enableMpt));
            upsert("mptQtyFactor", String.valueOf(c.mptQtyFactor));
            upsert("minAbsoluteProfit", String.valueOf(c.minAbsoluteProfit));
            upsert("cprWidthSqueezeMult",  String.valueOf(c.cprWidthSqueezeMult));
            upsert("cprWidthWideMult",     String.valueOf(c.cprWidthWideMult));
            upsert("enableCprStateA",      String.valueOf(c.enableCprStateA));
            upsert("enableCprStateB",      String.valueOf(c.enableCprStateB));
            upsert("enableCprStateC",      String.valueOf(c.enableCprStateC));
            // narrowRangeRatioThreshold removed — z-score is self-calibrating
            upsert("insideCprMaxWidth", String.valueOf(c.insideCprMaxWidth));
            upsert("narrowCprZoneCollapseWidthPct", String.valueOf(c.narrowCprZoneCollapseWidthPct));
            upsert("scanMinPrice", String.valueOf(c.scanMinPrice));
            upsert("scanMaxPrice", String.valueOf(c.scanMaxPrice));
            upsert("enableOpeningRefresh", String.valueOf(c.enableOpeningRefresh));
            upsert("openingRefreshTime", c.openingRefreshTime);
            upsert("enableTargetTolerance", String.valueOf(c.enableTargetTolerance));
            upsert("targetToleranceAtr", String.valueOf(c.targetToleranceAtr));
            upsert("enableIndexAlignment",   String.valueOf(c.enableIndexAlignment));
            upsert("enableStockHtfAlignment", String.valueOf(c.enableStockHtfAlignment));
            upsert("enableStockHtf1hEma20Check", String.valueOf(c.enableStockHtf1hEma20Check));
        } catch (Exception e) {
            log.error("[RiskSettingsStore] Failed to save {}: {}", mode, e.getMessage());
        }
    }

    private void upsert(String key, String value) {
        SettingEntity entity = settingRepo.findBySettingKey(key).orElse(new SettingEntity(key, value));
        entity.setSettingValue(value);
        settingRepo.save(entity);
    }

    // ── load ──────────────────────────────────────────────────────────────────
    private void load(String mode) {
        try {
            List<SettingEntity> all = settingRepo.findAll();
            if (all.isEmpty()) return;
            Cfg c = cfgFor(mode);
            for (SettingEntity s : all) {
                String k = s.getSettingKey();
                String v = s.getSettingValue();
                if (v == null) continue;
                switch (k) {
                    case "tradingStartTime"  -> c.tradingStartTime = v;
                    case "tradingEndTime"    -> c.tradingEndTime = v;
                    case "totalCapital"      -> c.totalCapital = Double.parseDouble(v);
                    case "maxRiskPerDayPct"  -> c.maxRiskPerDayPct = Double.parseDouble(v);
                    case "riskPerTrade"      -> c.riskPerTrade = Double.parseDouble(v);
                    case "autoSquareOffTime" -> c.autoSquareOffTime = v;
                    case "atrMultiplier"     -> c.atrMultiplier = Double.parseDouble(v);
                    // enableSessionMoveLimit / sessionMoveLimit removed — feature deleted.
                    case "enableSessionMoveLimit", "sessionMoveLimit" -> { /* legacy, ignored */ }
                    case "brokeragePerOrder" -> c.brokeragePerOrder = Double.parseDouble(v);
                    case "sttRate"           -> c.sttRate = Double.parseDouble(v);
                    case "exchangeRate"      -> c.exchangeRate = Double.parseDouble(v);
                    case "gstRate"           -> c.gstRate = Double.parseDouble(v);
                    case "sebiRate"          -> c.sebiRate = Double.parseDouble(v);
                    case "stampDutyRate"     -> c.stampDutyRate = Double.parseDouble(v);
                    case "brokeragePct"      -> c.brokeragePct = Double.parseDouble(v);
                    case "fixedQuantity"     -> c.fixedQuantity = Integer.parseInt(v);
                    case "capitalPerTrade"   -> c.capitalPerTrade = Double.parseDouble(v);
                    case "telegramAlertFrequency" -> c.telegramAlertFrequency = Integer.parseInt(v);
                    // enableLargeCandleFilter / largeCandleAtrThreshold removed — legacy keys silently ignored
                    case "enableGapCheck" -> c.enableGapCheck = Boolean.parseBoolean(v);
                    case "enableWeeklyLevelTargetShift" -> c.enableWeeklyLevelTargetShift = Boolean.parseBoolean(v);
                    // Silently ignored legacy keys (removed features): enableTargetRescue,
                    // enableDayHighLowTargetShift, enableDailySma200TargetShift,
                    // dayHighLowShiftMinDistAtr, enableSplitTarget, t1DistancePct,
                    // splitMinDistanceAtr. Old JSON files round-trip without errors.
                    case "enableHtfHurdleFilter" -> c.enableHtfHurdleFilter = Boolean.parseBoolean(v);
                    case "htfHurdleMinHeadroomAtr" -> c.htfHurdleMinHeadroomAtr = Math.max(0, Double.parseDouble(v));
                    case "enableDailyAtrExhaustionFilter" -> c.enableDailyAtrExhaustionFilter = Boolean.parseBoolean(v);
                    case "dailyAtrExhaustionMult"        -> c.dailyAtrExhaustionMult         = Math.max(0, Double.parseDouble(v));
                    case "enableOpenRangeFilter" -> c.enableOpenRangeFilter = Boolean.parseBoolean(v);
                    case "openRangeMinutes"      -> c.openRangeMinutes      = Math.max(5, Math.min(240, Integer.parseInt(v)));
                    case "enableIndexOpenRangeFilter" -> c.enableIndexOpenRangeFilter = Boolean.parseBoolean(v);
                    // enableMarubozuBreakout legacy key — silently ignored; Marubozu is now always on.
                    case "marubozuBreakoutBodyAtrMult"             -> c.marubozuBreakoutBodyAtrMult           = Math.max(0, Double.parseDouble(v));
                    case "marubozuBreakoutMaxBodyAtrMult"          -> c.marubozuBreakoutMaxBodyAtrMult        = Math.max(0, Double.parseDouble(v));
                    case "marubozuBreakoutMaxOppositeWickPctOfBody" -> c.marubozuBreakoutMaxOppositeWickPctOfBody = Math.max(0, Double.parseDouble(v));
                    case "enableIndexHtfHurdleFilter" -> c.enableIndexHtfHurdleFilter = Boolean.parseBoolean(v);
                    case "enableIndexHtfAlignment"    -> c.enableIndexHtfAlignment    = Boolean.parseBoolean(v);
                    case "indexHtfHurdleMinHeadroomAtr" -> c.indexHtfHurdleMinHeadroomAtr = Math.max(0, Double.parseDouble(v));
                    case "enableIndex5mHurdleFilter" -> c.enableIndex5mHurdleFilter = Boolean.parseBoolean(v);
                    case "index5mHurdleMinHeadroomAtr" -> c.index5mHurdleMinHeadroomAtr = Math.max(0, Double.parseDouble(v));
                    case "enableStructuralSl"    -> c.enableStructuralSl = Boolean.parseBoolean(v);
                    case "structuralSlBufferAtr" -> c.structuralSlBufferAtr = Double.parseDouble(v);
                    case "singleLevelSlBufferAtr" -> c.singleLevelSlBufferAtr = Double.parseDouble(v);
                    case "dayHighLowMinAtr" -> c.dayHighLowMinAtr = Double.parseDouble(v);
                    case "enableRiskRewardFilter" -> c.enableRiskRewardFilter = Boolean.parseBoolean(v);
                    case "minRiskRewardRatio" -> c.minRiskRewardRatio = Double.parseDouble(v);
                    // enableEmaTrendCheck removed — 5-min EMA factor is hard-baked into the
                    // strict trend state machine in BreakoutScanner. Legacy keys silently ignored.
                    case "enableEmaTrendCheck",
                         "enableSmaTrendCheck",
                         "enableSmaTrendCheckLenient",
                         "enableSmaAlignmentCheck",
                         "enableSmaAlignmentCheckLenient",
                         "enableEmaDirectionCheck", "enableEma200DirectionCheck", "enableEmaCrossoverCheck",
                         "emaPatternLookback", "smaPatternLookback",
                         "braidedMinCrossovers", "braidedMaxSpreadAtr",
                         "railwayMaxCv", "railwayMinSpreadAtr", "railwayMinSlopeAtr",
                         "requireRtpPattern", "buyRequiresRrtp", "sellRequiresFrtp",
                         "skipTradesInZigZag", "allowTradesInZigZag",
                         "emaCloseDistanceAtr", "smaCloseDistanceAtr" -> { /* legacy — SMA50/200/pattern removed */ }
                    // VWAP confirmation (ATP) filter removed — drop legacy keys silently.
                    case "enableEmaVsAtpCheck", "enableSmaVsAtpCheck" -> { /* drop */ }
                    case "enableEmaLevelCountFilter", "enableSmaLevelCountFilter" -> {
                        c.enableEmaLevelCountFilter = Boolean.parseBoolean(v);
                        if ("enableSmaLevelCountFilter".equals(k)) logLegacyOnce("enableSmaLevelCountFilter");
                    }
                    case "emaLevelMinRangePct", "smaLevelMinRangePct" -> {
                        c.emaLevelMinRangePct = Math.max(0, Math.min(100, Integer.parseInt(v)));
                        if ("smaLevelMinRangePct".equals(k)) logLegacyOnce("smaLevelMinRangePct");
                    }
                    case "emaLevelFilterMorningSkip", "smaLevelFilterMorningSkip" -> {
                        c.emaLevelFilterMorningSkip = Boolean.parseBoolean(v);
                        if ("smaLevelFilterMorningSkip".equals(k)) logLegacyOnce("smaLevelFilterMorningSkip");
                    }
                    case "emaLevelFilterMorningSkipUntil", "smaLevelFilterMorningSkipUntil" -> {
                        if (v != null && !v.isEmpty()) c.emaLevelFilterMorningSkipUntil = v;
                        if ("smaLevelFilterMorningSkipUntil".equals(k)) logLegacyOnce("smaLevelFilterMorningSkipUntil");
                    }
                    case "enableTargetShift" -> c.enableTargetShift = Boolean.parseBoolean(v);
                    case "enableLargeCandleBodyFilter",
                         "largeCandleBodyAtrThreshold",
                         "marubozuBodyAtrMult",
                         "marubozuMaxBodyAtrMult",
                         "marubozuMaxWicksPctOfBody" -> { /* legacy keys — patterns retired; ignored on load */ }
                    case "goodSizeCandleBodyAtrMult"          -> c.goodSizeCandleBodyAtrMult = Double.parseDouble(v);
                    case "goodSizeCandleMaxBodyAtrMult"       -> c.goodSizeCandleMaxBodyAtrMult = Double.parseDouble(v);
                    // Shared opposing-wick cap. Accepts the new name + the legacy good-size
                    // key so existing risk-settings.json files keep loading without losing the value.
                    case "confirmationMaxOppositeWickRatio",
                         "goodSizeCandleMaxOppositeWickRatio" -> c.confirmationMaxOppositeWickRatio = Double.parseDouble(v);
                    // Legacy pin bar settings — retired in favor of the single range-relative path.
                    case "pinBarRejectionWickBodyMult",
                         "pinBarOppositeWickBodyMult",
                         "pinBarSmallBodyMaxRangeRatio" -> { /* ignored — field removed */ }
                    case "pinBarDominantWickMinRangeRatio" -> c.pinBarDominantWickMinRangeRatio = Double.parseDouble(v);
                    case "pinBarOppositeWickMaxRangeRatio" -> c.pinBarOppositeWickMaxRangeRatio = Double.parseDouble(v);
                    case "engulfingMinBodyMultiple"    -> { /* legacy — redundant with bar 2 penetration check */ }
                    case "outsideReversalMinBodyAtrMult" -> c.outsideReversalMinBodyAtrMult = Double.parseDouble(v);
                    case "outsideReversalMaxBodyAtrMult" -> c.outsideReversalMaxBodyAtrMult = Double.parseDouble(v);
                    case "outsideReversalPenetrationPct" -> c.outsideReversalPenetrationPct = Double.parseDouble(v);
                    // Legacy keys from earlier pattern variants — each maps onto the closest
                    // outsideReversal* equivalent so older risk-settings.json files keep working.
                    case "engulfingMinBodyAtrMult",
                         "piercingMinBodyAtrMult",
                         "tweezerMinBodyAtrMult",
                         "piercingPrevBodyAtrMult",
                         "tweezerPrevBodyAtrMult"        -> c.outsideReversalMinBodyAtrMult = Double.parseDouble(v);
                    case "engulfingMaxBodyAtrMult",
                         "piercingMaxBodyAtrMult",
                         "tweezerMaxBodyAtrMult"         -> c.outsideReversalMaxBodyAtrMult = Double.parseDouble(v);
                    case "engulfingBar2PenetrationPct",
                         "piercingPenetrationPct"        -> c.outsideReversalPenetrationPct = Double.parseDouble(v);
                    case "tweezerLowHighMatchAtr"        -> { /* legacy — discriminator no longer applicable */ }
                    case "engulfingPrevMinBodyAtrMult",
                         "engulfingPrevMaxBodyAtrMult" -> { /* legacy — folded into the shared engulfingMin/MaxBodyAtrMult band */ }
                    case "haramiBodyAtrMult"           -> c.haramiBodyAtrMult = Double.parseDouble(v);
                    case "haramiBodyMaxAtrMult"        -> c.haramiBodyMaxAtrMult = Double.parseDouble(v);
                    case "haramiInnerBodyMaxRatio"     -> { /* legacy — bar 2 now containment-only */ }
                    case "haramiBar3PenetrationPct"    -> c.haramiBar3PenetrationPct = Double.parseDouble(v);
                    case "haramiConfirmBodyAtrMult",
                         "haramiConfirmMaxBodyAtrMult" -> { /* legacy — folded into the shared haramiBody*AtrMult band */ }
                    case "dojiBodyMaxRangeRatio"       -> c.dojiBodyMaxRangeRatio = Double.parseDouble(v);
                    case "dojiConfirmBodyAtrMult"      -> c.dojiConfirmBodyAtrMult = Double.parseDouble(v);
                    case "dojiConfirmMaxBodyAtrMult"   -> c.dojiConfirmMaxBodyAtrMult = Double.parseDouble(v);
                    case "dojiPrevBodyAtrMult"         -> c.dojiConfirmBodyAtrMult = Double.parseDouble(v); // legacy key
                    case "starOuterBodyAtrMult"        -> c.starOuterBodyAtrMult = Double.parseDouble(v);
                    case "starOuterMaxBodyAtrMult"     -> c.starOuterMaxBodyAtrMult = Double.parseDouble(v);
                    case "starMiddleBodyMaxMultOfOuter" -> c.starMiddleBodyMaxMultOfOuter = Double.parseDouble(v);
                    case "starBar3PenetrationPct"       -> c.starBar3PenetrationPct = Double.parseDouble(v);
                    case "levelTouchToleranceAtr"      -> c.levelTouchToleranceAtr = Math.max(0, Double.parseDouble(v));
                    case "entryProximityAtrMult"       -> c.entryProximityAtrMult = Math.max(0, Double.parseDouble(v));
                    // Legacy small-candle / volume filter keys — features removed.
                    case "enableSmallCandleFilter",
                         "smallCandleAtrThreshold",
                         "smallCandleBodyAtrThreshold",
                         "smallCandleMoveAtrThreshold",
                         "wickRejectionRatio",
                         "enableVolumeFilter",
                         "volumeMultiple",
                         "volumeLookback" -> { /* legacy — removed */ }
                    case "enableTrailingSl"   -> c.enableTrailingSl = Boolean.parseBoolean(v);
                    case "enableSmaCrossExit" -> { /* legacy — SMA cross exit removed */ }
                    case "enablePriceEmaExit", "enablePriceSmaExit" -> {
                        c.enablePriceEmaExit = Boolean.parseBoolean(v);
                        if ("enablePriceSmaExit".equals(k)) logLegacyOnce("enablePriceSmaExit");
                    }
                    // Legacy exit toggles + per-symbol trade limit — features removed.
                    case "enableNiftyReversalCprExit",
                         "enableNiftyHtfHurdleExit",
                         "enableVirginCprTouchExit",
                         "perSymbolDailyTradeLimit",
                         "lptMaxTradesPerStockPerDay" -> { /* legacy — removed */ }
                    case "virginCprExpiryDays" -> c.virginCprExpiryDays = Math.max(0, Integer.parseInt(v));
                    // Virgin CPR Hurdle Filter removed — drop legacy keys silently.
                    case "enableVirginCprHurdleFilter", "virginCprHurdleHeadroomAtr" -> { /* drop */ }
                    case "breakevenTriggerPct" -> c.breakevenTriggerPct = Double.parseDouble(v);
                    case "breakevenSlAtrMult"  -> c.breakevenSlAtrMult  = Double.parseDouble(v);
                    // Legacy fib-stage keys — fold old stage-1 values into the new breakeven
                    // knobs (same semantics) and silently ignore stage-2 (feature removed).
                    case "fibStage1TriggerPct" -> c.breakevenTriggerPct = Double.parseDouble(v);
                    case "fibStage1SlAtrMult"  -> c.breakevenSlAtrMult  = Double.parseDouble(v);
                    case "fibStage2TriggerPct", "fibStage2SlPct" -> { /* legacy — stage 2 removed */ }
                    // Legacy day-type-split keys (NormalDays / IvOvDays / EvDays) silently
                    // fold into the new single-toggle pair: any legacy-true value flips the
                    // new toggle to true on load (preserving the user's prior intent — if
                    // they had ANY of the splits set to skip, the consolidated toggle stays
                    // on). Rewritten on next save as just skipR3S3 / skipR4S4.
                    case "skipR3S3NormalDays", "skipR3S3IvOvDays", "skipR3S3EvDays" -> {
                        if (Boolean.parseBoolean(v)) c.skipR3S3 = true;
                    }
                    case "skipR4S4NormalDays", "skipR4S4IvOvDays", "skipR4S4EvDays" -> {
                        if (Boolean.parseBoolean(v)) c.skipR4S4 = true;
                    }
                    case "skipR1PdhS1Pdl" -> c.skipR1PdhS1Pdl = Boolean.parseBoolean(v);
                    case "skipR2S2" -> c.skipR2S2 = Boolean.parseBoolean(v);
                    case "skipR3S3" -> c.skipR3S3 = Boolean.parseBoolean(v);
                    case "skipR4S4" -> c.skipR4S4 = Boolean.parseBoolean(v);
                    case "enableMeanReversionTrades" -> c.enableMeanReversionTrades = Boolean.parseBoolean(v);
                    case "enableMagnetTrades"        -> c.enableMagnetTrades        = Boolean.parseBoolean(v);
                    case "magnetTradesQtyFactor"     -> c.magnetTradesQtyFactor     = Double.parseDouble(v);
                    case "meanReversionQtyFactor"    -> c.meanReversionQtyFactor    = Double.parseDouble(v);
                    case "atrPeriod" -> c.atrPeriod = Integer.parseInt(v);
                    case "signalSource"      -> c.signalSource = v;
                    case "scannerTimeframe"  -> c.scannerTimeframe = Integer.parseInt(v);
                    case "higherTimeframe"   -> c.higherTimeframe = Integer.parseInt(v);
                    // VWAP confirmation (ATP) filter removed — drop legacy key silently.
                    case "enableAtpCheck"    -> { /* drop */ }
                    case "enableHpt"         -> c.enableHpt = Boolean.parseBoolean(v);
                    case "enableLpt", "lptQtyFactor" -> { /* legacy — LPT tier removed */ }
                    case "enableMpt"         -> c.enableMpt = Boolean.parseBoolean(v);
                    case "mptQtyFactor"      -> c.mptQtyFactor = Double.parseDouble(v);
                    case "neutralWeeklyQtyFactor" -> { /* removed — weekly NEUTRAL no longer downgraded */ }
                    case "enableWeeklyNeutralTrades" -> { /* removed — weekly NEUTRAL no longer downgraded */ }
                    case "insideOrQtyFactor", "skipInsideOrOnEv", "skipInsideOrOnIv", "skipInsideOrOnOv" -> {
                        /* removed — inside-OR special-cases gone */
                    }
                    case "enableNarrowOrOverride", "narrowOrMaxAdrPct", "narrowOrMaxAtr" -> {
                        /* removed — narrow OR override feature deleted */
                    }
                    case "enableCprDayRelationFilter" -> { /* removed — 2D-CPR feature deleted */ }
                    case "minAbsoluteProfit" -> c.minAbsoluteProfit = Double.parseDouble(v);
                    // Legacy keys — silently ignored on load (current adaptive classifier
                    // is width-only with a 20-day baseline; legacy band + TR multiplier dropped).
                    case "narrowCprMaxWidth", "narrowCprMinWidth", "trueRangeSqueezeMult" -> { /* drop */ }
                    case "cprWidthSqueezeMult"  -> c.cprWidthSqueezeMult  = Math.max(0, Double.parseDouble(v));
                    case "cprWidthWideMult"     -> c.cprWidthWideMult     = Math.max(0, Double.parseDouble(v));
                    case "enableCprStateA"      -> c.enableCprStateA = Boolean.parseBoolean(v);
                    case "enableCprStateB"      -> c.enableCprStateB = Boolean.parseBoolean(v);
                    case "enableCprStateC"      -> c.enableCprStateC = Boolean.parseBoolean(v);
                    // narrowRangeRatioThreshold — legacy key, silently ignored
                    case "insideCprMaxWidth" -> c.insideCprMaxWidth = Double.parseDouble(v);
                    case "narrowCprZoneCollapseWidthPct" -> c.narrowCprZoneCollapseWidthPct = Double.parseDouble(v);
                    // Legacy watchlist gate — removed. DB Stock Universe is the source of truth.
                    case "scanUniverse" -> { /* ignored — field removed */ }
                    case "scanMinPrice" -> c.scanMinPrice = Double.parseDouble(v);
                    case "scanMaxPrice" -> c.scanMaxPrice = Double.parseDouble(v);
                    // Legacy watchlist + OR keys — features removed.
                    case "scanMinTurnover", "scanMinVolume",
                         "scanMinBeta", "scanMaxBeta",
                         "scanCapFilter",
                         "openingRangeMinutes" -> { /* legacy — removed */ }
                    case "scanOnlyNifty50" -> { /* ignored — field removed, DB Stock Universe gates the watchlist */ }
                    case "enableOpeningRefresh" -> c.enableOpeningRefresh = Boolean.parseBoolean(v);
                    case "openingRefreshTime" -> c.openingRefreshTime = v;
                    case "enableTargetTolerance" -> c.enableTargetTolerance = Boolean.parseBoolean(v);
                    case "targetToleranceAtr" -> c.targetToleranceAtr = Double.parseDouble(v);
                    case "enableIndexAlignment"   -> c.enableIndexAlignment = Boolean.parseBoolean(v);
                    case "enableStockHtfAlignment" -> c.enableStockHtfAlignment = Boolean.parseBoolean(v);
                    case "enableStockHtf1hEma20Check" -> c.enableStockHtf1hEma20Check = Boolean.parseBoolean(v);
                    // Legacy NIFTY trend-factor toggles — features removed (EMA20 is now
                    // always-on; FUT VWAP factor dropped entirely). Old risk-settings.json
                    // files keep loading without errors; values are silently ignored.
                    case "enableNiftyEma20Factor", "enableNiftySma20Factor",
                         "enableNiftyFutVwapFactor" -> { /* legacy — removed */ }
                    case "indexAlignmentHardSkip", "indexOpposedQtyFactor" -> { /* removed — soft mode deleted */ }
                }
            }
            log.info("[RiskSettingsStore] Loaded {}: start={} end={} totalCapital={} maxRiskPerDayPct={}% riskPerTrade={} autoSquareOff={} atrMult={} brokerage={} fixedQty={} capitalPerTrade={} trailingSl={} skipR3S3={} skipR4S4={}", mode, c.tradingStartTime, c.tradingEndTime, c.totalCapital, c.maxRiskPerDayPct, c.riskPerTrade, c.autoSquareOffTime, c.atrMultiplier, c.brokeragePerOrder, c.fixedQuantity, c.capitalPerTrade, c.enableTrailingSl, c.skipR3S3, c.skipR4S4);
        } catch (Exception e) {
            log.error("[RiskSettingsStore] Failed to load {}: {}", mode, e.getMessage());
        }
    }

    // ── Legacy-key migration logging ──────────────────────────────────────────
    // Tracks which legacy SMA-named keys we've already announced this process so the SMA→EMA
    // rename migration logs each key exactly once instead of spamming the log per load() call.
    private final java.util.Set<String> loggedLegacyKeys = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private void logLegacyOnce(String legacyKey) {
        if (loggedLegacyKeys.add(legacyKey)) {
            log.info("[RiskSettingsStore] Migrated legacy SMA setting key '{}' → new EMA-named field (will rewrite on next save)", legacyKey);
        }
    }
}
