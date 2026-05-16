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
        // Pin bar (hammer / shooting star): rejection wick ≥ N × body, opposite wick ≤ N × body.
        volatile double pinBarRejectionWickBodyMult = 2.0;
        volatile double pinBarOppositeWickBodyMult  = 0.30;
        // Pin bar small-body fallback — when body ≤ smallBodyMaxRangeRatio × range, the
        // body-relative test loses meaning (a tiny body makes the wick multiplicative cap
        // collapse). Fall back to a range-relative geometric test: rejection wick must dominate
        // the bar's total range, and the opposite wick must stay capped relative to range.
        // Set smallBodyMaxRangeRatio = 0 to disable the fallback entirely.
        volatile double pinBarSmallBodyMaxRangeRatio    = 0.25;
        volatile double pinBarDominantWickMinRangeRatio = 0.60;
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
        // NIFTY-level macro hurdle. When on, skip ALL stock trades while NIFTY's prior 1h close
        // hasn't decisively cleared the nearest weekly hurdle in the trade direction (R1/PWH/
        // weekly TC/Pivot/BC for buys; S1/PWL/... for sells). Mirrors per-stock HTF Hurdle
        // applied to NIFTY's own data. Default off — opt-in.
        volatile boolean enableNiftyHtfHurdleFilter = false;
        // Headroom check for the NIFTY HTF Hurdle filter. When > 0, trades are also rejected if
        // the nearest hurdle in the OPPOSITE direction (above NIFTY LTP for buys / below for
        // sells) is closer than this many NIFTY ATRs. Guards against firing when an upcoming
        // weekly level is right above NIFTY (likely to cap the move). 0 = headroom check off.
        volatile double niftyHurdleMinHeadroomAtr = 1.0;
        // 5-min variant of the NIFTY HTF Hurdle filter, against NIFTY's *daily* CPR levels.
        // When on, every stock breakout requires NIFTY's prior 5-min close to have cleared
        // its nearest daily-CPR hurdle in trade direction (R1/R2/R3/R4 + daily TC/Pivot/BC for
        // buys; S1/S2/S3/S4 + TC/Pivot/BC for sells). Default off.
        volatile boolean enableNifty5mHurdleFilter = false;
        // Headroom check for the NIFTY 5m Hurdle filter — mirror of niftyHurdleMinHeadroomAtr
        // but applied to the 5m filter's daily-CPR candidate set. Trades are also rejected
        // when the nearest hurdle in the OPPOSITE direction (above NIFTY LTP for buys / below
        // for sells) is closer than this many NIFTY ATRs. 0 = headroom check off.
        volatile double nifty5mHurdleMinHeadroomAtr = 1.0;
        // In-progress 1h candle direction must agree with the 5-min breakout direction. Buy
        // requires the currently-forming 1h bar to be green (close > open); sell requires red
        // (close < open). Doji passes both. Fail-open if no in-progress bar yet. Default off.
        volatile boolean enableHtfCandleFilter = false;
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
        // Matches the BULL/BEAR chip on scanner cards. Fail-open if EMA not loaded.
        volatile boolean enableEmaTrendCheck = true;
        volatile boolean enableEmaVsAtpCheck = true; // buy requires 20 EMA > ATP (VWAP), sell requires 20 EMA < ATP
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
        // Virgin CPR Hurdle filter — treats the active virgin CPR as a zone (BC..TC). Rejects
        // all stock signals when NIFTY's prior 5m close is inside the zone, and rejects in the
        // trade direction when the close is within virginCprHurdleHeadroomAtr × NIFTY ATR of
        // the zone edge. Default off — opt-in.
        volatile boolean enableVirginCprHurdleFilter = false;
        volatile double  virginCprHurdleHeadroomAtr  = 1.0;
        // Breakeven SL — single-stage move. When the peak (long) / trough (short) reaches
        // {@code breakevenTriggerPct}% of the range from entry to target, the SL is moved to
        // {@code entry ± breakevenSlAtrMult × ATR}. Once moved, it stays put (never widens).
        volatile double breakevenTriggerPct = 61.8;   // peak/trough hits this % of (entry→target) range
        volatile double breakevenSlAtrMult  = 1.0;    // new SL = entry ± N × ATR
        // Extended-level breakouts (R3/S3, R4/S4) are skipped on normal IV/OV days but allowed
        // on EV (gap up/down) days regardless. Toggles default ON (skip on normal days).
        // Daily extended-level skip — split by day type. IV/OV = open print inside CPR or
        // between CPR and R2/S2; EV = open print outside R2/S2 (gap). Defaults preserve the
        // previous "skip on all days" behavior. The legacy keys skipR3S3NormalDays /
        // skipR4S4NormalDays are still read by load() for backward compat (seed both new
        // fields with the same value), then dropped from the rewritten JSON on next save.
        volatile boolean skipR3S3IvOvDays = true;
        volatile boolean skipR3S3EvDays   = true;
        volatile boolean skipR4S4IvOvDays = true;
        volatile boolean skipR4S4EvDays   = true;
        // HTF (weekly) extended-level skips — independent of the daily extended-level skips
        // above. When on, breakout close past weekly R3/R4 (buys) or S3/S4 (sells) is skipped
        // regardless of the daily setup. Default true (matches daily skip stance).
        volatile boolean skipHtfR3S3NormalDays = true;
        volatile boolean skipHtfR4S4NormalDays = true;
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
        volatile boolean enableAtpCheck = true; // require ATP confirmation for scanner signals
        volatile boolean enableHpt      = true;  // High Probable Trade signals (weekly+daily aligned)
        // Medium Probable Trade — produced by static counter-trend setups (S1+PDL/S2-S4 buys,
        // R1+PDH/R2-R4 sells). Trades at reduced qty via mptQtyFactor.
        volatile boolean enableMpt      = true;
        volatile double mptQtyFactor    = 0.75;
        volatile double minAbsoluteProfit = 500; // skip if qty × target_distance < this amount (₹)
        // CPR Width scanner group toggles
        volatile double narrowCprMaxWidth = 0.1;  // CPR width % upper threshold for narrow CPR stocks
        // Narrow-CPR single-level SL buffer threshold. When CPR width % is below this
        // value, the three zone setups (CPR, R1+PDH, S1+PDL) get the same extra
        // singleLevelSlBufferAtr cushion that pure single-level setups (R2/R3/R4, S2/S3/S4)
        // already receive — narrow CPR means the zone-width cushion is too small to absorb
        // a normal pullback, so we widen the SL the same way as for a pure single level.
        // Breakout detection and zone-broken state tracking are NOT affected — only the SL.
        // 0 = feature disabled. Default 0.1% matches typical "very tight" CPR.
        volatile double narrowCprZoneCollapseWidthPct = 0.1;
        volatile double narrowCprMinWidth = 0.0;  // CPR width % lower threshold — narrow = [min, max). 0 = no min.
        // narrowRangeRatioThreshold removed — z-score of PDH-PDL/CPR ratio is self-calibrating
        volatile double insideCprMaxWidth = 0.5;  // max CPR width % for inside CPR stocks (0 = no filter)
        // Scan universe — fixed at NIFTY 100. The backend always loads + subscribes the full
        // 100-stock universe so NIFTY 100 stocks (beyond the 50-index) can still be scanned for
        // signals. The scanner page has a client-side filter chip to view N50-only or all 100.
        // Field kept for forward-compat in case a per-user override is ever exposed again.
        volatile String scanUniverse = "NIFTY100";
        volatile double scanMinPrice = 300;      // min stock price filter (0 = no filter)
        volatile double scanMaxPrice = 0;        // max stock price filter (0 = no max)
        // Watchlist universe gate. When true, the watchlist is restricted to NIFTY 50 stocks
        // only. When false, all stocks in the bhavcopy cache are eligible (subject to the
        // CPR-width and other scanner filters). Replaces the legacy scanIncludeNS/NL/IS/IL
        // bucket toggles (which filtered by Narrow/Inside CPR × Small/Large daily range).
        volatile boolean scanOnlyNifty50 = true;
        // Opening refresh — re-fetches today's candles from Fyers /data/history after
        // 9:20 to correct any wrong live-tick-built first candle (Fyers' live WS data is
        // unreliable during 9:15-9:25 per their own docs). Re-seeds completedCandles, SMA, ATR,
        // firstCandleClose, dayOpen. Configurable HH:mm time (IST).
        volatile boolean enableOpeningRefresh = true;
        volatile String  openingRefreshTime   = "09:25"; // IST, HH:mm
        // Target Tolerance — discount structural target by ATR fraction so near-miss reversals fill
        volatile boolean enableTargetTolerance = true;
        volatile double targetToleranceAtr = 0.10; // discount structural target by this fraction of ATR
        // NIFTY Index Alignment Filter — when on, buys require NIFTY state == BULLISH and
        // sells require BEARISH; every other state (SIDEWAYS / NEUTRAL / opposite-direction)
        // skips the trade outright. No soft mode / qty reduction.
        volatile boolean enableIndexAlignment = false;        // master toggle, opt-in
        // When ON, NIFTY's last-close vs EMA20 becomes a third factor gating the reversal
        // states. When OFF, BULLISH_REVERSAL / BEARISH_REVERSAL fall back to the pure
        // CPR-disagrees-with-futVwap definition (no EMA gate). BULLISH / BEARISH always
        // require unanimous CPR + futVwap regardless of this flag.
        volatile boolean enableNiftyEma20Factor = true;
        // When ON (default), NIFTY futures 5-min close vs that bar's stamped VWAP is one of
        // the trend factors. When OFF, the state machine drops it entirely — trend reduces
        // to CPR (plus EMA20 if that factor is also enabled). With both this and the EMA20
        // factor disabled, only BULLISH / BEARISH / NEUTRAL / SIDEWAYS can fire; reversal
        // states never appear (they require an enabled optional factor to disagree with CPR).
        volatile boolean enableNiftyFutVwapFactor = true;
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
    public boolean isEnableNiftyHtfHurdleFilter() { return cfg().enableNiftyHtfHurdleFilter; }
    public double  getNiftyHurdleMinHeadroomAtr() { return cfg().niftyHurdleMinHeadroomAtr; }
    public boolean isEnableNifty5mHurdleFilter()  { return cfg().enableNifty5mHurdleFilter; }
    public double  getNifty5mHurdleMinHeadroomAtr() { return cfg().nifty5mHurdleMinHeadroomAtr; }
    public boolean isEnableHtfCandleFilter()      { return cfg().enableHtfCandleFilter; }
    public boolean isEnableStructuralSl()    { return cfg().enableStructuralSl; }
    public double  getStructuralSlBufferAtr(){ return cfg().structuralSlBufferAtr; }
    public double  getSingleLevelSlBufferAtr(){ return cfg().singleLevelSlBufferAtr; }
    public double getDayHighLowMinAtr()            { return cfg().dayHighLowMinAtr; }
    public boolean isEnableRiskRewardFilter()      { return cfg().enableRiskRewardFilter; }
    public double  getMinRiskRewardRatio()         { return cfg().minRiskRewardRatio; }
    public boolean isEnableEmaTrendCheck()          { return cfg().enableEmaTrendCheck; }
    public boolean isEnableEmaVsAtpCheck()          { return cfg().enableEmaVsAtpCheck; }
    public boolean isEnableEmaLevelCountFilter()   { return cfg().enableEmaLevelCountFilter; }
    public int getEmaLevelMinRangePct()            { return cfg().emaLevelMinRangePct; }
    public boolean isEmaLevelFilterMorningSkip()       { return cfg().emaLevelFilterMorningSkip; }
    public String  getEmaLevelFilterMorningSkipUntil() { return cfg().emaLevelFilterMorningSkipUntil; }
    public boolean isEnableTargetShift() { return cfg().enableTargetShift; }
    public double getGoodSizeCandleBodyAtrMult()          { return cfg().goodSizeCandleBodyAtrMult; }
    public double getGoodSizeCandleMaxBodyAtrMult()       { return cfg().goodSizeCandleMaxBodyAtrMult; }
    public double getConfirmationMaxOppositeWickRatio()   { return cfg().confirmationMaxOppositeWickRatio; }
    public double getPinBarRejectionWickBodyMult() { return cfg().pinBarRejectionWickBodyMult; }
    public double getPinBarOppositeWickBodyMult()  { return cfg().pinBarOppositeWickBodyMult; }
    public double getPinBarSmallBodyMaxRangeRatio()    { return cfg().pinBarSmallBodyMaxRangeRatio; }
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
    public boolean isEnableTrailingSl() { return cfg().enableTrailingSl; }
    public boolean isEnablePriceEmaExit() { return cfg().enablePriceEmaExit; }
    public int getVirginCprExpiryDays() { return cfg().virginCprExpiryDays; }
    public boolean isEnableVirginCprHurdleFilter() { return cfg().enableVirginCprHurdleFilter; }
    public double  getVirginCprHurdleHeadroomAtr() { return cfg().virginCprHurdleHeadroomAtr; }
    public double getBreakevenTriggerPct() { return cfg().breakevenTriggerPct; }
    public double getBreakevenSlAtrMult()  { return cfg().breakevenSlAtrMult; }
    public boolean isSkipR3S3IvOvDays() { return cfg().skipR3S3IvOvDays; }
    public boolean isSkipR3S3EvDays()   { return cfg().skipR3S3EvDays; }
    public boolean isSkipR4S4IvOvDays() { return cfg().skipR4S4IvOvDays; }
    public boolean isSkipR4S4EvDays()   { return cfg().skipR4S4EvDays; }
    public boolean isSkipHtfR3S3NormalDays() { return cfg().skipHtfR3S3NormalDays; }
    public boolean isSkipHtfR4S4NormalDays() { return cfg().skipHtfR4S4NormalDays; }
    public boolean isEnableMeanReversionTrades() { return cfg().enableMeanReversionTrades; }
    public boolean isEnableMagnetTrades()        { return cfg().enableMagnetTrades; }
    public double  getMagnetTradesQtyFactor()    { return cfg().magnetTradesQtyFactor; }
    public double  getMeanReversionQtyFactor()   { return cfg().meanReversionQtyFactor; }
    public int getAtrPeriod() { return cfg().atrPeriod; }

    public String  getSignalSource()      { return cfg().signalSource; }
    public int     getScannerTimeframe()  { return cfg().scannerTimeframe; }
    public int     getHigherTimeframe()   { return cfg().higherTimeframe; }
    public boolean isEnableAtpCheck()    { return cfg().enableAtpCheck; }
    public boolean isEnableHpt()          { return cfg().enableHpt; }
    public boolean isEnableMpt()          { return cfg().enableMpt; }
    public double getMptQtyFactor()       { return cfg().mptQtyFactor; }
    public double getMinAbsoluteProfit() { return cfg().minAbsoluteProfit; }
    public double getNarrowCprMaxWidth() { return cfg().narrowCprMaxWidth; }
    public double getNarrowCprMinWidth() { return cfg().narrowCprMinWidth; }
    public double getInsideCprMaxWidth() { return cfg().insideCprMaxWidth; }
    public double getNarrowCprZoneCollapseWidthPct() { return cfg().narrowCprZoneCollapseWidthPct; }
    public String getScanUniverse() { String u = cfg().scanUniverse; return u != null && !u.isEmpty() ? u : "NIFTY100"; }
    public double getScanMinPrice() { return cfg().scanMinPrice; }
    public double getScanMaxPrice() { return cfg().scanMaxPrice; }
    public boolean isScanOnlyNifty50() { return cfg().scanOnlyNifty50; }
    public boolean isEnableOpeningRefresh()    { return cfg().enableOpeningRefresh; }
    public String  getOpeningRefreshTime()     { return cfg().openingRefreshTime; }
    public boolean isEnableTargetTolerance()   { return cfg().enableTargetTolerance; }
    public double getTargetToleranceAtr()      { return cfg().targetToleranceAtr; }
    public boolean isEnableIndexAlignment()    { return cfg().enableIndexAlignment; }
    public boolean isEnableNiftyEma20Factor()  { return cfg().enableNiftyEma20Factor; }
    public boolean isEnableNiftyFutVwapFactor(){ return cfg().enableNiftyFutVwapFactor; }
    public void setSignalSource(String v)      { cfg().signalSource = v; }
    public void setScannerTimeframe(int v)     { cfg().scannerTimeframe = v; }
    public void setHigherTimeframe(int v)      { cfg().higherTimeframe = v; }
    public void setEnableAtpCheck(boolean v)  { cfg().enableAtpCheck = v; }
    public void setEnableHpt(boolean v)        { cfg().enableHpt = v; }
    public void setEnableMpt(boolean v)        { cfg().enableMpt = v; }
    public void setMptQtyFactor(double v)      { cfg().mptQtyFactor = v; }
    public void setMinAbsoluteProfit(double v) { cfg().minAbsoluteProfit = v; }
    public void setNarrowCprMaxWidth(double v) { cfg().narrowCprMaxWidth = v; }
    public void setNarrowCprMinWidth(double v) { cfg().narrowCprMinWidth = Math.max(0, v); }
    public void setInsideCprMaxWidth(double v) { cfg().insideCprMaxWidth = v; }
    public void setNarrowCprZoneCollapseWidthPct(double v) { cfg().narrowCprZoneCollapseWidthPct = v; }
    public void setScanUniverse(String v) {
        // Always NIFTY 100 — the toggle was removed. Setter ignores input, kept only so
        // settings-save calls don't blow up if a stale UI client still posts the field.
        cfg().scanUniverse = "NIFTY100";
    }
    public void setScanMinPrice(double v) { cfg().scanMinPrice = v; }
    public void setScanMaxPrice(double v) { cfg().scanMaxPrice = v; }
    public void setScanOnlyNifty50(boolean v) { cfg().scanOnlyNifty50 = v; }
    public void setEnableOpeningRefresh(boolean v) { cfg().enableOpeningRefresh = v; }
    public void setOpeningRefreshTime(String v)    { cfg().openingRefreshTime = v; }
    public void setEnableTargetTolerance(boolean v) { cfg().enableTargetTolerance = v; }
    public void setTargetToleranceAtr(double v) { cfg().targetToleranceAtr = v; }
    public void setEnableIndexAlignment(boolean v)        { cfg().enableIndexAlignment = v; }
    public void setEnableNiftyEma20Factor(boolean v)      { cfg().enableNiftyEma20Factor = v; }
    public void setEnableNiftyFutVwapFactor(boolean v)    { cfg().enableNiftyFutVwapFactor = v; }
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
    public void setEnableNiftyHtfHurdleFilter(boolean v) { cfg().enableNiftyHtfHurdleFilter = v; }
    public void setNiftyHurdleMinHeadroomAtr(double v)   { cfg().niftyHurdleMinHeadroomAtr = Math.max(0, v); }
    public void setEnableNifty5mHurdleFilter(boolean v)  { cfg().enableNifty5mHurdleFilter = v; }
    public void setNifty5mHurdleMinHeadroomAtr(double v) { cfg().nifty5mHurdleMinHeadroomAtr = Math.max(0, v); }
    public void setEnableHtfCandleFilter(boolean v)      { cfg().enableHtfCandleFilter = v; }
    public void setEnableStructuralSl(boolean v)    { cfg().enableStructuralSl = v; }
    public void setStructuralSlBufferAtr(double v)  { cfg().structuralSlBufferAtr = v; }
    public void setSingleLevelSlBufferAtr(double v) { cfg().singleLevelSlBufferAtr = v; }
    public void setDayHighLowMinAtr(double v)              { cfg().dayHighLowMinAtr = v; }
    public void setEnableRiskRewardFilter(boolean v)       { cfg().enableRiskRewardFilter = v; }
    public void setMinRiskRewardRatio(double v)            { cfg().minRiskRewardRatio = v; }
    public void setEnableEmaTrendCheck(boolean v)         { cfg().enableEmaTrendCheck = v; }
    public void setEnableEmaVsAtpCheck(boolean v)         { cfg().enableEmaVsAtpCheck = v; }
    public void setEnableEmaLevelCountFilter(boolean v)    { cfg().enableEmaLevelCountFilter = v; }
    public void setEmaLevelMinRangePct(int v)               { cfg().emaLevelMinRangePct = Math.max(0, Math.min(100, v)); }
    public void setEmaLevelFilterMorningSkip(boolean v)     { cfg().emaLevelFilterMorningSkip = v; }
    public void setEmaLevelFilterMorningSkipUntil(String v) { if (v != null && !v.isEmpty()) cfg().emaLevelFilterMorningSkipUntil = v; }
    public void setEnableTargetShift(boolean v) { cfg().enableTargetShift = v; }
    public void setGoodSizeCandleBodyAtrMult(double v)          { cfg().goodSizeCandleBodyAtrMult = v; }
    public void setGoodSizeCandleMaxBodyAtrMult(double v)       { cfg().goodSizeCandleMaxBodyAtrMult = v; }
    public void setConfirmationMaxOppositeWickRatio(double v)   { cfg().confirmationMaxOppositeWickRatio = v; }
    public void setPinBarRejectionWickBodyMult(double v) { cfg().pinBarRejectionWickBodyMult = v; }
    public void setPinBarOppositeWickBodyMult(double v)  { cfg().pinBarOppositeWickBodyMult = v; }
    public void setPinBarSmallBodyMaxRangeRatio(double v)    { cfg().pinBarSmallBodyMaxRangeRatio = Math.max(0, v); }
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
    public void setEnableTrailingSl(boolean v) { cfg().enableTrailingSl = v; }
    public void setEnablePriceEmaExit(boolean v) { cfg().enablePriceEmaExit = v; }
    public void setVirginCprExpiryDays(int v) { cfg().virginCprExpiryDays = Math.max(0, v); }
    public void setEnableVirginCprHurdleFilter(boolean v) { cfg().enableVirginCprHurdleFilter = v; }
    public void setVirginCprHurdleHeadroomAtr(double v)   { cfg().virginCprHurdleHeadroomAtr = Math.max(0, v); }
    public void setBreakevenTriggerPct(double v) { cfg().breakevenTriggerPct = v; }
    public void setBreakevenSlAtrMult(double v)  { cfg().breakevenSlAtrMult = v; }
    public void setSkipR3S3IvOvDays(boolean v) { cfg().skipR3S3IvOvDays = v; }
    public void setSkipR3S3EvDays(boolean v)   { cfg().skipR3S3EvDays = v; }
    public void setSkipR4S4IvOvDays(boolean v) { cfg().skipR4S4IvOvDays = v; }
    public void setSkipR4S4EvDays(boolean v)   { cfg().skipR4S4EvDays = v; }
    public void setSkipHtfR3S3NormalDays(boolean v) { cfg().skipHtfR3S3NormalDays = v; }
    public void setSkipHtfR4S4NormalDays(boolean v) { cfg().skipHtfR4S4NormalDays = v; }
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
            upsert("enableNiftyHtfHurdleFilter", String.valueOf(c.enableNiftyHtfHurdleFilter));
            upsert("niftyHurdleMinHeadroomAtr", String.valueOf(c.niftyHurdleMinHeadroomAtr));
            upsert("enableNifty5mHurdleFilter", String.valueOf(c.enableNifty5mHurdleFilter));
            upsert("nifty5mHurdleMinHeadroomAtr", String.valueOf(c.nifty5mHurdleMinHeadroomAtr));
            upsert("enableHtfCandleFilter", String.valueOf(c.enableHtfCandleFilter));
            upsert("enableStructuralSl", String.valueOf(c.enableStructuralSl));
            upsert("structuralSlBufferAtr", String.valueOf(c.structuralSlBufferAtr));
            upsert("singleLevelSlBufferAtr", String.valueOf(c.singleLevelSlBufferAtr));
            upsert("dayHighLowMinAtr", String.valueOf(c.dayHighLowMinAtr));
            upsert("enableRiskRewardFilter", String.valueOf(c.enableRiskRewardFilter));
            upsert("minRiskRewardRatio", String.valueOf(c.minRiskRewardRatio));
            upsert("enableEmaTrendCheck", String.valueOf(c.enableEmaTrendCheck));
            upsert("enableEmaVsAtpCheck", String.valueOf(c.enableEmaVsAtpCheck));
            upsert("enableEmaLevelCountFilter", String.valueOf(c.enableEmaLevelCountFilter));
            upsert("emaLevelMinRangePct", String.valueOf(c.emaLevelMinRangePct));
            upsert("emaLevelFilterMorningSkip", String.valueOf(c.emaLevelFilterMorningSkip));
            upsert("emaLevelFilterMorningSkipUntil", c.emaLevelFilterMorningSkipUntil);
            upsert("enableTargetShift", String.valueOf(c.enableTargetShift));
            upsert("goodSizeCandleBodyAtrMult",    String.valueOf(c.goodSizeCandleBodyAtrMult));
            upsert("goodSizeCandleMaxBodyAtrMult", String.valueOf(c.goodSizeCandleMaxBodyAtrMult));
            upsert("confirmationMaxOppositeWickRatio", String.valueOf(c.confirmationMaxOppositeWickRatio));
            upsert("pinBarRejectionWickBodyMult", String.valueOf(c.pinBarRejectionWickBodyMult));
            upsert("pinBarOppositeWickBodyMult", String.valueOf(c.pinBarOppositeWickBodyMult));
            upsert("pinBarSmallBodyMaxRangeRatio", String.valueOf(c.pinBarSmallBodyMaxRangeRatio));
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
            upsert("enableTrailingSl", String.valueOf(c.enableTrailingSl));
            upsert("enablePriceEmaExit", String.valueOf(c.enablePriceEmaExit));
            upsert("virginCprExpiryDays", String.valueOf(c.virginCprExpiryDays));
            upsert("enableVirginCprHurdleFilter", String.valueOf(c.enableVirginCprHurdleFilter));
            upsert("virginCprHurdleHeadroomAtr",  String.valueOf(c.virginCprHurdleHeadroomAtr));
            upsert("breakevenTriggerPct", String.valueOf(c.breakevenTriggerPct));
            upsert("breakevenSlAtrMult",  String.valueOf(c.breakevenSlAtrMult));
            upsert("skipR3S3IvOvDays", String.valueOf(c.skipR3S3IvOvDays));
            upsert("skipR3S3EvDays",   String.valueOf(c.skipR3S3EvDays));
            upsert("skipR4S4IvOvDays", String.valueOf(c.skipR4S4IvOvDays));
            upsert("skipR4S4EvDays",   String.valueOf(c.skipR4S4EvDays));
            upsert("skipHtfR3S3NormalDays", String.valueOf(c.skipHtfR3S3NormalDays));
            upsert("skipHtfR4S4NormalDays", String.valueOf(c.skipHtfR4S4NormalDays));
            upsert("enableMeanReversionTrades", String.valueOf(c.enableMeanReversionTrades));
            upsert("enableMagnetTrades",        String.valueOf(c.enableMagnetTrades));
            upsert("magnetTradesQtyFactor",     String.valueOf(c.magnetTradesQtyFactor));
            upsert("meanReversionQtyFactor",    String.valueOf(c.meanReversionQtyFactor));
            upsert("atrPeriod", String.valueOf(c.atrPeriod));
            upsert("signalSource", c.signalSource);
            upsert("scannerTimeframe", String.valueOf(c.scannerTimeframe));
            upsert("higherTimeframe", String.valueOf(c.higherTimeframe));
            upsert("enableAtpCheck", String.valueOf(c.enableAtpCheck));
            upsert("enableHpt", String.valueOf(c.enableHpt));
            upsert("enableMpt", String.valueOf(c.enableMpt));
            upsert("mptQtyFactor", String.valueOf(c.mptQtyFactor));
            upsert("minAbsoluteProfit", String.valueOf(c.minAbsoluteProfit));
            upsert("narrowCprMaxWidth", String.valueOf(c.narrowCprMaxWidth));
            upsert("narrowCprMinWidth", String.valueOf(c.narrowCprMinWidth));
            // narrowRangeRatioThreshold removed — z-score is self-calibrating
            upsert("insideCprMaxWidth", String.valueOf(c.insideCprMaxWidth));
            upsert("narrowCprZoneCollapseWidthPct", String.valueOf(c.narrowCprZoneCollapseWidthPct));
            upsert("scanUniverse", "NIFTY100");  // toggle removed — always pinned
            upsert("scanMinPrice", String.valueOf(c.scanMinPrice));
            upsert("scanMaxPrice", String.valueOf(c.scanMaxPrice));
            upsert("scanOnlyNifty50", String.valueOf(c.scanOnlyNifty50));
            upsert("enableOpeningRefresh", String.valueOf(c.enableOpeningRefresh));
            upsert("openingRefreshTime", c.openingRefreshTime);
            upsert("enableTargetTolerance", String.valueOf(c.enableTargetTolerance));
            upsert("targetToleranceAtr", String.valueOf(c.targetToleranceAtr));
            upsert("enableIndexAlignment",   String.valueOf(c.enableIndexAlignment));
            upsert("enableNiftyEma20Factor", String.valueOf(c.enableNiftyEma20Factor));
            upsert("enableNiftyFutVwapFactor", String.valueOf(c.enableNiftyFutVwapFactor));
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
                    case "enableNiftyHtfHurdleFilter" -> c.enableNiftyHtfHurdleFilter = Boolean.parseBoolean(v);
                    case "niftyHurdleMinHeadroomAtr" -> c.niftyHurdleMinHeadroomAtr = Math.max(0, Double.parseDouble(v));
                    case "enableNifty5mHurdleFilter" -> c.enableNifty5mHurdleFilter = Boolean.parseBoolean(v);
                    case "nifty5mHurdleMinHeadroomAtr" -> c.nifty5mHurdleMinHeadroomAtr = Math.max(0, Double.parseDouble(v));
                    case "enableHtfCandleFilter" -> c.enableHtfCandleFilter = Boolean.parseBoolean(v);
                    case "enableStructuralSl"    -> c.enableStructuralSl = Boolean.parseBoolean(v);
                    case "structuralSlBufferAtr" -> c.structuralSlBufferAtr = Double.parseDouble(v);
                    case "singleLevelSlBufferAtr" -> c.singleLevelSlBufferAtr = Double.parseDouble(v);
                    case "dayHighLowMinAtr" -> c.dayHighLowMinAtr = Double.parseDouble(v);
                    case "enableRiskRewardFilter" -> c.enableRiskRewardFilter = Boolean.parseBoolean(v);
                    case "minRiskRewardRatio" -> c.minRiskRewardRatio = Double.parseDouble(v);
                    case "enableEmaTrendCheck", "enableSmaTrendCheck" -> {
                        c.enableEmaTrendCheck = Boolean.parseBoolean(v);
                        if ("enableSmaTrendCheck".equals(k)) logLegacyOnce("enableSmaTrendCheck");
                    }
                    // Legacy keys — silently ignored.
                    case "enableSmaTrendCheckLenient",
                         "enableSmaAlignmentCheck",
                         "enableSmaAlignmentCheckLenient",
                         "enableEmaDirectionCheck", "enableEma200DirectionCheck", "enableEmaCrossoverCheck",
                         "emaPatternLookback", "smaPatternLookback",
                         "braidedMinCrossovers", "braidedMaxSpreadAtr",
                         "railwayMaxCv", "railwayMinSpreadAtr", "railwayMinSlopeAtr",
                         "requireRtpPattern", "buyRequiresRrtp", "sellRequiresFrtp",
                         "skipTradesInZigZag", "allowTradesInZigZag",
                         "emaCloseDistanceAtr", "smaCloseDistanceAtr" -> { /* legacy — SMA50/200/pattern removed */ }
                    case "enableEmaVsAtpCheck", "enableSmaVsAtpCheck" -> {
                        c.enableEmaVsAtpCheck = Boolean.parseBoolean(v);
                        if ("enableSmaVsAtpCheck".equals(k)) logLegacyOnce("enableSmaVsAtpCheck");
                    }
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
                    case "pinBarRejectionWickBodyMult" -> c.pinBarRejectionWickBodyMult = Double.parseDouble(v);
                    case "pinBarOppositeWickBodyMult"  -> c.pinBarOppositeWickBodyMult = Double.parseDouble(v);
                    case "pinBarSmallBodyMaxRangeRatio"    -> c.pinBarSmallBodyMaxRangeRatio = Math.max(0, Double.parseDouble(v));
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
                    case "enableVirginCprHurdleFilter" -> c.enableVirginCprHurdleFilter = Boolean.parseBoolean(v);
                    case "virginCprHurdleHeadroomAtr" -> c.virginCprHurdleHeadroomAtr = Math.max(0, Double.parseDouble(v));
                    case "breakevenTriggerPct" -> c.breakevenTriggerPct = Double.parseDouble(v);
                    case "breakevenSlAtrMult"  -> c.breakevenSlAtrMult  = Double.parseDouble(v);
                    // Legacy fib-stage keys — fold old stage-1 values into the new breakeven
                    // knobs (same semantics) and silently ignore stage-2 (feature removed).
                    case "fibStage1TriggerPct" -> c.breakevenTriggerPct = Double.parseDouble(v);
                    case "fibStage1SlAtrMult"  -> c.breakevenSlAtrMult  = Double.parseDouble(v);
                    case "fibStage2TriggerPct", "fibStage2SlPct" -> { /* legacy — stage 2 removed */ }
                    // Legacy NormalDays keys: split into the new IvOv + Ev pair, both seeded
                    // with the legacy value so the user's prior intent is preserved on first
                    // load after upgrade. Once any save happens, the new keys overwrite.
                    case "skipR3S3NormalDays" -> {
                        boolean lv = Boolean.parseBoolean(v);
                        c.skipR3S3IvOvDays = lv;
                        c.skipR3S3EvDays   = lv;
                    }
                    case "skipR4S4NormalDays" -> {
                        boolean lv = Boolean.parseBoolean(v);
                        c.skipR4S4IvOvDays = lv;
                        c.skipR4S4EvDays   = lv;
                    }
                    case "skipR3S3IvOvDays" -> c.skipR3S3IvOvDays = Boolean.parseBoolean(v);
                    case "skipR3S3EvDays"   -> c.skipR3S3EvDays   = Boolean.parseBoolean(v);
                    case "skipR4S4IvOvDays" -> c.skipR4S4IvOvDays = Boolean.parseBoolean(v);
                    case "skipR4S4EvDays"   -> c.skipR4S4EvDays   = Boolean.parseBoolean(v);
                    case "skipHtfR3S3NormalDays" -> c.skipHtfR3S3NormalDays = Boolean.parseBoolean(v);
                    case "skipHtfR4S4NormalDays" -> c.skipHtfR4S4NormalDays = Boolean.parseBoolean(v);
                    case "enableMeanReversionTrades" -> c.enableMeanReversionTrades = Boolean.parseBoolean(v);
                    case "enableMagnetTrades"        -> c.enableMagnetTrades        = Boolean.parseBoolean(v);
                    case "magnetTradesQtyFactor"     -> c.magnetTradesQtyFactor     = Double.parseDouble(v);
                    case "meanReversionQtyFactor"    -> c.meanReversionQtyFactor    = Double.parseDouble(v);
                    case "atrPeriod" -> c.atrPeriod = Integer.parseInt(v);
                    case "signalSource"      -> c.signalSource = v;
                    case "scannerTimeframe"  -> c.scannerTimeframe = Integer.parseInt(v);
                    case "higherTimeframe"   -> c.higherTimeframe = Integer.parseInt(v);
                    case "enableAtpCheck"   -> c.enableAtpCheck = Boolean.parseBoolean(v);
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
                    case "narrowCprMaxWidth" -> c.narrowCprMaxWidth = Double.parseDouble(v);
                    case "narrowCprMinWidth" -> c.narrowCprMinWidth = Math.max(0, Double.parseDouble(v));
                    // narrowRangeRatioThreshold — legacy key, silently ignored
                    case "insideCprMaxWidth" -> c.insideCprMaxWidth = Double.parseDouble(v);
                    case "narrowCprZoneCollapseWidthPct" -> c.narrowCprZoneCollapseWidthPct = Double.parseDouble(v);
                    // scanUniverse — always NIFTY 100 (the toggle was removed). Any legacy
                    // persisted value (NIFTY50) gets migrated forward on load.
                    case "scanUniverse" -> c.scanUniverse = "NIFTY100";
                    case "scanMinPrice" -> c.scanMinPrice = Double.parseDouble(v);
                    case "scanMaxPrice" -> c.scanMaxPrice = Double.parseDouble(v);
                    // Legacy watchlist + OR keys — features removed.
                    case "scanMinTurnover", "scanMinVolume",
                         "scanMinBeta", "scanMaxBeta",
                         "scanCapFilter",
                         "openingRangeMinutes" -> { /* legacy — removed */ }
                    case "scanOnlyNifty50" -> c.scanOnlyNifty50 = Boolean.parseBoolean(v);
                    case "enableOpeningRefresh" -> c.enableOpeningRefresh = Boolean.parseBoolean(v);
                    case "openingRefreshTime" -> c.openingRefreshTime = v;
                    case "enableTargetTolerance" -> c.enableTargetTolerance = Boolean.parseBoolean(v);
                    case "targetToleranceAtr" -> c.targetToleranceAtr = Double.parseDouble(v);
                    case "enableIndexAlignment"   -> c.enableIndexAlignment = Boolean.parseBoolean(v);
                    case "enableNiftyEma20Factor", "enableNiftySma20Factor" -> {
                        c.enableNiftyEma20Factor = Boolean.parseBoolean(v);
                        if ("enableNiftySma20Factor".equals(k)) logLegacyOnce("enableNiftySma20Factor");
                    }
                    case "enableNiftyFutVwapFactor" -> c.enableNiftyFutVwapFactor = Boolean.parseBoolean(v);
                    case "indexAlignmentHardSkip", "indexOpposedQtyFactor" -> { /* removed — soft mode deleted */ }
                }
            }
            log.info("[RiskSettingsStore] Loaded {}: start={} end={} totalCapital={} maxRiskPerDayPct={}% riskPerTrade={} autoSquareOff={} atrMult={} brokerage={} fixedQty={} capitalPerTrade={} trailingSl={} skipR3S3(IvOv/Ev)={}/{} skipR4S4(IvOv/Ev)={}/{}", mode, c.tradingStartTime, c.tradingEndTime, c.totalCapital, c.maxRiskPerDayPct, c.riskPerTrade, c.autoSquareOffTime, c.atrMultiplier, c.brokeragePerOrder, c.fixedQuantity, c.capitalPerTrade, c.enableTrailingSl, c.skipR3S3IvOvDays, c.skipR3S3EvDays, c.skipR4S4IvOvDays, c.skipR4S4EvDays);
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
