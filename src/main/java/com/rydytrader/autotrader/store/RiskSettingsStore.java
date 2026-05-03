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
        volatile boolean enableSessionMoveLimit = true;
        volatile double sessionMoveLimit = 2.0;   // qty halved if session move exceeds this %
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
        volatile boolean enableLargeCandleBodyFilter = true;
        volatile double largeCandleBodyAtrThreshold = 4.0; // skip if candle body > N × ATR (exhaustion risk)
        volatile boolean enableTargetShift = true; // shift target to next level if default target < threshold ATR. If false, skip the entry.
        volatile boolean enableGapCheck = true;     // halve qty if day open or first candle beyond R2/S2
        volatile boolean enableDayHighLowTargetShift = true; // shift target to day high/low if between entry and target
        // Shift target to daily 5-min SMA 200 when it sits between entry and target. Useful
        // mainly when running the lenient SMA price gate (which doesn't validate against 200) —
        // the 200 SMA can still act as resistance/support on the way to the structural target.
        volatile boolean enableDailySma200TargetShift = true;
        volatile double dayHighLowShiftMinDistAtr = 2.0; // skip day H/L shift if distance < N ATR from close
        volatile boolean enableWeeklyLevelTargetShift = true; // shift target to weekly CPR levels if between entry and target
        volatile boolean enableWeeklySmaTargetShift = true;   // shift target to weekly (HTF 60-min) SMA 20/50/200 if between entry and target
        volatile boolean enableHtfHurdleFilter = true; // HPT→LPT when 5-min close lands inside R1/PWH (buy) or S1/PWL (sell) zone
        // NIFTY-level macro hurdle. When on, skip ALL stock trades while NIFTY's prior 1h close
        // hasn't decisively cleared the nearest weekly hurdle in the trade direction (R1/PWH/
        // weekly TC/Pivot/BC for buys; S1/PWL/... for sells). Mirrors per-stock HTF Hurdle
        // applied to NIFTY's own data. Default off — opt-in.
        volatile boolean enableNiftyHtfHurdleFilter = false;
        volatile boolean enableHtfSmaAlignment = true; // HPT→LPT when live LTP not above/below 1h SMA 20/50 together
        volatile boolean enableHtfSmaAlignmentCheck = false; // HPT→LPT when 1h SMAs not in order (20>50 buy, 20<50 sell)
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
        // SMA filters
        // 5-min SMA trend gate: buy requires close above SMA(20/50/200), sell requires below all three.
        // Matches the BULL/BEAR chip on scanner cards. Fail-open if any SMA not loaded.
        volatile boolean enableSmaTrendCheck = true;
        // Lenient variant: only requires close above/below SMA 20 AND SMA 50 (skips SMA 200).
        // If enableSmaTrendCheck is also on, the strict 3-SMA gate wins (strict implies lenient).
        volatile boolean enableSmaTrendCheckLenient = false;
        // SMA alignment gate (stricter than trend check): buy requires 20>50>200, sell requires 20<50<200.
        volatile boolean enableSmaAlignmentCheck = false;
        // Lenient alignment: buy requires only 20 > 50 (skips 50 > 200); mirror for sells.
        // If enableSmaAlignmentCheck is also on, the strict 3-SMA gate wins (strict implies lenient).
        volatile boolean enableSmaAlignmentCheckLenient = false;
        volatile boolean enableSmaVsAtpCheck = true; // buy requires 20 SMA > ATP (VWAP), sell requires 20 SMA < ATP
        // SMA 20/50 pattern detection thresholds (Braided vs Railway Track).
        // Bumped for SMA behaviour (smoother, smaller spreads, shallower slopes than EMA).
        // 5-min pattern lookback. 24 × 5min = 120 min window — long enough to filter routine
        // pullbacks within real trends, short enough to catch fresh structural shifts within a session.
        volatile int smaPatternLookback = 24;
        // 1h (HTF) pattern lookback. 10 × 60min = 10 hours — kept short because each bar carries
        // much more signal at 1h timeframe; same algorithm reads cleanly with fewer samples.
        volatile int smaPatternLookbackHtf = 10;
        volatile int braidedMinCrossovers = 2;       // ≥ this many crossovers in lookback = BRAIDED
        volatile double braidedMaxSpreadAtr = 0.10;  // mean|spread| ≤ this × ATR = BRAIDED (SMAs truly overlapping)
        volatile double railwayMaxCv = 0.25;         // stddev/mean ratio for RAILWAY (stability)
        volatile double railwayMinSpreadAtr = 0.20;  // mean|spread| ≥ this × ATR for RAILWAY (meaningful separation; SMA spreads smaller than EMA)
        volatile double railwayMinSlopeAtr = 0.20;   // (sma[last]−sma[first])/ATR ≥ this for R-RTP (both 20 & 50 must actually rise; symmetric for F-RTP). SMA lag makes slopes shallower.
        // SMA pattern trade filters
        // When ON, buys require R-RTP pattern and sells require F-RTP pattern (direction-matched).
        volatile boolean requireRtpPattern = false;
        volatile boolean skipTradesInZigZag = true; // when ON (default), all trades (buy & sell) blocked when pattern is ZIG ZAG
        volatile double smaCloseDistanceAtr = 0.75;  // legacy — kept for backward compat with old risk-settings.json
        // SMA level-count filter — counts CPR zones strictly between SMA and the broken level.
        // Allow only when count == 0 (SMA is in the zone immediately adjacent to the broken level).
        volatile boolean enableSmaLevelCountFilter = true;
        // Secondary proximity constraint on top of the level-count filter. Requires SMA to sit
        // within (100 - smaLevelMinRangePct)% of the range from the broken level to the nearest
        // non-broken zone edge on the other side. 50 = SMA must be in the upper half of that
        // range (buy) / lower half (sell). 0 = proximity check disabled.
        volatile int smaLevelMinRangePct = 50;
        // Morning skip: when enabled, the SMA level-count filter is bypassed before this time.
        // Rationale: in the first hour of the session price can run hard while SMA(20) lags
        // behind, making the filter reject otherwise valid breakouts.
        volatile boolean smaLevelFilterMorningSkip = false;
        volatile String  smaLevelFilterMorningSkipUntil = "10:15"; // HH:mm IST
        volatile boolean enableSmallCandleFilter = false; // reject if candle body and move past level both fall short of their ATR floors
        volatile double smallCandleAtrThreshold = 0.5; // legacy single-knob (kept for backward-compat load only; no longer drives logic)
        volatile double smallCandleBodyAtrThreshold = 0.5;  // body floor — small body = weak conviction
        volatile double smallCandleMoveAtrThreshold = 0.15; // move-past-level floor — tiny push past the level = barely cleared
        volatile double wickRejectionRatio = 1.5; // breakout wick must be >= this * body to allow small body candle
        volatile double oppositeWickRatio = 2.0; // opposite wick >= this * body = counter-pressure, reject
        volatile boolean enableVolumeFilter = false; // reject if candle volume < volumeMultiple * avg volume
        volatile double volumeMultiple = 2.0; // breakout candle must have this x avg volume
        volatile int volumeLookback = 20; // average volume over last N candles (max 20)
        volatile boolean enableTrailingSl = true; // enable Fibonacci-based trailing SL (base=breakout level, ceiling=target, stages at 61.8% and 78.6%)
        // Defensive 5-min SMA cross exit: at every 5-min candle close, if SMA 20 has stacked
        // against the trade direction (LONG: SMA 20 < SMA 50; SHORT: SMA 20 > SMA 50) the bot
        // squareoffs the position before SL hits. Default off — material behavior change.
        volatile boolean enableSmaCrossExit = false;
        // Defensive Price-vs-SMA exit. At every 5-min candle close, if the just-closed bar's
        // close is against the trade direction relative to the 5-min SMA 20 (LONG: close < SMA 20;
        // SHORT: close > SMA 20), squareoff the position before SL hits. Independent of the
        // SMA-cross exit above; both can be enabled together. Default off — material behavior change.
        volatile boolean enablePriceSmaExit = false;
        // Per-symbol daily trade limit. When >0, halts further trades on a symbol once today's
        // count of wins OR today's count of losses (separately, NOT total) reaches the threshold.
        // E.g. limit=2: 2W+0L → stop, 0W+2L → stop, 1W+1L → continue, 2W+1L → stop.
        // Counts only fully-closed trades (T1 partial-fill rows are excluded). 0 = disabled.
        volatile int perSymbolDailyTradeLimit = 2;
        // Fibonacci trailing SL — all four knobs stored as percent (0–100).
        volatile double fibStage1TriggerPct = 61.8;   // LTP hits this % of range → stage 1 activates
        volatile double fibStage1SlAtrMult = 1.0;     // stage 1 SL = entry ± N × ATR
        volatile double fibStage2TriggerPct = 78.6;   // LTP hits this % of range → stage 2 activates
        volatile double fibStage2SlPct = 61.8;        // stage 2 SL = base + this % of range
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
        volatile int    atrPeriod = 14;        // ATR lookback period for initial SL
        // Scanner settings
        volatile String signalSource    = "TRADINGVIEW"; // TRADINGVIEW or INTERNAL
        volatile int    scannerTimeframe = 15;  // candle timeframe in minutes
        volatile int    higherTimeframe  = 60;  // higher TF for weekly trend (minutes) — Fyers native resolution
        volatile boolean enableAtpCheck = true; // require ATP confirmation for scanner signals
        volatile boolean enableHpt      = true;  // High Probable Trade signals (weekly+daily aligned)
        volatile boolean enableLpt      = true;  // Low Probable Trade signals (everything else, half qty)
        volatile double lptQtyFactor    = 0.50;  // LPT qty multiplier (0.50 = half)
        // Medium Probable Trade — new tier under LTF-priority probability model. Trade fires
        // as MPT when only one of LTF (5-min vs daily CPR) or HTF (weekly) aligns with trade
        // direction; magnet trades also classify as MPT (HTF-only alignment).
        volatile boolean enableMpt      = true;
        volatile double mptQtyFactor    = 0.75;
        // Weekly NEUTRAL trades → LPT. Use enableLpt toggle to skip them.
        // Inside-OR breakouts are always downgraded to LPT (no skip toggles, no qty reduction)
        volatile double smallRangeAdrPct = 50.0; // prev day's range ≤ this % of 20-day ADR → SMALL classification (NS / IS)
        volatile double minAbsoluteProfit = 500; // skip if qty × target_distance < this amount (₹)
        // CPR Width scanner group toggles
        volatile double narrowCprMaxWidth = 0.1;  // CPR width % upper threshold for narrow CPR stocks
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
        volatile double scanMinTurnover = 0;     // min daily turnover in ₹ Cr (0 = no filter)
        volatile long   scanMinVolume = 0;       // min previous day volume (0 = no filter)
        volatile double scanMinBeta = 0;         // min stock beta (0 = no filter)
        volatile double scanMaxBeta = 0;         // max stock beta (0 = no filter)
        // scanCapFilter removed — NIFTY 50 universe means all stocks are LARGE; cap filter was a no-op
        volatile boolean scanIncludeNS = true;   // Narrow + Small Range (z < -1.5)
        volatile boolean scanIncludeNL = true;   // Narrow + Large Range
        volatile boolean scanIncludeIS = true;   // Inside + Small Range
        volatile boolean scanIncludeIL = false;  // Inside + Large Range
        // Opening Range
        volatile int openingRangeMinutes = 30; // 0=disabled, 15/30/45/60
        // Opening refresh — re-fetches today's candles from Fyers /data/history after
        // 9:20 to correct any wrong live-tick-built first candle (Fyers' live WS data is
        // unreliable during 9:15-9:25 per their own docs). Re-seeds completedCandles, SMA, ATR,
        // firstCandleClose, dayOpen, OR. Configurable HH:mm time (IST).
        volatile boolean enableOpeningRefresh = true;
        volatile String  openingRefreshTime   = "09:25"; // IST, HH:mm
        // Split Targets (T1/T2)
        volatile boolean enableSplitTarget = true;
        volatile int t1DistancePct = 50;           // T1 at N% of target distance (25/50/75)
        volatile double splitMinDistanceAtr = 0;   // min distance in ATR multiples to split (0 = always)
        // Target Tolerance — discount structural target by ATR fraction so near-miss reversals fill
        volatile boolean enableTargetTolerance = true;
        volatile double targetToleranceAtr = 0.10; // discount structural target by this fraction of ATR
        // NIFTY Index Alignment Filter — when on, buys require NIFTY state == BULLISH and
        // sells require BEARISH; every other state (SIDEWAYS / NEUTRAL / opposite-direction)
        // skips the trade outright. No soft mode / qty reduction.
        volatile boolean enableIndexAlignment = false;        // master toggle, opt-in
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
    public boolean isEnableSessionMoveLimit() { return cfg().enableSessionMoveLimit; }
    public double getSessionMoveLimit() { return cfg().sessionMoveLimit; }
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
    public boolean isEnableDayHighLowTargetShift() { return cfg().enableDayHighLowTargetShift; }
    public boolean isEnableDailySma200TargetShift() { return cfg().enableDailySma200TargetShift; }
    public double getDayHighLowShiftMinDistAtr() { return cfg().dayHighLowShiftMinDistAtr; }
    public boolean isEnableWeeklyLevelTargetShift() { return cfg().enableWeeklyLevelTargetShift; }
    public boolean isEnableWeeklySmaTargetShift()   { return cfg().enableWeeklySmaTargetShift; }
    public boolean isEnableHtfHurdleFilter()    { return cfg().enableHtfHurdleFilter; }
    public boolean isEnableNiftyHtfHurdleFilter() { return cfg().enableNiftyHtfHurdleFilter; }
    public boolean isEnableHtfSmaAlignment()    { return cfg().enableHtfSmaAlignment; }
    public boolean isEnableHtfSmaAlignmentCheck() { return cfg().enableHtfSmaAlignmentCheck; }
    public boolean isEnableStructuralSl()    { return cfg().enableStructuralSl; }
    public double  getStructuralSlBufferAtr(){ return cfg().structuralSlBufferAtr; }
    public double  getSingleLevelSlBufferAtr(){ return cfg().singleLevelSlBufferAtr; }
    public double getDayHighLowMinAtr()            { return cfg().dayHighLowMinAtr; }
    public boolean isEnableRiskRewardFilter()      { return cfg().enableRiskRewardFilter; }
    public double  getMinRiskRewardRatio()         { return cfg().minRiskRewardRatio; }
    public boolean isEnableSmaTrendCheck()          { return cfg().enableSmaTrendCheck; }
    public boolean isEnableSmaTrendCheckLenient()   { return cfg().enableSmaTrendCheckLenient; }
    public boolean isEnableSmaAlignmentCheck()      { return cfg().enableSmaAlignmentCheck; }
    public boolean isEnableSmaAlignmentCheckLenient() { return cfg().enableSmaAlignmentCheckLenient; }
    public boolean isEnableSmaVsAtpCheck()          { return cfg().enableSmaVsAtpCheck; }
    public int    getSmaPatternLookback()           { return cfg().smaPatternLookback; }
    public int    getSmaPatternLookbackHtf()        { return cfg().smaPatternLookbackHtf; }
    public int    getBraidedMinCrossovers()         { return cfg().braidedMinCrossovers; }
    public double getBraidedMaxSpreadAtr()          { return cfg().braidedMaxSpreadAtr; }
    public double getRailwayMaxCv()                 { return cfg().railwayMaxCv; }
    public double getRailwayMinSpreadAtr()          { return cfg().railwayMinSpreadAtr; }
    public double getRailwayMinSlopeAtr()           { return cfg().railwayMinSlopeAtr; }
    public boolean isRequireRtpPattern()            { return cfg().requireRtpPattern; }
    public boolean isSkipTradesInZigZag()           { return cfg().skipTradesInZigZag; }
    public double getSmaCloseDistanceAtr()         { return cfg().smaCloseDistanceAtr; }
    public boolean isEnableSmaLevelCountFilter()   { return cfg().enableSmaLevelCountFilter; }
    public int getSmaLevelMinRangePct()            { return cfg().smaLevelMinRangePct; }
    public boolean isSmaLevelFilterMorningSkip()       { return cfg().smaLevelFilterMorningSkip; }
    public String  getSmaLevelFilterMorningSkipUntil() { return cfg().smaLevelFilterMorningSkipUntil; }
    public boolean isEnableTargetShift() { return cfg().enableTargetShift; }
    public boolean isEnableSmallCandleFilter() { return cfg().enableSmallCandleFilter; }
    public boolean isEnableLargeCandleBodyFilter() { return cfg().enableLargeCandleBodyFilter; }
    public double getLargeCandleBodyAtrThreshold() { return cfg().largeCandleBodyAtrThreshold; }
    public boolean isEnableTrailingSl() { return cfg().enableTrailingSl; }
    public boolean isEnableSmaCrossExit() { return cfg().enableSmaCrossExit; }
    public boolean isEnablePriceSmaExit() { return cfg().enablePriceSmaExit; }
    public int getPerSymbolDailyTradeLimit() { return cfg().perSymbolDailyTradeLimit; }
    public double getFibStage1TriggerPct() { return cfg().fibStage1TriggerPct; }
    public double getFibStage1SlAtrMult()  { return cfg().fibStage1SlAtrMult; }
    public double getFibStage2TriggerPct() { return cfg().fibStage2TriggerPct; }
    public double getFibStage2SlPct()      { return cfg().fibStage2SlPct; }
    public boolean isSkipR3S3IvOvDays() { return cfg().skipR3S3IvOvDays; }
    public boolean isSkipR3S3EvDays()   { return cfg().skipR3S3EvDays; }
    public boolean isSkipR4S4IvOvDays() { return cfg().skipR4S4IvOvDays; }
    public boolean isSkipR4S4EvDays()   { return cfg().skipR4S4EvDays; }
    public boolean isSkipHtfR3S3NormalDays() { return cfg().skipHtfR3S3NormalDays; }
    public boolean isSkipHtfR4S4NormalDays() { return cfg().skipHtfR4S4NormalDays; }
    public int getAtrPeriod() { return cfg().atrPeriod; }
    public double getSmallCandleAtrThreshold() { return cfg().smallCandleAtrThreshold; }
    public double getSmallCandleBodyAtrThreshold() { return cfg().smallCandleBodyAtrThreshold; }
    public double getSmallCandleMoveAtrThreshold() { return cfg().smallCandleMoveAtrThreshold; }
    public double getWickRejectionRatio() { return cfg().wickRejectionRatio; }
    public double getOppositeWickRatio() { return cfg().oppositeWickRatio; }
    public boolean isEnableVolumeFilter() { return cfg().enableVolumeFilter; }
    public double getVolumeMultiple() { return cfg().volumeMultiple; }
    public int getVolumeLookback() { return cfg().volumeLookback; }

    public String  getSignalSource()      { return cfg().signalSource; }
    public int     getScannerTimeframe()  { return cfg().scannerTimeframe; }
    public int     getHigherTimeframe()   { return cfg().higherTimeframe; }
    public boolean isEnableAtpCheck()    { return cfg().enableAtpCheck; }
    public boolean isEnableHpt()          { return cfg().enableHpt; }
    public boolean isEnableLpt()          { return cfg().enableLpt; }
    public double getLptQtyFactor()       { return cfg().lptQtyFactor; }
    public boolean isEnableMpt()          { return cfg().enableMpt; }
    public double getMptQtyFactor()       { return cfg().mptQtyFactor; }
    public double getSmallRangeAdrPct() { return cfg().smallRangeAdrPct; }
    public double getMinAbsoluteProfit() { return cfg().minAbsoluteProfit; }
    public double getNarrowCprMaxWidth() { return cfg().narrowCprMaxWidth; }
    public double getNarrowCprMinWidth() { return cfg().narrowCprMinWidth; }
    public double getInsideCprMaxWidth() { return cfg().insideCprMaxWidth; }
    public String getScanUniverse() { String u = cfg().scanUniverse; return u != null && !u.isEmpty() ? u : "NIFTY100"; }
    public double getScanMinPrice() { return cfg().scanMinPrice; }
    public double getScanMaxPrice() { return cfg().scanMaxPrice; }
    public double getScanMinTurnover() { return cfg().scanMinTurnover; }
    public long   getScanMinVolume()   { return cfg().scanMinVolume; }
    public double getScanMinBeta() { return cfg().scanMinBeta; }
    public double getScanMaxBeta() { return cfg().scanMaxBeta; }
    public boolean isScanIncludeNS() { return cfg().scanIncludeNS; }
    public boolean isScanIncludeNL() { return cfg().scanIncludeNL; }
    public boolean isScanIncludeIS() { return cfg().scanIncludeIS; }
    public boolean isScanIncludeIL() { return cfg().scanIncludeIL; }
    public int getOpeningRangeMinutes()        { return cfg().openingRangeMinutes; }
    public boolean isEnableOpeningRefresh()    { return cfg().enableOpeningRefresh; }
    public String  getOpeningRefreshTime()     { return cfg().openingRefreshTime; }
    public boolean isEnableSplitTarget()       { return cfg().enableSplitTarget; }
    public int getT1DistancePct()              { return cfg().t1DistancePct; }
    public double getSplitMinDistanceAtr()     { return cfg().splitMinDistanceAtr; }
    public boolean isEnableTargetTolerance()   { return cfg().enableTargetTolerance; }
    public double getTargetToleranceAtr()      { return cfg().targetToleranceAtr; }
    public boolean isEnableIndexAlignment()    { return cfg().enableIndexAlignment; }
    public void setSignalSource(String v)      { cfg().signalSource = v; }
    public void setScannerTimeframe(int v)     { cfg().scannerTimeframe = v; }
    public void setHigherTimeframe(int v)      { cfg().higherTimeframe = v; }
    public void setEnableAtpCheck(boolean v)  { cfg().enableAtpCheck = v; }
    public void setEnableHpt(boolean v)        { cfg().enableHpt = v; }
    public void setEnableLpt(boolean v)        { cfg().enableLpt = v; }
    public void setLptQtyFactor(double v)      { cfg().lptQtyFactor = v; }
    public void setEnableMpt(boolean v)        { cfg().enableMpt = v; }
    public void setMptQtyFactor(double v)      { cfg().mptQtyFactor = v; }
    public void setSmallRangeAdrPct(double v) { cfg().smallRangeAdrPct = v; }
    public void setMinAbsoluteProfit(double v) { cfg().minAbsoluteProfit = v; }
    public void setNarrowCprMaxWidth(double v) { cfg().narrowCprMaxWidth = v; }
    public void setNarrowCprMinWidth(double v) { cfg().narrowCprMinWidth = Math.max(0, v); }
    public void setInsideCprMaxWidth(double v) { cfg().insideCprMaxWidth = v; }
    public void setScanUniverse(String v) {
        // Always NIFTY 100 — the toggle was removed. Setter ignores input, kept only so
        // settings-save calls don't blow up if a stale UI client still posts the field.
        cfg().scanUniverse = "NIFTY100";
    }
    public void setScanMinPrice(double v) { cfg().scanMinPrice = v; }
    public void setScanMaxPrice(double v) { cfg().scanMaxPrice = v; }
    public void setScanMinTurnover(double v) { cfg().scanMinTurnover = v; }
    public void setScanMinVolume(long v)     { cfg().scanMinVolume = v; }
    public void setScanMinBeta(double v) { cfg().scanMinBeta = v; }
    public void setScanMaxBeta(double v) { cfg().scanMaxBeta = v; }
    public void setScanIncludeNS(boolean v) { cfg().scanIncludeNS = v; }
    public void setScanIncludeNL(boolean v) { cfg().scanIncludeNL = v; }
    public void setScanIncludeIS(boolean v) { cfg().scanIncludeIS = v; }
    public void setScanIncludeIL(boolean v) { cfg().scanIncludeIL = v; }
    public void setOpeningRangeMinutes(int v)  { cfg().openingRangeMinutes = v; }
    public void setEnableOpeningRefresh(boolean v) { cfg().enableOpeningRefresh = v; }
    public void setOpeningRefreshTime(String v)    { cfg().openingRefreshTime = v; }
    public void setEnableSplitTarget(boolean v) { cfg().enableSplitTarget = v; }
    public void setT1DistancePct(int v)        { cfg().t1DistancePct = v; }
    public void setSplitMinDistanceAtr(double v) { cfg().splitMinDistanceAtr = v; }
    public void setEnableTargetTolerance(boolean v) { cfg().enableTargetTolerance = v; }
    public void setTargetToleranceAtr(double v) { cfg().targetToleranceAtr = v; }
    public void setEnableIndexAlignment(boolean v)        { cfg().enableIndexAlignment = v; }
    public void setTradingStartTime(String v)  { cfg().tradingStartTime = v; }
    public void setTradingEndTime(String v)    { cfg().tradingEndTime = v; }
    public void setTotalCapital(double v)       { cfg().totalCapital = v; }
    public void setMaxRiskPerDayPct(double v)  { cfg().maxRiskPerDayPct = v; }
    public void setRiskPerTrade(double v)      { cfg().riskPerTrade = v; }
    public void setAutoSquareOffTime(String v) { cfg().autoSquareOffTime = v; }
    public void setAtrMultiplier(double v)     { cfg().atrMultiplier = v; }
    public void setEnableSessionMoveLimit(boolean v) { cfg().enableSessionMoveLimit = v; }
    public void setSessionMoveLimit(double v) { cfg().sessionMoveLimit = v; }
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
    public void setEnableDayHighLowTargetShift(boolean v) { cfg().enableDayHighLowTargetShift = v; }
    public void setEnableDailySma200TargetShift(boolean v) { cfg().enableDailySma200TargetShift = v; }
    public void setDayHighLowShiftMinDistAtr(double v) { cfg().dayHighLowShiftMinDistAtr = v; }
    public void setEnableWeeklyLevelTargetShift(boolean v) { cfg().enableWeeklyLevelTargetShift = v; }
    public void setEnableWeeklySmaTargetShift(boolean v)   { cfg().enableWeeklySmaTargetShift = v; }
    public void setEnableHtfHurdleFilter(boolean v)    { cfg().enableHtfHurdleFilter = v; }
    public void setEnableNiftyHtfHurdleFilter(boolean v) { cfg().enableNiftyHtfHurdleFilter = v; }
    public void setEnableHtfSmaAlignment(boolean v)    { cfg().enableHtfSmaAlignment = v; }
    public void setEnableHtfSmaAlignmentCheck(boolean v) { cfg().enableHtfSmaAlignmentCheck = v; }
    public void setEnableStructuralSl(boolean v)    { cfg().enableStructuralSl = v; }
    public void setStructuralSlBufferAtr(double v)  { cfg().structuralSlBufferAtr = v; }
    public void setSingleLevelSlBufferAtr(double v) { cfg().singleLevelSlBufferAtr = v; }
    public void setDayHighLowMinAtr(double v)              { cfg().dayHighLowMinAtr = v; }
    public void setEnableRiskRewardFilter(boolean v)       { cfg().enableRiskRewardFilter = v; }
    public void setMinRiskRewardRatio(double v)            { cfg().minRiskRewardRatio = v; }
    public void setEnableSmaTrendCheck(boolean v)         { cfg().enableSmaTrendCheck = v; }
    public void setEnableSmaTrendCheckLenient(boolean v)  { cfg().enableSmaTrendCheckLenient = v; }
    public void setEnableSmaAlignmentCheck(boolean v)     { cfg().enableSmaAlignmentCheck = v; }
    public void setEnableSmaAlignmentCheckLenient(boolean v) { cfg().enableSmaAlignmentCheckLenient = v; }
    public void setEnableSmaVsAtpCheck(boolean v)         { cfg().enableSmaVsAtpCheck = v; }
    public void setRequireRtpPattern(boolean v)           { cfg().requireRtpPattern = v; }
    public void setSkipTradesInZigZag(boolean v)          { cfg().skipTradesInZigZag = v; }
    public void setSmaCloseDistanceAtr(double v)           { cfg().smaCloseDistanceAtr = v; }
    public void setEnableSmaLevelCountFilter(boolean v)    { cfg().enableSmaLevelCountFilter = v; }
    public void setSmaLevelMinRangePct(int v)               { cfg().smaLevelMinRangePct = Math.max(0, Math.min(100, v)); }
    public void setSmaLevelFilterMorningSkip(boolean v)     { cfg().smaLevelFilterMorningSkip = v; }
    public void setSmaLevelFilterMorningSkipUntil(String v) { if (v != null && !v.isEmpty()) cfg().smaLevelFilterMorningSkipUntil = v; }
    public void setEnableTargetShift(boolean v) { cfg().enableTargetShift = v; }
    public void setEnableSmallCandleFilter(boolean v) { cfg().enableSmallCandleFilter = v; }
    public void setEnableLargeCandleBodyFilter(boolean v) { cfg().enableLargeCandleBodyFilter = v; }
    public void setLargeCandleBodyAtrThreshold(double v) { cfg().largeCandleBodyAtrThreshold = v; }
    public void setEnableTrailingSl(boolean v) { cfg().enableTrailingSl = v; }
    public void setEnableSmaCrossExit(boolean v) { cfg().enableSmaCrossExit = v; }
    public void setEnablePriceSmaExit(boolean v) { cfg().enablePriceSmaExit = v; }
    public void setPerSymbolDailyTradeLimit(int v) { cfg().perSymbolDailyTradeLimit = Math.max(0, v); }
    public void setFibStage1TriggerPct(double v) { cfg().fibStage1TriggerPct = v; }
    public void setFibStage1SlAtrMult(double v)  { cfg().fibStage1SlAtrMult = v; }
    public void setFibStage2TriggerPct(double v) { cfg().fibStage2TriggerPct = v; }
    public void setFibStage2SlPct(double v)      { cfg().fibStage2SlPct = v; }
    public void setSkipR3S3IvOvDays(boolean v) { cfg().skipR3S3IvOvDays = v; }
    public void setSkipR3S3EvDays(boolean v)   { cfg().skipR3S3EvDays = v; }
    public void setSkipR4S4IvOvDays(boolean v) { cfg().skipR4S4IvOvDays = v; }
    public void setSkipR4S4EvDays(boolean v)   { cfg().skipR4S4EvDays = v; }
    public void setSkipHtfR3S3NormalDays(boolean v) { cfg().skipHtfR3S3NormalDays = v; }
    public void setSkipHtfR4S4NormalDays(boolean v) { cfg().skipHtfR4S4NormalDays = v; }
    public void setAtrPeriod(int v) { cfg().atrPeriod = v; }
    public void setSmallCandleAtrThreshold(double v) { cfg().smallCandleAtrThreshold = v; }
    public void setSmallCandleBodyAtrThreshold(double v) { cfg().smallCandleBodyAtrThreshold = v; }
    public void setSmallCandleMoveAtrThreshold(double v) { cfg().smallCandleMoveAtrThreshold = v; }
    public void setWickRejectionRatio(double v) { cfg().wickRejectionRatio = v; }
    public void setOppositeWickRatio(double v) { cfg().oppositeWickRatio = v; }
    public void setEnableVolumeFilter(boolean v) { cfg().enableVolumeFilter = v; }
    public void setVolumeMultiple(double v) { cfg().volumeMultiple = v; }
    public void setVolumeLookback(int v) { cfg().volumeLookback = Math.min(v, 20); }

    // ── mode-specific getters/setters (used by SettingsController) ────────────
    public String getTradingStartTime(String mode)  { return cfgFor(mode).tradingStartTime; }
    public String getTradingEndTime(String mode)    { return cfgFor(mode).tradingEndTime; }
    public double getTotalCapital(String mode)       { return cfgFor(mode).totalCapital; }
    public double getMaxRiskPerDayPct(String mode)  { return cfgFor(mode).maxRiskPerDayPct; }
    public double getRiskPerTrade(String mode)      { return cfgFor(mode).riskPerTrade; }
    public double getMaxDailyLoss(String mode)      { return cfgFor(mode).totalCapital * cfgFor(mode).maxRiskPerDayPct / 100.0; }
    public String getAutoSquareOffTime(String mode) { return cfgFor(mode).autoSquareOffTime; }
    public double getAtrMultiplier(String mode)     { return cfgFor(mode).atrMultiplier; }
    public double getSessionMoveLimit(String mode) { return cfgFor(mode).sessionMoveLimit; }
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
    public boolean isEnableSmallCandleFilter(String mode) { return cfgFor(mode).enableSmallCandleFilter; }
    public boolean isEnableTrailingSl(String mode) { return cfgFor(mode).enableTrailingSl; }
    public double getSmallCandleAtrThreshold(String mode) { return cfgFor(mode).smallCandleAtrThreshold; }
    public double getSmallCandleBodyAtrThreshold(String mode) { return cfgFor(mode).smallCandleBodyAtrThreshold; }
    public double getSmallCandleMoveAtrThreshold(String mode) { return cfgFor(mode).smallCandleMoveAtrThreshold; }
    public double getWickRejectionRatio(String mode) { return cfgFor(mode).wickRejectionRatio; }
    public double getOppositeWickRatio(String mode) { return cfgFor(mode).oppositeWickRatio; }
    public boolean isEnableVolumeFilter(String mode) { return cfgFor(mode).enableVolumeFilter; }
    public double getVolumeMultiple(String mode) { return cfgFor(mode).volumeMultiple; }
    public int getVolumeLookback(String mode) { return cfgFor(mode).volumeLookback; }

    public void setTradingStartTime(String mode, String v)  { cfgFor(mode).tradingStartTime = v; }
    public void setTradingEndTime(String mode, String v)    { cfgFor(mode).tradingEndTime = v; }
    public void setTotalCapital(String mode, double v)       { cfgFor(mode).totalCapital = v; }
    public void setMaxRiskPerDayPct(String mode, double v)  { cfgFor(mode).maxRiskPerDayPct = v; }
    public void setRiskPerTrade(String mode, double v)      { cfgFor(mode).riskPerTrade = v; }
    public void setAutoSquareOffTime(String mode, String v) { cfgFor(mode).autoSquareOffTime = v; }
    public void setAtrMultiplier(String mode, double v)     { cfgFor(mode).atrMultiplier = v; }
    public void setSessionMoveLimit(String mode, double v) { cfgFor(mode).sessionMoveLimit = v; }
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
    public void setEnableSmallCandleFilter(String mode, boolean v) { cfgFor(mode).enableSmallCandleFilter = v; }
    public void setEnableTrailingSl(String mode, boolean v) { cfgFor(mode).enableTrailingSl = v; }
    public void setSmallCandleAtrThreshold(String mode, double v) { cfgFor(mode).smallCandleAtrThreshold = v; }
    public void setSmallCandleBodyAtrThreshold(String mode, double v) { cfgFor(mode).smallCandleBodyAtrThreshold = v; }
    public void setSmallCandleMoveAtrThreshold(String mode, double v) { cfgFor(mode).smallCandleMoveAtrThreshold = v; }
    public void setWickRejectionRatio(String mode, double v) { cfgFor(mode).wickRejectionRatio = v; }
    public void setOppositeWickRatio(String mode, double v) { cfgFor(mode).oppositeWickRatio = v; }
    public void setEnableVolumeFilter(String mode, boolean v) { cfgFor(mode).enableVolumeFilter = v; }
    public void setVolumeMultiple(String mode, double v) { cfgFor(mode).volumeMultiple = v; }
    public void setVolumeLookback(String mode, int v) { cfgFor(mode).volumeLookback = Math.min(v, 20); }

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
            upsert("enableSessionMoveLimit", String.valueOf(c.enableSessionMoveLimit));
            upsert("sessionMoveLimit", String.valueOf(c.sessionMoveLimit));
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
            upsert("enableDayHighLowTargetShift", String.valueOf(c.enableDayHighLowTargetShift));
            upsert("enableDailySma200TargetShift", String.valueOf(c.enableDailySma200TargetShift));
            upsert("dayHighLowShiftMinDistAtr", String.valueOf(c.dayHighLowShiftMinDistAtr));
            upsert("enableWeeklyLevelTargetShift", String.valueOf(c.enableWeeklyLevelTargetShift));
            upsert("enableWeeklySmaTargetShift", String.valueOf(c.enableWeeklySmaTargetShift));
            upsert("enableHtfHurdleFilter", String.valueOf(c.enableHtfHurdleFilter));
            upsert("enableNiftyHtfHurdleFilter", String.valueOf(c.enableNiftyHtfHurdleFilter));
            upsert("enableHtfSmaAlignment", String.valueOf(c.enableHtfSmaAlignment));
            upsert("enableHtfSmaAlignmentCheck", String.valueOf(c.enableHtfSmaAlignmentCheck));
            upsert("enableStructuralSl", String.valueOf(c.enableStructuralSl));
            upsert("structuralSlBufferAtr", String.valueOf(c.structuralSlBufferAtr));
            upsert("singleLevelSlBufferAtr", String.valueOf(c.singleLevelSlBufferAtr));
            upsert("dayHighLowMinAtr", String.valueOf(c.dayHighLowMinAtr));
            upsert("enableRiskRewardFilter", String.valueOf(c.enableRiskRewardFilter));
            upsert("minRiskRewardRatio", String.valueOf(c.minRiskRewardRatio));
            upsert("enableSmaTrendCheck", String.valueOf(c.enableSmaTrendCheck));
            upsert("enableSmaTrendCheckLenient", String.valueOf(c.enableSmaTrendCheckLenient));
            upsert("enableSmaAlignmentCheck", String.valueOf(c.enableSmaAlignmentCheck));
            upsert("enableSmaAlignmentCheckLenient", String.valueOf(c.enableSmaAlignmentCheckLenient));
            upsert("enableSmaVsAtpCheck", String.valueOf(c.enableSmaVsAtpCheck));
            upsert("smaPatternLookback", String.valueOf(c.smaPatternLookback));
            upsert("smaPatternLookbackHtf", String.valueOf(c.smaPatternLookbackHtf));
            upsert("braidedMinCrossovers", String.valueOf(c.braidedMinCrossovers));
            upsert("braidedMaxSpreadAtr", String.valueOf(c.braidedMaxSpreadAtr));
            upsert("railwayMaxCv", String.valueOf(c.railwayMaxCv));
            upsert("railwayMinSpreadAtr", String.valueOf(c.railwayMinSpreadAtr));
            upsert("railwayMinSlopeAtr", String.valueOf(c.railwayMinSlopeAtr));
            upsert("requireRtpPattern", String.valueOf(c.requireRtpPattern));
            upsert("skipTradesInZigZag", String.valueOf(c.skipTradesInZigZag));
            upsert("smaCloseDistanceAtr", String.valueOf(c.smaCloseDistanceAtr));
            upsert("enableSmaLevelCountFilter", String.valueOf(c.enableSmaLevelCountFilter));
            upsert("smaLevelMinRangePct", String.valueOf(c.smaLevelMinRangePct));
            upsert("smaLevelFilterMorningSkip", String.valueOf(c.smaLevelFilterMorningSkip));
            upsert("smaLevelFilterMorningSkipUntil", c.smaLevelFilterMorningSkipUntil);
            upsert("enableTargetShift", String.valueOf(c.enableTargetShift));
            upsert("enableSmallCandleFilter", String.valueOf(c.enableSmallCandleFilter));
            upsert("enableLargeCandleBodyFilter", String.valueOf(c.enableLargeCandleBodyFilter));
            upsert("largeCandleBodyAtrThreshold", String.valueOf(c.largeCandleBodyAtrThreshold));
            upsert("smallCandleAtrThreshold", String.valueOf(c.smallCandleAtrThreshold));
            upsert("smallCandleBodyAtrThreshold", String.valueOf(c.smallCandleBodyAtrThreshold));
            upsert("smallCandleMoveAtrThreshold", String.valueOf(c.smallCandleMoveAtrThreshold));
            upsert("wickRejectionRatio", String.valueOf(c.wickRejectionRatio));
            upsert("oppositeWickRatio", String.valueOf(c.oppositeWickRatio));
            upsert("enableVolumeFilter", String.valueOf(c.enableVolumeFilter));
            upsert("volumeMultiple", String.valueOf(c.volumeMultiple));
            upsert("volumeLookback", String.valueOf(c.volumeLookback));
            upsert("enableTrailingSl", String.valueOf(c.enableTrailingSl));
            upsert("enableSmaCrossExit", String.valueOf(c.enableSmaCrossExit));
            upsert("enablePriceSmaExit", String.valueOf(c.enablePriceSmaExit));
            upsert("perSymbolDailyTradeLimit", String.valueOf(c.perSymbolDailyTradeLimit));
            upsert("fibStage1TriggerPct", String.valueOf(c.fibStage1TriggerPct));
            upsert("fibStage1SlAtrMult",  String.valueOf(c.fibStage1SlAtrMult));
            upsert("fibStage2TriggerPct", String.valueOf(c.fibStage2TriggerPct));
            upsert("fibStage2SlPct",      String.valueOf(c.fibStage2SlPct));
            upsert("skipR3S3IvOvDays", String.valueOf(c.skipR3S3IvOvDays));
            upsert("skipR3S3EvDays",   String.valueOf(c.skipR3S3EvDays));
            upsert("skipR4S4IvOvDays", String.valueOf(c.skipR4S4IvOvDays));
            upsert("skipR4S4EvDays",   String.valueOf(c.skipR4S4EvDays));
            upsert("skipHtfR3S3NormalDays", String.valueOf(c.skipHtfR3S3NormalDays));
            upsert("skipHtfR4S4NormalDays", String.valueOf(c.skipHtfR4S4NormalDays));
            upsert("atrPeriod", String.valueOf(c.atrPeriod));
            upsert("signalSource", c.signalSource);
            upsert("scannerTimeframe", String.valueOf(c.scannerTimeframe));
            upsert("higherTimeframe", String.valueOf(c.higherTimeframe));
            upsert("enableAtpCheck", String.valueOf(c.enableAtpCheck));
            upsert("enableHpt", String.valueOf(c.enableHpt));
            upsert("enableLpt", String.valueOf(c.enableLpt));
            upsert("lptQtyFactor", String.valueOf(c.lptQtyFactor));
            upsert("enableMpt", String.valueOf(c.enableMpt));
            upsert("mptQtyFactor", String.valueOf(c.mptQtyFactor));
            upsert("smallRangeAdrPct", String.valueOf(c.smallRangeAdrPct));
            upsert("minAbsoluteProfit", String.valueOf(c.minAbsoluteProfit));
            upsert("narrowCprMaxWidth", String.valueOf(c.narrowCprMaxWidth));
            upsert("narrowCprMinWidth", String.valueOf(c.narrowCprMinWidth));
            // narrowRangeRatioThreshold removed — z-score is self-calibrating
            upsert("insideCprMaxWidth", String.valueOf(c.insideCprMaxWidth));
            upsert("scanUniverse", "NIFTY100");  // toggle removed — always pinned
            upsert("scanMinPrice", String.valueOf(c.scanMinPrice));
            upsert("scanMaxPrice", String.valueOf(c.scanMaxPrice));
            upsert("scanMinTurnover", String.valueOf(c.scanMinTurnover));
            upsert("scanMinVolume", String.valueOf(c.scanMinVolume));
            upsert("scanMinBeta", String.valueOf(c.scanMinBeta));
            upsert("scanMaxBeta", String.valueOf(c.scanMaxBeta));
            upsert("scanIncludeNS", String.valueOf(c.scanIncludeNS));
            upsert("scanIncludeNL", String.valueOf(c.scanIncludeNL));
            upsert("scanIncludeIS", String.valueOf(c.scanIncludeIS));
            upsert("scanIncludeIL", String.valueOf(c.scanIncludeIL));
            upsert("openingRangeMinutes", String.valueOf(c.openingRangeMinutes));
            upsert("enableOpeningRefresh", String.valueOf(c.enableOpeningRefresh));
            upsert("openingRefreshTime", c.openingRefreshTime);
            upsert("enableSplitTarget", String.valueOf(c.enableSplitTarget));
            upsert("t1DistancePct", String.valueOf(c.t1DistancePct));
            upsert("splitMinDistanceAtr", String.valueOf(c.splitMinDistanceAtr));
            upsert("enableTargetTolerance", String.valueOf(c.enableTargetTolerance));
            upsert("targetToleranceAtr", String.valueOf(c.targetToleranceAtr));
            upsert("enableIndexAlignment", String.valueOf(c.enableIndexAlignment));
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
                    case "enableSessionMoveLimit" -> c.enableSessionMoveLimit = Boolean.parseBoolean(v);
                    case "sessionMoveLimit"  -> c.sessionMoveLimit = Double.parseDouble(v);
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
                    case "enableDayHighLowTargetShift" -> c.enableDayHighLowTargetShift = Boolean.parseBoolean(v);
                    case "enableDailySma200TargetShift" -> c.enableDailySma200TargetShift = Boolean.parseBoolean(v);
                    case "dayHighLowShiftMinDistAtr" -> c.dayHighLowShiftMinDistAtr = Double.parseDouble(v);
                    case "enableWeeklyLevelTargetShift" -> c.enableWeeklyLevelTargetShift = Boolean.parseBoolean(v);
                    case "enableWeeklyEmaTargetShift", "enableWeeklySmaTargetShift" -> c.enableWeeklySmaTargetShift = Boolean.parseBoolean(v);
                    case "enableHtfHurdleFilter" -> c.enableHtfHurdleFilter = Boolean.parseBoolean(v);
                    case "enableNiftyHtfHurdleFilter" -> c.enableNiftyHtfHurdleFilter = Boolean.parseBoolean(v);
                    case "enableHtfSmaAlignment" -> c.enableHtfSmaAlignment = Boolean.parseBoolean(v);
                    case "enableHtfSmaAlignmentCheck" -> c.enableHtfSmaAlignmentCheck = Boolean.parseBoolean(v);
                    case "enableStructuralSl"    -> c.enableStructuralSl = Boolean.parseBoolean(v);
                    case "structuralSlBufferAtr" -> c.structuralSlBufferAtr = Double.parseDouble(v);
                    case "singleLevelSlBufferAtr" -> c.singleLevelSlBufferAtr = Double.parseDouble(v);
                    case "dayHighLowMinAtr" -> c.dayHighLowMinAtr = Double.parseDouble(v);
                    case "enableRiskRewardFilter" -> c.enableRiskRewardFilter = Boolean.parseBoolean(v);
                    case "minRiskRewardRatio" -> c.minRiskRewardRatio = Double.parseDouble(v);
                    case "enableEmaTrendCheck", "enableSmaTrendCheck" -> c.enableSmaTrendCheck = Boolean.parseBoolean(v);
                    case "enableSmaTrendCheckLenient" -> c.enableSmaTrendCheckLenient = Boolean.parseBoolean(v);
                    case "enableSmaAlignmentCheck" -> c.enableSmaAlignmentCheck = Boolean.parseBoolean(v);
                    case "enableSmaAlignmentCheckLenient" -> c.enableSmaAlignmentCheckLenient = Boolean.parseBoolean(v);
                    // Legacy keys — silently ignored (semantics differ, no safe fold).
                    case "enableEmaDirectionCheck", "enableEma200DirectionCheck", "enableEmaCrossoverCheck" -> { /* legacy */ }
                    case "enableEmaVsAtpCheck", "enableSmaVsAtpCheck" -> c.enableSmaVsAtpCheck = Boolean.parseBoolean(v);
                    case "emaPatternLookback", "smaPatternLookback" -> c.smaPatternLookback = Integer.parseInt(v);
                    case "smaPatternLookbackHtf" -> c.smaPatternLookbackHtf = Integer.parseInt(v);
                    case "braidedMinCrossovers" -> c.braidedMinCrossovers = Integer.parseInt(v);
                    case "braidedMaxSpreadAtr" -> c.braidedMaxSpreadAtr = Double.parseDouble(v);
                    case "railwayMaxCv" -> c.railwayMaxCv = Double.parseDouble(v);
                    case "railwayMinSpreadAtr" -> c.railwayMinSpreadAtr = Double.parseDouble(v);
                    case "railwayMinSlopeAtr" -> c.railwayMinSlopeAtr = Double.parseDouble(v);
                    case "requireRtpPattern" -> c.requireRtpPattern = Boolean.parseBoolean(v);
                    // Legacy: if either old toggle was ON, new combined toggle is ON.
                    case "buyRequiresRrtp", "sellRequiresFrtp" -> { if (Boolean.parseBoolean(v)) c.requireRtpPattern = true; }
                    case "skipTradesInZigZag" -> c.skipTradesInZigZag = Boolean.parseBoolean(v);
                    case "allowTradesInZigZag" -> c.skipTradesInZigZag = !Boolean.parseBoolean(v); // legacy (inverted semantic)
                    case "emaCloseDistanceAtr", "smaCloseDistanceAtr" -> c.smaCloseDistanceAtr = Double.parseDouble(v);
                    case "enableEmaLevelCountFilter", "enableSmaLevelCountFilter" -> c.enableSmaLevelCountFilter = Boolean.parseBoolean(v);
                    case "smaLevelMinRangePct" -> c.smaLevelMinRangePct = Math.max(0, Math.min(100, Integer.parseInt(v)));
                    case "smaLevelFilterMorningSkip" -> c.smaLevelFilterMorningSkip = Boolean.parseBoolean(v);
                    case "smaLevelFilterMorningSkipUntil" -> { if (v != null && !v.isEmpty()) c.smaLevelFilterMorningSkipUntil = v; }
                    case "enableTargetShift" -> c.enableTargetShift = Boolean.parseBoolean(v);
                    case "enableSmallCandleFilter" -> c.enableSmallCandleFilter = Boolean.parseBoolean(v);
                    case "enableLargeCandleBodyFilter" -> c.enableLargeCandleBodyFilter = Boolean.parseBoolean(v);
                    case "largeCandleBodyAtrThreshold" -> c.largeCandleBodyAtrThreshold = Double.parseDouble(v);
                    case "smallCandleAtrThreshold" -> {
                        // Legacy single knob: also seed the new split fields if those keys haven't been
                        // saved yet. Once the user saves the new fields explicitly, this no-op overwrites
                        // are harmless because save() writes both keys.
                        c.smallCandleAtrThreshold = Double.parseDouble(v);
                    }
                    case "smallCandleBodyAtrThreshold" -> c.smallCandleBodyAtrThreshold = Double.parseDouble(v);
                    case "smallCandleMoveAtrThreshold" -> c.smallCandleMoveAtrThreshold = Double.parseDouble(v);
                    case "wickRejectionRatio" -> c.wickRejectionRatio = Double.parseDouble(v);
                    case "oppositeWickRatio" -> c.oppositeWickRatio = Double.parseDouble(v);
                    case "enableVolumeFilter" -> c.enableVolumeFilter = Boolean.parseBoolean(v);
                    case "volumeMultiple" -> c.volumeMultiple = Double.parseDouble(v);
                    case "volumeLookback" -> c.volumeLookback = Integer.parseInt(v);
                    case "enableTrailingSl"   -> c.enableTrailingSl = Boolean.parseBoolean(v);
                    case "enableSmaCrossExit" -> c.enableSmaCrossExit = Boolean.parseBoolean(v);
                    case "enablePriceSmaExit" -> c.enablePriceSmaExit = Boolean.parseBoolean(v);
                    case "perSymbolDailyTradeLimit" -> c.perSymbolDailyTradeLimit = Math.max(0, Integer.parseInt(v));
                    case "fibStage1TriggerPct" -> c.fibStage1TriggerPct = Double.parseDouble(v);
                    case "fibStage1SlAtrMult"  -> c.fibStage1SlAtrMult  = Double.parseDouble(v);
                    case "fibStage2TriggerPct" -> c.fibStage2TriggerPct = Double.parseDouble(v);
                    case "fibStage2SlPct"      -> c.fibStage2SlPct      = Double.parseDouble(v);
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
                    case "atrPeriod" -> c.atrPeriod = Integer.parseInt(v);
                    case "signalSource"      -> c.signalSource = v;
                    case "scannerTimeframe"  -> c.scannerTimeframe = Integer.parseInt(v);
                    case "higherTimeframe"   -> c.higherTimeframe = Integer.parseInt(v);
                    case "enableAtpCheck"   -> c.enableAtpCheck = Boolean.parseBoolean(v);
                    case "enableHpt"         -> c.enableHpt = Boolean.parseBoolean(v);
                    case "enableLpt"         -> c.enableLpt = Boolean.parseBoolean(v);
                    case "lptQtyFactor"      -> c.lptQtyFactor = Double.parseDouble(v);
                    case "enableMpt"         -> c.enableMpt = Boolean.parseBoolean(v);
                    case "mptQtyFactor"      -> c.mptQtyFactor = Double.parseDouble(v);
                    case "neutralWeeklyQtyFactor" -> { /* removed — weekly NEUTRAL is plain LPT now */ }
                    case "enableWeeklyNeutralTrades" -> { /* removed — weekly NEUTRAL is LPT; use enableLpt to skip */ }
                    case "insideOrQtyFactor", "skipInsideOrOnEv", "skipInsideOrOnIv", "skipInsideOrOnOv" -> {
                        /* removed — inside-OR breakouts always just become LPT, no skip/qty-reduction */
                    }
                    case "enableNarrowOrOverride", "narrowOrMaxAdrPct", "narrowOrMaxAtr" -> {
                        /* removed — narrow OR override feature deleted */
                    }
                    case "smallRangeAdrPct" -> c.smallRangeAdrPct = Double.parseDouble(v);
                    case "enableCprDayRelationFilter" -> { /* removed — 2D-CPR feature deleted */ }
                    case "minAbsoluteProfit" -> c.minAbsoluteProfit = Double.parseDouble(v);
                    case "narrowCprMaxWidth" -> c.narrowCprMaxWidth = Double.parseDouble(v);
                    case "narrowCprMinWidth" -> c.narrowCprMinWidth = Math.max(0, Double.parseDouble(v));
                    // narrowRangeRatioThreshold — legacy key, silently ignored
                    case "insideCprMaxWidth" -> c.insideCprMaxWidth = Double.parseDouble(v);
                    // scanUniverse — always NIFTY 100 (the toggle was removed). Any legacy
                    // persisted value (NIFTY50) gets migrated forward on load.
                    case "scanUniverse" -> c.scanUniverse = "NIFTY100";
                    case "scanMinPrice" -> c.scanMinPrice = Double.parseDouble(v);
                    case "scanMaxPrice" -> c.scanMaxPrice = Double.parseDouble(v);
                    case "scanMinTurnover" -> c.scanMinTurnover = Double.parseDouble(v);
                    case "scanMinVolume" -> c.scanMinVolume = Long.parseLong(v);
                    case "scanMinBeta" -> c.scanMinBeta = Double.parseDouble(v);
                    case "scanMaxBeta" -> c.scanMaxBeta = Double.parseDouble(v);
                    case "scanCapFilter" -> { /* legacy key, silently ignored — cap filter removed */ }
                    case "scanIncludeNS" -> c.scanIncludeNS = Boolean.parseBoolean(v);
                    case "scanIncludeNL" -> c.scanIncludeNL = Boolean.parseBoolean(v);
                    case "scanIncludeIS" -> c.scanIncludeIS = Boolean.parseBoolean(v);
                    case "scanIncludeIL" -> c.scanIncludeIL = Boolean.parseBoolean(v);
                    case "openingRangeMinutes" -> c.openingRangeMinutes = Integer.parseInt(v);
                    case "enableOpeningRefresh" -> c.enableOpeningRefresh = Boolean.parseBoolean(v);
                    case "openingRefreshTime" -> c.openingRefreshTime = v;
                    case "enableSplitTarget" -> c.enableSplitTarget = Boolean.parseBoolean(v);
                    case "t1DistancePct" -> c.t1DistancePct = Integer.parseInt(v);
                    case "splitMinDistanceAtr" -> c.splitMinDistanceAtr = Double.parseDouble(v);
                    case "enableTargetTolerance" -> c.enableTargetTolerance = Boolean.parseBoolean(v);
                    case "targetToleranceAtr" -> c.targetToleranceAtr = Double.parseDouble(v);
                    case "enableIndexAlignment" -> c.enableIndexAlignment = Boolean.parseBoolean(v);
                    case "indexAlignmentHardSkip", "indexOpposedQtyFactor" -> { /* removed — soft mode deleted */ }
                }
            }
            log.info("[RiskSettingsStore] Loaded {}: start={} end={} totalCapital={} maxRiskPerDayPct={}% riskPerTrade={} autoSquareOff={} atrMult={} sessionMove={}% brokerage={} fixedQty={} capitalPerTrade={} trailingSl={} skipR3S3(IvOv/Ev)={}/{} skipR4S4(IvOv/Ev)={}/{}", mode, c.tradingStartTime, c.tradingEndTime, c.totalCapital, c.maxRiskPerDayPct, c.riskPerTrade, c.autoSquareOffTime, c.atrMultiplier, c.sessionMoveLimit, c.brokeragePerOrder, c.fixedQuantity, c.capitalPerTrade, c.enableTrailingSl, c.skipR3S3IvOvDays, c.skipR3S3EvDays, c.skipR4S4IvOvDays, c.skipR4S4EvDays);
        } catch (Exception e) {
            log.error("[RiskSettingsStore] Failed to load {}: {}", mode, e.getMessage());
        }
    }
}
