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
        volatile boolean enableR4S4       = false; // allow BUY_ABOVE_R4 / SELL_BELOW_S4
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
        volatile double dayHighLowShiftMinDistAtr = 1.0; // skip day H/L shift if distance < N ATR from close
        volatile boolean enableWeeklyLevelTargetShift = true; // shift target to weekly CPR levels if between entry and target
        // Structural SL — opt-in, anchors SL to the S/R level the trade is testing (per setup family)
        // When on, we compute both structural and default SL and pick the TIGHTER one.
        volatile boolean enableStructuralSl = false;   // when false, always use close ± atrMultiplier × ATR
        volatile double  structuralSlBufferAtr = 1.0;  // ATR multiplier added below/above the structural anchor
        volatile double dayHighLowMinAtr = 0.5; // min distance in ATR for day high/low shifted target (0 = no check)
        // Risk/Reward filter — skip trade if |target−entry| / |entry−SL| < minRiskRewardRatio
        volatile boolean enableRiskRewardFilter = true;
        volatile double  minRiskRewardRatio     = 1.0;
        // EMA filters
        volatile boolean enableEmaDirectionCheck = true; // buy requires close > EMA(20), sell requires close < EMA(20)
        volatile boolean enableEma200DirectionCheck = true; // buy requires close > EMA(200), sell requires close < EMA(200)
        volatile boolean enableEmaCrossoverCheck = true; // buy requires 20 EMA > 200 EMA, sell requires 20 EMA < 200 EMA
        volatile double emaCloseDistanceAtr = 0.75;  // legacy — kept for backward compat with old risk-settings.json
        // EMA level-count filter — counts CPR zones strictly between EMA and the broken level.
        // Allow only when count == 0 (EMA is in the zone immediately adjacent to the broken level).
        volatile boolean enableEmaLevelCountFilter = true;
        volatile boolean enableSmallCandleFilter = false; // reject if candle move from breakout level < smallCandleAtrThreshold ATR
        volatile double smallCandleAtrThreshold = 0.5; // ATR multiplier for small candle filter
        volatile double wickRejectionRatio = 1.5; // breakout wick must be >= this * body to allow small body candle
        volatile double oppositeWickRatio = 2.0; // opposite wick >= this * body = counter-pressure, reject
        volatile boolean enableVolumeFilter = false; // reject if candle volume < volumeMultiple * avg volume
        volatile double volumeMultiple = 2.0; // breakout candle must have this x avg volume
        volatile int volumeLookback = 20; // average volume over last N candles (max 20)
        volatile boolean enableTrailingSl = true; // enable trailing SL
        volatile boolean trailingSlNoTarget = false; // when true + trailing SL enabled: skip fixed target, let trailing SL close the trade
        volatile boolean enableR3S3 = true;      // enable R3/S3 breakouts
        volatile double r3s3QtyFactor = 0.75;   // R3/S3 qty multiplier
        volatile double r4s4QtyFactor = 0.5;    // R4/S4 qty multiplier
        volatile int    atrPeriod = 14;        // ATR lookback period for initial SL
        volatile double trailingSlAtrMultiplier = 2.0; // ATR multiplier for trailing SL (separate from initial SL)
        volatile double trailingSlActivationAtr = 1.0; // trailing SL only activates after price moves this × ATR in profit
        // Scanner settings
        volatile String signalSource    = "TRADINGVIEW"; // TRADINGVIEW or INTERNAL
        volatile int    scannerTimeframe = 15;  // candle timeframe in minutes
        volatile int    higherTimeframe  = 75;  // higher TF for weekly trend (minutes)
        volatile boolean enableAtpCheck = true; // require ATP confirmation for scanner signals
        volatile boolean enableHpt      = true;  // High Probable Trade signals (weekly+daily aligned)
        volatile boolean enableLpt      = true;  // Low Probable Trade signals (everything else, half qty)
        volatile double lptQtyFactor    = 0.50;  // LPT qty multiplier (0.50 = half)
        volatile double neutralWeeklyQtyFactor = 0.50; // extra qty multiplier when weekly trend is NEUTRAL (stacks on LPT)
        volatile boolean enableWeeklyNeutralTrades = true; // if false, skip all trades when weekly trend is NEUTRAL
        volatile double insideOrQtyFactor = 0.50; // qty multiplier when breakout is inside OR range on IV/OV days
        volatile double minAbsoluteProfit = 500; // skip if qty × target_distance < this amount (₹)
        // CPR Width scanner group toggles
        volatile double narrowCprMaxWidth = 0.1;  // CPR width % threshold for narrow CPR stocks
        // narrowRangeRatioThreshold removed — z-score of PDH-PDL/CPR ratio is self-calibrating
        volatile double insideCprMaxWidth = 0.5;  // max CPR width % for inside CPR stocks (0 = no filter)
        volatile double scanMinPrice = 300;      // min stock price filter (0 = no filter)
        volatile double scanMaxPrice = 0;        // max stock price filter (0 = no max)
        volatile double scanMinTurnover = 0;     // min daily turnover in ₹ Cr (0 = no filter)
        volatile long   scanMinVolume = 0;       // min previous day volume (0 = no filter)
        volatile double scanMinBeta = 0;         // min stock beta (0 = no filter)
        volatile double scanMaxBeta = 0;         // max stock beta (0 = no filter)
        volatile String scanCapFilter = "ALL";   // ALL, or comma-separated: LARGE,MID,SMALL
        volatile boolean scanIncludeNS = true;   // Narrow + Small Range (z < -1.5)
        volatile boolean scanIncludeNL = true;   // Narrow + Large Range
        volatile boolean scanIncludeIS = true;   // Inside + Small Range
        volatile boolean scanIncludeIL = false;  // Inside + Large Range
        // Opening Range
        volatile int openingRangeMinutes = 30; // 0=disabled, 15/30/45/60
        // Opening refresh — re-fetches today's candles from Fyers /data/history after
        // 9:20 to correct any wrong live-tick-built first candle (Fyers' live WS data is
        // unreliable during 9:15-9:25 per their own docs). Re-seeds completedCandles, EMA, ATR,
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
        // NIFTY Index Alignment Filter — downgrade HPT → LPT for trades opposed to NIFTY trend
        volatile boolean enableIndexAlignment = false;        // master toggle, opt-in
        volatile boolean indexAlignmentHardSkip = false;      // true = hard skip opposed trades; false = HPT→LPT downgrade
        volatile boolean weeklyReversalHardSkip = true;        // true = skip trades opposed to weekly reversal; false = HPT→LPT
        volatile int indexBullishThreshold = 2;               // score >= this → BULLISH
        volatile int indexStrongBullishThreshold = 5;         // score >= this → STRONG_BULLISH
        volatile int indexBearishThreshold = -2;              // score <= this → BEARISH
        volatile int indexStrongBearishThreshold = -5;        // score <= this → STRONG_BEARISH
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
    public boolean isEnableR4S4()        { return cfg().enableR4S4; }
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
    public double getDayHighLowShiftMinDistAtr() { return cfg().dayHighLowShiftMinDistAtr; }
    public boolean isEnableWeeklyLevelTargetShift() { return cfg().enableWeeklyLevelTargetShift; }
    public boolean isEnableStructuralSl()    { return cfg().enableStructuralSl; }
    public double  getStructuralSlBufferAtr(){ return cfg().structuralSlBufferAtr; }
    public double getDayHighLowMinAtr()            { return cfg().dayHighLowMinAtr; }
    public boolean isEnableRiskRewardFilter()      { return cfg().enableRiskRewardFilter; }
    public double  getMinRiskRewardRatio()         { return cfg().minRiskRewardRatio; }
    public boolean isEnableEmaDirectionCheck()      { return cfg().enableEmaDirectionCheck; }
    public boolean isEnableEma200DirectionCheck()   { return cfg().enableEma200DirectionCheck; }
    public boolean isEnableEmaCrossoverCheck()     { return cfg().enableEmaCrossoverCheck; }
    public double getEmaCloseDistanceAtr()         { return cfg().emaCloseDistanceAtr; }
    public boolean isEnableEmaLevelCountFilter()   { return cfg().enableEmaLevelCountFilter; }
    public boolean isEnableTargetShift() { return cfg().enableTargetShift; }
    public boolean isEnableSmallCandleFilter() { return cfg().enableSmallCandleFilter; }
    public boolean isEnableLargeCandleBodyFilter() { return cfg().enableLargeCandleBodyFilter; }
    public double getLargeCandleBodyAtrThreshold() { return cfg().largeCandleBodyAtrThreshold; }
    public boolean isEnableTrailingSl() { return cfg().enableTrailingSl; }
    public boolean isTrailingSlNoTarget() { return cfg().trailingSlNoTarget; }
    public boolean isEnableR3S3() { return cfg().enableR3S3; }
    public double getR3s3QtyFactor() { return cfg().r3s3QtyFactor; }
    public double getR4s4QtyFactor() { return cfg().r4s4QtyFactor; }
    public int getAtrPeriod() { return cfg().atrPeriod; }
    public double getTrailingSlAtrMultiplier() { return cfg().trailingSlAtrMultiplier; }
    public double getTrailingSlActivationAtr() { return cfg().trailingSlActivationAtr; }
    public double getSmallCandleAtrThreshold() { return cfg().smallCandleAtrThreshold; }
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
    public double getNeutralWeeklyQtyFactor() { return cfg().neutralWeeklyQtyFactor; }
    public boolean isEnableWeeklyNeutralTrades() { return cfg().enableWeeklyNeutralTrades; }
    public double getInsideOrQtyFactor() { return cfg().insideOrQtyFactor; }
    public double getMinAbsoluteProfit() { return cfg().minAbsoluteProfit; }
    public double getNarrowCprMaxWidth() { return cfg().narrowCprMaxWidth; }
    public double getInsideCprMaxWidth() { return cfg().insideCprMaxWidth; }
    public double getScanMinPrice() { return cfg().scanMinPrice; }
    public double getScanMaxPrice() { return cfg().scanMaxPrice; }
    public double getScanMinTurnover() { return cfg().scanMinTurnover; }
    public long   getScanMinVolume()   { return cfg().scanMinVolume; }
    public double getScanMinBeta() { return cfg().scanMinBeta; }
    public double getScanMaxBeta() { return cfg().scanMaxBeta; }
    public String getScanCapFilter() { return cfg().scanCapFilter; }
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
    public boolean isIndexAlignmentHardSkip()  { return cfg().indexAlignmentHardSkip; }
    public boolean isWeeklyReversalHardSkip()  { return cfg().weeklyReversalHardSkip; }
    public int getIndexBullishThreshold()       { return cfg().indexBullishThreshold; }
    public int getIndexStrongBullishThreshold() { return cfg().indexStrongBullishThreshold; }
    public int getIndexBearishThreshold()       { return cfg().indexBearishThreshold; }
    public int getIndexStrongBearishThreshold() { return cfg().indexStrongBearishThreshold; }
    public void setSignalSource(String v)      { cfg().signalSource = v; }
    public void setScannerTimeframe(int v)     { cfg().scannerTimeframe = v; }
    public void setHigherTimeframe(int v)      { cfg().higherTimeframe = v; }
    public void setEnableAtpCheck(boolean v)  { cfg().enableAtpCheck = v; }
    public void setEnableHpt(boolean v)        { cfg().enableHpt = v; }
    public void setEnableLpt(boolean v)        { cfg().enableLpt = v; }
    public void setLptQtyFactor(double v)      { cfg().lptQtyFactor = v; }
    public void setNeutralWeeklyQtyFactor(double v) { cfg().neutralWeeklyQtyFactor = v; }
    public void setEnableWeeklyNeutralTrades(boolean v) { cfg().enableWeeklyNeutralTrades = v; }
    public void setInsideOrQtyFactor(double v) { cfg().insideOrQtyFactor = v; }
    public void setMinAbsoluteProfit(double v) { cfg().minAbsoluteProfit = v; }
    public void setNarrowCprMaxWidth(double v) { cfg().narrowCprMaxWidth = v; }
    public void setInsideCprMaxWidth(double v) { cfg().insideCprMaxWidth = v; }
    public void setScanMinPrice(double v) { cfg().scanMinPrice = v; }
    public void setScanMaxPrice(double v) { cfg().scanMaxPrice = v; }
    public void setScanMinTurnover(double v) { cfg().scanMinTurnover = v; }
    public void setScanMinVolume(long v)     { cfg().scanMinVolume = v; }
    public void setScanMinBeta(double v) { cfg().scanMinBeta = v; }
    public void setScanMaxBeta(double v) { cfg().scanMaxBeta = v; }
    public void setScanCapFilter(String v) { cfg().scanCapFilter = v; }
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
    public void setIndexAlignmentHardSkip(boolean v)      { cfg().indexAlignmentHardSkip = v; }
    public void setWeeklyReversalHardSkip(boolean v)     { cfg().weeklyReversalHardSkip = v; }
    public void setIndexBullishThreshold(int v)           { cfg().indexBullishThreshold = v; }
    public void setIndexStrongBullishThreshold(int v)     { cfg().indexStrongBullishThreshold = v; }
    public void setIndexBearishThreshold(int v)           { cfg().indexBearishThreshold = v; }
    public void setIndexStrongBearishThreshold(int v)     { cfg().indexStrongBearishThreshold = v; }
    public void setTradingStartTime(String v)  { cfg().tradingStartTime = v; }
    public void setTradingEndTime(String v)    { cfg().tradingEndTime = v; }
    public void setTotalCapital(double v)       { cfg().totalCapital = v; }
    public void setMaxRiskPerDayPct(double v)  { cfg().maxRiskPerDayPct = v; }
    public void setRiskPerTrade(double v)      { cfg().riskPerTrade = v; }
    public void setAutoSquareOffTime(String v) { cfg().autoSquareOffTime = v; }
    public void setAtrMultiplier(double v)     { cfg().atrMultiplier = v; }
    public void setEnableR4S4(boolean v)       { cfg().enableR4S4 = v; }
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
    public void setDayHighLowShiftMinDistAtr(double v) { cfg().dayHighLowShiftMinDistAtr = v; }
    public void setEnableWeeklyLevelTargetShift(boolean v) { cfg().enableWeeklyLevelTargetShift = v; }
    public void setEnableStructuralSl(boolean v)    { cfg().enableStructuralSl = v; }
    public void setStructuralSlBufferAtr(double v)  { cfg().structuralSlBufferAtr = v; }
    public void setDayHighLowMinAtr(double v)              { cfg().dayHighLowMinAtr = v; }
    public void setEnableRiskRewardFilter(boolean v)       { cfg().enableRiskRewardFilter = v; }
    public void setMinRiskRewardRatio(double v)            { cfg().minRiskRewardRatio = v; }
    public void setEnableEmaDirectionCheck(boolean v)       { cfg().enableEmaDirectionCheck = v; }
    public void setEnableEma200DirectionCheck(boolean v)   { cfg().enableEma200DirectionCheck = v; }
    public void setEnableEmaCrossoverCheck(boolean v)     { cfg().enableEmaCrossoverCheck = v; }
    public void setEmaCloseDistanceAtr(double v)           { cfg().emaCloseDistanceAtr = v; }
    public void setEnableEmaLevelCountFilter(boolean v)    { cfg().enableEmaLevelCountFilter = v; }
    public void setEnableTargetShift(boolean v) { cfg().enableTargetShift = v; }
    public void setEnableSmallCandleFilter(boolean v) { cfg().enableSmallCandleFilter = v; }
    public void setEnableLargeCandleBodyFilter(boolean v) { cfg().enableLargeCandleBodyFilter = v; }
    public void setLargeCandleBodyAtrThreshold(double v) { cfg().largeCandleBodyAtrThreshold = v; }
    public void setEnableTrailingSl(boolean v) { cfg().enableTrailingSl = v; }
    public void setTrailingSlNoTarget(boolean v) { cfg().trailingSlNoTarget = v; }
    public void setEnableR3S3(boolean v) { cfg().enableR3S3 = v; }
    public void setR3s3QtyFactor(double v) { cfg().r3s3QtyFactor = v; }
    public void setR4s4QtyFactor(double v) { cfg().r4s4QtyFactor = v; }
    public void setAtrPeriod(int v) { cfg().atrPeriod = v; }
    public void setTrailingSlAtrMultiplier(double v) { cfg().trailingSlAtrMultiplier = v; }
    public void setTrailingSlActivationAtr(double v) { cfg().trailingSlActivationAtr = v; }
    public void setSmallCandleAtrThreshold(double v) { cfg().smallCandleAtrThreshold = v; }
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
    public boolean isEnableR4S4(String mode)       { return cfgFor(mode).enableR4S4; }
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
    public void setEnableR4S4(String mode, boolean v)       { cfgFor(mode).enableR4S4 = v; }
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
            upsert("enableR4S4", String.valueOf(c.enableR4S4));
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
            upsert("dayHighLowShiftMinDistAtr", String.valueOf(c.dayHighLowShiftMinDistAtr));
            upsert("enableWeeklyLevelTargetShift", String.valueOf(c.enableWeeklyLevelTargetShift));
            upsert("enableStructuralSl", String.valueOf(c.enableStructuralSl));
            upsert("structuralSlBufferAtr", String.valueOf(c.structuralSlBufferAtr));
            upsert("dayHighLowMinAtr", String.valueOf(c.dayHighLowMinAtr));
            upsert("enableRiskRewardFilter", String.valueOf(c.enableRiskRewardFilter));
            upsert("minRiskRewardRatio", String.valueOf(c.minRiskRewardRatio));
            upsert("enableEmaDirectionCheck", String.valueOf(c.enableEmaDirectionCheck));
            upsert("enableEma200DirectionCheck", String.valueOf(c.enableEma200DirectionCheck));
            upsert("enableEmaCrossoverCheck", String.valueOf(c.enableEmaCrossoverCheck));
            upsert("emaCloseDistanceAtr", String.valueOf(c.emaCloseDistanceAtr));
            upsert("enableEmaLevelCountFilter", String.valueOf(c.enableEmaLevelCountFilter));
            upsert("enableTargetShift", String.valueOf(c.enableTargetShift));
            upsert("enableSmallCandleFilter", String.valueOf(c.enableSmallCandleFilter));
            upsert("enableLargeCandleBodyFilter", String.valueOf(c.enableLargeCandleBodyFilter));
            upsert("largeCandleBodyAtrThreshold", String.valueOf(c.largeCandleBodyAtrThreshold));
            upsert("smallCandleAtrThreshold", String.valueOf(c.smallCandleAtrThreshold));
            upsert("wickRejectionRatio", String.valueOf(c.wickRejectionRatio));
            upsert("oppositeWickRatio", String.valueOf(c.oppositeWickRatio));
            upsert("enableVolumeFilter", String.valueOf(c.enableVolumeFilter));
            upsert("volumeMultiple", String.valueOf(c.volumeMultiple));
            upsert("volumeLookback", String.valueOf(c.volumeLookback));
            upsert("enableTrailingSl", String.valueOf(c.enableTrailingSl));
            upsert("trailingSlNoTarget", String.valueOf(c.trailingSlNoTarget));
            upsert("enableR3S3", String.valueOf(c.enableR3S3));
            upsert("r3s3QtyFactor", String.valueOf(c.r3s3QtyFactor));
            upsert("r4s4QtyFactor", String.valueOf(c.r4s4QtyFactor));
            upsert("atrPeriod", String.valueOf(c.atrPeriod));
            upsert("trailingSlAtrMultiplier", String.valueOf(c.trailingSlAtrMultiplier));
            upsert("trailingSlActivationAtr", String.valueOf(c.trailingSlActivationAtr));
            upsert("signalSource", c.signalSource);
            upsert("scannerTimeframe", String.valueOf(c.scannerTimeframe));
            upsert("higherTimeframe", String.valueOf(c.higherTimeframe));
            upsert("enableAtpCheck", String.valueOf(c.enableAtpCheck));
            upsert("enableHpt", String.valueOf(c.enableHpt));
            upsert("enableLpt", String.valueOf(c.enableLpt));
            upsert("lptQtyFactor", String.valueOf(c.lptQtyFactor));
            upsert("neutralWeeklyQtyFactor", String.valueOf(c.neutralWeeklyQtyFactor));
            upsert("enableWeeklyNeutralTrades", String.valueOf(c.enableWeeklyNeutralTrades));
            upsert("insideOrQtyFactor", String.valueOf(c.insideOrQtyFactor));
            upsert("minAbsoluteProfit", String.valueOf(c.minAbsoluteProfit));
            upsert("narrowCprMaxWidth", String.valueOf(c.narrowCprMaxWidth));
            // narrowRangeRatioThreshold removed — z-score is self-calibrating
            upsert("insideCprMaxWidth", String.valueOf(c.insideCprMaxWidth));
            upsert("scanMinPrice", String.valueOf(c.scanMinPrice));
            upsert("scanMaxPrice", String.valueOf(c.scanMaxPrice));
            upsert("scanMinTurnover", String.valueOf(c.scanMinTurnover));
            upsert("scanMinVolume", String.valueOf(c.scanMinVolume));
            upsert("scanMinBeta", String.valueOf(c.scanMinBeta));
            upsert("scanMaxBeta", String.valueOf(c.scanMaxBeta));
            upsert("scanCapFilter", c.scanCapFilter);
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
            upsert("indexAlignmentHardSkip", String.valueOf(c.indexAlignmentHardSkip));
            upsert("weeklyReversalHardSkip", String.valueOf(c.weeklyReversalHardSkip));
            upsert("indexBullishThreshold", String.valueOf(c.indexBullishThreshold));
            upsert("indexStrongBullishThreshold", String.valueOf(c.indexStrongBullishThreshold));
            upsert("indexBearishThreshold", String.valueOf(c.indexBearishThreshold));
            upsert("indexStrongBearishThreshold", String.valueOf(c.indexStrongBearishThreshold));
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
                    case "enableR4S4"        -> c.enableR4S4 = Boolean.parseBoolean(v);
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
                    case "dayHighLowShiftMinDistAtr" -> c.dayHighLowShiftMinDistAtr = Double.parseDouble(v);
                    case "enableWeeklyLevelTargetShift" -> c.enableWeeklyLevelTargetShift = Boolean.parseBoolean(v);
                    case "enableStructuralSl"    -> c.enableStructuralSl = Boolean.parseBoolean(v);
                    case "structuralSlBufferAtr" -> c.structuralSlBufferAtr = Double.parseDouble(v);
                    case "dayHighLowMinAtr" -> c.dayHighLowMinAtr = Double.parseDouble(v);
                    case "enableRiskRewardFilter" -> c.enableRiskRewardFilter = Boolean.parseBoolean(v);
                    case "minRiskRewardRatio" -> c.minRiskRewardRatio = Double.parseDouble(v);
                    case "enableEmaDirectionCheck" -> c.enableEmaDirectionCheck = Boolean.parseBoolean(v);
                    case "enableEma200DirectionCheck" -> c.enableEma200DirectionCheck = Boolean.parseBoolean(v);
                    case "enableEmaCrossoverCheck" -> c.enableEmaCrossoverCheck = Boolean.parseBoolean(v);
                    case "emaCloseDistanceAtr" -> c.emaCloseDistanceAtr = Double.parseDouble(v);
                    case "enableEmaLevelCountFilter" -> c.enableEmaLevelCountFilter = Boolean.parseBoolean(v);
                    case "enableTargetShift" -> c.enableTargetShift = Boolean.parseBoolean(v);
                    case "enableSmallCandleFilter" -> c.enableSmallCandleFilter = Boolean.parseBoolean(v);
                    case "enableLargeCandleBodyFilter" -> c.enableLargeCandleBodyFilter = Boolean.parseBoolean(v);
                    case "largeCandleBodyAtrThreshold" -> c.largeCandleBodyAtrThreshold = Double.parseDouble(v);
                    case "smallCandleAtrThreshold" -> c.smallCandleAtrThreshold = Double.parseDouble(v);
                    case "wickRejectionRatio" -> c.wickRejectionRatio = Double.parseDouble(v);
                    case "oppositeWickRatio" -> c.oppositeWickRatio = Double.parseDouble(v);
                    case "enableVolumeFilter" -> c.enableVolumeFilter = Boolean.parseBoolean(v);
                    case "volumeMultiple" -> c.volumeMultiple = Double.parseDouble(v);
                    case "volumeLookback" -> c.volumeLookback = Integer.parseInt(v);
                    case "enableTrailingSl"   -> c.enableTrailingSl = Boolean.parseBoolean(v);
                    case "trailingSlNoTarget" -> c.trailingSlNoTarget = Boolean.parseBoolean(v);
                    case "enableR3S3" -> c.enableR3S3 = Boolean.parseBoolean(v);
                    case "r3s3QtyFactor" -> c.r3s3QtyFactor = Double.parseDouble(v);
                    case "r4s4QtyFactor" -> c.r4s4QtyFactor = Double.parseDouble(v);
                    case "atrPeriod" -> c.atrPeriod = Integer.parseInt(v);
                    case "trailingSlAtrMultiplier" -> c.trailingSlAtrMultiplier = Double.parseDouble(v);
                    case "trailingSlActivationAtr" -> c.trailingSlActivationAtr = Double.parseDouble(v);
                    case "signalSource"      -> c.signalSource = v;
                    case "scannerTimeframe"  -> c.scannerTimeframe = Integer.parseInt(v);
                    case "higherTimeframe"   -> c.higherTimeframe = Integer.parseInt(v);
                    case "enableAtpCheck"   -> c.enableAtpCheck = Boolean.parseBoolean(v);
                    case "enableHpt"         -> c.enableHpt = Boolean.parseBoolean(v);
                    case "enableLpt"         -> c.enableLpt = Boolean.parseBoolean(v);
                    case "lptQtyFactor"      -> c.lptQtyFactor = Double.parseDouble(v);
                    case "neutralWeeklyQtyFactor" -> c.neutralWeeklyQtyFactor = Double.parseDouble(v);
                    case "enableWeeklyNeutralTrades" -> c.enableWeeklyNeutralTrades = Boolean.parseBoolean(v);
                    case "insideOrQtyFactor" -> c.insideOrQtyFactor = Double.parseDouble(v);
                    case "minAbsoluteProfit" -> c.minAbsoluteProfit = Double.parseDouble(v);
                    case "narrowCprMaxWidth" -> c.narrowCprMaxWidth = Double.parseDouble(v);
                    // narrowRangeRatioThreshold — legacy key, silently ignored
                    case "insideCprMaxWidth" -> c.insideCprMaxWidth = Double.parseDouble(v);
                    case "scanMinPrice" -> c.scanMinPrice = Double.parseDouble(v);
                    case "scanMaxPrice" -> c.scanMaxPrice = Double.parseDouble(v);
                    case "scanMinTurnover" -> c.scanMinTurnover = Double.parseDouble(v);
                    case "scanMinVolume" -> c.scanMinVolume = Long.parseLong(v);
                    case "scanMinBeta" -> c.scanMinBeta = Double.parseDouble(v);
                    case "scanMaxBeta" -> c.scanMaxBeta = Double.parseDouble(v);
                    case "scanCapFilter" -> c.scanCapFilter = v;
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
                    case "indexAlignmentHardSkip" -> c.indexAlignmentHardSkip = Boolean.parseBoolean(v);
                    case "weeklyReversalHardSkip" -> c.weeklyReversalHardSkip = Boolean.parseBoolean(v);
                    case "indexBullishThreshold" -> c.indexBullishThreshold = Integer.parseInt(v);
                    case "indexStrongBullishThreshold" -> c.indexStrongBullishThreshold = Integer.parseInt(v);
                    case "indexBearishThreshold" -> c.indexBearishThreshold = Integer.parseInt(v);
                    case "indexStrongBearishThreshold" -> c.indexStrongBearishThreshold = Integer.parseInt(v);
                }
            }
            log.info("[RiskSettingsStore] Loaded {}: start={} end={} totalCapital={} maxRiskPerDayPct={}% riskPerTrade={} autoSquareOff={} atrMult={} enableR4S4={} sessionMove={}% brokerage={} fixedQty={} capitalPerTrade={} trailingSl={}", mode, c.tradingStartTime, c.tradingEndTime, c.totalCapital, c.maxRiskPerDayPct, c.riskPerTrade, c.autoSquareOffTime, c.atrMultiplier, c.enableR4S4, c.sessionMoveLimit, c.brokeragePerOrder, c.fixedQuantity, c.capitalPerTrade, c.enableTrailingSl);
        } catch (Exception e) {
            log.error("[RiskSettingsStore] Failed to load {}: {}", mode, e.getMessage());
        }
    }
}
