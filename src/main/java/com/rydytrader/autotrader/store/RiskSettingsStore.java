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
        volatile boolean enableLargeCandleBodyFilter = true;
        volatile double largeCandleBodyAtrThreshold = 4.0; // skip if candle body > N × ATR (exhaustion risk)
        // ── Candle pattern thresholds (BreakoutScanner / CandlePatternDetector) ──
        // Marubozu (used in two-bar MARUBOZU_RETEST): full-body conviction candle.
        // body ≥ N × ATR AND total wicks ≤ N × body.
        volatile double marubozuBodyAtrMult         = 1.0;
        volatile double marubozuMaxWicksPctOfBody   = 0.10;
        // Good-size candle (used in two-bar GOOD_SIZE_CANDLE_RETEST): decent body without
        // marubozu-strict wick rules. body ≥ N × ATR; opposing wick ≤ N × body so a green
        // bar with a long upper wick (shooting-star-shape) or red bar with long lower wick
        // (hammer-shape) doesn't qualify even though the body is big enough.
        volatile double goodSizeCandleBodyAtrMult           = 0.6;
        volatile double goodSizeCandleMaxOppositeWickRatio  = 1.0;
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
        // Engulfing: current body ≥ N × prev body. 1.0 = strict; 0.9 allows near-engulfing.
        volatile double engulfingMinBodyMultiple    = 1.0;
        // Engulfing absolute size floor: current body ≥ N × ATR. Without this, a "weak engulfing"
        // (engulfer + engulfed both tiny) could fire on noise. 0.5 = current bar must be at least
        // half an ATR. Set to 0 to disable the floor.
        volatile double engulfingMinBodyAtrMult     = 0.5;
        // Piercing line / dark cloud cover (2-bar partial reversal — bar 2 doesn't fully engulf).
        // prev body ≥ N × ATR (bar 1 must be a real directional bar); bar 2 closes ≥ N% into bar 1.
        volatile double piercingPrevBodyAtrMult     = 0.5;
        volatile double piercingPenetrationPct      = 0.5;
        // Tweezer top / bottom (2-bar matched-extreme reversal). prev body ≥ N × ATR (real
        // directional bar). Matching extremes within N × ATR tolerance. Strict color flip
        // enforced (red→green for bottom, green→red for top — built-in, not configurable).
        volatile double tweezerPrevBodyAtrMult      = 0.5;
        volatile double tweezerLowHighMatchAtr      = 0.10;
        // Three Inside Up / Three Inside Down (3-bar harami + confirmation). bar1 large
        // directional (≥ N × ATR), bar2 opposite color with body fully INSIDE bar1's body
        // and body ≤ N × bar1 body (small inside bar — harami constraint), bar3 closes past
        // bar1's open in reversal direction (confirmation).
        volatile double haramiBodyAtrMult           = 0.5;
        volatile double haramiInnerBodyMaxRatio     = 0.5;
        // Doji reversal (2-bar): bar 1 (prev) is the doji — body ≤ dojiBodyMaxRangeRatio × range.
        // Bar 2 (curr) is the strong directional confirmation — body ≥ dojiConfirmBodyAtrMult × ATR.
        volatile double dojiBodyMaxRangeRatio       = 0.10;
        volatile double dojiConfirmBodyAtrMult      = 0.5;
        // Morning / evening star: bar1 + bar3 body ≥ N × ATR; bar2 body ≤ N × bar1 body.
        volatile double starOuterBodyAtrMult        = 0.5;
        volatile double starMiddleBodyMaxMultOfOuter = 0.3;
        // Retest touch rule slack — bar's extreme can fall short of the level by up to
        // (toleranceAtr × ATR) and still count as a touch. Absorbs tick-level whisker misses.
        volatile double levelTouchToleranceAtr      = 0.2;
        volatile boolean enableTargetShift = true; // shift target to next level if default target < threshold ATR. If false, skip the entry.
        volatile boolean enableGapCheck = true;     // halve qty if day open or first candle beyond R2/S2
        volatile boolean enableDayHighLowTargetShift = true; // shift target to day high/low if between entry and target
        // Shift target to daily 5-min SMA 200 when it sits between entry and target. Useful
        // mainly when running the lenient SMA price gate (which doesn't validate against 200) —
        // the 200 SMA can still act as resistance/support on the way to the structural target.
        volatile boolean enableDailySma200TargetShift = true;
        volatile double dayHighLowShiftMinDistAtr = 2.0; // skip day H/L shift if distance < N ATR from close
        volatile boolean enableWeeklyLevelTargetShift = true; // shift target to weekly CPR levels if between entry and target
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
        // NIFTY reversal-CPR-touch defensive exit. When NIFTY is in BULLISH_REVERSAL (NIFTY
        // below CPR + bullish SMAs) and price climbs back to/past CPR bottom, exit all open
        // LONG positions — the bullish move played out, NIFTY entering CPR consolidation. Mirror
        // for BEARISH_REVERSAL: NIFTY above CPR drops to/below CPR top → exit all SHORT positions.
        // Default off.
        volatile boolean enableNiftyReversalCprExit = false;
        // NIFTY HTF Hurdle break — early exit. When a stock trade was taken with the NIFTY
        // HTF Hurdle filter actively gating it (i.e., a hurdle existed in trade direction
        // and NIFTY's prior 15-min close had cleared it), capture that 15-min bar's LOW
        // (for buys) / HIGH (for sells). On every NIFTY 5-min candle close, if the just-
        // closed bar's close breaches that captured level, the HTF breakout has structurally
        // failed — square off the position with reason NIFTY_HURDLE_FAIL. Default off.
        volatile boolean enableNiftyHtfHurdleExit = false;
        // Per-symbol daily trade limit. When >0, halts further trades on a symbol once today's
        // count of wins OR today's count of losses (separately, NOT total) reaches the threshold.
        // E.g. limit=2: 2W+0L → stop, 0W+2L → stop, 1W+1L → continue, 2W+1L → stop.
        // Counts only fully-closed trades (T1 partial-fill rows are excluded). 0 = disabled.
        volatile int perSymbolDailyTradeLimit = 2;
        // Per-symbol daily LPT trade limit. Caps how many LPT-tagged trades fire on a single
        // symbol per trading day. LPT is currently produced when a stock is misaligned with
        // NIFTY but passes all stock-level filters (downgrade-not-reject behavior). 0 = disabled.
        // Default 1 — at most one LPT trade per stock per day.
        volatile int lptMaxTradesPerStockPerDay = 1;
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
        // Virgin CPR-Touch defensive exit — when NIFTY's just-closed 5-min bar's range touches
        // the active virgin CPR zone (bar high ≥ zoneBot AND bar low ≤ zoneTop), close all open
        // positions at market. Trigger reason: VIRGIN_CPR_TOUCH. Default off — opt-in.
        volatile boolean enableVirginCprTouchExit = false;
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
        // Master toggle for the 8 counter-trend setups: 2 magnets (BUY_ABOVE_S1_PDL,
        // SELL_BELOW_R1_PDH) + 6 deep mean-rev (BUY_ABOVE_S2/S3/S4, SELL_BELOW_R2/R3/R4).
        // When off, none of them fire. All 8 are day-type agnostic — fire on IV/OV/EV alike.
        // Default on — preserves existing magnet behavior.
        volatile boolean enableMeanReversionTrades = true;
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
        // Watchlist universe gate. When true, the watchlist is restricted to NIFTY 50 stocks
        // only. When false, all stocks in the bhavcopy cache are eligible (subject to the
        // CPR-width and other scanner filters). Replaces the legacy scanIncludeNS/NL/IS/IL
        // bucket toggles (which filtered by Narrow/Inside CPR × Small/Large daily range).
        volatile boolean scanOnlyNifty50 = true;
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
    public boolean isEnableSmaTrendCheck()          { return cfg().enableSmaTrendCheck; }
    public boolean isEnableSmaTrendCheckLenient()   { return cfg().enableSmaTrendCheckLenient; }
    public boolean isEnableSmaAlignmentCheck()      { return cfg().enableSmaAlignmentCheck; }
    public boolean isEnableSmaAlignmentCheckLenient() { return cfg().enableSmaAlignmentCheckLenient; }
    public boolean isEnableSmaVsAtpCheck()          { return cfg().enableSmaVsAtpCheck; }
    public int    getSmaPatternLookback()           { return cfg().smaPatternLookback; }
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
    public double getMarubozuBodyAtrMult()         { return cfg().marubozuBodyAtrMult; }
    public double getMarubozuMaxWicksPctOfBody()   { return cfg().marubozuMaxWicksPctOfBody; }
    public double getGoodSizeCandleBodyAtrMult()          { return cfg().goodSizeCandleBodyAtrMult; }
    public double getGoodSizeCandleMaxOppositeWickRatio() { return cfg().goodSizeCandleMaxOppositeWickRatio; }
    public double getPinBarRejectionWickBodyMult() { return cfg().pinBarRejectionWickBodyMult; }
    public double getPinBarOppositeWickBodyMult()  { return cfg().pinBarOppositeWickBodyMult; }
    public double getPinBarSmallBodyMaxRangeRatio()    { return cfg().pinBarSmallBodyMaxRangeRatio; }
    public double getPinBarDominantWickMinRangeRatio() { return cfg().pinBarDominantWickMinRangeRatio; }
    public double getPinBarOppositeWickMaxRangeRatio() { return cfg().pinBarOppositeWickMaxRangeRatio; }
    public double getEngulfingMinBodyMultiple()    { return cfg().engulfingMinBodyMultiple; }
    public double getEngulfingMinBodyAtrMult()     { return cfg().engulfingMinBodyAtrMult; }
    public double getPiercingPrevBodyAtrMult()     { return cfg().piercingPrevBodyAtrMult; }
    public double getPiercingPenetrationPct()      { return cfg().piercingPenetrationPct; }
    public double getTweezerPrevBodyAtrMult()      { return cfg().tweezerPrevBodyAtrMult; }
    public double getTweezerLowHighMatchAtr()      { return cfg().tweezerLowHighMatchAtr; }
    public double getHaramiBodyAtrMult()           { return cfg().haramiBodyAtrMult; }
    public double getHaramiInnerBodyMaxRatio()     { return cfg().haramiInnerBodyMaxRatio; }
    public double getDojiBodyMaxRangeRatio()       { return cfg().dojiBodyMaxRangeRatio; }
    public double getDojiConfirmBodyAtrMult()      { return cfg().dojiConfirmBodyAtrMult; }
    public double getStarOuterBodyAtrMult()        { return cfg().starOuterBodyAtrMult; }
    public double getStarMiddleBodyMaxMultOfOuter() { return cfg().starMiddleBodyMaxMultOfOuter; }
    public double getLevelTouchToleranceAtr()      { return cfg().levelTouchToleranceAtr; }
    public boolean isEnableTrailingSl() { return cfg().enableTrailingSl; }
    public boolean isEnableSmaCrossExit() { return cfg().enableSmaCrossExit; }
    public boolean isEnablePriceSmaExit() { return cfg().enablePriceSmaExit; }
    public boolean isEnableNiftyReversalCprExit() { return cfg().enableNiftyReversalCprExit; }
    public boolean isEnableNiftyHtfHurdleExit()    { return cfg().enableNiftyHtfHurdleExit; }
    public int getPerSymbolDailyTradeLimit() { return cfg().perSymbolDailyTradeLimit; }
    public int getLptMaxTradesPerStockPerDay() { return cfg().lptMaxTradesPerStockPerDay; }
    public int getVirginCprExpiryDays() { return cfg().virginCprExpiryDays; }
    public boolean isEnableVirginCprHurdleFilter() { return cfg().enableVirginCprHurdleFilter; }
    public double  getVirginCprHurdleHeadroomAtr() { return cfg().virginCprHurdleHeadroomAtr; }
    public boolean isEnableVirginCprTouchExit()    { return cfg().enableVirginCprTouchExit; }
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
    public boolean isEnableMeanReversionTrades() { return cfg().enableMeanReversionTrades; }
    public int getAtrPeriod() { return cfg().atrPeriod; }
    public double getSmallCandleAtrThreshold() { return cfg().smallCandleAtrThreshold; }
    public double getSmallCandleBodyAtrThreshold() { return cfg().smallCandleBodyAtrThreshold; }
    public double getSmallCandleMoveAtrThreshold() { return cfg().smallCandleMoveAtrThreshold; }
    public double getWickRejectionRatio() { return cfg().wickRejectionRatio; }
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
    public boolean isScanOnlyNifty50() { return cfg().scanOnlyNifty50; }
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
    public void setScanOnlyNifty50(boolean v) { cfg().scanOnlyNifty50 = v; }
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
    public void setMarubozuBodyAtrMult(double v)         { cfg().marubozuBodyAtrMult = v; }
    public void setMarubozuMaxWicksPctOfBody(double v)   { cfg().marubozuMaxWicksPctOfBody = v; }
    public void setGoodSizeCandleBodyAtrMult(double v)          { cfg().goodSizeCandleBodyAtrMult = v; }
    public void setGoodSizeCandleMaxOppositeWickRatio(double v) { cfg().goodSizeCandleMaxOppositeWickRatio = v; }
    public void setPinBarRejectionWickBodyMult(double v) { cfg().pinBarRejectionWickBodyMult = v; }
    public void setPinBarOppositeWickBodyMult(double v)  { cfg().pinBarOppositeWickBodyMult = v; }
    public void setPinBarSmallBodyMaxRangeRatio(double v)    { cfg().pinBarSmallBodyMaxRangeRatio = Math.max(0, v); }
    public void setPinBarDominantWickMinRangeRatio(double v) { cfg().pinBarDominantWickMinRangeRatio = v; }
    public void setPinBarOppositeWickMaxRangeRatio(double v) { cfg().pinBarOppositeWickMaxRangeRatio = v; }
    public void setEngulfingMinBodyMultiple(double v)    { cfg().engulfingMinBodyMultiple = v; }
    public void setEngulfingMinBodyAtrMult(double v)     { cfg().engulfingMinBodyAtrMult = v; }
    public void setPiercingPrevBodyAtrMult(double v)     { cfg().piercingPrevBodyAtrMult = v; }
    public void setPiercingPenetrationPct(double v)      { cfg().piercingPenetrationPct = v; }
    public void setTweezerPrevBodyAtrMult(double v)      { cfg().tweezerPrevBodyAtrMult = v; }
    public void setTweezerLowHighMatchAtr(double v)      { cfg().tweezerLowHighMatchAtr = v; }
    public void setHaramiBodyAtrMult(double v)           { cfg().haramiBodyAtrMult = v; }
    public void setHaramiInnerBodyMaxRatio(double v)     { cfg().haramiInnerBodyMaxRatio = v; }
    public void setDojiBodyMaxRangeRatio(double v)       { cfg().dojiBodyMaxRangeRatio = v; }
    public void setDojiConfirmBodyAtrMult(double v)      { cfg().dojiConfirmBodyAtrMult = v; }
    public void setStarOuterBodyAtrMult(double v)        { cfg().starOuterBodyAtrMult = v; }
    public void setStarMiddleBodyMaxMultOfOuter(double v) { cfg().starMiddleBodyMaxMultOfOuter = v; }
    public void setLevelTouchToleranceAtr(double v)      { cfg().levelTouchToleranceAtr = Math.max(0, v); }
    public void setEnableTrailingSl(boolean v) { cfg().enableTrailingSl = v; }
    public void setEnableSmaCrossExit(boolean v) { cfg().enableSmaCrossExit = v; }
    public void setEnablePriceSmaExit(boolean v) { cfg().enablePriceSmaExit = v; }
    public void setEnableNiftyReversalCprExit(boolean v) { cfg().enableNiftyReversalCprExit = v; }
    public void setEnableNiftyHtfHurdleExit(boolean v)   { cfg().enableNiftyHtfHurdleExit = v; }
    public void setPerSymbolDailyTradeLimit(int v) { cfg().perSymbolDailyTradeLimit = Math.max(0, v); }
    public void setLptMaxTradesPerStockPerDay(int v) { cfg().lptMaxTradesPerStockPerDay = Math.max(0, v); }
    public void setVirginCprExpiryDays(int v) { cfg().virginCprExpiryDays = Math.max(0, v); }
    public void setEnableVirginCprHurdleFilter(boolean v) { cfg().enableVirginCprHurdleFilter = v; }
    public void setVirginCprHurdleHeadroomAtr(double v)   { cfg().virginCprHurdleHeadroomAtr = Math.max(0, v); }
    public void setEnableVirginCprTouchExit(boolean v)    { cfg().enableVirginCprTouchExit = v; }
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
    public void setEnableMeanReversionTrades(boolean v) {
        cfg().enableMeanReversionTrades = v;
        // Mean-reversion setups classify as MPT downstream — turning on the master toggle
        // also turns on the MPT tier gate so the trades aren't silently blocked by a stale
        // enableMpt=false value. One-directional: turning mean-rev OFF leaves enableMpt alone.
        if (v) cfg().enableMpt = true;
    }
    public void setAtrPeriod(int v) { cfg().atrPeriod = v; }
    public void setSmallCandleAtrThreshold(double v) { cfg().smallCandleAtrThreshold = v; }
    public void setSmallCandleBodyAtrThreshold(double v) { cfg().smallCandleBodyAtrThreshold = v; }
    public void setSmallCandleMoveAtrThreshold(double v) { cfg().smallCandleMoveAtrThreshold = v; }
    public void setWickRejectionRatio(double v) { cfg().wickRejectionRatio = v; }
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
            upsert("enableSmaTrendCheck", String.valueOf(c.enableSmaTrendCheck));
            upsert("enableSmaTrendCheckLenient", String.valueOf(c.enableSmaTrendCheckLenient));
            upsert("enableSmaAlignmentCheck", String.valueOf(c.enableSmaAlignmentCheck));
            upsert("enableSmaAlignmentCheckLenient", String.valueOf(c.enableSmaAlignmentCheckLenient));
            upsert("enableSmaVsAtpCheck", String.valueOf(c.enableSmaVsAtpCheck));
            upsert("smaPatternLookback", String.valueOf(c.smaPatternLookback));
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
            upsert("marubozuBodyAtrMult", String.valueOf(c.marubozuBodyAtrMult));
            upsert("marubozuMaxWicksPctOfBody", String.valueOf(c.marubozuMaxWicksPctOfBody));
            upsert("goodSizeCandleBodyAtrMult", String.valueOf(c.goodSizeCandleBodyAtrMult));
            upsert("goodSizeCandleMaxOppositeWickRatio", String.valueOf(c.goodSizeCandleMaxOppositeWickRatio));
            upsert("pinBarRejectionWickBodyMult", String.valueOf(c.pinBarRejectionWickBodyMult));
            upsert("pinBarOppositeWickBodyMult", String.valueOf(c.pinBarOppositeWickBodyMult));
            upsert("pinBarSmallBodyMaxRangeRatio", String.valueOf(c.pinBarSmallBodyMaxRangeRatio));
            upsert("pinBarDominantWickMinRangeRatio", String.valueOf(c.pinBarDominantWickMinRangeRatio));
            upsert("pinBarOppositeWickMaxRangeRatio", String.valueOf(c.pinBarOppositeWickMaxRangeRatio));
            upsert("engulfingMinBodyMultiple", String.valueOf(c.engulfingMinBodyMultiple));
            upsert("engulfingMinBodyAtrMult",  String.valueOf(c.engulfingMinBodyAtrMult));
            upsert("piercingPrevBodyAtrMult",  String.valueOf(c.piercingPrevBodyAtrMult));
            upsert("piercingPenetrationPct",   String.valueOf(c.piercingPenetrationPct));
            upsert("tweezerPrevBodyAtrMult",   String.valueOf(c.tweezerPrevBodyAtrMult));
            upsert("tweezerLowHighMatchAtr",   String.valueOf(c.tweezerLowHighMatchAtr));
            upsert("haramiBodyAtrMult",        String.valueOf(c.haramiBodyAtrMult));
            upsert("haramiInnerBodyMaxRatio",  String.valueOf(c.haramiInnerBodyMaxRatio));
            upsert("dojiBodyMaxRangeRatio", String.valueOf(c.dojiBodyMaxRangeRatio));
            upsert("dojiConfirmBodyAtrMult", String.valueOf(c.dojiConfirmBodyAtrMult));
            upsert("starOuterBodyAtrMult", String.valueOf(c.starOuterBodyAtrMult));
            upsert("starMiddleBodyMaxMultOfOuter", String.valueOf(c.starMiddleBodyMaxMultOfOuter));
            upsert("levelTouchToleranceAtr", String.valueOf(c.levelTouchToleranceAtr));
            upsert("smallCandleAtrThreshold", String.valueOf(c.smallCandleAtrThreshold));
            upsert("smallCandleBodyAtrThreshold", String.valueOf(c.smallCandleBodyAtrThreshold));
            upsert("smallCandleMoveAtrThreshold", String.valueOf(c.smallCandleMoveAtrThreshold));
            upsert("wickRejectionRatio", String.valueOf(c.wickRejectionRatio));
            upsert("enableVolumeFilter", String.valueOf(c.enableVolumeFilter));
            upsert("volumeMultiple", String.valueOf(c.volumeMultiple));
            upsert("volumeLookback", String.valueOf(c.volumeLookback));
            upsert("enableTrailingSl", String.valueOf(c.enableTrailingSl));
            upsert("enableSmaCrossExit", String.valueOf(c.enableSmaCrossExit));
            upsert("enablePriceSmaExit", String.valueOf(c.enablePriceSmaExit));
            upsert("enableNiftyReversalCprExit", String.valueOf(c.enableNiftyReversalCprExit));
            upsert("enableNiftyHtfHurdleExit",   String.valueOf(c.enableNiftyHtfHurdleExit));
            upsert("perSymbolDailyTradeLimit", String.valueOf(c.perSymbolDailyTradeLimit));
            upsert("lptMaxTradesPerStockPerDay", String.valueOf(c.lptMaxTradesPerStockPerDay));
            upsert("virginCprExpiryDays", String.valueOf(c.virginCprExpiryDays));
            upsert("enableVirginCprHurdleFilter", String.valueOf(c.enableVirginCprHurdleFilter));
            upsert("virginCprHurdleHeadroomAtr",  String.valueOf(c.virginCprHurdleHeadroomAtr));
            upsert("enableVirginCprTouchExit",    String.valueOf(c.enableVirginCprTouchExit));
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
            upsert("enableMeanReversionTrades", String.valueOf(c.enableMeanReversionTrades));
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
            upsert("scanOnlyNifty50", String.valueOf(c.scanOnlyNifty50));
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
                    case "enableDayHighLowTargetShift" -> c.enableDayHighLowTargetShift = Boolean.parseBoolean(v);
                    case "enableDailySma200TargetShift" -> c.enableDailySma200TargetShift = Boolean.parseBoolean(v);
                    case "dayHighLowShiftMinDistAtr" -> c.dayHighLowShiftMinDistAtr = Double.parseDouble(v);
                    case "enableWeeklyLevelTargetShift" -> c.enableWeeklyLevelTargetShift = Boolean.parseBoolean(v);
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
                    case "enableEmaTrendCheck", "enableSmaTrendCheck" -> c.enableSmaTrendCheck = Boolean.parseBoolean(v);
                    case "enableSmaTrendCheckLenient" -> c.enableSmaTrendCheckLenient = Boolean.parseBoolean(v);
                    case "enableSmaAlignmentCheck" -> c.enableSmaAlignmentCheck = Boolean.parseBoolean(v);
                    case "enableSmaAlignmentCheckLenient" -> c.enableSmaAlignmentCheckLenient = Boolean.parseBoolean(v);
                    // Legacy keys — silently ignored (semantics differ, no safe fold).
                    case "enableEmaDirectionCheck", "enableEma200DirectionCheck", "enableEmaCrossoverCheck" -> { /* legacy */ }
                    case "enableEmaVsAtpCheck", "enableSmaVsAtpCheck" -> c.enableSmaVsAtpCheck = Boolean.parseBoolean(v);
                    case "emaPatternLookback", "smaPatternLookback" -> c.smaPatternLookback = Integer.parseInt(v);
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
                    case "marubozuBodyAtrMult"         -> c.marubozuBodyAtrMult = Double.parseDouble(v);
                    case "marubozuMaxWicksPctOfBody"   -> c.marubozuMaxWicksPctOfBody = Double.parseDouble(v);
                    case "goodSizeCandleBodyAtrMult"          -> c.goodSizeCandleBodyAtrMult = Double.parseDouble(v);
                    case "goodSizeCandleMaxOppositeWickRatio" -> c.goodSizeCandleMaxOppositeWickRatio = Double.parseDouble(v);
                    case "pinBarRejectionWickBodyMult" -> c.pinBarRejectionWickBodyMult = Double.parseDouble(v);
                    case "pinBarOppositeWickBodyMult"  -> c.pinBarOppositeWickBodyMult = Double.parseDouble(v);
                    case "pinBarSmallBodyMaxRangeRatio"    -> c.pinBarSmallBodyMaxRangeRatio = Math.max(0, Double.parseDouble(v));
                    case "pinBarDominantWickMinRangeRatio" -> c.pinBarDominantWickMinRangeRatio = Double.parseDouble(v);
                    case "pinBarOppositeWickMaxRangeRatio" -> c.pinBarOppositeWickMaxRangeRatio = Double.parseDouble(v);
                    case "engulfingMinBodyMultiple"    -> c.engulfingMinBodyMultiple = Double.parseDouble(v);
                    case "engulfingMinBodyAtrMult"     -> c.engulfingMinBodyAtrMult = Double.parseDouble(v);
                    case "piercingPrevBodyAtrMult"     -> c.piercingPrevBodyAtrMult = Double.parseDouble(v);
                    case "piercingPenetrationPct"      -> c.piercingPenetrationPct = Double.parseDouble(v);
                    case "tweezerPrevBodyAtrMult"      -> c.tweezerPrevBodyAtrMult = Double.parseDouble(v);
                    case "tweezerLowHighMatchAtr"      -> c.tweezerLowHighMatchAtr = Double.parseDouble(v);
                    case "haramiBodyAtrMult"           -> c.haramiBodyAtrMult = Double.parseDouble(v);
                    case "haramiInnerBodyMaxRatio"     -> c.haramiInnerBodyMaxRatio = Double.parseDouble(v);
                    case "dojiBodyMaxRangeRatio"       -> c.dojiBodyMaxRangeRatio = Double.parseDouble(v);
                    case "dojiConfirmBodyAtrMult"      -> c.dojiConfirmBodyAtrMult = Double.parseDouble(v);
                    case "dojiPrevBodyAtrMult"         -> c.dojiConfirmBodyAtrMult = Double.parseDouble(v); // legacy key
                    case "starOuterBodyAtrMult"        -> c.starOuterBodyAtrMult = Double.parseDouble(v);
                    case "starMiddleBodyMaxMultOfOuter" -> c.starMiddleBodyMaxMultOfOuter = Double.parseDouble(v);
                    case "levelTouchToleranceAtr"      -> c.levelTouchToleranceAtr = Math.max(0, Double.parseDouble(v));
                    case "smallCandleAtrThreshold" -> {
                        // Legacy single knob: also seed the new split fields if those keys haven't been
                        // saved yet. Once the user saves the new fields explicitly, this no-op overwrites
                        // are harmless because save() writes both keys.
                        c.smallCandleAtrThreshold = Double.parseDouble(v);
                    }
                    case "smallCandleBodyAtrThreshold" -> c.smallCandleBodyAtrThreshold = Double.parseDouble(v);
                    case "smallCandleMoveAtrThreshold" -> c.smallCandleMoveAtrThreshold = Double.parseDouble(v);
                    case "wickRejectionRatio" -> c.wickRejectionRatio = Double.parseDouble(v);
                    case "enableVolumeFilter" -> c.enableVolumeFilter = Boolean.parseBoolean(v);
                    case "volumeMultiple" -> c.volumeMultiple = Double.parseDouble(v);
                    case "volumeLookback" -> c.volumeLookback = Integer.parseInt(v);
                    case "enableTrailingSl"   -> c.enableTrailingSl = Boolean.parseBoolean(v);
                    case "enableSmaCrossExit" -> c.enableSmaCrossExit = Boolean.parseBoolean(v);
                    case "enablePriceSmaExit" -> c.enablePriceSmaExit = Boolean.parseBoolean(v);
                    case "enableNiftyReversalCprExit" -> c.enableNiftyReversalCprExit = Boolean.parseBoolean(v);
                    case "enableNiftyHtfHurdleExit"   -> c.enableNiftyHtfHurdleExit = Boolean.parseBoolean(v);
                    case "perSymbolDailyTradeLimit" -> c.perSymbolDailyTradeLimit = Math.max(0, Integer.parseInt(v));
                    case "lptMaxTradesPerStockPerDay" -> c.lptMaxTradesPerStockPerDay = Math.max(0, Integer.parseInt(v));
                    case "virginCprExpiryDays" -> c.virginCprExpiryDays = Math.max(0, Integer.parseInt(v));
                    case "enableVirginCprHurdleFilter" -> c.enableVirginCprHurdleFilter = Boolean.parseBoolean(v);
                    case "virginCprHurdleHeadroomAtr" -> c.virginCprHurdleHeadroomAtr = Math.max(0, Double.parseDouble(v));
                    case "enableVirginCprTouchExit" -> c.enableVirginCprTouchExit = Boolean.parseBoolean(v);
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
                    case "enableMeanReversionTrades" -> c.enableMeanReversionTrades = Boolean.parseBoolean(v);
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
                    case "scanOnlyNifty50" -> c.scanOnlyNifty50 = Boolean.parseBoolean(v);
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
            log.info("[RiskSettingsStore] Loaded {}: start={} end={} totalCapital={} maxRiskPerDayPct={}% riskPerTrade={} autoSquareOff={} atrMult={} brokerage={} fixedQty={} capitalPerTrade={} trailingSl={} skipR3S3(IvOv/Ev)={}/{} skipR4S4(IvOv/Ev)={}/{}", mode, c.tradingStartTime, c.tradingEndTime, c.totalCapital, c.maxRiskPerDayPct, c.riskPerTrade, c.autoSquareOffTime, c.atrMultiplier, c.brokeragePerOrder, c.fixedQuantity, c.capitalPerTrade, c.enableTrailingSl, c.skipR3S3IvOvDays, c.skipR3S3EvDays, c.skipR4S4IvOvDays, c.skipR4S4EvDays);
        } catch (Exception e) {
            log.error("[RiskSettingsStore] Failed to load {}: {}", mode, e.getMessage());
        }
    }
}
