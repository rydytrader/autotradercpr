package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Swing breakout scanner for weekly narrow/inside CPR stocks.
 * Listens to 75-min candle closes from SwingCandleAggregator and detects
 * breakouts against weekly CPR levels (TC/BC, R1-R4, S1-S4, PWH, PWL).
 * Uses monthly CPR trend for HPT/MPT/LPT probability.
 */
@Service
public class SwingScanner implements SwingCandleAggregator.SwingCandleCloseListener, CandleAggregator.DailyResetListener {

    private static final Logger log = LoggerFactory.getLogger(SwingScanner.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm");

    private final WeeklyCprService weeklyCprService;
    private final WeeklyAtrService weeklyAtrService;
    private final WeeklyVwapService weeklyVwapService;
    private final MonthlyCprService monthlyCprService;
    private final CandleAggregator candleAggregator;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;

    // Track which weekly levels have been broken this week per symbol (prevents re-fire)
    private final ConcurrentHashMap<String, Set<String>> brokenWeeklyLevels = new ConcurrentHashMap<>();

    // Last swing signal per symbol (for scanner dashboard)
    private final ConcurrentHashMap<String, SwingSignalInfo> lastSwingSignal = new ConcurrentHashMap<>();

    // Signal history for the week
    private final ConcurrentHashMap<String, List<SwingSignalInfo>> swingSignalHistory = new ConcurrentHashMap<>();

    private volatile int tradedCountThisWeek = 0;
    private volatile int filteredCountThisWeek = 0;

    public SwingScanner(WeeklyCprService weeklyCprService,
                        WeeklyAtrService weeklyAtrService,
                        WeeklyVwapService weeklyVwapService,
                        MonthlyCprService monthlyCprService,
                        CandleAggregator candleAggregator,
                        RiskSettingsStore riskSettings,
                        EventService eventService) {
        this.weeklyCprService = weeklyCprService;
        this.weeklyAtrService = weeklyAtrService;
        this.weeklyVwapService = weeklyVwapService;
        this.monthlyCprService = monthlyCprService;
        this.candleAggregator = candleAggregator;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
    }

    @Override
    public void onSwingCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        // Only scan if signal source is INTERNAL
        if (!"INTERNAL".equalsIgnoreCase(riskSettings.getSignalSource())) return;

        try {
            scanForWeeklyBreakout(fyersSymbol, completedCandle);
        } catch (Exception e) {
            log.error("[SwingScanner] Error scanning {}: {}", fyersSymbol, e.getMessage());
        }
    }

    @Override
    public void onDailyReset() {
        // Weekly levels persist across the week — only reset on Monday
        // (handled by weeklyReset() which should be called at week boundary)
    }

    /** Call on Monday morning to reset broken levels for the new week. */
    public void weeklyReset() {
        brokenWeeklyLevels.clear();
        lastSwingSignal.clear();
        swingSignalHistory.clear();
        tradedCountThisWeek = 0;
        filteredCountThisWeek = 0;
        log.info("[SwingScanner] Weekly reset — cleared broken levels and signals");
    }

    private void scanForWeeklyBreakout(String fyersSymbol, CandleAggregator.CandleBar candle) {
        WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(fyersSymbol);
        if (wl == null) {
            log.debug("[SwingScanner] {} — no weekly levels available", fyersSymbol);
            return;
        }

        double weeklyAtr = weeklyAtrService.getWeeklyAtr(fyersSymbol);
        if (weeklyAtr <= 0) {
            log.debug("[SwingScanner] {} — no weekly ATR available", fyersSymbol);
            return;
        }

        // Already in CNC position for this symbol?
        // TODO: check CNC positions specifically (PositionManager currently tracks by symbol only)
        String pos = PositionManager.getPosition(fyersSymbol);
        if (!"NONE".equals(pos)) return;

        double open = candle.open, close = candle.close, high = candle.high, low = candle.low;
        boolean greenCandle = close > open;
        boolean redCandle = close < open;

        // Weekly VWAP check (analogous to daily ATP check)
        double weeklyVwap = weeklyVwapService.getWeeklyVwap(fyersSymbol);

        Set<String> broken = brokenWeeklyLevels.getOrDefault(fyersSymbol, Collections.emptySet());

        // BUY signals — green candle
        if (greenCandle) {
            String buySetup = detectWeeklyBuyBreakout(open, high, low, close, wl, weeklyVwap, broken);
            if (buySetup != null) {
                double ltp = candleAggregator.getLtp(fyersSymbol);
                String prob = monthlyCprService.getSwingProbability(fyersSymbol, ltp, "BUY");
                recordSignal(fyersSymbol, buySetup, "BUY", close, weeklyAtr, prob, candle);
            }
        }

        // SELL signals — red candle
        if (redCandle) {
            String sellSetup = detectWeeklySellBreakout(open, high, low, close, wl, weeklyVwap, broken);
            if (sellSetup != null) {
                double ltp = candleAggregator.getLtp(fyersSymbol);
                String prob = monthlyCprService.getSwingProbability(fyersSymbol, ltp, "SELL");
                recordSignal(fyersSymbol, sellSetup, "SELL", close, weeklyAtr, prob, candle);
            }
        }
    }

    /**
     * Detect buy breakout against weekly CPR levels.
     * Priority: R4 > R3 > R2 > R1/PWH > CPR > S1/PWL
     * Same two-path logic as BreakoutScanner:
     *   Path 1: open or low below level, close above
     *   Path 2: open above level, low dips below, close above (wick rejection)
     */
    private String detectWeeklyBuyBreakout(double open, double high, double low, double close,
                                            WeeklyCprService.WeeklyLevels wl, double vwap, Set<String> broken) {
        if (vwap > 0 && close < vwap) return null; // VWAP check

        double cprTop = wl.top;

        if (close > wl.r4 && ((open < wl.r4 || low < wl.r4) || (low < wl.r4 && open > wl.r4)) && !broken.contains("SWING_BUY_R4")) return "SWING_BUY_R4";
        if (close > wl.r3 && ((open < wl.r3 || low < wl.r3) || (low < wl.r3 && open > wl.r3)) && !broken.contains("SWING_BUY_R3")) return "SWING_BUY_R3";
        if (close > wl.r2 && ((open < wl.r2 || low < wl.r2) || (low < wl.r2 && open > wl.r2)) && !broken.contains("SWING_BUY_R2")) return "SWING_BUY_R2";

        // R1 / PWH — use higher of the two
        double r1ph = Math.max(wl.r1, wl.ph);
        if (close > r1ph && ((open < r1ph || low < r1ph) || (low < r1ph && open > r1ph)) && !broken.contains("SWING_BUY_R1")) return "SWING_BUY_R1";

        // CPR breakout (above CPR top)
        if (close > cprTop && ((open < cprTop || low < cprTop) || (low < cprTop && open > cprTop)) && !broken.contains("SWING_BUY_CPR")) return "SWING_BUY_CPR";

        // S1 / PWL — use higher of the two (buying above support)
        double s1pl = Math.max(wl.s1, wl.pl);
        if (close > s1pl && ((open < s1pl || low < s1pl) || (low < s1pl && open > s1pl)) && !broken.contains("SWING_BUY_S1")) return "SWING_BUY_S1";

        return null;
    }

    /**
     * Detect sell breakout against weekly CPR levels.
     * Priority: S4 > S3 > S2 > S1/PWL > CPR > R1/PWH
     */
    private String detectWeeklySellBreakout(double open, double high, double low, double close,
                                             WeeklyCprService.WeeklyLevels wl, double vwap, Set<String> broken) {
        if (vwap > 0 && close > vwap) return null; // VWAP check

        double cprBot = wl.bot;

        if (close < wl.s4 && ((open > wl.s4 || high > wl.s4) || (high > wl.s4 && open < wl.s4)) && !broken.contains("SWING_SELL_S4")) return "SWING_SELL_S4";
        if (close < wl.s3 && ((open > wl.s3 || high > wl.s3) || (high > wl.s3 && open < wl.s3)) && !broken.contains("SWING_SELL_S3")) return "SWING_SELL_S3";
        if (close < wl.s2 && ((open > wl.s2 || high > wl.s2) || (high > wl.s2 && open < wl.s2)) && !broken.contains("SWING_SELL_S2")) return "SWING_SELL_S2";

        double s1pl = Math.min(wl.s1, wl.pl);
        if (close < s1pl && ((open > s1pl || high > s1pl) || (high > s1pl && open < s1pl)) && !broken.contains("SWING_SELL_S1")) return "SWING_SELL_S1";

        if (close < cprBot && ((open > cprBot || high > cprBot) || (high > cprBot && open < cprBot)) && !broken.contains("SWING_SELL_CPR")) return "SWING_SELL_CPR";

        double r1ph = Math.min(wl.r1, wl.ph);
        if (close < r1ph && ((open > r1ph || high > r1ph) || (high > r1ph && open < r1ph)) && !broken.contains("SWING_SELL_R1")) return "SWING_SELL_R1";

        return null;
    }

    private void recordSignal(String fyersSymbol, String setup, String side, double close,
                              double weeklyAtr, String probability, CandleAggregator.CandleBar candle) {
        // Mark level as broken
        brokenWeeklyLevels.computeIfAbsent(fyersSymbol, k -> ConcurrentHashMap.newKeySet()).add(setup);

        String time = ZonedDateTime.now(IST).toLocalTime().format(TIME_FMT);
        SwingSignalInfo info = new SwingSignalInfo();
        info.setup = setup;
        info.side = side;
        info.time = time;
        info.probability = probability;
        info.close = close;
        info.weeklyAtr = weeklyAtr;
        info.status = "DETECTED";

        lastSwingSignal.put(fyersSymbol, info);
        swingSignalHistory.computeIfAbsent(fyersSymbol, k -> Collections.synchronizedList(new ArrayList<>())).add(info);

        String logMsg = "[SWING] " + setup + " for " + fyersSymbol
            + " @ " + String.format("%.2f", close)
            + " | ATR=" + String.format("%.2f", weeklyAtr)
            + " | " + probability
            + " | 75min O=" + String.format("%.2f", candle.open)
            + " H=" + String.format("%.2f", candle.high)
            + " L=" + String.format("%.2f", candle.low)
            + " C=" + String.format("%.2f", candle.close);
        log.info(logMsg);
        eventService.log(logMsg);

        // TODO: Phase 4 — feed into SignalProcessor with strategy=SWING, productType=CNC
        tradedCountThisWeek++;
    }

    // ── Public API for dashboard ─────────────────────────────────────────────

    public SwingSignalInfo getLastSwingSignal(String fyersSymbol) {
        return lastSwingSignal.get(fyersSymbol);
    }

    public List<SwingSignalInfo> getSwingSignalHistory(String fyersSymbol) {
        return swingSignalHistory.getOrDefault(fyersSymbol, Collections.emptyList());
    }

    public Map<String, SwingSignalInfo> getAllSwingSignals() {
        return Collections.unmodifiableMap(lastSwingSignal);
    }

    public int getTradedCountThisWeek() { return tradedCountThisWeek; }
    public int getFilteredCountThisWeek() { return filteredCountThisWeek; }

    public Set<String> getBrokenWeeklyLevels(String fyersSymbol) {
        return brokenWeeklyLevels.get(fyersSymbol);
    }

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        s = s.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        return s;
    }

    // ── Signal info ─────────────────────────────────────────────────────────

    public static class SwingSignalInfo {
        public String setup;
        public String side;
        public String time;
        public String probability;
        public double close;
        public double weeklyAtr;
        public String status; // DETECTED, TRADED, FILTERED
    }
}
