package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.controller.TradingController;
import com.rydytrader.autotrader.dto.CprLevels;
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
 * Internal CPR breakout scanner.
 * Listens for completed 15-min candles, detects breakouts against CPR levels,
 * and feeds signals into the existing trading pipeline.
 */
@Service
public class BreakoutScanner implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(BreakoutScanner.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm");

    private final BhavcopyService bhavcopyService;
    private final AtrService atrService;
    private final WeeklyCprService weeklyCprService;
    private final CandleAggregator candleAggregator;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private TradingController tradingController;

    // Track which levels have been broken today per symbol (prevents re-fire)
    private final ConcurrentHashMap<String, Set<String>> brokenLevels = new ConcurrentHashMap<>();

    // Track signals generated today (for scanner dashboard)
    private final ConcurrentHashMap<String, SignalInfo> lastSignal = new ConcurrentHashMap<>();

    // Watchlist symbols (set by MarketDataService)
    private volatile List<String> watchlistSymbols = Collections.emptyList();

    public BreakoutScanner(BhavcopyService bhavcopyService,
                           AtrService atrService,
                           WeeklyCprService weeklyCprService,
                           CandleAggregator candleAggregator,
                           RiskSettingsStore riskSettings,
                           EventService eventService) {
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.candleAggregator = candleAggregator;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
    }

    public void setWatchlistSymbols(List<String> symbols) {
        this.watchlistSymbols = symbols;
    }

    // ── CandleCloseListener ─────────────────────────────────────────────────

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        // Only scan if signal source is INTERNAL
        if (!"INTERNAL".equalsIgnoreCase(riskSettings.getSignalSource())) return;
        if (!watchlistSymbols.contains(fyersSymbol)) return;

        try {
            scanForBreakout(fyersSymbol, completedCandle);
        } catch (Exception e) {
            log.error("[Scanner] Error scanning {}: {}", fyersSymbol, e.getMessage());
        }
    }

    /**
     * Check if a completed candle breaks any CPR level.
     */
    private void scanForBreakout(String fyersSymbol, CandleAggregator.CandleBar candle) {
        String ticker = extractTicker(fyersSymbol);
        CprLevels levels = bhavcopyService.getCprLevels(ticker);
        if (levels == null) return;

        double atr = atrService.getAtr(fyersSymbol);
        if (atr <= 0) return; // no ATR available

        double open = candle.open;
        double close = candle.close;
        boolean greenCandle = close > open;
        boolean redCandle = close < open;

        // VWAP check
        double vwap = candleAggregator.getVwap(fyersSymbol);

        // Already in position for this symbol?
        if (!"NONE".equals(PositionManager.getPosition(fyersSymbol))) return;

        Set<String> broken = brokenLevels.getOrDefault(fyersSymbol, Collections.emptySet());

        double low = candle.low;
        double high = candle.high;

        // Check BUY signals — requires green candle (close > open)
        if (greenCandle) {
            String buySetup = detectBuyBreakout(open, high, low, close, levels, vwap, broken);
            if (buySetup != null) {
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, true);
                if (!isProbabilityEnabled(prob)) return;
                fireSignal(fyersSymbol, buySetup, open, high, low, close, candle.volume, atr, levels, prob);
                return;
            }
        }

        // Check SELL signals — requires red candle (close < open)
        if (redCandle) {
            String sellSetup = detectSellBreakout(open, high, low, close, levels, vwap, broken);
            if (sellSetup != null) {
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, false);
                if (!isProbabilityEnabled(prob)) return;
                fireSignal(fyersSymbol, sellSetup, open, high, low, close, candle.volume, atr, levels, prob);
            }
        }
    }

    /**
     * Detect buy breakout — returns setup name or null.
     * Priority: R4 > R3 > R2 > R1/PDH > CPR > S1/PDL (only if LPT enabled)
     * Two paths per level:
     *   Path 1 (standard breakout): open or low below level, close above — candle broke through
     *   Path 2 (wick rejection):    open above level, low dips below level, close above — buyers defended
     */
    private String detectBuyBreakout(double open, double high, double low, double close,
                                      CprLevels levels, double vwap, Set<String> broken) {
        // VWAP check for buys: close must be above VWAP
        if (riskSettings.isEnableVwapCheck() && vwap > 0 && close < vwap) return null;

        double r4 = levels.getR4(), r3 = levels.getR3(), r2 = levels.getR2();
        double r1 = levels.getR1(), ph = levels.getPh();
        double pp = levels.getPivot(), tc = levels.getTc(), bc = levels.getBc();
        double s1 = levels.getS1(), pl = levels.getPl();

        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);

        // Check from highest to lowest — standard breakout OR wick rejection
        if (riskSettings.isEnableR4S4() && close > r4
                && ((open < r4 || low < r4) || (low < r4 && open > r4))
                && !broken.contains("BUY_ABOVE_R4")) return "BUY_ABOVE_R4";
        if (close > r3
                && ((open < r3 || low < r3) || (low < r3 && open > r3))
                && !broken.contains("BUY_ABOVE_R3")) return "BUY_ABOVE_R3";
        if (close > r2
                && ((open < r2 || low < r2) || (low < r2 && open > r2))
                && !broken.contains("BUY_ABOVE_R2")) return "BUY_ABOVE_R2";
        if (close > r1 && close > ph
                && ((open < r1 || open < ph || low < r1 || low < ph) || (low < Math.min(r1, ph) && open > Math.min(r1, ph)))
                && !broken.contains("BUY_ABOVE_R1_PDH")) return "BUY_ABOVE_R1_PDH";
        if (close > cprTop
                && ((open < pp || open < tc || open < bc || low < pp || low < tc || low < bc) || (low < cprBot && open > cprBot && close > cprTop))
                && !broken.contains("BUY_ABOVE_CPR")) return "BUY_ABOVE_CPR";
        if (riskSettings.isEnableLpt() && close > s1 && close > pl
                && ((open < s1 || open < pl || low < s1 || low < pl) || (low < Math.min(s1, pl) && open > Math.min(s1, pl)))
                && !broken.contains("BUY_ABOVE_S1_PDL")) return "BUY_ABOVE_S1_PDL";

        return null;
    }

    /**
     * Detect sell breakout — returns setup name or null.
     * Priority: S4 > S3 > S2 > S1/PDL > CPR > R1/PDH (only if LPT enabled)
     * Two paths per level:
     *   Path 1 (standard breakdown): open or high above level, close below — candle broke through
     *   Path 2 (wick rejection):     open below level, high pokes above level, close below — sellers defended
     */
    private String detectSellBreakout(double open, double high, double low, double close,
                                       CprLevels levels, double vwap, Set<String> broken) {
        // VWAP check for sells: close must be below VWAP
        if (riskSettings.isEnableVwapCheck() && vwap > 0 && close > vwap) return null;

        double s4 = levels.getS4(), s3 = levels.getS3(), s2 = levels.getS2();
        double s1 = levels.getS1(), pl = levels.getPl();
        double pp = levels.getPivot(), tc = levels.getTc(), bc = levels.getBc();
        double r1 = levels.getR1(), ph = levels.getPh();

        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);

        // Check from highest to lowest — standard breakdown OR wick rejection
        if (riskSettings.isEnableR4S4() && close < s4
                && ((open > s4 || high > s4) || (high > s4 && open < s4))
                && !broken.contains("SELL_BELOW_S4")) return "SELL_BELOW_S4";
        if (close < s3
                && ((open > s3 || high > s3) || (high > s3 && open < s3))
                && !broken.contains("SELL_BELOW_S3")) return "SELL_BELOW_S3";
        if (close < s2
                && ((open > s2 || high > s2) || (high > s2 && open < s2))
                && !broken.contains("SELL_BELOW_S2")) return "SELL_BELOW_S2";
        if (close < s1 && close < pl
                && ((open > s1 || open > pl || high > s1 || high > pl) || (high > Math.max(s1, pl) && open < Math.max(s1, pl)))
                && !broken.contains("SELL_BELOW_S1_PDL")) return "SELL_BELOW_S1_PDL";
        if (close < cprBot
                && ((open > pp || open > tc || open > bc || high > pp || high > tc || high > bc) || (high > cprTop && open < cprTop && close < cprBot))
                && !broken.contains("SELL_BELOW_CPR")) return "SELL_BELOW_CPR";
        if (riskSettings.isEnableLpt() && close < r1 && close < ph
                && ((open > r1 || open > ph || high > r1 || high > ph) || (high > Math.max(r1, ph) && open < Math.max(r1, ph)))
                && !broken.contains("SELL_BELOW_R1_PDH")) return "SELL_BELOW_R1_PDH";

        return null;
    }

    /**
     * Build signal payload and feed into TradingController.
     */
    private void fireSignal(String fyersSymbol, String setup, double open, double high,
                            double low, double close, long candleVolume, double atr, CprLevels levels, String prob) {
        String timeStr = ZonedDateTime.now(IST).toLocalTime().format(TIME_FMT);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("setup", setup);
        payload.put("symbol", fyersSymbol);
        payload.put("close", close);
        payload.put("candleOpen", open);
        payload.put("candleHigh", high);
        payload.put("candleLow", low);
        payload.put("candleVolume", candleVolume);
        payload.put("avgVolume", candleAggregator.getAvgVolume(fyersSymbol, riskSettings.getVolumeLookback()));
        payload.put("atr", atr);
        payload.put("dayOpen", candleAggregator.getDayOpen(fyersSymbol));
        payload.put("probability", prob);
        payload.put("r1", levels.getR1());
        payload.put("r2", levels.getR2());
        payload.put("r3", levels.getR3());
        payload.put("r4", levels.getR4());
        payload.put("s1", levels.getS1());
        payload.put("s2", levels.getS2());
        payload.put("s3", levels.getS3());
        payload.put("s4", levels.getS4());
        payload.put("ph", levels.getPh());
        payload.put("pl", levels.getPl());
        payload.put("tc", levels.getTc());
        payload.put("bc", levels.getBc());

        eventService.log("[SCANNER] " + setup + " for " + fyersSymbol + " | close=" + String.format("%.2f", close)
            + " | ATR=" + String.format("%.2f", atr) + " | " + prob + " | " + timeStr);

        // Feed into TradingController (same pipeline as TradingView webhook)
        try {
            var response = tradingController.receiveSignal(payload);
            String status = response.getBody() != null ? response.getBody() : "";

            // Track signal info for dashboard
            SignalInfo info = new SignalInfo();
            info.setup = setup;
            info.time = timeStr;
            boolean filtered = status.contains("failed") || status.contains("filtered") || status.contains("ignored");
            info.status = filtered ? "FILTERED" : "TRADED";
            info.detail = status;
            lastSignal.put(fyersSymbol, info);

            // Only mark level as broken if signal was actually traded
            if (!filtered) {
                brokenLevels.computeIfAbsent(fyersSymbol, k -> ConcurrentHashMap.newKeySet()).add(setup);
            }

        } catch (Exception e) {
            log.error("[Scanner] Failed to process signal for {}: {}", fyersSymbol, e.getMessage());
            SignalInfo info = new SignalInfo();
            info.setup = setup;
            info.time = timeStr;
            info.status = "ERROR";
            info.detail = e.getMessage();
            lastSignal.put(fyersSymbol, info);
        }
    }

    private boolean isProbabilityEnabled(String prob) {
        return switch (prob) {
            case "HPT" -> riskSettings.isEnableHpt();
            case "MPT" -> riskSettings.isEnableMpt();
            case "LPT" -> riskSettings.isEnableLpt();
            default -> false;
        };
    }

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        s = s.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        return s;
    }

    // ── Public API for scanner dashboard ─────────────────────────────────────

    /** Clear broken levels for a symbol when its position is closed. Allows re-entry. */
    public void clearBrokenLevels(String symbol) {
        brokenLevels.remove(symbol);
    }

    public Set<String> getBrokenLevels(String symbol) {
        return brokenLevels.getOrDefault(symbol, Collections.emptySet());
    }

    public SignalInfo getLastSignal(String symbol) {
        return lastSignal.get(symbol);
    }

    public Map<String, SignalInfo> getAllSignals() {
        return Collections.unmodifiableMap(lastSignal);
    }

    /** Clear all state for end of day. */
    public void clearAll() {
        brokenLevels.clear();
        lastSignal.clear();
    }

    // ── Signal info for dashboard ────────────────────────────────────────────

    public static class SignalInfo {
        public String setup;
        public String time;
        public String status; // TRADED, FILTERED, ERROR
        public String detail;
    }
}
