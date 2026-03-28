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

    // Track which levels have been broken today per symbol
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

        // Get previous candle for breakout comparison
        CandleAggregator.CandleBar prevCandle = candleAggregator.getPreviousCandle(fyersSymbol);
        double prevClose = prevCandle != null ? prevCandle.close : 0;
        double close = candle.close;

        // VWAP check
        double vwap = candleAggregator.getVwap(fyersSymbol);

        // Probability filter
        String prob = weeklyCprService.getProbability(fyersSymbol);
        if (!isProbabilityEnabled(prob)) return;

        // Already in position for this symbol?
        if (!"NONE".equals(PositionManager.getPosition(fyersSymbol))) return;

        // Check BUY signals (highest level wins)
        String buySetup = detectBuyBreakout(close, prevClose, levels, vwap);
        if (buySetup != null) {
            fireSignal(fyersSymbol, buySetup, close, atr, levels, prob);
            return;
        }

        // Check SELL signals (highest level wins)
        String sellSetup = detectSellBreakout(close, prevClose, levels, vwap);
        if (sellSetup != null) {
            fireSignal(fyersSymbol, sellSetup, close, atr, levels, prob);
        }
    }

    /**
     * Detect buy breakout — returns setup name or null.
     * Priority: R4 > R3 > R2 > R1/PDH > CPR > S1/PDL (only if LPT enabled)
     */
    private String detectBuyBreakout(double close, double prevClose, CprLevels levels, double vwap) {
        // VWAP check for buys: close must be above VWAP
        if (riskSettings.isEnableVwapCheck() && vwap > 0 && close < vwap) return null;

        double r4 = levels.getR4(), r3 = levels.getR3(), r2 = levels.getR2();
        double r1 = levels.getR1(), ph = levels.getPh();
        double tc = levels.getTc(), bc = levels.getBc();
        double s1 = levels.getS1(), pl = levels.getPl();

        double r1Pdh = Math.max(r1, ph);
        double cprTop = Math.max(tc, bc);
        double s1Pdl = Math.max(s1, pl);

        // Check from highest to lowest (dedup: only fire highest)
        if (riskSettings.isEnableR4S4() && close > r4 && prevClose <= r4) return "BUY_ABOVE_R4";
        if (close > r3 && prevClose <= r3) return "BUY_ABOVE_R3";
        if (close > r2 && prevClose <= r2) return "BUY_ABOVE_R2";
        if (close > r1Pdh && prevClose <= r1Pdh) return "BUY_ABOVE_R1_PDH";
        if (close > cprTop && prevClose <= cprTop) return "BUY_ABOVE_CPR";
        // S1/PDL buy is LPT-only (counter-trend)
        if (riskSettings.isEnableLpt() && close > s1Pdl && prevClose <= s1Pdl) return "BUY_ABOVE_S1_PDL";

        return null;
    }

    /**
     * Detect sell breakout — returns setup name or null.
     * Priority: S4 > S3 > S2 > S1/PDL > CPR > R1/PDH (only if LPT enabled)
     */
    private String detectSellBreakout(double close, double prevClose, CprLevels levels, double vwap) {
        // VWAP check for sells: close must be below VWAP
        if (riskSettings.isEnableVwapCheck() && vwap > 0 && close > vwap) return null;

        double s4 = levels.getS4(), s3 = levels.getS3(), s2 = levels.getS2();
        double s1 = levels.getS1(), pl = levels.getPl();
        double tc = levels.getTc(), bc = levels.getBc();
        double r1 = levels.getR1(), ph = levels.getPh();

        double s1Pdl = Math.min(s1, pl);
        double cprBot = Math.min(tc, bc);
        double r1Pdh = Math.min(r1, ph);

        if (riskSettings.isEnableR4S4() && close < s4 && prevClose >= s4) return "SELL_BELOW_S4";
        if (close < s3 && prevClose >= s3) return "SELL_BELOW_S3";
        if (close < s2 && prevClose >= s2) return "SELL_BELOW_S2";
        if (close < s1Pdl && prevClose >= s1Pdl) return "SELL_BELOW_S1_PDL";
        if (close < cprBot && prevClose >= cprBot) return "SELL_BELOW_CPR";
        if (riskSettings.isEnableLpt() && close < r1Pdh && prevClose >= r1Pdh) return "SELL_BELOW_R1_PDH";

        return null;
    }

    /**
     * Build signal payload and feed into TradingController.
     */
    private void fireSignal(String fyersSymbol, String setup, double close, double atr,
                            CprLevels levels, String prob) {
        String timeStr = ZonedDateTime.now(IST).toLocalTime().format(TIME_FMT);

        // Track broken level
        brokenLevels.computeIfAbsent(fyersSymbol, k -> ConcurrentHashMap.newKeySet()).add(setup);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("setup", setup);
        payload.put("symbol", fyersSymbol);
        payload.put("close", close);
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
            info.status = status.contains("failed") || status.contains("filtered") || status.contains("ignored")
                ? "FILTERED" : "TRADED";
            info.detail = status;
            lastSignal.put(fyersSymbol, info);

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
