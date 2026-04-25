package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.controller.TradingController;
import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
public class BreakoutScanner implements CandleAggregator.CandleCloseListener, CandleAggregator.DailyResetListener {

    private static final Logger log = LoggerFactory.getLogger(BreakoutScanner.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm");
    // Uses MarketHolidayService.MARKET_OPEN_MINUTE for market hours
    private static final String SCANNER_STATE_FILE = "../store/config/scanner-state.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Holds the current candle being scanned — used by DH/DL detection to compute prior day H/L
    private final ThreadLocal<CandleAggregator.CandleBar> currentCandle = new ThreadLocal<>();

    private final BhavcopyService bhavcopyService;
    private final AtrService atrService;
    private final WeeklyCprService weeklyCprService;
    private final CandleAggregator candleAggregator;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;
    private final LatencyTracker latencyTracker;
    private final SmaService smaService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private MarketDataService marketDataService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private TradingController tradingController;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private IndexTrendService indexTrendService;

    // Track which levels have been broken today per symbol (prevents re-fire)
    private final ConcurrentHashMap<String, Set<String>> brokenLevels = new ConcurrentHashMap<>();

    // Track signals generated today (for scanner dashboard)
    private final ConcurrentHashMap<String, SignalInfo> lastSignal = new ConcurrentHashMap<>();

    // Signal history for the day (all traded + filtered signals per symbol)
    private final ConcurrentHashMap<String, List<SignalInfo>> signalHistory = new ConcurrentHashMap<>();

    // Watchlist symbols (set by MarketDataService)
    private volatile List<String> watchlistSymbols = Collections.emptyList();

    // Last scan cycle stats
    private volatile int lastScanCount = 0;
    private volatile String lastScanTime = "";
    private volatile long lastScanBoundary = 0;
    private volatile int tradedCountToday = 0;
    private volatile int filteredCountToday = 0;

    public BreakoutScanner(BhavcopyService bhavcopyService,
                           AtrService atrService,
                           WeeklyCprService weeklyCprService,
                           CandleAggregator candleAggregator,
                           RiskSettingsStore riskSettings,
                           EventService eventService,
                           LatencyTracker latencyTracker,
                           SmaService smaService) {
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.candleAggregator = candleAggregator;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.latencyTracker = latencyTracker;
        this.smaService = smaService;
        loadState();
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

        // Check if stock passes the CPR Width Scanner settings (NS/NL/IS/IL)
        if (!isBreakoutEligible(fyersSymbol)) return;

        // Skip candles that started before market open
        if (completedCandle.startMinute < MarketHolidayService.MARKET_OPEN_MINUTE) return;

        // Track scan cycle — reset counter when boundary changes
        if (completedCandle.startMinute != lastScanBoundary) {
            lastScanBoundary = completedCandle.startMinute;
            lastScanCount = 0;
        }
        lastScanCount++;
        lastScanTime = ZonedDateTime.now(IST).toLocalTime().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm"));

        try {
            log.info("[Scanner] Candle close: {} start={} O={} H={} L={} C={}",
                fyersSymbol, completedCandle.startMinute,
                String.format("%.2f", completedCandle.open), String.format("%.2f", completedCandle.high),
                String.format("%.2f", completedCandle.low), String.format("%.2f", completedCandle.close));
            scanForBreakout(fyersSymbol, completedCandle);
        } catch (Exception e) {
            log.error("[Scanner] Error scanning {}: {}", fyersSymbol, e.getMessage());
        }
    }

    /**
     * Check if a completed candle breaks any CPR level.
     */
    private void scanForBreakout(String fyersSymbol, CandleAggregator.CandleBar candle) {
        currentCandle.set(candle);
        try {
            scanForBreakoutInner(fyersSymbol, candle);
        } finally {
            currentCandle.remove();
        }
    }

    private void scanForBreakoutInner(String fyersSymbol, CandleAggregator.CandleBar candle) {
        String ticker = extractTicker(fyersSymbol);
        CprLevels levels = bhavcopyService.getCprLevels(ticker);
        if (levels == null) {
            eventService.log("[WARNING] " + fyersSymbol + " — no CPR levels available, skipping scan");
            return;
        }

        double atr = atrService.getAtr(fyersSymbol);
        if (atr <= 0) {
            eventService.log("[WARNING] " + fyersSymbol + " — no ATR available, skipping scan");
            return;
        }

        double open = candle.open;
        double close = candle.close;
        boolean greenCandle = close > open;
        boolean redCandle = close < open;

        // ATP check
        double atp = candleAggregator.getAtp(fyersSymbol);

        // Already in position for this symbol?
        String pos = PositionManager.getPosition(fyersSymbol);
        if (!"NONE".equals(pos)) {
            eventService.log("[SCANNER] " + fyersSymbol + " — skipped, position already open (" + pos + ")");
            return;
        }

        Set<String> broken = brokenLevels.getOrDefault(fyersSymbol, Collections.emptySet());

        double low = candle.low;
        double high = candle.high;

        // 5-min SMA trend log — fires only when a structural breakout WOULD have matched but
        // was blocked by the SMA trend filter. Prevents noisy "blocked by SMA" logs on every
        // candle close for symbols that have no potential setup at all.
        double sma20Now  = smaService.getSma(fyersSymbol);
        double sma50Now  = smaService.getSma50(fyersSymbol);
        double sma200Now = smaService.getSma200(fyersSymbol);
        if (riskSettings.isEnableSmaTrendCheck() && sma20Now > 0 && sma50Now > 0 && sma200Now > 0) {
            boolean bullClose = close > sma20Now && close > sma50Now && close > sma200Now;
            boolean bearClose = close < sma20Now && close < sma50Now && close < sma200Now;
            boolean bullAligned = sma20Now > sma50Now && sma50Now > sma200Now;
            boolean bearAligned = sma20Now < sma50Now && sma50Now < sma200Now;
            boolean alignmentOn = riskSettings.isEnableSmaAlignmentCheck();
            if (greenCandle && !(bullClose && (!alignmentOn || bullAligned))) {
                String potentialSetup = detectBuyBreakout(open, high, low, close, levels, atp, broken, fyersSymbol, true);
                if (potentialSetup != null) {
                    String reason = !bullClose ? "close not above all SMAs" : "SMA not aligned (need 20>50>200)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + potentialSetup + " blocked by 5-min SMA trend — " + reason + ": close="
                        + String.format("%.2f", close) + " sma20=" + String.format("%.2f", sma20Now)
                        + " sma50=" + String.format("%.2f", sma50Now) + " sma200=" + String.format("%.2f", sma200Now));
                }
            } else if (redCandle && !(bearClose && (!alignmentOn || bearAligned))) {
                String potentialSetup = detectSellBreakout(open, high, low, close, levels, atp, broken, fyersSymbol, true);
                if (potentialSetup != null) {
                    String reason = !bearClose ? "close not below all SMAs" : "SMA not aligned (need 20<50<200)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + potentialSetup + " blocked by 5-min SMA trend — " + reason + ": close="
                        + String.format("%.2f", close) + " sma20=" + String.format("%.2f", sma20Now)
                        + " sma50=" + String.format("%.2f", sma50Now) + " sma200=" + String.format("%.2f", sma200Now));
                }
            }
        }

        // Check BUY signals — requires green candle (close > open)
        if (greenCandle) {
            // ATP check for buys: close must be above ATP
            if (riskSettings.isEnableAtpCheck() && atp > 0 && close < atp) {
                // Only log if a breakout would have been detected without ATP check
                String wouldMatch = detectBuyBreakout(open, high, low, close, levels, 0, broken, fyersSymbol);
                if (wouldMatch != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + wouldMatch + " — skipped, close (" + String.format("%.2f", close) + ") below ATP (" + String.format("%.2f", atp) + ")");
                }
                return;
            }
            String buySetup = detectBuyBreakout(open, high, low, close, levels, atp, broken, fyersSymbol);
            if (buySetup != null) {
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, true, buySetup);
                if (!isProbabilityEnabled(prob)) {
                    // Only log when an HPT setup was filtered — natively-LPT setups (magnets,
                    // counter-trend bounces) are expected to be skipped silently when LPT is off,
                    // otherwise the event log fills with noise the user has already opted out of.
                    if ("HPT".equals(prob)) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " — skipped, HPT not enabled");
                    }
                    return;
                }
                // SMA level-count filter: skip if any CPR zone sits between SMA and broken level
                if (evaluateSmaFilter(fyersSymbol, buySetup, close, levels, atr) == 2) return;
                // NIFTY index alignment filter — hard skip, qty reduction (soft), or no-op.
                NiftyAlignStatus alignBuy = checkIndexAlignment(fyersSymbol, buySetup, true);
                if (alignBuy == NiftyAlignStatus.SKIP) return;
                boolean niftyOpposedBuy = alignBuy == NiftyAlignStatus.OPPOSED_SOFT;
                String buyNote = niftyOpposedBuy
                    ? "NIFTY opposes buy — probability downgraded to LPT"
                    : null;
                fireSignal(fyersSymbol, buySetup, open, high, low, close, candle.volume, atr, levels, prob, buyNote, niftyOpposedBuy);
                return;
            } else {
                if (!broken.isEmpty()) {
                    String wouldMatch = detectBuyBreakout(open, high, low, close, levels, atp, Collections.emptySet(), fyersSymbol);
                    if (wouldMatch != null && broken.contains(wouldMatch)) {
                        eventService.log("[INFO] " + fyersSymbol + " " + wouldMatch + " — skipped, level already traded");
                    }
                }
                // Debug: detect without ATP to see if ATP blocked it
                String noAtpMatch = detectBuyBreakout(open, high, low, close, levels, 0, broken, fyersSymbol);
                if (noAtpMatch != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + noAtpMatch + " — no breakout detected (O=" + String.format("%.2f", open) + " H=" + String.format("%.2f", high) + " L=" + String.format("%.2f", low) + " C=" + String.format("%.2f", close) + " ATP=" + String.format("%.2f", atp) + ")");
                }
            }
        }

        // Check SELL signals — requires red candle (close < open)
        if (redCandle) {
            // ATP check for sells: close must be below ATP
            if (riskSettings.isEnableAtpCheck() && atp > 0 && close > atp) {
                String wouldMatch = detectSellBreakout(open, high, low, close, levels, 0, broken, fyersSymbol);
                if (wouldMatch != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + wouldMatch + " — skipped, close (" + String.format("%.2f", close) + ") above ATP (" + String.format("%.2f", atp) + ")");
                }
                return;
            }
            String sellSetup = detectSellBreakout(open, high, low, close, levels, atp, broken, fyersSymbol);
            if (sellSetup != null) {
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, false, sellSetup);
                if (!isProbabilityEnabled(prob)) {
                    // Only log when an HPT setup was filtered — natively-LPT setups (magnets,
                    // counter-trend bounces) are expected to be skipped silently when LPT is off.
                    if ("HPT".equals(prob)) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " — skipped, HPT not enabled");
                    }
                    return;
                }
                // SMA level-count filter: skip if any CPR zone sits between SMA and broken level
                if (evaluateSmaFilter(fyersSymbol, sellSetup, close, levels, atr) == 2) return;
                // NIFTY index alignment filter — hard skip, qty reduction (soft), or no-op.
                NiftyAlignStatus alignSell = checkIndexAlignment(fyersSymbol, sellSetup, false);
                if (alignSell == NiftyAlignStatus.SKIP) return;
                boolean niftyOpposedSell = alignSell == NiftyAlignStatus.OPPOSED_SOFT;
                String sellNote = niftyOpposedSell
                    ? "NIFTY opposes sell — probability downgraded to LPT"
                    : null;
                fireSignal(fyersSymbol, sellSetup, open, high, low, close, candle.volume, atr, levels, prob, sellNote, niftyOpposedSell);
            } else {
                // No sell breakout detected — log if close is below a key level for debugging
                if (!broken.isEmpty()) {
                    String wouldMatch = detectSellBreakout(open, high, low, close, levels, atp, Collections.emptySet(), fyersSymbol);
                    if (wouldMatch != null && broken.contains(wouldMatch)) {
                        eventService.log("[INFO] " + fyersSymbol + " " + wouldMatch + " — skipped, level already traded");
                    }
                }
                // Debug: detect without ATP to see if ATP blocked it
                String noAtpMatch = detectSellBreakout(open, high, low, close, levels, 0, broken, fyersSymbol);
                if (noAtpMatch != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + noAtpMatch + " — no breakout detected (O=" + String.format("%.2f", open) + " H=" + String.format("%.2f", high) + " L=" + String.format("%.2f", low) + " C=" + String.format("%.2f", close) + " ATP=" + String.format("%.2f", atp) + ")");
                }
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
                                      CprLevels levels, double atp, Set<String> broken, String fyersSymbol) {
        return detectBuyBreakout(open, high, low, close, levels, atp, broken, fyersSymbol, false);
    }

    /**
     * @param skipTrendFilters when true, bypasses ATP / SMA-trend / SMA-vs-ATP / pattern
     *                         filter gates and only checks the structural candle-vs-level
     *                         patterns. Used by the caller to pre-detect "would there be a
     *                         setup if filters were off?" before logging filter rejections.
     */
    private String detectBuyBreakout(double open, double high, double low, double close,
                                      CprLevels levels, double atp, Set<String> broken, String fyersSymbol,
                                      boolean skipTrendFilters) {
        // ATP check for buys: close must be above ATP
        if (!skipTrendFilters && riskSettings.isEnableAtpCheck() && atp > 0 && close < atp) return null;
        // 5-min SMA trend check — now gated at the caller level (scanForBreakoutInner)
        // before reaching this function, so no double-logging on diagnostic re-calls.
        double sma    = smaService.getSma(fyersSymbol);      // still needed by isEnableSmaVsAtpCheck below
        double sma50  = smaService.getSma50(fyersSymbol);
        double sma200 = smaService.getSma200(fyersSymbol);
        if (!skipTrendFilters && riskSettings.isEnableSmaTrendCheck()
                && sma > 0 && sma50 > 0 && sma200 > 0
                && !(close > sma && close > sma50 && close > sma200)) {
            return null;  // silently — the log fired once at the caller
        }
        // SMA alignment check for buys: 20 > 50 > 200 (stricter than close > SMAs)
        if (!skipTrendFilters && riskSettings.isEnableSmaAlignmentCheck()
                && sma > 0 && sma50 > 0 && sma200 > 0
                && !(sma > sma50 && sma50 > sma200)) {
            return null;
        }
        // 20 SMA vs ATP/VWAP check for buys: 20 SMA must be above ATP
        if (!skipTrendFilters && riskSettings.isEnableSmaVsAtpCheck() && sma > 0 && atp > 0 && sma < atp) return null;
        // SMA 20/50 pattern filters for buys
        if (!skipTrendFilters && (riskSettings.isRequireRtpPattern() || riskSettings.isSkipTradesInZigZag())) {
            double atrForPattern = atrService.getAtr(fyersSymbol);
            if (atrForPattern > 0) {
                String pat = smaService.getSmaPattern(fyersSymbol,
                    riskSettings.getSmaPatternLookback(), atrForPattern,
                    riskSettings.getBraidedMinCrossovers(), riskSettings.getBraidedMaxSpreadAtr(),
                    riskSettings.getRailwayMaxCv(), riskSettings.getRailwayMinSpreadAtr());
                if (riskSettings.isSkipTradesInZigZag() && "BRAIDED".equals(pat)) return null;
                if (riskSettings.isRequireRtpPattern() && !"RAILWAY_UP".equals(pat)) return null;
            }
        }

        double r4 = levels.getR4(), r3 = levels.getR3(), r2 = levels.getR2();
        double r1 = levels.getR1(), ph = levels.getPh();
        double pp = levels.getPivot(), tc = levels.getTc(), bc = levels.getBc();
        double s1 = levels.getS1(), pl = levels.getPl();

        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);

        // Check from highest to lowest — standard breakout OR wick rejection
        if (close > r4 && ((open < r4 || low < r4) || (low < r4 && open > r4)) && !broken.contains("BUY_ABOVE_R4")) return "BUY_ABOVE_R4";
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
        // CPR Magnet: bounce UP from S1/PDL toward CPR (close must still be below CPR)
        double s1pl = Math.max(s1, pl);
        if (close > s1pl && close < cprBot
                && ((open < s1 || open < pl || low < s1 || low < pl) || (low < Math.min(s1, pl) && open > Math.min(s1, pl)))
                && !broken.contains("BUY_ABOVE_S1_PDL")) return "BUY_ABOVE_S1_PDL";
        // Mean-reversion bounces from below S-levels (gap-down reversals).
        // Pattern: candle opened below the level (price was extended down) and closed above it.
        double s2 = levels.getS2(), s3 = levels.getS3(), s4 = levels.getS4();
        if (open <= s4 && close > s4 && !broken.contains("BUY_ABOVE_S4")) return "BUY_ABOVE_S4";
        if (open <= s3 && close > s3 && !broken.contains("BUY_ABOVE_S3")) return "BUY_ABOVE_S3";
        if (open <= s2 && close > s2 && !broken.contains("BUY_ABOVE_S2")) return "BUY_ABOVE_S2";

        // Day High breakout (lowest priority — only after OR locks)
        double dayHigh = candleAggregator.getDayHighExcluding(fyersSymbol, currentCandle.get());
        if (dayHigh > 0 && candleAggregator.isOpeningRangeLocked(fyersSymbol)
                && close > dayHigh && ((open < dayHigh || low < dayHigh) || (low < dayHigh && open > dayHigh))
                && !broken.contains("BUY_ABOVE_DH")) return "BUY_ABOVE_DH";

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
                                       CprLevels levels, double atp, Set<String> broken, String fyersSymbol) {
        return detectSellBreakout(open, high, low, close, levels, atp, broken, fyersSymbol, false);
    }

    private String detectSellBreakout(double open, double high, double low, double close,
                                       CprLevels levels, double atp, Set<String> broken, String fyersSymbol,
                                       boolean skipTrendFilters) {
        // ATP check for sells: close must be below ATP
        if (!skipTrendFilters && riskSettings.isEnableAtpCheck() && atp > 0 && close > atp) return null;
        // 5-min SMA trend check — now gated at the caller level (scanForBreakoutInner) before
        // reaching this function, so no double-logging on diagnostic re-calls.
        double sma    = smaService.getSma(fyersSymbol);      // still needed by isEnableSmaVsAtpCheck below
        double sma50  = smaService.getSma50(fyersSymbol);
        double sma200 = smaService.getSma200(fyersSymbol);
        if (!skipTrendFilters && riskSettings.isEnableSmaTrendCheck()
                && sma > 0 && sma50 > 0 && sma200 > 0
                && !(close < sma && close < sma50 && close < sma200)) {
            return null;  // silently — the log fired once at the caller
        }
        // SMA alignment check for sells: 20 < 50 < 200 (stricter than close < SMAs)
        if (!skipTrendFilters && riskSettings.isEnableSmaAlignmentCheck()
                && sma > 0 && sma50 > 0 && sma200 > 0
                && !(sma < sma50 && sma50 < sma200)) {
            return null;
        }
        // 20 SMA vs ATP/VWAP check for sells: 20 SMA must be below ATP
        if (!skipTrendFilters && riskSettings.isEnableSmaVsAtpCheck() && sma > 0 && atp > 0 && sma > atp) return null;
        // SMA 20/50 pattern filters for sells
        if (!skipTrendFilters && (riskSettings.isRequireRtpPattern() || riskSettings.isSkipTradesInZigZag())) {
            double atrForPattern = atrService.getAtr(fyersSymbol);
            if (atrForPattern > 0) {
                String pat = smaService.getSmaPattern(fyersSymbol,
                    riskSettings.getSmaPatternLookback(), atrForPattern,
                    riskSettings.getBraidedMinCrossovers(), riskSettings.getBraidedMaxSpreadAtr(),
                    riskSettings.getRailwayMaxCv(), riskSettings.getRailwayMinSpreadAtr());
                if (riskSettings.isSkipTradesInZigZag() && "BRAIDED".equals(pat)) return null;
                if (riskSettings.isRequireRtpPattern() && !"RAILWAY_DOWN".equals(pat)) return null;
            }
        }

        double s4 = levels.getS4(), s3 = levels.getS3(), s2 = levels.getS2();
        double s1 = levels.getS1(), pl = levels.getPl();
        double pp = levels.getPivot(), tc = levels.getTc(), bc = levels.getBc();
        double r1 = levels.getR1(), ph = levels.getPh();

        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);

        // Check from highest to lowest — standard breakdown OR wick rejection
        if (close < s4 && ((open > s4 || high > s4) || (high > s4 && open < s4)) && !broken.contains("SELL_BELOW_S4")) return "SELL_BELOW_S4";
        if (close < s3
                && ((open > s3 || high > s3) || (high > s3 && open < s3))
                && !broken.contains("SELL_BELOW_S3")) return "SELL_BELOW_S3";
        if (close < s2
                && ((open > s2 || high > s2) || (high > s2 && open < s2))
                && !broken.contains("SELL_BELOW_S2")) return "SELL_BELOW_S2";
        // Mean-reversion fades from above R-levels (gap-up reversals).
        // Pattern: candle opened above the level (price was extended) and closed below it.
        double r2 = levels.getR2(), r3 = levels.getR3(), r4 = levels.getR4();
        if (open >= r4 && close < r4 && !broken.contains("SELL_BELOW_R4")) return "SELL_BELOW_R4";
        if (open >= r3 && close < r3 && !broken.contains("SELL_BELOW_R3")) return "SELL_BELOW_R3";
        if (open >= r2 && close < r2 && !broken.contains("SELL_BELOW_R2")) return "SELL_BELOW_R2";
        if (close < s1 && close < pl
                && ((open > s1 || open > pl || high > s1 || high > pl) || (high > Math.max(s1, pl) && open < Math.max(s1, pl)))
                && !broken.contains("SELL_BELOW_S1_PDL")) return "SELL_BELOW_S1_PDL";
        if (close < cprBot
                && ((open > pp || open > tc || open > bc || high > pp || high > tc || high > bc) || (high > cprTop && open < cprTop && close < cprBot))
                && !broken.contains("SELL_BELOW_CPR")) return "SELL_BELOW_CPR";
        // CPR Magnet: rejection DOWN from R1/PDH toward CPR (close must still be above CPR)
        double r1ph = Math.min(r1, ph);
        if (close < r1ph && close > cprTop
                && ((open > r1 || open > ph || high > r1 || high > ph) || (high > Math.max(r1, ph) && open < Math.max(r1, ph)))
                && !broken.contains("SELL_BELOW_R1_PDH")) return "SELL_BELOW_R1_PDH";

        // Day Low breakout (lowest priority — only after OR locks)
        double dayLow = candleAggregator.getDayLowExcluding(fyersSymbol, currentCandle.get());
        if (dayLow > 0 && candleAggregator.isOpeningRangeLocked(fyersSymbol)
                && close < dayLow && ((open > dayLow || high > dayLow) || (high > dayLow && open < dayLow))
                && !broken.contains("SELL_BELOW_DL")) return "SELL_BELOW_DL";

        return null;
    }

    /**
     * Build signal payload and feed into TradingController.
     */
    private void fireSignal(String fyersSymbol, String setup, double open, double high,
                            double low, double close, long candleVolume, double atr, CprLevels levels, String prob) {
        fireSignal(fyersSymbol, setup, open, high, low, close, candleVolume, atr, levels, prob, null);
    }

    private void fireSignal(String fyersSymbol, String setup, double open, double high,
                            double low, double close, long candleVolume, double atr, CprLevels levels,
                            String prob, String scannerNote) {
        fireSignal(fyersSymbol, setup, open, high, low, close, candleVolume, atr, levels, prob, scannerNote, false);
    }

    private void fireSignal(String fyersSymbol, String setup, double open, double high,
                            double low, double close, long candleVolume, double atr, CprLevels levels,
                            String prob, String scannerNote, boolean niftyOpposed) {
        // Use the closing candle's exchange-derived close time, not system clock.
        // Server clock typically runs ~50-500ms behind exchange wall time due to network
        // latency, so system-time logs were showing 09:24:59 for a candle that actually
        // closed at exchange time 09:25:00. Deriving the label from the candle itself
        // removes that ambiguity.
        CandleAggregator.CandleBar ctxCandle = currentCandle.get();
        String timeStr;
        if (ctxCandle != null && ctxCandle.startMinute > 0) {
            int closeMin = (int) (ctxCandle.startMinute + riskSettings.getScannerTimeframe());
            timeStr = String.format("%02d:%02d:00", closeMin / 60, closeMin % 60);
        } else {
            timeStr = ZonedDateTime.now(IST).toLocalTime().format(TIME_FMT);
        }
        latencyTracker.mark(fyersSymbol, setup, LatencyTracker.Stage.SIGNAL_DETECTED);

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
        payload.put("firstCandleClose", candleAggregator.getFirstCandleClose(fyersSymbol));
        payload.put("probability", prob);
        payload.put("niftyOpposed", niftyOpposed);
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
        payload.put("cprDayRelation", levels.getCprDayRelation());
        payload.put("dayHigh", candleAggregator.getDayHighBeforeLast(fyersSymbol));
        payload.put("dayLow", candleAggregator.getDayLowBeforeLast(fyersSymbol));
        if (scannerNote != null && !scannerNote.isEmpty()) payload.put("scannerNote", scannerNote);

        // 5-min SMA trend + pattern snapshot at signal time
        double sma20Now  = smaService.getSma(fyersSymbol);
        double sma50Now  = smaService.getSma50(fyersSymbol);
        double sma200Now = smaService.getSma200(fyersSymbol);
        String trend = "--";
        if (sma20Now > 0 && sma50Now > 0 && sma200Now > 0) {
            boolean allAbove = close > sma20Now && close > sma50Now && close > sma200Now;
            boolean allBelow = close < sma20Now && close < sma50Now && close < sma200Now;
            trend = allAbove ? "BULLISH" : allBelow ? "BEARISH" : "NEUTRAL";
        }
        String pattern5m = "";
        if (sma20Now > 0 && sma50Now > 0 && atr > 0) {
            pattern5m = smaService.getSmaPattern(fyersSymbol,
                riskSettings.getSmaPatternLookback(), atr,
                riskSettings.getBraidedMinCrossovers(), riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(), riskSettings.getRailwayMinSpreadAtr());
        }
        String patternLabel = "RAILWAY_UP".equals(pattern5m) ? "R-RTP"
                            : "RAILWAY_DOWN".equals(pattern5m) ? "F-RTP"
                            : "BRAIDED".equals(pattern5m) ? "ZIG ZAG"
                            : "--";
        eventService.log("[SCANNER] " + fyersSymbol + " " + setup + " | close=" + String.format("%.2f", close)
            + " | ATR=" + String.format("%.2f", atr) + " | " + prob
            + " | 5m trend=" + trend + " pattern=" + patternLabel + " | " + timeStr);

        // Feed into TradingController (same pipeline as TradingView webhook)
        try {
            var response = tradingController.receiveSignal(payload);
            String status = response.getBody() != null ? response.getBody() : "";

            // Track signal info for dashboard
            SignalInfo info = new SignalInfo();
            info.setup = setup;
            info.time = timeStr;
            info.price = close;
            boolean filtered = status.contains("failed") || status.contains("filtered") || status.contains("ignored");
            info.status = filtered ? "FILTERED" : "TRADED";
            if (filtered) filteredCountToday++; else tradedCountToday++;
            info.detail = status;
            lastSignal.put(fyersSymbol, info);
            signalHistory.computeIfAbsent(fyersSymbol, k -> Collections.synchronizedList(new ArrayList<>())).add(info);

            // Only mark level as broken if signal was actually traded
            if (!filtered) {
                brokenLevels.computeIfAbsent(fyersSymbol, k -> ConcurrentHashMap.newKeySet()).add(setup);
            }
            saveState();

        } catch (Exception e) {
            log.error("[Scanner] Failed to process signal for {}: {}", fyersSymbol, e.getMessage());
            SignalInfo info = new SignalInfo();
            info.setup = setup;
            info.time = timeStr;
            info.status = "ERROR";
            info.detail = e.getMessage();
            lastSignal.put(fyersSymbol, info);
            signalHistory.computeIfAbsent(fyersSymbol, k -> Collections.synchronizedList(new ArrayList<>())).add(info);
            saveState();
        }
    }

    /**
     * NIFTY index alignment filter. Two modes (controlled by riskSettings.isIndexAlignmentHardSkip):
     *
     *   Soft mode (default): downgrades HPT trades to LPT when trade direction opposes NIFTY.
     *                        LPT trades pass through unchanged.
     *   Hard skip mode:      returns null (signal to caller: skip the trade entirely) when ANY
     *                        trade (HPT or LPT) opposes NIFTY. Skips magnets/reversals too if
     *                        they happen to be in the opposition direction — by design.
     *
     * Returns the (possibly modified) probability string, or null to signal "skip this trade".
     */
    /**
     * Result of the NIFTY alignment check. {@code SKIP} = hard-skip mode rejected the trade;
     * {@code OPPOSED_SOFT} = NIFTY opposes but soft mode — keep probability, SignalProcessor
     * will apply {@code indexOpposedQtyFactor} qty reduction; {@code OK} = aligned or check disabled.
     */
    private enum NiftyAlignStatus { OK, OPPOSED_SOFT, SKIP }

    private NiftyAlignStatus checkIndexAlignment(String fyersSymbol, String setup, boolean isBuy) {
        if (!riskSettings.isEnableIndexAlignment()) return NiftyAlignStatus.OK;
        if (indexTrendService == null) return NiftyAlignStatus.OK;
        try {
            com.rydytrader.autotrader.dto.IndexTrend nifty = indexTrendService.getNiftyTrend();
            if (!nifty.isDataAvailable()) return NiftyAlignStatus.OK;
            String state = nifty.getState();
            boolean opposed = isBuy
                ? ("BEARISH".equals(state) || "STRONG_BEARISH".equals(state))
                : ("BULLISH".equals(state) || "STRONG_BULLISH".equals(state));
            if (!opposed) return NiftyAlignStatus.OK;

            if (riskSettings.isIndexAlignmentHardSkip()) {
                eventService.log("[SCANNER] " + fyersSymbol + " " + setup
                    + " SKIPPED — NIFTY " + state + " (score " + nifty.getTotalScore() + ") opposes "
                    + (isBuy ? "buy" : "sell") + " [hard skip mode]");
                return NiftyAlignStatus.SKIP;
            }
            // Soft mode: trade still fires at its scanner probability (HPT/MPT/LPT).
            // SignalProcessor applies indexOpposedQtyFactor to reduce size.
            double factor = riskSettings.getIndexOpposedQtyFactor();
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup
                + " — NIFTY " + state + " (score " + nifty.getTotalScore() + ") opposes "
                + (isBuy ? "buy" : "sell") + " — qty reduced to " + String.format("%.0f%%", factor * 100));
            return NiftyAlignStatus.OPPOSED_SOFT;
        } catch (Exception e) {
            log.warn("[BreakoutScanner] Index alignment check failed for {}: {}", fyersSymbol, e.getMessage());
        }
        return NiftyAlignStatus.OK;
    }

    private boolean isProbabilityEnabled(String prob) {
        return switch (prob) {
            case "HPT" -> riskSettings.isEnableHpt();
            case "LPT" -> riskSettings.isEnableLpt();
            default -> false;
        };
    }

    /**
     * Check if a stock passes the CPR Width Scanner settings.
     * A stock must match at least one enabled group to be eligible for breakout signals.
     */
    private boolean isBreakoutEligible(String fyersSymbol) {
        String ticker = extractTicker(fyersSymbol);
        com.rydytrader.autotrader.dto.CprLevels cpr = bhavcopyService.getCprLevels(ticker);
        if (cpr == null) return false;

        // Price filter
        double minPrice = riskSettings.getScanMinPrice();
        if (minPrice > 0 && cpr.getClose() < minPrice) return false;

        double wpct = cpr.getCprWidthPct();
        boolean isNarrow = wpct >= riskSettings.getNarrowCprMinWidth() && wpct < riskSettings.getNarrowCprMaxWidth();
        boolean isInside = bhavcopyService.getInsideCprStocks().stream()
                .anyMatch(c -> c.getSymbol().equals(ticker));
        String nrt = cpr.getNarrowRangeType();

        // Narrow CPR → NS/NL toggles
        if (isNarrow) {
            if ("SMALL".equals(nrt) && riskSettings.isScanIncludeNS()) return true;
            if ("LARGE".equals(nrt) && riskSettings.isScanIncludeNL()) return true;
            if (nrt == null && (riskSettings.isScanIncludeNS() || riskSettings.isScanIncludeNL())) return true;
        }
        // Inside-only CPR → IS/IL toggles + width filter
        double insideMaxWidth = riskSettings.getInsideCprMaxWidth();
        if (!isNarrow && isInside && (insideMaxWidth <= 0 || cpr.getCprWidthPct() <= insideMaxWidth)) {
            if ("SMALL".equals(nrt) && riskSettings.isScanIncludeIS()) return true;
            if ("LARGE".equals(nrt) && riskSettings.isScanIncludeIL()) return true;
            if (nrt == null && (riskSettings.isScanIncludeIS() || riskSettings.isScanIncludeIL())) return true;
        }

        return false;
    }

    /**
     * SMA level-count filter. Counts CPR zones strictly between SMA and the broken level.
     * Pair zones (CPR, R1/PDH, S1/PDL) are collapsed — each counted at most once.
     * The setup's own zone is excluded. For DH/DL, the highest R-zone (or lowest S-zone)
     * that price has already passed is excluded — e.g. if DH is above R3, R3 is excluded.
     *
     * Returns 0 = pass, 2 = skip. No halve tier. If filter disabled, always returns 0.
     */
    private int evaluateSmaFilter(String fyersSymbol, String setup, double close,
                                   CprLevels levels, double atr) {
        if (!riskSettings.isEnableSmaLevelCountFilter()) return 0;

        // Morning skip: bypass the filter while price runs hard and SMA(20) lags.
        if (riskSettings.isSmaLevelFilterMorningSkip()) {
            String until = riskSettings.getSmaLevelFilterMorningSkipUntil();
            if (until != null && !until.isEmpty()) {
                try {
                    java.time.LocalTime nowIst = java.time.ZonedDateTime.now(IST).toLocalTime();
                    java.time.LocalTime cutoff = java.time.LocalTime.parse(until);
                    if (nowIst.isBefore(cutoff)) return 0;
                } catch (Exception ignored) {}
            }
        }

        double sma = smaService.getSma(fyersSymbol);
        if (sma <= 0) return 0;
        double broken = getBreakoutLevelPrice(setup, levels, fyersSymbol);
        if (broken <= 0) return 0;

        double lo = Math.min(sma, broken);
        double hi = Math.max(sma, broken);

        double cprBot = Math.min(levels.getTc(), levels.getBc());
        double cprTop = Math.max(levels.getTc(), levels.getBc());
        double r1lo   = Math.min(levels.getR1(), levels.getPh());
        double r1hi   = Math.max(levels.getR1(), levels.getPh());
        double s1lo   = Math.min(levels.getS1(), levels.getPl());
        double s1hi   = Math.max(levels.getS1(), levels.getPl());

        String excludedZone = excludedZoneFor(setup, levels, fyersSymbol);

        // Zone list, bottom-to-top. name → edges
        String[] zoneNames = { "S4", "S3", "S2", "S1PDL", "CPR", "R1PDH", "R2", "R3", "R4" };
        double[][] zoneEdges = {
            { levels.getS4() },
            { levels.getS3() },
            { levels.getS2() },
            { s1lo, s1hi },
            { cprBot, cprTop },
            { r1lo, r1hi },
            { levels.getR2() },
            { levels.getR3() },
            { levels.getR4() }
        };

        int count = 0;
        StringBuilder between = new StringBuilder();
        for (int i = 0; i < zoneNames.length; i++) {
            if (zoneNames[i].equals(excludedZone)) continue;
            boolean anyEdgeInInterval = false;
            for (double e : zoneEdges[i]) {
                if (e > 0 && e > lo && e < hi) { anyEdgeInInterval = true; break; }
            }
            if (anyEdgeInInterval) {
                count++;
                if (between.length() > 0) between.append(", ");
                between.append(zoneNames[i]);
            }
        }

        if (count > 0) {
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup
                + " — skipped, SMA(" + String.format("%.2f", sma) + ") is "
                + count + " zone(s) away from broken " + String.format("%.2f", broken)
                + " [zones between: " + between + "]");
            return 2;
        }

        // Secondary proximity check: SMA must be within (100 - smaLevelMinRangePct)% of the
        // range between the broken level and the nearest zone edge on the other side. Catches
        // the "count=0 but SMA at bottom of the zone" gap — SMA can pass the count check while
        // still being far below the breakout level within the same zone.
        int minRangePct = riskSettings.getSmaLevelMinRangePct();
        if (minRangePct > 0) {
            boolean isBuy = setup.startsWith("BUY_");
            // Nearest zone edge on the other side of the broken level, skipping the broken zone.
            // For buy: highest edge strictly below broken. For sell: lowest edge strictly above.
            double boundaryEdge = 0;
            for (int i = 0; i < zoneNames.length; i++) {
                if (zoneNames[i].equals(excludedZone)) continue;
                for (double e : zoneEdges[i]) {
                    if (e <= 0) continue;
                    if (isBuy) {
                        if (e < broken && e > boundaryEdge) boundaryEdge = e;
                    } else {
                        if (e > broken && (boundaryEdge == 0 || e < boundaryEdge)) boundaryEdge = e;
                    }
                }
            }
            if (boundaryEdge > 0) {
                double range = Math.abs(broken - boundaryEdge);
                double maxDist = range * (100 - minRangePct) / 100.0;
                double actualDist = Math.abs(sma - broken);
                if (actualDist > maxDist) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + setup
                        + " — skipped, SMA(" + String.format("%.2f", sma) + ") too far from broken "
                        + String.format("%.2f", broken) + " (dist " + String.format("%.2f", actualDist)
                        + " > max " + String.format("%.2f", maxDist)
                        + " = " + (100 - minRangePct) + "% of range " + String.format("%.2f", range)
                        + " to boundary " + String.format("%.2f", boundaryEdge)
                        + " — SMA must be at least " + minRangePct + "% away from boundary)");
                    return 2;
                }
            }
        }
        return 0;
    }

    /** Returns the zone name the setup is breaking — excluded from the "levels between" count. */
    private String excludedZoneFor(String setup, CprLevels levels, String fyersSymbol) {
        return switch (setup) {
            case "BUY_ABOVE_CPR", "SELL_BELOW_CPR"       -> "CPR";
            case "BUY_ABOVE_R1_PDH", "SELL_BELOW_R1_PDH" -> "R1PDH";
            case "BUY_ABOVE_S1_PDL", "SELL_BELOW_S1_PDL" -> "S1PDL";
            case "BUY_ABOVE_R2", "SELL_BELOW_R2"         -> "R2";
            case "BUY_ABOVE_R3", "SELL_BELOW_R3"         -> "R3";
            case "BUY_ABOVE_R4", "SELL_BELOW_R4"         -> "R4";
            case "BUY_ABOVE_S2", "SELL_BELOW_S2"         -> "S2";
            case "BUY_ABOVE_S3", "SELL_BELOW_S3"         -> "S3";
            case "BUY_ABOVE_S4", "SELL_BELOW_S4"         -> "S4";
            // DH/DL: find the highest R-zone (buy) or lowest S-zone (sell) that price has passed
            case "BUY_ABOVE_DH" -> {
                double dh = candleAggregator.getDayHighBeforeLast(fyersSymbol);
                if (dh > levels.getR4()) yield "R4";
                if (dh > levels.getR3()) yield "R3";
                if (dh > levels.getR2()) yield "R2";
                if (dh > Math.max(levels.getR1(), levels.getPh())) yield "R1PDH";
                if (dh > Math.max(levels.getTc(), levels.getBc())) yield "CPR";
                yield "";
            }
            case "SELL_BELOW_DL" -> {
                double dl = candleAggregator.getDayLowBeforeLast(fyersSymbol);
                if (dl < levels.getS4()) yield "S4";
                if (dl < levels.getS3()) yield "S3";
                if (dl < levels.getS2()) yield "S2";
                if (dl < Math.min(levels.getS1(), levels.getPl())) yield "S1PDL";
                if (dl < Math.min(levels.getTc(), levels.getBc())) yield "CPR";
                yield "";
            }
            default -> "";
        };
    }


    /** Get the breakout level price for a given setup name. */
    private double getBreakoutLevelPrice(String setup, CprLevels levels, String fyersSymbol) {
        return switch (setup) {
            case "BUY_ABOVE_CPR"    -> Math.max(levels.getTc(), levels.getBc());
            case "BUY_ABOVE_R1_PDH" -> Math.max(levels.getR1(), levels.getPh());
            case "BUY_ABOVE_R2"     -> levels.getR2();
            case "BUY_ABOVE_R3"     -> levels.getR3();
            case "BUY_ABOVE_R4"     -> levels.getR4();
            case "BUY_ABOVE_S1_PDL" -> Math.max(levels.getS1(), levels.getPl());
            case "SELL_BELOW_CPR"    -> Math.min(levels.getTc(), levels.getBc());
            case "SELL_BELOW_S1_PDL" -> Math.min(levels.getS1(), levels.getPl());
            case "SELL_BELOW_S2"     -> levels.getS2();
            case "SELL_BELOW_S3"     -> levels.getS3();
            case "SELL_BELOW_S4"     -> levels.getS4();
            case "SELL_BELOW_R1_PDH" -> Math.min(levels.getR1(), levels.getPh());
            case "SELL_BELOW_R2"     -> levels.getR2();
            case "SELL_BELOW_R3"     -> levels.getR3();
            case "SELL_BELOW_R4"     -> levels.getR4();
            case "BUY_ABOVE_S2"      -> levels.getS2();
            case "BUY_ABOVE_S3"      -> levels.getS3();
            case "BUY_ABOVE_S4"      -> levels.getS4();
            // Day-high / Day-low breakouts: level is today's session high/low (excluding the
            // breakout candle itself). Without these cases the SMA level-count filter was
            // skipped entirely for DH/DL trades because broken=0 short-circuits to pass.
            case "BUY_ABOVE_DH"      -> candleAggregator.getDayHighExcluding(fyersSymbol, currentCandle.get());
            case "SELL_BELOW_DL"     -> candleAggregator.getDayLowExcluding(fyersSymbol, currentCandle.get());
            default -> 0;
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
        saveState();
    }

    public Set<String> getBrokenLevels(String symbol) {
        return brokenLevels.getOrDefault(symbol, Collections.emptySet());
    }

    public int getLastScanCount() { return lastScanCount; }
    public String getLastScanTime() { return lastScanTime; }
    public int getTradedCountToday() { return tradedCountToday; }
    public int getFilteredCountToday() { return filteredCountToday; }

    public SignalInfo getLastSignal(String symbol) {
        return lastSignal.get(symbol);
    }

    public Map<String, SignalInfo> getAllSignals() {
        return Collections.unmodifiableMap(lastSignal);
    }

    public List<SignalInfo> getSignalHistory(String symbol) {
        return signalHistory.getOrDefault(symbol, Collections.emptyList());
    }

    /** Clear all state for end of day. */
    public void clearAll() {
        brokenLevels.clear();
        lastSignal.clear();
        signalHistory.clear();
        tradedCountToday = 0;
        filteredCountToday = 0;
        lastScanCount = 0;
        lastScanTime = "";
        lastScanBoundary = 0;
        saveState();
    }

    @Override
    public void onDailyReset() {
        log.info("[Scanner] Daily reset — clearing signals, broken levels, signal history");
        clearAll();
        eventService.log("[INFO] Scanner daily reset — signals and broken levels cleared for new trading day");
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    public void saveState() {
        try {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("date", ZonedDateTime.now(IST).toLocalDate().toString());

            // Save lastSignal
            Map<String, Map<String, Object>> signals = new LinkedHashMap<>();
            for (var entry : lastSignal.entrySet()) {
                Map<String, Object> sig = new LinkedHashMap<>();
                sig.put("setup", entry.getValue().setup);
                sig.put("time", entry.getValue().time);
                sig.put("status", entry.getValue().status);
                sig.put("detail", entry.getValue().detail);
                sig.put("price", entry.getValue().price);
                signals.put(entry.getKey(), sig);
            }
            state.put("signals", signals);

            // Save brokenLevels
            Map<String, List<String>> broken = new LinkedHashMap<>();
            for (var entry : brokenLevels.entrySet()) {
                broken.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
            state.put("brokenLevels", broken);
            state.put("tradedCountToday", tradedCountToday);
            state.put("filteredCountToday", filteredCountToday);

            // Save signalHistory (all signals per symbol for the day)
            Map<String, List<Map<String, Object>>> history = new LinkedHashMap<>();
            for (var entry : signalHistory.entrySet()) {
                List<Map<String, Object>> list = new ArrayList<>();
                for (SignalInfo si : entry.getValue()) {
                    Map<String, Object> sig = new LinkedHashMap<>();
                    sig.put("setup", si.setup);
                    sig.put("time", si.time);
                    sig.put("status", si.status);
                    sig.put("detail", si.detail);
                    sig.put("price", si.price);
                    list.add(sig);
                }
                history.put(entry.getKey(), list);
            }
            state.put("signalHistory", history);

            Files.writeString(Paths.get(SCANNER_STATE_FILE),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
        } catch (Exception e) {
            log.error("[Scanner] Failed to save state: {}", e.getMessage());
        }
    }

    public void loadState() {
        try {
            Path path = Paths.get(SCANNER_STATE_FILE);
            if (!Files.exists(path)) return;

            JsonNode root = mapper.readTree(Files.readString(path));

            // Only load if same day
            String savedDate = root.has("date") ? root.get("date").asText() : "";
            String today = ZonedDateTime.now(IST).toLocalDate().toString();
            if (!today.equals(savedDate)) {
                log.info("[Scanner] State file from {} — stale, starting fresh", savedDate);
                return;
            }

            // Load signals
            JsonNode signalsNode = root.get("signals");
            if (signalsNode != null) {
                signalsNode.fields().forEachRemaining(entry -> {
                    SignalInfo info = new SignalInfo();
                    JsonNode v = entry.getValue();
                    info.setup = v.has("setup") ? v.get("setup").asText() : "";
                    info.time = v.has("time") ? v.get("time").asText() : "";
                    info.status = v.has("status") ? v.get("status").asText() : "";
                    info.detail = v.has("detail") ? v.get("detail").asText() : "";
                    info.price = v.has("price") ? v.get("price").asDouble() : 0;
                    lastSignal.put(entry.getKey(), info);
                });
            }

            // Load brokenLevels
            JsonNode brokenNode = root.get("brokenLevels");
            if (brokenNode != null) {
                brokenNode.fields().forEachRemaining(entry -> {
                    Set<String> levels = ConcurrentHashMap.newKeySet();
                    entry.getValue().forEach(n -> levels.add(n.asText()));
                    brokenLevels.put(entry.getKey(), levels);
                });
            }

            // Load signalHistory
            JsonNode historyNode = root.get("signalHistory");
            if (historyNode != null) {
                historyNode.fields().forEachRemaining(entry -> {
                    List<SignalInfo> list = Collections.synchronizedList(new ArrayList<>());
                    entry.getValue().forEach(node -> {
                        SignalInfo si = new SignalInfo();
                        si.setup = node.has("setup") ? node.get("setup").asText() : "";
                        si.time = node.has("time") ? node.get("time").asText() : "";
                        si.status = node.has("status") ? node.get("status").asText() : "";
                        si.detail = node.has("detail") ? node.get("detail").asText() : "";
                        si.price = node.has("price") ? node.get("price").asDouble() : 0;
                        list.add(si);
                    });
                    signalHistory.put(entry.getKey(), list);
                });
            }

            // Load counters
            if (root.has("tradedCountToday")) tradedCountToday = root.get("tradedCountToday").asInt();
            if (root.has("filteredCountToday")) filteredCountToday = root.get("filteredCountToday").asInt();

            log.info("[Scanner] Restored state: {} signals, {} broken levels, {} traded, {} filtered, {} signal histories",
                lastSignal.size(), brokenLevels.size(), tradedCountToday, filteredCountToday, signalHistory.size());
        } catch (Exception e) {
            log.error("[Scanner] Failed to load state: {}", e.getMessage());
        }
    }

    // ── Signal info for dashboard ────────────────────────────────────────────

    public static class SignalInfo {
        public String setup;
        public String time;
        public String status; // TRADED, FILTERED, ERROR
        public String detail;
        public double price;  // candle close at signal time
    }
}
