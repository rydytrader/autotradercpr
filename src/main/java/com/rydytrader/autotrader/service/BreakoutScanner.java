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

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private TradeHistoryService tradeHistoryService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private HtfSmaService htfSmaService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private MarketHolidayService marketHolidayService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    @org.springframework.context.annotation.Lazy
    private NiftyOptionOiService niftyOptionOiService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    @org.springframework.context.annotation.Lazy
    private VirginCprService virginCprService;

    // Track which levels have been broken today per symbol (prevents re-fire)
    private final ConcurrentHashMap<String, Set<String>> brokenLevels = new ConcurrentHashMap<>();
    /** Single armed buy-side setup per symbol — the highest CPR-level setup whose level sits
     *  strictly below the latest close. Recomputed every candle close so the armed level rolls
     *  forward as price advances and rolls back if price retreats. Eligible for retest-pattern
     *  entries (Route 2). Cleared at day rollover. */
    private final ConcurrentHashMap<String, String> armedBuyLevel  = new ConcurrentHashMap<>();
    /** Single armed sell-side setup per symbol — lowest CPR-level setup whose level sits strictly
     *  above the latest close. Mirror of {@link #armedBuyLevel}. */
    private final ConcurrentHashMap<String, String> armedSellLevel = new ConcurrentHashMap<>();
    /** Trigger-route tag for the most recent fired signal — picked up by fireSignal so the
     *  trade-log description records which path entered ("MARUBOZU_BREAKOUT", "HAMMER_RETEST",
     *  etc.). Per-symbol, accessed only on the candle-close thread for that symbol. */
    private final ConcurrentHashMap<String, String> lastTriggerRoute = new ConcurrentHashMap<>();

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
        backfillFromEventLog();
    }

    /**
     * One-shot recovery for today's pre-recordRejection-shipment rejections. Pre-fireSignal
     * filters (SMA trend, ATP, level-count, level-proximity, NIFTY hard-skip, position-open,
     * level-broken) only logged to event-log.txt before the structured capture sites shipped.
     * This parser scans today's event log for [SCANNER] / [INFO] rejection lines, classifies
     * them by reason, and injects SignalInfo entries into signalHistory so the EOD-Analysis
     * page sees them. Skips lines whose (symbol, setup, time) tuple already matches a
     * persisted entry — safe to call multiple times across restarts.
     */
    public void backfillFromEventLog() {
        try {
            Path logPath = Paths.get("../store/events/event-log.txt");
            if (!Files.exists(logPath)) return;

            // Build a dedupe key set from existing signalHistory so we don't double-insert.
            java.util.Set<String> existingKeys = new java.util.HashSet<>();
            for (var entry : signalHistory.entrySet()) {
                String sym = entry.getKey();
                for (SignalInfo si : entry.getValue()) {
                    existingKeys.add(sym + "|" + (si.setup != null ? si.setup : "") + "|" + (si.time != null ? si.time : ""));
                }
            }

            // Patterns for the rejection log lines we want to recover.
            // Normalised time format used by signalHistory is HH:mm:00 (5-min candle close);
            // event-log timestamps are HH:mm:ss. We snap the seconds to :00 to match.
            java.util.regex.Pattern reSkipBlock = java.util.regex.Pattern.compile(
                "^(\\d{2}):(\\d{2}):(\\d{2}) - \\[SCANNER\\] (NSE:[A-Z0-9&-]+(?:-EQ|-INDEX)) (\\S+) (.+)$");
            java.util.regex.Pattern rePosOpen = java.util.regex.Pattern.compile(
                "^(\\d{2}):(\\d{2}):(\\d{2}) - \\[SCANNER\\] (NSE:[A-Z0-9&-]+(?:-EQ|-INDEX)) — skipped, position already open \\(([^)]+)\\)$");
            java.util.regex.Pattern reLevelBroken = java.util.regex.Pattern.compile(
                "^(\\d{2}):(\\d{2}):(\\d{2}) - \\[INFO\\] (NSE:[A-Z0-9&-]+(?:-EQ|-INDEX)) (\\S+) — skipped, level already traded$");

            int injected = 0;
            int seen = 0;
            try (java.io.BufferedReader r = Files.newBufferedReader(logPath, java.nio.charset.StandardCharsets.UTF_8)) {
                String line;
                while ((line = r.readLine()) != null) {
                    seen++;

                    // 1) Position-open skip (no setup field)
                    java.util.regex.Matcher mPos = rePosOpen.matcher(line);
                    if (mPos.matches()) {
                        String time = mPos.group(1) + ":" + mPos.group(2) + ":00";
                        String sym  = mPos.group(4);
                        String detail = "position " + mPos.group(5) + " already open";
                        if (insertIfAbsent(existingKeys, sym, "", time, "POSITION_OPEN", detail, 0)) injected++;
                        continue;
                    }

                    // 2) Level-already-traded ([INFO] prefix)
                    java.util.regex.Matcher mBr = reLevelBroken.matcher(line);
                    if (mBr.matches()) {
                        String time = mBr.group(1) + ":" + mBr.group(2) + ":00";
                        String sym  = mBr.group(4);
                        String setup = mBr.group(5);
                        if (insertIfAbsent(existingKeys, sym, setup, time, "LEVEL_BROKEN",
                                "level " + setup + " already traded today", 0)) injected++;
                        continue;
                    }

                    // 3) [SCANNER] X SETUP <rest> — classify by rest
                    java.util.regex.Matcher m = reSkipBlock.matcher(line);
                    if (!m.matches()) continue;
                    String time  = m.group(1) + ":" + m.group(2) + ":00";
                    String sym   = m.group(4);
                    String setup = m.group(5);
                    String rest  = m.group(6);
                    String restLower = rest.toLowerCase();

                    // Skip TRADED success lines — those are already captured by fireSignal.
                    // Success lines look like:  | close=... | ATR=... | HPT | 5m trend=... | TIME
                    if (rest.startsWith("| close=")) continue;
                    if (rest.startsWith("|"))         continue;

                    String filterName;
                    String detail;
                    if (restLower.contains("blocked by 5-min sma trend")) {
                        // Strip the "blocked by 5-min SMA trend — " prefix to keep detail concise.
                        String dash = " — ";
                        int idx = rest.indexOf(dash);
                        detail = idx >= 0 ? rest.substring(idx + dash.length()) : rest;
                        filterName = restLower.contains("not aligned") ? "SMA_ALIGNMENT" : "SMA_TREND";
                    } else if (restLower.contains("zone(s) away from broken")) {
                        filterName = "SMA_20_DISTANCE";
                        detail = stripSkippedPrefix(rest);
                    } else if (restLower.contains("too far from broken zone")) {
                        filterName = "LEVEL_PROXIMITY";
                        detail = stripSkippedPrefix(rest);
                    } else if (restLower.contains("below atp") || restLower.contains("above atp")) {
                        filterName = "ATP";
                        detail = stripSkippedPrefix(rest);
                    } else if (restLower.contains("hpt not enabled") || restLower.contains("lpt not enabled") || restLower.contains("mpt not enabled")) {
                        filterName = "PROB_DISABLED";
                        detail = stripSkippedPrefix(rest);
                    } else if (restLower.startsWith("skipped — nifty") && restLower.contains("opposes")) {
                        filterName = "NIFTY_OPPOSED";
                        detail = stripSkippedPrefix(rest);
                    } else {
                        // Not a recognised pre-fireSignal rejection — skip.
                        continue;
                    }

                    if (insertIfAbsent(existingKeys, sym, setup, time, filterName, detail, 0)) injected++;
                }
            }

            if (injected > 0) {
                log.info("[Scanner] Backfilled {} rejection entries from event-log ({} lines scanned)", injected, seen);
                eventService.log("[INFO] EOD audit backfill — restored " + injected
                    + " rejection entries from today's event log");
                saveState();
            }
        } catch (Exception e) {
            log.warn("[Scanner] backfillFromEventLog failed: {}", e.getMessage());
        }
    }

    /** Helper for {@link #backfillFromEventLog()}: drop the leading "— skipped, " preamble
     *  if present so the recovered detail mirrors what live capture stores. */
    private String stripSkippedPrefix(String rest) {
        String key = "— skipped, ";
        int idx = rest.indexOf(key);
        return idx >= 0 ? rest.substring(idx + key.length()) : rest;
    }

    /** Helper for {@link #backfillFromEventLog()}: insert a SignalInfo only when no entry
     *  with the same (symbol, setup, time) already exists. Returns true if inserted. */
    private boolean insertIfAbsent(java.util.Set<String> seenKeys, String sym, String setup,
                                   String time, String filterName, String detail, double price) {
        String key = sym + "|" + setup + "|" + time;
        if (!seenKeys.add(key)) return false;
        SignalInfo info = new SignalInfo();
        info.setup = setup;
        info.time = time;
        info.status = "FILTERED";
        info.filterName = filterName;
        info.detail = detail;
        info.price = price;
        signalHistory.computeIfAbsent(sym, k -> Collections.synchronizedList(new ArrayList<>())).add(info);
        filteredCountToday++;
        return true;
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

        // Gate the breakout scan on the user's configured trading window. Suppresses both
        // the filter-rejection log noise AND prevents premature brokenLevels marking that
        // would silence a legitimate post-start-time fire on the same level.
        if (!isWithinTradingWindow()) return;

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

    /** True when current IST time is within [tradingStartTime, tradingEndTime]. Falls back
     *  to "always on" if either setting is malformed so a typo can't silence the scanner. */
    private boolean isWithinTradingWindow() {
        try {
            java.time.LocalTime now   = ZonedDateTime.now(IST).toLocalTime();
            java.time.LocalTime start = java.time.LocalTime.parse(riskSettings.getTradingStartTime());
            java.time.LocalTime end   = java.time.LocalTime.parse(riskSettings.getTradingEndTime());
            return !now.isBefore(start) && !now.isAfter(end);
        } catch (Exception e) {
            return true;
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

        // Arm CPR levels — record any setup whose level the close has crossed today. Armed
        // levels stay armed for the rest of the day (until daily reset). Pattern-retest
        // entries require the level to be armed beforehand.
        armLevelsForCandle(fyersSymbol, close, levels);

        // ATP check
        double atp = candleAggregator.getAtp(fyersSymbol);

        // Already in position for this symbol?
        String pos = PositionManager.getPosition(fyersSymbol);
        if (!"NONE".equals(pos)) {
            String detail = "position " + pos + " already open";
            eventService.log("[SCANNER] " + fyersSymbol + " — skipped, " + detail);
            recordRejection(fyersSymbol, "", close, "POSITION_OPEN", detail);
            return;
        }

        // Per-symbol daily trade limit. Halt new trades on the symbol once today's wins OR
        // losses (counted separately, not summed) reach the threshold. 0 = disabled.
        int perSymbolLimit = riskSettings.getPerSymbolDailyTradeLimit();
        if (perSymbolLimit > 0 && tradeHistoryService != null) {
            int[] wl = tradeHistoryService.getSymbolTodayResult(fyersSymbol);
            int wins = wl[0], losses = wl[1];
            if (wins >= perSymbolLimit || losses >= perSymbolLimit) {
                String detail = "today " + wins + "W / " + losses + "L — limit " + perSymbolLimit
                    + " (" + (wins >= perSymbolLimit ? "wins" : "losses") + ") reached";
                eventService.log("[SCANNER] " + fyersSymbol + " — skipped, " + detail);
                recordRejection(fyersSymbol, "", close, "SYMBOL_DAILY_LIMIT", detail);
                return;
            }
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
        boolean strictTrendOn  = riskSettings.isEnableSmaTrendCheck();
        boolean lenientTrendOn = riskSettings.isEnableSmaTrendCheckLenient();
        if ((strictTrendOn || lenientTrendOn) && sma20Now > 0 && sma50Now > 0
                && (lenientTrendOn || sma200Now > 0)) {
            boolean bullClose;
            boolean bearClose;
            String priceFailReason;
            if (strictTrendOn) {
                // Strict gate wins when both are on (it implies the lenient gate).
                bullClose = close > sma20Now && close > sma50Now && close > sma200Now;
                bearClose = close < sma20Now && close < sma50Now && close < sma200Now;
                priceFailReason = "close not above all SMAs (20/50/200)";
            } else {
                // Lenient-only: 200 SMA is ignored.
                bullClose = close > sma20Now && close > sma50Now;
                bearClose = close < sma20Now && close < sma50Now;
                priceFailReason = "close not above SMA 20 and 50 (lenient)";
            }
            // Alignment evaluation: strict needs 200; lenient only needs 20 vs 50.
            boolean strictAlignOn  = riskSettings.isEnableSmaAlignmentCheck();
            boolean lenientAlignOn = riskSettings.isEnableSmaAlignmentCheckLenient();
            boolean canEvalStrictAlign = sma200Now > 0;
            boolean bullAligned;
            boolean bearAligned;
            if (strictAlignOn && canEvalStrictAlign) {
                // Strict wins when both are on.
                bullAligned = sma20Now > sma50Now && sma50Now > sma200Now;
                bearAligned = sma20Now < sma50Now && sma50Now < sma200Now;
            } else {
                // Lenient-only or strict can't be evaluated yet (200 warming up).
                bullAligned = sma20Now > sma50Now;
                bearAligned = sma20Now < sma50Now;
            }
            boolean alignmentOn = (strictAlignOn && canEvalStrictAlign) || lenientAlignOn;
            String priceFailReasonSell = priceFailReason.replace("not above", "not below");
            // Alignment rejection labels follow the gate that's actually evaluating.
            boolean strictAlignEvaluated = strictAlignOn && canEvalStrictAlign;
            String alignBuyReason  = strictAlignEvaluated
                ? "SMA not aligned (need 20>50>200)"
                : "SMA not aligned (need 20>50, lenient)";
            String alignSellReason = strictAlignEvaluated
                ? "SMA not aligned (need 20<50<200)"
                : "SMA not aligned (need 20<50, lenient)";
            if (greenCandle && !(bullClose && (!alignmentOn || bullAligned))) {
                String potentialSetup = detectBuyBreakout(open, high, low, close, levels, atp, broken, fyersSymbol, true);
                if (potentialSetup != null) {
                    // Two distinct rejection causes share this branch — surface them as separate
                    // filterName values so the EOD page can chip-filter by which check tripped.
                    boolean priceFail = !bullClose;
                    String reason = priceFail ? priceFailReason : alignBuyReason;
                    String filterName = priceFail ? "SMA_TREND" : "SMA_ALIGNMENT";
                    String detail = reason + ": close=" + String.format("%.2f", close)
                        + " sma20=" + String.format("%.2f", sma20Now)
                        + " sma50=" + String.format("%.2f", sma50Now)
                        + " sma200=" + String.format("%.2f", sma200Now);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + potentialSetup + " blocked by 5-min SMA trend — " + detail);
                    recordRejection(fyersSymbol, potentialSetup, close, filterName, detail);
                }
            } else if (redCandle && !(bearClose && (!alignmentOn || bearAligned))) {
                String potentialSetup = detectSellBreakout(open, high, low, close, levels, atp, broken, fyersSymbol, true);
                if (potentialSetup != null) {
                    boolean priceFail = !bearClose;
                    String reason = priceFail ? priceFailReasonSell : alignSellReason;
                    String filterName = priceFail ? "SMA_TREND" : "SMA_ALIGNMENT";
                    String detail = reason + ": close=" + String.format("%.2f", close)
                        + " sma20=" + String.format("%.2f", sma20Now)
                        + " sma50=" + String.format("%.2f", sma50Now)
                        + " sma200=" + String.format("%.2f", sma200Now);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + potentialSetup + " blocked by 5-min SMA trend — " + detail);
                    recordRejection(fyersSymbol, potentialSetup, close, filterName, detail);
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
                    String detail = "close (" + String.format("%.2f", close) + ") below ATP (" + String.format("%.2f", atp) + ")";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + wouldMatch + " — skipped, " + detail);
                    recordRejection(fyersSymbol, wouldMatch, close, "ATP", detail);
                }
                return;
            }
            String buySetup = detectBuyBreakout(open, high, low, close, levels, atp, broken, fyersSymbol);
            if (buySetup != null) {
                // Master toggle for the counter-trend family — DH classification depends on
                // close-vs-CPR position (DH above CPR top is trend-following, not mean-rev).
                if (isMeanReversionOrMagnet(buySetup, close, levels.getTc(), levels.getBc())
                        && !riskSettings.isEnableMeanReversionTrades()) {
                    String detail = "mean-reversion trades disabled (master toggle off)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " — skipped, " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "MEAN_REVERSION_DISABLED", detail);
                    return;
                }
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, true, buySetup, close);
                if (prob == null) {
                    // LTF-priority gate rejected: standard buy with close ≤ daily TC (no LTF
                    // support). Magnets always fire (HTF/LTF bypassed) so they don't reach here.
                    String detail = "buy requires close > daily TC (LTF bullish); close="
                        + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " — skipped, " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "LTF_OPPOSED", detail);
                    return;
                }
                if (!isProbabilityEnabled(prob)) {
                    // Probability tier toggled off in settings (e.g. enableMpt=false).
                    String detail = prob + " not enabled";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " — skipped, " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "PROB_DISABLED", detail);
                    return;
                }
                // SMA level-count filter: skip if any CPR zone sits between SMA and broken level
                if (evaluateSmaFilter(fyersSymbol, buySetup, close, levels, atr) == 2) return;
                // NIFTY index alignment filter — when on, a misaligned trade is downgraded to
                // LPT (smaller size) instead of being rejected. LPT trades skip ALL remaining
                // NIFTY-level filters (HTF Hurdle, 5m Hurdle, NIFTY HTF Candle Direction) — the
                // trade is taken purely on stock-level merits. If enableLpt is off, the
                // misaligned trade is rejected outright.
                boolean niftyDowngraded = false;
                if (checkIndexAlignment(fyersSymbol, buySetup, true) == NiftyAlignStatus.SKIP) {
                    String niftyState = indexTrendService != null ? indexTrendService.getStickyState() : "?";
                    if (!riskSettings.isEnableLpt()) {
                        recordRejection(fyersSymbol, buySetup, close, "NIFTY_OPPOSED",
                            "NIFTY " + niftyState + " — buy would downgrade to LPT but LPT disabled");
                        return;
                    }
                    int lptLimit = riskSettings.getLptMaxTradesPerStockPerDay();
                    if (lptLimit > 0 && tradeHistoryService != null) {
                        int lptToday = tradeHistoryService.getSymbolTodayCountByProbability(fyersSymbol, "LPT");
                        if (lptToday >= lptLimit) {
                            recordRejection(fyersSymbol, buySetup, close, "LPT_DAILY_LIMIT",
                                "buy would downgrade to LPT but " + lptToday + " LPT trade(s) already today (limit "
                                + lptLimit + ")");
                            return;
                        }
                    }
                    prob = "LPT";
                    niftyDowngraded = true;
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " — NIFTY "
                        + niftyState + " misaligned, downgraded to LPT (NIFTY filters bypassed)");
                }
                // NIFTY HTF Hurdle — only when not downgraded to LPT.
                if (!niftyDowngraded) {
                    String niftyHurdleReject = checkNiftyHurdle(true);
                    if (niftyHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " SKIPPED — " + niftyHurdleReject);
                        recordRejection(fyersSymbol, buySetup, close, "NIFTY_HURDLE", niftyHurdleReject);
                        return;
                    }
                    // NIFTY OI Hurdle — independent toggle for the Max Call OI strike check.
                    String niftyOiHurdleReject = checkNiftyOiHurdle(true);
                    if (niftyOiHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " SKIPPED — " + niftyOiHurdleReject);
                        recordRejection(fyersSymbol, buySetup, close, "NIFTY_OI_HURDLE", niftyOiHurdleReject);
                        return;
                    }
                    // NIFTY 5m Hurdle — prior 5-min NIFTY close must have cleared nearest daily CPR hurdle.
                    String nifty5mHurdleReject = checkNifty5mHurdle(true, candle.startMinute);
                    if (nifty5mHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " SKIPPED — " + nifty5mHurdleReject);
                        recordRejection(fyersSymbol, buySetup, close, "NIFTY_5M_HURDLE", nifty5mHurdleReject);
                        return;
                    }
                    // Virgin CPR Hurdle — zone-based: inside zone or within headroom × ATR rejects.
                    String virginCprHurdleReject = checkNiftyVirginCprHurdle(true, candle.startMinute);
                    if (virginCprHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " SKIPPED — " + virginCprHurdleReject);
                        recordRejection(fyersSymbol, buySetup, close, "VIRGIN_CPR_HURDLE", virginCprHurdleReject);
                        return;
                    }
                }
                // In-progress 1h candle direction — buy needs the currently-forming 1h bar to be green.
                // This is the STOCK's 1h candle, always applied.
                String htfCandleReject = checkHtfCandleColor(true, fyersSymbol);
                if (htfCandleReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " SKIPPED — " + htfCandleReject);
                    recordRejection(fyersSymbol, buySetup, close, "HTF_CANDLE_OPPOSED", htfCandleReject);
                    return;
                }
                // NIFTY in-progress 1h candle direction — only when not downgraded to LPT.
                if (!niftyDowngraded) {
                    String niftyHtfCandleReject = checkNiftyHtfCandleColor(true);
                    if (niftyHtfCandleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + " SKIPPED — " + niftyHtfCandleReject);
                        recordRejection(fyersSymbol, buySetup, close, "NIFTY_HTF_CANDLE_OPPOSED", niftyHtfCandleReject);
                        return;
                    }
                }
                fireSignal(fyersSymbol, buySetup, open, high, low, close, candle.volume, atr, levels, prob,
                    lastTriggerRoute.remove(fyersSymbol));
                return;
            } else {
                if (!broken.isEmpty()) {
                    String wouldMatch = detectBuyBreakout(open, high, low, close, levels, atp, Collections.emptySet(), fyersSymbol);
                    if (wouldMatch != null && broken.contains(wouldMatch)) {
                        String detail = "level " + wouldMatch + " already traded today";
                        eventService.log("[INFO] " + fyersSymbol + " " + wouldMatch + " — skipped, level already traded");
                        recordRejection(fyersSymbol, wouldMatch, close, "LEVEL_BROKEN", detail);
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
                    String detail = "close (" + String.format("%.2f", close) + ") above ATP (" + String.format("%.2f", atp) + ")";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + wouldMatch + " — skipped, " + detail);
                    recordRejection(fyersSymbol, wouldMatch, close, "ATP", detail);
                }
                return;
            }
            String sellSetup = detectSellBreakout(open, high, low, close, levels, atp, broken, fyersSymbol);
            if (sellSetup != null) {
                // Master toggle for the counter-trend family — DL classification depends on
                // close-vs-CPR position (DL below CPR bottom is trend-following, not mean-rev).
                if (isMeanReversionOrMagnet(sellSetup, close, levels.getTc(), levels.getBc())
                        && !riskSettings.isEnableMeanReversionTrades()) {
                    String detail = "mean-reversion trades disabled (master toggle off)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " — skipped, " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "MEAN_REVERSION_DISABLED", detail);
                    return;
                }
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, false, sellSetup, close);
                if (prob == null) {
                    String detail = "sell requires close < daily BC (LTF bearish); close="
                        + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " — skipped, " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "LTF_OPPOSED", detail);
                    return;
                }
                if (!isProbabilityEnabled(prob)) {
                    String detail = prob + " not enabled";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " — skipped, " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "PROB_DISABLED", detail);
                    return;
                }
                // SMA level-count filter: skip if any CPR zone sits between SMA and broken level
                if (evaluateSmaFilter(fyersSymbol, sellSetup, close, levels, atr) == 2) return;
                // NIFTY index alignment filter — when on, a misaligned trade is downgraded to
                // LPT (smaller size) instead of being rejected. LPT trades skip ALL remaining
                // NIFTY-level filters (HTF Hurdle, 5m Hurdle, NIFTY HTF Candle Direction) — the
                // trade is taken purely on stock-level merits. If enableLpt is off, the
                // misaligned trade is rejected outright.
                boolean niftyDowngraded = false;
                if (checkIndexAlignment(fyersSymbol, sellSetup, false) == NiftyAlignStatus.SKIP) {
                    String niftyState = indexTrendService != null ? indexTrendService.getStickyState() : "?";
                    if (!riskSettings.isEnableLpt()) {
                        recordRejection(fyersSymbol, sellSetup, close, "NIFTY_OPPOSED",
                            "NIFTY " + niftyState + " — sell would downgrade to LPT but LPT disabled");
                        return;
                    }
                    int lptLimit = riskSettings.getLptMaxTradesPerStockPerDay();
                    if (lptLimit > 0 && tradeHistoryService != null) {
                        int lptToday = tradeHistoryService.getSymbolTodayCountByProbability(fyersSymbol, "LPT");
                        if (lptToday >= lptLimit) {
                            recordRejection(fyersSymbol, sellSetup, close, "LPT_DAILY_LIMIT",
                                "sell would downgrade to LPT but " + lptToday + " LPT trade(s) already today (limit "
                                + lptLimit + ")");
                            return;
                        }
                    }
                    prob = "LPT";
                    niftyDowngraded = true;
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " — NIFTY "
                        + niftyState + " misaligned, downgraded to LPT (NIFTY filters bypassed)");
                }
                // NIFTY HTF Hurdle — only when not downgraded to LPT.
                if (!niftyDowngraded) {
                    String niftyHurdleReject = checkNiftyHurdle(false);
                    if (niftyHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " SKIPPED — " + niftyHurdleReject);
                        recordRejection(fyersSymbol, sellSetup, close, "NIFTY_HURDLE", niftyHurdleReject);
                        return;
                    }
                    // NIFTY OI Hurdle — independent toggle for the Max Put OI strike check.
                    String niftyOiHurdleReject = checkNiftyOiHurdle(false);
                    if (niftyOiHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " SKIPPED — " + niftyOiHurdleReject);
                        recordRejection(fyersSymbol, sellSetup, close, "NIFTY_OI_HURDLE", niftyOiHurdleReject);
                        return;
                    }
                    // NIFTY 5m Hurdle — prior 5-min NIFTY close must have cleared nearest daily CPR hurdle.
                    String nifty5mHurdleReject = checkNifty5mHurdle(false, candle.startMinute);
                    if (nifty5mHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " SKIPPED — " + nifty5mHurdleReject);
                        recordRejection(fyersSymbol, sellSetup, close, "NIFTY_5M_HURDLE", nifty5mHurdleReject);
                        return;
                    }
                    // Virgin CPR Hurdle — zone-based: inside zone or within headroom × ATR rejects.
                    String virginCprHurdleReject = checkNiftyVirginCprHurdle(false, candle.startMinute);
                    if (virginCprHurdleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " SKIPPED — " + virginCprHurdleReject);
                        recordRejection(fyersSymbol, sellSetup, close, "VIRGIN_CPR_HURDLE", virginCprHurdleReject);
                        return;
                    }
                }
                // In-progress 1h candle direction — sell needs the currently-forming 1h bar to be red.
                // This is the STOCK's 1h candle, always applied.
                String htfCandleReject = checkHtfCandleColor(false, fyersSymbol);
                if (htfCandleReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " SKIPPED — " + htfCandleReject);
                    recordRejection(fyersSymbol, sellSetup, close, "HTF_CANDLE_OPPOSED", htfCandleReject);
                    return;
                }
                // NIFTY in-progress 1h candle direction — only when not downgraded to LPT.
                if (!niftyDowngraded) {
                    String niftyHtfCandleReject = checkNiftyHtfCandleColor(false);
                    if (niftyHtfCandleReject != null) {
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + " SKIPPED — " + niftyHtfCandleReject);
                        recordRejection(fyersSymbol, sellSetup, close, "NIFTY_HTF_CANDLE_OPPOSED", niftyHtfCandleReject);
                        return;
                    }
                }
                fireSignal(fyersSymbol, sellSetup, open, high, low, close, candle.volume, atr, levels, prob,
                    lastTriggerRoute.remove(fyersSymbol));
            } else {
                // No sell breakout detected — log if close is below a key level for debugging
                if (!broken.isEmpty()) {
                    String wouldMatch = detectSellBreakout(open, high, low, close, levels, atp, Collections.emptySet(), fyersSymbol);
                    if (wouldMatch != null && broken.contains(wouldMatch)) {
                        String detail = "level " + wouldMatch + " already traded today";
                        eventService.log("[INFO] " + fyersSymbol + " " + wouldMatch + " — skipped, level already traded");
                        recordRejection(fyersSymbol, wouldMatch, close, "LEVEL_BROKEN", detail);
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
        // Lenient 5-min SMA trend check (close > SMA 20 AND > SMA 50, ignores 200). Independent
        // of the strict gate above; if both are on, the strict check has already filtered.
        if (!skipTrendFilters && riskSettings.isEnableSmaTrendCheckLenient()
                && sma > 0 && sma50 > 0
                && !(close > sma && close > sma50)) {
            return null;
        }
        // SMA alignment check for buys: 20 > 50 > 200 (stricter than close > SMAs)
        if (!skipTrendFilters && riskSettings.isEnableSmaAlignmentCheck()
                && sma > 0 && sma50 > 0 && sma200 > 0
                && !(sma > sma50 && sma50 > sma200)) {
            return null;
        }
        // Lenient alignment for buys: only requires 20 > 50 (skips 50 > 200).
        if (!skipTrendFilters && riskSettings.isEnableSmaAlignmentCheckLenient()
                && sma > 0 && sma50 > 0
                && !(sma > sma50)) {
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
        double tc = levels.getTc(), bc = levels.getBc();
        double s1 = levels.getS1(), pl = levels.getPl();
        double s2 = levels.getS2(), s3 = levels.getS3(), s4 = levels.getS4();

        double cprTop = Math.max(tc, bc);
        double r1ph   = Math.max(r1, ph);
        double s1pl   = Math.max(s1, pl);

        // Two routes per level (priority R4 → R3 → R2 → R1+PDH → CPR → S1+PDL → S2 → S3 → S4):
        //   Route 1 — bullish marubozu that closes past the level fires immediately. Iterates
        //             every level so the highest level the marubozu broke wins.
        //   Route 2 — pattern retest at the single armed buy level (closest level below the
        //             latest close). Pattern's lowest point must reach the level. Patterns:
        //             hammer, bullish engulfing, bullish doji rev, morning star.
        double atrForPattern = atrService.getAtr(fyersSymbol);
        String armed = armedBuyLevel.get(fyersSymbol);

        if (r4 > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_R4", r4, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (r3 > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_R3", r3, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (r2 > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_R2", r2, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (r1ph > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_R1_PDH", r1ph, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (cprTop > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_CPR", cprTop, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (s1pl > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_S1_PDL", s1pl, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (s2 > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_S2", s2, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (s3 > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_S3", s3, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (s4 > 0) {
            String hit = checkBuyAtLevel("BUY_ABOVE_S4", s4, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }

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
        // Lenient 5-min SMA trend check (close < SMA 20 AND < SMA 50, ignores 200).
        if (!skipTrendFilters && riskSettings.isEnableSmaTrendCheckLenient()
                && sma > 0 && sma50 > 0
                && !(close < sma && close < sma50)) {
            return null;
        }
        // SMA alignment check for sells: 20 < 50 < 200 (stricter than close < SMAs)
        if (!skipTrendFilters && riskSettings.isEnableSmaAlignmentCheck()
                && sma > 0 && sma50 > 0 && sma200 > 0
                && !(sma < sma50 && sma50 < sma200)) {
            return null;
        }
        // Lenient alignment for sells: only requires 20 < 50 (skips 50 < 200).
        if (!skipTrendFilters && riskSettings.isEnableSmaAlignmentCheckLenient()
                && sma > 0 && sma50 > 0
                && !(sma < sma50)) {
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
        double tc = levels.getTc(), bc = levels.getBc();
        double r1 = levels.getR1(), ph = levels.getPh();
        double r2v = levels.getR2(), r3v = levels.getR3(), r4v = levels.getR4();

        double cprBot = Math.min(tc, bc);
        double s1plLo = Math.min(s1, pl);
        double r1phLo = Math.min(r1, ph);

        // Two routes per level (priority S4 → S3 → S2 → S1+PDL → CPR → R1+PDH → R2 → R3 → R4):
        //   Route 1 — bearish marubozu that closes past the level fires immediately.
        //   Route 2 — pattern retest at the single armed sell level (closest level above the
        //             latest close). Pattern's highest point must reach the level. Patterns:
        //             shooting star, bearish engulfing, bearish doji rev, evening star.
        double atrForPattern = atrService.getAtr(fyersSymbol);
        String armed = armedSellLevel.get(fyersSymbol);

        if (s4 > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_S4", s4, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (s3 > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_S3", s3, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (s2 > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_S2", s2, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (s1plLo > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_S1_PDL", s1plLo, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (cprBot > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_CPR", cprBot, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (r1phLo > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_R1_PDH", r1phLo, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (r2v > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_R2", r2v, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (r3v > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_R3", r3v, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }
        if (r4v > 0) {
            String hit = checkSellAtLevel("SELL_BELOW_R4", r4v, open, high, low, close, atrForPattern, broken, armed, fyersSymbol);
            if (hit != null) return hit;
        }

        return null;
    }

    /**
     * Check both routes (marubozu breakout + pattern retest) for a single buy setup at one
     * level. Returns the setup name on hit, null otherwise. Sets {@code lastTriggerRoute}
     * so {@link #fireSignal} can record which route fired.
     */
    private String checkBuyAtLevel(String setupName, double level,
                                    double open, double high, double low, double close,
                                    double atr, Set<String> broken, String armed, String fyersSymbol) {
        if (broken.contains(setupName)) return null;
        double maruBody  = riskSettings.getMarubozuBodyAtrMult();
        double maruWicks = riskSettings.getMarubozuMaxWicksPctOfBody();
        double pinReject = riskSettings.getPinBarRejectionWickBodyMult();
        double pinOpp    = riskSettings.getPinBarOppositeWickBodyMult();
        // Route 1 — bullish marubozu past the level. If the bar's open is already above the
        // level, the level was broken earlier — this is a retest rejection (tagged
        // MARUBOZU_RETEST). If the open is at/below the level and the bar punched through,
        // it's a fresh breakout (tagged MARUBOZU_BREAKOUT).
        if (CandlePatternDetector.isBullishMarubozu(open, high, low, close, atr, maruBody, maruWicks)
                && close > level && (open <= level || low <= level)) {
            if (!isLargeCandleBlocked(close - open, atr)) {
                String tag = (open > level) ? "MARUBOZU_RETEST" : "MARUBOZU_BREAKOUT";
                lastTriggerRoute.put(fyersSymbol, tag);
                return setupName;
            }
        }
        // Route 2 — retest pattern at the single armed buy level. Pattern's lowest point must
        // reach the level.
        if (!setupName.equals(armed)) return null;
        CandleAggregator.CandleBar curr = candleAggregator.getLastCompletedCandle(fyersSymbol);
        if (curr == null) return null;
        CandleAggregator.CandleBar prev = candleAggregator.getPreviousCandle(fyersSymbol);
        double engMin    = riskSettings.getEngulfingMinBodyMultiple();
        double engAtr    = riskSettings.getEngulfingMinBodyAtrMult();
        double pierPrev  = riskSettings.getPiercingPrevBodyAtrMult();
        double pierPen   = riskSettings.getPiercingPenetrationPct();
        double tweezPrev = riskSettings.getTweezerPrevBodyAtrMult();
        double tweezMatch = riskSettings.getTweezerLowHighMatchAtr();
        double dojiBody  = riskSettings.getDojiBodyMaxRangeRatio();
        double dojiPrev  = riskSettings.getDojiPrevBodyAtrMult();
        double starOuter = riskSettings.getStarOuterBodyAtrMult();
        double starMid   = riskSettings.getStarMiddleBodyMaxMultOfOuter();
        double haramiBody = riskSettings.getHaramiBodyAtrMult();
        double haramiInner = riskSettings.getHaramiInnerBodyMaxRatio();
        // Hammer (1 bar)
        if (CandlePatternDetector.isBullishHammer(open, high, low, close, pinReject, pinOpp)
                && low <= level
                && !isLargeCandleBlocked(Math.abs(close - open), atr)) {
            lastTriggerRoute.put(fyersSymbol, "HAMMER_RETEST");
            return setupName;
        }
        // Bullish engulfing (2 bars)
        if (prev != null && CandlePatternDetector.isBullishEngulfing(prev, curr, engMin, atr, engAtr)
                && Math.min(prev.low, curr.low) <= level
                && !isLargeCandleBlocked(close - open, atr)) {
            lastTriggerRoute.put(fyersSymbol, "ENGULFING_RETEST");
            return setupName;
        }
        // Piercing line (2 bars) — partial reversal, weaker than engulfing
        if (prev != null && CandlePatternDetector.isPiercingLine(prev, curr, atr, pierPrev, pierPen)
                && Math.min(prev.low, curr.low) <= level
                && !isLargeCandleBlocked(close - open, atr)) {
            lastTriggerRoute.put(fyersSymbol, "PIERCING_RETEST");
            return setupName;
        }
        // Tweezer bottom (2 bars) — matching lows, color flip, body sizing relaxed
        if (prev != null && CandlePatternDetector.isTweezerBottom(prev, curr, atr, tweezPrev, tweezMatch)
                && Math.min(prev.low, curr.low) <= level) {
            // Body unconstrained for tweezer — matching extreme is the signature, no large-body check.
            lastTriggerRoute.put(fyersSymbol, "TWEEZER_RETEST");
            return setupName;
        }
        // Bullish doji reversal (2 bars)
        if (prev != null && CandlePatternDetector.isBullishDojiReversal(prev, curr, atr, dojiBody, dojiPrev)
                && Math.min(prev.low, curr.low) <= level) {
            // Doji is intentionally a tiny body; large-candle filter doesn't apply.
            lastTriggerRoute.put(fyersSymbol, "DOJI_RETEST");
            return setupName;
        }
        // Morning star (3 bars) — pull bars from the deque.
        CandleAggregator.CandleBar bar1 = thirdMostRecentCandle(fyersSymbol);
        if (bar1 != null && prev != null
                && CandlePatternDetector.isMorningStar(bar1, prev, curr, atr, starOuter, starMid)
                && Math.min(Math.min(bar1.low, prev.low), curr.low) <= level
                && !isLargeCandleBlocked(close - open, atr)) {
            lastTriggerRoute.put(fyersSymbol, "STAR_RETEST");
            return setupName;
        }
        // Three Inside Up (3-bar harami + confirmation)
        if (bar1 != null && prev != null
                && CandlePatternDetector.isThreeInsideUp(bar1, prev, curr, atr, haramiBody, haramiInner)
                && Math.min(Math.min(bar1.low, prev.low), curr.low) <= level
                && !isLargeCandleBlocked(close - open, atr)) {
            lastTriggerRoute.put(fyersSymbol, "HARAMI_RETEST");
            return setupName;
        }
        return null;
    }

    /**
     * Sell mirror of {@link #checkBuyAtLevel}.
     */
    private String checkSellAtLevel(String setupName, double level,
                                     double open, double high, double low, double close,
                                     double atr, Set<String> broken, String armed, String fyersSymbol) {
        if (broken.contains(setupName)) return null;
        double maruBody  = riskSettings.getMarubozuBodyAtrMult();
        double maruWicks = riskSettings.getMarubozuMaxWicksPctOfBody();
        double pinReject = riskSettings.getPinBarRejectionWickBodyMult();
        double pinOpp    = riskSettings.getPinBarOppositeWickBodyMult();
        // Route 1 — bearish marubozu past the level. If the bar's open is already below the
        // level, the level was broken down earlier — this is a retest rejection (tagged
        // MARUBOZU_RETEST). If the open is at/above the level and the bar punched through,
        // it's a fresh breakdown (tagged MARUBOZU_BREAKOUT).
        if (CandlePatternDetector.isBearishMarubozu(open, high, low, close, atr, maruBody, maruWicks)
                && close < level && (open >= level || high >= level)) {
            if (!isLargeCandleBlocked(open - close, atr)) {
                String tag = (open < level) ? "MARUBOZU_RETEST" : "MARUBOZU_BREAKOUT";
                lastTriggerRoute.put(fyersSymbol, tag);
                return setupName;
            }
        }
        // Route 2 — retest pattern at the single armed sell level. Pattern's highest point
        // must reach the level.
        if (!setupName.equals(armed)) return null;
        CandleAggregator.CandleBar curr = candleAggregator.getLastCompletedCandle(fyersSymbol);
        if (curr == null) return null;
        CandleAggregator.CandleBar prev = candleAggregator.getPreviousCandle(fyersSymbol);
        double engMin    = riskSettings.getEngulfingMinBodyMultiple();
        double engAtr    = riskSettings.getEngulfingMinBodyAtrMult();
        double pierPrev  = riskSettings.getPiercingPrevBodyAtrMult();
        double pierPen   = riskSettings.getPiercingPenetrationPct();
        double tweezPrev = riskSettings.getTweezerPrevBodyAtrMult();
        double tweezMatch = riskSettings.getTweezerLowHighMatchAtr();
        double dojiBody  = riskSettings.getDojiBodyMaxRangeRatio();
        double dojiPrev  = riskSettings.getDojiPrevBodyAtrMult();
        double starOuter = riskSettings.getStarOuterBodyAtrMult();
        double starMid   = riskSettings.getStarMiddleBodyMaxMultOfOuter();
        double haramiBody = riskSettings.getHaramiBodyAtrMult();
        double haramiInner = riskSettings.getHaramiInnerBodyMaxRatio();
        // Shooting star (1 bar)
        if (CandlePatternDetector.isShootingStar(open, high, low, close, pinReject, pinOpp)
                && high >= level
                && !isLargeCandleBlocked(Math.abs(close - open), atr)) {
            lastTriggerRoute.put(fyersSymbol, "HAMMER_RETEST");
            return setupName;
        }
        // Bearish engulfing (2 bars)
        if (prev != null && CandlePatternDetector.isBearishEngulfing(prev, curr, engMin, atr, engAtr)
                && Math.max(prev.high, curr.high) >= level
                && !isLargeCandleBlocked(open - close, atr)) {
            lastTriggerRoute.put(fyersSymbol, "ENGULFING_RETEST");
            return setupName;
        }
        // Dark cloud cover (2 bars) — partial reversal, weaker than engulfing
        if (prev != null && CandlePatternDetector.isDarkCloudCover(prev, curr, atr, pierPrev, pierPen)
                && Math.max(prev.high, curr.high) >= level
                && !isLargeCandleBlocked(open - close, atr)) {
            lastTriggerRoute.put(fyersSymbol, "PIERCING_RETEST");
            return setupName;
        }
        // Tweezer top (2 bars) — matching highs, color flip, body sizing relaxed
        if (prev != null && CandlePatternDetector.isTweezerTop(prev, curr, atr, tweezPrev, tweezMatch)
                && Math.max(prev.high, curr.high) >= level) {
            lastTriggerRoute.put(fyersSymbol, "TWEEZER_RETEST");
            return setupName;
        }
        // Bearish doji reversal (2 bars)
        if (prev != null && CandlePatternDetector.isBearishDojiReversal(prev, curr, atr, dojiBody, dojiPrev)
                && Math.max(prev.high, curr.high) >= level) {
            lastTriggerRoute.put(fyersSymbol, "DOJI_RETEST");
            return setupName;
        }
        // Evening star (3 bars)
        CandleAggregator.CandleBar bar1 = thirdMostRecentCandle(fyersSymbol);
        if (bar1 != null && prev != null
                && CandlePatternDetector.isEveningStar(bar1, prev, curr, atr, starOuter, starMid)
                && Math.max(Math.max(bar1.high, prev.high), curr.high) >= level
                && !isLargeCandleBlocked(open - close, atr)) {
            lastTriggerRoute.put(fyersSymbol, "STAR_RETEST");
            return setupName;
        }
        // Three Inside Down (3-bar harami + confirmation)
        if (bar1 != null && prev != null
                && CandlePatternDetector.isThreeInsideDown(bar1, prev, curr, atr, haramiBody, haramiInner)
                && Math.max(Math.max(bar1.high, prev.high), curr.high) >= level
                && !isLargeCandleBlocked(open - close, atr)) {
            lastTriggerRoute.put(fyersSymbol, "HARAMI_RETEST");
            return setupName;
        }
        return null;
    }

    /** Returns the 3rd-most-recent completed candle (for morning/evening star), or null. */
    private CandleAggregator.CandleBar thirdMostRecentCandle(String fyersSymbol) {
        List<CandleAggregator.CandleBar> list = candleAggregator.getCompletedCandles(fyersSymbol);
        if (list == null || list.size() < 3) return null;
        return list.get(list.size() - 3);
    }

    /** Large-candle-body filter — if enabled, reject when the entry-trigger bar's body
     *  exceeds {@code largeCandleBodyAtrThreshold × ATR}. Avoids exhaustion / blow-off
     *  entries. {@code body} is passed in absolute terms by the caller. */
    private boolean isLargeCandleBlocked(double body, double atr) {
        if (!riskSettings.isEnableLargeCandleBodyFilter()) return false;
        if (atr <= 0) return false;
        double maxBody = riskSettings.getLargeCandleBodyAtrThreshold() * atr;
        if (maxBody <= 0) return false;
        return body > maxBody;
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
            info.filterName = classifyDownstreamRejection(status, filtered);
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
            info.filterName = "EXCEPTION";
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
    /** Result of the NIFTY alignment check. SKIP = trade rejected; OK = aligned or check off. */
    private enum NiftyAlignStatus { OK, SKIP }

    /**
     * NIFTY index alignment filter.
     * <ul>
     *   <li>Buys allowed when NIFTY state ∈ {BULLISH, BULLISH_REVERSAL}.</li>
     *   <li>Sells allowed when NIFTY state ∈ {BEARISH, BEARISH_REVERSAL}.</li>
     *   <li>SIDEWAYS / NEUTRAL / opposite-direction → skip.</li>
     * </ul>
     * BULLISH_REVERSAL = CPR bearish but both 5-min SMA factors flipped bullish (downtrend rolling
     * over). BEARISH_REVERSAL is the mirror — taken on the assumption the structural turn anticipates
     * the trade direction. Returns OK if the filter is disabled or index data isn't available yet.
     */
    private NiftyAlignStatus checkIndexAlignment(String fyersSymbol, String setup, boolean isBuy) {
        if (!riskSettings.isEnableIndexAlignment()) return NiftyAlignStatus.OK;
        if (indexTrendService == null) return NiftyAlignStatus.OK;
        try {
            // Sticky state — only updates at NIFTY 5-min candle close, so trade decisions
            // don't oscillate tick-to-tick during a bar.
            String state = indexTrendService.getStickyState();
            if ("NEUTRAL".equals(state)) return NiftyAlignStatus.OK; // no data / inside CPR
            boolean aligned = isBuy
                ? ("BULLISH".equals(state) || "BULLISH_REVERSAL".equals(state))
                : ("BEARISH".equals(state) || "BEARISH_REVERSAL".equals(state));
            if (aligned) return NiftyAlignStatus.OK;
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup
                + " SKIPPED — NIFTY " + state + ", need "
                + (isBuy ? "BULLISH or BULLISH_REVERSAL for buy" : "BEARISH or BEARISH_REVERSAL for sell"));
            return NiftyAlignStatus.SKIP;
        } catch (Exception e) {
            log.warn("[BreakoutScanner] Index alignment check failed for {}: {}", fyersSymbol, e.getMessage());
        }
        return NiftyAlignStatus.OK;
    }

    /**
     * NIFTY HTF Hurdle filter. When NIFTY's prior 15-min close hasn't cleared its nearest
     * weekly hurdle in trade direction, all stock trades in that direction are skipped
     * until the next 15-min close commits.
     *
     * <p>The "15-min close" is derived from the 5-min candle grid — the close of the most
     * recent completed 5-min bar whose end aligns with a 15-min boundary (9:30, 9:45,
     * 10:00, …). Switching from 1-hour to 15-minute makes the filter react ~4× faster,
     * which is the explicit user requirement.
     *
     * <p>Returns a non-null reason string when the filter rejects, or null when the trade
     * is allowed (filter off, no hurdle, hurdle cleared, or fail-open data missing).
     */
    private String checkNiftyHurdle(boolean isBuy) {
        if (!riskSettings.isEnableNiftyHtfHurdleFilter()) return null;
        if (marketDataService == null || weeklyCprService == null) return null;
        try {
            String niftySym = IndexTrendService.NIFTY_SYMBOL;
            double niftyPrice = marketDataService.getLtp(niftySym);
            if (niftyPrice <= 0) return null; // no LTP yet — fail-open

            WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(niftySym);
            if (wl == null) return null; // weekly levels not loaded — fail-open

            // Base weekly candidates plus the trade-direction's option-chain Max OI strike when
            // available. Max Call OI = resistance (joins buy hurdle set); Max Put OI = support
            // (joins sell hurdle set). OI strikes are zero until NiftyOptionOiService's first
            // successful fetch — falls through to base set with no regression.
            java.util.List<Double> candidateLevels = new java.util.ArrayList<>(6);
            java.util.List<String> candidateNames  = new java.util.ArrayList<>(6);
            if (isBuy) {
                candidateLevels.add(wl.r1);    candidateNames.add("weekly R1");
                candidateLevels.add(wl.ph);    candidateNames.add("weekly PWH");
                candidateLevels.add(wl.tc);    candidateNames.add("weekly TC");
                candidateLevels.add(wl.pivot); candidateNames.add("weekly Pivot");
                candidateLevels.add(wl.bc);    candidateNames.add("weekly BC");
            } else {
                candidateLevels.add(wl.s1);    candidateNames.add("weekly S1");
                candidateLevels.add(wl.pl);    candidateNames.add("weekly PWL");
                candidateLevels.add(wl.tc);    candidateNames.add("weekly TC");
                candidateLevels.add(wl.pivot); candidateNames.add("weekly Pivot");
                candidateLevels.add(wl.bc);    candidateNames.add("weekly BC");
            }
            // Max Call/Put OI is split into checkNiftyOiHurdle so the user can toggle it
            // independently of the weekly-levels HTF gate. Virgin CPR is intentionally NOT
            // added here — daily-level concept stays at the daily-CPR (5m) gate.

            // Nearest hurdle in trade direction relative to NIFTY's current price.
            double chosenLevel = 0;
            String chosenName = null;
            for (int i = 0; i < candidateLevels.size(); i++) {
                double lv = candidateLevels.get(i);
                if (lv <= 0) continue;
                if (isBuy) {
                    if (lv < niftyPrice && lv > chosenLevel) { chosenLevel = lv; chosenName = candidateNames.get(i); }
                } else {
                    if (lv > niftyPrice && (chosenName == null || lv < chosenLevel)) {
                        chosenLevel = lv; chosenName = candidateNames.get(i);
                    }
                }
            }
            if (chosenName == null) return null; // no hurdle in trade direction → clear path

            // Last completed 15-min close on NIFTY. Pre-9:30 IST (before NIFTY's first 15-min
            // boundary of the day), no 15-min close exists yet — silently fail-open so the
            // filter doesn't reject during the warmup window. Trades only fire from 9:30
            // onwards anyway, by which time the 9:25-9:30 5-min bar has finalized and IS the
            // day's first 15-min close.
            Double priorHtfClose = candleAggregator != null
                ? candleAggregator.getLast15MinClose(niftySym) : null;
            if (priorHtfClose == null || priorHtfClose <= 0) return null;

            boolean cleared = isBuy ? priorHtfClose > chosenLevel : priorHtfClose < chosenLevel;
            if (!cleared) {
                return "NIFTY HTF hurdle at " + chosenName
                    + ": NIFTY " + String.format("%.2f", niftyPrice)
                    + ", prior 15-min close=" + String.format("%.2f", priorHtfClose)
                    + ", level " + String.format("%.2f", chosenLevel);
            }

            // Headroom check — reject if the nearest hurdle in the OPPOSITE direction (above
            // NIFTY LTP for buys, below for sells) is closer than minHeadroomAtr × NIFTY ATR.
            // Guards against firing when an upcoming weekly level is right above NIFTY (likely
            // to cap the move). Uses the same candidate set as the "cleared past" check.
            double minHeadroomAtr = riskSettings.getNiftyHurdleMinHeadroomAtr();
            if (minHeadroomAtr > 0 && atrService != null) {
                double niftyAtr = atrService.getAtr(niftySym);
                if (niftyAtr > 0) {
                    double minHeadroomPts = minHeadroomAtr * niftyAtr;
                    double upcomingLevel = 0;
                    String upcomingName = null;
                    for (int i = 0; i < candidateLevels.size(); i++) {
                        double lv = candidateLevels.get(i);
                        if (lv <= 0) continue;
                        if (isBuy) {
                            // For buys, upcoming hurdle = lowest level above LTP
                            if (lv > niftyPrice && (upcomingName == null || lv < upcomingLevel)) {
                                upcomingLevel = lv; upcomingName = candidateNames.get(i);
                            }
                        } else {
                            // For sells, upcoming hurdle = highest level below LTP
                            if (lv < niftyPrice && lv > upcomingLevel) {
                                upcomingLevel = lv; upcomingName = candidateNames.get(i);
                            }
                        }
                    }
                    if (upcomingName != null) {
                        double headroomPts = isBuy ? upcomingLevel - niftyPrice : niftyPrice - upcomingLevel;
                        if (headroomPts < minHeadroomPts) {
                            return "NIFTY hurdle ahead at " + upcomingName
                                + " (" + String.format("%.2f", upcomingLevel) + "): only "
                                + String.format("%.2f", headroomPts) + " pts headroom, need "
                                + String.format("%.2f", minHeadroomPts)
                                + " (" + minHeadroomAtr + " × NIFTY ATR " + String.format("%.2f", niftyAtr) + ")";
                        }
                    }
                }
            }
            return null; // prior 1h close (or LTP fallback) has cleared the hurdle, headroom OK
        } catch (Exception e) {
            log.warn("[BreakoutScanner] NIFTY hurdle check failed: {}", e.getMessage());
            return null; // fail-open
        }
    }

    /**
     * Independent NIFTY OI Hurdle filter — same prior-1h-close + headroom rules as
     * {@link #checkNiftyHurdle} but with a single candidate (Max Call OI for buys,
     * Max Put OI for sells). Split out so the OI level can be toggled separately.
     *
     * <p>Returns null on pass (filter off, no OI loaded, OI cleared past, or no prior
     * 1h close available — fail-open). Returns a non-null reason string to reject.
     */
    private String checkNiftyOiHurdle(boolean isBuy) {
        if (!riskSettings.isEnableNiftyOiHurdleFilter()) return null;
        if (marketDataService == null || niftyOptionOiService == null) return null;
        try {
            String niftySym = IndexTrendService.NIFTY_SYMBOL;
            double niftyPrice = marketDataService.getLtp(niftySym);
            if (niftyPrice <= 0) return null;

            double oiStrike = isBuy ? niftyOptionOiService.getMaxCallOiStrike()
                                    : niftyOptionOiService.getMaxPutOiStrike();
            String oiName = isBuy ? "Max Call OI" : "Max Put OI";
            if (oiStrike <= 0) return null; // OI not loaded yet — fail-open

            // Cleared-past check applies only when the OI strike is BEHIND NIFTY in trade
            // direction (i.e., NIFTY has pushed past the resistance/support). Otherwise the
            // headroom check below handles "OI right ahead of NIFTY".
            boolean strikeBehindLtp = isBuy ? oiStrike < niftyPrice : oiStrike > niftyPrice;
            if (strikeBehindLtp) {
                Double priorHtfClose = weeklyCprService != null ? weeklyCprService.getLastHigherTfClose(niftySym) : null;
                boolean usedPrevSession = false;
                boolean usedLiveLtp = false;
                boolean firstTradingDay = marketHolidayService != null && marketHolidayService.isFirstTradingDayOfWeek();
                if ((priorHtfClose == null || priorHtfClose <= 0) && !firstTradingDay && htfSmaService != null) {
                    Double prev = htfSmaService.getLastClose(niftySym);
                    if (prev != null && prev > 0) {
                        priorHtfClose = prev;
                        usedPrevSession = true;
                    }
                }
                if ((priorHtfClose == null || priorHtfClose <= 0) && firstTradingDay) {
                    priorHtfClose = niftyPrice;
                    usedLiveLtp = true;
                }
                if (priorHtfClose == null || priorHtfClose <= 0) {
                    return "NIFTY OI hurdle at " + oiName
                        + ": NIFTY " + String.format("%.2f", niftyPrice)
                        + ", level " + String.format("%.2f", oiStrike)
                        + ", no prior 1h close available"
                        + (firstTradingDay ? " — first trading day of week" : "");
                }
                boolean cleared = isBuy ? priorHtfClose > oiStrike : priorHtfClose < oiStrike;
                if (!cleared) {
                    String label = usedLiveLtp ? ", live LTP="
                                 : usedPrevSession ? ", prev session 1h close="
                                 : ", prior 1h close=";
                    return "NIFTY OI hurdle at " + oiName
                        + ": NIFTY " + String.format("%.2f", niftyPrice)
                        + label + String.format("%.2f", priorHtfClose)
                        + ", level " + String.format("%.2f", oiStrike);
                }
            }

            // Headroom check — when the OI strike is AHEAD of NIFTY in trade direction and
            // the gap is smaller than minHeadroomAtr × NIFTY ATR, the move is likely capped
            // by the option-writer wall before the breakout pays.
            double minHeadroomAtr = riskSettings.getNiftyOiHurdleMinHeadroomAtr();
            if (minHeadroomAtr > 0 && atrService != null) {
                double niftyAtr = atrService.getAtr(niftySym);
                if (niftyAtr > 0) {
                    boolean strikeAhead = isBuy ? oiStrike > niftyPrice : oiStrike < niftyPrice;
                    if (strikeAhead) {
                        double headroomPts = isBuy ? oiStrike - niftyPrice : niftyPrice - oiStrike;
                        double minHeadroomPts = minHeadroomAtr * niftyAtr;
                        if (headroomPts < minHeadroomPts) {
                            return "NIFTY OI hurdle ahead at " + oiName
                                + " (" + String.format("%.2f", oiStrike) + "): only "
                                + String.format("%.2f", headroomPts) + " pts headroom, need "
                                + String.format("%.2f", minHeadroomPts)
                                + " (" + minHeadroomAtr + " × NIFTY ATR " + String.format("%.2f", niftyAtr) + ")";
                        }
                    }
                }
            }
            return null;
        } catch (Exception e) {
            log.warn("[BreakoutScanner] NIFTY OI hurdle check failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * In-progress 1h HTF candle direction filter. When enabled, requires the currently-forming
     * 60-min bar's body to agree with the trade direction. Buys reject when the 1h bar is
     * strictly red (close &lt; open); sells reject when it's strictly green (close &gt; open).
     * Doji (close == open) passes both — only a clearly opposite-direction bar blocks the trade.
     *
     * <p>Fail-open when no in-progress bar exists yet (e.g. very first ticks of a new 1h
     * bucket, or low-volume stocks between boundaries) — consistent with other "data missing"
     * branches in the scanner. Returns null on pass, a short rejection-reason String otherwise.
     */
    private String checkHtfCandleColor(boolean isBuy, String fyersSymbol) {
        if (!riskSettings.isEnableHtfCandleFilter()) return null;
        if (marketDataService == null) return null;
        CandleAggregator.CandleBar bar = marketDataService.getInProgressHtfCandle(fyersSymbol);
        if (bar == null || bar.open <= 0 || bar.close <= 0) return null; // fail-open

        if (isBuy && bar.close < bar.open) {
            return "HTF 1h candle red — buy requires green: 1h open="
                + String.format("%.2f", bar.open) + ", close=" + String.format("%.2f", bar.close);
        }
        if (!isBuy && bar.close > bar.open) {
            return "HTF 1h candle green — sell requires red: 1h open="
                + String.format("%.2f", bar.open) + ", close=" + String.format("%.2f", bar.close);
        }
        return null;
    }

    /**
     * 5-min variant of {@link #checkNiftyHurdle}, against NIFTY's <i>daily</i> CPR levels (not
     * weekly). When a stock's 5-min breakout fires, NIFTY's prior 5-min close must already have
     * cleared its nearest daily-CPR hurdle in the trade direction. Guards against firing while
     * NIFTY's <i>current</i> 5-min is just now crossing a daily resistance/support — wait for
     * confirmation that the prior bar already closed past the level.
     *
     * <p>Daily levels considered:
     * <ul>
     *   <li>Buys: {BC, Pivot, TC, R1, R2, R3, R4} → highest level strictly below NIFTY LTP
     *       (the most recent resistance NIFTY has cleared).</li>
     *   <li>Sells: {TC, Pivot, BC, S1, S2, S3, S4} → lowest level strictly above NIFTY LTP.</li>
     * </ul>
     *
     * <p>Listener-ordering safe — the prior NIFTY 5-min has {@code startMinute ==
     * stockBucketStartMinute - 5}. We match on {@code startMinute} regardless of whether
     * NIFTY's onCandleClose listener has fired yet for the current boundary:
     * <ol>
     *   <li>If NIFTY's listener fired first, the just-closed bar is the last completed and the
     *       prior we want is the second-to-last → {@link CandleAggregator#getPreviousCandle}.</li>
     *   <li>If NIFTY's listener hasn't fired, the just-closed bar is still in {@code currentCandle}
     *       and the prior we want is the most recent completed → {@link CandleAggregator#getLastCompletedCandle}.</li>
     * </ol>
     *
     * <p>Returns null on pass (filter off, no hurdle in trade direction, prior 5m cleared, or no
     * prior 5m available — first bar of session, fail-open). Returns a non-null reason string
     * to reject.
     */
    private String checkNifty5mHurdle(boolean isBuy, long stockBucketStartMinute) {
        if (!riskSettings.isEnableNifty5mHurdleFilter()) return null;
        if (candleAggregator == null || marketDataService == null || bhavcopyService == null) return null;

        String niftySym = IndexTrendService.NIFTY_SYMBOL;
        double niftyLtp = marketDataService.getLtp(niftySym);
        if (niftyLtp <= 0) return null; // no LTP — fail-open

        var cpr = bhavcopyService.getCprLevels("NIFTY50");
        if (cpr == null) return null; // daily CPR not loaded — fail-open

        // Daily-CPR candidate set — inner zone (BC/Pivot/TC) + R1/PDH (buy) or S1/PDL (sell).
        // Extended levels (R2/R3/R4, S2/S3/S4) are intentionally excluded: the 5m filter is
        // about confirming inner-zone clearance, not chasing far-out projections. Virgin CPR
        // (TC/Pivot/BC) is appended when an active record is in cache.
        java.util.List<Double> candidateList = new java.util.ArrayList<>();
        java.util.List<String> nameList = new java.util.ArrayList<>();
        if (isBuy) {
            candidateList.add(cpr.getBc());    nameList.add("daily BC");
            candidateList.add(cpr.getPivot()); nameList.add("daily Pivot");
            candidateList.add(cpr.getTc());    nameList.add("daily TC");
            candidateList.add(cpr.getR1());    nameList.add("R1");
            candidateList.add(cpr.getPh());    nameList.add("PDH");
        } else {
            candidateList.add(cpr.getTc());    nameList.add("daily TC");
            candidateList.add(cpr.getPivot()); nameList.add("daily Pivot");
            candidateList.add(cpr.getBc());    nameList.add("daily BC");
            candidateList.add(cpr.getS1());    nameList.add("S1");
            candidateList.add(cpr.getPl());    nameList.add("PDL");
        }
        // Virgin CPR is now handled by the dedicated checkNiftyVirginCprHurdle filter — it's a
        // zone, not a level, so flat-list integration here doesn't model "trade rejected when
        // NIFTY is inside the zone" correctly. See checkNiftyVirginCprHurdle below.
        double[] candidates = new double[candidateList.size()];
        String[] names      = new String[nameList.size()];
        for (int i = 0; i < candidateList.size(); i++) {
            candidates[i] = candidateList.get(i);
            names[i]      = nameList.get(i);
        }

        // Nearest hurdle in trade direction: highest below LTP for buys, lowest above for sells.
        double chosenLevel = 0;
        String chosenName = null;
        for (int i = 0; i < candidates.length; i++) {
            double lv = candidates[i];
            if (lv <= 0) continue;
            if (isBuy) {
                if (lv < niftyLtp && lv > chosenLevel) { chosenLevel = lv; chosenName = names[i]; }
            } else {
                if (lv > niftyLtp && (chosenName == null || lv < chosenLevel)) {
                    chosenLevel = lv; chosenName = names[i];
                }
            }
        }
        if (chosenName == null) return null; // no hurdle in trade direction → clear path

        // Resolve NIFTY's prior 5-min close — the bar at startMinute = stockBucket - 5.
        long priorStartMinute = stockBucketStartMinute - 5;
        CandleAggregator.CandleBar priorBar = null;
        CandleAggregator.CandleBar prev = candleAggregator.getPreviousCandle(niftySym);
        if (prev != null && prev.startMinute == priorStartMinute && prev.close > 0) {
            priorBar = prev;
        } else {
            CandleAggregator.CandleBar last = candleAggregator.getLastCompletedCandle(niftySym);
            if (last != null && last.startMinute == priorStartMinute && last.close > 0) {
                priorBar = last;
            }
        }
        if (priorBar == null) return null; // first 5-min of session — fail-open

        double priorClose = priorBar.close;
        boolean cleared = isBuy ? priorClose > chosenLevel : priorClose < chosenLevel;
        if (!cleared) {
            return "NIFTY 5m hurdle at " + chosenName
                + ": NIFTY " + String.format("%.2f", niftyLtp)
                + ", prior 5m close " + String.format("%.2f", priorClose)
                + ", level " + String.format("%.2f", chosenLevel);
        }

        // Headroom check — mirror of the NIFTY HTF Hurdle headroom logic, applied to the
        // 5m filter's daily-CPR candidate set. Reject if the nearest hurdle in the OPPOSITE
        // direction is closer than minHeadroomAtr × NIFTY ATR.
        double minHeadroomAtr = riskSettings.getNifty5mHurdleMinHeadroomAtr();
        if (minHeadroomAtr > 0 && atrService != null) {
            double niftyAtr = atrService.getAtr(niftySym);
            if (niftyAtr > 0) {
                double minHeadroomPts = minHeadroomAtr * niftyAtr;
                double upcomingLevel = 0;
                String upcomingName = null;
                for (int i = 0; i < candidates.length; i++) {
                    double lv = candidates[i];
                    if (lv <= 0) continue;
                    if (isBuy) {
                        // For buys, upcoming hurdle = lowest level above LTP
                        if (lv > niftyLtp && (upcomingName == null || lv < upcomingLevel)) {
                            upcomingLevel = lv; upcomingName = names[i];
                        }
                    } else {
                        // For sells, upcoming hurdle = highest level below LTP
                        if (lv < niftyLtp && lv > upcomingLevel) {
                            upcomingLevel = lv; upcomingName = names[i];
                        }
                    }
                }
                if (upcomingName != null) {
                    double headroomPts = isBuy ? upcomingLevel - niftyLtp : niftyLtp - upcomingLevel;
                    if (headroomPts < minHeadroomPts) {
                        return "NIFTY 5m hurdle ahead at " + upcomingName
                            + " (" + String.format("%.2f", upcomingLevel) + "): only "
                            + String.format("%.2f", headroomPts) + " pts headroom, need "
                            + String.format("%.2f", minHeadroomPts)
                            + " (" + minHeadroomAtr + " × NIFTY ATR " + String.format("%.2f", niftyAtr) + ")";
                    }
                }
            }
        }
        return null;
    }

    /**
     * NIFTY Virgin CPR Hurdle filter — treats the active virgin CPR as a zone (BC..TC).
     * <ul>
     *   <li><b>Inside zone</b> — NIFTY's prior 5m close lands within [BC, TC]: reject every
     *       stock signal regardless of direction. Price is consolidating in the zone, neither
     *       direction has clarity.</li>
     *   <li><b>Headroom check</b> — close outside the zone but within
     *       {@code virginCprHurdleHeadroomAtr × NIFTY ATR} of the zone edge in trade direction:
     *       reject. Mirrors the 5m hurdle's headroom logic.
     *     <ul>
     *       <li>Buy: zone above NIFTY (close &lt; BC) and (BC − close) &lt; headroom × ATR → reject.</li>
     *       <li>Sell: zone below NIFTY (close &gt; TC) and (close − TC) &lt; headroom × ATR → reject.</li>
     *     </ul>
     *   </li>
     * </ul>
     *
     * <p>Returns null on pass (filter off, no active virgin CPR, no prior 5m close, or NIFTY far
     * enough from the zone). Returns a non-null reason string to reject. Bucket: {@code VIRGIN_CPR_HURDLE}.
     */
    private String checkNiftyVirginCprHurdle(boolean isBuy, long stockBucketStartMinute) {
        if (!riskSettings.isEnableVirginCprHurdleFilter()) return null;
        if (virginCprService == null || candleAggregator == null) return null;

        VirginCprService.Snapshot vc = virginCprService.getActiveVirginCpr();
        if (vc == null || vc.tc <= 0 || vc.bc <= 0) return null;

        String niftySym = IndexTrendService.NIFTY_SYMBOL;
        // Resolve NIFTY's prior 5m close — same listener-order-safe pattern as checkNifty5mHurdle.
        long priorStartMinute = stockBucketStartMinute - 5;
        CandleAggregator.CandleBar priorBar = null;
        CandleAggregator.CandleBar prev = candleAggregator.getPreviousCandle(niftySym);
        if (prev != null && prev.startMinute == priorStartMinute && prev.close > 0) {
            priorBar = prev;
        } else {
            CandleAggregator.CandleBar last = candleAggregator.getLastCompletedCandle(niftySym);
            if (last != null && last.startMinute == priorStartMinute && last.close > 0) {
                priorBar = last;
            }
        }
        if (priorBar == null) return null; // first 5m of session — fail-open

        double priorClose = priorBar.close;
        double zoneTop = Math.max(vc.tc, vc.bc);
        double zoneBot = Math.min(vc.tc, vc.bc);

        // 1) Inside-zone rejection — both directions blocked.
        if (priorClose >= zoneBot && priorClose <= zoneTop) {
            return "NIFTY (" + String.format("%.2f", priorClose)
                + ") inside virgin CPR zone (" + String.format("%.2f", zoneBot)
                + "—" + String.format("%.2f", zoneTop) + ")";
        }

        // 2) Headroom check — directional, only when zone is in trade direction.
        double headroomAtr = riskSettings.getVirginCprHurdleHeadroomAtr();
        if (headroomAtr <= 0 || atrService == null) return null;
        double atr = atrService.getAtr(niftySym);
        if (atr <= 0) return null;
        double minHeadroomPts = headroomAtr * atr;

        if (isBuy && priorClose < zoneBot) {
            double dist = zoneBot - priorClose;
            if (dist < minHeadroomPts) {
                return "Virgin CPR zone (" + String.format("%.2f", zoneBot)
                    + "—" + String.format("%.2f", zoneTop) + ") only "
                    + String.format("%.2f", dist) + " pts above NIFTY (need "
                    + String.format("%.2f", minHeadroomPts)
                    + ", " + headroomAtr + " × NIFTY ATR " + String.format("%.2f", atr) + ")";
            }
        } else if (!isBuy && priorClose > zoneTop) {
            double dist = priorClose - zoneTop;
            if (dist < minHeadroomPts) {
                return "Virgin CPR zone (" + String.format("%.2f", zoneBot)
                    + "—" + String.format("%.2f", zoneTop) + ") only "
                    + String.format("%.2f", dist) + " pts below NIFTY (need "
                    + String.format("%.2f", minHeadroomPts)
                    + ", " + headroomAtr + " × NIFTY ATR " + String.format("%.2f", atr) + ")";
            }
        }

        return null;
    }

    /**
     * Macro-level NIFTY direction filter — requires NIFTY's last <b>completed 15-min</b>
     * candle body to agree with the trade direction. Buys reject when NIFTY's last 15-min
     * bar is strictly red; sells reject when it's strictly green. Doji passes both.
     *
     * <p>Also flags <b>rejection candles</b> — even a correct-color candle is rejected when
     * its directional wick exceeds <code>niftyHtfCandleMaxWickRatio</code> &times; body. For
     * a buy, a long upper wick on a green 15-min bar means buyers couldn't hold the highs
     * (selling into strength) — bad context for a stock breakout. Mirror for sells (long
     * lower wick on a red bar = bottom rejection). Doji bodies skip the wick-ratio check.
     *
     * <p>The 15-min bar is synthesized from three consecutive 5-min bars on the existing
     * 5-min grid (see {@link CandleAggregator#getLastCompleted15MinCandle}). Filter
     * fail-opens before 9:30 IST (no completed 15-min window exists yet today) or when
     * fewer than 3 completed 5-min bars are available for NIFTY.
     */
    private String checkNiftyHtfCandleColor(boolean isBuy) {
        if (!riskSettings.isEnableNiftyHtfCandleFilter()) return null;
        if (candleAggregator == null) return null;
        CandleAggregator.CandleBar bar = candleAggregator
            .getLastCompleted15MinCandle(IndexTrendService.NIFTY_SYMBOL);
        if (bar == null || bar.open <= 0 || bar.close <= 0) return null; // fail-open

        // 1) Body color check.
        if (isBuy && bar.close < bar.open) {
            return "NIFTY 15m candle red — buy requires green: NIFTY 15m open="
                + String.format("%.2f", bar.open) + ", close=" + String.format("%.2f", bar.close);
        }
        if (!isBuy && bar.close > bar.open) {
            return "NIFTY 15m candle green — sell requires red: NIFTY 15m open="
                + String.format("%.2f", bar.open) + ", close=" + String.format("%.2f", bar.close);
        }

        // 2) Wick rejection check. For a green/buy candle, the upper wick (high - close) is the
        // concerning side — selling pressure capped the move. For a red/sell candle, the lower
        // wick (close - low) is concerning — buying pressure floored the move.
        double maxWickRatio = riskSettings.getNiftyHtfCandleMaxWickRatio();
        if (maxWickRatio > 0 && bar.high > 0 && bar.low > 0) {
            double body, wick;
            String wickName;
            if (isBuy) {
                body = bar.close - bar.open;          // green body
                wick = bar.high - bar.close;          // upper wick
                wickName = "upper";
            } else {
                body = bar.open - bar.close;          // red body
                wick = bar.close - bar.low;           // lower wick
                wickName = "lower";
            }
            if (body > 0 && wick > maxWickRatio * body) {
                return "NIFTY 15m " + wickName + " wick rejection: wick="
                    + String.format("%.2f", wick) + " > " + maxWickRatio + " × body="
                    + String.format("%.2f", body) + " (open=" + String.format("%.2f", bar.open)
                    + ", high=" + String.format("%.2f", bar.high)
                    + ", low=" + String.format("%.2f", bar.low)
                    + ", close=" + String.format("%.2f", bar.close) + ")";
            }
        }
        return null;
    }

    /**
     * Counter-trend setup family — magnets (S1+PDL / R1+PDH) and deep mean-rev (S2/S3/S4 buys,
     * R2/R3/R4 sells). Gated by the <code>enableMeanReversionTrades</code> master toggle and
     * bypass the LTF gate at probability assignment.
     */
    private static boolean isMeanReversionOrMagnet(String setup, double close, double tc, double bc) {
        if (setup == null) return false;
        // Static counter-trend list — magnets (S1+PDL / R1+PDH) and deep mean-rev (S2-S4 buys,
        // R2-R4 sells). DH/DL and 20-SMA setups are removed on this branch.
        return "BUY_ABOVE_S1_PDL".equals(setup)
            || "BUY_ABOVE_S2".equals(setup)
            || "BUY_ABOVE_S3".equals(setup)
            || "BUY_ABOVE_S4".equals(setup)
            || "SELL_BELOW_R1_PDH".equals(setup)
            || "SELL_BELOW_R2".equals(setup)
            || "SELL_BELOW_R3".equals(setup)
            || "SELL_BELOW_R4".equals(setup);
    }

    private boolean isProbabilityEnabled(String prob) {
        if (prob == null) return false;
        return switch (prob) {
            case "HPT" -> riskSettings.isEnableHpt();
            case "MPT" -> riskSettings.isEnableMpt();
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

        // A zone counts as "between" SMA and broken only if the SMA-to-broken path FULLY
        // crosses through it — i.e., ALL of its valid edges sit inside the interval. For a
        // single-edge level (R2/R3/R4/S2/S3/S4/DH/DL) that's the one edge. For a two-edge
        // zone (R1+PDH, CPR, S1+PDL) it requires BOTH edges. If only one edge is in the
        // interval, the SMA is sitting INSIDE the zone (or straddling its boundary), which
        // means the zone is at the SMA's level — not a wall between them.
        int count = 0;
        StringBuilder between = new StringBuilder();
        for (int i = 0; i < zoneNames.length; i++) {
            if (zoneNames[i].equals(excludedZone)) continue;
            int validEdges = 0;
            int edgesInInterval = 0;
            for (double e : zoneEdges[i]) {
                if (e <= 0) continue;
                validEdges++;
                if (e > lo && e < hi) edgesInInterval++;
            }
            boolean fullyBetween = validEdges > 0 && edgesInInterval == validEdges;
            if (fullyBetween) {
                count++;
                if (between.length() > 0) between.append(", ");
                between.append(zoneNames[i]);
            }
        }

        if (count > 0) {
            String detail = "SMA(" + String.format("%.2f", sma) + ") is "
                + count + " zone(s) away from broken " + String.format("%.2f", broken)
                + " [zones between: " + between + "]";
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup + " — skipped, " + detail);
            recordRejection(fyersSymbol, setup, close, "SMA_20_DISTANCE", detail);
            return 2;
        }

        // Secondary proximity check: SMA must be within (100 - smaLevelMinRangePct)% of the
        // range between the NEAR edge of the broken zone and the nearest zone edge on the
        // other side. Anchoring at the near edge (bottom of zone for buy, top for sell) means
        // the zone's own width doesn't get counted as proximity distance — wider zones aren't
        // double-penalised. Single-edge levels (R2/R3/R4 etc.) keep nearEdge == broken.
        int minRangePct = riskSettings.getSmaLevelMinRangePct();
        if (minRangePct > 0) {
            boolean isBuy = setup.startsWith("BUY_");

            // Near edge of the broken zone — the inner edge facing the SMA.
            double nearEdge = broken;
            for (int i = 0; i < zoneNames.length; i++) {
                if (!zoneNames[i].equals(excludedZone)) continue;
                if (zoneEdges[i].length == 2) {
                    double e1 = zoneEdges[i][0], e2 = zoneEdges[i][1];
                    nearEdge = isBuy ? Math.min(e1, e2) : Math.max(e1, e2);
                }
                break;
            }

            // Nearest zone edge on the other side of the broken zone, skipping the broken zone.
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
                double range = Math.abs(nearEdge - boundaryEdge);
                double maxDist = range * (100 - minRangePct) / 100.0;
                // Directional subtraction + clamp at 0: SMA inside or past the broken zone
                // (at/above nearEdge for buys, at/below for sells) is "0% distance" — the
                // breakout has already swept through it.
                double rawDist = isBuy ? (nearEdge - sma) : (sma - nearEdge);
                double actualDist = Math.max(0, rawDist);
                if (actualDist > maxDist) {
                    int actualPct = (int) Math.round(actualDist / range * 100.0);
                    String detail = "SMA(" + String.format("%.2f", sma)
                        + ") too far from broken zone — must sit within "
                        + (100 - minRangePct) + "% of range from "
                        + (isBuy ? "bottom" : "top") + " of broken zone"
                        + " (zone edge " + String.format("%.2f", nearEdge)
                        + " → boundary " + String.format("%.2f", boundaryEdge)
                        + " = range " + String.format("%.2f", range)
                        + "; dist " + String.format("%.2f", actualDist)
                        + " = " + actualPct + "% > " + (100 - minRangePct) + "%)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + setup + " — skipped, " + detail);
                    recordRejection(fyersSymbol, setup, close, "LEVEL_PROXIMITY", detail);
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

    /**
     * Recompute the active armed buy / sell setup for this symbol from the current close.
     * Single armed level per direction, rolling with price:
     * <ul>
     *   <li>Armed buy = the highest CPR-level setup whose level &lt; close (closest broken-above
     *       level — the one the next dip would naturally retest).</li>
     *   <li>Armed sell = the lowest CPR-level setup whose level &gt; close (closest
     *       broken-below-from-above level — the one the next pop would retest).</li>
     * </ul>
     * If no level qualifies (e.g., close above R4 — nothing higher to retest from), the
     * corresponding side is cleared. Recomputed on every candle close so the armed level
     * tracks where price actually is, not stale historical breaks.
     */
    private void armLevelsForCandle(String fyersSymbol, double close, CprLevels levels) {
        if (close <= 0 || levels == null) return;

        double r4 = levels.getR4(), r3 = levels.getR3(), r2 = levels.getR2();
        double r1 = levels.getR1(), ph = levels.getPh();
        double tc = levels.getTc(), bc = levels.getBc();
        double s1 = levels.getS1(), pl = levels.getPl();
        double s2 = levels.getS2(), s3 = levels.getS3(), s4 = levels.getS4();

        double cprTop = Math.max(tc, bc);
        double cprBot = Math.min(tc, bc);
        double r1ph   = Math.max(r1, ph);    // upper edge of R1+PDH zone (used for buy retest)
        double r1phLo = Math.min(r1, ph);    // lower edge (used for sell retest)
        double s1pl   = Math.max(s1, pl);    // upper edge of S1+PDL zone (used for buy retest)
        double s1plLo = Math.min(s1, pl);    // lower edge (used for sell retest)

        // Buy-side: scan from highest level to lowest; first one strictly below close wins.
        // Order matches the priority loop in detectBuyBreakout.
        String buy = null;
        if      (r4 > 0    && close > r4)    buy = "BUY_ABOVE_R4";
        else if (r3 > 0    && close > r3)    buy = "BUY_ABOVE_R3";
        else if (r2 > 0    && close > r2)    buy = "BUY_ABOVE_R2";
        else if (r1ph > 0  && close > r1ph)  buy = "BUY_ABOVE_R1_PDH";
        else if (cprTop > 0 && close > cprTop) buy = "BUY_ABOVE_CPR";
        else if (s1pl > 0  && close > s1pl)  buy = "BUY_ABOVE_S1_PDL";
        else if (s2 > 0    && close > s2)    buy = "BUY_ABOVE_S2";
        else if (s3 > 0    && close > s3)    buy = "BUY_ABOVE_S3";
        else if (s4 > 0    && close > s4)    buy = "BUY_ABOVE_S4";

        // Sell-side: scan from lowest level to highest; first one strictly above close wins.
        String sell = null;
        if      (s4 > 0    && close < s4)    sell = "SELL_BELOW_S4";
        else if (s3 > 0    && close < s3)    sell = "SELL_BELOW_S3";
        else if (s2 > 0    && close < s2)    sell = "SELL_BELOW_S2";
        else if (s1plLo > 0 && close < s1plLo) sell = "SELL_BELOW_S1_PDL";
        else if (cprBot > 0 && close < cprBot) sell = "SELL_BELOW_CPR";
        else if (r1phLo > 0 && close < r1phLo) sell = "SELL_BELOW_R1_PDH";
        else if (r2 > 0    && close < r2)    sell = "SELL_BELOW_R2";
        else if (r3 > 0    && close < r3)    sell = "SELL_BELOW_R3";
        else if (r4 > 0    && close < r4)    sell = "SELL_BELOW_R4";

        if (buy != null)  armedBuyLevel.put(fyersSymbol, buy);
        else              armedBuyLevel.remove(fyersSymbol);
        if (sell != null) armedSellLevel.put(fyersSymbol, sell);
        else              armedSellLevel.remove(fyersSymbol);
    }

    /** Clear broken levels for a symbol when its position is closed. Allows re-entry.
     *  Armed levels (close-past history) persist across position-close events — they only
     *  reset at daily rollover via {@link #clearAll()}. */
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

    /** Read-only view of the per-symbol signal history map for the day. Used by the EOD-Analysis
     *  endpoint to enumerate symbols with signals + iterate the full audit trail. */
    public Map<String, List<SignalInfo>> getSignalHistoryAll() {
        return Collections.unmodifiableMap(signalHistory);
    }

    /** Clear all state for end of day. */
    public void clearAll() {
        brokenLevels.clear();
        armedBuyLevel.clear();
        armedSellLevel.clear();
        lastTriggerRoute.clear();
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
                sig.put("filterName", entry.getValue().filterName != null ? entry.getValue().filterName : "");
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
                    sig.put("filterName", si.filterName != null ? si.filterName : "");
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
                    info.filterName = v.has("filterName") ? v.get("filterName").asText() : "";
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
                        si.filterName = node.has("filterName") ? node.get("filterName").asText() : "";
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

    /** Build the HH:mm:00 time string for the currently-processing candle, mirroring the
     *  format fireSignal uses. Falls back to system clock if no candle context is set. */
    private String currentSignalTime() {
        CandleAggregator.CandleBar ctxCandle = currentCandle.get();
        if (ctxCandle != null && ctxCandle.startMinute > 0) {
            int closeMin = (int) (ctxCandle.startMinute + riskSettings.getScannerTimeframe());
            return String.format("%02d:%02d:00", closeMin / 60, closeMin % 60);
        }
        return ZonedDateTime.now(IST).toLocalTime().format(TIME_FMT);
    }

    /** Append a structured FILTERED entry to signalHistory + bump the filtered-counter. Used at
     *  every pre-fireSignal early-return so the EOD-Analysis page sees the full audit trail.
     *  The free-text {@code detail} mirrors what's already in the [SCANNER] event-log line. */
    private void recordRejection(String fyersSymbol, String setup, double price, String filterName, String detail) {
        SignalInfo info = new SignalInfo();
        info.setup = setup != null ? setup : "";
        info.time = currentSignalTime();
        info.status = "FILTERED";
        info.filterName = filterName;
        info.detail = detail;
        info.price = price;
        lastSignal.put(fyersSymbol, info);
        signalHistory.computeIfAbsent(fyersSymbol, k -> Collections.synchronizedList(new ArrayList<>())).add(info);
        filteredCountToday++;
        saveState();
    }

    /** Map TradingController/SignalProcessor response text into a stable {@code filterName}
     *  enum so the EOD-Analysis UI can group/chip-filter by reason. Returns "" for
     *  passes (TRADED) and "DOWNSTREAM" as a catch-all for rejections that don't match
     *  a known prefix. Order matters — the composite "downgraded to LPT (X)" wrapper is
     *  inspected first so the inner X surfaces as the real filter name. */
    private String classifyDownstreamRejection(String responseText, boolean filtered) {
        if (!filtered) return "";
        if (responseText == null) return "DOWNSTREAM";
        String s = responseText.toLowerCase();

        // Composite LPT-disabled rejection — drill into the parenthesised inner reason.
        // Order matters: more specific first (HTF SMA order > HTF SMA not aligned > HTF hurdle).
        if (s.contains("probability downgraded to lpt") || s.contains("→ lpt")) {
            if (s.contains("htf hurdle"))              return "HTF_HURDLE";
            if (s.contains("htf sma order"))           return "HTF_SMA_ALIGNMENT";
            if (s.contains("htf sma not aligned"))     return "HTF_SMA_TREND";
            if (s.contains("nifty opposed"))           return "NIFTY_OPPOSED";
            if (s.contains("inside-or"))               return "INSIDE_OR";
            if (s.contains("ev reversal"))             return "EV_REVERSAL";
            return "LPT_DISABLED";
        }

        // LTF/HTF probability gates (new under LTF-priority classification)
        if (s.contains("ltf opposed") || s.contains("requires close >") || s.contains("requires close <")) return "LTF_OPPOSED";
        if (s.contains("htf not aligned") || s.contains("magnet ") && s.contains("requires weekly")) return "HTF_NOT_ALIGNED";

        // Order-layer gates
        if (s.contains("outside trading hours"))                  return "TRADING_HOURS";
        if (s.contains("risk exposure") || s.contains("daily loss")) return "RISK_LIMIT";
        if (s.contains("kill switch"))                             return "KILL_SWITCH";

        // Structural / setup-level gates
        // HTF extended-level (weekly R3/R4 / S3/S4) bucketed separately from the daily
        // R3/S3 / R4/S4 skip — both are independent toggles in Settings, so they should be
        // chip-filterable independently in the Signal Trail. Order matters: HTF check first
        // (its message is "HTF extended-level…" so the daily check would also match it).
        if (s.contains("htf extended-level"))                      return "EXTENDED_LEVEL_HTF";
        if (s.contains("extended-level"))                          return "EXTENDED_LEVEL_DAILY";
        if (s.contains("is inside") && s.contains("zone"))         return "DH_DL_ZONE";
        if (s.contains("inside cpr") || s.contains("dh/dl"))       return "DH_DL_ZONE";
        if (s.contains("invalid atr"))                             return "INVALID_ATR";
        if (s.contains("wrong side of entry"))                     return "WRONG_SIDE_TARGET";

        // Candle-shape / volume gates
        if (s.contains("small candle"))                            return "SMALL_CANDLE";
        if (s.contains("opposite wick pressure"))                  return "OPPOSITE_WICK";
        if (s.contains("large candle body"))                       return "LARGE_CANDLE";
        if (s.contains("low volume"))                              return "LOW_VOLUME";

        // EV / OR gates
        if (s.contains("mean-reversion setup only allowed"))       return "MEAN_REVERSION_DAY";
        if (s.contains("opposes or break"))                        return "EV_OR_OPPOSED";
        if (s.contains("inside or range"))                         return "EV_OR_INSIDE";
        if (s.contains("ev ") && s.contains("detected"))           return "EV_GAP_OPPOSED";

        // Risk / reward / profit
        if (s.contains("risk/reward") || s.contains("risk\\reward")) return "RISK_REWARD";
        if (s.contains("absolute profit too low"))                 return "MIN_PROFIT";

        // Order placement
        if (s.contains("order failed") || s.contains("rejected by broker")) return "ORDER_FAILED";

        return "DOWNSTREAM";
    }

    // ── Signal info for dashboard ────────────────────────────────────────────

    public static class SignalInfo {
        public String setup;
        public String time;
        public String status; // TRADED, FILTERED, ERROR
        public String detail;
        public double price;  // candle close at signal time
        public String filterName; // "" for TRADED; stable enum for FILTERED (drives EOD-Analysis grouping)
    }
}
