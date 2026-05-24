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
    private final EmaService emaService;
    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private HtfEmaService htfEmaService;

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
    private MarketHolidayService marketHolidayService;

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

    /** Per-symbol, per-level "broken-state" for all 9 levels (R4/R3/R2/R1+PDH/CPR/S1+PDL/
     *  S2/S3/S4). Updated incrementally on each candle close. For two-line zones (CPR,
     *  R1+PDH, S1+PDL) a close above the UPPER edge sets UP and clears DOWN; a close below
     *  the LOWER edge sets DOWN and clears UP; a close inside the zone leaves both flags
     *  unchanged (deeper retests don't invalidate the prior breakout). For single-line
     *  levels (R2/R3/R4, S2/S3/S4) upper == lower == line, so a close on either side flips
     *  the state directly with no in-between zone. Read by {@link #isRetestArmed} from
     *  the retest gates. Reset on daily rollover via {@link #clearAll()}. */
    static final class LevelBrokenState {
        boolean up, down;
    }
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, LevelBrokenState>> levelStateBySymbol = new ConcurrentHashMap<>();
    /** Epoch (seconds) of the most-recently-applied prior candle per symbol. Used by
     *  {@link #advanceZoneState} to skip bars already folded into the state on subsequent
     *  scans. 0 = nothing applied yet (fresh session / restart). */
    private final ConcurrentHashMap<String, Long> lastZoneAppliedEpoch = new ConcurrentHashMap<>();
    /** Trigger-route tag for the most recent fired signal — picked up by fireSignal so the
     *  trade-log description records which path entered. Pure retest model: every pattern
     *  requires (a) prior level break captured in the level-broken state and (b) some bar
     *  in the pattern touching the level. Tags: "HAMMER_RETEST", "OUTSIDE_REVERSAL_RETEST",
     *  "DOJI_RETEST", "STAR_RETEST", "HARAMI_RETEST", "GOOD_SIZE_CANDLE_RETEST". Per-symbol,
     *  accessed only on the candle-close thread for that symbol. */
    private final ConcurrentHashMap<String, String> lastTriggerRoute = new ConcurrentHashMap<>();
    /** Pending Index HTF Hurdle break-guard per stock symbol. Set by
     *  {@link #checkPrimaryIndexHtfHurdle} when the filter passes AND a hurdle existed in
     *  trade direction (i.e., the primary index's gating 15-min close cleared an actual
     *  level). Consumed by the entry-fill handler which persists the guard onto the
     *  {@code PositionEntity}. Cleared at day reset. */
    private final ConcurrentHashMap<String, NiftyHurdleGuard> pendingHurdleGuards = new ConcurrentHashMap<>();

    /** Captured 15-min confirmation-bar level for the NIFTY HTF Hurdle break-guard. {@code low}
     *  is defended for buys; {@code high} for sells. */
    public record NiftyHurdleGuard(double low, double high, boolean isBuy) {}

    // Track signals generated today (for scanner dashboard)
    private final ConcurrentHashMap<String, SignalInfo> lastSignal = new ConcurrentHashMap<>();

    // Signal history for the day (all traded + filtered signals per symbol)
    private final ConcurrentHashMap<String, List<SignalInfo>> signalHistory = new ConcurrentHashMap<>();

    // Watchlist symbols (set by MarketDataService)
    private volatile List<String> watchlistSymbols = Collections.emptyList();

    // Last scan cycle stats — atomic so multi-threaded scanning maintains accurate counts
    private final java.util.concurrent.atomic.AtomicInteger lastScanCount = new java.util.concurrent.atomic.AtomicInteger();
    private volatile String lastScanTime = "";
    private final java.util.concurrent.atomic.AtomicLong lastScanBoundary = new java.util.concurrent.atomic.AtomicLong();
    private volatile int tradedCountToday = 0;
    private volatile int filteredCountToday = 0;

    // Worker pool for per-symbol scanning. All scans happen off the CandleAggregator thread so
    // (a) one slow symbol can't block the others and (b) the aggregator thread stays free to
    // process incoming ticks while scans are in flight. Sized for the typical NIFTY-50 watchlist;
    // newCachedThreadPool would create more under load but a fixed pool gives bounded resource use.
    private final java.util.concurrent.ExecutorService scanExecutor =
        java.util.concurrent.Executors.newFixedThreadPool(12, r -> {
            Thread t = new Thread(r, "scanner-worker");
            t.setDaemon(true);
            return t;
        });

    public BreakoutScanner(BhavcopyService bhavcopyService,
                           AtrService atrService,
                           WeeklyCprService weeklyCprService,
                           CandleAggregator candleAggregator,
                           RiskSettingsStore riskSettings,
                           EventService eventService,
                           LatencyTracker latencyTracker,
                           EmaService emaService) {
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.candleAggregator = candleAggregator;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.latencyTracker = latencyTracker;
        this.emaService = emaService;
        loadState();
        backfillFromEventLog();
    }

    /**
     * One-shot recovery for today's pre-recordRejection-shipment rejections. Pre-fireSignal
     * filters (EMA trend, level-count, level-proximity, NIFTY hard-skip, position-open,
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
                    if (restLower.contains("blocked by 5-min ema trend")
                            || restLower.contains("blocked by 5-min sma trend")) {
                        // Strip the "blocked by 5-min EMA trend — " prefix to keep detail concise.
                        // Also accepts the legacy "SMA trend" wording so today's pre-rename log
                        // lines still get reclassified on the EOD audit.
                        String dash = " — ";
                        int idx = rest.indexOf(dash);
                        detail = idx >= 0 ? rest.substring(idx + dash.length()) : rest;
                        filterName = restLower.contains("not aligned") ? "EMA_ALIGNMENT" : "EMA_TREND";
                    } else if (restLower.contains("zone(s) away from broken")) {
                        filterName = "EMA_20_DISTANCE";
                        detail = stripSkippedPrefix(rest);
                    } else if (restLower.contains("too far from broken zone")) {
                        filterName = "LEVEL_PROXIMITY";
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

        // Defence-in-depth: skip the scan entirely on non-trading days (NSE holiday / weekend).
        // SignalProcessor also blocks at signal entry, but suppressing here avoids event-log
        // noise from mock-session ticks that build 5-min bars on Saturdays.
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;

        // Check if stock passes the CPR Width Scanner settings (NS/NL/IS/IL)
        if (!isBreakoutEligible(fyersSymbol)) return;

        // Skip candles that started before market open
        if (completedCandle.startMinute < MarketHolidayService.MARKET_OPEN_MINUTE) return;

        // Gate the breakout scan on the user's configured trading window. Suppresses both
        // the filter-rejection log noise AND prevents premature brokenLevels marking that
        // would silence a legitimate post-start-time fire on the same level.
        if (!isWithinTradingWindow()) return;

        // Race-fix: force-finalize NIFTY's and the stock's primary-index same-bucket bar
        // BEFORE reading any trend state. Without this, a stock tick arriving before
        // NIFTY's tick in the same bucket would have the scanner reading stale prior-bucket
        // index state. The forceFinalize is idempotent — if NIFTY's bar already finalized,
        // it's a no-op.
        if (candleAggregator != null) {
            candleAggregator.forceFinalizeBucket(IndexTrendService.NIFTY_SYMBOL, completedCandle.startMinute);
            if (bhavcopyService != null) {
                String stockTicker = extractTicker(fyersSymbol);
                String primaryIndex = bhavcopyService.getPrimaryIndexTicker(stockTicker);
                if (primaryIndex != null && !"NIFTY50".equals(primaryIndex)) {
                    candleAggregator.forceFinalizeBucket(
                        "NSE:" + primaryIndex + "-INDEX", completedCandle.startMinute);
                }
            }
        }

        // Track scan cycle — reset counter when boundary changes. Atomic compare-and-set so
        // the first scan into a new boundary resets the count cleanly even with parallel
        // workers racing in.
        long prevBoundary = lastScanBoundary.get();
        if (completedCandle.startMinute != prevBoundary) {
            if (lastScanBoundary.compareAndSet(prevBoundary, completedCandle.startMinute)) {
                lastScanCount.set(0);
            }
        }
        lastScanCount.incrementAndGet();
        lastScanTime = ZonedDateTime.now(IST).toLocalTime().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm"));

        // Dispatch the per-symbol scan to the worker pool so all symbols at this boundary
        // can scan in parallel instead of serialized on the CandleAggregator scheduler thread.
        scanExecutor.submit(() -> {
            try {
                log.info("[Scanner] Candle close: {} start={} O={} H={} L={} C={}",
                    fyersSymbol, completedCandle.startMinute,
                    String.format("%.2f", completedCandle.open), String.format("%.2f", completedCandle.high),
                    String.format("%.2f", completedCandle.low), String.format("%.2f", completedCandle.close));
                scanForBreakout(fyersSymbol, completedCandle);
            } catch (Exception e) {
                log.error("[Scanner] Error scanning {}: {}", fyersSymbol, e.getMessage());
            }
        });
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

        // Fold any prior bars not yet applied into the zone-broken state, then detection
        // reads state-up-to-PREV. Current bar's close is never applied here — it becomes
        // the "prev" on the NEXT scan and gets folded in at that point.
        advanceZoneState(fyersSymbol, levels);

        // Already in position for this symbol?
        String pos = PositionManager.getPosition(fyersSymbol);
        if (!"NONE".equals(pos)) {
            String detail = "position " + pos + " already open";
            eventService.log("[SCANNER] " + fyersSymbol + " — skipped, " + detail);
            recordRejection(fyersSymbol, "", close, "POSITION_OPEN", detail);
            return;
        }

        Set<String> broken = brokenLevels.getOrDefault(fyersSymbol, Collections.emptySet());

        double low = candle.low;
        double high = candle.high;

        // ── Stock 5m direction gate (top-level, hard-baked) ────────────────────
        // Replaces the old EMA Price Filter. Pattern detection only proceeds in the
        // directions the gate allows; per-setup verification happens after detection
        // since the gate distinguishes trend-following vs counter-trend.
        //
        //   • Trend-following BUY  → stock 5m state == BULLISH  (close > CPR top AND > EMA20)
        //   • Counter-trend  BUY   → shallow dip                (close < CPR bot AND > EMA20)
        //   • Trend-following SELL → stock 5m state == BEARISH  (close < CPR bot AND < EMA20)
        //   • Counter-trend  SELL  → shallow rally              (close > CPR top AND < EMA20)
        //
        // If neither TF nor CT path qualifies in a direction, we skip detection in that
        // direction entirely — no noisy "blocked by EMA" log on candles that were never
        // going to trade.
        double ema20Now = emaService.getEma(fyersSymbol);
        double gateCprTop = Math.max(levels.getTc(), levels.getBc());
        double gateCprBot = Math.min(levels.getTc(), levels.getBc());
        boolean gateAboveCpr = close > gateCprTop;
        boolean gateBelowCpr = close < gateCprBot;
        boolean gateAboveEma = ema20Now > 0 && close > ema20Now;
        boolean gateBelowEma = ema20Now > 0 && close < ema20Now;
        boolean canFireTfBuy   = gateAboveCpr && gateAboveEma;   // strict BULLISH
        boolean canFireCtBuy   = gateBelowCpr && gateAboveEma;   // shallow dip
        boolean canFireTfSell  = gateBelowCpr && gateBelowEma;   // strict BEARISH
        boolean canFireCtSell  = gateAboveCpr && gateBelowEma;   // shallow rally
        boolean canFireAnyBuy  = canFireTfBuy  || canFireCtBuy;
        boolean canFireAnySell = canFireTfSell || canFireCtSell;

        // Check BUY signals — color-agnostic. Pin bar hammers can be red-bodied (the long
        // lower wick is the rejection, not the body color). The other buy patterns
        // (engulfing/doji/star/three-inside-up/good-size) all require a green close
        // internally, so they self-reject on red bars and only the hammer benefits.
        if (canFireAnyBuy) {
            String buySetup = detectBuyBreakout(open, high, low, close, levels, broken, fyersSymbol);
            if (buySetup != null) {
                // Per-setup direction gate — classify the matched setup and verify it
                // against the stock 5m state. TF setups need strict BULLISH state; CT
                // setups need shallow-dip (close < CPR bot AND close > EMA20).
                boolean isTfBuy = isTrendFollowingBuy(buySetup);
                if (isTfBuy && !canFireTfBuy) {
                    String detail = "trend-following buy needs strict BULLISH (close > CPR top "
                        + String.format("%.2f", gateCprTop) + " AND > EMA20 "
                        + String.format("%.2f", ema20Now) + "); close=" + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "STOCK_5M_TREND", detail);
                    return;
                }
                if (!isTfBuy && !canFireCtBuy) {
                    String detail = "counter-trend buy needs shallow dip (close < CPR bot "
                        + String.format("%.2f", gateCprBot) + " AND > EMA20 "
                        + String.format("%.2f", ema20Now) + "); close=" + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "STOCK_5M_TREND", detail);
                    return;
                }
                // Magnet gate — BUY_ABOVE_S1_PDL only.
                if (isMagnet(buySetup) && !riskSettings.isEnableMagnetTrades()) {
                    String detail = "magnet trades disabled (toggle off)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "MAGNET_DISABLED", detail);
                    return;
                }
                // Mean-reversion gate — deep fades from S2/S3/S4.
                if (isMeanReversion(buySetup) && !riskSettings.isEnableMeanReversionTrades()) {
                    String detail = "mean-reversion trades disabled (toggle off)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "MEAN_REVERSION_DISABLED", detail);
                    return;
                }
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, true, buySetup, close);
                if (prob == null) {
                    // LTF-priority gate rejected: standard buy with close ≤ daily CPR top (no LTF
                    // support). Magnets always fire (HTF/LTF bypassed) so they don't reach here.
                    // CPR can invert (BC > TC) — always reference the actual upper edge, not the
                    // TC label, so the message matches the real gate.
                    double cprTop = Math.max(levels.getTc(), levels.getBc());
                    String detail = "buy requires close > daily CPR top (" + String.format("%.2f", cprTop)
                        + ") (LTF bullish); close=" + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "LTF_OPPOSED", detail);
                    return;
                }
                if (!isProbabilityEnabled(prob)) {
                    // Probability tier toggled off in settings (e.g. enableMpt=false).
                    String detail = prob + " not enabled";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "PROB_DISABLED", detail);
                    return;
                }
                // Daily ATR exhaustion — reject when today's TR (gap-inclusive) has consumed
                // ≥ mult × the stock's 14-day daily ATR. Symmetric: both buys and sells gated.
                if (riskSettings.isEnableDailyAtrExhaustionFilter()) {
                    double dailyAtr = bhavcopyService.getDailyAtr(extractTicker(fyersSymbol));
                    double mult = riskSettings.getDailyAtrExhaustionMult();
                    if (dailyAtr > 0 && mult > 0) {
                        double dayHigh = candleAggregator.getDayHigh(fyersSymbol);
                        double dayLow  = candleAggregator.getDayLow(fyersSymbol);
                        double prevClose = levels.getClose();
                        if (dayHigh > 0 && dayLow > 0 && prevClose > 0) {
                            double todayTr = Math.max(dayHigh - dayLow,
                                Math.max(Math.abs(dayHigh - prevClose), Math.abs(dayLow - prevClose)));
                            if (todayTr >= mult * dailyAtr) {
                                String detail = String.format(
                                    "Today TR %.2f (gap-inclusive) >= %.2f × daily ATR %.2f — move exhausted",
                                    todayTr, mult, dailyAtr);
                                eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                                recordRejection(fyersSymbol, buySetup, close, "DAILY_ATR_EXHAUSTED", detail);
                                return;
                            }
                        }
                    }
                }
                // Open Range Filter — strict ORB. The OR window is [9:15, 9:15+orMins).
                // The bar that *closes* at orEndMinute is the LAST bar of the OR — when the
                // scanner fires on its close, the OR is FINALIZED (mirror of how the 5-min
                // EMA is stepped before BreakoutScanner sees it). So "still forming" means
                // the bar's CLOSE minute is strictly before orEndMinute.
                //
                // Counter-trend setups (magnet, mean-reversion) skip the OR gate — a stock
                // pulled back to deep support is typically below OR high, and applying the
                // OR filter would reject most counter-trend buys. The HTF/Index gates still
                // confirm the bigger-picture trend.
                if (riskSettings.isEnableOpenRangeFilter() && !isCounterTrend(buySetup)) {
                    int orMins = riskSettings.getOpenRangeMinutes();
                    long orEndMinute = MarketHolidayService.MARKET_OPEN_MINUTE + orMins;
                    long closeMinute = candle.startMinute + riskSettings.getScannerTimeframe();
                    if (closeMinute < orEndMinute) {
                        String detail = "OR not yet formed (first " + orMins + " minutes)";
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                        recordRejection(fyersSymbol, buySetup, close, "OPEN_RANGE_FORMING", detail);
                        return;
                    }
                    double orHigh = candleAggregator.getOpenRangeHigh(fyersSymbol, orMins);
                    double orLow  = candleAggregator.getOpenRangeLow(fyersSymbol, orMins);
                    if (orHigh > 0 && orLow > 0 && close <= orHigh) {
                        String detail = close < orLow
                            ? String.format("close %.2f below OR low %.2f — buy direction opposed", close, orLow)
                            : String.format("close %.2f inside OR [%.2f, %.2f]", close, orLow, orHigh);
                        eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                        recordRejection(fyersSymbol, buySetup, close, "OPEN_RANGE", detail);
                        return;
                    }
                }
                // Primary-Index Open Range Filter — same ORB rules applied to the stock's
                // primary index (NIFTY 50 OR the mapped sector index). Uses the index's own
                // OR + its latest 5-min close — if the index is inside / below its OR, no
                // buys on stocks mapped to that index. Counter-trend setups skip this gate
                // for the same reason as the stock-side OR — a pullback to deep support is
                // typically below OR high on both stock and index timeframes.
                if (riskSettings.isEnableIndexOpenRangeFilter() && !isCounterTrend(buySetup)) {
                    int orMins = riskSettings.getOpenRangeMinutes();
                    long orEndMinute = MarketHolidayService.MARKET_OPEN_MINUTE + orMins;
                    long closeMinute = candle.startMinute + riskSettings.getScannerTimeframe();
                    String stockTicker = extractTicker(fyersSymbol);
                    String primaryIndex = bhavcopyService.getPrimaryIndexTicker(stockTicker);
                    if (primaryIndex != null && !primaryIndex.isEmpty()) {
                        String indexSym = "NSE:" + primaryIndex + "-INDEX";
                        if (closeMinute < orEndMinute) {
                            String detail = "index " + primaryIndex + " OR not yet formed (first " + orMins + " minutes)";
                            eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                            recordRejection(fyersSymbol, buySetup, close, "INDEX_OPEN_RANGE_FORMING", detail);
                            return;
                        }
                        double idxOrHigh = candleAggregator.getOpenRangeHigh(indexSym, orMins);
                        double idxOrLow  = candleAggregator.getOpenRangeLow(indexSym, orMins);
                        CandleAggregator.CandleBar idxBar = candleAggregator.getLastCompletedCandle(indexSym);
                        double idxClose = idxBar != null ? idxBar.close : 0;
                        if (idxOrHigh > 0 && idxOrLow > 0 && idxClose > 0 && idxClose <= idxOrHigh) {
                            String detail = idxClose < idxOrLow
                                ? String.format("index %s close %.2f below OR low %.2f — buy direction opposed", primaryIndex, idxClose, idxOrLow)
                                : String.format("index %s close %.2f inside OR [%.2f, %.2f]", primaryIndex, idxClose, idxOrLow, idxOrHigh);
                            eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                            recordRejection(fyersSymbol, buySetup, close, "INDEX_OPEN_RANGE", detail);
                            return;
                        }
                    }
                }
                // EMA level-count filter: skip if any CPR zone sits between EMA and broken level
                if (evaluateEmaFilter(fyersSymbol, buySetup, close, levels, atr) == 2) return;
                // NIFTY index alignment filter — misaligned trades are hard-rejected. The
                // previous LPT downgrade path has been removed: a stock-trade direction that
                // opposes the NIFTY composite trend has no edge on its own.
                // Primary-index alignment — replaces the old NIFTY + sector double-check.
                // Each stock maps to one primary index (NIFTY 50 OR a sector index) via the
                // Stock Universe table; the alignment filter runs against that index only.
                if (checkIndexAlignment(fyersSymbol, buySetup, true) == NiftyAlignStatus.SKIP) {
                    String primary = bhavcopyService != null ? bhavcopyService.getPrimaryIndexTicker(extractTicker(fyersSymbol)) : "?";
                    String state = indexTrendService != null ? indexTrendService.getTrendStateForTicker(primary) : "?";
                    String detail = "Primary index " + primary + " " + state + " — buy direction opposes index trend";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "INDEX_OPPOSED", detail);
                    return;
                }
                // Stock HTF Trend Alignment — confirms the stock's own 1-hour HTF trend (weekly
                // CPR + 1-hour EMA20) agrees with the buy direction.
                if (checkStockHtfAlignment(fyersSymbol, buySetup, true) == NiftyAlignStatus.SKIP) {
                    double htfEma = htfEmaService != null ? htfEmaService.getEma(fyersSymbol) : 0;
                    String stockHtfState = weeklyCprService != null
                        ? weeklyCprService.getStockHtfTrendState(fyersSymbol, htfEma) : "?";
                    String detail = "Stock 1h " + stockHtfState + " — buy direction opposes stock HTF trend";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "STOCK_HTF_OPPOSED", detail);
                    return;
                }
                // Index HTF Trend Alignment — primary index's 1-hour state (weekly CPR + 1h EMA20)
                // must agree with the trade direction. Mirrors checkStockHtfAlignment on the index.
                if (checkIndexHtfAlignment(fyersSymbol, buySetup, true) == NiftyAlignStatus.SKIP) {
                    String primary = bhavcopyService != null ? bhavcopyService.getPrimaryIndexTicker(extractTicker(fyersSymbol)) : "?";
                    String idxHtfState = indexTrendService != null ? indexTrendService.getHtfTrendStateForTicker(primary) : "?";
                    String detail = "Index " + primary + " 1h " + idxHtfState + " — buy direction opposes index HTF trend";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, buySetup, close, "INDEX_HTF_OPPOSED", detail);
                    return;
                }
                // Index HTF Hurdle — stock's primary-index 1-hour close must clear its nearest weekly hurdle.
                String indexHtfReject = checkPrimaryIndexHtfHurdle(true, fyersSymbol);
                if (indexHtfReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + indexHtfReject);
                    recordRejection(fyersSymbol, buySetup, close, "INDEX_HTF_HURDLE", indexHtfReject);
                    return;
                }
                // Index 5m Hurdle — stock's primary-index 5-min close must clear nearest daily-CPR hurdle.
                String index5mReject = checkPrimaryIndex5mHurdle(true, candle.startMinute, fyersSymbol);
                if (index5mReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + index5mReject);
                    recordRejection(fyersSymbol, buySetup, close, "INDEX_5M_HURDLE", index5mReject);
                    return;
                }
                // Per-stock HTF Hurdle — stock's 1-hour close must have cleared nearest weekly level.
                String stockHtfReject = checkStockHtfHurdle(true, fyersSymbol, close, atr);
                if (stockHtfReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + buySetup + routeFor(fyersSymbol) + " SKIPPED — " + stockHtfReject);
                    recordRejection(fyersSymbol, buySetup, close, "HTF_HURDLE", stockHtfReject);
                    return;
                }
                fireSignal(fyersSymbol, buySetup, open, high, low, close, candle.volume, atr, levels, prob,
                    lastTriggerRoute.remove(fyersSymbol));
                return;
            } else {
                if (!broken.isEmpty()) {
                    String wouldMatch = detectBuyBreakout(open, high, low, close, levels, Collections.emptySet(), fyersSymbol);
                    if (wouldMatch != null && broken.contains(wouldMatch)) {
                        String detail = "level " + wouldMatch + " already traded today";
                        eventService.log("[INFO] " + fyersSymbol + " " + wouldMatch + " — skipped, level already traded");
                        recordRejection(fyersSymbol, wouldMatch, close, "LEVEL_BROKEN", detail);
                    }
                }
                // (Removed redundant atp=0 re-detect — ATP gate already verified passing at the
                // top of this block, so the re-detect produced no new information and just
                // double-fired the proximity-rejection log.)
            }
        }

        // Check SELL signals — color-agnostic. Pin bar shooting stars can be green-bodied
        // (the long upper wick is the rejection, not the body color). The other sell
        // patterns all require a red close internally, so they self-reject on green bars
        // and only the shooting star benefits.
        if (canFireAnySell) {
            String sellSetup = detectSellBreakout(open, high, low, close, levels, broken, fyersSymbol);
            if (sellSetup != null) {
                // Per-setup direction gate — TF sells need strict BEARISH; CT sells need
                // shallow rally (close > CPR top AND close < EMA20).
                boolean isTfSell = isTrendFollowingSell(sellSetup);
                if (isTfSell && !canFireTfSell) {
                    String detail = "trend-following sell needs strict BEARISH (close < CPR bot "
                        + String.format("%.2f", gateCprBot) + " AND < EMA20 "
                        + String.format("%.2f", ema20Now) + "); close=" + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "STOCK_5M_TREND", detail);
                    return;
                }
                if (!isTfSell && !canFireCtSell) {
                    String detail = "counter-trend sell needs shallow rally (close > CPR top "
                        + String.format("%.2f", gateCprTop) + " AND < EMA20 "
                        + String.format("%.2f", ema20Now) + "); close=" + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "STOCK_5M_TREND", detail);
                    return;
                }
                // Magnet gate — SELL_BELOW_R1_PDH only.
                if (isMagnet(sellSetup) && !riskSettings.isEnableMagnetTrades()) {
                    String detail = "magnet trades disabled (toggle off)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "MAGNET_DISABLED", detail);
                    return;
                }
                // Mean-reversion gate — deep fades from R2/R3/R4.
                if (isMeanReversion(sellSetup) && !riskSettings.isEnableMeanReversionTrades()) {
                    String detail = "mean-reversion trades disabled (toggle off)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "MEAN_REVERSION_DISABLED", detail);
                    return;
                }
                String prob = weeklyCprService.getProbabilityForDirection(fyersSymbol, false, sellSetup, close);
                if (prob == null) {
                    // CPR can invert (BC > TC) — always reference the actual lower edge, not the
                    // BC label, so the message matches the real gate (close < min(TC, BC)).
                    double cprBot = Math.min(levels.getTc(), levels.getBc());
                    String detail = "sell requires close < daily CPR bottom (" + String.format("%.2f", cprBot)
                        + ") (LTF bearish); close=" + String.format("%.2f", close);
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "LTF_OPPOSED", detail);
                    return;
                }
                if (!isProbabilityEnabled(prob)) {
                    String detail = prob + " not enabled";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " — skipped, " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "PROB_DISABLED", detail);
                    return;
                }
                // Daily ATR exhaustion — mirror of the buy-side gate.
                if (riskSettings.isEnableDailyAtrExhaustionFilter()) {
                    double dailyAtr = bhavcopyService.getDailyAtr(extractTicker(fyersSymbol));
                    double mult = riskSettings.getDailyAtrExhaustionMult();
                    if (dailyAtr > 0 && mult > 0) {
                        double dayHigh = candleAggregator.getDayHigh(fyersSymbol);
                        double dayLow  = candleAggregator.getDayLow(fyersSymbol);
                        double prevClose = levels.getClose();
                        if (dayHigh > 0 && dayLow > 0 && prevClose > 0) {
                            double todayTr = Math.max(dayHigh - dayLow,
                                Math.max(Math.abs(dayHigh - prevClose), Math.abs(dayLow - prevClose)));
                            if (todayTr >= mult * dailyAtr) {
                                String detail = String.format(
                                    "Today TR %.2f (gap-inclusive) >= %.2f × daily ATR %.2f — move exhausted",
                                    todayTr, mult, dailyAtr);
                                eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                                recordRejection(fyersSymbol, sellSetup, close, "DAILY_ATR_EXHAUSTED", detail);
                                return;
                            }
                        }
                    }
                }
                // Open Range Filter — strict ORB mirror of the buy gate. OR is forming until
                // the bar's CLOSE minute reaches orEndMinute (the last OR bar closes exactly
                // at orEndMinute and finalizes the OR). Counter-trend setups skip the OR
                // gate — see the buy-side equivalent for rationale.
                if (riskSettings.isEnableOpenRangeFilter() && !isCounterTrend(sellSetup)) {
                    int orMins = riskSettings.getOpenRangeMinutes();
                    long orEndMinute = MarketHolidayService.MARKET_OPEN_MINUTE + orMins;
                    long closeMinute = candle.startMinute + riskSettings.getScannerTimeframe();
                    if (closeMinute < orEndMinute) {
                        String detail = "OR not yet formed (first " + orMins + " minutes)";
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                        recordRejection(fyersSymbol, sellSetup, close, "OPEN_RANGE_FORMING", detail);
                        return;
                    }
                    double orHigh = candleAggregator.getOpenRangeHigh(fyersSymbol, orMins);
                    double orLow  = candleAggregator.getOpenRangeLow(fyersSymbol, orMins);
                    if (orHigh > 0 && orLow > 0 && close >= orLow) {
                        String detail = close > orHigh
                            ? String.format("close %.2f above OR high %.2f — sell direction opposed", close, orHigh)
                            : String.format("close %.2f inside OR [%.2f, %.2f]", close, orLow, orHigh);
                        eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                        recordRejection(fyersSymbol, sellSetup, close, "OPEN_RANGE", detail);
                        return;
                    }
                }
                // Primary-Index Open Range Filter — sell mirror. Counter-trend sells skip
                // this gate (the deep rally at R-side resistance is typically above the
                // index's OR low — would reject most counter-trend sells).
                if (riskSettings.isEnableIndexOpenRangeFilter() && !isCounterTrend(sellSetup)) {
                    int orMins = riskSettings.getOpenRangeMinutes();
                    long orEndMinute = MarketHolidayService.MARKET_OPEN_MINUTE + orMins;
                    long closeMinute = candle.startMinute + riskSettings.getScannerTimeframe();
                    String stockTicker = extractTicker(fyersSymbol);
                    String primaryIndex = bhavcopyService.getPrimaryIndexTicker(stockTicker);
                    if (primaryIndex != null && !primaryIndex.isEmpty()) {
                        String indexSym = "NSE:" + primaryIndex + "-INDEX";
                        if (closeMinute < orEndMinute) {
                            String detail = "index " + primaryIndex + " OR not yet formed (first " + orMins + " minutes)";
                            eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                            recordRejection(fyersSymbol, sellSetup, close, "INDEX_OPEN_RANGE_FORMING", detail);
                            return;
                        }
                        double idxOrHigh = candleAggregator.getOpenRangeHigh(indexSym, orMins);
                        double idxOrLow  = candleAggregator.getOpenRangeLow(indexSym, orMins);
                        CandleAggregator.CandleBar idxBar = candleAggregator.getLastCompletedCandle(indexSym);
                        double idxClose = idxBar != null ? idxBar.close : 0;
                        if (idxOrHigh > 0 && idxOrLow > 0 && idxClose > 0 && idxClose >= idxOrLow) {
                            String detail = idxClose > idxOrHigh
                                ? String.format("index %s close %.2f above OR high %.2f — sell direction opposed", primaryIndex, idxClose, idxOrHigh)
                                : String.format("index %s close %.2f inside OR [%.2f, %.2f]", primaryIndex, idxClose, idxOrLow, idxOrHigh);
                            eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                            recordRejection(fyersSymbol, sellSetup, close, "INDEX_OPEN_RANGE", detail);
                            return;
                        }
                    }
                }
                // EMA level-count filter: skip if any CPR zone sits between EMA and broken level
                if (evaluateEmaFilter(fyersSymbol, sellSetup, close, levels, atr) == 2) return;
                // NIFTY index alignment filter — misaligned trades are hard-rejected. The
                // previous LPT downgrade path has been removed.
                if (checkIndexAlignment(fyersSymbol, sellSetup, false) == NiftyAlignStatus.SKIP) {
                    String primary = bhavcopyService != null ? bhavcopyService.getPrimaryIndexTicker(extractTicker(fyersSymbol)) : "?";
                    String state = indexTrendService != null ? indexTrendService.getTrendStateForTicker(primary) : "?";
                    String detail = "Primary index " + primary + " " + state + " — sell direction opposes index trend";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "INDEX_OPPOSED", detail);
                    return;
                }
                // Stock HTF Trend Alignment — confirms the stock's own 1-hour HTF trend (weekly
                // CPR + 1-hour EMA20) agrees with the sell direction.
                if (checkStockHtfAlignment(fyersSymbol, sellSetup, false) == NiftyAlignStatus.SKIP) {
                    double htfEma = htfEmaService != null ? htfEmaService.getEma(fyersSymbol) : 0;
                    String stockHtfState = weeklyCprService != null
                        ? weeklyCprService.getStockHtfTrendState(fyersSymbol, htfEma) : "?";
                    String detail = "Stock 1h " + stockHtfState + " — sell direction opposes stock HTF trend";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "STOCK_HTF_OPPOSED", detail);
                    return;
                }
                // Index HTF Trend Alignment — sell mirror.
                if (checkIndexHtfAlignment(fyersSymbol, sellSetup, false) == NiftyAlignStatus.SKIP) {
                    String primary = bhavcopyService != null ? bhavcopyService.getPrimaryIndexTicker(extractTicker(fyersSymbol)) : "?";
                    String idxHtfState = indexTrendService != null ? indexTrendService.getHtfTrendStateForTicker(primary) : "?";
                    String detail = "Index " + primary + " 1h " + idxHtfState + " — sell direction opposes index HTF trend";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + detail);
                    recordRejection(fyersSymbol, sellSetup, close, "INDEX_HTF_OPPOSED", detail);
                    return;
                }
                // Index HTF Hurdle — stock's primary-index 1-hour close must clear its nearest weekly hurdle.
                String indexHtfReject = checkPrimaryIndexHtfHurdle(false, fyersSymbol);
                if (indexHtfReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + indexHtfReject);
                    recordRejection(fyersSymbol, sellSetup, close, "INDEX_HTF_HURDLE", indexHtfReject);
                    return;
                }
                // Index 5m Hurdle — stock's primary-index 5-min close must clear nearest daily-CPR hurdle.
                String index5mReject = checkPrimaryIndex5mHurdle(false, candle.startMinute, fyersSymbol);
                if (index5mReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + index5mReject);
                    recordRejection(fyersSymbol, sellSetup, close, "INDEX_5M_HURDLE", index5mReject);
                    return;
                }
                // Per-stock HTF Hurdle — stock's 1-hour close must have cleared nearest weekly level.
                String stockHtfReject = checkStockHtfHurdle(false, fyersSymbol, close, atr);
                if (stockHtfReject != null) {
                    eventService.log("[SCANNER] " + fyersSymbol + " " + sellSetup + routeFor(fyersSymbol) + " SKIPPED — " + stockHtfReject);
                    recordRejection(fyersSymbol, sellSetup, close, "HTF_HURDLE", stockHtfReject);
                    return;
                }
                fireSignal(fyersSymbol, sellSetup, open, high, low, close, candle.volume, atr, levels, prob,
                    lastTriggerRoute.remove(fyersSymbol));
            } else {
                // No sell breakout detected — log if close is below a key level for debugging
                if (!broken.isEmpty()) {
                    String wouldMatch = detectSellBreakout(open, high, low, close, levels, Collections.emptySet(), fyersSymbol);
                    if (wouldMatch != null && broken.contains(wouldMatch)) {
                        String detail = "level " + wouldMatch + " already traded today";
                        eventService.log("[INFO] " + fyersSymbol + " " + wouldMatch + " — skipped, level already traded");
                        recordRejection(fyersSymbol, wouldMatch, close, "LEVEL_BROKEN", detail);
                    }
                }
                // (Removed redundant atp=0 re-detect — ATP gate already verified passing at the
                // top of this block, so the re-detect produced no new information and just
                // double-fired the proximity-rejection log.)
            }
        }
    }

    /**
     * Detect buy breakout — returns setup name or null.
     * Priority: R4 > R3 > R2 > R1/PDH > CPR > S1/PDL (counter-trend levels via mean-reversion master toggle)
     * Two paths per level:
     *   Path 1 (standard breakout): open or low below level, close above — candle broke through
     *   Path 2 (wick rejection):    open above level, low dips below level, close above — buyers defended
     */
    private String detectBuyBreakout(double open, double high, double low, double close,
                                      CprLevels levels, Set<String> broken, String fyersSymbol) {
        // 5-min EMA trend gating happens at the top-level direction gate in
        // scanForBreakoutInner (canFireAnyBuy + per-setup TF/CT verification). This
        // method just runs pattern detection across all 9 levels in priority order.

        double r4 = levels.getR4(), r3 = levels.getR3(), r2 = levels.getR2();
        double r1 = levels.getR1(), ph = levels.getPh();
        double tc = levels.getTc(), bc = levels.getBc();
        double s1 = levels.getS1(), pl = levels.getPl();
        double s2 = levels.getS2(), s3 = levels.getS3(), s4 = levels.getS4();

        double cprTop = Math.max(tc, bc);
        double r1ph   = Math.max(r1, ph);
        double s1pl   = Math.max(s1, pl);

        // Pure retest model (priority R4 → R3 → R2 → R1+PDH → CPR → S1+PDL → S2 → S3 → S4):
        // multi-bar pattern retest at the single armed buy level (closest level below the
        // latest close). Patterns: hammer, bullish outside reversal (engulfing), bullish
        // doji reversal, morning star, three inside up, good-size candle (catch-all).
        // Pattern's lowest point must reach the level; the level's incremental broken-up
        // state must be set.
        //
        // All retest gating uses {@link #levelStateBySymbol}: close past upper edge sets UP,
        // close past lower edge sets DOWN, close inside a two-line zone preserves state.
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
     * Priority: S4 > S3 > S2 > S1/PDL > CPR > R1/PDH (counter-trend levels via mean-reversion master toggle)
     * Two paths per level:
     *   Path 1 (standard breakdown): open or high above level, close below — candle broke through
     *   Path 2 (wick rejection):     open below level, high pokes above level, close below — sellers defended
     */
    private String detectSellBreakout(double open, double high, double low, double close,
                                       CprLevels levels, Set<String> broken, String fyersSymbol) {
        // 5-min EMA trend gating happens at the top-level direction gate in
        // scanForBreakoutInner (canFireAnySell + per-setup TF/CT verification).

        double s4 = levels.getS4(), s3 = levels.getS3(), s2 = levels.getS2();
        double s1 = levels.getS1(), pl = levels.getPl();
        double tc = levels.getTc(), bc = levels.getBc();
        double r1 = levels.getR1(), ph = levels.getPh();
        double r2v = levels.getR2(), r3v = levels.getR3(), r4v = levels.getR4();

        double cprBot = Math.min(tc, bc);
        double s1plLo = Math.min(s1, pl);
        double r1phLo = Math.min(r1, ph);

        // Retest-only model (priority S4 → S3 → S2 → S1+PDL → CPR → R1+PDH → R2 → R3 → R4):
        // multi-bar pattern retest at the single armed sell level (closest level above the
        // latest close). Patterns: shooting star, bearish outside reversal (engulfing),
        // bearish doji reversal, evening star, three inside down. Pattern's highest point
        // must reach the level; prev candle's close must already be past it. Two-line
        // zones defer their gate to the incremental zone-broken state.
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
     * Check the retest-pattern path for a single buy setup at one
     * level. Returns the setup name on hit, null otherwise. Sets {@code lastTriggerRoute}
     * so {@link #fireSignal} can record which route fired.
     */
    /**
     * @param level  the breakout line used for the current bar's close-past-level + touch.
     *               For zone setups (CPR / R1+PDH / S1+PDL) this is the upper edge; the retest
     *               gate then defers to the incrementally-maintained zone-broken state via
     *               {@link #isZoneRetestArmed}. For single-line setups (R2/R3/R4/S2/S3/S4)
     *               the retest gate uses the strict {@code prev.close > level} check.
     */
    private String checkBuyAtLevel(String setupName, double level,
                                    double open, double high, double low, double close,
                                    double atr, Set<String> broken, String armed, String fyersSymbol) {
        if (broken.contains(setupName)) return null;
        // curr/prev fetched here so the Marubozu fresh-break path below can use them —
        // runs BEFORE the armed-level gate because Marubozu doesn't need a prior retest.
        CandleAggregator.CandleBar curr = candleAggregator.getLastCompletedCandle(fyersSymbol);
        if (curr == null) return null;
        CandleAggregator.CandleBar prev = candleAggregator.getPreviousCandle(fyersSymbol);
        if (prev == null) return null;
        // Entry-proximity threshold — applied by acceptOrRejectProximity for both the
        // Marubozu Breakout path and every retest pattern below.
        double proximityAtr = riskSettings.getEntryProximityAtrMult();

        // ── Marubozu Breakout (fresh-break, single-bar) ──────────────────────────
        // Fires when (a) THIS bar's close crosses {@code level} the prior bar's close
        // didn't, (b) bar is a Marubozu (body in [floor, ceiling] × ATR; upper wick ≤
        // X% of body), and (c) volume > 20-bar average. Independent of the retest
        // path — runs even when this setup isn't the armed level.
        if (riskSettings.isEnableMarubozuBreakout()
                && prev.close <= level && close > level
                && CandlePatternDetector.isBullishMarubozu(open, high, low, close, atr,
                        riskSettings.getMarubozuBreakoutBodyAtrMult(),
                        riskSettings.getMarubozuBreakoutMaxBodyAtrMult(),
                        riskSettings.getMarubozuBreakoutMaxOppositeWickPctOfBody())) {
            long confirmVol = curr.volume;
            double avgVol = candleAggregator.getAvgVolume(fyersSymbol, 20);
            if (confirmVol > 0 && avgVol > 0 && confirmVol > avgVol) {
                return acceptOrRejectProximity(fyersSymbol, setupName, "MARUBOZU_BREAKOUT",
                        close, level, atr, proximityAtr, true);
            }
        }

        double pinDomWickRng = riskSettings.getPinBarDominantWickMinRangeRatio();
        double pinOppWickRng = riskSettings.getPinBarOppositeWickMaxRangeRatio();
        // Retest-only model — multi-bar pattern retest at the single armed buy level.
        // Pattern's lowest point must reach the level.
        if (!setupName.equals(armed)) return null;
        double outsideMin = riskSettings.getOutsideReversalMinBodyAtrMult();
        double outsideMax = riskSettings.getOutsideReversalMaxBodyAtrMult();
        double outsidePen = riskSettings.getOutsideReversalPenetrationPct();
        double dojiBody  = riskSettings.getDojiBodyMaxRangeRatio();
        double dojiConfirm = riskSettings.getDojiConfirmBodyAtrMult();
        double dojiConfirmMax = riskSettings.getDojiConfirmMaxBodyAtrMult();
        double starOuter = riskSettings.getStarOuterBodyAtrMult();
        double starOuterMax = riskSettings.getStarOuterMaxBodyAtrMult();
        double starMid   = riskSettings.getStarMiddleBodyMaxMultOfOuter();
        double starBar3Pen = riskSettings.getStarBar3PenetrationPct();
        double haramiBody = riskSettings.getHaramiBodyAtrMult();
        double haramiBodyMax = riskSettings.getHaramiBodyMaxAtrMult();
        double haramiBar3Pen = riskSettings.getHaramiBar3PenetrationPct();
        // Shared opposing-wick cap applied to every confirmation bar (bar 2 of 2-bar
        // patterns, bar 3 of 3-bar patterns) AND the single-bar Good Size catch-all below.
        double confirmWickMax = riskSettings.getConfirmationMaxOppositeWickRatio();
        // Buy-side touch slack: lows that fall just short of the level by < tol still count
        // as a touch. Absorbs tick-level whisker misses on retest patterns.
        double touchTol  = Math.max(0, riskSettings.getLevelTouchToleranceAtr()) * atr;
        double touchLvl  = level + touchTol;

        // Retest-only — the level-broken state machine flips on the closing bar via
        // applyCandleToLevelState; the bot then waits for one of the 6 retest patterns
        // on a subsequent bar. Two-line zones: close past the breakout edge sets the
        // broken-up flag, close past the opposite edge invalidates, closes inside the
        // band preserve prior state so deeper retests stay valid.
        if (!isRetestArmed(fyersSymbol, setupName)) return null;

        // Pattern matchers — retest path. Order is specificity-first: strictest shape
        // checked earliest so the most informative tag wins when multiple patterns match
        // the same bar. Good-size candle (loosest) is the catch-all at the end.
        // (Marubozu retest dropped — geometrically rare and not a classical retest pattern;
        // near-marubozu bars now fall through to GOOD_SIZE_CANDLE_RETEST.)
        // Hammer (1 bar) — specific pin-bar reversal shape. No body-size band (small body
        // is the signature — pin-bar wick math already constrains it).
        if (CandlePatternDetector.isBullishHammer(open, high, low, close,
                    pinDomWickRng, pinOppWickRng)
                && low <= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "HAMMER_RETEST", close, level, atr, proximityAtr, true);
        }
        // Outside Reversal (Engulfing, 2 bars) — strict color flip, shared body band,
        // bar 2 closes past bar 1's open (classical engulfing) at the default penetration.
        if (prev != null && CandlePatternDetector.isBullishOutsideReversal(prev, curr, atr, outsideMin, outsideMax, outsidePen, confirmWickMax)
                && Math.min(prev.low, curr.low) <= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "OUTSIDE_REVERSAL_RETEST", close, level, atr, proximityAtr, true);
        }
        // Bullish doji reversal (2 bars) — doji at level, then strong green confirmation.
        if (prev != null && CandlePatternDetector.isBullishDojiReversal(prev, curr, atr, dojiBody, dojiConfirm, dojiConfirmMax, confirmWickMax)
                && Math.min(prev.low, curr.low) <= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "DOJI_RETEST", close, level, atr, proximityAtr, true);
        }
        // Morning star (3 bars) — 3-bar classical reversal.
        CandleAggregator.CandleBar bar1 = thirdMostRecentCandle(fyersSymbol);
        if (bar1 != null && prev != null
                && CandlePatternDetector.isMorningStar(bar1, prev, curr, atr, starOuter, starOuterMax, starMid, starBar3Pen, confirmWickMax)
                && Math.min(Math.min(bar1.low, prev.low), curr.low) <= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "STAR_RETEST", close, level, atr, proximityAtr, true);
        }
        // Three Inside Up (3-bar harami + confirmation). Bar 3 body must sit in
        // [haramiConfirm, haramiConfirmMax] × ATR — symmetric with Morning/Evening Star.
        if (bar1 != null && prev != null
                && CandlePatternDetector.isThreeInsideUp(bar1, prev, curr, atr,
                        haramiBody, haramiBodyMax, haramiBar3Pen, confirmWickMax)
                && Math.min(Math.min(bar1.low, prev.low), curr.low) <= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "HARAMI_RETEST", close, level, atr, proximityAtr, true);
        }
        // Good-Size Candle (1 bar) — catch-all loosest check. Green body in [floor, ceiling]
        // × ATR; the shared confirmWickMax caps the upper wick so shooting-star shapes don't
        // qualify. Fires only when no stricter named pattern matched above.
        //
        // Directional extent (close − low) is used for the floor/ceiling — it represents
        // the FULL bullish travel of the bar (body + lower rejection wick), not just the
        // open→close span. This captures bars where price dipped, got bought aggressively,
        // and closed strong — the body alone understates the actual buying pressure.
        double goodSizeBody    = riskSettings.getGoodSizeCandleBodyAtrMult();
        double goodSizeMaxBody = riskSettings.getGoodSizeCandleMaxBodyAtrMult();
        double bodyAbs         = close - open;
        double upperWick       = high - Math.max(open, close);
        double dirExtent       = close - low;                       // bullish travel = body + lower wick
        boolean bodyOk    = goodSizeBody    <= 0 || (atr > 0 && dirExtent >= goodSizeBody * atr);
        boolean bodyCapOk = goodSizeMaxBody <= 0 || atr <= 0 || dirExtent <= goodSizeMaxBody * atr;
        boolean wickOk    = confirmWickMax  <= 0 || bodyAbs <= 0 || upperWick <= confirmWickMax * bodyAbs;
        if (close > open && close > level && bodyOk && bodyCapOk && wickOk
                && low <= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "GOOD_SIZE_CANDLE_RETEST", close, level, atr, proximityAtr, true);
        }
        return null;
    }

    /**
     * Sell mirror of {@link #checkBuyAtLevel}.
     */
    /**
     * @param level  breakdown line for the current bar's close-below-level + touch. For zone
     *               setups this is the lower edge; the retest gate defers to the
     *               incrementally-maintained zone-broken state. For single-line setups it
     *               drives the strict {@code prev.close < level} retest check.
     */
    private String checkSellAtLevel(String setupName, double level,
                                     double open, double high, double low, double close,
                                     double atr, Set<String> broken, String armed, String fyersSymbol) {
        if (broken.contains(setupName)) return null;
        // curr/prev fetched up-front so the Marubozu fresh-break path below can use them.
        CandleAggregator.CandleBar curr = candleAggregator.getLastCompletedCandle(fyersSymbol);
        if (curr == null) return null;
        CandleAggregator.CandleBar prev = candleAggregator.getPreviousCandle(fyersSymbol);
        if (prev == null) return null;
        double proximityAtr = riskSettings.getEntryProximityAtrMult();

        // ── Marubozu Breakdown (fresh-break, single-bar, sell mirror) ────────────
        if (riskSettings.isEnableMarubozuBreakout()
                && prev.close >= level && close < level
                && CandlePatternDetector.isBearishMarubozu(open, high, low, close, atr,
                        riskSettings.getMarubozuBreakoutBodyAtrMult(),
                        riskSettings.getMarubozuBreakoutMaxBodyAtrMult(),
                        riskSettings.getMarubozuBreakoutMaxOppositeWickPctOfBody())) {
            long confirmVol = curr.volume;
            double avgVol = candleAggregator.getAvgVolume(fyersSymbol, 20);
            if (confirmVol > 0 && avgVol > 0 && confirmVol > avgVol) {
                return acceptOrRejectProximity(fyersSymbol, setupName, "MARUBOZU_BREAKOUT",
                        close, level, atr, proximityAtr, false);
            }
        }

        double pinDomWickRng = riskSettings.getPinBarDominantWickMinRangeRatio();
        double pinOppWickRng = riskSettings.getPinBarOppositeWickMaxRangeRatio();
        // Retest-only model — multi-bar pattern retest at the single armed sell level.
        // Pattern's highest point must reach the level.
        if (!setupName.equals(armed)) return null;
        double outsideMin = riskSettings.getOutsideReversalMinBodyAtrMult();
        double outsideMax = riskSettings.getOutsideReversalMaxBodyAtrMult();
        double outsidePen = riskSettings.getOutsideReversalPenetrationPct();
        double dojiBody  = riskSettings.getDojiBodyMaxRangeRatio();
        double dojiConfirm = riskSettings.getDojiConfirmBodyAtrMult();
        double dojiConfirmMax = riskSettings.getDojiConfirmMaxBodyAtrMult();
        double starOuter = riskSettings.getStarOuterBodyAtrMult();
        double starOuterMax = riskSettings.getStarOuterMaxBodyAtrMult();
        double starMid   = riskSettings.getStarMiddleBodyMaxMultOfOuter();
        double starBar3Pen = riskSettings.getStarBar3PenetrationPct();
        double haramiBody = riskSettings.getHaramiBodyAtrMult();
        double haramiBodyMax = riskSettings.getHaramiBodyMaxAtrMult();
        double haramiBar3Pen = riskSettings.getHaramiBar3PenetrationPct();
        // Shared opposing-wick cap — same one used on the buy side.
        double confirmWickMax = riskSettings.getConfirmationMaxOppositeWickRatio();
        // Sell-side touch slack: highs that fall just short of the level by < tol still count
        // as a touch. Mirror of the buy-side whisker tolerance.
        double touchTol  = Math.max(0, riskSettings.getLevelTouchToleranceAtr()) * atr;
        double touchLvl  = level - touchTol;

        // Retest-only (mirror of buy logic). The level-broken state machine flips on
        // the closing bar and the bot waits for one of the 6 retest patterns.
        if (!isRetestArmed(fyersSymbol, setupName)) return null;

        // Pattern matchers — retest path. Order is specificity-first; mirror of the buy chain.
        // (Marubozu retest dropped — geometrically rare; near-marubozu bars fall through to
        // GOOD_SIZE_CANDLE_RETEST below.)
        // Shooting star (1 bar) — pin-bar reversal; small-body by definition.
        if (CandlePatternDetector.isShootingStar(open, high, low, close,
                    pinDomWickRng, pinOppWickRng)
                && high >= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "HAMMER_RETEST", close, level, atr, proximityAtr, false);
        }
        // Outside Reversal (2 bars) — unified pattern (mirror of buy side).
        if (prev != null && CandlePatternDetector.isBearishOutsideReversal(prev, curr, atr, outsideMin, outsideMax, outsidePen, confirmWickMax)
                && Math.max(prev.high, curr.high) >= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "OUTSIDE_REVERSAL_RETEST", close, level, atr, proximityAtr, false);
        }
        // Bearish doji reversal (2 bars) — doji at level, then strong red confirmation.
        if (prev != null && CandlePatternDetector.isBearishDojiReversal(prev, curr, atr, dojiBody, dojiConfirm, dojiConfirmMax, confirmWickMax)
                && Math.max(prev.high, curr.high) >= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "DOJI_RETEST", close, level, atr, proximityAtr, false);
        }
        // Evening star (3 bars) — 3-bar classical reversal.
        CandleAggregator.CandleBar bar1 = thirdMostRecentCandle(fyersSymbol);
        if (bar1 != null && prev != null
                && CandlePatternDetector.isEveningStar(bar1, prev, curr, atr, starOuter, starOuterMax, starMid, starBar3Pen, confirmWickMax)
                && Math.max(Math.max(bar1.high, prev.high), curr.high) >= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "STAR_RETEST", close, level, atr, proximityAtr, false);
        }
        // Three Inside Down (3-bar harami + confirmation). Bar 3 body band same as buy side.
        if (bar1 != null && prev != null
                && CandlePatternDetector.isThreeInsideDown(bar1, prev, curr, atr,
                        haramiBody, haramiBodyMax, haramiBar3Pen, confirmWickMax)
                && Math.max(Math.max(bar1.high, prev.high), curr.high) >= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "HARAMI_RETEST", close, level, atr, proximityAtr, false);
        }
        // Good-Size Candle (1 bar) — catch-all loosest check. Red body in [floor, ceiling]
        // × ATR; the shared confirmWickMax caps the lower wick so hammer-shaped bars don't
        // qualify. Fires only when no stricter named pattern matched above.
        //
        // Directional extent (high − close) is used for the floor/ceiling — it represents
        // the FULL bearish travel of the bar (upper rejection wick + body), not just the
        // open→close span. Captures bars where price spiked up, got sold aggressively,
        // and closed weak — the body alone understates the actual selling pressure.
        double goodSizeBody    = riskSettings.getGoodSizeCandleBodyAtrMult();
        double goodSizeMaxBody = riskSettings.getGoodSizeCandleMaxBodyAtrMult();
        double bodyAbs         = open - close;
        double lowerWick       = Math.min(open, close) - low;
        double dirExtent       = high - close;                      // bearish travel = upper wick + body
        boolean bodyOk    = goodSizeBody    <= 0 || (atr > 0 && dirExtent >= goodSizeBody * atr);
        boolean bodyCapOk = goodSizeMaxBody <= 0 || atr <= 0 || dirExtent <= goodSizeMaxBody * atr;
        boolean wickOk    = confirmWickMax  <= 0 || bodyAbs <= 0 || lowerWick <= confirmWickMax * bodyAbs;
        if (close < open && close < level && bodyOk && bodyCapOk && wickOk
                && high >= touchLvl) {
            return acceptOrRejectProximity(fyersSymbol, setupName, "GOOD_SIZE_CANDLE_RETEST", close, level, atr, proximityAtr, false);
        }
        return null;
    }

    /**
     * Build a `" [PATTERN_NAME]"` suffix for log lines so traders can see which candlestick
     * pattern formed at the bar — both for trades that fired and for trades that got rejected
     * by downstream filters (NIFTY hurdle, R/R, etc.). Pattern is set by
     * {@code checkBuyAtLevel} / {@code checkSellAtLevel} into {@code lastTriggerRoute} when a
     * match is detected, BEFORE the post-pattern filter chain runs. Returns empty string when
     * no pattern was detected for this symbol's current scan (e.g. ATP / EMA-trend reject
     * before pattern matching even began).
     */
    private String routeFor(String fyersSymbol) {
        String r = lastTriggerRoute.get(fyersSymbol);
        return r != null && !r.isEmpty() ? " [" + r + "]" : "";
    }

    /**
     * Finalize a matched pattern: apply the entry-proximity gate, log + reject if the
     * confirmation close drifted too far from the retested level. Otherwise stamp the
     * route tag and return the setup name. Called from every pattern-match block in
     * checkBuyAtLevel / checkSellAtLevel to centralize the proximity check.
     */
    private String acceptOrRejectProximity(String fyersSymbol, String setupName, String routeTag,
                                           double close, double level, double atr,
                                           double proximityAtr, boolean isBuy) {
        if (proximityAtr > 0 && atr > 0) {
            double distance = isBuy ? (close - level) : (level - close);
            if (distance > proximityAtr * atr) {
                String detail = String.format(
                    "close (%.2f) is %.2f pts %s level (%.2f), threshold %.2f × ATR (%.2f)",
                    close, distance, isBuy ? "above" : "below", level, proximityAtr, proximityAtr * atr);
                eventService.log("[SCANNER] " + fyersSymbol + " " + setupName
                    + " [" + routeTag + "] blocked by entry proximity — " + detail);
                recordRejection(fyersSymbol, setupName, close, "ENTRY_PROXIMITY", detail);
                return null;
            }
        }
        // Append a VOLUME CONFIRMATION suffix when the confirmation candle's volume exceeds
        // its 20-period rolling average. The suffix flows through routeFor() into every
        // downstream skip/reject log line and into fireSignal's payload, so the Signal Trail
        // and the SignalProcessor [SUCCESS] log both surface it.
        CandleAggregator.CandleBar confirmBar = candleAggregator.getLastCompletedCandle(fyersSymbol);
        long confirmVol = confirmBar != null ? confirmBar.volume : 0;
        double avgVol = candleAggregator.getAvgVolume(fyersSymbol, 20);
        if (confirmVol > 0 && avgVol > 0 && confirmVol > avgVol) {
            routeTag = routeTag + " + VOLUME CONFIRMATION";
        }
        lastTriggerRoute.put(fyersSymbol, routeTag);
        return setupName;
    }

    /**
     * Apply one candle's close to the per-symbol level-broken state across all 9 levels.
     * Called once per new prior candle in {@link #advanceZoneState}. For each level a close
     * strictly past one edge sets that direction and clears the opposite; a close on or
     * inside the band leaves the state unchanged. For single-line levels upper == lower,
     * so close > line → UP, close < line → DOWN, close == line → no change.
     */
    private void applyCandleToLevelState(String fyersSymbol, double close, CprLevels levels) {
        ConcurrentHashMap<String, LevelBrokenState> map =
            levelStateBySymbol.computeIfAbsent(fyersSymbol, k -> new ConcurrentHashMap<>());

        double r4 = levels.getR4(), r3 = levels.getR3(), r2 = levels.getR2();
        double r1 = levels.getR1(), ph = levels.getPh();
        double tc = levels.getTc(), bc = levels.getBc();
        double s1 = levels.getS1(), pl = levels.getPl();
        double s2 = levels.getS2(), s3 = levels.getS3(), s4 = levels.getS4();

        applyOneLevel(map, "R4",     close, r4, r4);
        applyOneLevel(map, "R3",     close, r3, r3);
        applyOneLevel(map, "R2",     close, r2, r2);
        applyOneLevel(map, "R1_PDH", close, Math.max(r1, ph), Math.min(r1, ph));
        applyOneLevel(map, "CPR",    close, Math.max(tc, bc), Math.min(tc, bc));
        applyOneLevel(map, "S1_PDL", close, Math.max(s1, pl), Math.min(s1, pl));
        applyOneLevel(map, "S2",     close, s2, s2);
        applyOneLevel(map, "S3",     close, s3, s3);
        applyOneLevel(map, "S4",     close, s4, s4);
    }

    private static void applyOneLevel(ConcurrentHashMap<String, LevelBrokenState> map,
                                       String levelKey, double close, double upper, double lower) {
        if (upper <= 0 || lower <= 0) return;
        LevelBrokenState s = map.computeIfAbsent(levelKey, k -> new LevelBrokenState());
        if (close > upper)      { s.up = true;  s.down = false; }
        else if (close < lower) { s.down = true; s.up = false; }
        // close inside zone / on either edge → preserve prior state.
    }

    /**
     * Bring the level-broken state up to "state-up-to-PREV" for this symbol. Applies any
     * completed bar (except the most recent — that's the current candle being scanned)
     * whose epoch is newer than {@link #lastZoneAppliedEpoch}. On the first call after a
     * fresh start / mid-day restart, walks all today's prior bars; on subsequent calls
     * normally only one new bar gets applied. Handles skipped scans correctly because
     * application is keyed on epoch, not on call count.
     */
    private void advanceZoneState(String fyersSymbol, CprLevels levels) {
        List<CandleAggregator.CandleBar> bars = candleAggregator.getCompletedCandles(fyersSymbol);
        if (bars == null || bars.size() < 2) return;
        long lastApplied = lastZoneAppliedEpoch.getOrDefault(fyersSymbol, 0L);
        long newLast = lastApplied;
        for (int i = 0; i < bars.size() - 1; i++) {
            CandleAggregator.CandleBar bar = bars.get(i);
            if (bar.epochSec > lastApplied) {
                applyCandleToLevelState(fyersSymbol, bar.close, levels);
                if (bar.epochSec > newLast) newLast = bar.epochSec;
            }
        }
        if (newLast > lastApplied) {
            lastZoneAppliedEpoch.put(fyersSymbol, newLast);
            // Persist immediately — a restart between this advance and the next signal-driven
            // saveState would otherwise lose the up/down flips and force a full rebuild from
            // re-fetched candle history (which is the silent-loss pathway we're closing here).
            saveState();
        }
    }

    /**
     * Returns true if the given setup's "broken" state is set for the symbol. Unified across
     * single-line levels (R2/R3/R4, S2/S3/S4) and two-line zones (CPR, R1+PDH, S1+PDL) — the
     * state-update rule treats single lines as a degenerate zone with upper == lower.
     */
    private boolean isRetestArmed(String fyersSymbol, String setupName) {
        Map<String, LevelBrokenState> map = levelStateBySymbol.get(fyersSymbol);
        if (map == null) return false;
        String levelKey;
        boolean wantUp;
        if (setupName.startsWith("BUY_ABOVE_")) {
            levelKey = setupName.substring("BUY_ABOVE_".length());
            wantUp = true;
        } else if (setupName.startsWith("SELL_BELOW_")) {
            levelKey = setupName.substring("SELL_BELOW_".length());
            wantUp = false;
        } else {
            return false;
        }
        LevelBrokenState s = map.get(levelKey);
        if (s == null) return false;
        return wantUp ? s.up : s.down;
    }

    /** Returns the 3rd-most-recent completed candle (for morning/evening star), or null. */
    private CandleAggregator.CandleBar thirdMostRecentCandle(String fyersSymbol) {
        List<CandleAggregator.CandleBar> list = candleAggregator.getCompletedCandles(fyersSymbol);
        if (list == null || list.size() < 3) return null;
        return list.get(list.size() - 3);
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

        // 5-min EMA trend snapshot at signal time — single EMA20 trendline only.
        double ema20Now = emaService.getEma(fyersSymbol);
        String trend = "--";
        if (ema20Now > 0) {
            trend = close > ema20Now ? "BULLISH" : close < ema20Now ? "BEARISH" : "NEUTRAL";
        }
        // Include the candle-route tag (HAMMER_RETEST, OUTSIDE_REVERSAL_RETEST,
        // DOJI_RETEST, STAR_RETEST, HARAMI_RETEST, GOOD_SIZE_CANDLE_RETEST) when present
        // so the event log makes it clear which path fired the entry. Every pattern
        // requires a confirmed retest of the broken level.
        String routeTag = (scannerNote != null && !scannerNote.isEmpty()) ? " | route=" + scannerNote : "";
        eventService.log("[SCANNER] " + fyersSymbol + " " + setup + " | close=" + String.format("%.2f", close)
            + " | ATR=" + String.format("%.2f", atr) + " | " + prob + routeTag
            + " | 5m trend=" + trend + " | " + timeStr);

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
            info.pattern = scannerNote != null ? scannerNote : "";  // route/pattern tag consumed from lastTriggerRoute by caller
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
     * NIFTY index alignment filter. Hard-reject mode only — a trade direction that opposes
     * the NIFTY composite trend is rejected outright. (The previous LPT-downgrade soft path
     * has been removed.)
     */
    /** Result of the NIFTY alignment check. SKIP = trade rejected; OK = aligned or check off. */
    private enum NiftyAlignStatus { OK, SKIP }

    /**
     * NIFTY index alignment filter. The allowed-NIFTY-state set depends on the setup family:
     * <ul>
     *   <li><b>Counter-trend (magnet + mean-reversion)</b> setups require FULL agreement on
     *       NIFTY direction — only BULLISH for buys, only BEARISH for sells. Reversal states
     *       (NIFTY rolling over with CPR still on the opposite side) are NOT enough — a magnet
     *       bounce at S1+PDL while NIFTY is BEARISH_REVERSAL (above CPR but rolling bearish)
     *       has no edge.</li>
     *   <li><b>Trend-following (HPT)</b> setups also accept the matching reversal state, since
     *       a reversal hand-off lines up with the breakout direction even before CPR confirms.</li>
     *   <li>SIDEWAYS / NEUTRAL / opposite-direction → skip in both cases.</li>
     * </ul>
     * Returns OK if the filter is disabled or index data isn't available yet.
     */
    private NiftyAlignStatus checkIndexAlignment(String fyersSymbol, String setup, boolean isBuy) {
        if (!riskSettings.isEnableIndexAlignment()) return NiftyAlignStatus.OK;
        if (indexTrendService == null || bhavcopyService == null) return NiftyAlignStatus.OK;
        try {
            String stockTicker = extractTicker(fyersSymbol);
            String primaryIndex = bhavcopyService.getPrimaryIndexTicker(stockTicker);
            if (primaryIndex == null || primaryIndex.isEmpty()) primaryIndex = "NIFTY50";
            String state = indexTrendService.getTrendStateForTicker(primaryIndex);
            // Index trend is strict 2-factor (daily CPR + 5-min EMA20): BULLISH / BEARISH /
            // INSIDE / SIDEWAYS / NEUTRAL. No REVERSAL states.
            //   • NEUTRAL → fail-open (no data yet, pre-market or CPR not loaded).
            //   • INSIDE / SIDEWAYS → hard reject (no clean directional bias).
            //   • Buys require state = BULLISH (close > CPR top AND > 5m EMA20).
            //   • Sells require state = BEARISH (close < CPR bot AND < 5m EMA20).
            if ("NEUTRAL".equals(state)) return NiftyAlignStatus.OK;
            boolean aligned = isBuy ? "BULLISH".equals(state) : "BEARISH".equals(state);
            String requiredStates = isBuy ? "BULLISH" : "BEARISH";
            if (aligned) return NiftyAlignStatus.OK;
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup + routeFor(fyersSymbol)
                + " INDEX MISALIGNED — " + primaryIndex + " " + state + ", trade direction needs " + requiredStates);
            return NiftyAlignStatus.SKIP;
        } catch (Exception e) {
            log.warn("[BreakoutScanner] Index alignment check failed for {}: {}", fyersSymbol, e.getMessage());
        }
        return NiftyAlignStatus.OK;
    }

    /**
     * Index HTF Trend Alignment filter. Mirrors {@link #checkStockHtfAlignment} but on the
     * stock's primary <i>index</i> 1-hour timeframe. Strict 2-factor: index 1-hour close vs
     * index weekly CPR top/bot + index 1-hour EMA20. Buys require BULLISH; sells require
     * BEARISH. NEUTRAL fail-opens. Default off.
     */
    private NiftyAlignStatus checkIndexHtfAlignment(String fyersSymbol, String setup, boolean isBuy) {
        if (!riskSettings.isEnableIndexHtfAlignment()) return NiftyAlignStatus.OK;
        if (indexTrendService == null || bhavcopyService == null) return NiftyAlignStatus.OK;
        try {
            String stockTicker = extractTicker(fyersSymbol);
            String primaryIndex = bhavcopyService.getPrimaryIndexTicker(stockTicker);
            if (primaryIndex == null || primaryIndex.isEmpty()) primaryIndex = "NIFTY50";
            String state = indexTrendService.getHtfTrendStateForTicker(primaryIndex);
            if ("NEUTRAL".equals(state)) return NiftyAlignStatus.OK;     // fail-open on missing data
            boolean aligned = isBuy ? "BULLISH".equals(state) : "BEARISH".equals(state);
            if (aligned) return NiftyAlignStatus.OK;
            String requiredStates = isBuy ? "BULLISH" : "BEARISH";
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup + routeFor(fyersSymbol)
                + " INDEX HTF MISALIGNED — " + primaryIndex + " 1h " + state + ", trade direction needs " + requiredStates);
            return NiftyAlignStatus.SKIP;
        } catch (Exception e) {
            log.warn("[BreakoutScanner] Index HTF alignment check failed for {}: {}", fyersSymbol, e.getMessage());
        }
        return NiftyAlignStatus.OK;
    }

    /**
     * Stock HTF Trend Alignment filter. Mirrors {@link #checkIndexAlignment} but on the
     * <i>stock's own</i> 1-hour timeframe: requires the stock's HTF trend state — derived
     * from (1-hour close vs weekly CPR) + (1-hour close vs 1-hour EMA20) — to agree with
     * the trade direction. Buy needs BULLISH; sell needs BEARISH (strict 2-factor, no
     * REVERSAL accept). INSIDE rejects (close inside weekly CPR = no directional bias);
     * NEUTRAL fail-opens (missing weekly levels or 1-hour close).
     */
    private NiftyAlignStatus checkStockHtfAlignment(String fyersSymbol, String setup, boolean isBuy) {
        if (!riskSettings.isEnableStockHtfAlignment()) return NiftyAlignStatus.OK;
        if (weeklyCprService == null || htfEmaService == null) return NiftyAlignStatus.OK;
        try {
            boolean useEma = riskSettings.isEnableStockHtf1hEma20Check();
            double htfEma = htfEmaService.getEma(fyersSymbol);
            String state = weeklyCprService.getStockHtfTrendState(fyersSymbol, htfEma, useEma);
            if ("NEUTRAL".equals(state)) return NiftyAlignStatus.OK;     // fail-open on missing data

            // useEma=true  → strict 2-factor (1h close > weekly CPR top AND > 1h EMA20).
            // useEma=false → CPR-only (1h close > weekly CPR top is enough). Either way,
            //                buys need BULLISH; sells need BEARISH.
            boolean aligned = isBuy ? "BULLISH".equals(state) : "BEARISH".equals(state);
            if (aligned) return NiftyAlignStatus.OK;

            String requiredStates = isBuy ? "BULLISH" : "BEARISH";
            String mode = useEma ? "CPR+EMA" : "CPR-only";
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup + routeFor(fyersSymbol)
                + " STOCK HTF MISALIGNED (" + mode + ") — stock 1h " + state + ", trade direction needs " + requiredStates);
            return NiftyAlignStatus.SKIP;
        } catch (Exception e) {
            log.warn("[BreakoutScanner] Stock HTF alignment check failed for {}: {}", fyersSymbol, e.getMessage());
        }
        return NiftyAlignStatus.OK;
    }

    /**
     * Index HTF Hurdle filter. When the stock's primary index's most-recent 1-hour close
     * hasn't cleared its nearest weekly hurdle in trade direction, trades in that direction
     * are skipped until the next 1-hour close commits.
     *
     * <p>The primary index is resolved per stock via the {@code stocks.primary_index_id}
     * mapping (NIFTY 50 or a sector index). Returns a non-null reason string when the
     * filter rejects, or null when the trade is allowed (filter off, no hurdle, hurdle
     * cleared, or fail-open on missing data).
     *
     * <p>When the filter passes AND a hurdle existed in trade direction (the trade was
     * actually gated past something), captures the primary index's last completed 15-min
     * bar low/high into {@link #pendingHurdleGuards} keyed by {@code fyersSymbol} for the
     * early-exit service to persist onto the position record at fill time.
     */
    private String checkPrimaryIndexHtfHurdle(boolean isBuy, String fyersSymbol) {
        if (!riskSettings.isEnableIndexHtfHurdleFilter()) return null;
        if (marketDataService == null || weeklyCprService == null) return null;
        try {
            // Resolve the stock's primary index — alignment & hurdle filters now run against
            // each stock's mapped index (NIFTY 50 or a sector index) instead of always NIFTY.
            String stockTicker = bhavcopyService != null ? extractTicker(fyersSymbol) : null;
            String primaryIndex = bhavcopyService != null ? bhavcopyService.getPrimaryIndexTicker(stockTicker) : "NIFTY50";
            if (primaryIndex == null || primaryIndex.isEmpty()) primaryIndex = "NIFTY50";
            String indexSym = "NSE:" + primaryIndex + "-INDEX";
            double indexLtp = marketDataService.getLtp(indexSym);
            if (indexLtp <= 0) return null; // no LTP yet — fail-open

            WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(indexSym);
            if (wl == null) return null; // weekly levels not loaded — fail-open

            // Weekly hurdle candidates in trade direction.
            java.util.List<Double> candidateLevels = new java.util.ArrayList<>(6);
            java.util.List<String> candidateNames  = new java.util.ArrayList<>(6);
            if (isBuy) {
                candidateLevels.add(wl.r1);    candidateNames.add("weekly R1");
                candidateLevels.add(wl.ph);    candidateNames.add("weekly PWH");
                candidateLevels.add(wl.tc);    candidateNames.add("weekly TC");
                candidateLevels.add(wl.pivot); candidateNames.add("weekly Pivot");
                candidateLevels.add(wl.bc);    candidateNames.add("weekly BC");
                candidateLevels.add(wl.r2);    candidateNames.add("weekly R2");
                candidateLevels.add(wl.r3);    candidateNames.add("weekly R3");
                candidateLevels.add(wl.r4);    candidateNames.add("weekly R4");
            } else {
                candidateLevels.add(wl.s1);    candidateNames.add("weekly S1");
                candidateLevels.add(wl.pl);    candidateNames.add("weekly PWL");
                candidateLevels.add(wl.tc);    candidateNames.add("weekly TC");
                candidateLevels.add(wl.pivot); candidateNames.add("weekly Pivot");
                candidateLevels.add(wl.bc);    candidateNames.add("weekly BC");
                candidateLevels.add(wl.s2);    candidateNames.add("weekly S2");
                candidateLevels.add(wl.s3);    candidateNames.add("weekly S3");
                candidateLevels.add(wl.s4);    candidateNames.add("weekly S4");
            }
            // Virgin CPR is intentionally NOT added here — daily-level concept stays at
            // the daily-CPR (5m) gate.

            // Nearest hurdle in trade direction relative to the primary index's current price.
            double chosenLevel = 0;
            String chosenName = null;
            for (int i = 0; i < candidateLevels.size(); i++) {
                double lv = candidateLevels.get(i);
                if (lv <= 0) continue;
                if (isBuy) {
                    if (lv < indexLtp && lv > chosenLevel) { chosenLevel = lv; chosenName = candidateNames.get(i); }
                } else {
                    if (lv > indexLtp && (chosenName == null || lv < chosenLevel)) {
                        chosenLevel = lv; chosenName = candidateNames.get(i);
                    }
                }
            }
            if (chosenName == null) return null; // no hurdle in trade direction → clear path

            // Most-recently-completed 1-hour close on the primary index (session-aligned:
            // 10:15, 11:15, …, 15:15). Pre-10:15 IST today, falls back to the previous trading
            // day's last 1-hour close within the current ISO week. On Monday pre-10:15 there's
            // no current-week fallback → null → REJECT (hurdle exists but the 1-hour hasn't
            // yet committed either way).
            Double htfClose = candleAggregator != null
                ? candleAggregator.getLast1HourClose(indexSym) : null;
            if (htfClose == null || htfClose <= 0) {
                return primaryIndex + " HTF hurdle at " + chosenName
                    + " (" + String.format("%.2f", chosenLevel) + ") — waiting for first 1-hour close (10:15 IST)";
            }

            boolean cleared = isBuy ? htfClose > chosenLevel : htfClose < chosenLevel;
            if (!cleared) {
                return primaryIndex + " HTF hurdle at " + chosenName
                    + ": LTP " + String.format("%.2f", indexLtp)
                    + ", 1-hour close=" + String.format("%.2f", htfClose)
                    + ", level " + String.format("%.2f", chosenLevel);
            }

            // Headroom check — reject if the nearest hurdle in the OPPOSITE direction is
            // closer than minHeadroomAtr × index ATR.
            double minHeadroomAtr = riskSettings.getIndexHtfHurdleMinHeadroomAtr();
            if (minHeadroomAtr > 0 && atrService != null) {
                double indexAtr = atrService.getAtr(indexSym);
                if (indexAtr > 0) {
                    double minHeadroomPts = minHeadroomAtr * indexAtr;
                    double upcomingLevel = 0;
                    String upcomingName = null;
                    for (int i = 0; i < candidateLevels.size(); i++) {
                        double lv = candidateLevels.get(i);
                        if (lv <= 0) continue;
                        if (isBuy) {
                            if (lv > indexLtp && (upcomingName == null || lv < upcomingLevel)) {
                                upcomingLevel = lv; upcomingName = candidateNames.get(i);
                            }
                        } else {
                            if (lv < indexLtp && lv > upcomingLevel) {
                                upcomingLevel = lv; upcomingName = candidateNames.get(i);
                            }
                        }
                    }
                    if (upcomingName != null) {
                        double headroomPts = isBuy ? upcomingLevel - indexLtp : indexLtp - upcomingLevel;
                        if (headroomPts < minHeadroomPts) {
                            return primaryIndex + " hurdle ahead at " + upcomingName
                                + " (" + String.format("%.2f", upcomingLevel) + "): only "
                                + String.format("%.2f", headroomPts) + " pts headroom, need "
                                + String.format("%.2f", minHeadroomPts)
                                + " (" + minHeadroomAtr + " × " + primaryIndex + " ATR " + String.format("%.2f", indexAtr) + ")";
                        }
                    }
                }
            }
            // Filter passed. Capture the gating 15-min bar's low/high so the early-exit
            // service can defend that level after the trade fills.
            CandleAggregator.CandleBar gatingBar = candleAggregator != null
                ? candleAggregator.getLastCompleted15MinCandle(indexSym) : null;
            if (gatingBar != null && gatingBar.low > 0 && gatingBar.high > 0 && fyersSymbol != null) {
                pendingHurdleGuards.put(fyersSymbol,
                    new NiftyHurdleGuard(gatingBar.low, gatingBar.high, isBuy));
            }
            return null; // prior 1-hour close has cleared the hurdle, headroom OK
        } catch (Exception e) {
            log.warn("[BreakoutScanner] Index HTF hurdle check failed: {}", e.getMessage());
            return null; // fail-open
        }
    }

    /**
     * Read-only "is NIFTY currently at a crucial level" check for the open-positions UI.
     * Runs the same 3 hurdle filters used at trade-entry time (HTF, 5-min, virgin CPR) but
     * does NOT capture any guards or fire side effects. Returns a human-readable reason
     * string (or pipe-joined concatenation if more than one fires) describing the active
     * hurdles, or {@code null} if all three are clear / disabled. Used by the positions
     * table to highlight rows where NIFTY's current position would block a NEW trade in
     * the same direction — informational only, doesn't affect the open position.
     */
    /**
     * Structured single-chip view of the *nearest* NIFTY hurdle in trade direction.
     * Used by the scanner page's NIFTY card. Considers all three filters (HTF / 5m /
     * Virgin CPR) and returns the one whose relevant level is geometrically nearest
     * to NIFTY's current LTP. Tie-break order: HTF > 5m > Virgin.
     *
     * <p>State map:
     * <ul>
     *   <li>{@code WAITING} — behind level/zone exists and the relevant close (5m for
     *       daily/Virgin, 15m for HTF) hasn't yet cleared it. Entries are blocked.</li>
     *   <li>{@code AHEAD_BLOCKED} — no behind blocker, but the nearest level ahead is
     *       within {@code minHeadroomAtr × ATR}. Entries are blocked via headroom.</li>
     *   <li>{@code AHEAD_CLEAR} — nearest level ahead is beyond the headroom threshold.
     *       Informational only; entries pass this filter.</li>
     * </ul>
     *
     * <p>Returns {@code null} when no level exists in trade direction across all enabled
     * filters, or when required data is missing.
     */
    public record HurdleStatus(String level, String category, String state, double distance) {}

    public HurdleStatus getNiftyNearestHurdle(boolean isBuy) {
        if (marketDataService == null) return null;
        String niftySym = IndexTrendService.NIFTY_SYMBOL;
        double niftyLtp = marketDataService.getLtp(niftySym);
        if (niftyLtp <= 0) return null;
        double niftyAtr = atrService != null ? atrService.getAtr(niftySym) : 0;

        // Same-bucket NIFTY 5m close, used by the 5m / Virgin filters. May be null when
        // the matching bucket hasn't completed yet — those filters then drop out.
        Double niftyClose = resolveCurrentBucket5mClose(niftySym);

        java.util.List<HurdleStatus> candidates = new java.util.ArrayList<>(3);
        HurdleStatus c;
        if (riskSettings.isEnableIndexHtfHurdleFilter()
                && (c = computeHtfCandidate(isBuy, niftySym, niftyLtp, niftyAtr,
                        riskSettings.getIndexHtfHurdleMinHeadroomAtr(), "HTF")) != null) candidates.add(c);
        if (riskSettings.isEnableIndex5mHurdleFilter()
                && (c = compute5mCandidate(isBuy, "NIFTY50", niftyLtp, niftyAtr, niftyClose,
                        riskSettings.getIndex5mHurdleMinHeadroomAtr(), "5m")) != null) candidates.add(c);
        if (candidates.isEmpty()) return null;

        candidates.sort((a, b) -> {
            int d = Double.compare(a.distance(), b.distance());
            if (d != 0) return d;
            return hurdleCategoryRank(a.category()) - hurdleCategoryRank(b.category());
        });
        return candidates.get(0);
    }

    private static int hurdleCategoryRank(String cat) {
        return switch (cat) {
            case "HTF" -> 0;
            case "5m"  -> 1;
            default    -> 99;
        };
    }

    /** Most recent NIFTY 5-min completed bar's close, matched to the bar that just fired
     *  the scanner. Mirrors the same-bucket lookup in {@link #checkPrimaryIndex5mHurdle}. */
    private Double resolveCurrentBucket5mClose(String niftySym) {
        if (candleAggregator == null) return null;
        CandleAggregator.CandleBar last = candleAggregator.getLastCompletedCandle(niftySym);
        if (last != null && last.close > 0) return last.close;
        return null;
    }

    /**
     * Per-stock equivalent of {@link #getNiftyNearestHurdle(boolean)} — returns the nearest
     * active hurdle considering THREE sources, picking the candidate whose level sits
     * geographically closest to the relevant LTP (normalized as % so cross-instrument
     * candidates compare fairly):
     * <ol>
     *   <li><b>Stock HTF</b> — stock's own weekly levels vs stock LTP / 1-hour close.</li>
     *   <li><b>Index HTF</b> — primary index's weekly levels vs index LTP / 1-hour close.</li>
     *   <li><b>Index 5m</b> — primary index's daily CPR / R / S levels vs index LTP / 5m close.</li>
     * </ol>
     * The level name is prefixed with the source descriptor — "Stock Weekly R1",
     * "NIFTYBANK Weekly R1", or "NIFTYBANK Daily R1+PDH" — so the scanner chip identifies
     * whether it's a stock-level or index-level hurdle. Category is one of
     * "Stock HTF" / "Index HTF" / "Index 5m". Returns null when no candidate is available
     * (all filters disabled, no data, or no hurdle in trade direction).
     */
    public HurdleStatus getStockNearestHurdle(String fyersSymbol, boolean isBuy) {
        if (marketDataService == null || bhavcopyService == null) return null;

        String stockTicker = extractTicker(fyersSymbol);
        String primaryIndex = bhavcopyService.getPrimaryIndexTicker(stockTicker);
        if (primaryIndex == null || primaryIndex.isEmpty()) primaryIndex = "NIFTY50";
        String indexSym = "NSE:" + primaryIndex + "-INDEX";

        // Candidates carry their (status, refLtp) for cross-instrument distance normalization.
        java.util.List<HurdleStatus> candidates = new java.util.ArrayList<>(3);
        java.util.List<Double>       refLtps    = new java.util.ArrayList<>(3);
        HurdleStatus c;

        // 1) Stock HTF — own weekly levels + own 1-hour close.
        if (riskSettings.isEnableHtfHurdleFilter()) {
            double stockLtp = marketDataService.getLtp(fyersSymbol);
            if (stockLtp > 0) {
                double stockAtr = atrService != null ? atrService.getAtr(fyersSymbol) : 0;
                c = computeHtfCandidate(isBuy, fyersSymbol, stockLtp, stockAtr,
                        riskSettings.getHtfHurdleMinHeadroomAtr(), "Stock HTF");
                if (c != null) {
                    candidates.add(new HurdleStatus("Stock " + c.level(), c.category(), c.state(), c.distance()));
                    refLtps.add(stockLtp);
                }
            }
        }

        // Primary index data — shared between Index HTF and Index 5m candidates.
        double indexLtp = marketDataService.getLtp(indexSym);
        if (indexLtp > 0) {
            double indexAtr = atrService != null ? atrService.getAtr(indexSym) : 0;

            // 2) Index HTF — primary index's weekly levels + 1-hour close.
            if (riskSettings.isEnableIndexHtfHurdleFilter()) {
                c = computeHtfCandidate(isBuy, indexSym, indexLtp, indexAtr,
                        riskSettings.getIndexHtfHurdleMinHeadroomAtr(), "Index HTF");
                if (c != null) {
                    candidates.add(new HurdleStatus(primaryIndex + " " + c.level(), c.category(), c.state(), c.distance()));
                    refLtps.add(indexLtp);
                }
            }

            // 3) Index 5m — primary index's daily CPR / R / S zones + 5m close.
            if (riskSettings.isEnableIndex5mHurdleFilter()) {
                Double indexClose5m = resolveCurrentBucket5mClose(indexSym);
                c = compute5mCandidate(isBuy, primaryIndex, indexLtp, indexAtr, indexClose5m,
                        riskSettings.getIndex5mHurdleMinHeadroomAtr(), "Index 5m");
                if (c != null) {
                    candidates.add(new HurdleStatus(primaryIndex + " " + c.level(), c.category(), c.state(), c.distance()));
                    refLtps.add(indexLtp);
                }
            }
        }

        if (candidates.isEmpty()) return null;

        // Pick the candidate with the smallest distance-as-fraction-of-LTP — fair cross-
        // instrument comparison since absolute point distances aren't comparable between a
        // stock at ₹500 and an index at ₹50,000.
        int bestIdx = 0;
        double bestPct = candidates.get(0).distance() / refLtps.get(0);
        for (int i = 1; i < candidates.size(); i++) {
            double pct = candidates.get(i).distance() / refLtps.get(i);
            if (pct < bestPct) { bestPct = pct; bestIdx = i; }
        }
        return candidates.get(bestIdx);
    }

    /** HTF candidate — weekly level closest to the given symbol's LTP in trade direction.
     *  Caller is responsible for checking the relevant filter-enabled flag. Returns null
     *  when weekly levels aren't loaded or no level exists ahead in trade direction. */
    private HurdleStatus computeHtfCandidate(boolean isBuy, String fyersSymbol, double ltp, double atr,
                                              double headroomAtrMul, String category) {
        if (weeklyCprService == null) return null;
        WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(fyersSymbol);
        if (wl == null) return null;

        double[] levels;
        String[] names;
        if (isBuy) {
            levels = new double[]{ wl.r1, wl.ph, wl.tc, wl.pivot, wl.bc, wl.r2, wl.r3, wl.r4 };
            names  = new String[]{ "Weekly R1", "Weekly PWH", "Weekly TC", "Weekly Pivot", "Weekly BC",
                                    "Weekly R2", "Weekly R3", "Weekly R4" };
        } else {
            levels = new double[]{ wl.s1, wl.pl, wl.tc, wl.pivot, wl.bc, wl.s2, wl.s3, wl.s4 };
            names  = new String[]{ "Weekly S1", "Weekly PWL", "Weekly TC", "Weekly Pivot", "Weekly BC",
                                    "Weekly S2", "Weekly S3", "Weekly S4" };
        }

        // Behind = nearest level we've already passed in trade direction.
        // For buy: highest level below LTP. For sell: lowest level above LTP.
        double behindLvl = 0; String behindName = null;
        double aheadLvl  = 0; String aheadName  = null;
        for (int i = 0; i < levels.length; i++) {
            double lv = levels[i];
            if (lv <= 0) continue;
            if (isBuy) {
                if (lv < ltp && lv > behindLvl) { behindLvl = lv; behindName = names[i]; }
                if (lv > ltp && (aheadName == null || lv < aheadLvl)) { aheadLvl = lv; aheadName = names[i]; }
            } else {
                if (lv > ltp && (behindName == null || lv < behindLvl)) { behindLvl = lv; behindName = names[i]; }
                if (lv < ltp && lv > aheadLvl) { aheadLvl = lv; aheadName = names[i]; }
            }
        }

        // WAITING: behind level not yet cleared by 1-hour close.
        if (behindName != null && candleAggregator != null) {
            Double htfClose = candleAggregator.getLast1HourClose(fyersSymbol);
            if (htfClose != null && htfClose > 0) {
                boolean cleared = isBuy ? htfClose > behindLvl : htfClose < behindLvl;
                if (!cleared) {
                    return new HurdleStatus(behindName, category, "WAITING", Math.abs(behindLvl - ltp));
                }
            }
        }

        // AHEAD_BLOCKED / AHEAD_CLEAR
        if (aheadName == null) return null;
        double headroomPts = Math.abs(aheadLvl - ltp);
        double minHeadroom = headroomAtrMul * atr;
        String state = (minHeadroom > 0 && headroomPts < minHeadroom) ? "AHEAD_BLOCKED" : "AHEAD_CLEAR";
        return new HurdleStatus(aheadName, category, state, headroomPts);
    }

    /** 5-min daily-level candidate — nearest CPR / R1+PDH / S1+PDL zone OR R2/R3/R4 / S2/S3/S4
     *  single line in trade direction. Extended levels treated as zero-width zones (lo == hi).
     *  Caller is responsible for checking the relevant filter-enabled flag. */
    private HurdleStatus compute5mCandidate(boolean isBuy, String indexTicker, double indexLtp, double indexAtr,
                                             Double indexClose, double headroomAtrMul, String category) {
        if (bhavcopyService == null) return null;
        var cpr = bhavcopyService.getCprLevels(indexTicker);
        if (cpr == null) return null;

        // Index CPR squeeze decides whether R1+PDH and S1+PDL collapse into one zone or split
        // into two single-line levels. Layer 1 only (CPR width vs 14d SMA) — the zone-split
        // is a geometric question; TR doesn't enter it. Matches the chip helper.
        boolean indexCprNarrow = bhavcopyService.getAdaptiveCpr(indexTicker).cprNarrow();

        // Direction-restricted zone set (matches the HTF chip / gate pattern):
        //   • Buy considers CPR + R-side + PDC. S-side levels are skipped — for a buy, the
        //     relevant hurdles sit above LTP, and an already-passed support shouldn't
        //     surface as a buy hurdle.
        //   • Sell mirrors: CPR + S-side + PDC.
        // CPR and PDC are shared because they aren't intrinsically directional.
        double[][] zoneEdges;
        String[]   zoneNames;
        if (indexCprNarrow) {
            if (isBuy) {
                zoneEdges = new double[][] {
                    { Math.min(cpr.getTc(), cpr.getBc()), Math.max(cpr.getTc(), cpr.getBc()) },
                    { Math.min(cpr.getR1(), cpr.getPh()), Math.max(cpr.getR1(), cpr.getPh()) },
                    { cpr.getR2(), cpr.getR2() },
                    { cpr.getR3(), cpr.getR3() },
                    { cpr.getR4(), cpr.getR4() },
                    { cpr.getClose(), cpr.getClose() }
                };
                zoneNames = new String[] {
                    "Daily CPR", "Daily R1+PDH", "Daily R2", "Daily R3", "Daily R4", "Daily PDC"
                };
            } else {
                zoneEdges = new double[][] {
                    { Math.min(cpr.getTc(), cpr.getBc()), Math.max(cpr.getTc(), cpr.getBc()) },
                    { Math.min(cpr.getS1(), cpr.getPl()), Math.max(cpr.getS1(), cpr.getPl()) },
                    { cpr.getS2(), cpr.getS2() },
                    { cpr.getS3(), cpr.getS3() },
                    { cpr.getS4(), cpr.getS4() },
                    { cpr.getClose(), cpr.getClose() }
                };
                zoneNames = new String[] {
                    "Daily CPR", "Daily S1+PDL", "Daily S2", "Daily S3", "Daily S4", "Daily PDC"
                };
            }
        } else {
            // Wide CPR — split R1+PDH and S1+PDL into independent single-line levels.
            if (isBuy) {
                zoneEdges = new double[][] {
                    { Math.min(cpr.getTc(), cpr.getBc()), Math.max(cpr.getTc(), cpr.getBc()) },
                    { cpr.getR1(), cpr.getR1() },
                    { cpr.getPh(), cpr.getPh() },
                    { cpr.getR2(), cpr.getR2() },
                    { cpr.getR3(), cpr.getR3() },
                    { cpr.getR4(), cpr.getR4() },
                    { cpr.getClose(), cpr.getClose() }
                };
                zoneNames = new String[] {
                    "Daily CPR", "Daily R1", "Daily PDH", "Daily R2", "Daily R3", "Daily R4", "Daily PDC"
                };
            } else {
                zoneEdges = new double[][] {
                    { Math.min(cpr.getTc(), cpr.getBc()), Math.max(cpr.getTc(), cpr.getBc()) },
                    { cpr.getS1(), cpr.getS1() },
                    { cpr.getPl(), cpr.getPl() },
                    { cpr.getS2(), cpr.getS2() },
                    { cpr.getS3(), cpr.getS3() },
                    { cpr.getS4(), cpr.getS4() },
                    { cpr.getClose(), cpr.getClose() }
                };
                zoneNames = new String[] {
                    "Daily CPR", "Daily S1", "Daily PDL", "Daily S2", "Daily S3", "Daily S4", "Daily PDC"
                };
            }
        }

        // Behind zone — for buy: zone whose LO ≤ LTP (LTP entered from below) with the
        // highest HI; for sell: zone whose HI ≥ LTP with the lowest LO.
        int behindIdx = -1;
        double behindAnchor = isBuy ? -Double.MAX_VALUE : Double.MAX_VALUE;
        int aheadIdx = -1;
        double aheadAnchor = isBuy ? Double.MAX_VALUE : -Double.MAX_VALUE;
        for (int i = 0; i < zoneEdges.length; i++) {
            double lo = zoneEdges[i][0], hi = zoneEdges[i][1];
            if (lo <= 0 || hi <= 0) continue;
            if (isBuy) {
                if (lo <= indexLtp && hi > behindAnchor) { behindAnchor = hi; behindIdx = i; }
                if (lo > indexLtp  && lo < aheadAnchor)  { aheadAnchor  = lo; aheadIdx  = i; }
            } else {
                if (hi >= indexLtp && lo < behindAnchor) { behindAnchor = lo; behindIdx = i; }
                if (hi < indexLtp  && hi > aheadAnchor)  { aheadAnchor  = hi; aheadIdx  = i; }
            }
        }

        // WAITING: same-bucket 5m close hasn't cleared the behind zone's far edge.
        // Pre-close fallback (first bar of the day, or any time no completed candle is
        // available yet): use LTP. If LTP is currently inside the behind zone, the zone
        // is still active regardless — don't fall through and incorrectly report an ahead
        // level (e.g. R2) as the hurdle.
        if (behindIdx >= 0) {
            double zLo = zoneEdges[behindIdx][0], zHi = zoneEdges[behindIdx][1];
            double refClose = indexClose != null ? indexClose : indexLtp;
            boolean cleared = isBuy ? refClose > zHi : refClose < zLo;
            if (!cleared) {
                double farEdge = isBuy ? zHi : zLo;
                return new HurdleStatus(zoneNames[behindIdx], category, "WAITING", Math.abs(farEdge - indexLtp));
            }
        }

        // AHEAD_BLOCKED / AHEAD_CLEAR
        if (aheadIdx < 0) return null;
        double headroomPts = Math.abs(aheadAnchor - indexLtp);
        double minHeadroom = headroomAtrMul * indexAtr;
        String state = (minHeadroom > 0 && headroomPts < minHeadroom) ? "AHEAD_BLOCKED" : "AHEAD_CLEAR";
        return new HurdleStatus(zoneNames[aheadIdx], category, state, headroomPts);
    }

    /** Hurdle alert for the open-positions table — uses the same {@link #getStockNearestHurdle}
     *  pick the scanner card surfaces (3 sources: Stock HTF / Index HTF / Index 5m). Returns
     *  a formatted "{category} {level} {state}" string for WAITING / AHEAD_BLOCKED hurdles,
     *  null otherwise (no hurdle or AHEAD_CLEAR). Keeps the positions stripe aligned with
     *  whatever the card chip shows for the same symbol. */
    public String getStockHurdleAlert(boolean isBuy, String fyersSymbol) {
        HurdleStatus h = getStockNearestHurdle(fyersSymbol, isBuy);
        if (h == null) return null;
        String state = h.state();
        if ("AHEAD_CLEAR".equals(state)) return null;
        return h.category() + " " + h.level() + " — " + state;
    }

    /**
     * Current sticky NIFTY trend state (BULLISH / BEARISH / BULLISH_REVERSAL /
     * BEARISH_REVERSAL / SIDEWAYS / NEUTRAL). Stable across a 5-min candle; flips only
     * on NIFTY's 5-min candle close. Used by entry-fill handlers to snapshot the trend
     * at trade time so the positions UI can later detect a flip.
     */
    public String getCurrentNiftyTrend() {
        if (indexTrendService == null) return "NEUTRAL";
        String state = indexTrendService.getStickyState();
        return state != null ? state : "NEUTRAL";
    }

    /**
     * Current trend state of the stock's primary index (sector index for sector-mapped
     * stocks, NIFTY 50 for the NIFTY-mapped ones). Used by the positions UI for the
     * stripe + trend-flip check so each position reads its own index instead of always
     * NIFTY. Falls back to NEUTRAL when the index has no LTP / CPR data yet.
     */
    public String getCurrentPrimaryIndexTrend(String fyersSymbol) {
        if (indexTrendService == null) return "NEUTRAL";
        String state = indexTrendService.getSectorTrendForStock(fyersSymbol);
        return state != null ? state : "NEUTRAL";
    }

    /**
     * True if the current NIFTY trend has flipped its directional bias since the position's
     * entry. Treats the 4 trend states as two sides:
     * <ul>
     *   <li>Bullish side: BULLISH, BULLISH_REVERSAL</li>
     *   <li>Bearish side: BEARISH, BEARISH_REVERSAL</li>
     * </ul>
     * A flip is a cross between the two sides (BULLISH → BEARISH_REVERSAL counts, but
     * BULLISH → BULLISH_REVERSAL does not — the bias is still bullish, just weaker). The
     * natural cycle BULLISH → BEARISH_REVERSAL → BEARISH → BULLISH_REVERSAL → BULLISH
     * triggers a single flip when crossing the bullish/bearish boundary, not on every
     * adjacent-state transition.
     *
     * <p>SIDEWAYS and NEUTRAL belong to neither side — transitions involving them never
     * count as a flip.
     */
    public boolean isNiftyTrendFlipped(String entryTrend) {
        return isTrendFlipped(entryTrend, getCurrentNiftyTrend());
    }

    /** Primary-index flavour of {@link #isNiftyTrendFlipped} — compares the entry-time
     *  primary-index trend to the current primary-index trend for the same stock. */
    public boolean isPrimaryIndexTrendFlipped(String entryTrend, String fyersSymbol) {
        return isTrendFlipped(entryTrend, getCurrentPrimaryIndexTrend(fyersSymbol));
    }

    private static boolean isTrendFlipped(String entryTrend, String current) {
        if (entryTrend == null || entryTrend.isEmpty()) return false;
        if (current == null) return false;
        boolean entryBullish = "BULLISH".equals(entryTrend)  || "BULLISH_REVERSAL".equals(entryTrend);
        boolean entryBearish = "BEARISH".equals(entryTrend)  || "BEARISH_REVERSAL".equals(entryTrend);
        boolean nowBullish   = "BULLISH".equals(current)     || "BULLISH_REVERSAL".equals(current);
        boolean nowBearish   = "BEARISH".equals(current)     || "BEARISH_REVERSAL".equals(current);
        if (!entryBullish && !entryBearish) return false;    // entry had no side (SIDEWAYS/NEUTRAL)
        return (entryBullish && nowBearish) || (entryBearish && nowBullish);
    }

    /**
     * Consume + clear the pending NIFTY HTF Hurdle break-guard captured at signal-firing
     * time for {@code fyersSymbol}. Called by the entry-fill handler immediately after
     * persisting the position record. Returns null if no guard was captured for this symbol
     * (filter disabled at entry, no hurdle in trade direction, etc.) — caller should treat
     * null as "no guard applies".
     */
    public NiftyHurdleGuard consumePendingHurdleGuard(String fyersSymbol) {
        if (fyersSymbol == null) return null;
        return pendingHurdleGuards.remove(fyersSymbol);
    }

    /**
     * 5-min variant of {@link #checkPrimaryIndexHtfHurdle}, against the stock's primary index's
     * <i>daily</i> CPR levels (not weekly). When a stock's 5-min breakout fires, the primary
     * index's same-bucket 5-min close must already have cleared its nearest daily-CPR hurdle
     * in the trade direction. Guards against firing while the index is just now crossing a
     * daily resistance/support — wait for confirmation that the bar already closed past
     * the level.
     *
     * <p>R1+PDH and S1+PDL collapse into zones or split into single lines based on the index's
     * CPR width vs the {@code narrowCprMinWidth} / {@code narrowCprMaxWidth} band, matching
     * the chip helper {@link #compute5mCandidate}.
     *
     * <p>Returns null on pass (filter off, no hurdle in trade direction, same-bucket close
     * cleared, or no bar available — fail-open). Returns a non-null reason string to reject.
     */
    /**
     * Per-stock HTF Hurdle filter. Mirrors {@link #checkPrimaryIndexHtfHurdle} but on the
     * stock's own data: picks the nearest weekly level relative to the stock's current LTP,
     * then checks whether the stock's most-recently-completed 1-hour close has cleared that
     * level, and finally a headroom check against the nearest weekly hurdle in the opposite
     * direction.
     *
     * <p>Hurdle candidates (match Index HTF Hurdle for consistency): R1, PWH, weekly TC,
     * weekly Pivot, weekly BC, R2, R3, R4 for buys; S1, PWL, weekly TC, weekly Pivot,
     * weekly BC, S2, S3, S4 for sells.
     *
     * <p>Fail-open when: filter disabled, weekly levels not loaded, no hurdle in trade direction.
     * <b>Rejects</b> when a hurdle exists but no 1-hour close is available in the current ISO
     * week (Monday pre-10:15) — waits until 10:15 to confirm.
     */
    private String checkStockHtfHurdle(boolean isBuy, String fyersSymbol, double close, double atr) {
        if (!riskSettings.isEnableHtfHurdleFilter()) return null;
        if (weeklyCprService == null || candleAggregator == null) return null;

        WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(fyersSymbol);
        if (wl == null) return null;

        double[] candidates;
        String[] names;
        if (isBuy) {
            candidates = new double[]{ wl.r1, wl.ph, wl.tc, wl.pivot, wl.bc, wl.r2, wl.r3, wl.r4 };
            names      = new String[]{ "R1", "PWH", "weekly TC", "weekly Pivot", "weekly BC", "weekly R2", "weekly R3", "weekly R4" };
        } else {
            candidates = new double[]{ wl.s1, wl.pl, wl.tc, wl.pivot, wl.bc, wl.s2, wl.s3, wl.s4 };
            names      = new String[]{ "S1", "PWL", "weekly TC", "weekly Pivot", "weekly BC", "weekly S2", "weekly S3", "weekly S4" };
        }

        // Stock's current LTP — falls back to the breakout 5-min close if live LTP missing.
        double stockPrice = marketDataService != null ? marketDataService.getLtp(fyersSymbol) : 0;
        if (stockPrice <= 0) stockPrice = close;

        // Nearest weekly hurdle in trade direction relative to current price.
        double chosenLevel = 0;
        String chosenName = null;
        for (int i = 0; i < candidates.length; i++) {
            double lv = candidates[i];
            if (lv <= 0) continue;
            if (isBuy) {
                if (lv < stockPrice && lv > chosenLevel) { chosenLevel = lv; chosenName = names[i]; }
            } else {
                if (lv > stockPrice && (chosenName == null || lv < chosenLevel)) { chosenLevel = lv; chosenName = names[i]; }
            }
        }
        if (chosenName != null) {
            Double htfClose = candleAggregator.getLast1HourClose(fyersSymbol);
            if (htfClose == null || htfClose <= 0) {
                // No 1-hour close in current ISO week (Monday pre-10:15). Hurdle exists but
                // the 1-hour hasn't yet committed either way — reject and wait for the 10:15
                // close. Mirrors the NIFTY HTF Hurdle behaviour.
                return "HTF hurdle at weekly " + chosenName
                    + " (" + String.format("%.2f", chosenLevel) + ") — waiting for first 1-hour close (10:15 IST)";
            } else if (isBuy ? htfClose <= chosenLevel : htfClose >= chosenLevel) {
                return "HTF hurdle at weekly " + chosenName
                    + ": price=" + String.format("%.2f", stockPrice)
                    + ", 1-hour close=" + String.format("%.2f", htfClose)
                    + ", level=" + String.format("%.2f", chosenLevel);
            }
        }

        // Headroom check — nearest weekly hurdle in OPPOSITE direction must be ≥ minHeadroomAtr × ATR away.
        double minHeadroomAtr = riskSettings.getHtfHurdleMinHeadroomAtr();
        if (minHeadroomAtr > 0 && atr > 0) {
            double minHeadroomPts = minHeadroomAtr * atr;
            double upcomingLevel = 0;
            String upcomingName = null;
            for (int i = 0; i < candidates.length; i++) {
                double lv = candidates[i];
                if (lv <= 0) continue;
                if (isBuy) {
                    if (lv > stockPrice && (upcomingName == null || lv < upcomingLevel)) {
                        upcomingLevel = lv; upcomingName = names[i];
                    }
                } else {
                    if (lv < stockPrice && lv > upcomingLevel) {
                        upcomingLevel = lv; upcomingName = names[i];
                    }
                }
            }
            if (upcomingName != null) {
                double headroomPts = isBuy ? upcomingLevel - stockPrice : stockPrice - upcomingLevel;
                if (headroomPts < minHeadroomPts) {
                    return "HTF hurdle ahead at weekly " + upcomingName
                        + " (" + String.format("%.2f", upcomingLevel) + "): only "
                        + String.format("%.2f", headroomPts) + " pts headroom, need "
                        + String.format("%.2f", minHeadroomPts)
                        + " (" + minHeadroomAtr + " × ATR " + String.format("%.2f", atr) + ")";
                }
            }
        }

        return null;
    }

    private String checkPrimaryIndex5mHurdle(boolean isBuy, long stockBucketStartMinute, String fyersSymbol) {
        if (!riskSettings.isEnableIndex5mHurdleFilter()) return null;
        if (candleAggregator == null || marketDataService == null || bhavcopyService == null) return null;

        // Resolve the stock's primary index — gate runs against its daily CPR + 5m close.
        String stockTicker = fyersSymbol != null ? extractTicker(fyersSymbol) : null;
        String primaryIndex = bhavcopyService.getPrimaryIndexTicker(stockTicker);
        if (primaryIndex == null || primaryIndex.isEmpty()) primaryIndex = "NIFTY50";
        String indexSym = "NSE:" + primaryIndex + "-INDEX";
        double indexLtp = marketDataService.getLtp(indexSym);
        if (indexLtp <= 0) return null; // no LTP — fail-open

        var cpr = bhavcopyService.getCprLevels(primaryIndex);
        if (cpr == null) return null; // daily CPR not loaded — fail-open

        // Zone-based candidate set — CPR plus the direction-relevant side. CPR is included
        // because for REVERSAL states (BULLISH_REVERSAL with index below CPR, BEARISH_REVERSAL
        // with index above CPR), CPR sits AHEAD of LTP in the trade direction and is a real
        // hurdle. S-side levels are excluded for buy (and R-side for sell) — the gate now
        // mirrors the HTF gate's direction-restricted candidate set, matching the chip helper.
        double cprLow  = Math.min(cpr.getTc(), cpr.getBc());
        double cprHigh = Math.max(cpr.getTc(), cpr.getBc());
        // Index CPR squeeze (Layer 1 of the adaptive classifier — CPR width vs 14d SMA)
        // decides whether R1+PDH and S1+PDL collapse into one zone or split into single
        // lines. Same call as the chip helper {@link #compute5mCandidate}.
        boolean indexCprNarrow = bhavcopyService.getAdaptiveCpr(primaryIndex).cprNarrow();

        // Extended levels (R2/R3/R4, S2/S3/S4) treated as zero-width zones (lo == hi).
        double[][] zoneEdges;
        String[]   zoneNames;
        if (indexCprNarrow) {
            if (isBuy) {
                zoneEdges = new double[][] {
                    { cprLow, cprHigh },
                    { Math.min(cpr.getR1(), cpr.getPh()), Math.max(cpr.getR1(), cpr.getPh()) },
                    { cpr.getR2(), cpr.getR2() },
                    { cpr.getR3(), cpr.getR3() },
                    { cpr.getR4(), cpr.getR4() }
                };
                zoneNames = new String[] { "CPR", "R1+PDH", "R2", "R3", "R4" };
            } else {
                zoneEdges = new double[][] {
                    { cprLow, cprHigh },
                    { Math.min(cpr.getS1(), cpr.getPl()), Math.max(cpr.getS1(), cpr.getPl()) },
                    { cpr.getS2(), cpr.getS2() },
                    { cpr.getS3(), cpr.getS3() },
                    { cpr.getS4(), cpr.getS4() }
                };
                zoneNames = new String[] { "CPR", "S1+PDL", "S2", "S3", "S4" };
            }
        } else {
            // Wide CPR — R1+PDH and S1+PDL split into independent single-line levels.
            if (isBuy) {
                zoneEdges = new double[][] {
                    { cprLow, cprHigh },
                    { cpr.getR1(), cpr.getR1() },
                    { cpr.getPh(), cpr.getPh() },
                    { cpr.getR2(), cpr.getR2() },
                    { cpr.getR3(), cpr.getR3() },
                    { cpr.getR4(), cpr.getR4() }
                };
                zoneNames = new String[] { "CPR", "R1", "PDH", "R2", "R3", "R4" };
            } else {
                zoneEdges = new double[][] {
                    { cprLow, cprHigh },
                    { cpr.getS1(), cpr.getS1() },
                    { cpr.getPl(), cpr.getPl() },
                    { cpr.getS2(), cpr.getS2() },
                    { cpr.getS3(), cpr.getS3() },
                    { cpr.getS4(), cpr.getS4() }
                };
                zoneNames = new String[] { "CPR", "S1", "PDL", "S2", "S3", "S4" };
            }
        }

        // "Behind" zone — for buys, the highest zone whose lower edge is at/below LTP (we've at
        // least entered it from below); for sells, the lowest zone whose upper edge is at/above
        // LTP. The 5m close must clear the FAR edge of THAT zone:
        //   • Buy: 5m close > max(zone) — confirms we exited the zone upward
        //   • Sell: 5m close < min(zone) — confirms we exited the zone downward
        // A close still INSIDE the zone (or short of the far edge) rejects.
        int chosenIdx = -1;
        double chosenAnchor = isBuy ? -Double.MAX_VALUE : Double.MAX_VALUE;
        for (int i = 0; i < zoneEdges.length; i++) {
            double lo = zoneEdges[i][0], hi = zoneEdges[i][1];
            if (lo <= 0 || hi <= 0) continue;
            if (isBuy) {
                if (lo <= indexLtp && hi > chosenAnchor) { chosenAnchor = hi; chosenIdx = i; }
            } else {
                if (hi >= indexLtp && lo < chosenAnchor) { chosenAnchor = lo; chosenIdx = i; }
            }
        }
        if (chosenIdx < 0) return null; // no zone behind in trade direction → clear path

        String chosenName = zoneNames[chosenIdx];
        double zLo = zoneEdges[chosenIdx][0];
        double zHi = zoneEdges[chosenIdx][1];

        // Resolve the primary index's current 5-min close — same startMinute bucket as the
        // stock that just fired. The stock bar and the index bar close synchronously, so by
        // the time we evaluate this filter the index's same-bucket bar is already in
        // completedCandles. Using the current bar (not prior) means an index close past the
        // zone in the same bar that triggered the stock signal counts as confirmation — no
        // extra one-bar wait.
        CandleAggregator.CandleBar currentBar = null;
        CandleAggregator.CandleBar last = candleAggregator.getLastCompletedCandle(indexSym);
        if (last != null && last.startMinute == stockBucketStartMinute && last.close > 0) {
            currentBar = last;
        } else {
            CandleAggregator.CandleBar prev = candleAggregator.getPreviousCandle(indexSym);
            if (prev != null && prev.startMinute == stockBucketStartMinute && prev.close > 0) {
                currentBar = prev;
            }
        }
        if (currentBar == null) return null; // index same-bucket bar not yet completed — fail-open

        double indexClose = currentBar.close;
        boolean cleared = isBuy ? indexClose > zHi : indexClose < zLo;
        if (!cleared) {
            String where = (indexClose >= zLo && indexClose <= zHi)
                ? "inside " + chosenName + " zone"
                : "short of " + chosenName + " zone";
            return primaryIndex + " 5m " + where
                + " [" + String.format("%.2f", zLo) + ", " + String.format("%.2f", zHi) + "]"
                + ": 5m close " + String.format("%.2f", indexClose)
                + ", " + primaryIndex + " LTP " + String.format("%.2f", indexLtp);
        }

        // Headroom check — reject if the next zone ahead is closer than minHeadroomAtr × index ATR.
        double minHeadroomAtr = riskSettings.getIndex5mHurdleMinHeadroomAtr();
        if (minHeadroomAtr > 0 && atrService != null) {
            double indexAtr = atrService.getAtr(indexSym);
            if (indexAtr > 0) {
                double minHeadroomPts = minHeadroomAtr * indexAtr;
                int aheadIdx = -1;
                double aheadAnchor = isBuy ? Double.MAX_VALUE : -Double.MAX_VALUE;
                for (int i = 0; i < zoneEdges.length; i++) {
                    double lo = zoneEdges[i][0], hi = zoneEdges[i][1];
                    if (lo <= 0 || hi <= 0) continue;
                    if (isBuy) {
                        if (lo > indexLtp && lo < aheadAnchor) { aheadAnchor = lo; aheadIdx = i; }
                    } else {
                        if (hi < indexLtp && hi > aheadAnchor) { aheadAnchor = hi; aheadIdx = i; }
                    }
                }
                if (aheadIdx >= 0) {
                    double headroomPts = Math.abs(aheadAnchor - indexLtp);
                    if (headroomPts < minHeadroomPts) {
                        return primaryIndex + " 5m hurdle ahead at " + zoneNames[aheadIdx]
                            + " zone (near edge " + String.format("%.2f", aheadAnchor) + "): only "
                            + String.format("%.2f", headroomPts) + " pts headroom, need "
                            + String.format("%.2f", minHeadroomPts)
                            + " (" + minHeadroomAtr + " × " + primaryIndex + " ATR " + String.format("%.2f", indexAtr) + ")";
                    }
                }
            }
        }
        return null;
    }

    /**
     * Magnet setup — BUY_ABOVE_S1_PDL or SELL_BELOW_R1_PDH only. First structural level pair;
     * gated by <code>enableMagnetTrades</code> and qty-sized by <code>magnetTradesQtyFactor</code>.
     */
    private static boolean isMagnet(String setup) {
        return "BUY_ABOVE_S1_PDL".equals(setup) || "SELL_BELOW_R1_PDH".equals(setup);
    }

    /**
     * Deep mean-reversion setup — BUY_ABOVE_S2/S3/S4 or SELL_BELOW_R2/R3/R4. Far support/
     * resistance fades; gated by <code>enableMeanReversionTrades</code> and qty-sized by
     * <code>meanReversionQtyFactor</code>.
     */
    private static boolean isMeanReversion(String setup) {
        if (setup == null) return false;
        return "BUY_ABOVE_S2".equals(setup)
            || "BUY_ABOVE_S3".equals(setup)
            || "BUY_ABOVE_S4".equals(setup)
            || "SELL_BELOW_R2".equals(setup)
            || "SELL_BELOW_R3".equals(setup)
            || "SELL_BELOW_R4".equals(setup);
    }

    /** Trend-following BUY: breakout at CPR / R1+PDH / R2+ — bigger trend up. */
    private static boolean isTrendFollowingBuy(String setup) {
        return "BUY_ABOVE_CPR".equals(setup)
            || "BUY_ABOVE_R1_PDH".equals(setup)
            || "BUY_ABOVE_R2".equals(setup)
            || "BUY_ABOVE_R3".equals(setup)
            || "BUY_ABOVE_R4".equals(setup);
    }

    /** Trend-following SELL: breakdown at CPR / S1+PDL / S2+ — bigger trend down. */
    private static boolean isTrendFollowingSell(String setup) {
        return "SELL_BELOW_CPR".equals(setup)
            || "SELL_BELOW_S1_PDL".equals(setup)
            || "SELL_BELOW_S2".equals(setup)
            || "SELL_BELOW_S3".equals(setup)
            || "SELL_BELOW_S4".equals(setup);
    }

    /** Counter-trend = magnet + mean-reversion. BUY at S-side or SELL at R-side. */
    private static boolean isCounterTrend(String setup) {
        return isMagnet(setup) || isMeanReversion(setup);
    }

    private boolean isProbabilityEnabled(String prob) {
        if (prob == null) return false;
        return switch (prob) {
            case "HPT" -> riskSettings.isEnableHpt();
            case "MPT" -> riskSettings.isEnableMpt();
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

        // Universe gate — DB-backed Stock Universe (Settings → Stock Universe) controls eligibility.
        if (!bhavcopyService.isInScanUniverse(ticker)) return false;

        // Adaptive state must be enabled in Settings (matches the scanner watchlist gate).
        BhavcopyService.CprState state = bhavcopyService.getAdaptiveCpr(ticker).state();
        boolean stateOk = switch (state) {
            case NARROW            -> riskSettings.isEnableCprStateA();
            case AVERAGE           -> riskSettings.isEnableCprStateB();
            case WIDE              -> riskSettings.isEnableCprStateC();
            case INSUFFICIENT_DATA -> false;
        };
        if (stateOk) return true;

        // Inside-only CPR — width filter still applies via insideCprMaxWidth.
        boolean isInside = bhavcopyService.getInsideCprStocks().stream()
                .anyMatch(c -> c.getSymbol().equals(ticker));
        double insideMaxWidth = riskSettings.getInsideCprMaxWidth();
        if (isInside && (insideMaxWidth <= 0 || cpr.getCprWidthPct() <= insideMaxWidth)) {
            return true;
        }

        return false;
    }

    /**
     * EMA level-count filter. Counts CPR zones strictly between EMA and the broken level.
     * Pair zones (CPR, R1/PDH, S1/PDL) are collapsed — each counted at most once.
     * The setup's own zone is excluded. For DH/DL, the highest R-zone (or lowest S-zone)
     * that price has already passed is excluded — e.g. if DH is above R3, R3 is excluded.
     *
     * Returns 0 = pass, 2 = skip. No halve tier. If filter disabled, always returns 0.
     */
    private int evaluateEmaFilter(String fyersSymbol, String setup, double close,
                                   CprLevels levels, double atr) {
        if (!riskSettings.isEnableEmaLevelCountFilter()) return 0;

        // Morning skip: bypass the filter while price runs hard and EMA(20) lags.
        if (riskSettings.isEmaLevelFilterMorningSkip()) {
            String until = riskSettings.getEmaLevelFilterMorningSkipUntil();
            if (until != null && !until.isEmpty()) {
                try {
                    java.time.LocalTime nowIst = java.time.ZonedDateTime.now(IST).toLocalTime();
                    java.time.LocalTime cutoff = java.time.LocalTime.parse(until);
                    if (nowIst.isBefore(cutoff)) return 0;
                } catch (Exception ignored) {}
            }
        }

        double ema = emaService.getEma(fyersSymbol);
        if (ema <= 0) return 0;
        double broken = getBreakoutLevelPrice(setup, levels, fyersSymbol);
        if (broken <= 0) return 0;

        double lo = Math.min(ema, broken);
        double hi = Math.max(ema, broken);

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

        // A zone counts as "between" EMA and broken only if the EMA-to-broken path FULLY
        // crosses through it — i.e., ALL of its valid edges sit inside the interval. For a
        // single-edge level (R2/R3/R4/S2/S3/S4/DH/DL) that's the one edge. For a two-edge
        // zone (R1+PDH, CPR, S1+PDL) it requires BOTH edges. If only one edge is in the
        // interval, the EMA is sitting INSIDE the zone (or straddling its boundary), which
        // means the zone is at the EMA's level — not a wall between them.
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
            String detail = "EMA(" + String.format("%.2f", ema) + ") is "
                + count + " zone(s) away from broken " + String.format("%.2f", broken)
                + " [zones between: " + between + "]";
            eventService.log("[SCANNER] " + fyersSymbol + " " + setup + routeFor(fyersSymbol) + " — skipped, " + detail);
            recordRejection(fyersSymbol, setup, close, "EMA_20_DISTANCE", detail);
            return 2;
        }

        // Secondary proximity check: EMA must be within (100 - emaLevelMinRangePct)% of the
        // range between the NEAR edge of the broken zone and the nearest zone edge on the
        // other side. Anchoring at the near edge (bottom of zone for buy, top for sell) means
        // the zone's own width doesn't get counted as proximity distance — wider zones aren't
        // double-penalised. Single-edge levels (R2/R3/R4 etc.) keep nearEdge == broken.
        int minRangePct = riskSettings.getEmaLevelMinRangePct();
        if (minRangePct > 0) {
            boolean isBuy = setup.startsWith("BUY_");

            // Near edge of the broken zone — the inner edge facing the EMA.
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
                // Directional subtraction + clamp at 0: EMA inside or past the broken zone
                // (at/above nearEdge for buys, at/below for sells) is "0% distance" — the
                // breakout has already swept through it.
                double rawDist = isBuy ? (nearEdge - ema) : (ema - nearEdge);
                double actualDist = Math.max(0, rawDist);
                if (actualDist > maxDist) {
                    int actualPct = (int) Math.round(actualDist / range * 100.0);
                    String detail = "EMA(" + String.format("%.2f", ema)
                        + ") too far from broken zone — must sit within "
                        + (100 - minRangePct) + "% of range from "
                        + (isBuy ? "bottom" : "top") + " of broken zone"
                        + " (zone edge " + String.format("%.2f", nearEdge)
                        + " → boundary " + String.format("%.2f", boundaryEdge)
                        + " = range " + String.format("%.2f", range)
                        + "; dist " + String.format("%.2f", actualDist)
                        + " = " + actualPct + "% > " + (100 - minRangePct) + "%)";
                    eventService.log("[SCANNER] " + fyersSymbol + " " + setup + routeFor(fyersSymbol) + " — skipped, " + detail);
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
     * Single armed level per direction, rolling with price. VWAP joins the CPR levels as a
     * peer candidate (its value comes from live Fyers ATP), so when VWAP sits closer to
     * close than any CPR neighbor on the trade-direction side, it wins arming.
     * <ul>
     *   <li>Armed buy = the candidate with the highest level value strictly below close
     *       (closest broken-above level — the one the next dip would naturally retest).</li>
     *   <li>Armed sell = the candidate with the lowest level value strictly above close
     *       (closest broken-below-from-above level — the one the next pop would retest).</li>
     * </ul>
     * If no candidate qualifies, the corresponding side is cleared. Recomputed on every
     * candle close so the armed level tracks where price actually is.
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

        // Buy-side: pick the candidate with the HIGHEST level value strictly BELOW close.
        String buy = null;
        double bestBuyLevel = -Double.MAX_VALUE;
        if (r4    > 0 && close > r4    && r4    > bestBuyLevel) { bestBuyLevel = r4;    buy = "BUY_ABOVE_R4"; }
        if (r3    > 0 && close > r3    && r3    > bestBuyLevel) { bestBuyLevel = r3;    buy = "BUY_ABOVE_R3"; }
        if (r2    > 0 && close > r2    && r2    > bestBuyLevel) { bestBuyLevel = r2;    buy = "BUY_ABOVE_R2"; }
        if (r1ph  > 0 && close > r1ph  && r1ph  > bestBuyLevel) { bestBuyLevel = r1ph;  buy = "BUY_ABOVE_R1_PDH"; }
        if (cprTop > 0 && close > cprTop && cprTop > bestBuyLevel) { bestBuyLevel = cprTop; buy = "BUY_ABOVE_CPR"; }
        if (s1pl  > 0 && close > s1pl  && s1pl  > bestBuyLevel) { bestBuyLevel = s1pl;  buy = "BUY_ABOVE_S1_PDL"; }
        if (s2    > 0 && close > s2    && s2    > bestBuyLevel) { bestBuyLevel = s2;    buy = "BUY_ABOVE_S2"; }
        if (s3    > 0 && close > s3    && s3    > bestBuyLevel) { bestBuyLevel = s3;    buy = "BUY_ABOVE_S3"; }
        if (s4    > 0 && close > s4    && s4    > bestBuyLevel) { bestBuyLevel = s4;    buy = "BUY_ABOVE_S4"; }

        // Sell-side: pick the candidate with the LOWEST level value strictly ABOVE close.
        String sell = null;
        double bestSellLevel = Double.MAX_VALUE;
        if (s4     > 0 && close < s4     && s4     < bestSellLevel) { bestSellLevel = s4;     sell = "SELL_BELOW_S4"; }
        if (s3     > 0 && close < s3     && s3     < bestSellLevel) { bestSellLevel = s3;     sell = "SELL_BELOW_S3"; }
        if (s2     > 0 && close < s2     && s2     < bestSellLevel) { bestSellLevel = s2;     sell = "SELL_BELOW_S2"; }
        if (s1plLo > 0 && close < s1plLo && s1plLo < bestSellLevel) { bestSellLevel = s1plLo; sell = "SELL_BELOW_S1_PDL"; }
        if (cprBot > 0 && close < cprBot && cprBot < bestSellLevel) { bestSellLevel = cprBot; sell = "SELL_BELOW_CPR"; }
        if (r1phLo > 0 && close < r1phLo && r1phLo < bestSellLevel) { bestSellLevel = r1phLo; sell = "SELL_BELOW_R1_PDH"; }
        if (r2     > 0 && close < r2     && r2     < bestSellLevel) { bestSellLevel = r2;     sell = "SELL_BELOW_R2"; }
        if (r3     > 0 && close < r3     && r3     < bestSellLevel) { bestSellLevel = r3;     sell = "SELL_BELOW_R3"; }
        if (r4     > 0 && close < r4     && r4     < bestSellLevel) { bestSellLevel = r4;     sell = "SELL_BELOW_R4"; }

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

    public int getLastScanCount() { return lastScanCount.get(); }
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
        pendingHurdleGuards.clear();
        lastSignal.clear();
        signalHistory.clear();
        levelStateBySymbol.clear();
        lastZoneAppliedEpoch.clear();
        tradedCountToday = 0;
        filteredCountToday = 0;
        lastScanCount.set(0);
        lastScanTime = "";
        lastScanBoundary.set(0);
        saveState();
    }

    @Override
    public void onDailyReset() {
        log.info("[Scanner] Daily reset — clearing signals, broken levels, signal history");
        clearAll();
        eventService.log("[INFO] Scanner daily reset — signals and broken levels cleared for new trading day");
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    public synchronized void saveState() {
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
                sig.put("pattern", entry.getValue().pattern != null ? entry.getValue().pattern : "");
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
                    sig.put("pattern", si.pattern != null ? si.pattern : "");
                    list.add(sig);
                }
                history.put(entry.getKey(), list);
            }
            state.put("signalHistory", history);

            // Save per-level broken state (up/down per CPR level per symbol). Without this,
            // a restart erases the in-memory level-state and the retest-arming flags collapse
            // to false until advanceZoneState walks today's bars again — which can silently
            // miss signals if AtrService hasn't finished re-seeding history yet.
            Map<String, Map<String, Map<String, Boolean>>> levelState = new LinkedHashMap<>();
            for (var symEntry : levelStateBySymbol.entrySet()) {
                Map<String, Map<String, Boolean>> perLevel = new LinkedHashMap<>();
                for (var levelEntry : symEntry.getValue().entrySet()) {
                    Map<String, Boolean> flags = new LinkedHashMap<>();
                    flags.put("up", levelEntry.getValue().up);
                    flags.put("down", levelEntry.getValue().down);
                    perLevel.put(levelEntry.getKey(), flags);
                }
                levelState.put(symEntry.getKey(), perLevel);
            }
            state.put("levelState", levelState);

            // Save watermark so advanceZoneState only re-applies bars closed during downtime.
            state.put("lastZoneAppliedEpoch", new LinkedHashMap<>(lastZoneAppliedEpoch));

            // Save current armed buy/sell setup per symbol. These get recomputed on the next
            // candle close, but persisting them keeps the chip / UI accurate immediately on
            // restart and matches behaviour with the level state.
            state.put("armedBuyLevel", new LinkedHashMap<>(armedBuyLevel));
            state.put("armedSellLevel", new LinkedHashMap<>(armedSellLevel));

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
                    info.pattern = v.has("pattern") ? v.get("pattern").asText() : "";
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
                        si.pattern = node.has("pattern") ? node.get("pattern").asText() : "";
                        list.add(si);
                    });
                    signalHistory.put(entry.getKey(), list);
                });
            }

            // Load counters
            if (root.has("tradedCountToday")) tradedCountToday = root.get("tradedCountToday").asInt();
            if (root.has("filteredCountToday")) filteredCountToday = root.get("filteredCountToday").asInt();

            // Load per-level broken state (up/down per CPR level per symbol).
            JsonNode levelStateNode = root.get("levelState");
            if (levelStateNode != null) {
                levelStateNode.fields().forEachRemaining(symEntry -> {
                    ConcurrentHashMap<String, LevelBrokenState> perLevel = new ConcurrentHashMap<>();
                    symEntry.getValue().fields().forEachRemaining(levelEntry -> {
                        LevelBrokenState s = new LevelBrokenState();
                        JsonNode flags = levelEntry.getValue();
                        s.up   = flags.has("up")   && flags.get("up").asBoolean();
                        s.down = flags.has("down") && flags.get("down").asBoolean();
                        perLevel.put(levelEntry.getKey(), s);
                    });
                    levelStateBySymbol.put(symEntry.getKey(), perLevel);
                });
            }

            // Load watermark — advanceZoneState will only apply bars closed after this.
            JsonNode watermarkNode = root.get("lastZoneAppliedEpoch");
            if (watermarkNode != null) {
                watermarkNode.fields().forEachRemaining(e ->
                    lastZoneAppliedEpoch.put(e.getKey(), e.getValue().asLong()));
            }

            // Load armed buy/sell levels.
            JsonNode armedBuy = root.get("armedBuyLevel");
            if (armedBuy != null) armedBuy.fields().forEachRemaining(e -> armedBuyLevel.put(e.getKey(), e.getValue().asText()));
            JsonNode armedSell = root.get("armedSellLevel");
            if (armedSell != null) armedSell.fields().forEachRemaining(e -> armedSellLevel.put(e.getKey(), e.getValue().asText()));

            log.info("[Scanner] Restored state: {} signals, {} broken levels, {} traded, {} filtered, {} signal histories, {} level-state symbols, {} armed buy, {} armed sell",
                lastSignal.size(), brokenLevels.size(), tradedCountToday, filteredCountToday, signalHistory.size(),
                levelStateBySymbol.size(), armedBuyLevel.size(), armedSellLevel.size());
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
        // Candle pattern stamped by acceptOrRejectProximity when a pattern matched on this
        // scan. Empty for early-gate rejections (ATP / EMA-trend / position-open / etc.)
        // that fire before pattern detection. Stays valid for downstream-filter rejections
        // (HTF opposed, hurdle, etc.) because lastTriggerRoute is only cleared by fireSignal.
        String route = lastTriggerRoute.get(fyersSymbol);
        info.pattern = route != null ? route : "";
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

        // NIFTY opposed (hard reject) — replaces the legacy LPT-downgrade composite bucket.
        if (s.contains("opposes nifty composite") || s.contains("nifty opposed")) return "NIFTY_OPPOSED";

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
        public String pattern;    // candle pattern (HAMMER_RETEST / OUTSIDE_REVERSAL_RETEST / DOJI_RETEST / STAR_RETEST / HARAMI_RETEST / GOOD_SIZE_CANDLE_RETEST), or "" when no pattern matched (early gate rejections)
    }
}
