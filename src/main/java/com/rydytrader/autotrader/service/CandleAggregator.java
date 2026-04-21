package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.websocket.HsmBinaryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.rydytrader.autotrader.util.FileIoUtils;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * Buffers real-time ticks into fixed-interval candles (default 15 min).
 * On each candle close (clock boundary), notifies the BreakoutScanner.
 */
@Service
public class CandleAggregator {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregator.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String OR_STATE_FILE = "../store/config/or-state.json";
    private static final String CANDLE_HISTORY_FILE = "../store/cache/candle-history.json";
    private static final String LEGACY_PRIORS_FILE  = "../store/cache/candle-priors.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    // true only for the Spring-managed main aggregator. The manually-created htfAggregator
    // leaves this false so it doesn't contend on the same priors cache file.
    private volatile boolean persistPriors = false;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService marketHolidayService;

    // Current forming candle per symbol
    private final ConcurrentHashMap<String, CandleBar> currentCandles = new ConcurrentHashMap<>();

    // Last N completed candles per symbol (rolling buffer for ATR updates)
    private final ConcurrentHashMap<String, Deque<CandleBar>> completedCandles = new ConcurrentHashMap<>();

    // Day open price per symbol
    private final ConcurrentHashMap<String, Double> dayOpen = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> firstCandleClose = new ConcurrentHashMap<>();

    // Opening Range tracking
    private final ConcurrentHashMap<String, Double> openingRangeHigh = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> openingRangeLow = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> openingRangeLocked = new ConcurrentHashMap<>();

    // Latest ATP per symbol (from exchange avg_trade_price)
    private final ConcurrentHashMap<String, Double> latestAtp = new ConcurrentHashMap<>();

    // Latest LTP per symbol
    private final ConcurrentHashMap<String, Double> latestLtp = new ConcurrentHashMap<>();

    // Latest cumulative volume per symbol (for computing candle volume deltas)
    private final ConcurrentHashMap<String, Long> lastCumulativeVol = new ConcurrentHashMap<>();

    // Prior-day candles (newest last) — seeded from multi-day Fyers history, used only for
    // volume average fallback when today's completed candle count is insufficient.
    private final ConcurrentHashMap<String, List<CandleBar>> priorDayCandles = new ConcurrentHashMap<>();

    // Latest change % per symbol
    private final ConcurrentHashMap<String, Double> latestChangePct = new ConcurrentHashMap<>();

    private final RiskSettingsStore riskSettings;

    // Candle close listeners
    private final List<CandleCloseListener> listeners = new CopyOnWriteArrayList<>();

    // Track which symbols have logged their time source (one-time per symbol per day)
    private final Set<String> timeSourceLogged = ConcurrentHashMap.newKeySet();

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private EventService eventService;

    // Last candle close cycle stats
    private volatile int lastCycleProcessed = 0;
    private volatile String lastCycleTime = "";

    public CandleAggregator(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
    }

    /**
     * Runs only for the Spring-managed main aggregator (not the manually-constructed
     * htfAggregator). Loads prior-day candle volumes from disk so the 20-candle volume
     * baseline is available from the first tick of the day without needing a multi-day
     * Fyers fetch on every restart.
     */
    @jakarta.annotation.PostConstruct
    public void initMainInstance() {
        this.persistPriors = true;
        log.info("[CandleAggregator] Main aggregator initialized — priors cache enabled");
        loadPriorsFromDisk();
        // NOTE: intentionally no eager save here. Writing an empty file at boot would poison
        // any good cache loaded during a subsequent restart. Saves fire naturally after
        // seedCandles populates priors and on every candle close.
    }

    // Daily reset tracker
    private volatile String currentTradingDate = "";

    private volatile int timeframeMinutes = 5;

    // Scheduler for clock-boundary candle finalization
    private ScheduledExecutorService scheduler;

    public interface CandleCloseListener {
        void onCandleClose(String fyersSymbol, CandleBar completedCandle);
    }

    public void addListener(CandleCloseListener listener) {
        listeners.add(listener);
    }

    public void setTimeframe(int minutes) {
        this.timeframeMinutes = minutes;
    }

    /**
     * Start the candle close scheduler. Call after market data service starts.
     */
    private ScheduledFuture<?> boundaryCheckerFuture;
    private volatile String lastRestartTime = "";
    private volatile int restartCount = 0;

    public void start() {
        loadOrState();
        boolean isRestart = scheduler != null;
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
        if (isRestart) {
            restartCount++;
            lastRestartTime = java.time.ZonedDateTime.now(IST)
                .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        }
        scheduler = Executors.newSingleThreadScheduledExecutor();

        // Schedule candle check every second — only finalizes at clock boundaries
        boundaryCheckerFuture = scheduler.scheduleAtFixedRate(this::checkCandleBoundary, 1, 1, TimeUnit.SECONDS);
        log.info("[CandleAggregator] Started with {}min timeframe", timeframeMinutes);
    }

    public String getLastRestartTime() { return lastRestartTime; }
    public int getRestartCount() { return restartCount; }

    public int getActiveCandleCount() { return currentCandles.size(); }

    public CandleBar getCurrentCandle(String symbol) { return currentCandles.get(symbol); }

    public int getLastCycleProcessed() { return lastCycleProcessed; }
    public String getLastCycleTime() { return lastCycleTime; }

    /** Check if the candle boundary scheduler is still alive. */
    public boolean isBoundaryCheckerAlive() {
        return boundaryCheckerFuture != null && !boundaryCheckerFuture.isDone() && !boundaryCheckerFuture.isCancelled();
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        currentCandles.clear();
        completedCandles.clear();
        dayOpen.clear();
        firstCandleClose.clear();
        latestAtp.clear();
        latestLtp.clear();
        latestChangePct.clear();
        log.info("[CandleAggregator] Stopped");
    }

    /**
     * Called for every tick from MarketDataService.
     */
    public void onTick(HsmBinaryParser.RawTick raw) {
        if (raw.fyersSymbol == null || raw.fyersSymbol.isEmpty()) return;
        String symbol = raw.fyersSymbol;
        double ltp = raw.ltp;
        if (ltp <= 0) return;

        long nowMinute;
        if (raw.exchFeedTime > 0) {
            LocalTime t = Instant.ofEpochSecond(raw.exchFeedTime).atZone(IST).toLocalTime();
            nowMinute = t.getHour() * 60L + t.getMinute();
        } else {
            LocalTime t = ZonedDateTime.now(IST).toLocalTime();
            nowMinute = t.getHour() * 60L + t.getMinute();
        }

        // Always update latestLtp so trend display reflects pre-market price
        latestLtp.put(symbol, ltp);

        // Skip candle aggregation / OR / ATP tracking outside NSE session (pre-market + post-close).
        // The session runs 09:15:00 to 15:29:59 inclusive — a tick at 15:30:00 belongs to the
        // post-close window and shouldn't extend today's chart or create a phantom 15:30 candle.
        if (nowMinute < MarketHolidayService.MARKET_OPEN_MINUTE
            || nowMinute >= MarketHolidayService.MARKET_CLOSE_MINUTE) return;

        // One-time log per symbol: which time source is used for candle assignment
        if (timeSourceLogged.add(symbol)) {
            String src = raw.exchFeedTime > 0 ? "exchange feed time (exchFeedTime=" + raw.exchFeedTime + ")" : "system clock fallback (exchFeedTime=0)";
            log.info("[CandleAggregator] {} using {} for candle assignment", symbol, src);
        }

        if (raw.changePercent != 0) latestChangePct.put(symbol, raw.changePercent);

        // Track day open from HSM open_price field
        if (raw.open > 0) dayOpen.putIfAbsent(symbol, raw.open);

        // Track Opening Range high/low from live ticks
        int orMinutes = riskSettings.getOpeningRangeMinutes();
        if (orMinutes > 0 && !openingRangeLocked.getOrDefault(symbol, false)) {
            long orEnd = MarketHolidayService.MARKET_OPEN_MINUTE + orMinutes;
            if (nowMinute >= MarketHolidayService.MARKET_OPEN_MINUTE && nowMinute < orEnd) {
                openingRangeHigh.merge(symbol, ltp, Math::max);
                openingRangeLow.merge(symbol, ltp, Math::min);
            } else if (nowMinute >= orEnd) {
                // OR window passed — if we have data, lock it; if not (late start), build from day high/low
                if (!openingRangeHigh.containsKey(symbol) && raw.high > 0 && raw.low > 0) {
                    openingRangeHigh.put(symbol, raw.high);
                    openingRangeLow.put(symbol, raw.low);
                    log.info("[CandleAggregator] {} OR late-start fallback: using day high/low H={} L={}",
                        symbol, raw.high, raw.low);
                }
                openingRangeLocked.put(symbol, true);
                saveOrState();
            }
        }

        // Track ATP from exchange avg_trade_price
        if (raw.atp > 0) latestAtp.put(symbol, raw.atp);

        // Track cumulative volume — only update when Fyers sends a non-zero value
        // (delta updates may not include volume, so we keep the last known value)
        long tickVol = raw.volume;
        if (tickVol > 0) {
            long prevCumVol = lastCumulativeVol.getOrDefault(symbol, 0L);
            // Detect WebSocket reconnect: if new cumVol is less than previous, reset candle volAtStart
            if (prevCumVol > 0 && tickVol < prevCumVol) {
                CandleBar existing = currentCandles.get(symbol);
                if (existing != null) {
                    existing.volAtStart = tickVol - existing.volume;
                    if (existing.volAtStart < 0) existing.volAtStart = tickVol;
                }
            }
            lastCumulativeVol.put(symbol, tickVol);
        }
        long cumVol = lastCumulativeVol.getOrDefault(symbol, 0L);

        // Use exchange timestamp for candle assignment (falls back to system clock)
        LocalTime tickTime;
        if (raw.exchFeedTime > 0) {
            tickTime = Instant.ofEpochSecond(raw.exchFeedTime).atZone(IST).toLocalTime();
        } else {
            tickTime = ZonedDateTime.now(IST).toLocalTime();
        }
        long candleStart = getCandleStartMinute(tickTime);

        currentCandles.compute(symbol, (k, existing) -> {
            if (existing == null || existing.startMinute != candleStart) {
                // Finalize the old candle before replacing (prevents race with boundary checker)
                if (existing != null && existing.open > 0) {
                    finalizeCandle(symbol, existing);
                }
                // New candle period — start fresh
                CandleBar c = new CandleBar();
                c.startMinute = candleStart;
                c.epochSec = ZonedDateTime.now(IST).toLocalDate().atStartOfDay(IST).toEpochSecond() + candleStart * 60L;
                c.open = ltp;
                c.high = ltp;
                c.low = ltp;
                c.close = ltp;
                c.volAtStart = cumVol;
                return c;
            }
            // Update existing candle
            if (ltp > existing.high) existing.high = ltp;
            if (ltp < existing.low) existing.low = ltp;
            existing.close = ltp;
            // Adjust volAtStart on first tick after mid-candle restart
            if (existing.volAtStart == -1 && cumVol > 0) {
                existing.volAtStart = cumVol - existing.volume;
            }
            // Update candle volume as delta from start (uses last known cumulative volume).
            // volAtStart == 0 is a LEGITIMATE starting value for illiquid stocks whose first
            // WS tick of the day arrives before any trade has executed. Using `> 0` here
            // caused those candles to stay at 0 volume forever. volAtStart == -1 is the
            // sentinel for mid-candle restart; it gets rewritten above before this guard.
            if (cumVol > 0 && existing.volAtStart >= 0) {
                existing.volume = cumVol - existing.volAtStart;
            }
            return existing;
        });
    }

    /**
     * Check if we've crossed a candle boundary and finalize candles.
     */
    private void checkCandleBoundary() {
        try {
            // Daily reset: detect new trading day and clear stale data
            String today = ZonedDateTime.now(IST).toLocalDate().toString();
            if (!today.equals(currentTradingDate) && !currentTradingDate.isEmpty()) {
                log.info("[CandleAggregator] New trading day detected ({} → {}), clearing daily state", currentTradingDate, today);
                clearDaily();
            }
            currentTradingDate = today;

            LocalTime now = ZonedDateTime.now(IST).toLocalTime();
            long nowMinute = now.getHour() * 60L + now.getMinute();
            // Run during market hours AND briefly past close to finalise the last 15:25 candle
            // at the 15:30 boundary. Post-15:30 new-candle creation is suppressed in the loop below.
            if (nowMinute < MarketHolidayService.MARKET_OPEN_MINUTE || nowMinute > MarketHolidayService.MARKET_CLOSE_MINUTE) return;
            long currentStart = getCandleStartMinute(now);

            int processed = 0;
            for (Map.Entry<String, CandleBar> entry : currentCandles.entrySet()) {
                String symbol = entry.getKey();
                CandleBar candle = entry.getValue();

                // If the current candle belongs to a previous period, finalize it.
                // Skip creating the NEW period if it would start at or after the close boundary
                // (e.g. a 15:30+ bucket when we've already stopped aggregating past 15:29:59).
                if (candle.startMinute < currentStart && candle.open > 0) {
                    finalizeCandle(symbol, candle);
                    processed++;
                    if (currentStart >= MarketHolidayService.MARKET_CLOSE_MINUTE) {
                        currentCandles.remove(symbol);  // no post-close bucket; clear the slot
                    } else {
                        double ltp = candle.close;
                        CandleBar newCandle = new CandleBar();
                        newCandle.startMinute = currentStart;
                        newCandle.epochSec = ZonedDateTime.now(IST).toLocalDate().atStartOfDay(IST).toEpochSecond() + currentStart * 60L;
                        newCandle.open = ltp;
                        newCandle.high = ltp;
                        newCandle.low = ltp;
                        newCandle.close = ltp;
                        currentCandles.put(symbol, newCandle);
                    }
                }
            }
            if (processed > 0) {
                lastCycleProcessed = processed;
                lastCycleTime = now.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm"));
            }
        } catch (Throwable e) {
            // Catch Throwable (not just Exception) to prevent ScheduledExecutorService from dying
            log.error("[CandleAggregator] Error in candle boundary check: {}", e.getMessage(), e);
        }
    }

    // Track last finalized candle per symbol to prevent double finalization
    private final ConcurrentHashMap<String, Long> lastFinalizedMinute = new ConcurrentHashMap<>();

    private void finalizeCandle(String symbol, CandleBar candle) {
        // Prevent double finalization (race between onTick and checkCandleBoundary)
        Long prev = lastFinalizedMinute.put(symbol, candle.startMinute);
        if (prev != null && prev == candle.startMinute) return; // already finalized

        // Snapshot VWAP (Fyers exchange-provided ATP) on the candle
        Double atpAtClose = latestAtp.get(symbol);
        if (atpAtClose != null && atpAtClose > 0) candle.vwap = atpAtClose;

        // Capture first candle close of the day — only the actual 9:15-9:20 candle is the writer.
        // Strict equality on startMinute means no later candle (post-restart, mid-day) can poison
        // this value. Uses put() rather than putIfAbsent so the legitimate 9:20 close always wins
        // over any earlier wrong value loaded from disk or set by seedCandles' partial bar.
        if (candle.startMinute == 555 && candle.close > 0) { // 555 = 9:15 AM, 9:15-9:20 candle
            firstCandleClose.put(symbol, candle.close);
            saveOrState();
        }

        // Update Opening Range from completed candle high/low
        int orMinutes = riskSettings.getOpeningRangeMinutes();
        if (orMinutes > 0 && !openingRangeLocked.getOrDefault(symbol, false)) {
            long orEnd = MarketHolidayService.MARKET_OPEN_MINUTE + orMinutes;
            if (candle.startMinute >= MarketHolidayService.MARKET_OPEN_MINUTE && candle.startMinute < orEnd) {
                if (candle.high > 0) openingRangeHigh.merge(symbol, candle.high, Math::max);
                if (candle.low > 0) openingRangeLow.merge(symbol, candle.low, Math::min);
            }
            // Lock OR when candle starts at or after OR end
            if (candle.startMinute >= orEnd) {
                openingRangeLocked.put(symbol, true);
                saveOrState();
            }
        }

        // Add to completed candles buffer. Cap must cover a full trading session for
        // DH/DL breakouts to see the true day high/low — 6h15m / 3-min = 125 candles,
        // so 128 is safe for any timeframe ≥ 3 min. (Previously capped at 30, which
        // evicted morning candles after ~2.5h and produced false DH/DL breakouts in
        // the afternoon.)
        completedCandles.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        Deque<CandleBar> history = completedCandles.get(symbol);
        history.addLast(candle);
        while (history.size() > 128) history.pollFirst();

        // Notify listeners
        for (CandleCloseListener listener : listeners) {
            try {
                listener.onCandleClose(symbol, candle);
            } catch (Exception e) {
                log.error("[CandleAggregator] Listener error for {}: {}", symbol, e.getMessage());
            }
        }

        // Persist priors + today's completedCandles on every close so a mid-day crash
        // restart can skip the Fyers catch-up fetch for CandleAggregator state.
        savePriorsToDisk();
    }

    /**
     * Get the candle start minute for a given time, session-aligned to 9:15 (market open).
     * For 15-min: 09:15→555, 09:30→570, 09:45→585.
     * For 60-min: 09:15→555, 10:15→615, 11:15→675 … 15:15→915 (partial 15-min tail).
     * For 75-min: 09:15→555, 10:30→630, 11:45→705, 13:00→780, 14:15→855 (partial 75-min tail).
     * Pre-market ticks (if any) fall back to floor-division from midnight.
     */
    private long getCandleStartMinute(LocalTime time) {
        long totalMinutes = time.getHour() * 60L + time.getMinute();
        long marketOpen = MarketHolidayService.MARKET_OPEN_MINUTE;
        if (totalMinutes < marketOpen) {
            return (totalMinutes / timeframeMinutes) * timeframeMinutes;
        }
        long offset = totalMinutes - marketOpen;
        return marketOpen + (offset / timeframeMinutes) * timeframeMinutes;
    }

    // ── Public accessors ──────────────────────────────────────────────────────

    public double getDayOpen(String symbol) {
        return dayOpen.getOrDefault(symbol, 0.0);
    }

    /** Day high from completed candles, excluding the given candle (used for DH breakout detection). */
    public double getDayHighExcluding(String symbol, CandleBar excluding) {
        java.util.Deque<CandleBar> candles = completedCandles.get(symbol);
        if (candles == null || candles.isEmpty()) return 0;
        double max = 0;
        for (CandleBar c : candles) {
            if (c == excluding) continue;
            if (c.high > max) max = c.high;
        }
        return max;
    }

    /** Day low from completed candles, excluding the given candle (used for DL breakout detection). */
    public double getDayLowExcluding(String symbol, CandleBar excluding) {
        java.util.Deque<CandleBar> candles = completedCandles.get(symbol);
        if (candles == null || candles.isEmpty()) return 0;
        double min = Double.MAX_VALUE;
        for (CandleBar c : candles) {
            if (c == excluding) continue;
            if (c.low > 0 && c.low < min) min = c.low;
        }
        return min == Double.MAX_VALUE ? 0 : min;
    }

    /** Day high excluding the most recent completed candle (for target shift after a breakout). */
    public double getDayHighBeforeLast(String symbol) {
        java.util.Deque<CandleBar> candles = completedCandles.get(symbol);
        if (candles == null || candles.isEmpty()) return 0;
        return getDayHighExcluding(symbol, candles.peekLast());
    }

    /** Day low excluding the most recent completed candle (for target shift after a breakout). */
    public double getDayLowBeforeLast(String symbol) {
        java.util.Deque<CandleBar> candles = completedCandles.get(symbol);
        if (candles == null || candles.isEmpty()) return 0;
        return getDayLowExcluding(symbol, candles.peekLast());
    }

    /** Get the close price of the first completed candle of the day for a symbol. */
    public double getFirstCandleClose(String symbol) {
        return firstCandleClose.getOrDefault(symbol, 0.0);
    }

    /** Number of symbols that have a captured first-candle close (should match watchlist after 9:20). */
    public int getFirstCandleCloseCount() {
        return (int) firstCandleClose.values().stream().filter(v -> v > 0).count();
    }

    /** Opening Range high (highest price in first N minutes). */
    public double getOpeningRangeHigh(String symbol) {
        return openingRangeHigh.getOrDefault(symbol, 0.0);
    }

    /** Opening Range low (lowest price in first N minutes). */
    public double getOpeningRangeLow(String symbol) {
        return openingRangeLow.getOrDefault(symbol, 0.0);
    }

    /** Whether the Opening Range period has completed and range is locked. */
    public boolean isOpeningRangeLocked(String symbol) {
        return openingRangeLocked.getOrDefault(symbol, false);
    }

    // ── OR State Persistence ─────────────────────────────────────────────────
    public void saveOrState() {
        try {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("date", ZonedDateTime.now(IST).toLocalDate().toString());

            Map<String, Double> fcMap = new LinkedHashMap<>(firstCandleClose);
            state.put("firstCandleClose", fcMap);

            Map<String, Double> orHighMap = new LinkedHashMap<>(openingRangeHigh);
            state.put("openingRangeHigh", orHighMap);

            Map<String, Double> orLowMap = new LinkedHashMap<>(openingRangeLow);
            state.put("openingRangeLow", orLowMap);

            Map<String, Boolean> orLockedMap = new LinkedHashMap<>(openingRangeLocked);
            state.put("openingRangeLocked", orLockedMap);

            Map<String, Double> dayOpenMap = new LinkedHashMap<>(dayOpen);
            state.put("dayOpen", dayOpenMap);

            Files.writeString(Paths.get(OR_STATE_FILE),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
        } catch (Exception e) {
            log.error("[CandleAggregator] Failed to save OR state: {}", e.getMessage());
        }
    }

    public void loadOrState() {
        try {
            Path path = Paths.get(OR_STATE_FILE);
            if (!Files.exists(path)) return;

            JsonNode root = mapper.readTree(Files.readString(path));
            String savedDate = root.has("date") ? root.get("date").asText() : "";
            String today = ZonedDateTime.now(IST).toLocalDate().toString();
            if (!today.equals(savedDate)) {
                log.info("[CandleAggregator] OR state from {} — stale, starting fresh", savedDate);
                return;
            }

            JsonNode fcNode = root.get("firstCandleClose");
            if (fcNode != null) {
                fcNode.fields().forEachRemaining(e -> firstCandleClose.putIfAbsent(e.getKey(), e.getValue().asDouble()));
            }
            JsonNode orHNode = root.get("openingRangeHigh");
            if (orHNode != null) {
                orHNode.fields().forEachRemaining(e -> openingRangeHigh.putIfAbsent(e.getKey(), e.getValue().asDouble()));
            }
            JsonNode orLNode = root.get("openingRangeLow");
            if (orLNode != null) {
                orLNode.fields().forEachRemaining(e -> openingRangeLow.putIfAbsent(e.getKey(), e.getValue().asDouble()));
            }
            JsonNode orLockedNode = root.get("openingRangeLocked");
            if (orLockedNode != null) {
                orLockedNode.fields().forEachRemaining(e -> openingRangeLocked.putIfAbsent(e.getKey(), e.getValue().asBoolean()));
            }
            JsonNode dayOpenNode = root.get("dayOpen");
            if (dayOpenNode != null) {
                dayOpenNode.fields().forEachRemaining(e -> dayOpen.putIfAbsent(e.getKey(), e.getValue().asDouble()));
            }

            log.info("[CandleAggregator] Loaded OR state: {} symbols, OR locked: {}",
                firstCandleClose.size(), openingRangeLocked.values().stream().filter(b -> b).count());
        } catch (Exception e) {
            log.error("[CandleAggregator] Failed to load OR state: {}", e.getMessage());
        }
    }

    // ── Prior-day candles cache (for 20-bar volume baseline) ────────────────────────
    //
    // Without this, every restart needs a multi-day Fyers fetch just to populate the
    // volume baseline. Persisting priorDayCandles means warm restarts need only today's
    // catch-up (1 day) and the baseline survives across weekends / long gaps.

    public synchronized void savePriorsToDisk() {
        if (!persistPriors) return;  // only the main aggregator persists
        // Don't overwrite a good cache with an empty one. If both maps are empty (e.g. the
        // eager save fires before seedCandles has run), skip — the next non-empty save after
        // login / candle close will produce a real snapshot.
        if (priorDayCandles.isEmpty() && completedCandles.isEmpty()) return;
        try {
            Path path = Paths.get(CANDLE_HISTORY_FILE);
            Files.createDirectories(path.getParent());
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("savedAt", Instant.now().atZone(IST).toString());
            data.put("priors", priorDayCandles);
            // Today's completed candles — persisted on every close so mid-day crash recovers
            // without a Fyers catch-up fetch for day high/low, volume averaging, etc.
            Map<String, List<CandleBar>> todayMap = new LinkedHashMap<>();
            for (Map.Entry<String, Deque<CandleBar>> e : completedCandles.entrySet()) {
                if (e.getValue() == null || e.getValue().isEmpty()) continue;
                todayMap.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
            data.put("today", todayMap);
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(data));
            FileIoUtils.atomicMoveWithRetry(tmp, path);
            if (log.isDebugEnabled()) {
                log.debug("[CandleAggregator] Saved priors cache — {} priors, {} today", priorDayCandles.size(), todayMap.size());
            }
        } catch (Exception e) {
            log.error("[CandleAggregator] Failed to save priors cache: {}", e.getMessage(), e);
        }
    }

    public synchronized void loadPriorsFromDisk() {
        try {
            Path path = Paths.get(CANDLE_HISTORY_FILE);
            // Migrate the old file name on first boot post-rename.
            if (!Files.exists(path)) {
                Path legacy = Paths.get(LEGACY_PRIORS_FILE);
                if (Files.exists(legacy)) {
                    try {
                        Files.createDirectories(path.getParent());
                        Files.move(legacy, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        log.info("[MIGRATE] Renamed {} -> {}", legacy, path);
                    } catch (Exception e) {
                        log.warn("[MIGRATE] Failed to rename {}: {}", legacy, e.getMessage());
                        return;
                    }
                } else {
                    return;
                }
            }
            JsonNode root = mapper.readTree(Files.readString(path));
            String savedAtStr = root.has("savedAt") ? root.get("savedAt").asText("") : "";
            if (savedAtStr.isEmpty()) return;
            java.time.LocalDate cacheDate;
            try { cacheDate = java.time.LocalDate.parse(savedAtStr.substring(0, 10)); }
            catch (Exception e) { return; }
            java.time.LocalDate today = java.time.LocalDate.now(IST);
            java.time.LocalDate lastTradingDay = marketHolidayService != null
                ? marketHolidayService.getLastTradingDay() : today;
            if (!cacheDate.equals(today) && !cacheDate.equals(lastTradingDay)) {
                log.info("[CandleAggregator] Priors cache stale (cacheDate={}, today={}, lastTradingDay={}) — will rebuild on next seed",
                    cacheDate, today, lastTradingDay);
                return;
            }
            // Priors (prior-day bars) — load unconditionally if cache is fresh.
            // Also accept the legacy "bySymbol" field name from the pre-v2 cache layout.
            JsonNode priorsNode = root.get("priors");
            if (priorsNode == null) priorsNode = root.get("bySymbol");
            int priorsCount = 0;
            if (priorsNode != null && priorsNode.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = priorsNode.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    JsonNode arr = e.getValue();
                    if (arr == null || !arr.isArray()) continue;
                    List<CandleBar> bars = deserializeBars(arr);
                    if (!bars.isEmpty()) {
                        priorDayCandles.put(e.getKey(), bars);
                        priorsCount++;
                    }
                }
            }
            // Today's completed candles — load ONLY if cache was from today. If it's from
            // last trading day, today's bars are stale (market already rolled over).
            int todayCount = 0;
            if (cacheDate.equals(today)) {
                JsonNode todayNode = root.get("today");
                if (todayNode != null && todayNode.isObject()) {
                    Iterator<Map.Entry<String, JsonNode>> it = todayNode.fields();
                    while (it.hasNext()) {
                        Map.Entry<String, JsonNode> e = it.next();
                        JsonNode arr = e.getValue();
                        if (arr == null || !arr.isArray()) continue;
                        List<CandleBar> bars = deserializeBars(arr);
                        if (!bars.isEmpty()) {
                            Deque<CandleBar> deque = completedCandles.computeIfAbsent(e.getKey(), k -> new ArrayDeque<>());
                            synchronized (deque) { deque.clear(); for (CandleBar b : bars) deque.addLast(b); }
                            todayCount++;
                        }
                    }
                }
            }
            if (priorsCount > 0 || todayCount > 0) {
                log.info("[CACHE] Candle state restored from cache (savedAt={}, priors={}, todayCandles={})",
                    savedAtStr, priorsCount, todayCount);
            }
        } catch (Exception e) {
            log.error("[CandleAggregator] Failed to load priors cache: {}", e.getMessage());
        }
    }

    private List<CandleBar> deserializeBars(JsonNode arr) {
        List<CandleBar> bars = new ArrayList<>();
        if (arr == null || !arr.isArray()) return bars;
        for (JsonNode b : arr) {
            try { bars.add(mapper.treeToValue(b, CandleBar.class)); }
            catch (Exception ignored) {}
        }
        return bars;
    }

    public double getAtp(String symbol) {
        return latestAtp.getOrDefault(symbol, 0.0);
    }

    public double getLtp(String symbol) {
        return latestLtp.getOrDefault(symbol, 0.0);
    }

    public double getChangePct(String symbol) {
        return latestChangePct.getOrDefault(symbol, 0.0);
    }

    /**
     * Get last completed candle for a symbol (for breakout comparison).
     */
    public CandleBar getLastCompletedCandle(String symbol) {
        Deque<CandleBar> history = completedCandles.get(symbol);
        if (history == null || history.isEmpty()) return null;
        return history.peekLast();
    }

    /**
     * Get the previous completed candle (the one before the last).
     */
    public CandleBar getPreviousCandle(String symbol) {
        Deque<CandleBar> history = completedCandles.get(symbol);
        if (history == null || history.size() < 2) return null;
        Iterator<CandleBar> it = history.descendingIterator();
        it.next(); // skip last
        return it.next();
    }

    /**
     * Get all completed candles for ATR calculation.
     */
    public List<CandleBar> getCompletedCandles(String symbol) {
        Deque<CandleBar> history = completedCandles.get(symbol);
        if (history == null) return Collections.emptyList();
        return new ArrayList<>(history);
    }

    /** Prior-day candles kept for volume-avg fallback and chart EMA warmup (ordered oldest → newest). */
    public List<CandleBar> getPriorDayCandles(String symbol) {
        List<CandleBar> priors = priorDayCandles.get(symbol);
        if (priors == null) return Collections.emptyList();
        return new ArrayList<>(priors);
    }

    /**
     * Seed completed candles from historical data (on startup/restart).
     */
    public void seedCandles(String symbol, List<CandleBar> candles) {
        Deque<CandleBar> history = completedCandles.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        history.clear();

        if (candles.isEmpty()) return;

        // Only today's bars belong in completedCandles — multi-day history is for ATR computation only.
        long todayStartEpoch = ZonedDateTime.now(IST).toLocalDate().atStartOfDay(IST).toEpochSecond();
        List<CandleBar> todays = new ArrayList<>();
        List<CandleBar> priors = new ArrayList<>();
        for (CandleBar c : candles) {
            if (c.epochSec >= todayStartEpoch) todays.add(c);
            else priors.add(c);
        }

        // Keep last N prior-day candles for volume-avg fallback + 50 SMA chart warmup (ordered oldest → newest)
        int keepPriors = 50;
        if (priors.size() > keepPriors) priors = priors.subList(priors.size() - keepPriors, priors.size());
        priorDayCandles.put(symbol, new ArrayList<>(priors));
        // Save refreshed priors so next restart can skip the multi-day history fetch for the baseline.
        savePriorsToDisk();

        if (todays.isEmpty()) return;

        // Check if the last today-bar is the current forming period (mid-candle restart)
        long currentStart = getCandleStartMinute(ZonedDateTime.now(IST).toLocalTime());
        CandleBar lastCandle = todays.get(todays.size() - 1);

        if (lastCandle.startMinute == currentStart) {
            lastCandle.volAtStart = -1; // sentinel: needs adjustment on first tick
            currentCandles.put(symbol, lastCandle);
            for (int i = 0; i < todays.size() - 1; i++) {
                history.addLast(todays.get(i));
            }
        } else {
            for (CandleBar c : todays) {
                history.addLast(c);
            }
        }

        // Authoritatively (re)seed first-candle close, day open, and opening range from
        // historical bars — these are the source of truth, overriding any earlier value
        // that may have been captured incorrectly during a tick race at session start.
        CandleBar firstBar = todays.get(0);
        if (firstBar.startMinute >= MarketHolidayService.MARKET_OPEN_MINUTE && firstBar.close > 0) {
            firstCandleClose.put(symbol, firstBar.close);
            if (firstBar.open > 0) dayOpen.put(symbol, firstBar.open);
        }
        int orMinutes = riskSettings.getOpeningRangeMinutes();
        if (orMinutes > 0) {
            long orEnd = MarketHolidayService.MARKET_OPEN_MINUTE + orMinutes;
            double orHi = 0, orLo = Double.MAX_VALUE;
            for (CandleBar c : todays) {
                if (c.startMinute >= MarketHolidayService.MARKET_OPEN_MINUTE && c.startMinute < orEnd) {
                    if (c.high > orHi) orHi = c.high;
                    if (c.low > 0 && c.low < orLo) orLo = c.low;
                }
            }
            if (orHi > 0 && orLo < Double.MAX_VALUE) {
                openingRangeHigh.put(symbol, orHi);
                openingRangeLow.put(symbol, orLo);
                long nowMinute = ZonedDateTime.now(IST).toLocalTime().getHour() * 60L
                    + ZonedDateTime.now(IST).toLocalTime().getMinute();
                if (nowMinute >= orEnd) openingRangeLocked.put(symbol, true);
            }
        }
        saveOrState();
    }

    /** Get volume of the current forming candle (live, updates every tick). */
    public long getCurrentCandleVolume(String symbol) {
        CandleBar c = currentCandles.get(symbol);
        return c != null ? c.volume : 0;
    }

    /** Get average volume of the last N candles — uses today's completed candles first,
     *  then backfills with prior-day history so average is available from market open. */
    public double getAvgVolume(String symbol, int periods) {
        long sum = 0;
        int count = 0;
        Deque<CandleBar> today = completedCandles.get(symbol);
        if (today != null) {
            Iterator<CandleBar> it = today.descendingIterator();
            while (it.hasNext() && count < periods) {
                sum += it.next().volume;
                count++;
            }
        }
        if (count < periods) {
            List<CandleBar> prior = priorDayCandles.get(symbol);
            if (prior != null && !prior.isEmpty()) {
                for (int i = prior.size() - 1; i >= 0 && count < periods; i--) {
                    sum += prior.get(i).volume;
                    count++;
                }
            }
        }
        return count > 0 ? (double) sum / count : 0;
    }

    /** Clear daily state for new trading day (called automatically on date change). */
    private void clearDaily() {
        // Roll today's completed candles into priorDayCandles before clearing.
        // Keeps the volume-avg baseline fresh without requiring bot restart.
        for (Map.Entry<String, Deque<CandleBar>> entry : completedCandles.entrySet()) {
            Deque<CandleBar> today = entry.getValue();
            if (today == null || today.isEmpty()) continue;
            List<CandleBar> priors = priorDayCandles.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
            priors.addAll(today);
            int keepPriors = 50;
            if (priors.size() > keepPriors) {
                priorDayCandles.put(entry.getKey(), new ArrayList<>(priors.subList(priors.size() - keepPriors, priors.size())));
            }
        }
        // Persist the refreshed priors before clearing today's state, so a restart tomorrow
        // morning (or later today) picks up the volume baseline without re-fetching Fyers.
        savePriorsToDisk();
        currentCandles.clear();
        completedCandles.clear();
        timeSourceLogged.clear();
        dayOpen.clear();
        firstCandleClose.clear();
        openingRangeHigh.clear();
        openingRangeLow.clear();
        openingRangeLocked.clear();
        latestAtp.clear();
        latestLtp.clear();
        latestChangePct.clear();
        lastCumulativeVol.clear();
        lastCycleProcessed = 0;
        lastCycleTime = "";
        // Notify listeners to reset their daily state
        for (CandleCloseListener listener : listeners) {
            try {
                if (listener instanceof DailyResetListener) {
                    ((DailyResetListener) listener).onDailyReset();
                }
            } catch (Exception e) {
                log.error("[CandleAggregator] Daily reset error for listener: {}", e.getMessage());
            }
        }
    }

    /** Interface for listeners that need daily reset notification. */
    public interface DailyResetListener {
        void onDailyReset();
    }

    /** Clear all state for end of day. */
    public void clearAll() {
        currentCandles.clear();
        completedCandles.clear();
        dayOpen.clear();
        firstCandleClose.clear();
        latestAtp.clear();
        latestLtp.clear();
        latestChangePct.clear();
        lastCumulativeVol.clear();
    }

    // ── Candle bar data class ─────────────────────────────────────────────────

    public static class CandleBar {
        public long startMinute; // minutes since midnight
        public long epochSec;    // bar start epoch seconds (for date filtering across multi-day history)
        public double open;
        public double high;
        public double low;
        public double close;
        public long volume;      // candle volume (delta of cumulative vol)
        public long volAtStart;  // cumulative vol when candle opened (internal)
        // Snapshot of indicator values at candle close (populated by CandleAggregator.finalizeCandle + EmaService.onCandleClose + AtrService.onCandleClose)
        public double vwap;      // exchange-provided ATP at candle close (Fyers)
        public double ema20;     // 20-period EMA after this candle was included
        public double ema50;     // 50-period EMA after this candle was included
        public double ema200;    // 200-period EMA after this candle was included
        public double atr;       // ATR after this candle was included (Wilder-smoothed)
        public String emaPattern; // "RAILWAY_UP" (R-RTP), "RAILWAY_DOWN" (F-RTP), "BRAIDED" (ZIG ZAG), or "" at close
        public double trueRange(CandleBar prev) {
            if (prev == null) return high - low;
            return Math.max(high - low, Math.max(Math.abs(high - prev.close), Math.abs(low - prev.close)));
        }
    }

}
