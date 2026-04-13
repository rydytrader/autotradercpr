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
    private static final ObjectMapper mapper = new ObjectMapper();

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

    // Latest change % per symbol
    private final ConcurrentHashMap<String, Double> latestChangePct = new ConcurrentHashMap<>();

    private final RiskSettingsStore riskSettings;

    // Candle close listeners
    private final List<CandleCloseListener> listeners = new CopyOnWriteArrayList<>();

    // First-bar diagnostic trace counter (per symbol)
    private final ConcurrentHashMap<String, Integer> firstBarTraceCount = new ConcurrentHashMap<>();

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private EventService eventService;

    // Last candle close cycle stats
    private volatile int lastCycleProcessed = 0;
    private volatile String lastCycleTime = "";

    public CandleAggregator(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
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

        long nowMinute = ZonedDateTime.now(IST).toLocalTime().getHour() * 60L
            + ZonedDateTime.now(IST).toLocalTime().getMinute();

        // Always update latestLtp so trend display reflects pre-market price
        latestLtp.put(symbol, ltp);

        // Skip candle aggregation / OR / ATP tracking for pre-market ticks
        if (nowMinute < MarketHolidayService.MARKET_OPEN_MINUTE) return;

        // ── DIAGNOSTIC: first 5-min window tracing ──
        // Log every tick a symbol receives in the 09:15-09:20 window so we can find
        // out why firstCandleClose drifts from the actual session open. Limited to
        // first 30 ticks per symbol per day to bound log volume.
        if (nowMinute < MarketHolidayService.MARKET_OPEN_MINUTE + 5) {
            int seen = firstBarTraceCount.merge(symbol, 1, Integer::sum);
            if (seen <= 30) {
                String time = ZonedDateTime.now(IST).toLocalTime()
                    .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
                eventService.log("[FIRST_BAR_TRACE] " + symbol + " " + time
                    + " ltp=" + ltp + " open=" + raw.open + " high=" + raw.high + " low=" + raw.low
                    + " atp=" + raw.atp + " vol=" + raw.volume + " seq=" + seen);
            }
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

        // Update current forming candle
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        long candleStart = getCandleStartMinute(now);

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
            // Update candle volume as delta from start (uses last known cumulative volume)
            if (cumVol > 0 && existing.volAtStart > 0) {
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
            // Only process during market hours (9:15 AM to 3:30 PM)
            if (nowMinute < MarketHolidayService.MARKET_OPEN_MINUTE || nowMinute > MarketHolidayService.MARKET_CLOSE_MINUTE) return;
            long currentStart = getCandleStartMinute(now);

            int processed = 0;
            for (Map.Entry<String, CandleBar> entry : currentCandles.entrySet()) {
                String symbol = entry.getKey();
                CandleBar candle = entry.getValue();

                // If the current candle belongs to a previous period, finalize it
                if (candle.startMinute < currentStart && candle.open > 0) {
                    finalizeCandle(symbol, candle);
                    processed++;
                    // Reset for new period
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

        // Capture first candle close of the day (only once per symbol per day)
        if (candle.startMinute >= 555 && candle.close > 0) { // 555 = 9:15 AM
            if (firstCandleClose.putIfAbsent(symbol, candle.close) == null) {
                saveOrState();
            }
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

        // Add to completed candles buffer (keep last 30)
        completedCandles.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        Deque<CandleBar> history = completedCandles.get(symbol);
        history.addLast(candle);
        while (history.size() > 30) history.pollFirst();

        // Notify listeners
        for (CandleCloseListener listener : listeners) {
            try {
                listener.onCandleClose(symbol, candle);
            } catch (Exception e) {
                log.error("[CandleAggregator] Listener error for {}: {}", symbol, e.getMessage());
            }
        }
    }

    /**
     * Get the candle start minute for a given time.
     * E.g., for 15-min candles: 09:15→555, 09:30→570, 09:45→585
     */
    private long getCandleStartMinute(LocalTime time) {
        long totalMinutes = time.getHour() * 60L + time.getMinute();
        return (totalMinutes / timeframeMinutes) * timeframeMinutes;
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
        for (CandleBar c : candles) {
            if (c.epochSec >= todayStartEpoch) todays.add(c);
        }
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

    /** Get average volume of the last N completed candles. */
    public double getAvgVolume(String symbol, int periods) {
        Deque<CandleBar> history = completedCandles.get(symbol);
        if (history == null || history.isEmpty()) return 0;
        long sum = 0;
        int count = 0;
        Iterator<CandleBar> it = history.descendingIterator();
        while (it.hasNext() && count < periods) {
            sum += it.next().volume;
            count++;
        }
        return count > 0 ? (double) sum / count : 0;
    }

    /** Clear daily state for new trading day (called automatically on date change). */
    private void clearDaily() {
        currentCandles.clear();
        completedCandles.clear();
        firstBarTraceCount.clear();
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
        public double trueRange(CandleBar prev) {
            if (prev == null) return high - low;
            return Math.max(high - low, Math.max(Math.abs(high - prev.close), Math.abs(low - prev.close)));
        }
    }

}
