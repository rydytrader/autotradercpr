package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.websocket.HsmBinaryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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

    // Current forming candle per symbol
    private final ConcurrentHashMap<String, CandleBar> currentCandles = new ConcurrentHashMap<>();

    // Last N completed candles per symbol (rolling buffer for ATR updates)
    private final ConcurrentHashMap<String, Deque<CandleBar>> completedCandles = new ConcurrentHashMap<>();

    // Day open price per symbol
    private final ConcurrentHashMap<String, Double> dayOpen = new ConcurrentHashMap<>();

    // Latest VWAP per symbol (from exchange avg_trade_price)
    private final ConcurrentHashMap<String, Double> latestVwap = new ConcurrentHashMap<>();

    // Latest LTP per symbol
    private final ConcurrentHashMap<String, Double> latestLtp = new ConcurrentHashMap<>();

    // Latest cumulative volume per symbol (for computing candle volume deltas)
    private final ConcurrentHashMap<String, Long> lastCumulativeVol = new ConcurrentHashMap<>();

    // Latest change % per symbol
    private final ConcurrentHashMap<String, Double> latestChangePct = new ConcurrentHashMap<>();

    // Candle close listeners
    private final List<CandleCloseListener> listeners = new CopyOnWriteArrayList<>();

    // Last candle close cycle stats
    private volatile int lastCycleProcessed = 0;
    private volatile String lastCycleTime = "";

    // Daily reset tracker
    private volatile String currentTradingDate = "";

    private volatile int timeframeMinutes = 15;

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

    public void start() {
        if (scheduler != null && !scheduler.isShutdown()) scheduler.shutdownNow();
        scheduler = Executors.newSingleThreadScheduledExecutor();

        // Schedule candle check every second — only finalizes at clock boundaries
        boundaryCheckerFuture = scheduler.scheduleAtFixedRate(this::checkCandleBoundary, 1, 1, TimeUnit.SECONDS);
        log.info("[CandleAggregator] Started with {}min timeframe", timeframeMinutes);
    }

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
        latestVwap.clear();
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

        // Ignore pre-market ticks (before 9:15 AM)
        long nowMinute = ZonedDateTime.now(IST).toLocalTime().getHour() * 60L
            + ZonedDateTime.now(IST).toLocalTime().getMinute();
        if (nowMinute < MarketHolidayService.MARKET_OPEN_MINUTE) return;

        latestLtp.put(symbol, ltp);
        if (raw.changePercent != 0) latestChangePct.put(symbol, raw.changePercent);

        // Track day open from HSM open_price field
        if (raw.open > 0) dayOpen.putIfAbsent(symbol, raw.open);

        // Track VWAP from exchange avg_trade_price
        if (raw.vwap > 0) latestVwap.put(symbol, raw.vwap);

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
                // New candle period — start fresh
                CandleBar c = new CandleBar();
                c.startMinute = candleStart;
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

    private void finalizeCandle(String symbol, CandleBar candle) {
        // Add to completed candles buffer (keep last 20)
        completedCandles.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        Deque<CandleBar> history = completedCandles.get(symbol);
        history.addLast(candle);
        while (history.size() > 20) history.pollFirst();

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

    public double getVwap(String symbol) {
        return latestVwap.getOrDefault(symbol, 0.0);
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

        // Check if the last candle is the current forming period (mid-candle restart)
        long currentStart = getCandleStartMinute(ZonedDateTime.now(IST).toLocalTime());
        CandleBar lastCandle = candles.get(candles.size() - 1);

        if (lastCandle.startMinute == currentStart) {
            // Last candle is partial (current period) — seed as current forming candle
            // volAtStart will be adjusted on first tick (see onTick)
            lastCandle.volAtStart = -1; // sentinel: needs adjustment on first tick
            currentCandles.put(symbol, lastCandle);
            // Add all except the last to completed history
            for (int i = 0; i < candles.size() - 1; i++) {
                history.addLast(candles.get(i));
            }
        } else {
            // All candles are completed
            for (CandleBar c : candles) {
                history.addLast(c);
            }
        }
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
        dayOpen.clear();
        latestVwap.clear();
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
        latestVwap.clear();
        latestLtp.clear();
        latestChangePct.clear();
        lastCumulativeVol.clear();
    }

    // ── Candle bar data class ─────────────────────────────────────────────────

    public static class CandleBar {
        public long startMinute; // minutes since midnight
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
