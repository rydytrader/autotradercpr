package com.rydytrader.autotrader.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Aggregates 75-minute candles for swing trading from the existing CandleAggregator's
 * 5-min candle close events. NSE 75-min structure: 5 candles per day aligned to market hours:
 *   9:15-10:30, 10:30-11:45, 11:45-13:00, 13:00-14:15, 14:15-15:30
 *
 * Listens to CandleAggregator.CandleCloseListener for 5-min candle closes, builds 75-min bars,
 * and emits to SwingCandleCloseListener when each 75-min candle completes.
 */
@Service
public class SwingCandleAggregator implements CandleAggregator.CandleCloseListener, CandleAggregator.DailyResetListener {

    private static final Logger log = LoggerFactory.getLogger(SwingCandleAggregator.class);

    // 75-min candle boundaries (minutes since midnight): 9:15=555, 10:30=630, 11:45=705, 13:00=780, 14:15=855
    private static final long[] BOUNDARIES = {555, 630, 705, 780, 855};

    // Current forming 75-min candle per symbol
    private final ConcurrentHashMap<String, CandleAggregator.CandleBar> currentCandles = new ConcurrentHashMap<>();
    // Completed 75-min candles (rolling buffer, max 30 = 6 trading days)
    private final ConcurrentHashMap<String, Deque<CandleAggregator.CandleBar>> completedCandles = new ConcurrentHashMap<>();
    // Track last finalized boundary to prevent double-emission
    private final ConcurrentHashMap<String, Long> lastFinalizedBoundary = new ConcurrentHashMap<>();

    private final List<SwingCandleCloseListener> listeners = new CopyOnWriteArrayList<>();

    // Set of symbols to aggregate (only WN/WI stocks)
    private volatile Set<String> swingSymbols = Collections.emptySet();

    public interface SwingCandleCloseListener {
        void onSwingCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle);
    }

    public void addListener(SwingCandleCloseListener listener) {
        listeners.add(listener);
    }

    public void setSwingSymbols(Set<String> symbols) {
        this.swingSymbols = symbols;
        log.info("[SwingCandle] Tracking {} swing symbols for 75-min candles", symbols.size());
    }

    /**
     * Called on every 5-min candle close from CandleAggregator.
     * Merges into 75-min candle and emits when the boundary crosses.
     */
    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (!swingSymbols.contains(fyersSymbol)) return;

        long candleEnd = completedCandle.startMinute + 5; // end time of 5-min candle (in minutes since midnight)
        long currentBoundary = get75MinBoundary(completedCandle.startMinute);
        if (currentBoundary < 0) return; // outside market hours

        CandleAggregator.CandleBar current = currentCandles.get(fyersSymbol);

        if (current == null || current.startMinute != currentBoundary) {
            // New 75-min candle — finalize old one if exists
            if (current != null && current.open > 0) {
                finalize75MinCandle(fyersSymbol, current);
            }
            // Start new 75-min candle
            CandleAggregator.CandleBar newCandle = new CandleAggregator.CandleBar();
            newCandle.startMinute = currentBoundary;
            newCandle.open = completedCandle.open;
            newCandle.high = completedCandle.high;
            newCandle.low = completedCandle.low;
            newCandle.close = completedCandle.close;
            newCandle.volume = completedCandle.volume;
            currentCandles.put(fyersSymbol, newCandle);
        } else {
            // Merge into current 75-min candle
            if (completedCandle.high > current.high) current.high = completedCandle.high;
            if (completedCandle.low < current.low) current.low = completedCandle.low;
            current.close = completedCandle.close;
            current.volume += completedCandle.volume;
        }

        // Check if this 5-min candle is the last one in the 75-min boundary
        long nextBoundary = getNextBoundary(currentBoundary);
        if (nextBoundary > 0 && candleEnd >= nextBoundary) {
            CandleAggregator.CandleBar toFinalize = currentCandles.get(fyersSymbol);
            if (toFinalize != null && toFinalize.open > 0) {
                finalize75MinCandle(fyersSymbol, toFinalize);
                currentCandles.remove(fyersSymbol);
            }
        }
    }

    private void finalize75MinCandle(String fyersSymbol, CandleAggregator.CandleBar candle) {
        Long prev = lastFinalizedBoundary.put(fyersSymbol, candle.startMinute);
        if (prev != null && prev == candle.startMinute) return; // already finalized

        Deque<CandleAggregator.CandleBar> history = completedCandles.computeIfAbsent(fyersSymbol, k -> new ConcurrentLinkedDeque<>());
        history.addLast(candle);
        while (history.size() > 30) history.pollFirst(); // keep ~6 trading days

        log.info("[SwingCandle] 75min close: {} start={} O={} H={} L={} C={} V={}",
            fyersSymbol, candle.startMinute,
            String.format("%.2f", candle.open), String.format("%.2f", candle.high),
            String.format("%.2f", candle.low), String.format("%.2f", candle.close), candle.volume);

        for (SwingCandleCloseListener listener : listeners) {
            try {
                listener.onSwingCandleClose(fyersSymbol, candle);
            } catch (Exception e) {
                log.error("[SwingCandle] Listener error for {}: {}", fyersSymbol, e.getMessage());
            }
        }
    }

    @Override
    public void onDailyReset() {
        // Finalize any open candles from yesterday
        for (var entry : currentCandles.entrySet()) {
            if (entry.getValue().open > 0) {
                finalize75MinCandle(entry.getKey(), entry.getValue());
            }
        }
        currentCandles.clear();
        lastFinalizedBoundary.clear();
        log.info("[SwingCandle] Daily reset — cleared forming candles");
    }

    /** Get completed 75-min candles for a symbol. */
    public List<CandleAggregator.CandleBar> getCompletedCandles(String symbol) {
        Deque<CandleAggregator.CandleBar> history = completedCandles.get(symbol);
        if (history == null) return Collections.emptyList();
        return new ArrayList<>(history);
    }

    /** Get current forming 75-min candle for a symbol. */
    public CandleAggregator.CandleBar getCurrentCandle(String symbol) {
        return currentCandles.get(symbol);
    }

    // ── Boundary helpers ─────────────────────────────────────────────────────

    /**
     * Get the 75-min candle boundary for a given minute.
     * Returns the start minute of the 75-min candle this minute belongs to.
     * Returns -1 if outside market hours.
     */
    private long get75MinBoundary(long minuteSinceMidnight) {
        // Walk boundaries in reverse to find which 75-min slot this falls into
        for (int i = BOUNDARIES.length - 1; i >= 0; i--) {
            if (minuteSinceMidnight >= BOUNDARIES[i]) {
                return BOUNDARIES[i];
            }
        }
        return -1; // before market open
    }

    /**
     * Get the next 75-min boundary after the given one.
     * Returns -1 if this is the last candle of the day (14:15-15:30).
     */
    private long getNextBoundary(long currentBoundary) {
        for (int i = 0; i < BOUNDARIES.length - 1; i++) {
            if (BOUNDARIES[i] == currentBoundary) {
                return BOUNDARIES[i + 1];
            }
        }
        // Last boundary (14:15) → market close at 15:30 = 930
        if (currentBoundary == BOUNDARIES[BOUNDARIES.length - 1]) {
            return 930; // 15:30
        }
        return -1;
    }
}
