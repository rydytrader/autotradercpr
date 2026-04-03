package com.rydytrader.autotrader.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Tracks latency at each stage of the signal-to-order pipeline.
 * Stages: SIGNAL_DETECTED → SIGNAL_PROCESSED → ORDER_PLACED → ORDER_RESPONSE → ORDER_FILLED
 */
@Service
public class LatencyTracker {

    private static final Logger log = LoggerFactory.getLogger(LatencyTracker.class);

    // Current in-flight signals (keyed by symbol)
    private final ConcurrentHashMap<String, TradeLatency> inflight = new ConcurrentHashMap<>();

    // Completed latency records (last 50)
    private final ConcurrentLinkedDeque<TradeLatency> completed = new ConcurrentLinkedDeque<>();
    private static final int MAX_COMPLETED = 50;

    public enum Stage {
        SIGNAL_DETECTED,      // CprBreakoutStrategy detects breakout
        SIGNAL_PROCESSED,     // SignalProcessor validates + computes qty
        ORDER_PLACED,         // OrderService sends to Fyers API
        ORDER_RESPONSE,       // Fyers responds with order ID
        ORDER_FILLED          // Entry fill confirmed
    }

    public static class TradeLatency {
        public String symbol;
        public String setup;
        public final Map<Stage, Long> timestamps = new LinkedHashMap<>();

        public long getLatency(Stage from, Stage to) {
            Long t1 = timestamps.get(from);
            Long t2 = timestamps.get(to);
            if (t1 == null || t2 == null) return -1;
            return t2 - t1;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("symbol", symbol);
            m.put("setup", setup);
            long signalToProcessed = getLatency(Stage.SIGNAL_DETECTED, Stage.SIGNAL_PROCESSED);
            long processedToOrder = getLatency(Stage.SIGNAL_PROCESSED, Stage.ORDER_PLACED);
            long orderToResponse = getLatency(Stage.ORDER_PLACED, Stage.ORDER_RESPONSE);
            long responseToFill = getLatency(Stage.ORDER_RESPONSE, Stage.ORDER_FILLED);
            long endToEnd = getLatency(Stage.SIGNAL_DETECTED, Stage.ORDER_FILLED);
            m.put("signalToProcessed", signalToProcessed >= 0 ? signalToProcessed + "ms" : "--");
            m.put("processedToOrder", processedToOrder >= 0 ? processedToOrder + "ms" : "--");
            m.put("orderToResponse", orderToResponse >= 0 ? orderToResponse + "ms" : "--");
            m.put("responseToFill", responseToFill >= 0 ? responseToFill + "ms" : "--");
            m.put("endToEnd", endToEnd >= 0 ? endToEnd + "ms" : "--");
            return m;
        }
    }

    /** Start tracking a new signal for a symbol. */
    public void mark(String symbol, String setup, Stage stage) {
        TradeLatency tl = inflight.computeIfAbsent(symbol, k -> {
            TradeLatency t = new TradeLatency();
            t.symbol = symbol;
            t.setup = setup;
            return t;
        });
        tl.timestamps.put(stage, System.currentTimeMillis());

        if (stage == Stage.ORDER_FILLED) {
            // Move to completed
            inflight.remove(symbol);
            completed.addFirst(tl);
            while (completed.size() > MAX_COMPLETED) completed.removeLast();

            log.info("[Latency] {} {} — Signal→Processed: {}ms | Processed→Order: {}ms | Order→Response: {}ms | Response→Fill: {}ms | End-to-End: {}ms",
                symbol, setup,
                tl.getLatency(Stage.SIGNAL_DETECTED, Stage.SIGNAL_PROCESSED),
                tl.getLatency(Stage.SIGNAL_PROCESSED, Stage.ORDER_PLACED),
                tl.getLatency(Stage.ORDER_PLACED, Stage.ORDER_RESPONSE),
                tl.getLatency(Stage.ORDER_RESPONSE, Stage.ORDER_FILLED),
                tl.getLatency(Stage.SIGNAL_DETECTED, Stage.ORDER_FILLED));
        }
    }

    /** Cancel tracking for a symbol (signal filtered, not traded). */
    public void cancel(String symbol) {
        inflight.remove(symbol);
    }

    /** Get last N completed latency records (for health dashboard). */
    public List<Map<String, Object>> getCompleted() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (TradeLatency tl : completed) {
            result.add(tl.toMap());
        }
        return result;
    }

    /** Get average latencies from completed trades. */
    public Map<String, String> getAverages() {
        if (completed.isEmpty()) return Map.of("signalToFill", "--", "orderToFill", "--");
        long totalE2E = 0, totalOrder = 0;
        int countE2E = 0, countOrder = 0;
        for (TradeLatency tl : completed) {
            long e2e = tl.getLatency(Stage.SIGNAL_DETECTED, Stage.ORDER_FILLED);
            long otf = tl.getLatency(Stage.ORDER_PLACED, Stage.ORDER_FILLED);
            if (e2e >= 0) { totalE2E += e2e; countE2E++; }
            if (otf >= 0) { totalOrder += otf; countOrder++; }
        }
        Map<String, String> avg = new LinkedHashMap<>();
        avg.put("signalToFill", countE2E > 0 ? (totalE2E / countE2E) + "ms" : "--");
        avg.put("orderToFill", countOrder > 0 ? (totalOrder / countOrder) + "ms" : "--");
        return avg;
    }
}
