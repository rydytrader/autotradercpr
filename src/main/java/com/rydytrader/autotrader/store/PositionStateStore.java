package com.rydytrader.autotrader.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Persists open position state to disk so polling resumes correctly after server restarts.
 * One JSON file per symbol, stored in a per-mode positions/ directory.
 * Live:      ../store/live/positions/{symbol}.json
 * Simulator: ../store/simulator/positions/{symbol}.json
 */
@Component
public class PositionStateStore {

    private static final String LIVE_DIR = "../store/live/positions";
    private static final String SIM_DIR  = "../store/simulator/positions";
    private static final ObjectMapper mapper = new ObjectMapper();

    private ModeStore modeStore;

    public PositionStateStore() {
        new File(LIVE_DIR).mkdirs();
        new File(SIM_DIR).mkdirs();
    }

    @Autowired
    public void setModeStore(ModeStore modeStore) {
        this.modeStore = modeStore;
    }

    private String positionsDir() {
        return (modeStore == null || modeStore.isLive()) ? LIVE_DIR : SIM_DIR;
    }

    /** Converts symbol to a safe filename (e.g. "NSE:NIFTY25JUN" → "NSE_NIFTY25JUN"). */
    private String toFileName(String symbol) {
        return symbol.replace(":", "_").replace("/", "_") + ".json";
    }

    private Path filePath(String symbol) {
        return Paths.get(positionsDir(), toFileName(symbol));
    }

    public void save(String symbol, String side, int qty, double avgPrice,
                     String setup, String entryTime, double slPrice, double targetPrice) {
        try {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("symbol",      symbol);
            state.put("side",        side);
            state.put("qty",         qty);
            state.put("avgPrice",    avgPrice);
            state.put("setup",       setup != null ? setup : "");
            state.put("entryTime",   entryTime != null ? entryTime : "");
            state.put("slPrice",     slPrice);
            state.put("targetPrice", targetPrice);
            Files.writeString(filePath(symbol), mapper.writeValueAsString(state));
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to save " + symbol + ": " + e.getMessage());
        }
    }

    /** Updates persisted state with OCO order IDs and prices. */
    public void saveOcoState(String symbol, String slOrderId, String targetOrderId,
                             double slPrice, double targetPrice) {
        try {
            Map<String, Object> state = load(symbol);
            if (state == null) return;
            state.put("slOrderId", slOrderId);
            state.put("targetOrderId", targetOrderId);
            state.put("slPrice", slPrice);
            state.put("targetPrice", targetPrice);
            Files.writeString(filePath(symbol), mapper.writeValueAsString(state));
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to save OCO state for " + symbol + ": " + e.getMessage());
        }
    }

    public void clear(String symbol) {
        try {
            Files.deleteIfExists(filePath(symbol));
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to clear " + symbol + ": " + e.getMessage());
        }
    }

    /** Returns null if no persisted state exists for this symbol. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> load(String symbol) {
        try {
            Path path = filePath(symbol);
            if (!Files.exists(path)) return null;
            return mapper.readValue(Files.readString(path), Map.class);
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to load " + symbol + ": " + e.getMessage());
            return null;
        }
    }

    /** Deletes all persisted position files in the current mode's directory. */
    public void clearAll() {
        File dir = new File(positionsDir());
        File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));
        if (files == null) return;
        for (File f : files) {
            try { Files.deleteIfExists(f.toPath()); }
            catch (IOException e) { System.err.println("[PositionStateStore] Failed to delete " + f.getName() + ": " + e.getMessage()); }
        }
    }

    /** Loads all persisted positions from the current mode's directory. */
    @SuppressWarnings("unchecked")
    public Map<String, Map<String, Object>> loadAll() {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        try {
            File dir = new File(positionsDir());
            File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));
            if (files == null) return result;
            for (File f : files) {
                Map<String, Object> state = mapper.readValue(Files.readString(f.toPath()), Map.class);
                String symbol = state.get("symbol").toString();
                result.put(symbol, state);
            }
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to loadAll: " + e.getMessage());
        }
        return result;
    }
}
