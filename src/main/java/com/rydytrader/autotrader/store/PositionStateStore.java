package com.rydytrader.autotrader.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Persists open position state to disk so polling resumes correctly after server restarts.
 * File presence = active trade. File absence = no active trade = polling skipped.
 * Live state:      logs/live/position-state.json
 * Simulator state: logs/simulator/position-state.json
 */
@Component
public class PositionStateStore {

    private static final String LIVE_FILE = "../store/live/position-state.json";
    private static final String SIM_FILE  = "../store/simulator/position-state.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    private ModeStore modeStore;

    public PositionStateStore() {
        new File("../store/live").mkdirs();
        new File("../store/simulator").mkdirs();
    }

    @Autowired
    public void setModeStore(ModeStore modeStore) {
        this.modeStore = modeStore;
    }

    private String stateFile() {
        return (modeStore == null || modeStore.isLive()) ? LIVE_FILE : SIM_FILE;
    }

    public void save(String symbol, String side, int qty, double avgPrice, String setup, String entryTime) {
        try {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("symbol",    symbol);
            state.put("side",      side);
            state.put("qty",       qty);
            state.put("avgPrice",  avgPrice);
            state.put("setup",     setup != null ? setup : "");
            state.put("entryTime", entryTime != null ? entryTime : "");
            Files.writeString(Paths.get(stateFile()), mapper.writeValueAsString(state));
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to save: " + e.getMessage());
        }
    }

    public void clear() {
        try {
            Files.deleteIfExists(Paths.get(stateFile()));
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to clear: " + e.getMessage());
        }
    }

    /** Returns null if no persisted position exists. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> load() {
        try {
            Path path = Paths.get(stateFile());
            if (!Files.exists(path)) return null;
            return mapper.readValue(Files.readString(path), Map.class);
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to load: " + e.getMessage());
            return null;
        }
    }
}
