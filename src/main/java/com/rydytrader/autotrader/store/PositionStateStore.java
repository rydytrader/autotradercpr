package com.rydytrader.autotrader.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Persists open position state to disk so polling resumes correctly after server restarts.
 * File presence = active trade. File absence = no active trade = polling skipped.
 */
@Component
public class PositionStateStore {

    private static final String STATE_FILE = "logs/position-state.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    public PositionStateStore() {
        new File("logs").mkdirs();
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
            Files.writeString(Paths.get(STATE_FILE), mapper.writeValueAsString(state));
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to save: " + e.getMessage());
        }
    }

    public void clear() {
        try {
            Files.deleteIfExists(Paths.get(STATE_FILE));
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to clear: " + e.getMessage());
        }
    }

    /** Returns null if no persisted position exists. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> load() {
        try {
            Path path = Paths.get(STATE_FILE);
            if (!Files.exists(path)) return null;
            return mapper.readValue(Files.readString(path), Map.class);
        } catch (IOException e) {
            System.err.println("[PositionStateStore] Failed to load: " + e.getMessage());
            return null;
        }
    }
}