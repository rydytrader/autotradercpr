package com.rydytrader.autotrader.store.strategy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.util.FileIoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * On-disk state for every {@code ShortStraddle} instance. One JSON file per strategy id
 * at {@code ../store/data/strategies/short-straddle-<id>-state.json}. Tracks per-leg closure
 * timestamps and the lifecycle states (OPEN_BOTH / OPEN_CE_ONLY / OPEN_PE_ONLY / DONE_FOR_DAY).
 *
 * <p>Singleton {@code @Component} — manages an in-memory {@code Map<String, State>} keyed by
 * strategy id. Each instance asks the store for its own {@code State} via {@link #get(String)}
 * and persists via {@link #update(String, State)}.
 */
@Component
public class ShortStraddleStateStore {

    private static final Logger log = LoggerFactory.getLogger(ShortStraddleStateStore.class);
    private static final String STATE_DIR = "../store/data/strategies";
    private static final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static class State {
        public String  dayKey;
        /** ShortStraddle.LifecycleState enum name. */
        public String  state;
        public String  ceSymbol;
        public String  peSymbol;
        public int     ceQty;
        public int     peQty;
        public String  ceOrderId;
        public String  peOrderId;
        /** NIFTY LTP at the moment today's straddle was entered. 0 until first entry. */
        public double  lastEntryNifty;
        public double  ceEntryPremium;
        public double  peEntryPremium;
        /** Epoch millis when each leg was closed (SL hit or squareoff). 0 = leg still open. */
        public long    ceClosedAtMillis;
        public long    peClosedAtMillis;
        /** Per-leg realised P&L frozen at close. Dashboard leg card displays this once the
         *  leg is no longer running so it doesn't reset to 0 after the qty drops to 0. */
        public double  ceLegPnl;
        public double  peLegPnl;
        /** LTP captured at the moment each leg closed. Dashboard leg card displays this as
         *  the "Exit" price once the leg is no longer running. */
        public double  ceClosePremium;
        public double  peClosePremium;
        public double  realisedPnlToday;
        public double  sellPremiumTurnoverToday;
        public double  buyPremiumTurnoverToday;
        public int     orderCountToday;
        public String  currentWeeklyExpiry;
        /** Cycle events ring (ENTRY / CE_SL / PE_SL / SQUAREOFF) — {time, event, nifty, ce, pe, pnl}. */
        public java.util.List<java.util.Map<String, Object>> recentEvents;
        /** 1-minute per-leg premium samples for the dashboard chart. */
        public java.util.List<java.util.Map<String, Object>> combinedPremiumSamples;
        public long    updatedAtMillis;

        public State() {}
    }

    private final Map<String, State> byId = new ConcurrentHashMap<>();

    /** Load (or initialise) state for a strategy id. Reads the JSON file if it exists; otherwise
     *  returns a fresh empty {@code State}. Idempotent. Strategies should call this from their
     *  constructor / bootstrap so that subsequent {@link #get(String)} calls see the latest
     *  on-disk state. */
    public State load(String strategyId) {
        if (strategyId == null || strategyId.isBlank()) return new State();
        State existing = byId.get(strategyId);
        if (existing != null) return existing;
        State loaded = readFile(strategyId);
        if (loaded == null) loaded = new State();
        byId.put(strategyId, loaded);
        return loaded;
    }

    public State get(String strategyId) {
        if (strategyId == null) return null;
        return byId.computeIfAbsent(strategyId, this::loadFromDiskOrFresh);
    }

    public void update(String strategyId, State s) {
        if (strategyId == null || s == null) return;
        s.updatedAtMillis = System.currentTimeMillis();
        byId.put(strategyId, s);
        save(strategyId, s);
    }

    /** Remove the in-memory state map entry. Does NOT delete the on-disk file (soft-delete
     *  preserves history). Called when an instance is soft-deleted. */
    public void evict(String strategyId) {
        if (strategyId != null) byId.remove(strategyId);
    }

    private State loadFromDiskOrFresh(String strategyId) {
        State loaded = readFile(strategyId);
        return loaded != null ? loaded : new State();
    }

    private String fileFor(String strategyId) {
        return STATE_DIR + "/short-straddle-" + strategyId + "-state.json";
    }

    private State readFile(String strategyId) {
        String path = fileFor(strategyId);
        try {
            Path p = Path.of(path);
            if (!Files.exists(p)) {
                log.info("[ShortStraddleState] No prior state file for {} ({}), starting fresh", strategyId, path);
                return null;
            }
            String json = Files.readString(p);
            if (json == null || json.isBlank()) return null;
            State loaded = mapper.readValue(json, State.class);
            if (loaded != null) {
                log.info("[ShortStraddleState] Loaded {} state for {} (dayKey={}, ce={}, pe={})",
                    loaded.state, strategyId, loaded.dayKey, loaded.ceSymbol, loaded.peSymbol);
            }
            return loaded;
        } catch (IOException e) {
            log.warn("[ShortStraddleState] Failed to load {}: {}", path, e.getMessage());
            return null;
        }
    }

    private void save(String strategyId, State s) {
        String path = fileFor(strategyId);
        try {
            Path dst = Path.of(path);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(path + ".tmp");
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(s);
            Files.writeString(tmp, json);
            FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[ShortStraddleState] Failed to persist state for {}: {}", strategyId, e.getMessage());
        }
    }
}
