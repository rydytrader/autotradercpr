package com.rydytrader.autotrader.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.util.FileIoUtils;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Persists the in-memory state of {@code RollingStraddleService} to a JSON file on disk so that
 * a server restart mid-day can resume the open straddle without re-firing duplicate orders.
 *
 * <p>State file: {@code ../store/data/straddle-state.json}.
 *
 * <p>Mirrors the persistence pattern of {@code EmaService} / {@code HtfEmaService}: write to a
 * sibling {@code .tmp} file then atomic-move via {@link FileIoUtils#atomicMoveWithRetry}.
 */
@Component
public class StraddleStateStore {

    private static final Logger log = LoggerFactory.getLogger(StraddleStateStore.class);
    /** Strategy-namespaced state path. Each strategy has its own JSON file under
     *  {@code ../store/data/strategies/<strategyId>-state.json}. */
    private static final String STATE_FILE = "../store/data/strategies/combined-sl-roll-state.json";
    /** Legacy path kept for one-time migration on first boot after the refactor. If the
     *  new file doesn't exist but the old one does, we read the old, write to new, leave
     *  the old in place (operator can delete manually once happy). */
    private static final String LEGACY_STATE_FILE = "../store/data/straddle-state.json";

    private static final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static class State {
        public String  dayKey;          // ISO date for the trading day this state belongs to
        public String  state;           // RollingStraddleService.State enum name
        public double  lastEntryNifty;  // reference for ±% trigger; resets on each roll
        public String  ceSymbol;
        public String  peSymbol;
        public int     ceQty;
        public int     peQty;
        public int     rollCount;
        public String  ceOrderId;       // most recent sell order ID for CE leg
        public String  peOrderId;       // most recent sell order ID for PE leg
        // Fields below persist so a mid-day restart resumes MTM + day P&L + charges without
        // resetting them to zero. Day rollover clears them via rolloverIfNewDay regardless.
        public double  ceEntryPremium;
        public double  peEntryPremium;
        public double  realisedPnlToday;
        public double  sellPremiumTurnoverToday;
        public double  buyPremiumTurnoverToday;
        public int     orderCountToday;
        public String  currentWeeklyExpiry;
        /** Epoch millis when the combined-premium SL fired (or when the current Roll Wait
         *  Filter cycle started). Resets each time the timer re-arms. */
        public long    slHitTimeMillis;
        /** Roll Wait Filter — NIFTY LTP captured at the start of the current wait cycle
         *  (either at SL hit or when the cycle re-armed after a wide-range cycle). */
        public double  niftyAtSlHit;
        /** Highest NIFTY tick seen since {@link #niftyAtSlHit} was set. */
        public double  niftyHighSinceSl;
        /** Lowest NIFTY tick seen since {@link #niftyAtSlHit} was set. */
        public double  niftyLowSinceSl;
        /** Roll Wait Filter — number of wait cycles elapsed since the SL hit (1 on the first
         *  wait, increments each time the range was too wide and the timer re-armed). */
        public int     rollWaitCycles;
        /** Today's roll events ring (entry / roll / close lines for the dashboard). Persisted
         *  so the "Today's Roll Events" table also survives mid-day restart. */
        public java.util.List<java.util.Map<String, Object>> recentRolls;
        /** 1-minute combined-premium samples ({t: HH:mm, v: ceLtp+peLtp}) for the straddle
         *  premium chart. Cleared on day rollover; persisted so chart survives restart. */
        public java.util.List<java.util.Map<String, Object>> combinedPremiumSamples;
        public long    updatedAtMillis;

        public State() {}
    }

    private volatile State current = new State();

    @PostConstruct
    public void init() {
        load();
    }

    public synchronized State get() {
        return current;
    }

    public synchronized void update(State s) {
        if (s == null) return;
        s.updatedAtMillis = System.currentTimeMillis();
        this.current = s;
        save();
    }

    private void load() {
        try {
            Path p = Path.of(STATE_FILE);
            Path legacy = Path.of(LEGACY_STATE_FILE);
            // One-time migration: new path missing but legacy exists → read legacy, write to new.
            if (!Files.exists(p) && Files.exists(legacy)) {
                log.info("[StraddleState] Migrating legacy state file {} → {}", LEGACY_STATE_FILE, STATE_FILE);
                String legacyJson = Files.readString(legacy);
                if (legacyJson != null && !legacyJson.isBlank()) {
                    State loaded = mapper.readValue(legacyJson, State.class);
                    if (loaded != null) {
                        this.current = loaded;
                        save(); // writes to STATE_FILE
                        log.info("[StraddleState] Migration complete. Old file left in place for safety; remove {} manually when ready.", LEGACY_STATE_FILE);
                        log.info("[StraddleState] Loaded {} state for {} (rollCount={}, ce={}, pe={})",
                            loaded.state, loaded.dayKey, loaded.rollCount, loaded.ceSymbol, loaded.peSymbol);
                    }
                }
                return;
            }
            if (!Files.exists(p)) {
                log.info("[StraddleState] No prior state file ({}), starting fresh", STATE_FILE);
                return;
            }
            String json = Files.readString(p);
            if (json == null || json.isBlank()) return;
            State loaded = mapper.readValue(json, State.class);
            if (loaded != null) {
                this.current = loaded;
                log.info("[StraddleState] Loaded {} state for {} (rollCount={}, ce={}, pe={})",
                    loaded.state, loaded.dayKey, loaded.rollCount, loaded.ceSymbol, loaded.peSymbol);
            }
        } catch (IOException e) {
            log.warn("[StraddleState] Failed to load {}: {}", STATE_FILE, e.getMessage());
        }
    }

    private void save() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(current);
            Files.writeString(tmp, json);
            FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[StraddleState] Failed to persist state: {}", e.getMessage());
        }
    }
}
