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
    private static final String STATE_FILE = "../store/data/straddle-state.json";

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
