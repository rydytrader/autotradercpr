package com.rydytrader.autotrader.store.strategy;

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
 * On-disk state for {@code LegSlStrategy}. Mirrors {@code StraddleStateStore} but tracks per-leg
 * closure timestamps and uses the leg-sl lifecycle states (OPEN_BOTH / OPEN_CE_ONLY /
 * OPEN_PE_ONLY / DONE_FOR_DAY).
 *
 * <p>File: {@code ../store/data/strategies/leg-sl-state.json}.
 */
@Component
public class LegSlStateStore {

    private static final Logger log = LoggerFactory.getLogger(LegSlStateStore.class);
    private static final String STATE_FILE = "../store/data/strategies/leg-sl-state.json";
    private static final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static class State {
        public String  dayKey;
        /** LegSlStrategy.LifecycleState enum name. */
        public String  state;
        public String  ceSymbol;
        public String  peSymbol;
        public int     ceQty;
        public int     peQty;
        public String  ceOrderId;
        public String  peOrderId;
        public double  ceEntryPremium;
        public double  peEntryPremium;
        /** Epoch millis when each leg was closed (SL hit or squareoff). 0 = leg still open. */
        public long    ceClosedAtMillis;
        public long    peClosedAtMillis;
        public double  realisedPnlToday;
        public double  sellPremiumTurnoverToday;
        public double  buyPremiumTurnoverToday;
        public int     orderCountToday;
        public String  currentWeeklyExpiry;
        /** Cycle events ring (ENTRY / CE_SL / PE_SL / SQUAREOFF). Same shape as RollingStraddle's
         *  recentRolls list — {time, event, nifty, ce, pe, pnl}. */
        public java.util.List<java.util.Map<String, Object>> recentEvents;
        /** 1-minute combined-premium samples for the dashboard chart — same shape as the
         *  combined-sl-roll strategy's. */
        public java.util.List<java.util.Map<String, Object>> combinedPremiumSamples;
        public long    updatedAtMillis;

        public State() {}
    }

    private volatile State current = new State();

    @PostConstruct
    public void init() { load(); }

    public synchronized State get() { return current; }

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
                log.info("[LegSlState] No prior state file ({}), starting fresh", STATE_FILE);
                return;
            }
            String json = Files.readString(p);
            if (json == null || json.isBlank()) return;
            State loaded = mapper.readValue(json, State.class);
            if (loaded != null) {
                this.current = loaded;
                log.info("[LegSlState] Loaded {} state for {} (ce={}, pe={})",
                    loaded.state, loaded.dayKey, loaded.ceSymbol, loaded.peSymbol);
            }
        } catch (IOException e) {
            log.warn("[LegSlState] Failed to load {}: {}", STATE_FILE, e.getMessage());
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
            log.warn("[LegSlState] Failed to persist state: {}", e.getMessage());
        }
    }
}
