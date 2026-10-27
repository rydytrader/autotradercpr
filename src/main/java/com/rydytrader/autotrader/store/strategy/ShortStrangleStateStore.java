package com.rydytrader.autotrader.store.strategy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.util.FileIoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * On-disk state for every {@code ShortStrangle} instance (and, via the {@code ShortStraddleStateStore}
 * subclass, every {@code ShortStraddle} instance). One JSON file per strategy id at
 * {@code ../store/data/strategies/<prefix><id>-state.json}.
 *
 * <p>Since 2026-10-27 the state is a <b>leg book</b>: {@link State#legs} holds every short /
 * hedge leg opened today (or carried overnight when positional). The pre-leg-book
 * {@code ce* / pe* / hedge*} fields are kept on {@link State} purely so an older file can be
 * migrated on first load — they are no longer written.
 *
 * <p>Singleton {@code @Component} — manages an in-memory {@code Map<String, State>} keyed by
 * strategy id. Each instance asks the store for its own {@code State} via {@link #get(String)}
 * and persists via {@link #update(String, State)}.
 */
@Component
@Primary   // ShortStraddleStateStore extends this class — strangle injection points get this bean
public class ShortStrangleStateStore {

    private static final Logger log = LoggerFactory.getLogger(ShortStrangleStateStore.class);
    private static final String STATE_DIR = "../store/data/strategies";
    private static final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    /** One leg of the position book. Public fields so Jackson round-trips it without ceremony. */
    public static class Leg {
        /** Stable per-instance id ({@code L<n>}), unique for the day; carried across restarts. */
        public String  id;
        /** {@code CE} or {@code PE}. */
        public String  side;
        /** {@code SHORT} (sold) or {@code HEDGE} (bought). */
        public String  kind;
        public String  symbol;
        public int     qty;
        public String  orderId;
        /** Weighted-average fill price of the open quantity. */
        public double  entryPremium;
        public long    openedAtMillis;
        /** 0 while open. */
        public long    closedAtMillis;
        public double  closePremium;
        /** Realised P&L frozen at close (signed). 0 while open. */
        public double  pnl;
        public String  closeReason;
        /** Fyers product the leg was placed with: {@code INTRADAY} (MIS, bot squares off 15:20)
         *  or {@code MARGIN} (NRML, carried overnight). */
        public String  product;
        /** SL % of entry chosen in the open dialog (short legs). 0 = no %-based SL. */
        public double  slPct;
        /** Operator-set SL trigger premium (dashboard +/− / inline edit). 0 = none. */
        public double  slManual;
        /** COST button — SL collapsed to the entry premium. */
        public boolean slMovedToCost;
        /** Trailing-SL ratchet counter. */
        public int     trailingStepsHit;
        /** 1-minute premium samples {@code {t: "HH:mm", v: 12.35}} for the row-click chart. */
        public java.util.List<java.util.Map<String, Object>> samples;

        public Leg() {}
    }

    public static class State {
        public String  dayKey;
        /** ShortStrangle.LifecycleState enum name (ARMED / OPEN / DONE_FOR_DAY). Legacy files
         *  may carry OPEN_BOTH / OPEN_CE_ONLY / OPEN_PE_ONLY — migrated to OPEN on load. */
        public String  state;
        /** The leg book. {@code null} on a pre-leg-book file (→ migrate from the legacy fields). */
        public java.util.List<Leg> legs;
        /** Monotonic counter behind {@link Leg#id}. */
        public int     legSeq;
        /** Spot LTP at the moment today's first leg was opened. 0 until first entry. */
        public double  lastEntryNifty;
        public double  realisedPnlToday;
        public double  sellPremiumTurnoverToday;
        public double  buyPremiumTurnoverToday;
        public int     orderCountToday;
        /** Number of per-leg SL hits today. Cumulative across cycles; per-cycle delta is what
         *  gets written on each trade row. */
        public int     slHitsToday;
        // Cycle-start snapshots — used by persistStraddleTrade to write per-cycle deltas on
        // multi-cycle days. Refreshed every time the book goes from empty to open.
        public double  cycleStartRealisedPnl;
        public double  cycleStartSellTurnover;
        public double  cycleStartBuyTurnover;
        public int     cycleStartOrderCount;
        public int     cycleStartSlHits;
        /** Day-level Consumed Risk — sum of every closed leg's realised P&L today. */
        public double  consumedRiskToday;
        public String  currentWeeklyExpiry;
        /** Cycle events ring — {time, event, nifty, ce, pe, pnl}. */
        public java.util.List<java.util.Map<String, Object>> recentEvents;
        public long    updatedAtMillis;

        // ── Legacy two-leg fields (read-only, migration source) ─────────────────────────
        public String  ceSymbol, peSymbol;
        public int     ceQty, peQty;
        public String  ceOrderId, peOrderId;
        public double  ceEntryPremium, peEntryPremium;
        public long    ceClosedAtMillis, peClosedAtMillis;
        public double  ceLegPnl, peLegPnl;
        public double  ceClosePremium, peClosePremium;
        public boolean ceSlMovedToCost, peSlMovedToCost;
        public int     ceTrailingStepsHit, peTrailingStepsHit;
        public double  ceSlManual, peSlManual;
        public String  hedgeCeSymbol, hedgePeSymbol;
        public int     hedgeCeQty, hedgePeQty;
        public double  hedgeCeEntryPremium, hedgePeEntryPremium;
        public double  hedgeCeClosePremium, hedgePeClosePremium;
        public double  hedgeCeLegPnl, hedgePeLegPnl;
        /** Legacy 1-minute samples {t, v, ce, pe} — split into per-leg samples on migration. */
        public java.util.List<java.util.Map<String, Object>> combinedPremiumSamples;

        public State() {}
    }

    private final Map<String, State> byId = new ConcurrentHashMap<>();

    /** File-name prefix — overridden by the straddle store so the two instance families keep
     *  their existing files. */
    protected String filePrefix() { return "short-strangle-"; }

    /** Load (or initialise) state for a strategy id. Idempotent. */
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

    /** Remove the in-memory state map entry. Does NOT delete the on-disk file. */
    public void evict(String strategyId) {
        if (strategyId != null) byId.remove(strategyId);
    }

    private State loadFromDiskOrFresh(String strategyId) {
        State loaded = readFile(strategyId);
        return loaded != null ? loaded : new State();
    }

    private String fileFor(String strategyId) {
        return STATE_DIR + "/" + filePrefix() + strategyId + "-state.json";
    }

    private State readFile(String strategyId) {
        String path = fileFor(strategyId);
        try {
            Path p = Path.of(path);
            if (!Files.exists(p)) {
                log.info("[StrategyState] No prior state file for {} ({}), starting fresh", strategyId, path);
                return null;
            }
            String json = Files.readString(p);
            if (json == null || json.isBlank()) return null;
            State loaded = mapper.readValue(json, State.class);
            if (loaded != null) {
                log.info("[StrategyState] Loaded {} state for {} (dayKey={}, legs={})",
                    loaded.state, strategyId, loaded.dayKey, loaded.legs == null ? "legacy" : loaded.legs.size());
            }
            return loaded;
        } catch (IOException e) {
            log.warn("[StrategyState] Failed to load {}: {}", path, e.getMessage());
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
            log.warn("[StrategyState] Failed to persist state for {}: {}", strategyId, e.getMessage());
        }
    }
}
