package com.rydytrader.autotrader.store.manual;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.util.FileIoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persistent store for the manual options terminal. Keeps the currently-open manual positions
 * keyed by entry orderId, a side-map from close-orderId → entry-orderId (so the WS fill listener
 * can resolve close fills back to their parent position), and a rolling deque of the most
 * recent closed manual trades.
 *
 * <p>JSON-persisted at {@code ../store/data/manual-terminal-state.json} via the same Jackson +
 * atomic-temp-rename pattern as {@code ShortStraddleStateStore}. All writes serialise via the
 * intrinsic lock on this bean.
 */
@Component
public class ManualTerminalStore {

    private static final Logger log = LoggerFactory.getLogger(ManualTerminalStore.class);
    private static final String STATE_FILE = "../store/data/manual-terminal-state.json";
    private static final int    RECENT_LIMIT = 50;
    private static final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    /** Serialised representation of the entire store. Round-tripped to/from
     *  {@code manual-terminal-state.json} on every mutation. */
    public static class Snapshot {
        public List<ManualPosition>      openPositions      = new ArrayList<>();
        public Map<String, String>       closeOrderIdToEntry = new java.util.LinkedHashMap<>();
        public List<ManualClosedTrade>   recentTrades       = new ArrayList<>();
        public Snapshot() {}
    }

    private final Map<String, ManualPosition> openByOrderId = new ConcurrentHashMap<>();
    /** Close-orderId → entry-orderId. Lets {@link #findEntryForClose} resolve a close fill
     *  back to its parent position without scanning. */
    private final Map<String, String>         closeOrderIdToEntry = new ConcurrentHashMap<>();
    private final Deque<ManualClosedTrade>    recent = new ArrayDeque<>();

    @PostConstruct
    private void init() { load(); }

    // ── Public API ────────────────────────────────────────────────────────────

    public synchronized void putOpen(ManualPosition p) {
        if (p == null || p.orderId == null || p.orderId.isEmpty()) return;
        openByOrderId.put(p.orderId, p);
        save();
    }

    public Optional<ManualPosition> findByOrderId(String orderId) {
        if (orderId == null) return Optional.empty();
        return Optional.ofNullable(openByOrderId.get(orderId));
    }

    public Collection<ManualPosition> openSnapshot() {
        return new ArrayList<>(openByOrderId.values());
    }

    public synchronized void mapCloseToEntry(String closeOrderId, String entryOrderId) {
        if (closeOrderId == null || entryOrderId == null) return;
        closeOrderIdToEntry.put(closeOrderId, entryOrderId);
        save();
    }

    public Optional<String> findEntryForClose(String closeOrderId) {
        if (closeOrderId == null) return Optional.empty();
        return Optional.ofNullable(closeOrderIdToEntry.get(closeOrderId));
    }

    /** Persist after the fill listener overwrites avgPrice / filled. No structural change,
     *  just a save. */
    public synchronized void persistAfterFill() { save(); }

    /** Move {@code entry} from {@code openByOrderId} into the recent-trades ring buffer with
     *  the supplied close details. Drops oldest entry if size exceeds {@link #RECENT_LIMIT}.
     *  Also drops the close-orderId mapping. */
    public synchronized void completeClose(ManualPosition entry, String closeOrderId,
                                           double closePrice, long closeMillis) {
        if (entry == null) return;
        openByOrderId.remove(entry.orderId);
        if (closeOrderId != null) closeOrderIdToEntry.remove(closeOrderId);
        ManualClosedTrade t = new ManualClosedTrade();
        t.orderId     = entry.orderId;
        t.symbol      = entry.symbol;
        t.side        = entry.side;
        t.qty         = entry.qty;
        t.openPrice   = entry.avgPrice;
        t.closePrice  = closePrice;
        t.openMillis  = entry.openMillis;
        t.closeMillis = closeMillis;
        t.pnl         = "BUY".equalsIgnoreCase(entry.side)
            ? (closePrice - entry.avgPrice) * entry.qty
            : (entry.avgPrice - closePrice) * entry.qty;
        recent.addFirst(t);
        while (recent.size() > RECENT_LIMIT) recent.pollLast();
        save();
    }

    /** Archive an open position without a real close — used by the boot-reconciler when the
     *  position no longer exists at the broker (already squared off / cancelled). */
    public synchronized void archive(ManualPosition entry, String note) {
        if (entry == null) return;
        openByOrderId.remove(entry.orderId);
        ManualClosedTrade t = new ManualClosedTrade();
        t.orderId     = entry.orderId;
        t.symbol      = entry.symbol;
        t.side        = entry.side;
        t.qty         = entry.qty;
        t.openPrice   = entry.avgPrice;
        t.closePrice  = 0;
        t.pnl         = 0;
        t.openMillis  = entry.openMillis;
        t.closeMillis = System.currentTimeMillis();
        t.note        = note == null ? "" : note;
        recent.addFirst(t);
        while (recent.size() > RECENT_LIMIT) recent.pollLast();
        save();
    }

    public List<ManualClosedTrade> recentSnapshot() {
        return new ArrayList<>(recent);
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    private void load() {
        Path p = Path.of(STATE_FILE);
        if (!Files.exists(p)) {
            log.info("[ManualTerminalStore] No state file yet — starting empty.");
            return;
        }
        try {
            String json = Files.readString(p);
            if (json == null || json.isBlank()) return;
            Snapshot s = mapper.readValue(json, Snapshot.class);
            if (s == null) return;
            openByOrderId.clear();
            closeOrderIdToEntry.clear();
            recent.clear();
            if (s.openPositions != null) {
                for (ManualPosition mp : s.openPositions) {
                    if (mp != null && mp.orderId != null && !mp.orderId.isEmpty()) {
                        openByOrderId.put(mp.orderId, mp);
                    }
                }
            }
            if (s.closeOrderIdToEntry != null) closeOrderIdToEntry.putAll(s.closeOrderIdToEntry);
            if (s.recentTrades != null) {
                for (ManualClosedTrade ct : s.recentTrades) recent.addLast(ct);
            }
            log.info("[ManualTerminalStore] Loaded — {} open position(s), {} recent trade(s).",
                openByOrderId.size(), recent.size());
        } catch (IOException e) {
            log.warn("[ManualTerminalStore] Failed to load {}: {}", STATE_FILE, e.getMessage());
        }
    }

    private void save() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Snapshot s = new Snapshot();
            s.openPositions       = new ArrayList<>(openByOrderId.values());
            s.closeOrderIdToEntry = new java.util.LinkedHashMap<>(closeOrderIdToEntry);
            s.recentTrades        = new ArrayList<>(recent);
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(s);
            Files.writeString(tmp, json);
            FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[ManualTerminalStore] Failed to persist: {}", e.getMessage());
        }
    }
}
