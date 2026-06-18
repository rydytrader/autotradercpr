package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.util.FileIoUtils;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Real-time cumulative-since-09:15 OI bias tracker for the NIFTY option chain.
 *
 * <p>Driven by Fyers WebSocket OI ticks (no REST polling). {@link OptionOiSubscriber}
 * resolves the 21-strike window around ATM, subscribes the symbols, and routes
 * {@code OiTick} events into {@link #onOiTick}. Each tick updates one strike's CE or PE
 * OI, then incrementally recomputes the cumulative deltas and bias state.
 *
 * <p>Bias classifier (matches the operator's Python reference):
 * <ul>
 *   <li>{@code cumCE / cumPE ≥ 1.5} → {@code STRONG_BEARISH_BIAS}</li>
 *   <li>{@code cumCE / cumPE ≤ 0.66} → {@code STRONG_BULLISH_BIAS}</li>
 *   <li>otherwise → {@code NEUTRAL}</li>
 *   <li>{@code cumPE ≤ 0} edge: BEARISH if cumCE &gt; 0, else NEUTRAL.</li>
 * </ul>
 *
 * <p>State persists to {@code ../store/data/option-oi-state.json}. A mid-day restart
 * restores the per-strike 09:15 baselines, the sample ring, and the last cumulative
 * snapshot; on the first post-restart OI tick the running totals are recomputed live
 * against the disk baselines, so nothing is lost.
 *
 * <p>Display-only in Phase 1: the bias appears in the Positions header tiles + trend
 * modal and is logged next to every Camarilla entry. A future Phase 2 will use the same
 * in-memory snapshot as a trade-filter gate — zero added latency because the lookup is a
 * single in-process field read.
 */
@Service
public class OptionOiTracker {

    private static final Logger log = LoggerFactory.getLogger(OptionOiTracker.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/data/option-oi-state.json";
    private static final int    MAX_SAMPLES = 400;        // 1-min cadence × ~6h15m session = 375 + headroom
    private static final long   STALE_THRESHOLD_MS = 5 * 60_000L;
    private static final double RATIO_BEARISH_THRESHOLD = 1.5;
    private static final double RATIO_BULLISH_THRESHOLD = 0.66;
    private static final LocalTime SESSION_START = LocalTime.of(9, 15);
    private static final LocalTime SESSION_END   = LocalTime.of(15, 30);

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        // Old Phase 1 state.json carries fields the new schema doesn't have (e.g. atmStrike
        // at the top level). Don't fail load — just ignore them so the migration is silent.
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .findAndRegisterModules();

    private State state = new State();
    // Routing maps for the active window — rebuilt by setActiveWindow on each ATM change.
    // Not persisted: rebuilt from state.activeWindowStrikes on boot.
    private final ConcurrentHashMap<String, Long>   symbolToStrike = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> symbolToSide   = new ConcurrentHashMap<>();   // "CE" | "PE"

    @PostConstruct
    public void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        // Rebuild the routing maps from persisted state so post-restart ticks resolve
        // correctly even before the next ATM-change event re-runs setActiveWindow.
        if (state.windowSymbols != null) {
            for (StrikeSymbols ss : state.windowSymbols) {
                symbolToStrike.put(ss.ceSymbol(), ss.strike());
                symbolToStrike.put(ss.peSymbol(), ss.strike());
                symbolToSide.put(ss.ceSymbol(), "CE");
                symbolToSide.put(ss.peSymbol(), "PE");
            }
        }
        log.info("[OptionOi] booted — dayKey={} window={} baselined={} bias={}",
            state.dayKey,
            state.windowSymbols == null ? 0 : state.windowSymbols.size(),
            state.baselineByStrike == null ? 0 : state.baselineByStrike.size(),
            state.bias);
    }

    // ── Active window ───────────────────────────────────────────────────────────

    /** Replaces the active 21-strike window. Returns the list of newly-entering Fyers
     *  symbols (the caller should call {@code subscribeAdditional} on these). Strikes that
     *  leave the window have their per-strike baseline + latest OI dropped, and their
     *  contribution removed from the running cumulative totals. */
    public synchronized List<String> setActiveWindow(List<StrikeSymbols> window) {
        if (window == null) window = new ArrayList<>();
        Set<Long>   newStrikeSet = new HashSet<>();
        Set<String> newSymbolSet = new HashSet<>();
        for (StrikeSymbols ss : window) {
            newStrikeSet.add(ss.strike());
            newSymbolSet.add(ss.ceSymbol());
            newSymbolSet.add(ss.peSymbol());
        }

        // 1. Drop strikes that left the window — un-credit their contribution.
        if (state.activeStrikes != null) {
            for (Long oldStrike : new ArrayList<>(state.activeStrikes)) {
                if (newStrikeSet.contains(oldStrike)) continue;
                long[] base = state.baselineByStrike.remove(oldStrike);
                long[] last = state.latestByStrike.remove(oldStrike);
                if (base != null && last != null) {
                    state.cumulativeCeChange -= (last[0] - base[0]);
                    state.cumulativePeChange -= (last[1] - base[1]);
                }
            }
        }
        state.activeStrikes = new ArrayList<>(newStrikeSet);
        state.windowSymbols = new ArrayList<>(window);

        // 2. Compute newly-entering symbols (not previously in the routing map).
        List<String> entering = new ArrayList<>();
        for (String sym : newSymbolSet) {
            if (!symbolToStrike.containsKey(sym)) entering.add(sym);
        }

        // 3. Rebuild routing maps. Symbols that left the window stay in the map only if
        //    their parent strike is still active; otherwise drop the routing so their
        //    inbound ticks become no-ops at the tracker level.
        symbolToStrike.clear();
        symbolToSide.clear();
        for (StrikeSymbols ss : window) {
            symbolToStrike.put(ss.ceSymbol(), ss.strike());
            symbolToStrike.put(ss.peSymbol(), ss.strike());
            symbolToSide.put(ss.ceSymbol(), "CE");
            symbolToSide.put(ss.peSymbol(), "PE");
        }

        recomputeRatioAndBias();
        return entering;
    }

    // ── Tick handling ───────────────────────────────────────────────────────────

    /** Called by {@link OptionOiSubscriber}'s OI-listener bridge for every OI update.
     *  Symbols outside the active window are ignored. */
    public synchronized void onOiTick(String fyersSymbol, long oi, long exchFeedTimeSec) {
        if (oi <= 0 || fyersSymbol == null) return;
        Long strike = symbolToStrike.get(fyersSymbol);
        String side = symbolToSide.get(fyersSymbol);
        if (strike == null || side == null) return;

        // Outside market window: still update the latest map so the bias recovers fast on
        // next session, but DO NOT shift the baseline.
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        boolean inSession = !now.isBefore(SESSION_START) && !now.isAfter(SESSION_END);

        if (inSession) {
            rolloverIfNewDay();
            if (state.baselineTakenAt == null) state.baselineTakenAt = LocalDateTime.now(IST);
        }

        long[] baseline = state.baselineByStrike.get(strike);
        long[] latest   = state.latestByStrike.computeIfAbsent(strike, k -> new long[]{0, 0});

        int idx = "CE".equals(side) ? 0 : 1;
        long old = latest[idx];

        // First OI seen for this strike-side → record as baseline (for that side).
        if (baseline == null || baseline[idx] == 0) {
            if (baseline == null) {
                baseline = new long[]{0, 0};
                state.baselineByStrike.put(strike, baseline);
            }
            baseline[idx] = oi;
            latest[idx]   = oi;
            // Baseline tick contributes 0 to the cumulative. Done.
        } else if (old != oi) {
            latest[idx] = oi;
            long delta = oi - old;
            if ("CE".equals(side)) state.cumulativeCeChange += delta;
            else                   state.cumulativePeChange += delta;
        }

        state.lastSampleAt        = LocalDateTime.now(IST);
        state.lastTickMillis      = System.currentTimeMillis();
        state.diff                = state.cumulativeCeChange - state.cumulativePeChange;
        String previousBias       = state.bias;
        recomputeRatioAndBias();

        // Save on bias-state transitions (a structural change worth durable preservation).
        // Non-transition ticks are saved every 3 minutes by snapshotForChart().
        if (!java.util.Objects.equals(previousBias, state.bias)) saveToDisk();
    }

    /** Periodic chart snapshot — records the current cumulative + bias into the bounded
     *  sample ring every minute during market hours. The trend modal's Chart.js line
     *  plots from this ring. Also doubles as the canonical "save to disk" tick. */
    @Scheduled(cron = "0 * 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public synchronized void snapshotForChart() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        if (now.isBefore(SESSION_START) || now.isAfter(SESSION_END)) return;
        if (state.baselineTakenAt == null) return;     // no ticks yet

        rolloverIfNewDay();
        state.samplesTaken++;
        SampleRecord rec = new SampleRecord(
            now.format(DateTimeFormatter.ofPattern("HH:mm")),
            state.cumulativeCeChange, state.cumulativePeChange,
            round2(state.ceToPeRatio), state.bias);
        state.samples.add(rec);
        while (state.samples.size() > MAX_SAMPLES) state.samples.remove(0);
        saveToDisk();
    }

    /** End-of-day reset — clears baseline, samples, cumulative. Strike subscriptions stay
     *  on the WebSocket (will resume against a fresh baseline tomorrow). */
    @Scheduled(cron = "0 31 15 * * MON-FRI", zone = "Asia/Kolkata")
    public synchronized void endOfDayReset() {
        log.info("[OptionOi] end-of-day reset — clearing baseline + {} samples", state.samples.size());
        State fresh = new State();
        fresh.windowSymbols = state.windowSymbols;       // keep routing — same window for tomorrow
        fresh.activeStrikes = state.activeStrikes;
        state = fresh;
        saveToDisk();
    }

    // ── Read API ────────────────────────────────────────────────────────────────

    public synchronized Snapshot snapshot() {
        String bias = state.bias;
        if (isStale()) bias = "STALE";
        return new Snapshot(
            state.baselineTakenAt,
            state.lastSampleAt,
            state.samplesTaken,
            state.cumulativeCeChange,
            state.cumulativePeChange,
            state.diff,
            round2(state.ceToPeRatio),
            bias,
            atmFromActiveStrikes());
    }

    public synchronized History history() {
        return new History(state.baselineTakenAt, new ArrayList<>(state.samples));
    }

    public synchronized String biasLogLine() {
        if (state.baselineTakenAt == null) return "no-baseline-yet";
        String b = isStale() ? "STALE" : state.bias;
        return "CE " + sign(state.cumulativeCeChange)
             + " | PE " + sign(state.cumulativePeChange)
             + " | ratio " + round2(state.ceToPeRatio)
             + " | " + b;
    }

    private boolean isStale() {
        if (state.lastTickMillis == 0) return false;     // pre-first-tick — show NEUTRAL not STALE
        return (System.currentTimeMillis() - state.lastTickMillis) > STALE_THRESHOLD_MS;
    }

    /** Approximate ATM from the centre of the active-strike list — saves wiring the ATM
     *  value into a separate field. Returns 0 when no window is active. */
    private long atmFromActiveStrikes() {
        if (state.activeStrikes == null || state.activeStrikes.isEmpty()) return 0;
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (Long s : state.activeStrikes) { if (s < min) min = s; if (s > max) max = s; }
        return (min + max) / 2;
    }

    // ── Bias math ───────────────────────────────────────────────────────────────

    private void recomputeRatioAndBias() {
        state.diff        = state.cumulativeCeChange - state.cumulativePeChange;
        state.ceToPeRatio = computeRatio(state.cumulativeCeChange, state.cumulativePeChange);
        state.bias        = evaluateBias(state.cumulativeCeChange, state.cumulativePeChange);
    }

    static String evaluateBias(long cumCe, long cumPe) {
        if (cumPe <= 0) return cumCe > 0 ? "STRONG_BEARISH_BIAS" : "NEUTRAL";
        double ratio = (double) cumCe / (double) cumPe;
        if (ratio >= RATIO_BEARISH_THRESHOLD) return "STRONG_BEARISH_BIAS";
        if (ratio <= RATIO_BULLISH_THRESHOLD) return "STRONG_BULLISH_BIAS";
        return "NEUTRAL";
    }

    static double computeRatio(long cumCe, long cumPe) {
        if (cumPe == 0) return cumCe > 0 ? Double.POSITIVE_INFINITY : 0.0;
        return (double) cumCe / (double) cumPe;
    }

    // ── Day rollover + persistence ──────────────────────────────────────────────

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        State fresh = new State();
        fresh.windowSymbols = state.windowSymbols;       // re-baseline on next tick
        fresh.activeStrikes = state.activeStrikes;
        fresh.dayKey = today;
        state = fresh;
        saveToDisk();
    }

    private void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.samples == null) state.samples = new ArrayList<>();
                if (state.baselineByStrike == null) state.baselineByStrike = new TreeMap<>();
                if (state.latestByStrike == null) state.latestByStrike = new TreeMap<>();
            }
        } catch (IOException e) {
            log.warn("[OptionOi] failed to load state: {}", e.getMessage());
        }
    }

    private void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
            FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[OptionOi] failed to save state: {}", e.getMessage());
        }
    }

    private static double round2(double v) {
        if (Double.isInfinite(v) || Double.isNaN(v)) return v;
        return Math.round(v * 100.0) / 100.0;
    }

    private static final java.text.NumberFormat IN_NUMBER_FMT =
        java.text.NumberFormat.getNumberInstance(new java.util.Locale("en", "IN"));

    /** Sign-prefixed Indian-style number with comma grouping. e.g. 14496040 → "+1,44,96,040". */
    private static String sign(long v) {
        return (v >= 0 ? "+" : "-") + IN_NUMBER_FMT.format(Math.abs(v));
    }

    // ── State / DTOs ────────────────────────────────────────────────────────────

    public record StrikeSymbols(long strike, String ceSymbol, String peSymbol) {}

    public static class State {
        public String        dayKey            = LocalDate.now(IST).toString();
        public LocalDateTime baselineTakenAt;
        public LocalDateTime lastSampleAt;
        public long          lastTickMillis    = 0;
        public Map<Long, long[]> baselineByStrike = new TreeMap<>();
        public Map<Long, long[]> latestByStrike   = new TreeMap<>();
        public long          cumulativeCeChange  = 0;
        public long          cumulativePeChange  = 0;
        public long          diff                = 0;
        public double        ceToPeRatio         = 0.0;
        public String        bias                = "NEUTRAL";
        public int           samplesTaken        = 0;
        public List<SampleRecord> samples        = new ArrayList<>();
        public List<StrikeSymbols> windowSymbols  = new ArrayList<>();
        public List<Long>          activeStrikes  = new ArrayList<>();
    }

    public record SampleRecord(String t, long cumCe, long cumPe, double ratio, String bias) {}

    public record Snapshot(LocalDateTime baselineTakenAt,
                           LocalDateTime lastSampleAt,
                           int           samplesTaken,
                           long          cumulativeCeChange,
                           long          cumulativePeChange,
                           long          diff,
                           double        ceToPeRatio,
                           String        bias,
                           long          atmStrike) {}

    public record History(LocalDateTime baselineTakenAt, List<SampleRecord> samples) {}

    @SuppressWarnings("unused") // Mapping helper for backward compat — older state files used these keys.
    private static final Map<String, Object> _SCHEMA_NOTE = new LinkedHashMap<>();
}
