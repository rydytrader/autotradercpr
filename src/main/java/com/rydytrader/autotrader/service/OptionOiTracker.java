package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.store.RiskSettingsStore;
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
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Real-time cumulative-since-baseline OI tracker for the NIFTY option chain around
 * the day's selected ATM.
 *
 * <p>Driven by Fyers WebSocket OI ticks (no REST polling). {@link OptionOiSubscriber}
 * resolves the ±N strike window around AtmVwap's session-locked ATM, subscribes the
 * symbols, and routes {@code OiTick} events into {@link #onOiTick}. Each tick updates
 * one strike's CE or PE OI, then incrementally recomputes the cumulative deltas and
 * bias state.
 *
 * <p>Bias classifier — three states, 40 % threshold on the ratio of cumulative CE vs
 * PE change since baseline:
 * <ul>
 *   <li>{@code cumCE ≥ 1.40 · cumPE} → {@code BEARISH} (call writers dominant)</li>
 *   <li>{@code cumPE ≥ 1.40 · cumCE} → {@code BULLISH} (put writers dominant)</li>
 *   <li>otherwise → {@code NEUTRAL}</li>
 * </ul>
 * Edge cases: both zero → NEUTRAL; one side zero and the other positive → definitive
 * bias to the non-zero side.
 *
 * <p>Display-only for MVP — no disk persistence, no trade-filter gate. Intraday
 * restart re-baselines on next tick.
 */
@Service
public class OptionOiTracker {

    private static final Logger log = LoggerFactory.getLogger(OptionOiTracker.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int    MAX_SAMPLES        = 400;   // 1-min cadence × ~6h15m session = 375 + headroom
    private static final long   STALE_THRESHOLD_MS = 5 * 60_000L;
    /** Fallback threshold if the setting can't be read. Matches the default in
     *  {@code RiskSettingsStore.atmVwapOiBiasThresholdPct} (40 %). */
    private static final double DEFAULT_BIAS_THRESHOLD_PCT = 0.40;
    private static final LocalTime SESSION_START   = LocalTime.of(9, 15);
    private static final LocalTime SESSION_END     = LocalTime.of(15, 30);
    /** Where to persist the tracker state. Restored on boot to survive mid-day
     *  restarts — the cumulative curve resumes from disk, the routing map rebuilds
     *  from persisted window symbols, and the next OI tick keeps counting deltas
     *  against the on-disk per-strike baselines. */
    private static final String STATE_FILE = "../store/cache/option-oi-state.json";

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .findAndRegisterModules();

    private final RiskSettingsStore riskSettings;
    private State state = new State();

    public OptionOiTracker(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
    }

    @PostConstruct
    public void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        // Rebuild the routing maps from persisted state so post-restart ticks resolve
        // to their strike + side immediately — before AtmVwap re-fires notifyOiWindow.
        if (state.windowSymbols != null) {
            for (StrikeSymbols ss : state.windowSymbols) {
                symbolToStrike.put(ss.ceSymbol(), ss.strike());
                symbolToStrike.put(ss.peSymbol(), ss.strike());
                symbolToSide.put(ss.ceSymbol(), "CE");
                symbolToSide.put(ss.peSymbol(), "PE");
            }
        }
        log.info("[OptionOi] booted — dayKey={} atm={} window={} baselined={} samples={} bias={}",
            state.dayKey, state.atmStrike,
            state.windowSymbols == null ? 0 : state.windowSymbols.size(),
            state.baselineByStrike == null ? 0 : state.baselineByStrike.size(),
            state.samples == null ? 0 : state.samples.size(),
            state.bias);
    }

    /** Current bias threshold as a decimal (0.40 = 40 %). Reads live from
     *  {@link RiskSettingsStore} so operator changes take effect on the next tick
     *  without a restart. Guards a null store (test context) and negative / absurdly
     *  high configured values with a sane fallback. */
    private double currentBiasThreshold() {
        if (riskSettings == null) return DEFAULT_BIAS_THRESHOLD_PCT;
        try {
            double pct = riskSettings.getAtmVwapOiBiasThresholdPct();
            if (pct < 0 || pct > 500) return DEFAULT_BIAS_THRESHOLD_PCT;
            return pct / 100.0;
        } catch (Exception e) {
            return DEFAULT_BIAS_THRESHOLD_PCT;
        }
    }
    // Routing maps for the active window — rebuilt by setActiveWindow on each ATM
    // assignment. Empty until the subscriber calls setActiveWindow.
    private final ConcurrentHashMap<String, Long>   symbolToStrike = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> symbolToSide   = new ConcurrentHashMap<>();  // "CE" | "PE"

    // ── Active window ──────────────────────────────────────────────────────────

    /** Replaces the active strike window. Returns the newly-entering Fyers symbols so the
     *  caller can call {@code MarketDataService.subscribeAdditional} on just those. Strikes
     *  that leave the window have their per-strike baseline + latest OI dropped, and their
     *  contribution removed from the running cumulative totals. Called once per session by
     *  the subscriber after AtmVwap picks its ATM; repeat calls are idempotent when the
     *  window is unchanged. */
    public synchronized List<String> setActiveWindow(long atm, List<StrikeSymbols> window) {
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
        state.atmStrike     = atm;
        state.activeStrikes = new ArrayList<>(newStrikeSet);
        state.windowSymbols = new ArrayList<>(window);

        // 2. Compute newly-entering symbols (not previously in the routing map).
        List<String> entering = new ArrayList<>();
        for (String sym : newSymbolSet) {
            if (!symbolToStrike.containsKey(sym)) entering.add(sym);
        }

        // 3. Rebuild routing maps.
        symbolToStrike.clear();
        symbolToSide.clear();
        for (StrikeSymbols ss : window) {
            symbolToStrike.put(ss.ceSymbol(), ss.strike());
            symbolToStrike.put(ss.peSymbol(), ss.strike());
            symbolToSide.put(ss.ceSymbol(), "CE");
            symbolToSide.put(ss.peSymbol(), "PE");
        }
        recomputeBias();
        // Persist so a restart before the first tick still remembers the window mapping.
        saveToDisk();
        return entering;
    }

    // ── Tick handling ──────────────────────────────────────────────────────────

    /** Called via {@code MarketDataService.addOiListener} on every option OI tick.
     *  Symbols outside the active window are ignored. */
    public synchronized void onOiTick(String fyersSymbol, long oi, long exchFeedTimeSec) {
        if (oi <= 0 || fyersSymbol == null) return;
        Long strike = symbolToStrike.get(fyersSymbol);
        String side = symbolToSide.get(fyersSymbol);
        if (strike == null || side == null) return;

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

        state.lastSampleAt   = LocalDateTime.now(IST);
        state.lastTickMillis = System.currentTimeMillis();
        state.diff           = state.cumulativeCeChange - state.cumulativePeChange;
        String previousBias  = state.bias;
        recomputeBias();
        // Save on bias-state transitions — structural change worth preserving right away.
        // Non-transition ticks rely on the minute-cadence save inside snapshotForChart.
        if (!Objects.equals(previousBias, state.bias)) saveToDisk();
    }

    /** Chart snapshot — records the current cumulative + bias into the bounded ring every
     *  minute during market hours. The trade page's inline chart draws from this ring.
     *  Doubles as the canonical minute-cadence disk save. */
    @Scheduled(cron = "0 * 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public synchronized void snapshotForChart() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        if (now.isBefore(SESSION_START) || now.isAfter(SESSION_END)) return;
        if (state.baselineTakenAt == null) return;
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

    /** End-of-day reset. Baselines + samples + cumulative wiped. Active window kept — the
     *  subscriber will replace it next day when AtmVwap picks a fresh ATM. */
    @Scheduled(cron = "0 31 15 * * MON-FRI", zone = "Asia/Kolkata")
    public synchronized void endOfDayReset() {
        log.info("[OptionOi] end-of-day reset — clearing baseline + {} samples", state.samples.size());
        State fresh = new State();
        state = fresh;
        symbolToStrike.clear();
        symbolToSide.clear();
        saveToDisk();
    }

    // ── Read API ───────────────────────────────────────────────────────────────

    /** Snapshot of the currently-active ±N strike window (empty pre-ATM-resolution).
     *  Used by alternate-feed clients (GDFL) to subscribe the same 30 symbols the
     *  bias tracker watches, so all OI in the tracker's window can be sourced from
     *  one vendor. */
    public synchronized List<StrikeSymbols> activeWindow() {
        return state.windowSymbols == null ? List.of()
            : new ArrayList<>(state.windowSymbols);
    }

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
            state.atmStrike,
            state.activeStrikes == null ? 0 : state.activeStrikes.size(),
            round2(currentBiasThreshold() * 100.0),
            round2(actualBiasPct(state.cumulativeCeChange, state.cumulativePeChange)));
    }

    /** Actual bias magnitude — the difference between CE and PE cumulative OI change
     *  expressed as a percent of the LARGER side. Signed: positive = CE dominant
     *  (bearish tilt), negative = PE dominant (bullish tilt). Bounded in [-100, 100].
     *
     *  <p>Example: CE=100, PE=50 → (100−50)/100 × 100 = 50%. The same threshold value
     *  (40% default) is compared against this metric in {@link #evaluateBias}, so a
     *  pill reading of "50%" is a stronger signal than the 40% threshold and correctly
     *  flips to BEARISH.
     *
     *  <p>Cumulative flows that go negative (writers unwinding) are clamped to 0 for
     *  this calculation — bias reflects OI BUILDING, not OI reduction. */
    static double actualBiasPct(long cumCe, long cumPe) {
        double ce = Math.max(0, cumCe);
        double pe = Math.max(0, cumPe);
        if (ce == 0 && pe == 0) return 0;
        double bigger = Math.max(ce, pe);
        double smaller = Math.min(ce, pe);
        double magnitude = (bigger - smaller) / bigger * 100.0;
        return ce >= pe ? magnitude : -magnitude;
    }

    public synchronized History history() {
        return new History(state.baselineTakenAt, new ArrayList<>(state.samples));
    }

    private boolean isStale() {
        if (state.lastTickMillis == 0) return false;
        return (System.currentTimeMillis() - state.lastTickMillis) > STALE_THRESHOLD_MS;
    }

    // ── Bias math ──────────────────────────────────────────────────────────────

    private void recomputeBias() {
        state.diff        = state.cumulativeCeChange - state.cumulativePeChange;
        state.ceToPeRatio = computeRatio(state.cumulativeCeChange, state.cumulativePeChange);
        state.bias        = evaluateBias(state.cumulativeCeChange, state.cumulativePeChange, currentBiasThreshold());
    }

    /** Three-state bias — the difference between the two sides must be at least
     *  {@code threshold} (decimal, e.g. 0.40 = 40%) of the LARGER side to earn a
     *  directional label. Same metric as {@link #actualBiasPct} so the pill's
     *  displayed % and the threshold configured by the operator are directly
     *  comparable (e.g. pill shows "BEARISH · 50%" only when the display metric
     *  is ≥ the 40% threshold).
     *
     *  <p>Sign handling: cumulative-since-baseline can go negative when writers
     *  unwind. Only positive OI-build flows count toward a directional bias here;
     *  a negative side is treated as 0 flow. Both ≤ 0 → NEUTRAL. */
    static String evaluateBias(long cumCe, long cumPe, double threshold) {
        double ce = Math.max(0, cumCe);
        double pe = Math.max(0, cumPe);
        if (ce == 0 && pe == 0) return "NEUTRAL";
        double bigger = Math.max(ce, pe);
        double smaller = Math.min(ce, pe);
        double diffPctOfBigger = (bigger - smaller) / bigger;   // 0.0 … 1.0
        if (diffPctOfBigger < Math.max(0, threshold)) return "NEUTRAL";
        return ce >= pe ? "BEARISH" : "BULLISH";
    }

    static double computeRatio(long cumCe, long cumPe) {
        if (cumPe == 0) return cumCe > 0 ? Double.POSITIVE_INFINITY : 0.0;
        return (double) cumCe / (double) cumPe;
    }

    // ── Day rollover ───────────────────────────────────────────────────────────

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        State fresh = new State();
        fresh.dayKey        = today;
        fresh.windowSymbols = state.windowSymbols;   // re-baseline on next tick
        fresh.activeStrikes = state.activeStrikes;
        fresh.atmStrike     = state.atmStrike;
        state = fresh;
        saveToDisk();
    }

    // ── Persistence ────────────────────────────────────────────────────────────

    /** Deserialise persisted state. Discards the file when its {@code dayKey} isn't
     *  today (stale prior-session data would poison the cumulative curve). Silent
     *  on missing / malformed files — a fresh boot with no prior state is normal. */
    private void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s == null) return;
            String today = LocalDate.now(IST).toString();
            if (!today.equals(s.dayKey)) {
                log.info("[OptionOi] discarding stale state.json — dayKey={} today={}", s.dayKey, today);
                return;
            }
            // Defensive null-safety — older schemas may miss collections.
            if (s.samples          == null) s.samples          = new ArrayList<>();
            if (s.baselineByStrike == null) s.baselineByStrike = new TreeMap<>();
            if (s.latestByStrike   == null) s.latestByStrike   = new TreeMap<>();
            if (s.windowSymbols    == null) s.windowSymbols    = new ArrayList<>();
            if (s.activeStrikes    == null) s.activeStrikes    = new ArrayList<>();
            state = s;
        } catch (IOException e) {
            log.warn("[OptionOi] failed to load state: {}", e.getMessage());
        }
    }

    /** Write state to disk atomically. Best-effort — I/O failures are logged and
     *  swallowed so a full disk / permission issue never crashes the tick path. */
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

    // ── State / DTOs ───────────────────────────────────────────────────────────

    public record StrikeSymbols(long strike, String ceSymbol, String peSymbol) {}

    public static class State {
        public String        dayKey            = LocalDate.now(IST).toString();
        public LocalDateTime baselineTakenAt;
        public LocalDateTime lastSampleAt;
        public long          lastTickMillis    = 0;
        public long          atmStrike         = 0;
        public Map<Long, long[]> baselineByStrike = new TreeMap<>();
        public Map<Long, long[]> latestByStrike   = new TreeMap<>();
        public long          cumulativeCeChange  = 0;
        public long          cumulativePeChange  = 0;
        public long          diff                = 0;
        public double        ceToPeRatio         = 0.0;
        public String        bias                = "NEUTRAL";
        public int           samplesTaken        = 0;
        public List<SampleRecord>  samples       = new ArrayList<>();
        public List<StrikeSymbols> windowSymbols = new ArrayList<>();
        public List<Long>          activeStrikes = new ArrayList<>();
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
                           long          atmStrike,
                           int           activeStrikeCount,
                           double        biasThresholdPct,
                           /** Signed actual percent by which one side exceeds the other
                            *  since baseline. +ve = CE > PE (bearish tilt), −ve = PE > CE
                            *  (bullish tilt). See {@link #actualBiasPct}. */
                           double        biasActualPct) {}

    public record History(LocalDateTime baselineTakenAt, List<SampleRecord> samples) {}

    // Suppress unused-import compiler warning for LinkedHashMap — kept intentional in case
    // downstream ports need an ordered strike map.
    @SuppressWarnings("unused")
    private static final Map<String, Object> _RESERVED = new LinkedHashMap<>();
}
