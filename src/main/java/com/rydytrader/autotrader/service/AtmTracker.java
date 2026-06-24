package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.service.strategy.BalancedAtmSelector;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Tracks the NIFTY ATM strike, locked to the price at the configured trading
 * start time and held fixed for the rest of the session.
 *
 * <p>The ATM is resolved once per session — on the first NIFTY tick at or after
 * {@code camarillaTradingStartTime} (default 09:30 IST, configurable in
 * Settings) — and held fixed for the entire trading day. There is no intraday
 * drift check. If NIFTY moves 200 points by midday, the ATM the strategy was
 * set up around at start time stays the strategy's anchor. The watchlist, OI
 * subscription, and Camarilla level cache all stay on that one strike pair for
 * the whole session.
 *
 * <p>Why wait for the configured start time rather than 09:15:
 * <ul>
 *   <li>The first 15 minutes of a session are typically the noisiest. Letting
 *       NIFTY settle before locking the ATM avoids anchoring on a wick / gap
 *       that doesn't represent the day's true tradeable range.</li>
 *   <li>The Camarilla level math doesn't need it earlier — entry detection
 *       starts only after the same {@code camarillaTradingStartTime} gate.</li>
 *   <li>NSE-cumulative VWAP (used by the L4/VWAP breakdown filter) starts
 *       accumulating at 09:15 regardless of when we subscribe, so the late
 *       subscribe loses no VWAP history.</li>
 * </ul>
 *
 * <p>Crash / restart recovery: once resolved, the baseline ATM is persisted to
 * {@code ../store/cache/atm-baseline.json} along with the IST session date. A
 * mid-day JVM restart reads that file in {@code @PostConstruct}; if the date
 * matches today's IST date, the same ATM is restored and a single
 * {@link AtmChange} fires to listeners (replacing the live NIFTY-LTP resolve).
 * If the date doesn't match (yesterday's file from a weekend / holiday
 * restart) the file is ignored and the bootstrap loop resolves fresh.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li><b>Restore</b> — {@code @PostConstruct} reads disk. If today's record
 *       exists, stages a restore that the next bootstrap tick applies.</li>
 *   <li><b>Bootstrap</b> — runs every 30 s. Either applies the staged restore
 *       (fires AtmChange immediately, no NIFTY tick needed) or attempts a live
 *       resolve via {@link #tryResolveOnce()} once IST ≥ start-time.</li>
 *   <li><b>End-of-day reset</b> — at 15:31 IST, clears the in-memory baseline
 *       AND deletes the disk record so the next session starts fresh.</li>
 * </ul>
 */
@Service
public class AtmTracker {

    private static final Logger log = LoggerFactory.getLogger(AtmTracker.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/atm-baseline.json";
    private static final String LEGACY_STATE_FILE = "../store/data/atm-baseline.json";

    private final MarketDataService    marketDataService;
    private final BalancedAtmSelector  atmSelector;
    private final RiskSettingsStore    riskSettings;

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final CopyOnWriteArrayList<Consumer<AtmChange>> listeners = new CopyOnWriteArrayList<>();
    private volatile long baselineAtm = -1;
    /** ATM read from disk that's waiting for at least one listener to register
     *  before it can be applied + broadcast. Set in {@code @PostConstruct},
     *  consumed (and cleared) by the first bootstrap tick where listeners
     *  exist. {@code -1} means "no pending restore". */
    private volatile long pendingRestoreAtm = -1;

    public AtmTracker(MarketDataService marketDataService,
                      BalancedAtmSelector atmSelector,
                      RiskSettingsStore riskSettings) {
        this.marketDataService = marketDataService;
        this.atmSelector       = atmSelector;
        this.riskSettings      = riskSettings;
    }

    /** Register an ATM-change listener. The single session-open resolution fires an
     *  {@link AtmChange} with {@code oldAtm = -1} to every registered listener.
     *  Multiple consumers (Camarilla, OptionOiSubscriber, …) can subscribe
     *  independently; each receives the bootstrap event. */
    public void addListener(Consumer<AtmChange> l) {
        if (l != null) listeners.add(l);
    }

    /** Backward-compat single-listener entrypoint. New code should use
     *  {@link #addListener}; this is kept so existing callers don't need a rename. */
    public void setListener(Consumer<AtmChange> l) {
        addListener(l);
    }

    public long getCurrentAtm() { return baselineAtm; }

    /** Read the persisted baseline at boot. If the file's IST session date
     *  matches today, stage a restore for the next bootstrap tick. Otherwise
     *  the file is treated as stale and the bootstrap resolves fresh. */
    @PostConstruct
    public void restoreFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            // One-time migration — move legacy ../store/data/ file to ../store/cache/.
            if (!Files.exists(p)) {
                Path legacy = Path.of(LEGACY_STATE_FILE);
                if (Files.exists(legacy)) {
                    java.io.File parent = p.toFile().getParentFile();
                    if (parent != null && !parent.exists()) parent.mkdirs();
                    Files.move(legacy, p);
                    log.info("[AtmTracker] migrated {} → {}", legacy, p);
                } else {
                    return;
                }
            }
            Map<?, ?> raw = mapper.readValue(Files.readString(p), Map.class);
            if (raw == null) return;
            Object dateObj = raw.get("date");
            Object atmObj  = raw.get("atm");
            if (!(dateObj instanceof String dateStr) || !(atmObj instanceof Number atmNum)) return;
            String today = LocalDate.now(IST).toString();
            if (!today.equals(dateStr)) {
                log.info("[AtmTracker] disk record date {} != today {} — ignoring (fresh resolve)",
                    dateStr, today);
                return;
            }
            long restored = atmNum.longValue();
            if (restored <= 0) return;
            pendingRestoreAtm = restored;
            log.info("[AtmTracker] staged restore from disk — ATM {} for session {}",
                restored, dateStr);
        } catch (IOException e) {
            log.warn("[AtmTracker] failed to read {}: {}", STATE_FILE, e.getMessage());
        }
    }

    /** Bootstrap loop — fires every 30 s until the first ATM is set. Either
     *  applies a disk-staged restore or attempts a live resolve from NIFTY LTP. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 10_000)
    public void bootstrap() {
        if (baselineAtm > 0) return;
        if (listeners.isEmpty()) return;

        // Restore path — apply the disk-staged value immediately. No time gate
        // or NIFTY-tick dependency; we're trusting the file because its date
        // matched today's IST date in restoreFromDisk().
        long pending = pendingRestoreAtm;
        if (pending > 0) {
            AtmChange ev = new AtmChange(baselineAtm, pending);
            baselineAtm = pending;
            pendingRestoreAtm = -1;
            log.info("[AtmTracker] ATM restored from disk = {} — broadcasting to {} listener(s)",
                pending, listeners.size());
            fireAtmChange(ev);
            return;
        }

        tryResolveOnce();
    }

    /** End-of-day reset. Clears the in-memory baseline AND deletes the disk
     *  record so a restart tomorrow morning resolves fresh against the new
     *  session's NIFTY. Runs at 15:31 IST every weekday. */
    @Scheduled(cron = "0 31 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void endOfDayReset() {
        if (baselineAtm > 0) {
            log.info("[AtmTracker] end-of-day reset — clearing baseline ATM {}", baselineAtm);
            baselineAtm = -1;
        }
        try {
            Files.deleteIfExists(Path.of(STATE_FILE));
        } catch (IOException e) {
            log.warn("[AtmTracker] failed to delete {}: {}", STATE_FILE, e.getMessage());
        }
    }

    /** Resolve the ATM from the current NIFTY LTP and fire the bootstrap event.
     *  No-op when already baselined (the lock is intentional — no drift), before
     *  the configured {@code camarillaTradingStartTime}, after 15:30 IST, or when
     *  the spot can't be read yet. */
    private void tryResolveOnce() {
        if (baselineAtm > 0) return;
        LocalTime t = ZonedDateTime.now(IST).toLocalTime();
        if (t.isAfter(LocalTime.of(15, 30))) return;

        // Start-time gate: don't lock the ATM until the operator's configured
        // start time (default 09:30). Parsed every poll so a settings edit at
        // 09:18 still applies to today's resolution. Malformed/missing values
        // fall back to 09:15 — same as the legacy behaviour, fail-safe.
        LocalTime startTime = parseStartTime(riskSettings.getCamarillaTradingStartTime());
        if (t.isBefore(startTime)) return;

        double spot;
        try { spot = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { return; }
        if (spot <= 0) return;

        BalancedAtmSelector.AtmSelection sel = atmSelector.select(spot);
        if (sel == null) return;
        long openAtm = sel.chosenAtm();

        AtmChange ev = new AtmChange(baselineAtm, openAtm);
        baselineAtm = openAtm;
        log.info("[AtmTracker] ATM locked at {} — NIFTY={} at start-time gate {} (no drift this session)",
            openAtm, spot, startTime);
        saveToDisk();
        fireAtmChange(ev);
    }

    /** Atomically write the current baseline + today's IST date to the
     *  restore file. Same tmp+move pattern Camarilla state uses. Silent on
     *  failure — disk is a recovery aid, not a correctness dependency. */
    private void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("date", LocalDate.now(IST).toString());
            body.put("atm",  baselineAtm);
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(body));
            Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("[AtmTracker] failed to save baseline: {}", e.getMessage());
        }
    }

    /** Parse "HH:mm" into a {@link LocalTime}. Returns 09:15 IST on null/blank/
     *  malformed input — degrades to the prior at-open behaviour rather than
     *  blocking the session entirely. */
    private static LocalTime parseStartTime(String hhmm) {
        if (hhmm == null || hhmm.isBlank()) return LocalTime.of(9, 15);
        try { return LocalTime.parse(hhmm.trim()); }
        catch (Exception e) { return LocalTime.of(9, 15); }
    }

    private void fireAtmChange(AtmChange ev) {
        for (Consumer<AtmChange> l : listeners) {
            try { l.accept(ev); }
            catch (Exception e) { log.warn("[AtmTracker] listener threw: {}", e.getMessage()); }
        }
    }

    /** ATM transition event. {@code oldAtm = -1} signals the session-open bootstrap.
     *  With drift checks removed, this is the only AtmChange a listener will see
     *  per session — listeners that maintain watchlists / subscriptions should treat
     *  it as a one-shot setup, not a rebalance trigger. */
    public record AtmChange(long oldAtm, long newAtm) {}
}
