package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.BalancedAtmSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Tracks the spot-based NIFTY ATM with V2 strike-drift discipline.
 *
 * <p>Two scheduled loops:
 * <ul>
 *   <li><b>Bootstrap</b> — runs every 30 s until the first valid ATM is resolved
 *       (typically within seconds of market open). Sets the baseline and fires
 *       the initial {@link AtmChange} so the strategy can subscribe its 6-contract
 *       watchlist.</li>
 *   <li><b>Drift check</b> — runs every 30 minutes during market hours. Computes
 *       the current ATM and compares to the baseline. Fires an {@link AtmChange}
 *       only when the new ATM differs from the baseline (i.e., NIFTY drifted past
 *       a ±1 strike boundary). Strategy re-subscribes around the new ATM.</li>
 * </ul>
 *
 * <p>This matches the V2 spec's "30-Minute Check Loop": stable watchlist for half-
 * hour windows, no constant churn on minor 50-point oscillations.
 */
@Service
public class AtmTracker {

    private static final Logger log = LoggerFactory.getLogger(AtmTracker.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final MarketDataService    marketDataService;
    private final BalancedAtmSelector  atmSelector;

    private final CopyOnWriteArrayList<Consumer<AtmChange>> listeners = new CopyOnWriteArrayList<>();
    private volatile long baselineAtm = -1;

    public AtmTracker(MarketDataService marketDataService, BalancedAtmSelector atmSelector) {
        this.marketDataService = marketDataService;
        this.atmSelector       = atmSelector;
    }

    /** Register an ATM-change listener. The first successful resolution fires an
     *  {@link AtmChange} with {@code oldAtm = -1} to every registered listener — signals
     *  bootstrap. Multiple consumers (Camarilla, OptionOiSubscriber, …) can subscribe
     *  independently; each receives every event. */
    public void addListener(Consumer<AtmChange> l) {
        if (l != null) listeners.add(l);
    }

    /** Backward-compat single-listener entrypoint. New code should use
     *  {@link #addListener}; this is kept so existing callers don't need a rename. */
    public void setListener(Consumer<AtmChange> l) {
        addListener(l);
    }

    public long getCurrentAtm() { return baselineAtm; }

    /** Bootstrap loop — fires every 30 s until the first ATM resolution lands.
     *  After baseline is set, this becomes a no-op. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 10_000)
    public void bootstrap() {
        if (baselineAtm > 0) return;
        if (listeners.isEmpty()) return;
        tryUpdate();
    }

    /** Drift check — fires at every 15-min wall-clock boundary (09:00, 09:15, 09:30,
     *  09:45, …, 15:45 IST) on weekdays. Cron-aligned (not fixed-delay) so the checks
     *  land exactly on the candle aggregator's 15-min landmarks (every 5th 3-min bar
     *  close) — when that bar closes and evaluates L4/H4 patterns, the watchlist is
     *  already on the freshest ATM. Fires AtmChange only when the resolved ATM has
     *  moved past the baseline. */
    @Scheduled(cron = "0 0/15 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public void driftCheck() {
        if (baselineAtm <= 0) return;        // wait for bootstrap to complete
        if (listeners.isEmpty()) return;
        tryUpdate();
    }

    /** End-of-day reset. Clears the baseline at 15:31 IST every weekday so the
     *  dashboard's ATM display shows "—" overnight / on weekends, and so the next
     *  morning's bootstrap fires fresh against actual day-open NIFTY (not against
     *  a stale carryover from yesterday's close). */
    @Scheduled(cron = "0 31 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void endOfDayReset() {
        if (baselineAtm > 0) {
            log.info("[AtmTracker] end-of-day reset — clearing baseline ATM {}", baselineAtm);
            baselineAtm = -1;
        }
    }

    private void tryUpdate() {
        LocalTime t = ZonedDateTime.now(IST).toLocalTime();
        if (t.isBefore(LocalTime.of(9, 15)) || t.isAfter(LocalTime.of(15, 30))) return;

        double spot;
        try { spot = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { return; }
        if (spot <= 0) return;

        BalancedAtmSelector.AtmSelection sel = atmSelector.select(spot);
        if (sel == null) return;
        long newAtm = sel.chosenAtm();
        if (newAtm == baselineAtm) {
            // No drift — fire a status-only AtmChange (oldAtm == newAtm) so listeners can
            // log a heartbeat. Confirms drift checks are running even when ATM doesn't move.
            fireAtmChange(new AtmChange(baselineAtm, baselineAtm));
            return;
        }

        AtmChange ev = new AtmChange(baselineAtm, newAtm);
        baselineAtm = newAtm;
        log.info("[AtmTracker] ATM {} → {} (drift rebalance)",
            ev.oldAtm() < 0 ? "(boot)" : String.valueOf(ev.oldAtm()),
            ev.newAtm());
        fireAtmChange(ev);
    }

    private void fireAtmChange(AtmChange ev) {
        for (Consumer<AtmChange> l : listeners) {
            try { l.accept(ev); }
            catch (Exception e) { log.warn("[AtmTracker] listener threw: {}", e.getMessage()); }
        }
    }

    /** ATM transition event. {@code oldAtm = -1} signals initial bootstrap.
     *  The listener resolves the 6-contract watchlist (ATM, ITM ±1, OTM ±1) from
     *  the numeric ATM via its own {@link BalancedAtmSelector} usage. */
    public record AtmChange(long oldAtm, long newAtm) {}
}
