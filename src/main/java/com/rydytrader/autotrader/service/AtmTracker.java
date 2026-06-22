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
 * Tracks the NIFTY ATM strike, locked to the session's <b>open price</b>.
 *
 * <p>The ATM is resolved once per session — on the first NIFTY tick after 09:15 IST —
 * and held fixed for the entire trading day. There is no intraday drift check.
 * If NIFTY moves 200 points by midday, the ATM the strategy was set up around at
 * open stays the strategy's anchor. The watchlist, OI subscription, and Camarilla
 * level cache all stay on that one strike pair for the whole session.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li><b>Bootstrap</b> — runs every 30 s after boot until the first ATM is
 *       resolved from the live NIFTY LTP. Fires the single {@link AtmChange}
 *       (oldAtm = -1 → newAtm = open-derived) to every registered listener.</li>
 *   <li><b>End-of-day reset</b> — at 15:31 IST, clears the baseline so the
 *       dashboard reads "—" overnight and the next morning's bootstrap fires
 *       fresh against tomorrow's actual day-open NIFTY.</li>
 * </ul>
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

    /** Bootstrap loop — fires every 30 s until the first ATM resolution lands.
     *  After the baseline is set, this becomes a no-op for the rest of the session. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 10_000)
    public void bootstrap() {
        if (baselineAtm > 0) return;
        if (listeners.isEmpty()) return;
        tryResolveOnce();
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

    /** Resolve the ATM from the current NIFTY LTP and fire the bootstrap event.
     *  No-op when already baselined (the lock is intentional — no drift), outside
     *  market hours, or when the spot can't be read yet. */
    private void tryResolveOnce() {
        if (baselineAtm > 0) return;
        LocalTime t = ZonedDateTime.now(IST).toLocalTime();
        if (t.isBefore(LocalTime.of(9, 15)) || t.isAfter(LocalTime.of(15, 30))) return;

        double spot;
        try { spot = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { return; }
        if (spot <= 0) return;

        BalancedAtmSelector.AtmSelection sel = atmSelector.select(spot);
        if (sel == null) return;
        long openAtm = sel.chosenAtm();

        AtmChange ev = new AtmChange(baselineAtm, openAtm);
        baselineAtm = openAtm;
        log.info("[AtmTracker] ATM locked at {} (open-price derived; no drift checks this session)",
            openAtm);
        fireAtmChange(ev);
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
