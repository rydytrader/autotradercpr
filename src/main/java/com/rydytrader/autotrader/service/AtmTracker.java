package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.BalancedAtmSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

/**
 * Polls NIFTY spot every 5 s during market hours, resolves the spot-based ATM via
 * {@link BalancedAtmSelector} (NIFTY/50 rounded), and emits an event to a registered listener
 * whenever the chosen ATM strike crosses to a different value.
 *
 * <p>The listener receives an {@link AtmChange} record with the prior ATM, the new ATM,
 * and the corresponding CE/PE symbols on both sides. Listeners are responsible for
 * subscribing/unsubscribing aggregator buckets and updating their own state.
 */
@Service
public class AtmTracker {

    private static final Logger log = LoggerFactory.getLogger(AtmTracker.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final MarketDataService    marketDataService;
    private final BalancedAtmSelector  atmSelector;

    private volatile Consumer<AtmChange> listener;
    private volatile long currentAtm = -1;
    private volatile String currentCeSym = "";
    private volatile String currentPeSym = "";

    public AtmTracker(MarketDataService marketDataService, BalancedAtmSelector atmSelector) {
        this.marketDataService = marketDataService;
        this.atmSelector       = atmSelector;
    }

    /** Register the single ATM-change listener. The first call's first successful resolution
     *  fires an {@link AtmChange} with prior fields blank — this signals the listener to
     *  bootstrap. */
    public void setListener(Consumer<AtmChange> l) {
        this.listener = l;
    }

    public long getCurrentAtm()        { return currentAtm; }
    public String getCurrentCeSym()    { return currentCeSym; }
    public String getCurrentPeSym()    { return currentPeSym; }

    @Scheduled(fixedDelay = 5_000, initialDelay = 8_000)
    public void tick() {
        if (listener == null) return;
        LocalTime t = ZonedDateTime.now(IST).toLocalTime();
        if (t.isBefore(LocalTime.of(9, 15)) || t.isAfter(LocalTime.of(15, 30))) return;

        double spot;
        try { spot = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { return; }
        if (spot <= 0) return;

        BalancedAtmSelector.AtmSelection sel = atmSelector.select(spot);
        if (sel == null) return;
        long newAtm = sel.chosenAtm();
        if (newAtm == currentAtm) return;

        AtmChange ev = new AtmChange(
            currentAtm, currentCeSym, currentPeSym,
            newAtm, sel.ceSymbolAtChosen(), sel.peSymbolAtChosen());
        currentAtm    = newAtm;
        currentCeSym  = sel.ceSymbolAtChosen();
        currentPeSym  = sel.peSymbolAtChosen();
        try {
            log.info("[AtmTracker] ATM shifted {} → {} (CE={} PE={})",
                ev.oldAtm() < 0 ? "(boot)" : String.valueOf(ev.oldAtm()),
                ev.newAtm(), ev.newCeSym(), ev.newPeSym());
            listener.accept(ev);
        } catch (Exception e) {
            log.warn("[AtmTracker] listener threw: {}", e.getMessage());
        }
    }

    /** Payload emitted on every ATM transition. On the first emit, {@code oldAtm} is -1
     *  and the old-symbol strings are empty — listener should treat that as initial bootstrap. */
    public record AtmChange(
        long   oldAtm, String oldCeSym, String oldPeSym,
        long   newAtm, String newCeSym, String newPeSym
    ) {}
}
