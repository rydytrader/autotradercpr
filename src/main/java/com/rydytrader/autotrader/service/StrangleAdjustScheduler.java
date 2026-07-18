package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.StrangleAdjust;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the StrangleAdjust singleton's tick + fast-SL loops.
 *
 * <p>Two loops:
 * <ul>
 *   <li>{@code tick} — slow loop (~5 s). Day rollover, entry gate, timed squareoff,
 *       portfolio risk gate.</li>
 *   <li>{@code fastSlCheck} — fast loop (~500 ms). Tick-based SL watcher on open shorts.</li>
 * </ul>
 */
@Component
public class StrangleAdjustScheduler {

    private static final Logger log = LoggerFactory.getLogger(StrangleAdjustScheduler.class);

    private final StrangleAdjust strangleAdjust;

    public StrangleAdjustScheduler(StrangleAdjust strangleAdjust) {
        this.strangleAdjust = strangleAdjust;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        try { strangleAdjust.tick(); }
        catch (Throwable t) { log.error("[StrangleAdjustScheduler] tick threw: {}", t.getMessage(), t); }
    }

    @Scheduled(fixedDelay = 500)
    public void fastSlCheck() {
        try { strangleAdjust.fastSlCheck(); }
        catch (Throwable t) { log.error("[StrangleAdjustScheduler] fastSlCheck threw: {}", t.getMessage(), t); }
    }
}
