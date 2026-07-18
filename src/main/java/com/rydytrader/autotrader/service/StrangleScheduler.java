package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Strangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the Strangle singleton's tick + fast-SL loops.
 *
 * <p>Two loops:
 * <ul>
 *   <li>{@code tick} — slow loop (~5 s). Day rollover, entry gate, timed squareoff,
 *       portfolio risk gate.</li>
 *   <li>{@code fastSlCheck} — fast loop (~500 ms). Tick-based SL watcher on open shorts.</li>
 * </ul>
 */
@Component
public class StrangleScheduler {

    private static final Logger log = LoggerFactory.getLogger(StrangleScheduler.class);

    private final Strangle strangle;

    public StrangleScheduler(Strangle strangle) {
        this.strangle = strangle;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        try { strangle.tick(); }
        catch (Throwable t) { log.error("[StrangleScheduler] tick threw: {}", t.getMessage(), t); }
    }

    @Scheduled(fixedDelay = 500)
    public void fastSlCheck() {
        try { strangle.fastSlCheck(); }
        catch (Throwable t) { log.error("[StrangleScheduler] fastSlCheck threw: {}", t.getMessage(), t); }
    }
}
