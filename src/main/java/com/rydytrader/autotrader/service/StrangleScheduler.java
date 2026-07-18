package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Strangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the Strangle singleton's tick + fast-SL loops.
 * Mirror of StrangleAdjustScheduler but bound to the simple Strangle bean.
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
