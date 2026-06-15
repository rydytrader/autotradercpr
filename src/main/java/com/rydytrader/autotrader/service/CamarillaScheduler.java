package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Camarilla;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the Camarilla singleton's tick + fast-SL loops.
 *
 * <p>Two loops:
 * <ul>
 *   <li>{@code tick} — slow loop (~5 s). Day rollover + timed squareoff.</li>
 *   <li>{@code fastSlCheck} — fast loop (~500 ms). Target-watcher (spot vs Camarilla level).</li>
 * </ul>
 *
 * <p>Entry signal evaluation runs from the {@link CandleAggregator} callback on 5-min close
 * (registered by {@code Camarilla.@PostConstruct}), not from the scheduler.
 */
@Component
public class CamarillaScheduler {

    private static final Logger log = LoggerFactory.getLogger(CamarillaScheduler.class);

    private final Camarilla camarilla;

    public CamarillaScheduler(Camarilla camarilla) {
        this.camarilla = camarilla;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        try { camarilla.tick(); }
        catch (Throwable t) { log.error("[CamarillaScheduler] tick threw: {}", t.getMessage(), t); }
    }

    @Scheduled(fixedDelay = 500)
    public void fastSlCheck() {
        try { camarilla.fastSlCheck(); }
        catch (Throwable t) { log.error("[CamarillaScheduler] fastSlCheck threw: {}", t.getMessage(), t); }
    }
}
