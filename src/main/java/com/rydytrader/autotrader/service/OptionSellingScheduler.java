package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.OptionSelling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the OptionSelling singleton's tick + fast-SL loops.
 *
 * <p>Two loops:
 * <ul>
 *   <li>{@code tick} — slow loop (~5 s). Day rollover + timed squareoff + portfolio risk gate.</li>
 *   <li>{@code fastSlCheck} — fast loop (~500 ms). Tick-based SL watcher on open shorts.</li>
 * </ul>
 *
 * <p>Entry signal evaluation runs from the {@link CandleAggregator} callback on 3-min close
 * (registered by {@code OptionSelling.@PostConstruct}), not from the scheduler.
 */
@Component
public class OptionSellingScheduler {

    private static final Logger log = LoggerFactory.getLogger(OptionSellingScheduler.class);

    private final OptionSelling optionSelling;

    public OptionSellingScheduler(OptionSelling optionSelling) {
        this.optionSelling = optionSelling;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        try { optionSelling.tick(); }
        catch (Throwable t) { log.error("[OptionSellingScheduler] tick threw: {}", t.getMessage(), t); }
    }

    @Scheduled(fixedDelay = 500)
    public void fastSlCheck() {
        try { optionSelling.fastSlCheck(); }
        catch (Throwable t) { log.error("[OptionSellingScheduler] fastSlCheck threw: {}", t.getMessage(), t); }
    }
}
