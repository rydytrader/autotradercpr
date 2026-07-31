package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Drives the {@code tick} + {@code fastSlCheck} loops for every registered
 * {@link Strategy} bean.
 *
 * <p>Two loops:
 * <ul>
 *   <li>{@code tick} — slow loop (~5 s). Day rollover + timed squareoff + portfolio risk gate.</li>
 *   <li>{@code fastSlCheck} — fast loop (~500 ms). Tick-based SL watcher on open positions.</li>
 * </ul>
 *
 * <p>Entry signal evaluation runs from the {@link CandleAggregator} callback on 3-min close
 * (registered by each strategy's {@code @PostConstruct}), not from the scheduler.
 *
 * <p>Multi-strategy aware: Spring injects every {@code @Service implements Strategy}
 * bean as a {@code List<Strategy>}. Adding a new strategy costs zero scheduler changes
 * — the new bean shows up in the list on next boot.
 */
@Component
public class StrategyScheduler {

    private static final Logger log = LoggerFactory.getLogger(StrategyScheduler.class);

    private final List<Strategy> strategies;

    public StrategyScheduler(List<Strategy> strategies) {
        this.strategies = strategies == null ? List.of() : strategies;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        for (Strategy s : strategies) {
            if (s == null) continue;
            try { s.tick(); }
            catch (Throwable t) {
                log.error("[StrategyScheduler] {}.tick threw: {}", s.id(), t.getMessage(), t);
            }
        }
    }

    @Scheduled(fixedDelay = 500)
    public void fastSlCheck() {
        for (Strategy s : strategies) {
            if (s == null) continue;
            try { s.fastSlCheck(); }
            catch (Throwable t) {
                log.error("[StrategyScheduler] {}.fastSlCheck threw: {}", s.id(), t.getMessage(), t);
            }
        }
    }
}
