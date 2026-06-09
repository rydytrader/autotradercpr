package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.service.strategy.StrategyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Unified scheduler that fans tick / fastSlCheck / sampleCombinedPremium across every
 * registered {@link Strategy} (straddles and strangles alike). Each strategy decides via its
 * own state machine whether the call is a no-op or fires actual work.
 *
 * <p>Per-instance failures are isolated — a {@code Throwable} from one instance is logged but
 * doesn't prevent the other instances from running on the same tick.
 *
 * <p>The class name {@code StraddleScheduler} survives for git history continuity. The
 * scheduler is strategy-agnostic — both {@code ShortStraddle} and {@code ShortStrangle}
 * instances tick through the same loops.
 */
@Component
public class StraddleScheduler {

    private static final Logger log = LoggerFactory.getLogger(StraddleScheduler.class);

    private final StrategyRegistry registry;

    public StraddleScheduler(StrategyRegistry registry) {
        this.registry = registry;
    }

    /** Slow tick — drives entries, SL checks, timed squareoff, day rollover. */
    @Scheduled(fixedDelay = 5000)
    public void tickAll() {
        for (Strategy s : registry.all()) {
            if (!s.isEnabled()) continue;
            try { s.tick(); }
            catch (Throwable t) { log.error("[scheduler] instance {} tick threw: {}", s.id(), t.getMessage(), t); }
        }
    }

    /** Fast tick — per-leg SL trigger check only. ~500ms cadence so detection latency is
     *  ~500ms instead of ~5s. */
    @Scheduled(fixedDelay = 500)
    public void fastSlCheckAll() {
        for (Strategy s : registry.all()) {
            if (!s.isEnabled()) continue;
            try { s.fastSlCheck(); }
            catch (Throwable t) { log.error("[scheduler] instance {} fastSlCheck threw: {}", s.id(), t.getMessage(), t); }
        }
    }

    /** 1-min combined-premium sampler — drives the per-instance dashboard chart. */
    @Scheduled(cron = "0 * * * * MON-FRI", zone = "Asia/Kolkata")
    public void sampleAll() {
        for (Strategy s : registry.all()) {
            if (!s.isEnabled()) continue;
            try { s.sampleCombinedPremium(); }
            catch (Throwable t) { log.error("[scheduler] instance {} sample threw: {}", s.id(), t.getMessage(), t); }
        }
    }
}
