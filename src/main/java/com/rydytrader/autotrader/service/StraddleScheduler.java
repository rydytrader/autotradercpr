package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.ShortStraddle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Replaces the {@code @Scheduled} methods that used to live on the singleton
 * {@code LegSlStrategy}. Iterates every enabled {@link ShortStraddle} instance held by
 * {@link StraddleInstanceManager} and invokes the matching lifecycle hook on each.
 *
 * <p>Per-instance failures are isolated — a {@code Throwable} from one instance is logged but
 * doesn't prevent the other instances from running on the same tick.
 */
@Component
public class StraddleScheduler {

    private static final Logger log = LoggerFactory.getLogger(StraddleScheduler.class);

    private final StraddleInstanceManager manager;

    public StraddleScheduler(StraddleInstanceManager manager) {
        this.manager = manager;
    }

    /** Slow tick — drives entries, SL checks, timed squareoff, day rollover. */
    @Scheduled(fixedDelay = 5000)
    public void tickAll() {
        for (ShortStraddle s : manager.allEnabled()) {
            try { s.tick(); }
            catch (Throwable t) { log.error("[scheduler] instance {} tick threw: {}", s.id(), t.getMessage(), t); }
        }
    }

    /** Fast tick — per-leg SL trigger check only. ~500ms cadence so detection latency is
     *  ~500ms instead of ~5s. */
    @Scheduled(fixedDelay = 500)
    public void fastSlCheckAll() {
        for (ShortStraddle s : manager.allEnabled()) {
            try { s.fastSlCheck(); }
            catch (Throwable t) { log.error("[scheduler] instance {} fastSlCheck threw: {}", s.id(), t.getMessage(), t); }
        }
    }

    /** 1-min combined-premium sampler — drives the per-instance dashboard chart. */
    @Scheduled(cron = "0 * * * * MON-FRI", zone = "Asia/Kolkata")
    public void sampleAll() {
        for (ShortStraddle s : manager.allEnabled()) {
            try { s.sampleCombinedPremium(); }
            catch (Throwable t) { log.error("[scheduler] instance {} sample threw: {}", s.id(), t.getMessage(), t); }
        }
    }
}
