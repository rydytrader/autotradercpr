package com.rydytrader.autotrader.store.strategy;

import org.springframework.stereotype.Component;

/**
 * State store for {@code ShortStraddle} instances. Identical to {@link ShortStrangleStateStore}
 * (same {@code State} / {@code Leg} shape, same leg-book migration) but keeps its own in-memory
 * map and the historical {@code short-straddle-<id>-state.json} file name so existing instances
 * resume from their own files.
 */
@Component
public class ShortStraddleStateStore extends ShortStrangleStateStore {
    @Override
    protected String filePrefix() { return "short-straddle-"; }
}
