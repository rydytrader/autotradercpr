package com.rydytrader.autotrader.service.strategy;

import com.rydytrader.autotrader.service.StraddleInstanceManager;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * Thin facade over {@link StraddleInstanceManager}. Every operator-created instance of
 * {@code ShortStraddle} is registered with the manager at boot or at create-time, and the
 * registry exposes them as {@link Strategy} for the existing controllers / templates / sidebar.
 *
 * <p>{@code StraddleInstanceManager} is injected lazily because it has a transitive ObjectProvider
 * dependency on services (MarketDataService, OrderService) that depend on the registry — the
 * lazy proxy breaks the circular wiring.
 */
@Component
public class StrategyRegistry {

    private final StraddleInstanceManager manager;

    public StrategyRegistry(@Lazy StraddleInstanceManager manager) {
        this.manager = manager;
    }

    /** @return strategy with the given id, or null if not registered */
    public Strategy get(String id) {
        return id == null ? null : manager.get(id);
    }

    /** All active instances, oldest first. */
    public Collection<Strategy> all() {
        return manager.allAsStrategy();
    }

    /** True when no instances are registered (fresh install, no instances created yet). */
    public boolean isEmpty() {
        return manager.all().isEmpty();
    }
}
