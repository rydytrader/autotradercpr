package com.rydytrader.autotrader.service.strategy;

import com.rydytrader.autotrader.service.StraddleInstanceManager;
import com.rydytrader.autotrader.service.StrangleInstanceManager;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Thin facade over the per-strategy-type instance managers
 * ({@link StraddleInstanceManager} + {@link StrangleInstanceManager}). Every operator-created
 * instance of {@code ShortStraddle} or {@code ShortStrangle} is registered with its respective
 * manager at boot or at create-time, and the registry exposes them as {@link Strategy} for the
 * existing controllers / templates / sidebar.
 *
 * <p>Both managers are injected lazily because they have a transitive ObjectProvider
 * dependency on services (MarketDataService, OrderService) that depend on the registry — the
 * lazy proxies break the circular wiring.
 *
 * <p>Strategy IDs ({@code inst-N}) are globally unique across both managers (they share the
 * same JPA-generated primary key sequence on {@code strategy_instances}), so a {@link #get(String)}
 * lookup tries the straddle manager first; on miss it tries the strangle manager. {@link #all()}
 * concatenates the contents of both.
 */
@Component
public class StrategyRegistry {

    private final StraddleInstanceManager straddles;
    private final StrangleInstanceManager strangles;

    public StrategyRegistry(@Lazy StraddleInstanceManager straddles,
                             @Lazy StrangleInstanceManager strangles) {
        this.straddles = straddles;
        this.strangles = strangles;
    }

    /** @return strategy with the given id, or null if not registered */
    public Strategy get(String id) {
        if (id == null) return null;
        Strategy s = straddles.get(id);
        return s != null ? s : strangles.get(id);
    }

    /** All active instances across both managers. Straddles first, then strangles. */
    public Collection<Strategy> all() {
        java.util.List<Strategy> out = new ArrayList<>();
        out.addAll(straddles.allAsStrategy());
        out.addAll(strangles.allAsStrategy());
        return out;
    }

    /** True when no instances are registered (fresh install, no instances created yet). */
    public boolean isEmpty() {
        return straddles.all().isEmpty() && strangles.all().isEmpty();
    }
}
