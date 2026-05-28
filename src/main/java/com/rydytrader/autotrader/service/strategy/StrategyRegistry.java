package com.rydytrader.autotrader.service.strategy;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring-discovered registry of all {@link Strategy} implementations. Every {@code @Service}
 * that implements {@link Strategy} gets constructor-injected into the {@code strategies} list,
 * and we index them by {@link Strategy#id()} for O(1) lookup.
 *
 * <p>Adding a new strategy: declare a new {@code @Service} class implementing {@link Strategy}.
 * No code changes here, no registration code anywhere else. The {@link StrategyController},
 * settings endpoints, dashboard template, and sidebar nav all read from this registry.
 *
 * <p>Insertion order in the underlying map matches the order Spring resolves the list — which
 * for sibling {@code @Service} beans is the order they're declared in. Use {@code @Order} on
 * concrete strategies if a specific UI order is required.
 */
@Component
public class StrategyRegistry {

    private final Map<String, Strategy> byId;

    public StrategyRegistry(List<Strategy> strategies) {
        Map<String, Strategy> m = new LinkedHashMap<>();
        for (Strategy s : strategies) {
            if (s == null || s.id() == null || s.id().isBlank()) continue;
            m.put(s.id(), s);
        }
        this.byId = m;
    }

    /** @return strategy with the given id, or null if not registered */
    public Strategy get(String id) {
        return id == null ? null : byId.get(id);
    }

    /** All registered strategies in declaration order. */
    public Collection<Strategy> all() {
        return byId.values();
    }

    /** True when there are no registered strategies — e.g. during early boot. */
    public boolean isEmpty() {
        return byId.isEmpty();
    }
}
