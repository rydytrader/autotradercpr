package com.rydytrader.autotrader.strategy;

import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Discovers and manages all registered trading strategies.
 * Strategies are auto-discovered as Spring beans implementing TradingStrategy.
 */
@Service
public class StrategyRegistry {

    private static final Logger log = LoggerFactory.getLogger(StrategyRegistry.class);

    private final Map<String, TradingStrategy> strategies = new LinkedHashMap<>();
    private final RiskSettingsStore riskSettings;

    public StrategyRegistry(List<TradingStrategy> allStrategies, RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
        for (TradingStrategy s : allStrategies) {
            strategies.put(s.getName(), s);
            log.info("[StrategyRegistry] Registered strategy: {} ({})", s.getName(), s.getDisplayName());
        }
        log.info("[StrategyRegistry] Total strategies: {}", strategies.size());
    }

    /** Get the currently active strategy based on settings. */
    public TradingStrategy getActive() {
        String type = riskSettings.getStrategyType();
        TradingStrategy active = strategies.get(type);
        if (active == null && !strategies.isEmpty()) {
            active = strategies.values().iterator().next();
        }
        return active;
    }

    /** Get all registered strategies. */
    public List<TradingStrategy> getAll() {
        return new ArrayList<>(strategies.values());
    }

    /** Get a strategy by name. */
    public TradingStrategy get(String name) {
        return strategies.get(name);
    }

    /** Get all strategy names for settings dropdown. */
    public List<Map<String, String>> getStrategyList() {
        List<Map<String, String>> list = new ArrayList<>();
        for (TradingStrategy s : strategies.values()) {
            list.add(Map.of("name", s.getName(), "displayName", s.getDisplayName()));
        }
        return list;
    }
}
