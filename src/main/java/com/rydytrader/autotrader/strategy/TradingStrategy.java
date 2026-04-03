package com.rydytrader.autotrader.strategy;

import com.rydytrader.autotrader.service.CandleAggregator;

/**
 * Main pluggable strategy interface. Each trading strategy (CPR Breakout, MA Crossover, etc.)
 * implements this interface. Strategies are auto-discovered by StrategyRegistry.
 */
public interface TradingStrategy extends CandleAggregator.CandleCloseListener, CandleAggregator.DailyResetListener {

    /** Unique strategy identifier (e.g. "CPR_BREAKOUT"). */
    String getName();

    /** Human-readable name for UI (e.g. "CPR Breakout"). */
    String getDisplayName();

    /** Called on startup to load data, register listeners, etc. */
    void initialize();

    /** Returns the LevelProvider for this strategy. */
    LevelProvider getLevelProvider();

    /** Returns the ProbabilityCalculator for this strategy. */
    ProbabilityCalculator getProbabilityCalculator();

    /** Returns the WatchlistProvider for this strategy. */
    WatchlistProvider getWatchlistProvider();
}
