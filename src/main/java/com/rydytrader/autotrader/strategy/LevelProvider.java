package com.rydytrader.autotrader.strategy;

import java.util.List;
import java.util.Map;

/**
 * Provides price levels for a symbol. Each strategy has its own level provider
 * (e.g. CPR levels, pivot points, support/resistance, moving averages).
 */
public interface LevelProvider {

    /** Get all levels for a symbol (e.g. "R1"→245.5, "S1"→240.0). */
    Map<String, Double> getLevels(String symbol);

    /** Get symbols that this strategy is interested in scanning. */
    List<String> getWatchlistSymbols();

    /** Refresh level data (e.g. fetch new bhavcopy, recalculate pivots). */
    void refresh();
}
