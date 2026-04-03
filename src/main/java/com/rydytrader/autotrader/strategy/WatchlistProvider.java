package com.rydytrader.autotrader.strategy;

import java.util.List;
import java.util.Map;

/**
 * Provides watchlist data for the scanner/watchlist UI page.
 * Each strategy defines what symbols to show and what metadata to display.
 */
public interface WatchlistProvider {

    /** Get watchlist items with levels and metadata for the UI. */
    List<Map<String, Object>> getWatchlist();

    /** Get filter categories available for this strategy (e.g. "Narrow CPR", "Inside CPR"). */
    List<String> getFilterCategories();

    /** Get display name for the watchlist page title. */
    String getDisplayName();
}
