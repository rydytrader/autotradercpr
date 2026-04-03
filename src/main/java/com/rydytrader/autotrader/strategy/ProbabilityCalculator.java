package com.rydytrader.autotrader.strategy;

/**
 * Determines signal probability (HPT/MPT/LPT) based on strategy-specific criteria
 * (e.g. weekly trend alignment for CPR, trend strength for MA crossover).
 */
public interface ProbabilityCalculator {

    /**
     * Calculate probability for a signal.
     * @param symbol   the trading symbol
     * @param direction "BUY" or "SELL"
     * @return "HPT", "MPT", or "LPT"
     */
    String calculate(String symbol, String direction);
}
