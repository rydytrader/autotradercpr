package com.rydytrader.autotrader.dto;

/**
 * Current market regime for a stock (from HMM classification).
 */
public class RegimeState {

    private String symbol;
    private String regime;              // "BULLISH", "BEARISH", "NEUTRAL"
    private double confidence;          // 0.0 to 1.0
    private double[] statePosteriors;   // [bullish, bearish, neutral] probabilities
    private long lastUpdated;

    public RegimeState() {}

    public RegimeState(String symbol, String regime, double confidence,
                       double[] statePosteriors, long lastUpdated) {
        this.symbol = symbol;
        this.regime = regime;
        this.confidence = confidence;
        this.statePosteriors = statePosteriors;
        this.lastUpdated = lastUpdated;
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String v) { this.symbol = v; }

    public String getRegime() { return regime; }
    public void setRegime(String v) { this.regime = v; }

    public double getConfidence() { return confidence; }
    public void setConfidence(double v) { this.confidence = v; }

    public double[] getStatePosteriors() { return statePosteriors; }
    public void setStatePosteriors(double[] v) { this.statePosteriors = v; }

    public long getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(long v) { this.lastUpdated = v; }
}
