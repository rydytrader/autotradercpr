package com.rydytrader.autotrader.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Momentum metrics for a single stock — tracks whether it broke key levels with volume confirmation.
 */
public class MomentumMetrics {

    private String symbol;
    private double lastClose;
    private long   lastVolume;
    private double avgVolume20;
    private double volumeRatio;       // lastVolume / avgVolume20
    private double prevWeekHigh, prevWeekLow;
    private double prevMonthHigh, prevMonthLow;
    private double fiftyTwoWeekHigh, fiftyTwoWeekLow;
    private double marketCapCr;
    private List<String> tags = new ArrayList<>();

    public MomentumMetrics() {}

    public MomentumMetrics(String symbol) { this.symbol = symbol; }

    public String getSymbol() { return symbol; }
    public void setSymbol(String v) { this.symbol = v; }

    public double getLastClose() { return lastClose; }
    public void setLastClose(double v) { this.lastClose = v; }

    public long getLastVolume() { return lastVolume; }
    public void setLastVolume(long v) { this.lastVolume = v; }

    public double getAvgVolume20() { return avgVolume20; }
    public void setAvgVolume20(double v) { this.avgVolume20 = v; }

    public double getVolumeRatio() { return volumeRatio; }
    public void setVolumeRatio(double v) { this.volumeRatio = v; }

    public double getPrevWeekHigh() { return prevWeekHigh; }
    public void setPrevWeekHigh(double v) { this.prevWeekHigh = v; }

    public double getPrevWeekLow() { return prevWeekLow; }
    public void setPrevWeekLow(double v) { this.prevWeekLow = v; }

    public double getPrevMonthHigh() { return prevMonthHigh; }
    public void setPrevMonthHigh(double v) { this.prevMonthHigh = v; }

    public double getPrevMonthLow() { return prevMonthLow; }
    public void setPrevMonthLow(double v) { this.prevMonthLow = v; }

    public double getFiftyTwoWeekHigh() { return fiftyTwoWeekHigh; }
    public void setFiftyTwoWeekHigh(double v) { this.fiftyTwoWeekHigh = v; }

    public double getFiftyTwoWeekLow() { return fiftyTwoWeekLow; }
    public void setFiftyTwoWeekLow(double v) { this.fiftyTwoWeekLow = v; }

    public double getMarketCapCr() { return marketCapCr; }
    public void setMarketCapCr(double v) { this.marketCapCr = v; }

    public List<String> getTags() { return tags; }
    public void setTags(List<String> v) { this.tags = v; }
    public void addTag(String tag) { if (!tags.contains(tag)) tags.add(tag); }
    public boolean hasMomentumTag() { return !tags.isEmpty(); }
}
