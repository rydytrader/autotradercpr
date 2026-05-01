package com.rydytrader.autotrader.dto;

/**
 * Trend snapshot for an index (NIFTY 50). State is set entirely by NIFTY 50 breadth
 * (advancers vs decliners → 7-tier ADD score). Returned via REST API to drive the
 * NIFTY card on the scanner page.
 */
public class IndexTrend {

    private String symbol;          // e.g. NSE:NIFTY50-INDEX
    private String displayName;     // e.g. NIFTY 50
    private double ltp;
    private String state;           // BULLISH / BEARISH / SIDEWAYS / NEUTRAL — set from CPR + 5m SMA factors
    private boolean dataAvailable;  // false if any input is missing (e.g. before market open)
    private double changePct;       // % change of LTP vs prev day close
    private int breadthAdvancers;   // # NIFTY 50 stocks trading above prev close
    private int breadthDecliners;   // # below prev close
    private int breadthTotal;       // # with valid LTP + prev close
    private int addScore;           // ADD = advancers count scaled to 50-stock universe — drives 7-tier state
    // Three sticky factors driving the NIFTY trend state. All TRUE → BULLISH;
    // all FALSE → BEARISH; null on any factor or mixed → SIDEWAYS (unless CPR is null,
    // which is NEUTRAL).
    // Factor 1: NIFTY LTP vs daily CPR. true = above max(TC, BC), false = below min(TC, BC),
    // null = inside CPR or no CPR data.
    private Boolean cprBullish;
    // Factor 2: NIFTY LTP vs 5-min SMA 20 and 50. true = price > both, false = price < both,
    // null = between (mixed) or SMAs not yet seeded.
    private Boolean smaPriceBullish;
    // Factor 3: 5-min SMA 20 vs SMA 50 ordering. true = 20 > 50, false = 20 < 50, null = SMAs
    // not yet seeded.
    private Boolean smaAlignBullish;

    public IndexTrend() {}

    public String getSymbol() { return symbol; }
    public void setSymbol(String v) { this.symbol = v; }
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String v) { this.displayName = v; }
    public double getLtp() { return ltp; }
    public void setLtp(double v) { this.ltp = v; }
    public String getState() { return state; }
    public void setState(String v) { this.state = v; }
    public boolean isDataAvailable() { return dataAvailable; }
    public void setDataAvailable(boolean v) { this.dataAvailable = v; }
    public double getChangePct() { return changePct; }
    public void setChangePct(double v) { this.changePct = v; }
    public int getBreadthAdvancers() { return breadthAdvancers; }
    public void setBreadthAdvancers(int v) { this.breadthAdvancers = v; }
    public int getBreadthDecliners() { return breadthDecliners; }
    public void setBreadthDecliners(int v) { this.breadthDecliners = v; }
    public int getBreadthTotal() { return breadthTotal; }
    public void setBreadthTotal(int v) { this.breadthTotal = v; }
    public int getAddScore() { return addScore; }
    public void setAddScore(int v) { this.addScore = v; }
    public Boolean getCprBullish() { return cprBullish; }
    public void setCprBullish(Boolean v) { this.cprBullish = v; }
    public Boolean getSmaPriceBullish() { return smaPriceBullish; }
    public void setSmaPriceBullish(Boolean v) { this.smaPriceBullish = v; }
    public Boolean getSmaAlignBullish() { return smaAlignBullish; }
    public void setSmaAlignBullish(Boolean v) { this.smaAlignBullish = v; }
}
