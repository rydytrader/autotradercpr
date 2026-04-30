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
    private String state;           // EXTREME_BULLISH … EXTREME_BEARISH — set from breadth
    private boolean dataAvailable;  // false if any input is missing (e.g. before market open)
    private double changePct;       // % change of LTP vs prev day close
    private int breadthAdvancers;   // # NIFTY 50 stocks trading above prev close
    private int breadthDecliners;   // # below prev close
    private int breadthTotal;       // # with valid LTP + prev close
    private int addScore;           // ADD = advancers count scaled to 50-stock universe — drives 7-tier state
    // Spot NIFTY LTP vs near-month NIFTY futures VWAP. Positive = NIFTY trading above intraday
    // volume-weighted average (bullish bias). Negative = below. null = no data (futures VWAP
    // not yet available — pre-market, low-volume early session, or missing subscription).
    private Boolean futVwapBullish;
    // NIFTY LTP vs NIFTY daily CPR. true = above max(TC, BC), false = below min(TC, BC).
    // null = inside CPR (between BC and TC) or no CPR data.
    private Boolean cprBullish;

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
    public Boolean getFutVwapBullish() { return futVwapBullish; }
    public void setFutVwapBullish(Boolean v) { this.futVwapBullish = v; }
    public Boolean getCprBullish() { return cprBullish; }
    public void setCprBullish(Boolean v) { this.cprBullish = v; }
}
