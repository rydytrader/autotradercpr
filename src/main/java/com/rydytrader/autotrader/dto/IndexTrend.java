package com.rydytrader.autotrader.dto;

/**
 * Trend snapshot for the NIFTY 50 index. State is computed at every NIFTY 5-min candle
 * close from two sticky factors: NIFTY's just-closed candle close vs daily CPR, and
 * NIFTY futures' just-closed candle close vs that bar's stamped VWAP (Fyers ATP).
 * Returned via REST API to drive the NIFTY card on the scanner page.
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
    // Two sticky factors driving the NIFTY trend state. Both refreshed only at NIFTY's
    // 5-min candle close — read from the just-closed candle, NOT live LTP.
    // Factor 1: NIFTY index 5-min close vs daily CPR. true = above max(TC, BC),
    // false = below min(TC, BC), null = inside CPR or no candle yet.
    private Boolean cprBullish;
    // Factor 2: NIFTY futures 5-min close vs that bar's stamped VWAP (Fyers ATP).
    // true = futClose > futVwap, false = futClose < futVwap, null = candle missing / vwap 0.
    private Boolean futVwapBullish;
    // Underlying values used in the comparisons (sticky — captured at last 5-min close).
    private double niftyClose;   // NIFTY index 5-min close used for the CPR comparison
    private String futSymbol;    // active near-month NIFTY futures symbol (e.g., NSE:NIFTY26MARFUT)
    private double futClose;     // NIFTY futures 5-min close used for the VWAP comparison
    private double futVwap;      // futures' stamped VWAP at that bar's close (Fyers ATP)
    // NIFTY's daily CPR width as a % of price + a NARROW / NORMAL / WIDE label derived from
    // the user's narrowCprMaxWidth and insideCprMaxWidth scanner thresholds. Display-only.
    private double cprWidthPct;
    private String cprWidthCategory;
    // NIFTY 5-min SMA 20 — drives the NIFTY card's "SMA 20: X" chip color (green when LTP
    // is above, red when below). Sent on every REST poll AND every SSE tick so the chip
    // remains visible after market close instead of going blank when ticks stop.
    private double sma20;
    // True iff the user has enabled the NIFTY SMA20 factor in trend identification. UI
    // hides the SMA chip on the NIFTY card when this is false (the factor isn't being
    // used downstream, so there's no point showing the chip).
    private boolean sma20FactorEnabled;
    // NIFTY option-chain Max OI strikes refreshed every 15 minutes during market hours.
    // Max Call OI = intraday resistance hurdle; Max Put OI = intraday support hurdle.
    // Zero = not yet loaded.
    private double maxCallOiStrike;
    private long   maxCallOi;
    private double maxPutOiStrike;
    private long   maxPutOi;
    private String oiLastUpdated;

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
    public Boolean getFutVwapBullish() { return futVwapBullish; }
    public void setFutVwapBullish(Boolean v) { this.futVwapBullish = v; }
    public double getNiftyClose() { return niftyClose; }
    public void setNiftyClose(double v) { this.niftyClose = v; }
    public String getFutSymbol() { return futSymbol; }
    public void setFutSymbol(String v) { this.futSymbol = v; }
    public double getFutClose() { return futClose; }
    public void setFutClose(double v) { this.futClose = v; }
    public double getFutVwap() { return futVwap; }
    public void setFutVwap(double v) { this.futVwap = v; }
    public double getCprWidthPct() { return cprWidthPct; }
    public void setCprWidthPct(double v) { this.cprWidthPct = v; }
    public String getCprWidthCategory() { return cprWidthCategory; }
    public void setCprWidthCategory(String v) { this.cprWidthCategory = v; }
    public double getSma20() { return sma20; }
    public void setSma20(double v) { this.sma20 = v; }
    public boolean isSma20FactorEnabled() { return sma20FactorEnabled; }
    public void setSma20FactorEnabled(boolean v) { this.sma20FactorEnabled = v; }
    public double getMaxCallOiStrike() { return maxCallOiStrike; }
    public void setMaxCallOiStrike(double v) { this.maxCallOiStrike = v; }
    public long getMaxCallOi() { return maxCallOi; }
    public void setMaxCallOi(long v) { this.maxCallOi = v; }
    public double getMaxPutOiStrike() { return maxPutOiStrike; }
    public void setMaxPutOiStrike(double v) { this.maxPutOiStrike = v; }
    public long getMaxPutOi() { return maxPutOi; }
    public void setMaxPutOi(long v) { this.maxPutOi = v; }
    public String getOiLastUpdated() { return oiLastUpdated; }
    public void setOiLastUpdated(String v) { this.oiLastUpdated = v; }
}
