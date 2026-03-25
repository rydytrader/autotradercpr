package com.rydytrader.autotrader.dto;

public class TickData {
    private String fyersSymbol;   // e.g. "NSE:NIFTY50-INDEX"
    private String shortName;     // e.g. "NIFTY 50"
    private double ltp;
    private double change;
    private double changePercent;
    private double prevClose;
    private double open;
    private double high;
    private double low;
    private boolean hasPosition;

    public TickData() {}

    public TickData(String fyersSymbol, String shortName, double ltp, double prevClose) {
        this.fyersSymbol = fyersSymbol;
        this.shortName = shortName;
        this.ltp = ltp;
        this.prevClose = prevClose;
        if (prevClose > 0) {
            this.change = ltp - prevClose;
            this.changePercent = (change / prevClose) * 100;
        }
    }

    // --- getters & setters ---
    public String getFyersSymbol()           { return fyersSymbol; }
    public void setFyersSymbol(String v)     { this.fyersSymbol = v; }
    public String getShortName()             { return shortName; }
    public void setShortName(String v)       { this.shortName = v; }
    public double getLtp()                   { return ltp; }
    public void setLtp(double v)             { this.ltp = v; }
    public double getChange()                { return change; }
    public void setChange(double v)          { this.change = v; }
    public double getChangePercent()         { return changePercent; }
    public void setChangePercent(double v)   { this.changePercent = v; }
    public double getPrevClose()             { return prevClose; }
    public void setPrevClose(double v)       { this.prevClose = v; }
    public double getOpen()                  { return open; }
    public void setOpen(double v)            { this.open = v; }
    public double getHigh()                  { return high; }
    public void setHigh(double v)            { this.high = v; }
    public double getLow()                   { return low; }
    public void setLow(double v)             { this.low = v; }
    public boolean isHasPosition()           { return hasPosition; }
    public void setHasPosition(boolean v)    { this.hasPosition = v; }

    /** Recalculate change fields from current ltp and prevClose. */
    public void recalcChange() {
        if (prevClose > 0) {
            this.change = ltp - prevClose;
            this.changePercent = (change / prevClose) * 100;
        }
    }
}
