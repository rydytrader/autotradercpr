package com.rydytrader.autotrader.dto;

/**
 * Composite trend snapshot for an index (NIFTY 50). Computed by IndexTrendService
 * from weekly CPR + daily CPR + 20 EMA position + 20 EMA slope. Returned via REST API
 * to drive the NIFTY card on the scanner page.
 */
public class IndexTrend {

    private String symbol;          // e.g. NSE:NIFTY50-INDEX
    private String displayName;     // e.g. NIFTY 50
    private double ltp;
    private double ema;             // current 20 EMA value (for display + position check)
    private String weeklyTrend;     // STRONGLY_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONGLY_BEARISH
    private String dailyTrend;
    private double emaSlopePct;     // % per candle
    private int weeklyScore;
    private int dailyScore;
    private int emaPositionScore;   // +1 if LTP > EMA, -1 if LTP < EMA, 0 otherwise
    private int slopeScore;
    private int totalScore;         // sum of weekly + daily + emaPosition + slope
    private String state;           // STRONG_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONG_BEARISH
    private boolean dataAvailable;  // false if any input is missing (e.g. before market open)

    public IndexTrend() {}

    public String getSymbol() { return symbol; }
    public void setSymbol(String v) { this.symbol = v; }
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String v) { this.displayName = v; }
    public double getLtp() { return ltp; }
    public void setLtp(double v) { this.ltp = v; }
    public double getEma() { return ema; }
    public void setEma(double v) { this.ema = v; }
    public String getWeeklyTrend() { return weeklyTrend; }
    public void setWeeklyTrend(String v) { this.weeklyTrend = v; }
    public String getDailyTrend() { return dailyTrend; }
    public void setDailyTrend(String v) { this.dailyTrend = v; }
    public double getEmaSlopePct() { return emaSlopePct; }
    public void setEmaSlopePct(double v) { this.emaSlopePct = v; }
    public int getWeeklyScore() { return weeklyScore; }
    public void setWeeklyScore(int v) { this.weeklyScore = v; }
    public int getDailyScore() { return dailyScore; }
    public void setDailyScore(int v) { this.dailyScore = v; }
    public int getEmaPositionScore() { return emaPositionScore; }
    public void setEmaPositionScore(int v) { this.emaPositionScore = v; }
    public int getSlopeScore() { return slopeScore; }
    public void setSlopeScore(int v) { this.slopeScore = v; }
    public int getTotalScore() { return totalScore; }
    public void setTotalScore(int v) { this.totalScore = v; }
    public String getState() { return state; }
    public void setState(String v) { this.state = v; }
    public boolean isDataAvailable() { return dataAvailable; }
    public void setDataAvailable(boolean v) { this.dataAvailable = v; }
}
