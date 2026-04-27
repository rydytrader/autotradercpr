package com.rydytrader.autotrader.dto;

/**
 * Composite trend snapshot for an index (NIFTY 50). Computed by IndexTrendService
 * from weekly CPR + daily CPR + 20 SMA position + 20 SMA slope + 200 SMA position.
 * Returned via REST API to drive the NIFTY card on the scanner page.
 */
public class IndexTrend {

    private String symbol;          // e.g. NSE:NIFTY50-INDEX
    private String displayName;     // e.g. NIFTY 50
    private double ltp;
    private double sma;             // current 20 SMA value (for display + position check)
    private double sma50;           // current 50 SMA value (medium-term trend)
    private double sma200;          // current 200 SMA value (long-term trend)
    private String smaPattern;      // "RAILWAY_UP", "RAILWAY_DOWN", "BRAIDED", or ""
    // HTF (60-min) SMAs — long-term trend
    private double htfSma20;
    private double htfSma50;
    private double htfSma200;
    private String htfSmaPattern;
    private String weeklyTrend;     // STRONGLY_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONGLY_BEARISH
    private String dailyTrend;
    private double smaSlopePct;     // % per candle
    private int weeklyScore;
    private int dailyScore;
    private int smaPositionScore;   // +1 if LTP > SMA(20), -1 if LTP < SMA(20), 0 otherwise
    private int slopeScore;
    private int sma200PositionScore; // +1 if LTP > SMA(200), -1 if LTP < SMA(200), 0 otherwise
    private int openHlScore;        // -1 if O=H (bearish), +1 if O=L (bullish), 0 otherwise
    private int smaCrossoverScore;  // 5-min SMA Alignment: ±2 full (20>50>200) / ±1 partial (one pair) / 0
    private int smaPatternScore;    // ±2 R-RTP / F-RTP, 0 BRAIDED/none
    private int htfPriceScore;      // HTF Price (LTP vs 1h SMA 20/50/200): ±2 full / ±1 partial / 0
    private int htfAlignmentScore;  // HTF Alignment (1h SMA 20>50>200): ±2 full / ±1 partial / 0
    private int htfPatternScore;    // ±2 R-RTP / F-RTP on 1h SMA 20/50, 0 otherwise
    private boolean openEqualsHigh; // true if day open ≈ day high (within 0.05%)
    private boolean openEqualsLow;  // true if day open ≈ day low (within 0.05%)
    private int totalScore;         // sum of all components
    private String state;           // STRONG_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONG_BEARISH
    private boolean dataAvailable;  // false if any input is missing (e.g. before market open)
    private double changePct;       // % change of LTP vs prev day close (drives state classification)
    private int breadthAdvancers;   // # NIFTY 50 stocks trading above prev close
    private int breadthDecliners;   // # below prev close
    private int breadthTotal;       // # with valid LTP + prev close (advancers + decliners + flat)

    public IndexTrend() {}

    public String getSymbol() { return symbol; }
    public void setSymbol(String v) { this.symbol = v; }
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String v) { this.displayName = v; }
    public double getLtp() { return ltp; }
    public void setLtp(double v) { this.ltp = v; }
    public double getSma() { return sma; }
    public void setSma(double v) { this.sma = v; }
    public double getSma50() { return sma50; }
    public void setSma50(double v) { this.sma50 = v; }
    public double getSma200() { return sma200; }
    public void setSma200(double v) { this.sma200 = v; }
    public String getSmaPattern() { return smaPattern; }
    public void setSmaPattern(String v) { this.smaPattern = v; }
    public double getHtfSma20() { return htfSma20; }
    public void setHtfSma20(double v) { this.htfSma20 = v; }
    public double getHtfSma50() { return htfSma50; }
    public void setHtfSma50(double v) { this.htfSma50 = v; }
    public double getHtfSma200() { return htfSma200; }
    public void setHtfSma200(double v) { this.htfSma200 = v; }
    public String getHtfSmaPattern() { return htfSmaPattern; }
    public void setHtfSmaPattern(String v) { this.htfSmaPattern = v; }
    public String getWeeklyTrend() { return weeklyTrend; }
    public void setWeeklyTrend(String v) { this.weeklyTrend = v; }
    public String getDailyTrend() { return dailyTrend; }
    public void setDailyTrend(String v) { this.dailyTrend = v; }
    public double getSmaSlopePct() { return smaSlopePct; }
    public void setSmaSlopePct(double v) { this.smaSlopePct = v; }
    public int getWeeklyScore() { return weeklyScore; }
    public void setWeeklyScore(int v) { this.weeklyScore = v; }
    public int getDailyScore() { return dailyScore; }
    public void setDailyScore(int v) { this.dailyScore = v; }
    public int getSmaPositionScore() { return smaPositionScore; }
    public void setSmaPositionScore(int v) { this.smaPositionScore = v; }
    public int getSlopeScore() { return slopeScore; }
    public void setSlopeScore(int v) { this.slopeScore = v; }
    public int getSma200PositionScore() { return sma200PositionScore; }
    public void setSma200PositionScore(int v) { this.sma200PositionScore = v; }
    public int getOpenHlScore() { return openHlScore; }
    public void setOpenHlScore(int v) { this.openHlScore = v; }
    public boolean isOpenEqualsHigh() { return openEqualsHigh; }
    public void setOpenEqualsHigh(boolean v) { this.openEqualsHigh = v; }
    public boolean isOpenEqualsLow() { return openEqualsLow; }
    public void setOpenEqualsLow(boolean v) { this.openEqualsLow = v; }
    public int getSmaCrossoverScore() { return smaCrossoverScore; }
    public void setSmaCrossoverScore(int v) { this.smaCrossoverScore = v; }
    public int getSmaPatternScore() { return smaPatternScore; }
    public void setSmaPatternScore(int v) { this.smaPatternScore = v; }
    public int getHtfPriceScore() { return htfPriceScore; }
    public void setHtfPriceScore(int v) { this.htfPriceScore = v; }
    public int getHtfAlignmentScore() { return htfAlignmentScore; }
    public void setHtfAlignmentScore(int v) { this.htfAlignmentScore = v; }
    public int getHtfPatternScore() { return htfPatternScore; }
    public void setHtfPatternScore(int v) { this.htfPatternScore = v; }
    public int getTotalScore() { return totalScore; }
    public void setTotalScore(int v) { this.totalScore = v; }
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
}
