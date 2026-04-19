package com.rydytrader.autotrader.dto;

/**
 * Composite trend snapshot for an index (NIFTY 50). Computed by IndexTrendService
 * from weekly CPR + daily CPR + 20 EMA position + 20 EMA slope + 200 EMA position.
 * Returned via REST API to drive the NIFTY card on the scanner page.
 */
public class IndexTrend {

    private String symbol;          // e.g. NSE:NIFTY50-INDEX
    private String displayName;     // e.g. NIFTY 50
    private double ltp;
    private double ema;             // current 20 EMA value (for display + position check)
    private double ema50;           // current 50 EMA value (medium-term trend)
    private double ema200;          // current 200 EMA value (long-term trend)
    private String emaPattern;      // "RAILWAY_UP", "RAILWAY_DOWN", "BRAIDED", or ""
    // HTF (75-min) EMAs — display only, long-term trend
    private double htfEma20;
    private double htfEma50;
    private double htfEma200;
    private String htfEmaPattern;
    private String weeklyTrend;     // STRONGLY_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONGLY_BEARISH
    private String dailyTrend;
    private double emaSlopePct;     // % per candle
    private int weeklyScore;
    private int dailyScore;
    private int emaPositionScore;   // +1 if LTP > EMA(20), -1 if LTP < EMA(20), 0 otherwise
    private int slopeScore;
    private int ema200PositionScore; // +1 if LTP > EMA(200), -1 if LTP < EMA(200), 0 otherwise
    private int openHlScore;        // -1 if O=H (bearish), +1 if O=L (bullish), 0 otherwise
    private int emaCrossoverScore;  // +2 if both 20 and 50 above 200, +1 if only 20 above, -2/-1 symmetric for bearish, 0 otherwise
    private int emaPatternScore;    // +1 R-RTP (rising railway), -1 F-RTP (falling railway), 0 for braided/none
    private boolean openEqualsHigh; // true if day open ≈ day high (within 0.05%)
    private boolean openEqualsLow;  // true if day open ≈ day low (within 0.05%)
    private int totalScore;         // sum of all components
    private String state;           // STRONG_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONG_BEARISH
    private boolean dataAvailable;  // false if any input is missing (e.g. before market open)
    private boolean weeklyReversalActive; // true if 75-min candle rejected at weekly R1/PWH or S1/PWL

    public IndexTrend() {}

    public String getSymbol() { return symbol; }
    public void setSymbol(String v) { this.symbol = v; }
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String v) { this.displayName = v; }
    public double getLtp() { return ltp; }
    public void setLtp(double v) { this.ltp = v; }
    public double getEma() { return ema; }
    public void setEma(double v) { this.ema = v; }
    public double getEma50() { return ema50; }
    public void setEma50(double v) { this.ema50 = v; }
    public double getEma200() { return ema200; }
    public void setEma200(double v) { this.ema200 = v; }
    public String getEmaPattern() { return emaPattern; }
    public void setEmaPattern(String v) { this.emaPattern = v; }
    public double getHtfEma20() { return htfEma20; }
    public void setHtfEma20(double v) { this.htfEma20 = v; }
    public double getHtfEma50() { return htfEma50; }
    public void setHtfEma50(double v) { this.htfEma50 = v; }
    public double getHtfEma200() { return htfEma200; }
    public void setHtfEma200(double v) { this.htfEma200 = v; }
    public String getHtfEmaPattern() { return htfEmaPattern; }
    public void setHtfEmaPattern(String v) { this.htfEmaPattern = v; }
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
    public int getEma200PositionScore() { return ema200PositionScore; }
    public void setEma200PositionScore(int v) { this.ema200PositionScore = v; }
    public int getOpenHlScore() { return openHlScore; }
    public void setOpenHlScore(int v) { this.openHlScore = v; }
    public boolean isOpenEqualsHigh() { return openEqualsHigh; }
    public void setOpenEqualsHigh(boolean v) { this.openEqualsHigh = v; }
    public boolean isOpenEqualsLow() { return openEqualsLow; }
    public void setOpenEqualsLow(boolean v) { this.openEqualsLow = v; }
    public int getEmaCrossoverScore() { return emaCrossoverScore; }
    public void setEmaCrossoverScore(int v) { this.emaCrossoverScore = v; }
    public int getEmaPatternScore() { return emaPatternScore; }
    public void setEmaPatternScore(int v) { this.emaPatternScore = v; }
    public int getTotalScore() { return totalScore; }
    public void setTotalScore(int v) { this.totalScore = v; }
    public String getState() { return state; }
    public void setState(String v) { this.state = v; }
    public boolean isDataAvailable() { return dataAvailable; }
    public void setDataAvailable(boolean v) { this.dataAvailable = v; }
    public boolean isWeeklyReversalActive() { return weeklyReversalActive; }
    public void setWeeklyReversalActive(boolean v) { this.weeklyReversalActive = v; }
}
