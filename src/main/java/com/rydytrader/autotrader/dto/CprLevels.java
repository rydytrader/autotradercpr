package com.rydytrader.autotrader.dto;

public class CprLevels {

    private String symbol;
    private double high, low, close;
    private double pivot, tc, bc;
    private double r1, r2, r3, r4;
    private double s1, s2, s3, s4;
    private double ph, pl;
    private double cprWidth, cprWidthPct;
    private boolean narrowCpr;
    private double rangeZScore;       // legacy z-score field (kept for serialization compat, now 0)
    private double rangeAdrPct;       // previous day's range as % of 20-day ADR (drives SMALL/LARGE classification)
    private String narrowRangeType;   // "SMALL" / "LARGE" / null (only set for narrow/inside CPR)
    private long   volume;
    private double fiftyTwoWeekHigh, fiftyTwoWeekLow;
    private double turnover;          // previous day total traded value (₹)
    private double avgTurnover20;     // 20-day average turnover
    private double turnoverMultiple;  // turnover / avgTurnover20
    private long   avgVolume20;       // 20-day average volume
    private double volumeMultiple;    // volume / avgVolume20
    private double beta;              // stock beta vs NIFTY (25-day)
    private String capCategory;       // "LARGE", "MID", "SMALL"

    /** No-arg constructor for Jackson deserialization. */
    public CprLevels() {}

    public CprLevels(String symbol, double high, double low, double close) {
        this.symbol = symbol;
        this.high   = high;
        this.low    = low;
        this.close  = close;

        this.pivot = (high + low + close) / 3.0;
        this.bc    = (high + low) / 2.0;
        this.tc    = 2.0 * pivot - bc;

        this.r1 = 2.0 * pivot - low;
        this.s1 = 2.0 * pivot - high;

        double range = high - low;
        this.r2 = pivot + range;
        this.s2 = pivot - range;

        this.r3 = high + 2.0 * (pivot - low);
        this.s3 = low  - 2.0 * (high - pivot);

        this.r4 = r3 + (r2 - r1);
        this.s4 = s3 - (s1 - s2);

        this.ph = high;
        this.pl = low;

        this.cprWidth    = Math.abs(tc - bc);
        this.cprWidthPct = close > 0 ? cprWidth / close * 100.0 : 0;
        this.narrowCpr   = cprWidthPct < 0.1;
    }

    /** Round all calculated levels to the given tick size. */
    public void roundToTick(double tick) {
        if (tick <= 0) return;
        this.pivot = round(pivot, tick);
        this.tc    = round(tc, tick);
        this.bc    = round(bc, tick);
        this.r1    = round(r1, tick);
        this.r2    = round(r2, tick);
        this.r3    = round(r3, tick);
        this.r4    = round(r4, tick);
        this.s1    = round(s1, tick);
        this.s2    = round(s2, tick);
        this.s3    = round(s3, tick);
        this.s4    = round(s4, tick);
        this.cprWidth    = Math.abs(tc - bc);
        this.cprWidthPct = close > 0 ? cprWidth / close * 100.0 : 0;
        this.narrowCpr   = cprWidthPct < 0.1;
    }

    private static double round(double price, double tick) {
        return Math.round(price / tick) * tick;
    }

    public String  getSymbol()      { return symbol; }
    public double  getHigh()        { return high; }
    public double  getLow()         { return low; }
    public double  getClose()       { return close; }
    public double  getPivot()       { return pivot; }
    public double  getTc()          { return tc; }
    public double  getBc()          { return bc; }
    public double  getR1()          { return r1; }
    public double  getR2()          { return r2; }
    public double  getR3()          { return r3; }
    public double  getR4()          { return r4; }
    public double  getS1()          { return s1; }
    public double  getS2()          { return s2; }
    public double  getS3()          { return s3; }
    public double  getS4()          { return s4; }
    public double  getPh()          { return ph; }
    public double  getPl()          { return pl; }
    public double  getCprWidth()    { return cprWidth; }
    public double  getCprWidthPct() { return cprWidthPct; }
    public boolean isNarrowCpr()    { return narrowCpr; }
    public double  getRangeZScore()     { return rangeZScore; }
    public void    setRangeZScore(double v) { this.rangeZScore = v; }
    public double  getRangeAdrPct()     { return rangeAdrPct; }
    public void    setRangeAdrPct(double v) { this.rangeAdrPct = v; }
    public String  getNarrowRangeType() { return narrowRangeType; }
    public void    setNarrowRangeType(String v) { this.narrowRangeType = v; }

    public long    getVolume()             { return volume; }
    public void    setVolume(long v)       { this.volume = v; }
    public double  getFiftyTwoWeekHigh()   { return fiftyTwoWeekHigh; }
    public void    setFiftyTwoWeekHigh(double v) { this.fiftyTwoWeekHigh = v; }
    public double  getFiftyTwoWeekLow()    { return fiftyTwoWeekLow; }
    public void    setFiftyTwoWeekLow(double v)  { this.fiftyTwoWeekLow = v; }
    public double  getTurnover()          { return turnover; }
    public void    setTurnover(double v)  { this.turnover = v; }
    public double  getAvgTurnover20()     { return avgTurnover20; }
    public void    setAvgTurnover20(double v) { this.avgTurnover20 = v; }
    public double  getTurnoverMultiple()  { return turnoverMultiple; }
    public void    setTurnoverMultiple(double v) { this.turnoverMultiple = v; }
    public long    getAvgVolume20()       { return avgVolume20; }
    public void    setAvgVolume20(long v) { this.avgVolume20 = v; }
    public double  getVolumeMultiple()    { return volumeMultiple; }
    public void    setVolumeMultiple(double v) { this.volumeMultiple = v; }
    public double  getBeta()              { return beta; }
    public void    setBeta(double v)      { this.beta = v; }
    public String  getCapCategory()       { return capCategory; }
    public void    setCapCategory(String v) { this.capCategory = v; }
}
