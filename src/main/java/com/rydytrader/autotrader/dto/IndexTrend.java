package com.rydytrader.autotrader.dto;

/**
 * Trend snapshot for the NIFTY 50 index. State is computed at every NIFTY 5-min candle
 * close from two sticky factors: NIFTY's just-closed candle close vs daily CPR, and
 * NIFTY's just-closed candle close vs its 5-min 20-period EMA.
 * Returned via REST API to drive the NIFTY card on the scanner page.
 */
public class IndexTrend {

    private String symbol;          // e.g. NSE:NIFTY50-INDEX
    private String displayName;     // e.g. NIFTY 50
    private double ltp;
    private String state;           // BULLISH / BEARISH / BULLISH_REVERSAL / BEARISH_REVERSAL / SIDEWAYS / NEUTRAL
    private boolean dataAvailable;  // false if any input is missing (e.g. before market open)
    private double changePct;       // % change of LTP vs prev day close
    private int breadthAdvancers;   // # NIFTY 50 stocks trading above prev close
    private int breadthDecliners;   // # below prev close
    private int breadthTotal;       // # with valid LTP + prev close
    private int addScore;           // ADD = advancers count scaled to 50-stock universe — drives 7-tier state
    // Factor 1 of the NIFTY trend state: NIFTY index 5-min close vs daily CPR.
    // true = above max(TC, BC), false = below min(TC, BC), null = inside CPR or no candle yet.
    private Boolean cprBullish;
    // NIFTY index 5-min close used for the CPR comparison (sticky — captured at last 5-min close).
    private double niftyClose;
    // NIFTY's daily CPR width as a % of price + a NARROW / NORMAL / WIDE label derived from
    // the adaptive CPR squeeze/wide multiplier thresholds. Display-only.
    private double cprWidthPct;
    private String cprWidthCategory;
    // NIFTY 5-min EMA 20 — drives the NIFTY card's "EMA 20: X" chip color (green when LTP
    // is above, red when below). Sent on every REST poll AND every SSE tick so the chip
    // remains visible after market close instead of going blank when ticks stop.
    // EMA20 is always-on as the second NIFTY trend factor — no user toggle.
    private double ema20;
    // NIFTY 14-period ATR on the scanner timeframe (5-min by default). Display-only — shown
    // as a tooltip on the NIFTY card's LTP element so the user can see the current volatility
    // scale without opening another page.
    private double atr;
    // Nearest NIFTY hurdle in trade direction (BULLISH/BULLISH_REVERSAL → buy-side levels
    // above LTP; BEARISH/BEARISH_REVERSAL → sell-side levels below). Drives the scanner
    // page's "Hurdle: …" chip on the NIFTY card. Null when state is SIDEWAYS / NEUTRAL or
    // when no hurdle exists in trade direction across all three filters (HTF / 5m / Virgin).
    private HurdleInfo hurdle;

    public static class HurdleInfo {
        private String level;     // e.g. "Daily R1+PDH" / "Weekly TC" / "Virgin CPR"
        private String category;  // "5m" | "HTF" | "Virgin"
        private String state;     // "WAITING" | "AHEAD_BLOCKED" | "AHEAD_CLEAR"

        public HurdleInfo() {}
        public HurdleInfo(String level, String category, String state) {
            this.level = level; this.category = category; this.state = state;
        }
        public String getLevel()    { return level; }
        public void   setLevel(String v) { this.level = v; }
        public String getCategory() { return category; }
        public void   setCategory(String v) { this.category = v; }
        public String getState()    { return state; }
        public void   setState(String v) { this.state = v; }
    }

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
    public double getNiftyClose() { return niftyClose; }
    public void setNiftyClose(double v) { this.niftyClose = v; }
    public double getCprWidthPct() { return cprWidthPct; }
    public void setCprWidthPct(double v) { this.cprWidthPct = v; }
    public String getCprWidthCategory() { return cprWidthCategory; }
    public void setCprWidthCategory(String v) { this.cprWidthCategory = v; }
    public double getEma20() { return ema20; }
    public void setEma20(double v) { this.ema20 = v; }
    public double getAtr() { return atr; }
    public void setAtr(double v) { this.atr = v; }
    public HurdleInfo getHurdle() { return hurdle; }
    public void setHurdle(HurdleInfo v) { this.hurdle = v; }
}
