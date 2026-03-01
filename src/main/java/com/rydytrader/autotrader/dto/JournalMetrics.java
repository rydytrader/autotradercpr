package com.rydytrader.autotrader.dto;

import java.util.List;
import java.util.Map;

public class JournalMetrics {

    // ── SUMMARY ───────────────────────────────────
    private int    totalTrades;
    private int    wins;
    private int    losses;
    private int    breakeven;
    private double winRate;          // %
    private double grossProfit;
    private double grossLoss;
    private double netPnl;
    private double profitFactor;     // grossProfit / abs(grossLoss)
    private double expectancy;       // (avgWin * winRate) - (avgLoss * lossRate)
    private double avgWin;
    private double avgLoss;
    private double avgRR;            // avgWin / abs(avgLoss)
    private double maxWin;
    private double maxLoss;
    private int    maxConsecWins;
    private int    maxConsecLosses;

    // ── CHARTS ────────────────────────────────────
    private List<DailyPnl>         dailyPnl;       // bar chart
    private List<EquityPoint>      equityCurve;    // line chart
    private Map<String, Integer>   resultCounts;   // pie: PROFIT/LOSS/BREAKEVEN

    // ── BREAKDOWNS ────────────────────────────────
    private Map<String, SideStats>   sideBreakdown;   // LONG / SHORT
    private Map<String, ReasonStats> reasonBreakdown; // SL / TARGET / MANUAL

    // ── RAW TRADES ────────────────────────────────
    private List<TradeRecord> trades;

    // ── INNER CLASSES ─────────────────────────────
    public static class DailyPnl {
        public String date;
        public double pnl;
        public int    trades;
        public DailyPnl(String date, double pnl, int trades) {
            this.date = date; this.pnl = pnl; this.trades = trades;
        }
    }

    public static class EquityPoint {
        public String timestamp;
        public double cumPnl;
        public EquityPoint(String timestamp, double cumPnl) {
            this.timestamp = timestamp; this.cumPnl = cumPnl;
        }
    }

    public static class SideStats {
        public int    trades;
        public int    wins;
        public double pnl;
        public double winRate;
        public SideStats(int trades, int wins, double pnl) {
            this.trades  = trades;
            this.wins    = wins;
            this.pnl     = pnl;
            this.winRate = trades > 0 ? Math.round((wins * 100.0 / trades) * 10.0) / 10.0 : 0;
        }
    }

    public static class ReasonStats {
        public int    count;
        public double pnl;
        public double avgPnl;
        public ReasonStats(int count, double pnl) {
            this.count  = count;
            this.pnl    = pnl;
            this.avgPnl = count > 0 ? Math.round((pnl / count) * 100.0) / 100.0 : 0;
        }
    }

    // ── GETTERS / SETTERS ─────────────────────────
    public int    getTotalTrades()    { return totalTrades; }
    public int    getWins()           { return wins; }
    public int    getLosses()         { return losses; }
    public int    getBreakeven()      { return breakeven; }
    public double getWinRate()        { return winRate; }
    public double getGrossProfit()    { return grossProfit; }
    public double getGrossLoss()      { return grossLoss; }
    public double getNetPnl()         { return netPnl; }
    public double getProfitFactor()   { return profitFactor; }
    public double getExpectancy()     { return expectancy; }
    public double getAvgWin()         { return avgWin; }
    public double getAvgLoss()        { return avgLoss; }
    public double getAvgRR()          { return avgRR; }
    public double getMaxWin()         { return maxWin; }
    public double getMaxLoss()        { return maxLoss; }
    public int    getMaxConsecWins()  { return maxConsecWins; }
    public int    getMaxConsecLosses(){ return maxConsecLosses; }
    public List<DailyPnl>         getDailyPnl()        { return dailyPnl; }
    public List<EquityPoint>      getEquityCurve()     { return equityCurve; }
    public Map<String, Integer>   getResultCounts()    { return resultCounts; }
    public Map<String, SideStats>   getSideBreakdown()   { return sideBreakdown; }
    public Map<String, ReasonStats> getReasonBreakdown() { return reasonBreakdown; }
    public List<TradeRecord>      getTrades()          { return trades; }

    public void setTotalTrades(int v)    { totalTrades = v; }
    public void setWins(int v)           { wins = v; }
    public void setLosses(int v)         { losses = v; }
    public void setBreakeven(int v)      { breakeven = v; }
    public void setWinRate(double v)     { winRate = v; }
    public void setGrossProfit(double v) { grossProfit = v; }
    public void setGrossLoss(double v)   { grossLoss = v; }
    public void setNetPnl(double v)      { netPnl = v; }
    public void setProfitFactor(double v){ profitFactor = v; }
    public void setExpectancy(double v)  { expectancy = v; }
    public void setAvgWin(double v)      { avgWin = v; }
    public void setAvgLoss(double v)     { avgLoss = v; }
    public void setAvgRR(double v)       { avgRR = v; }
    public void setMaxWin(double v)      { maxWin = v; }
    public void setMaxLoss(double v)     { maxLoss = v; }
    public void setMaxConsecWins(int v)  { maxConsecWins = v; }
    public void setMaxConsecLosses(int v){ maxConsecLosses = v; }
    public void setDailyPnl(List<DailyPnl> v)                  { dailyPnl = v; }
    public void setEquityCurve(List<EquityPoint> v)             { equityCurve = v; }
    public void setResultCounts(Map<String, Integer> v)         { resultCounts = v; }
    public void setSideBreakdown(Map<String, SideStats> v)      { sideBreakdown = v; }
    public void setReasonBreakdown(Map<String, ReasonStats> v)  { reasonBreakdown = v; }
    public void setTrades(List<TradeRecord> v)                  { trades = v; }
}