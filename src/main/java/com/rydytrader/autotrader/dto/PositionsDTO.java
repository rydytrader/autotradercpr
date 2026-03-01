package com.rydytrader.autotrader.dto;

public class PositionsDTO {

    private String symbol;
    private int qty;
    private String side;
    private double avgPrice;
    private double ltp;
    private double pnl;
    private String setup;
    private String entryTime;

    public PositionsDTO(String symbol, int qty, String side, double avgPrice, double ltp, double pnl) {
        this(symbol, qty, side, avgPrice, ltp, pnl, "", "");
    }

    public PositionsDTO(String symbol, int qty, String side, double avgPrice, double ltp, double pnl, String setup) {
        this(symbol, qty, side, avgPrice, ltp, pnl, setup, "");
    }

    public PositionsDTO(String symbol, int qty, String side, double avgPrice, double ltp, double pnl, String setup, String entryTime) {
        this.symbol    = symbol;
        this.qty       = qty;
        this.side      = side;
        this.avgPrice  = avgPrice;
        this.ltp       = ltp;
        this.pnl       = pnl;
        this.setup     = setup != null ? setup : "";
        this.entryTime = entryTime != null ? entryTime : "";
    }

    public String getSymbol()    { return symbol; }
    public int    getQty()       { return qty; }
    public String getSide()      { return side; }
    public double getAvgPrice()  { return avgPrice; }
    public double getLtp()       { return ltp; }
    public double getPnl()       { return pnl; }
    public String getSetup()     { return setup; }
    public String getEntryTime() { return entryTime; }
}