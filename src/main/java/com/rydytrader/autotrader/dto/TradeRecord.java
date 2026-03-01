package com.rydytrader.autotrader.dto;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TradeRecord {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final String timestamp;
    private final String symbol;
    private final String side;
    private final int    qty;
    private final double entryPrice;
    private final double exitPrice;
    private final String exitReason;
    private final String setup;
    private final double pnl;
    private final String result;

    public TradeRecord(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason) {
        this(symbol, side, qty, entryPrice, exitPrice, exitReason, "");
    }

    public TradeRecord(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup) {
        this.timestamp  = LocalDateTime.now().format(FMT);
        this.symbol     = symbol;
        this.side       = side;
        this.qty        = qty;
        this.entryPrice = entryPrice;
        this.exitPrice  = exitPrice;
        this.exitReason = exitReason;
        this.setup      = setup != null ? setup : "";
        double raw = (exitPrice - entryPrice) * qty * ("SHORT".equals(side) ? -1 : 1);
        this.pnl    = Math.round(raw * 100.0) / 100.0;
        this.result = this.pnl >= 0 ? "PROFIT" : "LOSS";
    }

    // Used when reloading from CSV (timestamp already known)
    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason) {
        this(timestamp, symbol, side, qty, entryPrice, exitPrice, exitReason, "");
    }

    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup) {
        this.timestamp  = timestamp;
        this.symbol     = symbol;
        this.side       = side;
        this.qty        = qty;
        this.entryPrice = entryPrice;
        this.exitPrice  = exitPrice;
        this.exitReason = exitReason;
        this.setup      = setup != null ? setup : "";
        double raw = (exitPrice - entryPrice) * qty * ("SHORT".equals(side) ? -1 : 1);
        this.pnl    = Math.round(raw * 100.0) / 100.0;
        this.result = this.pnl >= 0 ? "PROFIT" : "LOSS";
    }

    public String getTimestamp()  { return timestamp; }
    public String getSymbol()     { return symbol; }
    public String getSide()       { return side; }
    public int    getQty()        { return qty; }
    public double getEntryPrice() { return entryPrice; }
    public double getExitPrice()  { return exitPrice; }
    public String getExitReason() { return exitReason; }
    public String getSetup()      { return setup; }
    public double getPnl()        { return pnl; }
    public String getResult()     { return result; }
}