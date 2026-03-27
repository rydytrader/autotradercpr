package com.rydytrader.autotrader.dto;

import com.rydytrader.autotrader.util.ChargesCalculator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Immutable record of a completed trade, including entry/exit prices, P&amp;L, charges, and outcome.
 * Used both for in-memory storage and CSV persistence.
 */
public class TradeRecord {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final double DEFAULT_BROKERAGE = 20.0;

    private final String timestamp;
    private final String symbol;
    private final String side;
    private final int    qty;
    private final double entryPrice;
    private final double exitPrice;
    private final String exitReason;
    private final String setup;
    private final double pnl;
    private final double charges;
    private final double netPnl;
    private final String result;
    private final String description;

    public TradeRecord(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason) {
        this(symbol, side, qty, entryPrice, exitPrice, exitReason, "", DEFAULT_BROKERAGE, null);
    }

    public TradeRecord(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup) {
        this(symbol, side, qty, entryPrice, exitPrice, exitReason, setup, DEFAULT_BROKERAGE, null);
    }

    public TradeRecord(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup,
                       double brokeragePerOrder) {
        this(symbol, side, qty, entryPrice, exitPrice, exitReason, setup, brokeragePerOrder, null);
    }

    public TradeRecord(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup,
                       double brokeragePerOrder, String description) {
        this.timestamp  = LocalDateTime.now().format(FMT);
        this.symbol     = symbol;
        this.side       = side;
        this.qty        = qty;
        this.entryPrice = entryPrice;
        this.exitPrice  = exitPrice;
        this.exitReason = exitReason;
        this.setup      = setup != null ? setup : "";
        this.description = description;
        double raw = (exitPrice - entryPrice) * qty * ("SHORT".equals(side) ? -1 : 1);
        this.pnl     = Math.round(raw * 100.0) / 100.0;
        this.charges = ChargesCalculator.calculate(side, qty, entryPrice, exitPrice, brokeragePerOrder).totalCharges;
        this.netPnl  = Math.round((this.pnl - this.charges) * 100.0) / 100.0;
        this.result  = this.netPnl >= 0 ? "PROFIT" : "LOSS";
    }

    // Used when reloading from CSV (timestamp already known)
    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason) {
        this(timestamp, symbol, side, qty, entryPrice, exitPrice, exitReason, "", DEFAULT_BROKERAGE);
    }

    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup) {
        this(timestamp, symbol, side, qty, entryPrice, exitPrice, exitReason, setup, DEFAULT_BROKERAGE);
    }

    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup,
                       double brokeragePerOrder) {
        this(timestamp, symbol, side, qty, entryPrice, exitPrice, exitReason, setup, brokeragePerOrder, null);
    }

    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup,
                       double brokeragePerOrder, String description) {
        this.timestamp  = timestamp;
        this.symbol     = symbol;
        this.side       = side;
        this.qty        = qty;
        this.entryPrice = entryPrice;
        this.exitPrice  = exitPrice;
        this.exitReason = exitReason;
        this.setup      = setup != null ? setup : "";
        this.description = description;
        double raw = (exitPrice - entryPrice) * qty * ("SHORT".equals(side) ? -1 : 1);
        this.pnl     = Math.round(raw * 100.0) / 100.0;
        this.charges = ChargesCalculator.calculate(side, qty, entryPrice, exitPrice, brokeragePerOrder).totalCharges;
        this.netPnl  = Math.round((this.pnl - this.charges) * 100.0) / 100.0;
        this.result  = this.netPnl >= 0 ? "PROFIT" : "LOSS";
    }

    // Used when reloading from DB with pre-computed charges
    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup,
                       double charges, boolean fromCsv) {
        this(timestamp, symbol, side, qty, entryPrice, exitPrice, exitReason, setup, charges, fromCsv, null);
    }

    public TradeRecord(String timestamp, String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup,
                       double charges, boolean fromCsv, String description) {
        this.timestamp  = timestamp;
        this.symbol     = symbol;
        this.side       = side;
        this.qty        = qty;
        this.entryPrice = entryPrice;
        this.exitPrice  = exitPrice;
        this.exitReason = exitReason;
        this.setup      = setup != null ? setup : "";
        this.description = description;
        double raw = (exitPrice - entryPrice) * qty * ("SHORT".equals(side) ? -1 : 1);
        this.pnl     = Math.round(raw * 100.0) / 100.0;
        this.charges = charges;
        this.netPnl  = Math.round((this.pnl - this.charges) * 100.0) / 100.0;
        this.result  = this.netPnl >= 0 ? "PROFIT" : "LOSS";
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
    public double getCharges()    { return charges; }
    public double getNetPnl()     { return netPnl; }
    public String getResult()     { return result; }
    public String getDescription() { return description; }
}