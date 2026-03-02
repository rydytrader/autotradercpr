package com.rydytrader.autotrader.dto;

/**
 * Data transfer object representing a single open position as displayed in the UI.
 */
public class PositionsDTO {

    private String symbol;
    private int qty;
    private String side;
    private double avgPrice;
    private double ltp;
    private double pnl;
    private String setup;
    private String entryTime;

    /**
     * Convenience constructor without setup or entryTime.
     *
     * @param symbol   trading symbol
     * @param qty      position quantity
     * @param side     "LONG" or "SHORT"
     * @param avgPrice average entry price
     * @param ltp      last traded price
     * @param pnl      unrealized profit/loss
     */
    public PositionsDTO(String symbol, int qty, String side, double avgPrice, double ltp, double pnl) {
        this(symbol, qty, side, avgPrice, ltp, pnl, "", "");
    }

    /**
     * Convenience constructor without entryTime.
     *
     * @param symbol   trading symbol
     * @param qty      position quantity
     * @param side     "LONG" or "SHORT"
     * @param avgPrice average entry price
     * @param ltp      last traded price
     * @param pnl      unrealized profit/loss
     * @param setup    trade setup label (e.g. "CPR Bounce")
     */
    public PositionsDTO(String symbol, int qty, String side, double avgPrice, double ltp, double pnl, String setup) {
        this(symbol, qty, side, avgPrice, ltp, pnl, setup, "");
    }

    /**
     * Full constructor with all fields.
     *
     * @param symbol    trading symbol
     * @param qty       position quantity
     * @param side      "LONG" or "SHORT"
     * @param avgPrice  average entry price
     * @param ltp       last traded price
     * @param pnl       unrealized profit/loss
     * @param setup     trade setup label
     * @param entryTime HH:mm:ss timestamp when the entry order was filled
     */
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

    /** Returns the trading symbol (e.g. "NSE:NIFTY25FEBFUT"). */
    public String getSymbol()    { return symbol; }

    /** Returns the net position quantity (positive for LONG, negative for SHORT). */
    public int    getQty()       { return qty; }

    /** Returns the position direction: "LONG" or "SHORT". */
    public String getSide()      { return side; }

    /** Returns the average entry price for this position. */
    public double getAvgPrice()  { return avgPrice; }

    /** Returns the last traded price for the symbol. */
    public double getLtp()       { return ltp; }

    /** Returns the current unrealized profit/loss. */
    public double getPnl()       { return pnl; }

    /** Returns the trade setup label associated with this position. */
    public String getSetup()     { return setup; }

    /** Returns the HH:mm:ss timestamp when the entry order was filled. */
    public String getEntryTime() { return entryTime; }
}