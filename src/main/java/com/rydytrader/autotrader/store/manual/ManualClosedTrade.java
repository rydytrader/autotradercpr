package com.rydytrader.autotrader.store.manual;

/**
 * One completed manual-terminal cycle (entry → close). Appended to {@code recent} on each
 * close fill. {@code side} reflects the opening leg's direction; {@code pnl} is signed —
 * positive = profit. Used by the analytics page's "Adjustments" mini-card and the
 * calendar day-modal's per-day Adjustments sub-table.
 */
public class ManualClosedTrade {
    public String orderId       = "";    // entry orderId, kept for traceability
    public String symbol        = "";
    public String side          = "";    // "BUY" or "SELL" — opening direction
    public int    qty           = 0;
    public double openPrice     = 0;
    public double closePrice    = 0;
    public double pnl           = 0;     // (side=BUY ? closePrice-openPrice : openPrice-closePrice) × qty
    public long   openMillis    = 0;
    public long   closeMillis   = 0;
    public String note          = "";    // free text — e.g. "reconciled at boot"

    public ManualClosedTrade() {}
}
