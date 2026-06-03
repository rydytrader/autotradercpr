package com.rydytrader.autotrader.store.manual;

/**
 * One open manual-terminal position. Keyed in {@link ManualTerminalStore} by the entry
 * {@code orderId}. {@code filled} starts false on placement and flips true the moment
 * {@link com.rydytrader.autotrader.service.OrderEventService}'s WS push lands with
 * status=2 (Filled); {@code avgPrice} is the broker-confirmed traded price.
 */
public class ManualPosition {
    public String  orderId       = "";
    public String  symbol        = "";
    public String  side          = "";           // "BUY" or "SELL"
    public int     qty           = 0;            // already × NIFTY_LOT_SIZE (65)
    public double  avgPrice      = 0;            // broker-confirmed; 0 while pending
    public long    openMillis    = 0;
    public String  product       = "INTRADAY";
    public boolean filled        = false;
    /** Operator-set stop-loss in PREMIUM POINTS — distance from entry. Default 50. 0 means
     *  no SL. {@code ManualTerminalService.sweepStopLosses} computes the trigger as
     *  {@code avgPrice ± stopLossPts} depending on side and auto-closes on breach
     *  (SELL → close when LTP ≥ avgPrice + pts, BUY → close when LTP ≤ avgPrice − pts). */
    public double  stopLossPts   = 50;
    /** True once an SL-triggered close order has been placed for this position — guards the
     *  monitor against firing twice while the close fill is in flight. */
    public boolean slTriggered   = false;

    public ManualPosition() {}
}
