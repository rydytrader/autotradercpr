package com.rydytrader.autotrader.util;

/**
 * Calculates Indian equity cash intraday trading charges for a complete round-trip trade.
 *
 * Charges breakdown (NSE Cash Intraday):
 * - Brokerage: lower of ₹X or 0.03% per order (entry + exit calculated separately)
 * - STT: 0.025% on sell-side turnover
 * - Exchange transaction charges: 0.00345% on total turnover (NSE cash)
 * - GST: 18% on (brokerage + exchange charges)
 * - SEBI charges: ₹10 per crore (0.0001%)
 * - Stamp duty: 0.003% on buy-side turnover
 */
public class ChargesCalculator {

    private static final double BROKERAGE_PCT      = 0.03 / 100.0;     // 0.03% per order
    private static final double STT_RATE           = 0.025 / 100.0;    // 0.025% on sell side
    private static final double EXCHANGE_RATE      = 0.00345 / 100.0;  // 0.00345% NSE cash
    private static final double GST_RATE           = 18.0 / 100.0;     // 18%
    private static final double SEBI_RATE          = 10.0 / 1_00_00_000.0; // ₹10 per crore
    private static final double STAMP_DUTY_RATE    = 0.003 / 100.0;    // 0.003%

    public static class Result {
        public final double brokerage;
        public final double stt;
        public final double exchangeCharges;
        public final double gst;
        public final double sebiCharges;
        public final double stampDuty;
        public final double totalCharges;

        public Result(double brokerage, double stt, double exchangeCharges,
                      double gst, double sebiCharges, double stampDuty) {
            this.brokerage       = round(brokerage);
            this.stt             = round(stt);
            this.exchangeCharges = round(exchangeCharges);
            this.gst             = round(gst);
            this.sebiCharges     = round(sebiCharges);
            this.stampDuty       = round(stampDuty);
            this.totalCharges    = round(brokerage + stt + exchangeCharges + gst + sebiCharges + stampDuty);
        }

        private static double round(double v) {
            return Math.round(v * 100.0) / 100.0;
        }
    }

    /**
     * Calculate charges for a complete round-trip trade (entry + exit).
     *
     * @param side             "LONG" or "SHORT"
     * @param qty              number of units
     * @param entryPrice       entry price per unit
     * @param exitPrice        exit price per unit
     * @param brokeragePerOrder flat brokerage per order (e.g. 20.0 for Fyers)
     */
    public static Result calculate(String side, int qty, double entryPrice, double exitPrice,
                                   double brokeragePerOrder) {
        double entryTurnover = entryPrice * qty;
        double exitTurnover  = exitPrice * qty;
        double totalTurnover = entryTurnover + exitTurnover;

        // Determine buy-side and sell-side turnover
        double buyTurnover, sellTurnover;
        if ("LONG".equals(side)) {
            buyTurnover  = entryTurnover;
            sellTurnover = exitTurnover;
        } else {
            buyTurnover  = exitTurnover;   // short exit = buy
            sellTurnover = entryTurnover;  // short entry = sell
        }

        // Brokerage: lower of flat cap or 0.03% per leg, calculated separately
        double buyBrokerage  = Math.min(brokeragePerOrder, buyTurnover * BROKERAGE_PCT);
        double sellBrokerage = Math.min(brokeragePerOrder, sellTurnover * BROKERAGE_PCT);
        double brokerage     = buyBrokerage + sellBrokerage;
        double stt             = sellTurnover * STT_RATE;
        double exchangeCharges = totalTurnover * EXCHANGE_RATE;
        double gst             = (brokerage + exchangeCharges) * GST_RATE;
        double sebiCharges     = totalTurnover * SEBI_RATE;
        double stampDuty       = buyTurnover * STAMP_DUTY_RATE;

        return new Result(brokerage, stt, exchangeCharges, gst, sebiCharges, stampDuty);
    }
}
