package com.rydytrader.autotrader.util;

/**
 * Calculates Indian equity cash intraday trading charges for a complete round-trip trade.
 *
 * Charges breakdown (NSE Cash Intraday):
 * - Brokerage: lower of ₹X or brokeragePct% per order (entry + exit calculated separately)
 * - STT: sttRate% on sell-side turnover
 * - Exchange transaction charges: exchangeRate% on total turnover (NSE cash)
 * - GST: gstRate% on (brokerage + exchange charges)
 * - SEBI charges: sebiRate ₹ per crore
 * - Stamp duty: stampDutyRate% on buy-side turnover
 */
public class ChargesCalculator {

    // Default rates (used when settings not available)
    private static final double DEF_BROKERAGE_PCT  = 0.03;
    private static final double DEF_STT_RATE       = 0.025;
    private static final double DEF_EXCHANGE_RATE  = 0.00345;
    private static final double DEF_GST_RATE       = 18.0;
    private static final double DEF_SEBI_RATE      = 10.0;
    private static final double DEF_STAMP_DUTY     = 0.003;

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
     * Calculate charges with default rates.
     */
    public static Result calculate(String side, int qty, double entryPrice, double exitPrice,
                                   double brokeragePerOrder) {
        return calculate(side, qty, entryPrice, exitPrice, brokeragePerOrder,
            DEF_STT_RATE, DEF_EXCHANGE_RATE, DEF_GST_RATE, DEF_SEBI_RATE, DEF_STAMP_DUTY, DEF_BROKERAGE_PCT);
    }

    /**
     * Calculate charges with configurable rates.
     *
     * @param side             "LONG" or "SHORT"
     * @param qty              number of units
     * @param entryPrice       entry price per unit
     * @param exitPrice        exit price per unit
     * @param brokeragePerOrder flat brokerage cap per order in ₹
     * @param sttRate          STT % on sell side (e.g. 0.025)
     * @param exchangeRate     Exchange % on total turnover (e.g. 0.00345)
     * @param gstRate          GST % on brokerage+exchange (e.g. 18.0)
     * @param sebiRate         SEBI ₹ per crore (e.g. 10.0)
     * @param stampDutyRate    Stamp duty % on buy side (e.g. 0.003)
     * @param brokeragePct     Brokerage % cap per order (e.g. 0.03)
     */
    public static Result calculate(String side, int qty, double entryPrice, double exitPrice,
                                   double brokeragePerOrder,
                                   double sttRate, double exchangeRate, double gstRate,
                                   double sebiRate, double stampDutyRate, double brokeragePct) {
        double entryTurnover = entryPrice * qty;
        double exitTurnover  = exitPrice * qty;
        double totalTurnover = entryTurnover + exitTurnover;

        double buyTurnover, sellTurnover;
        if ("LONG".equals(side)) {
            buyTurnover  = entryTurnover;
            sellTurnover = exitTurnover;
        } else {
            buyTurnover  = exitTurnover;
            sellTurnover = entryTurnover;
        }

        double brokPctRate = brokeragePct / 100.0;
        double buyBrokerage  = Math.min(brokeragePerOrder, buyTurnover * brokPctRate);
        double sellBrokerage = Math.min(brokeragePerOrder, sellTurnover * brokPctRate);
        double brokerage     = buyBrokerage + sellBrokerage;
        double stt             = sellTurnover * (sttRate / 100.0);
        double exchangeCharges = totalTurnover * (exchangeRate / 100.0);
        double gst             = (brokerage + exchangeCharges) * (gstRate / 100.0);
        double sebiCharges     = totalTurnover * (sebiRate / 1_00_00_000.0);
        double stampDuty       = buyTurnover * (stampDutyRate / 100.0);

        return new Result(brokerage, stt, exchangeCharges, gst, sebiCharges, stampDuty);
    }
}
