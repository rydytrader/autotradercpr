package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.stereotype.Service;

/**
 * Computes the base trade quantity based on settings:
 * - Fixed mode (fixedQuantity != -1): uses the configured fixed value
 * - Risk mode (fixedQuantity == -1): qty = riskPerTrade / |entry - SL|, capped by (capitalPerTrade × leverage) / 2 / price
 *
 * Quantity is always rounded to the nearest even number (min 2)
 * because the bot halves qty for R3/S3/R4/S4 extreme-level signals.
 */
@Service
public class QuantityService {

    private final RiskSettingsStore riskSettings;
    private final MarginDataService marginDataService;
    private final EventService eventService;

    public QuantityService(RiskSettingsStore riskSettings,
                           MarginDataService marginDataService,
                           EventService eventService) {
        this.riskSettings = riskSettings;
        this.marginDataService = marginDataService;
        this.eventService = eventService;
    }

    /** Backward-compat overload: returns fixed qty or minimum 2 when SL is unknown. */
    public int computeBaseQty(String fyersSymbol, double closePrice) {
        return computeBaseQty(fyersSymbol, closePrice, 0, "");
    }

    /** Backward-compat overload without setup. */
    public int computeBaseQty(String fyersSymbol, double closePrice, double slPrice) {
        return computeBaseQty(fyersSymbol, closePrice, slPrice, "");
    }

    /**
     * Computes the base quantity for a trade.
     *
     * @param fyersSymbol Fyers symbol (e.g. "NSE:HDFCBANK-EQ")
     * @param closePrice  current close/entry price
     * @param slPrice     stop loss price (0 if unknown — falls back to min qty in risk mode)
     * @param setup       breakout setup (e.g. "BUY_ABOVE_R1") — included in the log line for traceability
     * @return even quantity (minimum 2)
     */
    public int computeBaseQty(String fyersSymbol, double closePrice, double slPrice, String setup) {
        int fixedQty = riskSettings.getFixedQuantity();

        String tag = fyersSymbol + (setup != null && !setup.isEmpty() ? " " + setup : "");

        if (fixedQty != -1) {
            return roundToEven(fixedQty);
        }

        // Risk-based mode
        double riskPerTrade = riskSettings.getRiskPerTrade();
        double riskPerShare = Math.abs(closePrice - slPrice);
        if (riskPerTrade <= 0 || riskPerShare <= 0) {
            eventService.log("[WARN] " + tag + " riskPerTrade=" + fmt(riskPerTrade) + " riskPerShare=" + fmt(riskPerShare) + ", using minimum qty 2");
            return 2;
        }

        int riskQty = (int) (riskPerTrade / riskPerShare);

        // Capital cap: broker blocks margin for both SL+Target, so /2
        double capital = riskSettings.getCapitalPerTrade();
        int leverage = marginDataService.getLeverage(fyersSymbol);
        int capitalCapQty = Integer.MAX_VALUE;
        if (capital > 0 && closePrice > 0) {
            double effectiveCapital = (capital * leverage) / 2.0;
            capitalCapQty = (int) (effectiveCapital / closePrice);
        }

        int finalRawQty = Math.min(riskQty, capitalCapQty);
        int qty = roundDownToEven(finalRawQty);

        String capNote = (riskQty > capitalCapQty) ? " [CAPITAL-CAPPED from " + riskQty + "]" : "";
        eventService.log("[INFO] " + tag + " Qty computed: risk=" + fmt(riskPerTrade)
            + " / riskPerShare=" + fmt(riskPerShare)
            + " = " + riskQty
            + " | capitalCap=(" + fmt(capital) + "×" + leverage + ")/2/" + fmt(closePrice) + "=" + capitalCapQty
            + capNote
            + " → " + qty + " (even)");

        return qty;
    }

    /** Rounds to nearest even, minimum 2. Odd values round up. */
    private static int roundToEven(int n) {
        if (n <= 0) return 2;
        return Math.max(2, n % 2 != 0 ? n + 1 : n);
    }

    /** Rounds down to nearest even, minimum 2. */
    private static int roundDownToEven(int n) {
        return Math.max(2, (n / 2) * 2);
    }

    private static String fmt(double v) {
        return String.format("%.2f", v);
    }
}
