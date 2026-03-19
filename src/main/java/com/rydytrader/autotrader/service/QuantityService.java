package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.stereotype.Service;

/**
 * Computes the base trade quantity based on settings:
 * - Fixed mode (fixedQuantity != -1): uses the configured fixed value
 * - Capital mode (fixedQuantity == -1): qty = (capitalPerTrade × leverage) / closePrice
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

    /**
     * Computes the base quantity for a trade.
     *
     * @param fyersSymbol Fyers symbol (e.g. "NSE:HDFCBANK-EQ")
     * @param closePrice  current close price
     * @return even quantity (minimum 2)
     */
    public int computeBaseQty(String fyersSymbol, double closePrice) {
        int fixedQty = riskSettings.getFixedQuantity();

        if (fixedQty != -1) {
            return roundToEven(fixedQty);
        }

        // Capital-based mode
        double capital = riskSettings.getCapitalPerTrade();
        if (capital <= 0) {
            eventService.log("[WARN] capitalPerTrade not set, using minimum qty 2");
            return 2;
        }
        if (closePrice <= 0) {
            eventService.log("[WARN] closePrice <= 0, using minimum qty 2");
            return 2;
        }

        int leverage = marginDataService.getLeverage(fyersSymbol);
        double effectiveCapital = (capital * leverage) / 2.0; // divide by 2: broker blocks margin for both target + SL orders
        int rawQty = (int) (effectiveCapital / closePrice);
        int qty = roundDownToEven(rawQty);

        eventService.log("[INFO] Qty computed: capital=" + fmt(capital)
            + " × leverage=" + leverage + " / 2 / price=" + fmt(closePrice)
            + " = " + rawQty + " → " + qty + " (even)");

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
