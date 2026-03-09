package com.rydytrader.autotrader.store;

import org.springframework.stereotype.Component;

@Component
public class TradingStateStore {

    private volatile boolean tradingEnabled = true;

    public boolean isTradingEnabled() { return tradingEnabled; }

    public void setTradingEnabled(boolean enabled) {
        this.tradingEnabled = enabled;
        System.out.println("▶ Trading " + (enabled ? "ENABLED" : "DISABLED (kill switch active)"));
    }
}
