package com.rydytrader.autotrader.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TradingStateStore {

    private static final Logger log = LoggerFactory.getLogger(TradingStateStore.class);

    private volatile boolean tradingEnabled = true;

    public boolean isTradingEnabled() { return tradingEnabled; }

    public void setTradingEnabled(boolean enabled) {
        this.tradingEnabled = enabled;
        log.info("Trading {}", enabled ? "ENABLED" : "DISABLED (kill switch active)");
    }
}
