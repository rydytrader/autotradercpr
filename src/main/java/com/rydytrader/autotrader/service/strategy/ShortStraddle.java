package com.rydytrader.autotrader.service.strategy;

import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.entity.StrategyInstanceEntity;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.strategy.ShortStraddleStateStore;

/**
 * STRADDLE-typed instance of the manual position manager. Behaviour is identical to
 * {@link ShortStrangle}; the type only decides the sidebar group and the state-file family.
 */
public class ShortStraddle extends ShortStrangle {

    public ShortStraddle(StrategyInstanceEntity entity,
                         RiskSettingsStore riskSettings,
                         ShortStraddleStateStore stateStore,
                         EventService eventService,
                         TokenStore tokenStore,
                         FyersProperties fyersProperties,
                         FyersClientRouter fyersClient,
                         MarketDataService marketDataService,
                         OrderService orderService,
                         MarketHolidayService marketHolidayService,
                         com.rydytrader.autotrader.repository.StrategySessionRepository sessionRepo,
                         com.rydytrader.autotrader.repository.StrategyTradeRepository tradeRepo,
                         OrderEventService orderEventService,
                         BalancedAtmSelector atmSelector) {
        super(entity, riskSettings, stateStore, eventService, tokenStore, fyersProperties, fyersClient,
              marketDataService, orderService, marketHolidayService, sessionRepo, tradeRepo,
              orderEventService, atmSelector);
    }

    @Override public String strategyType() { return "STRADDLE"; }
}
