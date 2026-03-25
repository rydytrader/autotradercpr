package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.TradingStateStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class SimulatorController {

    private final TokenStore          tokenStore;
    private final EventService        eventService;
    private final PollingService       pollingService;
    private final TradingStateStore    tradingState;
    private final MarketDataService    marketDataService;
    private final OrderEventService    orderEventService;

    public SimulatorController(TokenStore tokenStore,
                                EventService eventService,
                                PollingService pollingService,
                                TradingStateStore tradingState,
                                MarketDataService marketDataService,
                                OrderEventService orderEventService) {
        this.tokenStore          = tokenStore;
        this.eventService        = eventService;
        this.pollingService      = pollingService;
        this.tradingState        = tradingState;
        this.marketDataService   = marketDataService;
        this.orderEventService   = orderEventService;
    }

    // ── LOGOUT ──────────────────────────────────────────────────────────────
    @PostMapping("/api/logout")
    public ResponseEntity<?> logout() {
        orderEventService.stop();
        marketDataService.stop();
        tokenStore.setAccessToken(null);
        pollingService.clearCachedPositions();
        PositionManager.resetAll();
        return ResponseEntity.ok(Map.of("redirect", "/"));
    }

    @GetMapping("/api/mode")
    public Map<String, String> getMode() {
        return Map.of("mode", "LIVE");
    }

    // ── KILL SWITCH ──────────────────────────────────────────────────────────
    @PostMapping("/api/trading/toggle")
    public Map<String, Object> toggleTrading() {
        boolean newState = !tradingState.isTradingEnabled();
        tradingState.setTradingEnabled(newState);
        eventService.log(newState
            ? "[SUCCESS] Trading ENABLED — kill switch deactivated"
            : "[WARNING] Trading DISABLED — kill switch activated");
        return Map.of("tradingEnabled", newState);
    }

    @GetMapping("/api/trading/status")
    public Map<String, Object> getTradingStatus() {
        return Map.of("tradingEnabled", tradingState.isTradingEnabled());
    }
}
