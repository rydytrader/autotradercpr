package com.rydytrader.autotrader.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.PollingService;

/**
 * Generic trading endpoints that operate on any symbol (equity-or-options agnostic).
 * Now that the equity breakout pipeline is gone, this file is a thin wrapper around
 * positions + status + logs. Straddle-specific endpoints live on {@code StraddleController}.
 */
@RestController
public class TradingController {

    private final PollingService     pollingService;
    private final OrderService       orderService;
    private final EventService       eventService;
    private final MarketDataService  marketDataService;
    private final OrderEventService  orderEventService;

    public TradingController(PollingService pollingService,
                              OrderService orderService,
                              EventService eventService,
                              MarketDataService marketDataService,
                              OrderEventService orderEventService) {
        this.pollingService     = pollingService;
        this.orderService       = orderService;
        this.eventService       = eventService;
        this.marketDataService  = marketDataService;
        this.orderEventService  = orderEventService;
    }

    // ── SQUARE OFF ───────────────────────────────────────────────────────────────
    @PostMapping("/api/squareoff")
    public ResponseEntity<Map<String, Object>> squareOff(@RequestBody Map<String, Object> payload) {
        String symbol   = payload.get("symbol").toString();
        int    quantity = Integer.parseInt(payload.get("quantity").toString());
        boolean success = pollingService.squareOff(symbol, quantity);
        return ResponseEntity.ok(Map.of("ok", success));
    }

    // ── POSITIONS ─────────────────────────────────────────────────────────────
    @GetMapping("/api/positions")
    public Map<String, Object> getPositions() {
        List<Map<String, Object>> positions = pollingService.fetchPositions().stream().map(p -> {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            double liveLtp = marketDataService.getLtp(p.getSymbol());
            double ltp = liveLtp > 0 ? liveLtp : p.getLtp();
            double pnl = "LONG".equals(p.getSide())
                ? (ltp - p.getAvgPrice()) * p.getQty()
                : (p.getAvgPrice() - ltp) * p.getQty();
            m.put("symbol",    p.getSymbol());
            m.put("qty",       p.getQty());
            m.put("side",      p.getSide());
            m.put("avgPrice",  p.getAvgPrice());
            m.put("ltp",       ltp);
            m.put("pnl",       pnl);
            m.put("setup",     p.getSetup());
            m.put("entryTime", p.getEntryTime());
            return m;
        }).collect(java.util.stream.Collectors.toList());

        double unrealizedPnl = positions.stream()
            .mapToDouble(p -> ((Number) p.get("pnl")).doubleValue()).sum();

        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("positions",    positions);
        result.put("lastSync",     pollingService.getLastSyncTime());
        result.put("unrealizedPnl",Math.round(unrealizedPnl * 100.0) / 100.0);
        return result;
    }

    // ── STATUS ────────────────────────────────────────────────────────────────
    @GetMapping("/status")
    public Map<String, String> getStatus() {
        return Map.of("status", pollingService.getConnectionStatus());
    }

    // ── EVENT LOG ─────────────────────────────────────────────────────────────
    @GetMapping("/api/events")
    public Map<String, Object> getEvents(
            @org.springframework.web.bind.annotation.RequestParam(name = "limit", defaultValue = "200") int limit) {
        List<String> all = eventService.getTradeLogs();
        int from = Math.max(0, all.size() - Math.max(1, limit));
        // Return most-recent first
        List<String> tail = new java.util.ArrayList<>(all.subList(from, all.size()));
        java.util.Collections.reverse(tail);
        return Map.of("events", tail, "total", all.size());
    }

    // Suppress unused warnings — orderService + eventService kept as dependencies in case
    // future straddle endpoints want direct access.
    @SuppressWarnings("unused")
    private Object unusedRef() { return new Object[]{ orderService, eventService, orderEventService }; }
}
