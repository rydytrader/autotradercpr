package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.mock.MockState;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.service.TradeHistoryService;
import com.rydytrader.autotrader.store.ModeStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.TradingStateStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class SimulatorController {

    private final ModeStore          modeStore;
    private final TokenStore         tokenStore;
    private final MockState          mockState;
    private final EventService       eventService;
    private final TradeHistoryService tradeHistoryService;
    private final PollingService       pollingService;
    private final PositionStateStore   positionStateStore;
    private final TradingStateStore    tradingState;

    public SimulatorController(ModeStore modeStore,
                                TokenStore tokenStore,
                                MockState mockState,
                                EventService eventService,
                                TradeHistoryService tradeHistoryService,
                                PollingService pollingService,
                                PositionStateStore positionStateStore,
                                TradingStateStore tradingState) {
        this.modeStore          = modeStore;
        this.tokenStore         = tokenStore;
        this.mockState          = mockState;
        this.eventService       = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.pollingService      = pollingService;
        this.positionStateStore  = positionStateStore;
        this.tradingState        = tradingState;
    }

    // ── MODE SWITCH ───────────────────────────────────────────────────────────
    @PostMapping("/api/logout")
    public ResponseEntity<?> logout() {
        tokenStore.setAccessToken(null);
        mockState.resetAll();
        pollingService.clearCachedPositions();
        PositionManager.resetAll();
        return ResponseEntity.ok(Map.of("redirect", "/"));
    }

    @PostMapping("/api/mode/switch")
    public ResponseEntity<?> switchMode(@RequestBody Map<String, String> body) {
        String modeStr = body.get("mode");
        ModeStore.Mode newMode;
        try {
            newMode = ModeStore.Mode.valueOf(modeStr.toUpperCase());
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid mode: " + modeStr));
        }

        // Clear in-memory state from the OLD mode before switching
        pollingService.clearCachedPositions();
        PositionManager.resetAll();

        modeStore.setMode(newMode);
        eventService.reloadLogsForCurrentMode();
        tradeHistoryService.reloadForCurrentMode();

        if (newMode == ModeStore.Mode.SIMULATOR) {
            tokenStore.setAccessToken("SIM_TOKEN");
            pollingService.syncPositionOnce();
            pollingService.startPositionSync();
        } else {
            tokenStore.setAccessToken(null);
            mockState.resetAll();
        }

        return ResponseEntity.ok(Map.of(
            "mode",     newMode.name(),
            "redirect", newMode == ModeStore.Mode.SIMULATOR ? "/home" : "/"
        ));
    }

    @GetMapping("/api/mode")
    public Map<String, String> getMode() {
        return Map.of("mode", modeStore.getMode().name());
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

    // ── SIMULATOR STATE (for control panel) ───────────────────────────────────
    @GetMapping("/api/simulator/state")
    public Map<String, Object> getSimState() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("symbol",       mockState.getActiveSymbol());
        snap.put("currentPrice", mockState.getCurrentPrice());
        snap.put("autoFill",     mockState.isAutoFill());
        snap.put("fillDelayMs",  mockState.getFillDelayMs());
        snap.put("position",     mockState.getPosition());
        snap.put("eventLog",     mockState.getEventLog());

        // Open position symbols and per-symbol prices for the symbol selector
        List<String> openSymbols = mockState.getAllPositions().stream()
            .map(p -> (String) p.get("symbol"))
            .collect(java.util.stream.Collectors.toList());
        snap.put("openSymbols", openSymbols);
        snap.put("prices",      mockState.getPriceBySymbol());

        List<Map<String, Object>> orderList = new ArrayList<>();
        mockState.getAllOrders().forEach(o -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id",         o.get("id"));
            item.put("tag",        o.get("orderTag"));
            item.put("symbol",     o.get("symbol"));
            item.put("side",       o.get("side"));
            item.put("qty",        o.get("qty"));
            item.put("type",       o.get("type"));
            item.put("stopPrice",  o.get("stopPrice"));
            item.put("limitPrice", o.get("limitPrice"));
            item.put("status",     o.get("status"));
            item.put("tradedPrice",o.get("tradedPrice"));
            orderList.add(item);
        });
        snap.put("orders", orderList);
        return snap;
    }

    // ── SIMULATOR SETTINGS ────────────────────────────────────────────────────
    @PostMapping("/api/simulator/settings")
    public Map<String, Object> updateSettings(@RequestBody Map<String, Object> body) {
        if (body.containsKey("symbol"))      mockState.setActiveSymbol((String) body.get("symbol"));
        if (body.containsKey("price"))       mockState.setCurrentPrice(Double.parseDouble(body.get("price").toString()));
        if (body.containsKey("autoFill"))    mockState.setAutoFill((Boolean) body.get("autoFill"));
        if (body.containsKey("fillDelayMs")) mockState.setFillDelayMs(Integer.parseInt(body.get("fillDelayMs").toString()));
        return Map.of("ok", true);
    }

    @PostMapping("/api/simulator/price")
    public Map<String, Object> setPrice(@RequestBody Map<String, Object> body) {
        double price  = Double.parseDouble(body.get("price").toString());
        String symbol = body.containsKey("symbol") ? body.get("symbol").toString() : null;
        if (symbol != null && !symbol.isEmpty()) {
            mockState.setCurrentPrice(symbol, price);
            mockState.setActiveSymbol(symbol);
            mockState.log("Price [" + symbol + "] → " + price);
        } else {
            mockState.setCurrentPrice(price);
            mockState.log("Price → " + price);
        }
        return Map.of("ok", true, "price", price);
    }

    // ── SCENARIOS ─────────────────────────────────────────────────────────────
    @PostMapping("/api/simulator/scenario/sl-hit")
    public Map<String, Object> slHit(@RequestBody(required = false) Map<String, Object> body) {
        String symbol = resolveSymbol(body);
        mockState.triggerSlHit(symbol);
        return Map.of("ok", true);
    }

    @PostMapping("/api/simulator/scenario/target-hit")
    public Map<String, Object> targetHit(@RequestBody(required = false) Map<String, Object> body) {
        String symbol = resolveSymbol(body);
        mockState.triggerTargetHit(symbol);
        return Map.of("ok", true);
    }

    @PostMapping("/api/simulator/scenario/square-off")
    public Map<String, Object> squareOff(@RequestBody(required = false) Map<String, Object> body) {
        String symbol = resolveSymbol(body);
        Map<String, Object> pos = mockState.getPosition(symbol);
        if (pos != null) {
            int    netQty     = Integer.parseInt(pos.get("netQty").toString());
            double entryPrice = Double.parseDouble(pos.get("netAvgPrice").toString());
            double exitPrice  = mockState.getCurrentPrice(symbol);
            String side       = netQty > 0 ? "LONG" : "SHORT";
            int    qty        = Math.abs(netQty);

            String setup = pollingService.getCurrentSetup(symbol);
            PositionManager.setPosition(symbol, "NONE");
            positionStateStore.clear(symbol);
            pollingService.clearCachedPositions(symbol);
            mockState.triggerManualSquareOff(symbol);

            tradeHistoryService.record(symbol, side, qty, entryPrice, exitPrice, "MANUAL", setup);
            eventService.log("[SUCCESS] Manual Square off for " + symbol + " at exit: " + exitPrice + " — SL and Target cancelled");
        } else {
            mockState.triggerManualSquareOff(symbol);
        }
        return Map.of("ok", true);
    }

    /** Resolves symbol from request body, falling back to mockState activeSymbol. */
    private String resolveSymbol(Map<String, Object> body) {
        if (body != null && body.containsKey("symbol")) {
            String sym = body.get("symbol").toString().trim();
            if (!sym.isEmpty()) return sym;
        }
        return mockState.getActiveSymbol();
    }

    @PostMapping("/api/simulator/scenario/fill-order")
    public Map<String, Object> fillOrder(@RequestBody Map<String, Object> body) {
        mockState.fillOrder((String) body.get("orderId"),
            Double.parseDouble(body.get("price").toString()));
        return Map.of("ok", true);
    }

    @PostMapping("/api/simulator/scenario/cancel-order")
    public Map<String, Object> cancelOrder(@RequestBody Map<String, Object> body) {
        mockState.cancelOrder((String) body.get("orderId"));
        return Map.of("ok", true);
    }

    @PostMapping("/api/simulator/reset")
    public Map<String, Object> reset() {
        mockState.resetAll();
        tradeHistoryService.clearToday();
        eventService.clearToday();
        pollingService.clearCachedPositions();
        positionStateStore.clearAll();
        PositionManager.resetAll();
        return Map.of("ok", true);
    }
}