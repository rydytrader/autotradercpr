package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.mock.MockState;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.service.TradeHistoryService;
import com.rydytrader.autotrader.store.ModeStore;
import com.rydytrader.autotrader.store.TokenStore;
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

    public SimulatorController(ModeStore modeStore,
                                TokenStore tokenStore,
                                MockState mockState,
                                EventService eventService,
                                TradeHistoryService tradeHistoryService,
                                PollingService pollingService) {
        this.modeStore          = modeStore;
        this.tokenStore         = tokenStore;
        this.mockState          = mockState;
        this.eventService       = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.pollingService      = pollingService;
    }

    // ── MODE SWITCH ───────────────────────────────────────────────────────────
    @PostMapping("/api/logout")
    public ResponseEntity<?> logout() {
        tokenStore.setAccessToken(null);
        mockState.resetAll();
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

        // Reset session state on switch
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
            "redirect", newMode == ModeStore.Mode.SIMULATOR ? "/positions" : "/"
        ));
    }

    @GetMapping("/api/mode")
    public Map<String, String> getMode() {
        return Map.of("mode", modeStore.getMode().name());
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
        double price = Double.parseDouble(body.get("price").toString());
        mockState.setCurrentPrice(price);
        mockState.log("Price → " + price);
        return Map.of("ok", true, "price", price);
    }

    // ── SCENARIOS ─────────────────────────────────────────────────────────────
    @PostMapping("/api/simulator/scenario/sl-hit")
    public Map<String, Object> slHit() {
        mockState.triggerSlHit();
        return Map.of("ok", true);
    }

    @PostMapping("/api/simulator/scenario/target-hit")
    public Map<String, Object> targetHit() {
        mockState.triggerTargetHit();
        return Map.of("ok", true);
    }

    @PostMapping("/api/simulator/scenario/square-off")
    public Map<String, Object> squareOff() {
        Map<String, Object> pos = mockState.getPosition();
        if (pos != null) {
            String symbol     = pos.get("symbol").toString();
            int    netQty     = Integer.parseInt(pos.get("netQty").toString());
            double entryPrice = Double.parseDouble(pos.get("netAvgPrice").toString());
            double exitPrice  = mockState.getCurrentPrice();
            String side       = netQty > 0 ? "LONG" : "SHORT";
            int    qty        = Math.abs(netQty);

            String setup = pollingService.getCurrentSetup();
            PositionManager.setPosition("NONE");
            mockState.triggerManualSquareOff();

            tradeHistoryService.record(symbol, side, qty, entryPrice, exitPrice, "MANUAL", setup);
            eventService.log("Square off at exit: " + exitPrice + " — SL and Target cancelled");
        } else {
            mockState.triggerManualSquareOff();
        }
        return Map.of("ok", true);
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
        tradeHistoryService.reloadForCurrentMode();
        return Map.of("ok", true);
    }
}