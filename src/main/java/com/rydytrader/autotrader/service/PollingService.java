package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.dto.PositionsDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.TokenStore;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Service
public class PollingService {

    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;
    private final FyersClientRouter fyersClient;
    private final OrderService      orderService;
    private final EventService      eventService;
    private final TradeHistoryService tradeHistoryService;

    private volatile List<PositionsDTO> cachedPositions = new ArrayList<>();
    private volatile String currentSetup = "";
    private volatile String currentEntryTime = "";
    private volatile String connectionStatus = "DISCONNECTED";

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    public PollingService(TokenStore tokenStore,
                          FyersProperties fyersProperties,
                          FyersClientRouter fyersClient,
                          OrderService orderService,
                          EventService eventService,
                          TradeHistoryService tradeHistoryService) {
        this.tokenStore          = tokenStore;
        this.fyersProperties     = fyersProperties;
        this.fyersClient         = fyersClient;
        this.orderService        = orderService;
        this.eventService        = eventService;
        this.tradeHistoryService = tradeHistoryService;
    }

    // ── ENTRY MONITOR + OCO ───────────────────────────────────────────────────
    public void monitorEntryAndPlaceOCO(OrderDTO entry, String symbol,
                                        int quantity, String position,
                                        int exitSide, double slPrice, double targetPrice) {
        monitorEntryAndPlaceOCO(entry, symbol, quantity, position, exitSide, slPrice, targetPrice, "");
    }

    public void monitorEntryAndPlaceOCO(OrderDTO entry, String symbol,
                                        int quantity, String position,
                                        int exitSide, double slPrice, double targetPrice, String setup) {
        class Holder { ScheduledFuture<?> future; }
        Holder holder = new Holder();

        holder.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                String status = getOrderStatus(entry.getId());
                if ("2".equals(status)) {
                    PositionManager.setPosition(position);
                    currentSetup = setup != null ? setup : "";
                    currentEntryTime = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                    eventService.log((position.equals("LONG") ? "BUY" : "SELL") + " order filled — placing SL + Target");

                    OrderDTO slOrder     = orderService.placeStopLoss(symbol, quantity, exitSide, slPrice);
                    OrderDTO targetOrder = orderService.placeTarget(symbol, quantity, exitSide, targetPrice);

                    if (slOrder != null && slOrder.getId() != null)
                        eventService.log("SL order placed at " + slPrice + " [ID: " + slOrder.getId() + "]");
                    if (targetOrder != null && targetOrder.getId() != null)
                        eventService.log("Target order placed at " + targetPrice + " [ID: " + targetOrder.getId() + "]");

                    if (slOrder != null && targetOrder != null)
                        monitorOCO(slOrder.getId(), targetOrder.getId(), symbol, quantity, exitSide, slPrice, targetPrice, position, setup);

                    if (holder.future != null) holder.future.cancel(false);

                } else if ("5".equals(status)) {
                    eventService.log((position.equals("LONG") ? "BUY" : "SELL") + " order rejected: " + entry.getMessage());
                    if (holder.future != null) holder.future.cancel(false);
                }
            } catch (Exception e) { e.printStackTrace(); }
        }, 0, 2, TimeUnit.SECONDS);
    }

    // ── OCO MONITOR ───────────────────────────────────────────────────────────
    private void monitorOCO(String slId, String targetId, String symbol, int qty,
                             int exitSide, double slPrice, double targetPrice, String positionSide, String setup) {
        class State {
            ScheduledFuture<?> future;
            boolean slManualCancelled     = false;
            boolean targetManualCancelled = false;
            boolean handled               = false;
            int ticks = 0;
        }
        State s = new State();

        s.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (s.handled) return;
                s.ticks++;
                if (s.ticks > 7200) { // 4-hour timeout
                    eventService.log("WARNING: OCO monitor timeout after 4 hours");
                    if (s.future != null) s.future.cancel(false);
                    return;
                }

                String slStatus     = getOrderStatus(slId);
                String targetStatus = getOrderStatus(targetId);

                boolean slFilled     = "2".equals(slStatus);
                boolean targetFilled = "2".equals(targetStatus);
                boolean slCancelled  = "5".equals(slStatus);
                boolean tgtCancelled = "5".equals(targetStatus);

                // SL hit
                if (slFilled && !s.handled) {
                    s.handled = true;
                    eventService.log("SL triggered at " + slPrice + " — cancelling target");
                    orderService.cancelOrder(targetId);
                    double exitPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    tradeHistoryService.record(symbol, positionSide, qty, entryPrice > 0 ? entryPrice : slPrice, exitPrice > 0 ? exitPrice : slPrice, "SL", setup);
                    PositionManager.setPosition("NONE");
                    if (s.future != null) s.future.cancel(false);
                }
                // Target hit
                else if (targetFilled && !s.handled) {
                    s.handled = true;
                    eventService.log("Target hit at " + targetPrice + " — cancelling SL");
                    orderService.cancelOrder(slId);
                    double exitPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    tradeHistoryService.record(symbol, positionSide, qty, entryPrice > 0 ? entryPrice : targetPrice, exitPrice > 0 ? exitPrice : targetPrice, "TARGET", setup);
                    PositionManager.setPosition("NONE");
                    if (s.future != null) s.future.cancel(false);
                }
                // SL manually cancelled — only log if position still open
                else if (slCancelled && !s.slManualCancelled && !PositionManager.getPosition().equals("NONE")) {
                    s.slManualCancelled = true;
                    if (!tgtCancelled) {
                        eventService.log("SL order manually cancelled — target still active");
                    } else {
                        eventService.log("SL order manually cancelled");
                    }
                }
                // Target manually cancelled — only log if position still open
                else if (tgtCancelled && !s.targetManualCancelled && !PositionManager.getPosition().equals("NONE")) {
                    s.targetManualCancelled = true;
                    if (!slCancelled) {
                        eventService.log("Target order manually cancelled — SL still active");
                    } else {
                        eventService.log("Target order manually cancelled");
                    }
                }
                // Both cancelled and position still open — unprotected warning
                else if (slCancelled && tgtCancelled && s.slManualCancelled && s.targetManualCancelled
                         && !PositionManager.getPosition().equals("NONE") && !s.handled) {
                    s.handled = true;
                    eventService.log("WARNING: Both SL and Target cancelled — position unprotected");
                    if (s.future != null) s.future.cancel(false);
                }
                // Position closed externally (e.g. simulator square off) — stop monitoring silently
                if (!s.handled && PositionManager.getPosition().equals("NONE") && slCancelled && tgtCancelled) {
                    s.handled = true;
                    if (s.future != null) s.future.cancel(false);
                }

            } catch (Exception e) { e.printStackTrace(); }
        }, 2, 2, TimeUnit.SECONDS);
    }

    // ── ORDER STATUS ──────────────────────────────────────────────────────────
    private String getOrderStatus(String orderId) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getOrder(orderId, auth);
            for (JsonNode order : node.get("orderBook")) {
                if (order.get("id").asText().equals(orderId)) {
                    return order.get("status").asText();
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
        return null;
    }

    // ── POSITION SYNC ─────────────────────────────────────────────────────────
    // ── SQUARE OFF ────────────────────────────────────────────────────────────
    public String getCurrentSetup() { return currentSetup; }

    public boolean squareOff(String symbol, int quantity) {
        try {
            String position = PositionManager.getPosition();
            if ("NONE".equals(position)) return false;
            int exitSide = "LONG".equals(position) ? -1 : 1;

            // Cancel SL/Target orders first then place exit market order
            eventService.log("Square off — cancelling SL/Target orders");
            orderService.cancelAllPendingOrders(symbol);

            OrderDTO exitOrder = orderService.placeExitOrder(symbol, quantity, exitSide);
            if (exitOrder == null || !"ok".equals(exitOrder.getStatus())) {
                eventService.log("Square off failed — exit order rejected");
                return false;
            }

            eventService.log("Square off executed — position closed");
            currentSetup = ""; currentEntryTime = "";
            PositionManager.setPosition("NONE");
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            eventService.log("Square off error: " + e.getMessage());
            return false;
        }
    }

    private volatile boolean positionSyncStarted = false;

    public void startPositionSync() {
        if (positionSyncStarted) return;
        positionSyncStarted = true;
        scheduler.scheduleAtFixedRate(() -> {
            if (tokenStore.getAccessToken() == null) return;
            syncPosition();
        }, 5, 10, TimeUnit.SECONDS);
    }

    private void syncPosition() {
        try {
            connectionStatus = "SYNCING";
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getPositions(auth);

            List<PositionsDTO> updatedList = new ArrayList<>();
            JsonNode positions = node.get("netPositions");
            if (positions != null) {
                for (JsonNode pos : positions) {
                    // support both "qty" (live) and "netQty" (mock)
                    int qty = pos.has("netQty") ? pos.get("netQty").asInt()
                                                : pos.has("qty") ? pos.get("qty").asInt() : 0;
                    if (qty != 0) {
                        int side = pos.get("side").asInt();
                        double avg = pos.has("netAvgPrice") ? pos.get("netAvgPrice").asDouble()
                                   : pos.has("netAvg")      ? pos.get("netAvg").asDouble() : 0;
                        double ltp = pos.has("ltp") ? pos.get("ltp").asDouble() : 0;
                        double pl  = pos.has("unrealizedProfit") ? pos.get("unrealizedProfit").asDouble()
                                   : pos.has("pl")               ? pos.get("pl").asDouble() : 0;
                        String posSide = side == 1 ? "LONG" : "SHORT";
                        String resolvedEntryTime = !currentEntryTime.isEmpty() ? currentEntryTime : eventService.getEntryTimeFromLogs();
                        if (currentEntryTime.isEmpty() && !resolvedEntryTime.isEmpty()) currentEntryTime = resolvedEntryTime;
                        updatedList.add(new PositionsDTO(
                            pos.get("symbol").asText(), qty, posSide, avg, ltp, pl, currentSetup, resolvedEntryTime
                        ));
                        PositionManager.setPosition(posSide);
                    }
                }
            }

            // Detect manual square-off
            if (updatedList.isEmpty() && !cachedPositions.isEmpty()
                    && !PositionManager.getPosition().equals("NONE")) {
                eventService.log("Position closed externally — recording trade");
                PositionsDTO prev = cachedPositions.get(0);
                double exitPrice = orderService.getExitPriceFromTradebook(prev.getSymbol());
                tradeHistoryService.record(prev.getSymbol(), prev.getSide(), Math.abs(prev.getQty()),
                    prev.getAvgPrice(), exitPrice > 0 ? exitPrice : prev.getLtp(), "MANUAL", currentSetup);
                currentSetup = ""; currentEntryTime = "";
                PositionManager.setPosition("NONE");
            } else if (updatedList.isEmpty()) {
                currentSetup = ""; currentEntryTime = "";
                PositionManager.setPosition("NONE");
            }

            cachedPositions = updatedList;
            connectionStatus = "CONNECTED";

        } catch (Exception e) {
            connectionStatus = "DISCONNECTED";
            e.printStackTrace();
        }
    }

    public void syncPositionOnce()           { syncPosition(); }
    public List<PositionsDTO> fetchPositions(){ return cachedPositions; }
    public String getConnectionStatus()       { return connectionStatus; }
}