package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.dto.PositionsDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

@Service
public class PollingService {

    private static final Logger log = LoggerFactory.getLogger(PollingService.class);

    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final FyersClientRouter   fyersClient;
    private final OrderService        orderService;
    private final EventService        eventService;
    private final TradeHistoryService tradeHistoryService;
    private final PositionStateStore  positionStateStore;
    private final RiskSettingsStore   riskSettings;
    private final TelegramService     telegramService;
    private final MarketDataService   marketDataService;
    private final OrderEventService   orderEventService;
    private final BreakoutScanner     breakoutScanner;

    @org.springframework.beans.factory.annotation.Autowired
    private LatencyTracker latencyTracker;

    // ── per-symbol state ──────────────────────────────────────────────────────
    private final ConcurrentHashMap<String, PositionsDTO> cachedPositions  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String>       setupBySymbol    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String>       probabilityBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String>       entryTimeBySymbol  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double>       entryAvgBySymbol   = new ConcurrentHashMap<>();
    private final Set<String> ocoHandledSymbols = ConcurrentHashMap.newKeySet();
    private final Set<String> ocoMonitoredSymbols = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> ocoPollingFutures = new ConcurrentHashMap<>();
    private final Set<String> pendingEntrySymbols = ConcurrentHashMap.newKeySet();

    private volatile boolean justRestored    = false;
    private volatile String  lastSyncTime    = "";
    private volatile String  connectionStatus = "DISCONNECTED";

    // 16 threads: supports multiple concurrent entry + OCO monitors across symbols
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(16);

    public PollingService(TokenStore tokenStore,
                          FyersProperties fyersProperties,
                          FyersClientRouter fyersClient,
                          OrderService orderService,
                          EventService eventService,
                          TradeHistoryService tradeHistoryService,
                          PositionStateStore positionStateStore,
                          RiskSettingsStore riskSettings,
                          TelegramService telegramService,
                          MarketDataService marketDataService,
                          OrderEventService orderEventService,
                          BreakoutScanner breakoutScanner) {
        this.tokenStore          = tokenStore;
        this.fyersProperties     = fyersProperties;
        this.fyersClient         = fyersClient;
        this.orderService        = orderService;
        this.eventService        = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.positionStateStore  = positionStateStore;
        this.riskSettings        = riskSettings;
        this.telegramService     = telegramService;
        this.marketDataService   = marketDataService;
        this.orderEventService   = orderEventService;
        this.breakoutScanner     = breakoutScanner;
        // Set back-reference to avoid circular DI
        orderEventService.setPollingService(this);
        // Start Order WebSocket early (before restore) so restored OCOs can use it
        if (tokenStore.isTokenAvailable()) {
            orderEventService.start();
        }
        restoreStateOnStartup();
        startAutoSquareOffScheduler();
        startTelegramSummaryScheduler();
        startEodSummaryScheduler();
    }

    // ── STARTUP RESTORE ───────────────────────────────────────────────────────
    private void restoreStateOnStartup() {
        Map<String, Map<String, Object>> allSaved = positionStateStore.loadAll();
        if (allSaved.isEmpty()) return;
        try {
            for (Map.Entry<String, Map<String, Object>> entry : allSaved.entrySet()) {
                Map<String, Object> saved = entry.getValue();
                String symbol    = saved.get("symbol").toString();
                String side      = saved.get("side").toString();
                int    origQty   = Integer.parseInt(saved.get("qty").toString());
                // After T1 partial fill, remaining qty is what's actually held at broker
                boolean t1Filled = Boolean.parseBoolean(String.valueOf(saved.getOrDefault("t1Filled", "false")));
                int remQty = 0;
                try { remQty = Integer.parseInt(String.valueOf(saved.getOrDefault("remainingQty", "0"))); }
                catch (NumberFormatException ignored) {}
                int qty = (t1Filled && remQty > 0) ? remQty : origQty;
                double avgPrice  = Double.parseDouble(saved.get("avgPrice").toString());
                String setup     = saved.getOrDefault("setup", "").toString();
                String entryTime = saved.getOrDefault("entryTime", "").toString();

                String probability = saved.getOrDefault("probability", "").toString();
                setupBySymbol.put(symbol, setup);
                probabilityBySymbol.put(symbol, probability);
                entryTimeBySymbol.put(symbol, entryTime);
                entryAvgBySymbol.put(symbol, avgPrice);
                PositionManager.setPosition(symbol, side);
                cachedPositions.put(symbol, new PositionsDTO(symbol, qty, side, avgPrice, avgPrice, 0.0, setup, entryTime));

                // Restart OCO monitor if SL/Target order IDs were persisted
                String slOrderId     = saved.getOrDefault("slOrderId", "").toString();
                String targetOrderId = saved.getOrDefault("targetOrderId", "").toString();
                int exitSide         = "LONG".equals(side) ? -1 : 1;
                if (!slOrderId.isEmpty() && !targetOrderId.isEmpty()) {
                    double slPrice     = Double.parseDouble(saved.getOrDefault("slPrice", "0").toString());
                    double targetPrice = Double.parseDouble(saved.getOrDefault("targetPrice", "0").toString());

                    // Check if SL or target already filled while bot was down
                    boolean handledOnRestore = checkOcoFilledOnRestore(
                        slOrderId, targetOrderId, symbol, qty, exitSide, side, setup, avgPrice, slPrice, targetPrice);
                    if (handledOnRestore) continue;

                    // Try WebSocket tracking first, fall back to polling
                    boolean wsTracked = orderEventService.trackOcoOrders(slOrderId, targetOrderId,
                        symbol, qty, side, exitSide, setup, avgPrice, slPrice, targetPrice);
                    if (wsTracked) {
                        log.info("[PollingService] Restored OCO via WebSocket for {} — SL: {} | Target: {}", symbol, slOrderId, targetOrderId);
                    } else {
                        monitorOCO(slOrderId, targetOrderId, symbol, qty, exitSide,
                            slPrice, targetPrice, side, setup, avgPrice);
                        log.info("[PollingService] Restored OCO via polling for {} — SL: {} | Target: {}", symbol, slOrderId, targetOrderId);
                    }
                } else {
                    // No saved OCO IDs — scan for manually placed SL/Target orders
                    log.info("[PollingService] Restored state on startup: {} {} (no OCO IDs — scanning for manual SL/Target)", side, symbol);
                    scanForManualOCO(symbol, qty, exitSide, side, setup, avgPrice);
                }
            }
            justRestored = true;
        } catch (Exception e) {
            log.error("[PollingService] Failed to restore state: {}", e.getMessage());
        }
    }

    /**
     * On restore, check if SL or target already filled while bot was down.
     * Queries the order book via REST API. Returns true if handled (position closed).
     */
    private boolean checkOcoFilledOnRestore(String slOrderId, String targetOrderId,
                                             String symbol, int qty, int exitSide,
                                             String side, String setup, double avgPrice,
                                             double slPrice, double targetPrice) {
        try {
            // Fetch order book once
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            if (auth.contains("null")) return false;

            JsonNode orderBook = fyersClient.getOrders(auth);
            JsonNode orders = orderBook != null ? orderBook.get("orderBook") : null;
            if (orders == null || !orders.isArray()) return false;

            String slStatus = null, targetStatus = null;
            double slFillPrice = 0, targetFillPrice = 0;
            for (JsonNode order : orders) {
                String oid = order.has("id") ? order.get("id").asText() : "";
                String onum = order.has("orderNumber") ? order.get("orderNumber").asText() : "";
                int status = order.has("status") ? order.get("status").asInt() : 0;
                double tradedPrice = order.has("tradedPrice") ? order.get("tradedPrice").asDouble() : 0;

                if (slOrderId.equals(oid) || slOrderId.equals(onum)) {
                    slStatus = String.valueOf(status);
                    slFillPrice = tradedPrice;
                }
                if (targetOrderId.equals(oid) || targetOrderId.equals(onum)) {
                    targetStatus = String.valueOf(status);
                    targetFillPrice = tradedPrice;
                }
            }

            // Check if SL filled
            if ("2".equals(slStatus)) {
                double exitPrice = slFillPrice > 0 ? slFillPrice : slPrice;
                double pnl = "LONG".equals(side) ? (exitPrice - avgPrice) * qty : (avgPrice - exitPrice) * qty;
                log.info("[PollingService] SL already filled on restore for {} at {}", symbol, exitPrice);
                eventService.log("[SUCCESS] SL hit for " + symbol + " at " + exitPrice
                    + " (detected on restart) | " + (pnl >= 0 ? "PROFIT" : "LOSS") + " ₹" + String.format("%.2f", Math.abs(pnl)));
                orderService.cancelOrder(targetOrderId);
                tradeHistoryService.record(symbol, side, qty, avgPrice, exitPrice, "SL", setup, null, probabilityBySymbol.getOrDefault(symbol, ""));
                clearSymbolState(symbol);
                return true;
            }

            // Check if target filled
            if ("2".equals(targetStatus)) {
                double exitPrice = targetFillPrice > 0 ? targetFillPrice : targetPrice;
                double pnl = "LONG".equals(side) ? (exitPrice - avgPrice) * qty : (avgPrice - exitPrice) * qty;
                log.info("[PollingService] Target already filled on restore for {} at {}", symbol, exitPrice);
                eventService.log("[SUCCESS] Target hit for " + symbol + " at " + exitPrice
                    + " (detected on restart) | " + (pnl >= 0 ? "PROFIT" : "LOSS") + " ₹" + String.format("%.2f", Math.abs(pnl)));
                orderService.cancelOrder(slOrderId);
                tradeHistoryService.record(symbol, side, qty, avgPrice, exitPrice, "TARGET", setup, null, probabilityBySymbol.getOrDefault(symbol, ""));
                clearSymbolState(symbol);
                return true;
            }

        } catch (Exception e) {
            log.error("[PollingService] Error checking OCO on restore for {}: {}", symbol, e.getMessage());
        }
        return false;
    }

    // ── ENTRY MONITOR + OCO ───────────────────────────────────────────────────
    public void monitorEntryAndPlaceOCO(OrderDTO entry, String symbol,
                                        int quantity, String position,
                                        int exitSide, double slPrice, double targetPrice, String setup,
                                        double atr, double atrMultiplier) {
        monitorEntryAndPlaceOCO(entry, symbol, quantity, position, exitSide, slPrice, targetPrice, setup, atr, atrMultiplier, null);
    }

    public void monitorEntryAndPlaceOCO(OrderDTO entry, String symbol,
                                        int quantity, String position,
                                        int exitSide, double slPrice, double targetPrice, String setup,
                                        double atr, double atrMultiplier, String description) {
        monitorEntryAndPlaceOCO(entry, symbol, quantity, position, exitSide, slPrice, targetPrice, setup, atr, atrMultiplier, description, false);
    }

    public void monitorEntryAndPlaceOCO(OrderDTO entry, String symbol,
                                        int quantity, String position,
                                        int exitSide, double slPrice, double targetPrice, String setup,
                                        double atr, double atrMultiplier, String description, boolean dayHighLowShifted) {

        // ── Try WebSocket-based tracking first (WS connected) ──
        if (orderEventService.isConnected()) {
            boolean tracked = orderEventService.trackEntryOrder(entry.getId(),
                new OrderEventService.EntryContext(symbol, quantity, position, exitSide, slPrice, targetPrice, setup, atr, atrMultiplier, description, dayHighLowShifted));
            if (tracked) {
                eventService.log("[INFO] Entry order " + entry.getId() + " tracked via WebSocket for " + symbol);
                return; // WebSocket will handle fill detection — no polling needed
            }
        }

        // ── Fallback: polling-based entry monitor ──
        class Holder {
            ScheduledFuture<?> future;
            boolean positionSet    = false;
            double  entryFillPrice = 0.0;
            double  adjustedSl     = slPrice; // recalculated from fill price
            int     ocoRetries     = 0;
            static final int MAX_OCO_RETRIES = 3;
        }
        Holder holder = new Holder();
        pendingEntrySymbols.add(symbol);

        holder.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                String status = getOrderStatus(entry.getId());
                if ("2".equals(status)) {

                    // Set position state only on the first filled tick
                    if (!holder.positionSet) {
                        holder.positionSet = true;
                        String entryTime = java.time.LocalDateTime.now()
                            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                        PositionManager.setPosition(symbol, position);
                        setupBySymbol.put(symbol, setup != null ? setup : "");
                        entryTimeBySymbol.put(symbol, entryTime);
                        pendingEntrySymbols.remove(symbol);
                        // Get fill price directly from order book — no tradebook API call needed
                        JsonNode entryNode = getOrderNode(entry.getId());
                        if (entryNode != null) {
                            if (entryNode.has("tradedPrice") && entryNode.get("tradedPrice").asDouble() > 0) {
                                holder.entryFillPrice = entryNode.get("tradedPrice").asDouble();
                            } else if (entryNode.has("limitPrice") && entryNode.get("limitPrice").asDouble() > 0) {
                                holder.entryFillPrice = entryNode.get("limitPrice").asDouble();
                            }
                        }
                        entryAvgBySymbol.put(symbol, holder.entryFillPrice);
                        // Recalculate SL from actual fill price (always ATR-based at entry)
                        // Chandelier Exit only adjusts SL later on candle-close events, not at initial placement
                        if (holder.entryFillPrice > 0 && atr > 0 && atrMultiplier > 0) {
                            holder.adjustedSl = "LONG".equals(position) ? holder.entryFillPrice - atr * atrMultiplier : holder.entryFillPrice + atr * atrMultiplier;
                            holder.adjustedSl = orderService.roundToTick(holder.adjustedSl, symbol);
                        }
                        double roundedTarget = orderService.roundToTick(targetPrice, symbol);
                        positionStateStore.save(symbol, position, quantity, holder.entryFillPrice,
                            setupBySymbol.get(symbol), entryTime, holder.adjustedSl, roundedTarget);
                        // Save description from signal processing
                        if (description != null && !description.isEmpty()) {
                            positionStateStore.appendDescription(symbol, description);
                        }
                        positionStateStore.appendDescription(symbol,
                            "[FILL] " + (position.equals("LONG") ? "BUY" : "SELL") + " filled @ "
                            + String.format("%.2f", holder.entryFillPrice) + ". SL recalculated → " + String.format("%.2f", holder.adjustedSl) + ".");
                        if (latencyTracker != null) latencyTracker.mark(symbol, setup, LatencyTracker.Stage.ORDER_FILLED);
                        eventService.log("[SUCCESS] " + (position.equals("LONG") ? "BUY" : "SELL")
                            + " order filled for " + symbol + " @ " + holder.entryFillPrice + " [ID: " + entry.getId() + "] — placing SL + Target");
                        marketDataService.updateSubscriptions();
                    }

                    holder.ocoRetries++;
                    boolean skipTarget = riskSettings.isEnableTrailingSl() && riskSettings.isTrailingSlNoTarget();

                    // Check if we should split targets
                    double targetDist = Math.abs(targetPrice - holder.entryFillPrice);
                    double entryAtr = atr > 0 ? atr : 1;
                    boolean shouldSplit = riskSettings.isEnableSplitTarget() && !skipTarget
                        && !dayHighLowShifted // skip split if target was shifted to day high/low
                        && quantity >= 4
                        && (riskSettings.getSplitMinDistanceAtr() <= 0 || targetDist > entryAtr * riskSettings.getSplitMinDistanceAtr());

                    if (shouldSplit) {
                        // Split target placement: SL (full) + T1 (half) + T2 (half)
                        int t1Qty = (quantity / 4) * 2;
                        int t2Qty = quantity - t1Qty;
                        double t1Pct = riskSettings.getT1DistancePct() / 100.0;
                        boolean isBuy = "LONG".equals(position);
                        // T1 uses ORIGINAL structural target — not the discounted value
                        double t1Price = isBuy
                            ? holder.entryFillPrice + t1Pct * (targetPrice - holder.entryFillPrice)
                            : holder.entryFillPrice - t1Pct * (holder.entryFillPrice - targetPrice);
                        t1Price = orderService.roundToTick(t1Price, symbol);
                        // T2 sits at the structural CPR level — apply tolerance discount
                        double t2Price = orderService.applyTargetTolerance(targetPrice, isBuy, atr, symbol);

                        OrderDTO slOrder = orderService.placeStopLoss(symbol, quantity, exitSide, holder.adjustedSl);
                        OrderDTO t1Order = orderService.placeTarget(symbol, t1Qty, exitSide, t1Price);
                        OrderDTO t2Order = orderService.placeTarget(symbol, t2Qty, exitSide, t2Price);

                        boolean slOk = slOrder != null && slOrder.getId() != null && !slOrder.getId().isEmpty();
                        boolean t1Ok = t1Order != null && t1Order.getId() != null && !t1Order.getId().isEmpty();
                        boolean t2Ok = t2Order != null && t2Order.getId() != null && !t2Order.getId().isEmpty();

                        if (slOk && t1Ok && t2Ok) {
                            double rSl = orderService.roundToTick(holder.adjustedSl, symbol);
                            eventService.log("[SUCCESS] Split targets for " + symbol
                                + " — SL: " + rSl + " [" + slOrder.getId() + "]"
                                + " | T1: " + t1Price + " qty=" + t1Qty + " [" + t1Order.getId() + "]"
                                + " | T2: " + t2Price + " qty=" + t2Qty + " [" + t2Order.getId() + "]");
                            positionStateStore.appendDescription(symbol,
                                "[SPLIT_TARGETS] T1 @ " + String.format("%.2f", t1Price) + " qty=" + t1Qty
                                + " | T2 @ " + String.format("%.2f", t2Price) + " qty=" + t2Qty);
                            positionStateStore.appendDescription(symbol,
                                "[SL_PLACED] @ " + String.format("%.2f", rSl) + " qty=" + quantity);
                            positionStateStore.saveSplitOcoState(symbol, slOrder.getId(),
                                t1Order.getId(), t2Order.getId(), holder.adjustedSl, t1Price, t2Price);

                            boolean wsTracked = orderEventService.trackSplitOcoOrders(
                                slOrder.getId(), t1Order.getId(), t2Order.getId(),
                                symbol, quantity, t1Qty, t2Qty,
                                position, exitSide, setup, holder.entryFillPrice,
                                holder.adjustedSl, t1Price, t2Price);
                            if (!wsTracked) {
                                monitorSplitOCO(slOrder.getId(), t1Order.getId(), t2Order.getId(),
                                    symbol, quantity, t1Qty, t2Qty, exitSide,
                                    holder.adjustedSl, t1Price, t2Price, position, setup, holder.entryFillPrice);
                            }
                            String prob = probabilityBySymbol.getOrDefault(symbol, "");
                            telegramService.notifyTradeOpened(symbol, position, quantity,
                                holder.entryFillPrice, holder.adjustedSl, t2Price, setup, prob);
                            if (holder.future != null) holder.future.cancel(false);
                        } else {
                            if (slOk) orderService.cancelOrder(slOrder.getId());
                            if (t1Ok) orderService.cancelOrder(t1Order.getId());
                            if (t2Ok) orderService.cancelOrder(t2Order.getId());
                            eventService.log("[ERROR] Split target placement failed for " + symbol
                                + " (attempt " + holder.ocoRetries + ") SL=" + slOk + " T1=" + t1Ok + " T2=" + t2Ok);
                            if (holder.ocoRetries >= Holder.MAX_OCO_RETRIES) {
                                eventService.log("[ERROR] Split target failed for " + symbol
                                    + " — squaring off to avoid unprotected position");
                                boolean closed = squareOff(symbol, quantity, "SL_TARGET_FAILED");
                                if (!closed) eventService.log("[ERROR] Emergency squareoff FAILED for " + symbol);
                                // Alert removed — emergency squareoff logged in event log
                                if (holder.future != null) holder.future.cancel(false);
                            }
                        }
                    } else {
                    // Single target placement (existing logic)
                    boolean isBuyForTol = "LONG".equals(position);
                    double placedTargetPrice = orderService.applyTargetTolerance(targetPrice, isBuyForTol, atr, symbol);

                    OrderDTO slOrder     = orderService.placeStopLoss(symbol, quantity, exitSide, holder.adjustedSl);
                    OrderDTO targetOrder = skipTarget ? null : orderService.placeTarget(symbol, quantity, exitSide, placedTargetPrice);

                    boolean slOk  = slOrder     != null && slOrder.getId()     != null && !slOrder.getId().isEmpty();
                    boolean tgtOk = skipTarget || (targetOrder != null && targetOrder.getId() != null && !targetOrder.getId().isEmpty());

                    if (slOk && tgtOk) {
                        double rSl = orderService.roundToTick(holder.adjustedSl, symbol);
                        String tgtId = skipTarget ? "" : targetOrder.getId();
                        double tgtPrice = skipTarget ? 0 : placedTargetPrice;
                        eventService.log("[SUCCESS] SL order placed for " + symbol + " at " + rSl + " [ID: " + slOrder.getId() + "]");
                        if (skipTarget) {
                            eventService.log("[INFO] No fixed target for " + symbol + " — trailing SL will close the trade");
                        } else {
                            if (Math.abs(placedTargetPrice - targetPrice) > 0.001) {
                                eventService.log("[SUCCESS] Target order placed for " + symbol + " at " + String.format("%.2f", placedTargetPrice)
                                    + " (structural " + String.format("%.2f", targetPrice) + " - tolerance) [ID: " + tgtId + "]");
                            } else {
                                eventService.log("[SUCCESS] Target order placed for " + symbol + " at " + String.format("%.2f", placedTargetPrice) + " [ID: " + tgtId + "]");
                            }
                            positionStateStore.appendDescription(symbol, "[TGT_PLACED] @ " + String.format("%.2f", placedTargetPrice) + " [" + tgtId + "]");
                        }
                        positionStateStore.saveOcoState(symbol, slOrder.getId(), tgtId, holder.adjustedSl, tgtPrice);
                        positionStateStore.appendDescription(symbol, "[SL_PLACED] @ " + String.format("%.2f", rSl) + " [" + slOrder.getId() + "]");
                        boolean wsTracked = orderEventService.trackOcoOrders(slOrder.getId(), tgtId,
                            symbol, quantity, position, exitSide, setup, holder.entryFillPrice);
                        if (!wsTracked) {
                            monitorOCO(slOrder.getId(), tgtId, symbol, quantity,
                                exitSide, holder.adjustedSl, tgtPrice, position, setup, holder.entryFillPrice);
                        }
                        String prob = probabilityBySymbol.getOrDefault(symbol, "");
                        telegramService.notifyTradeOpened(symbol, position, quantity,
                            holder.entryFillPrice, holder.adjustedSl, tgtPrice, setup, prob);
                        if (holder.future != null) holder.future.cancel(false);
                    } else {
                        if (slOk)  orderService.cancelOrder(slOrder.getId());
                        if (!skipTarget && targetOrder != null && targetOrder.getId() != null && !targetOrder.getId().isEmpty())
                            orderService.cancelOrder(targetOrder.getId());

                        if (!slOk)  eventService.log("[ERROR] SL order placement failed for " + symbol + " at " + slPrice);
                        if (!skipTarget && !tgtOk) eventService.log("[ERROR] Target order placement failed for " + symbol + " at " + targetPrice);

                        if (holder.ocoRetries >= Holder.MAX_OCO_RETRIES) {
                            eventService.log("[ERROR] Could not place SL/Target for " + symbol
                                + " after " + Holder.MAX_OCO_RETRIES + " attempts — squaring off to avoid unprotected position");
                            boolean closed = squareOff(symbol, quantity, "SL_TARGET_FAILED");
                            if (closed) {
                                eventService.log("[SUCCESS] Emergency squareoff for " + symbol + " — unprotected position closed");
                            } else {
                                eventService.log("[ERROR] Emergency squareoff FAILED for " + symbol + " — POSITION UNPROTECTED");
                            }
                            // Alert removed — emergency squareoff logged in event log
                            if (holder.future != null) holder.future.cancel(false);
                        } else {
                            eventService.log("[WARNING] Retrying SL/Target placement for " + symbol
                                + " (attempt " + holder.ocoRetries + "/" + Holder.MAX_OCO_RETRIES + ")");
                        }
                    }
                    } // end single-target else

                } else if ("5".equals(status)) {
                    pendingEntrySymbols.remove(symbol);
                    eventService.log("[ERROR] " + (position.equals("LONG") ? "BUY" : "SELL")
                        + " order rejected for " + symbol + ": " + entry.getMessage());
                    if (holder.future != null) holder.future.cancel(false);
                }
            } catch (Exception e) {
                eventService.log("[ERROR] Entry monitor error for " + symbol + ": " + e.getMessage());
                log.error("[PollingService] Entry monitor error for {}", symbol, e);
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    // ── SCAN FOR MANUALLY PLACED SL/TARGET ────────────────────────────────────
    // After auto-placement fails, polls the order book every 10s looking for
    // a pending SL-M (type 3) and Limit (type 1) order on the same symbol/side.
    // Once both are found, starts the OCO monitor automatically.
    public void scanForManualOCO(String symbol, int qty, int exitSide,
                                  String positionSide, String setup, double entryFillPrice) {
        class ScanState {
            ScheduledFuture<?> future;
            int ticks = 0;
        }
        ScanState ss = new ScanState();
        eventService.log("[INFO] Watching order book for manually placed SL/Target for " + symbol);

        ss.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                ss.ticks++;
                // Stop if OCO monitor already started for this symbol
                if (ocoMonitoredSymbols.contains(symbol)) {
                    if (ss.future != null) ss.future.cancel(false);
                    return;
                }
                // Stop scanning after 30 minutes (180 ticks × 10s)
                if (ss.ticks > 180) {
                    eventService.log("[WARNING] Manual OCO scan timed out for " + symbol + " after 30 min — position UNPROTECTED");
                    if (ss.future != null) ss.future.cancel(false);
                    return;
                }
                // Stop if position was closed externally
                if (PositionManager.getPosition(symbol).equals("NONE")) {
                    if (ss.future != null) ss.future.cancel(false);
                    return;
                }

                JsonNode orderBook = getOrderBook();
                if (orderBook == null) return;

                String slId = null, tgtId = null;
                double slPrice = 0, tgtPrice = 0;

                for (JsonNode order : orderBook) {
                    if (!order.has("symbol") || !order.get("symbol").asText().equals(symbol)) continue;
                    int status = order.has("status") ? order.get("status").asInt() : 0;
                    if (status != 6) continue; // only PENDING orders
                    int side = order.has("side") ? order.get("side").asInt() : 0;
                    if (side != exitSide) continue; // must be on exit side
                    int type = order.has("type") ? order.get("type").asInt() : 0;
                    String id = order.has("id") ? order.get("id").asText() : "";
                    if (id.isEmpty() && order.has("orderNumber")) id = order.get("orderNumber").asText();

                    if (type == 3 && slId == null) { // SL-M order
                        slId = id;
                        slPrice = order.has("stopPrice") ? order.get("stopPrice").asDouble() : 0;
                    } else if (type == 1 && tgtId == null) { // Limit order
                        tgtId = id;
                        tgtPrice = order.has("limitPrice") ? order.get("limitPrice").asDouble() : 0;
                    }
                }

                if (slId != null && tgtId != null) {
                    eventService.log("[SUCCESS] Detected manual SL (" + slId + " @ " + slPrice
                        + ") and Target (" + tgtId + " @ " + tgtPrice + ") for " + symbol + " — starting OCO monitor");
                    monitorOCO(slId, tgtId, symbol, qty, exitSide, slPrice, tgtPrice,
                        positionSide, setup, entryFillPrice);
                    if (ss.future != null) ss.future.cancel(false);
                }
            } catch (Exception e) {
                eventService.log("[ERROR] Manual OCO scan error for " + symbol + ": " + e.getMessage());
            }
        }, 5, 10, TimeUnit.SECONDS);
    }

    // ── OCO MONITOR ───────────────────────────────────────────────────────────
    private void monitorOCO(String slId, String targetId, String symbol, int qty,
                             int exitSide, double slPrice, double targetPrice,
                             String positionSide, String setup, double entryFillPrice) {
        class State {
            ScheduledFuture<?> future;
            boolean slManualCancelled     = false;
            boolean targetManualCancelled = false;
            boolean handled               = false;
            int ticks = 0;
            double currentSlPrice     = slPrice;
            double currentTargetPrice = targetPrice;
        }
        State s = new State();
        ocoMonitoredSymbols.add(symbol);
        // Store future reference for handoff cancellation
        positionStateStore.saveOcoState(symbol, slId, targetId, slPrice, targetPrice);
        eventService.log("[INFO] OCO monitor started for " + symbol + " — SL: " + slId + " | Target: " + targetId);

        s.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (s.handled) return;
                // Check if WebSocket already handled this position
                if ("NONE".equals(PositionManager.getPosition(symbol))) {
                    s.handled = true;
                    ocoMonitoredSymbols.remove(symbol);
                    if (s.future != null) s.future.cancel(false);
                    return;
                }
                s.ticks++;
                if (s.ticks > 4680) {
                    ocoMonitoredSymbols.remove(symbol);
                    eventService.log("[WARNING] OCO monitor timeout for " + symbol + " after 6.5 hours");
                    if (s.future != null) s.future.cancel(false);
                    return;
                }

                JsonNode slNode     = getOrderNode(slId);
                JsonNode targetNode = getOrderNode(targetId);

                String slStatus     = slNode != null && slNode.has("status") ? slNode.get("status").asText() : null;
                String targetStatus = targetNode != null && targetNode.has("status") ? targetNode.get("status").asText() : null;


                // First poll: sync baseline prices from Fyers (tick-rounded) and log diagnostics
                if (s.ticks == 1) {
                    if (slNode != null && slNode.has("stopPrice")) {
                        double liveSl = slNode.get("stopPrice").asDouble();
                        if (liveSl > 0) s.currentSlPrice = liveSl;
                    }
                    if (targetNode != null && targetNode.has("limitPrice")) {
                        double liveTgt = targetNode.get("limitPrice").asDouble();
                        if (liveTgt > 0) s.currentTargetPrice = liveTgt;
                    }
                    eventService.log("[INFO] " + symbol + " OCO first poll — SL status: " + slStatus + " | Target status: " + targetStatus);
                }

                boolean slFilled     = "2".equals(slStatus);
                boolean targetFilled = "2".equals(targetStatus);
                boolean slCancelled  = "1".equals(slStatus);   // Fyers: 1 = cancelled
                boolean tgtCancelled = "1".equals(targetStatus);
                boolean slRejected   = "5".equals(slStatus);   // Fyers: 5 = rejected
                boolean tgtRejected  = "5".equals(targetStatus);

                // ── Detect manual SL/Target price modifications ──
                if (slNode != null && "6".equals(slStatus) && slNode.has("stopPrice")) {
                    double liveSlPrice = slNode.get("stopPrice").asDouble();
                    if (liveSlPrice > 0 && Math.abs(liveSlPrice - s.currentSlPrice) > 0.10) {
                        eventService.log("[SUCCESS] SL modified for " + symbol + ": " + s.currentSlPrice + " → " + liveSlPrice);
                        s.currentSlPrice = liveSlPrice;
                        positionStateStore.saveOcoState(symbol, slId, targetId, s.currentSlPrice, s.currentTargetPrice);
                    }
                }
                if (targetNode != null && "6".equals(targetStatus) && targetNode.has("limitPrice")) {
                    double liveTargetPrice = targetNode.get("limitPrice").asDouble();
                    if (liveTargetPrice > 0 && Math.abs(liveTargetPrice - s.currentTargetPrice) > 0.10) {
                        eventService.log("[SUCCESS] Target modified for " + symbol + ": " + s.currentTargetPrice + " → " + liveTargetPrice);
                        s.currentTargetPrice = liveTargetPrice;
                        positionStateStore.saveOcoState(symbol, slId, targetId, s.currentSlPrice, s.currentTargetPrice);
                    }
                }

                // SL hit
                if (slFilled && !s.handled) {
                    s.handled = true;
                    ocoHandledSymbols.add(symbol);
                    ocoMonitoredSymbols.remove(symbol);
                    if (!orderService.cancelOrder(targetId)) {
                        eventService.log("[WARNING] Failed to cancel target order " + targetId + " for " + symbol);
                    }
                    // Use order ID for exact match — avoids picking up wrong trade on same symbol
                    double exitPrice  = orderService.getFilledPriceByOrderId(slId);
                    if (exitPrice <= 0) exitPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = entryFillPrice > 0 ? entryFillPrice
                                      : orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    double finalEntry = entryPrice > 0 ? entryPrice : s.currentSlPrice;
                    double finalExit  = exitPrice  > 0 ? exitPrice  : s.currentSlPrice;
                    double pnl = "LONG".equals(positionSide) ? (finalExit - finalEntry) * qty : (finalEntry - finalExit) * qty;
                    String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                    eventService.log("[SUCCESS] SL triggered for " + symbol + " at " + finalExit
                        + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)) + " — cancelling target");
                    positionStateStore.appendDescription(symbol,
                        "[EXIT] SL @ " + String.format("%.2f", finalExit) + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
                    String slDesc = positionStateStore.getDescription(symbol);
                    tradeHistoryService.record(symbol, positionSide, qty, finalEntry, finalExit, "SL", setup, slDesc, probabilityBySymbol.getOrDefault(symbol, ""));
                    clearSymbolState(symbol);
                    if (s.future != null) s.future.cancel(false);
                }
                // Target hit
                else if (targetFilled && !s.handled) {
                    s.handled = true;
                    ocoHandledSymbols.add(symbol);
                    ocoMonitoredSymbols.remove(symbol);
                    if (!orderService.cancelOrder(slId)) {
                        eventService.log("[WARNING] Failed to cancel SL order " + slId + " for " + symbol);
                    }
                    // Use order ID for exact match — avoids picking up wrong trade on same symbol
                    double exitPrice  = orderService.getFilledPriceByOrderId(targetId);
                    if (exitPrice <= 0) exitPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = entryFillPrice > 0 ? entryFillPrice
                                      : orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    double finalEntry = entryPrice > 0 ? entryPrice : s.currentTargetPrice;
                    double finalExit  = exitPrice  > 0 ? exitPrice  : s.currentTargetPrice;
                    double pnl = "LONG".equals(positionSide) ? (finalExit - finalEntry) * qty : (finalEntry - finalExit) * qty;
                    String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                    eventService.log("[SUCCESS] Target hit for " + symbol + " at " + finalExit
                        + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)) + " — cancelling SL");
                    positionStateStore.appendDescription(symbol,
                        "[EXIT] TARGET @ " + String.format("%.2f", finalExit) + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
                    String tgtDesc = positionStateStore.getDescription(symbol);
                    tradeHistoryService.record(symbol, positionSide, qty, finalEntry, finalExit, "TARGET", setup, tgtDesc, probabilityBySymbol.getOrDefault(symbol, ""));
                    clearSymbolState(symbol);
                    if (s.future != null) s.future.cancel(false);
                }
                // SL manually cancelled
                else if (slCancelled && !s.slManualCancelled
                         && !PositionManager.getPosition(symbol).equals("NONE")) {
                    s.slManualCancelled = true;
                    eventService.log(!tgtCancelled
                        ? "[WARNING] SL order manually cancelled for " + symbol + " — target still active"
                        : "[WARNING] SL order manually cancelled for " + symbol);
                }
                // Target manually cancelled
                else if (tgtCancelled && !s.targetManualCancelled
                         && !PositionManager.getPosition(symbol).equals("NONE")) {
                    s.targetManualCancelled = true;
                    eventService.log(!slCancelled
                        ? "[WARNING] Target order manually cancelled for " + symbol + " — SL still active"
                        : "[WARNING] Target order manually cancelled for " + symbol);
                }
                // Both cancelled — position unprotected
                else if (slCancelled && tgtCancelled && s.slManualCancelled && s.targetManualCancelled
                         && !PositionManager.getPosition(symbol).equals("NONE") && !s.handled) {
                    s.handled = true;
                    ocoMonitoredSymbols.remove(symbol);
                    eventService.log("[WARNING] Both SL and Target cancelled for " + symbol + " — position unprotected");
                    if (s.future != null) s.future.cancel(false);
                }
                // SL or Target rejected by Fyers — position unprotected
                else if ((slRejected || tgtRejected) && !s.handled) {
                    s.handled = true;
                    ocoMonitoredSymbols.remove(symbol);
                    if (slRejected && tgtRejected)
                        eventService.log("[ERROR] Both SL and Target orders rejected by Fyers for " + symbol + " — position UNPROTECTED");
                    else if (slRejected)
                        eventService.log("[ERROR] SL order rejected by Fyers for " + symbol + " — position UNPROTECTED");
                    else
                        eventService.log("[ERROR] Target order rejected by Fyers for " + symbol + " — position UNPROTECTED");
                    if (s.future != null) s.future.cancel(false);
                }
                // Position closed externally — stop monitoring silently
                if (!s.handled && PositionManager.getPosition(symbol).equals("NONE")
                        && slCancelled && tgtCancelled) {
                    s.handled = true;
                    ocoMonitoredSymbols.remove(symbol);
                    if (s.future != null) s.future.cancel(false);
                }

            } catch (Exception e) {
                eventService.log("[ERROR] OCO monitor exception for " + symbol + ": " + e.getMessage());
                log.error("[PollingService] OCO monitor exception for {}", symbol, e);
            }
        }, 5, 5, TimeUnit.SECONDS);
        ocoPollingFutures.put(symbol, s.future);
    }

    /** Split target OCO monitor — polls SL + T1 + T2 orders. */
    private void monitorSplitOCO(String slId, String t1Id, String t2Id,
                                  String symbol, int totalQty, int t1Qty, int t2Qty,
                                  int exitSide, double slPrice, double t1Price, double t2Price,
                                  String positionSide, String setup, double entryFillPrice) {
        if (ocoMonitoredSymbols.contains(symbol)) return;
        ocoMonitoredSymbols.add(symbol);

        class State {
            ScheduledFuture<?> future;
            boolean handled = false;
            boolean t1Hit = false;
            String currentSlId;
            int remainingQty;
            int ticks = 0;
            State() { currentSlId = slId; remainingQty = totalQty; }
        }
        State s = new State();

        s.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (s.handled || ++s.ticks > 4680) {
                    if (s.ticks > 4680) eventService.log("[WARNING] Split OCO monitor timed out for " + symbol);
                    ocoMonitoredSymbols.remove(symbol);
                    ocoPollingFutures.remove(symbol);
                    if (s.future != null) s.future.cancel(false);
                    return;
                }

                String slStatus = getOrderStatus(s.currentSlId);
                String t1Status = !s.t1Hit ? getOrderStatus(t1Id) : "2"; // already filled
                String t2Status = getOrderStatus(t2Id);

                // T1 filled (before T2 and SL)
                if (!s.t1Hit && "2".equals(t1Status) && !"2".equals(slStatus)) {
                    s.t1Hit = true;
                    s.remainingQty = totalQty - t1Qty;

                    double exitPrice = orderService.getFilledPriceByOrderId(t1Id);
                    double pnl = "LONG".equals(positionSide) ? (exitPrice - entryFillPrice) * t1Qty
                                                              : (entryFillPrice - exitPrice) * t1Qty;
                    eventService.log("[SUCCESS] T1 hit for " + symbol + " @ " + exitPrice + " qty=" + t1Qty
                        + " P&L ₹" + String.format("%.2f", pnl));

                    positionStateStore.appendDescription(symbol,
                        "[T1_HIT] @ " + String.format("%.2f", exitPrice) + " qty=" + t1Qty);
                    String desc = positionStateStore.getDescription(symbol);
                    String prob = probabilityBySymbol.getOrDefault(symbol, "");
                    tradeHistoryService.record(symbol, positionSide, t1Qty, entryFillPrice, exitPrice, "TARGET_1", setup, desc, prob);

                    // Cancel old SL, place new SL at breakeven
                    orderService.cancelOrder(s.currentSlId);
                    double breakeven = orderService.roundToTick(entryFillPrice, symbol);
                    OrderDTO newSl = orderService.placeStopLoss(symbol, s.remainingQty, exitSide, breakeven);
                    if (newSl != null && newSl.getId() != null && !newSl.getId().isEmpty()) {
                        s.currentSlId = newSl.getId();
                        positionStateStore.saveT1FilledState(symbol, s.currentSlId, breakeven, s.remainingQty);
                        updateCachedPositionQty(symbol, s.remainingQty);
                        positionStateStore.appendDescription(symbol,
                            "[SL_BREAKEVEN] New SL @ " + String.format("%.2f", breakeven) + " qty=" + s.remainingQty);
                        eventService.log("[SUCCESS] SL moved to breakeven " + breakeven + " for " + symbol + " remaining qty=" + s.remainingQty);
                    } else {
                        eventService.log("[ERROR] Failed to place breakeven SL for " + symbol + " — UNPROTECTED");
                    }

                    telegramService.notifyT1Hit(symbol, positionSide, t1Qty, exitPrice, pnl, entryFillPrice, s.remainingQty);
                }

                // T2 filled
                if ("2".equals(t2Status) && !s.handled) {
                    s.handled = true;
                    orderService.cancelOrder(s.currentSlId);
                    double exitPrice = orderService.getFilledPriceByOrderId(t2Id);
                    int exitQty = s.t1Hit ? t2Qty : totalQty; // shouldn't happen: T2 without T1
                    double pnl = "LONG".equals(positionSide) ? (exitPrice - entryFillPrice) * exitQty
                                                              : (entryFillPrice - exitPrice) * exitQty;
                    eventService.log("[SUCCESS] T2 hit for " + symbol + " @ " + exitPrice + " — fully closed");

                    positionStateStore.appendDescription(symbol,
                        "[T2_HIT] @ " + String.format("%.2f", exitPrice) + " qty=" + exitQty);
                    String desc = positionStateStore.getDescription(symbol);
                    String prob = probabilityBySymbol.getOrDefault(symbol, "");
                    tradeHistoryService.record(symbol, positionSide, exitQty, entryFillPrice, exitPrice, "TARGET_2", setup, desc, prob);

                    telegramService.notifyT2Hit(symbol, positionSide, exitQty, exitPrice, pnl);

                    clearSymbolState(symbol);
                    ocoMonitoredSymbols.remove(symbol);
                    ocoPollingFutures.remove(symbol);
                    if (s.future != null) s.future.cancel(false);
                }

                // SL filled
                if ("2".equals(slStatus) && !s.handled) {
                    s.handled = true;
                    if (!s.t1Hit) orderService.cancelOrder(t1Id);
                    orderService.cancelOrder(t2Id);

                    double exitPrice = orderService.getFilledPriceByOrderId(s.currentSlId);
                    int exitQty = s.t1Hit ? s.remainingQty : totalQty;
                    double pnl = "LONG".equals(positionSide) ? (exitPrice - entryFillPrice) * exitQty
                                                              : (entryFillPrice - exitPrice) * exitQty;
                    String reason = s.t1Hit ? "SL_BREAKEVEN" : "SL";
                    eventService.log("[SUCCESS] " + reason + " for " + symbol + " @ " + exitPrice
                        + " qty=" + exitQty + (s.t1Hit ? " (after T1, breakeven)" : " (before T1)"));

                    positionStateStore.appendDescription(symbol,
                        "[EXIT] " + reason + " @ " + String.format("%.2f", exitPrice) + " qty=" + exitQty);
                    String desc = positionStateStore.getDescription(symbol);
                    String prob = probabilityBySymbol.getOrDefault(symbol, "");
                    tradeHistoryService.record(symbol, positionSide, exitQty, entryFillPrice, exitPrice, reason, setup, desc, prob);

                    telegramService.notifySlHit(symbol, positionSide, exitQty, exitPrice, pnl, reason);

                    clearSymbolState(symbol);
                    ocoMonitoredSymbols.remove(symbol);
                    ocoPollingFutures.remove(symbol);
                    if (s.future != null) s.future.cancel(false);
                }

            } catch (Exception e) {
                eventService.log("[ERROR] Split OCO monitor exception for " + symbol + ": " + e.getMessage());
                log.error("[PollingService] Split OCO monitor exception for {}", symbol, e);
            }
        }, 5, 5, TimeUnit.SECONDS);
        ocoPollingFutures.put(symbol, s.future);
    }

    /** Clears all in-memory and persisted state for a single symbol. */
    private void clearSymbolState(String symbol) {
        positionStateStore.clear(symbol);
        cachedPositions.remove(symbol);
        setupBySymbol.remove(symbol);
        probabilityBySymbol.remove(symbol);
        entryTimeBySymbol.remove(symbol);
        entryAvgBySymbol.remove(symbol);
        PositionManager.setPosition(symbol, "NONE");
        marketDataService.updateSubscriptions();
        marketDataService.clearTrailedFlag(symbol);
        orderEventService.untrackSymbol(symbol);
        breakoutScanner.clearBrokenLevels(symbol);
    }

    // ── Public helpers for OrderEventService (WebSocket-based fill handling) ──

    /** Called by OrderEventService on WS entry fill to set in-memory symbol state. */
    public void setSymbolState(String symbol, String setup, String entryTime, double entryPrice) {
        setupBySymbol.put(symbol, setup != null ? setup : "");
        entryTimeBySymbol.put(symbol, entryTime);
        entryAvgBySymbol.put(symbol, entryPrice);
        pendingEntrySymbols.remove(symbol);
    }

    /** Called by OrderEventService to add a position to the cached positions map. */
    public void addCachedPosition(String symbol, int qty, String side, double avgPrice, String setup, String entryTime) {
        cachedPositions.put(symbol, new PositionsDTO(symbol, qty, side, avgPrice, avgPrice, 0.0, setup, entryTime));
    }

    /** Update qty of an existing cached position (e.g., after T1 partial fill). */
    public void updateCachedPositionQty(String symbol, int newQty) {
        PositionsDTO existing = cachedPositions.get(symbol);
        if (existing != null) {
            cachedPositions.put(symbol, new PositionsDTO(symbol, newQty, existing.getSide(),
                existing.getAvgPrice(), existing.getLtp(), 0.0, existing.getSetup(), existing.getEntryTime()));
        }
    }

    /** Called by OrderEventService to remove a position from the cached positions map. */
    public void removeCachedPosition(String symbol) {
        cachedPositions.remove(symbol);
    }

    /** Called by OrderEventService on WS OCO fill to clear state. */
    public void clearSymbolStateFromWs(String symbol) {
        clearSymbolState(symbol);
    }

    /** Get cached entry average price for a symbol. */
    public double getEntryAvg(String symbol) {
        return entryAvgBySymbol.getOrDefault(symbol, 0.0);
    }

    /**
     * Called when Order WebSocket connects — hand off polling OCO monitors to WebSocket tracking.
     * This handles the case where OCO was restored via polling on startup before WS was ready.
     */
    public void handoffOcoToWebSocket() {
        Map<String, Map<String, Object>> allSaved = positionStateStore.loadAll();
        int handedOff = 0;
        for (Map.Entry<String, Map<String, Object>> entry : allSaved.entrySet()) {
            Map<String, Object> saved = entry.getValue();
            Object symObj = saved.get("symbol");
            if (symObj == null) continue;
            String symbol = symObj.toString();
            String slOrderId = saved.get("slOrderId") != null ? saved.get("slOrderId").toString() : "";
            String targetOrderId = saved.get("targetOrderId") != null ? saved.get("targetOrderId").toString() : "";
            if (slOrderId.isEmpty()) continue;

            // Skip if position is already closed
            if ("NONE".equals(PositionManager.getPosition(symbol))) {
                log.info("[PollingService] Skipping handoff for {} — position already closed", symbol);
                continue;
            }

            String side = saved.get("side") != null ? saved.get("side").toString() : "";
            if (side.isEmpty()) continue;
            int qty = saved.get("qty") != null ? Integer.parseInt(saved.get("qty").toString()) : 0;
            double avgPrice = saved.get("avgPrice") != null ? Double.parseDouble(saved.get("avgPrice").toString()) : 0;
            String setup = saved.get("setup") != null ? saved.get("setup").toString() : "";
            int exitSide = "LONG".equals(side) ? -1 : 1;
            double slPrice = saved.get("slPrice") != null ? Double.parseDouble(saved.get("slPrice").toString()) : 0;
            double targetPrice = saved.get("targetPrice") != null ? Double.parseDouble(saved.get("targetPrice").toString()) : 0;

            boolean tracked = orderEventService.trackOcoOrders(slOrderId, targetOrderId,
                symbol, qty, side, exitSide, setup, avgPrice, slPrice, targetPrice);
            if (tracked) {
                // Preserve trailed flag if MarketDataService already trailed SL for this symbol
                if (marketDataService.isTrailed(symbol)) {
                    orderEventService.markAsTrailed(slOrderId);
                    log.info("[PollingService] Preserved trailed flag for {} during handoff", symbol);
                }
                handedOff++;
                ocoMonitoredSymbols.remove(symbol);
                ocoHandledSymbols.add(symbol);
                // Cancel the polling monitor immediately
                ScheduledFuture<?> pollingFuture = ocoPollingFutures.remove(symbol);
                if (pollingFuture != null) pollingFuture.cancel(false);
                log.info("[PollingService] Handed off OCO to WebSocket for {} — polling cancelled", symbol);
            }
        }
        if (handedOff > 0) {
            eventService.log("[INFO] Handed off " + handedOff + " OCO monitors to WebSocket");
        }
    }

    // ── ORDER STATUS (cached order book) ────────────────────────────────────
    private volatile JsonNode cachedOrderBook = null;
    private volatile long     orderBookFetchTime = 0;
    private static final long ORDER_BOOK_CACHE_MS = 5000; // 5 seconds

    /** Fetches the full order book once, caches for ORDER_BOOK_CACHE_MS. */
    private JsonNode getOrderBook() {
        long now = System.currentTimeMillis();
        if (cachedOrderBook != null && (now - orderBookFetchTime) < ORDER_BOOK_CACHE_MS) {
            return cachedOrderBook;
        }
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getOrders(auth);
            JsonNode orderBook = node != null ? node.get("orderBook") : null;
            if (orderBook != null && orderBook.isArray()) {
                cachedOrderBook = orderBook;
                orderBookFetchTime = now;
                return orderBook;
            }
            // Rate limit hit — return stale cache, normal 5s TTL will handle retry
            if (node != null && node.has("code") && node.get("code").asInt() == -429) {
                log.info("[PollingService] Fyers rate limit hit — using cached order book");
                return cachedOrderBook;
            }
            eventService.log("[WARNING] getOrders response missing orderBook: "
                + (node != null ? node.toString().substring(0, Math.min(200, node.toString().length())) : "null"));
        } catch (Exception e) {
            eventService.log("[ERROR] Failed to fetch order book: " + e.getMessage());
        }
        return cachedOrderBook; // return stale cache on error (better than null)
    }

    private String getOrderStatus(String orderId) {
        JsonNode node = getOrderNode(orderId);
        return node != null && node.has("status") ? node.get("status").asText() : null;
    }

    /** Returns the full order node from cached orderBook for the given orderId, or null. */
    private JsonNode getOrderNode(String orderId) {
        JsonNode orderBook = getOrderBook();
        if (orderBook == null) return null;
        for (JsonNode order : orderBook) {
            // Fyers may use "id" or "orderNumber" for the order identifier
            String oid = order.has("id") ? order.get("id").asText() : "";
            String onum = order.has("orderNumber") ? order.get("orderNumber").asText() : "";
            if (orderId.equals(oid) || orderId.equals(onum)) {
                return order;
            }
        }
        return null;
    }

    /**
     * Find the latest filled (status=2) order for a symbol and return its tradedPrice.
     * Scans order book for BUY orders if side=LONG, SELL orders if side=SHORT.
     */
    private double getLatestFilledPrice(String symbol, String side) {
        try {
            JsonNode orderBook = getOrderBook();
            if (orderBook == null) return 0;
            // Fyers side: 1=BUY, -1=SELL
            int expectedSide = "LONG".equals(side) ? 1 : -1;
            double latestPrice = 0;
            String latestTime = "";
            for (JsonNode order : orderBook) {
                String sym = order.has("symbol") ? order.get("symbol").asText() : "";
                int status = order.has("status") ? order.get("status").asInt() : 0;
                int orderSide = order.has("side") ? order.get("side").asInt() : 0;
                if (sym.equals(symbol) && status == 2 && orderSide == expectedSide) {
                    double tp = order.has("tradedPrice") ? order.get("tradedPrice").asDouble() : 0;
                    String time = order.has("orderDateTime") ? order.get("orderDateTime").asText() : "";
                    if (tp > 0 && time.compareTo(latestTime) > 0) {
                        latestPrice = tp;
                        latestTime = time;
                    }
                }
            }
            return latestPrice;
        } catch (Exception e) {
            log.error("[SyncPosition] Error finding fill price for {}: {}", symbol, e.getMessage());
            return 0;
        }
    }

    // ── SQUARE OFF ────────────────────────────────────────────────────────────
    public boolean squareOff(String symbol, int quantity) {
        return squareOff(symbol, quantity, "MANUAL");
    }

    public boolean squareOff(String symbol, int quantity, String reason) {
        String label = "AUTO_SQUAREOFF".equals(reason) ? "Auto square off" : "Manual Square off";
        String exitReason = reason != null ? reason : "MANUAL";
        try {
            String position = PositionManager.getPosition(symbol);
            if ("NONE".equals(position)) return false;
            int exitSide = "LONG".equals(position) ? -1 : 1;

            eventService.log("[SUCCESS] " + label + " for " + symbol + " — cancelling SL/Target orders");
            // Cancel by known order IDs first (fast, no API call to fetch order book)
            Map<String, Object> state = positionStateStore.load(symbol);
            String slOid  = state != null ? state.getOrDefault("slOrderId", "").toString() : "";
            String tgtOid = state != null ? state.getOrDefault("targetOrderId", "").toString() : "";
            if (!slOid.isEmpty())  orderService.cancelOrder(slOid);
            if (!tgtOid.isEmpty()) orderService.cancelOrder(tgtOid);
            // Fallback: cancel any remaining pending orders for the symbol
            if (slOid.isEmpty() && tgtOid.isEmpty()) {
                orderService.cancelAllPendingOrders(symbol);
            }

            // Mark as recently handled so WebSocket doesn't record this as MANUAL
            orderEventService.markRecentlyHandled(symbol);
            // Untrack OCO orders so WS doesn't interfere
            orderEventService.untrackSymbol(symbol);
            // Mark early so syncPosition doesn't record MANUAL during the sleep below
            ocoHandledSymbols.add(symbol);

            OrderDTO exitOrder = orderService.placeExitOrder(symbol, quantity, exitSide);
            if (exitOrder == null || !"ok".equals(exitOrder.getStatus())) {
                eventService.log("[ERROR] " + label + " failed for " + symbol + " — exit order rejected");
                return false;
            }

            // Wait for exit order to fill and appear in tradebook
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}

            double entryPrice = entryAvgBySymbol.getOrDefault(symbol, 0.0);
            if (entryPrice <= 0) {
                int entrySide = exitSide == 1 ? -1 : 1;
                entryPrice = orderService.getFilledPriceFromTradebook(symbol, entrySide);
            }

            // Get exit price by order ID first (most accurate), then tradebook, then LTP
            orderService.invalidateTradebookCache();
            double exitPrice = 0;
            if (exitOrder.getId() != null && !exitOrder.getId().isEmpty()) {
                exitPrice = orderService.getFilledPriceByOrderId(exitOrder.getId());
            }
            if (exitPrice <= 0) {
                exitPrice = orderService.getExitPriceFromTradebook(symbol, position);
            }
            if (exitPrice <= 0) {
                exitPrice = marketDataService.getLtp(symbol);
            }
            String setup = setupBySymbol.getOrDefault(symbol, "");
            if (entryPrice > 0 && exitPrice > 0) {
                double pnl = "LONG".equals(position) ? (exitPrice - entryPrice) * quantity : (entryPrice - exitPrice) * quantity;
                String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                eventService.log("[SUCCESS] " + label + " executed for " + symbol + " at " + exitPrice
                    + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
                positionStateStore.appendDescription(symbol,
                    "[EXIT] " + exitReason + " @ " + String.format("%.2f", exitPrice)
                    + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
                String desc = positionStateStore.getDescription(symbol);
                String prob = probabilityBySymbol.getOrDefault(symbol, "");
                tradeHistoryService.record(symbol, position, quantity, entryPrice, exitPrice, exitReason, setup, desc, prob);
                if (!"AUTO_SQUAREOFF".equals(exitReason)) {
                    telegramService.notifySlHit(symbol, position, quantity, exitPrice, pnl, exitReason);
                }
            } else {
                eventService.log("[SUCCESS] " + label + " executed for " + symbol + " — position closed");
                positionStateStore.appendDescription(symbol, "[EXIT] " + exitReason + " — position closed.");
                String desc = positionStateStore.getDescription(symbol);
                String prob = probabilityBySymbol.getOrDefault(symbol, "");
                tradeHistoryService.record(symbol, position, quantity, entryPrice, exitPrice > 0 ? exitPrice : 0, exitReason, setup, desc, prob);
            }
            clearSymbolState(symbol);
            return true;

        } catch (Exception e) {
            log.error("[PollingService] {} error for {}", label, symbol, e);
            eventService.log("[ERROR] " + label + " error for " + symbol + ": " + e.getMessage());
            return false;
        }
    }

    // ── AUTO SQUARE OFF SCHEDULER ───────────────────────────────────────────
    private volatile LocalDate lastAutoSquareOffDate = null;

    private void startAutoSquareOffScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                String sqTime = riskSettings.getAutoSquareOffTime();
                if (sqTime == null || sqTime.isBlank()) return;
                if (!PositionManager.hasAnyPosition()) return;

                LocalTime now = LocalTime.now();
                LocalTime target = LocalTime.parse(sqTime, DateTimeFormatter.ofPattern("HH:mm"));
                LocalDate today = LocalDate.now();

                // Trigger once per day when current time crosses the configured time
                if (now.isAfter(target) && !today.equals(lastAutoSquareOffDate)) {
                    lastAutoSquareOffDate = today;
                    eventService.log("[AUTO] Scheduled square off triggered at " + sqTime);
                    squareOffAll();
                }
            } catch (Exception e) {
                log.error("[PollingService] Auto square off check error: {}", e.getMessage());
            }
        }, 10, 15, TimeUnit.SECONDS);
    }

    // ── TELEGRAM END-OF-DAY SUMMARY ────────────────────────────────────────
    // Initialize to today if already past 3:30 PM so a restart after market close doesn't re-send
    private volatile LocalDate lastEodSummaryDate =
        LocalTime.now().isAfter(LocalTime.of(15, 30)) ? LocalDate.now() : null;

    private void startEodSummaryScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LocalTime now = LocalTime.now();
                LocalDate today = LocalDate.now();
                if (today.equals(lastEodSummaryDate)) return;
                if (now.isBefore(LocalTime.of(15, 30))) return;

                lastEodSummaryDate = today;
                List<com.rydytrader.autotrader.dto.TradeRecord> trades = tradeHistoryService.getTrades();
                int wins = 0, losses = 0;
                double totalPnl = 0, totalCharges = 0, totalNetPnl = 0;
                for (com.rydytrader.autotrader.dto.TradeRecord t : trades) {
                    if (t.getNetPnl() >= 0) wins++; else losses++;
                    totalPnl += t.getPnl();
                    totalCharges += t.getCharges();
                    totalNetPnl += t.getNetPnl();
                }
                telegramService.notifyDaySummary(trades.size(), wins, losses, totalPnl, totalCharges, totalNetPnl);
            } catch (Exception e) {
                log.error("[PollingService] EOD summary error: {}", e.getMessage());
            }
        }, 15, 15, TimeUnit.SECONDS);
    }

    // Periodic portfolio summary removed — only event-driven notifications now
    private void startTelegramSummaryScheduler() {
        // No-op: replaced by structured notifications (trade opened/T1/T2/SL/squareoff/EOD)
    }

    /** Squares off all open positions. */
    public void squareOffAll() {
        List<PositionsDTO> positions = fetchPositions();
        if (positions.isEmpty()) {
            eventService.log("[AUTO] No open positions to square off");
            return;
        }
        List<String> sqLines = new java.util.ArrayList<>();
        double totalPnl = 0;
        for (PositionsDTO pos : positions) {
            String symbol = pos.getSymbol();
            int qty = Math.abs(pos.getQty());
            if (qty == 0) continue;
            try {
                boolean ok = squareOff(symbol, qty, "AUTO_SQUAREOFF");
                if (!ok) {
                    eventService.log("[ERROR] Auto square off failed for " + symbol);
                    sqLines.add(symbol.replace("NSE:", "") + " | FAILED");
                } else {
                    double pnl = pos.getPnl();
                    totalPnl += pnl;
                    sqLines.add(symbol.replace("NSE:", "") + " | " + pos.getSide() + " " + qty
                        + " | P&L: " + (pnl >= 0 ? "+" : "") + String.format("%.2f", pnl));
                }
            } catch (Exception e) {
                eventService.log("[ERROR] Auto square off error for " + symbol + ": " + e.getMessage());
                sqLines.add(symbol.replace("NSE:", "") + " | ERROR");
            }
        }
        if (!sqLines.isEmpty()) {
            telegramService.notifyAutoSquareoff(sqLines, totalPnl);
        }
    }

    // ── POSITION SYNC ─────────────────────────────────────────────────────────
    private volatile boolean positionSyncStarted = false;

    public void startPositionSync() {
        if (positionSyncStarted) return;
        positionSyncStarted = true;
        scheduler.scheduleAtFixedRate(() -> {
            if (tokenStore.getAccessToken() == null) return;
            // Skip sync entirely when both WebSockets are connected (data + order)
            if (orderEventService.isConnected() && marketDataService.isConnected()) return;
            if (!PositionManager.hasAnyPosition() && positionStateStore.loadAll().isEmpty()) return;
            syncPosition();
        }, 5, 10, TimeUnit.SECONDS);
    }

    private void syncPosition() {
        try {
            connectionStatus = "SYNCING";
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getPositions(auth);

            Set<String> brokerSymbols = new HashSet<>();
            JsonNode positions = node.get("netPositions");
            if (positions != null) {
                for (JsonNode pos : positions) {
                    int qty = pos.has("netQty") ? pos.get("netQty").asInt()
                                                : pos.has("qty") ? pos.get("qty").asInt() : 0;
                    if (qty != 0) {
                        String symbol  = pos.get("symbol").asText();
                        if (pendingEntrySymbols.contains(symbol)) continue;
                        if (ocoHandledSymbols.contains(symbol)) {
                            // If PositionManager has an active position, a new trade opened after the old one closed
                            if (!"NONE".equals(PositionManager.getPosition(symbol))) {
                                ocoHandledSymbols.remove(symbol); // New position — let sync pick it up
                            } else {
                                continue; // Still a stale close — skip
                            }
                        }
                        int    side    = pos.get("side").asInt();
                        double avg     = pos.has("netAvgPrice") ? pos.get("netAvgPrice").asDouble()
                                       : pos.has("netAvg")      ? pos.get("netAvg").asDouble() : 0;
                        double ltp     = pos.has("ltp") ? pos.get("ltp").asDouble() : 0;
                        String posSide = side == 1 ? "LONG" : "SHORT";

                        // Load saved state once — used for both entryTime and avgPrice resolution
                        Map<String, Object> saved = positionStateStore.load(symbol);

                        String resolvedEntryTime = entryTimeBySymbol.getOrDefault(symbol, "");
                        if (resolvedEntryTime.isEmpty()) {
                            if (saved != null && saved.containsKey("entryTime"))
                                resolvedEntryTime = saved.get("entryTime").toString();
                        }
                        if (resolvedEntryTime.isEmpty())
                            resolvedEntryTime = eventService.getEntryTimeFromLogs();
                        if (resolvedEntryTime.isEmpty())
                            entryTimeBySymbol.put(symbol, resolvedEntryTime);

                        // Resolve entry price — never use Fyers netAvgPrice (blended across day's trades)
                        double resolvedAvg = 0;
                        double inMemAvg = entryAvgBySymbol.getOrDefault(symbol, 0.0);
                        if (inMemAvg > 0) {
                            resolvedAvg = inMemAvg;
                        } else if (saved != null && saved.containsKey("avgPrice")) {
                            double savedAvg = Double.parseDouble(saved.get("avgPrice").toString());
                            if (savedAvg > 0) resolvedAvg = savedAvg;
                        }
                        // Fallback: find the latest filled order for this symbol from order book
                        if (resolvedAvg <= 0) {
                            resolvedAvg = getLatestFilledPrice(symbol, posSide);
                            if (resolvedAvg > 0) {
                                log.info("[SyncPosition] {} resolved entry from order book: {}", symbol, String.format("%.2f", resolvedAvg));
                            } else {
                                // Last resort: use Fyers position avg (better than 0)
                                resolvedAvg = avg;
                                log.warn("[SyncPosition] {} no fill price found — using Fyers netAvgPrice {} as last resort", symbol, String.format("%.2f", avg));
                            }
                        }

                        String setup = setupBySymbol.getOrDefault(symbol, "");
                        if (setup.isEmpty() && saved != null && saved.containsKey("setup")) {
                            setup = saved.get("setup").toString();
                            setupBySymbol.put(symbol, setup);
                        }
                        int absQty = Math.abs(qty);
                        // Compute unrealized P&L from our resolved avg, not Fyers' day-level pl
                        // which includes realized P&L from earlier closed trades on the same symbol
                        double unrealizedPl = "LONG".equals(posSide)
                            ? (ltp - resolvedAvg) * absQty
                            : (resolvedAvg - ltp) * absQty;
                        boolean isNewPosition = !cachedPositions.containsKey(symbol);
                        cachedPositions.put(symbol, new PositionsDTO(symbol, absQty, posSide, resolvedAvg, ltp, unrealizedPl, setup, resolvedEntryTime));
                        PositionManager.setPosition(symbol, posSide);
                        if (isNewPosition) marketDataService.updateSubscriptions();
                        // Preserve existing OCO state (order IDs + prices) if already saved
                        Map<String, Object> existingState = positionStateStore.load(symbol);
                        double savedSl = existingState != null && existingState.containsKey("slPrice")
                            ? Double.parseDouble(existingState.get("slPrice").toString()) : 0;
                        double savedTarget = existingState != null && existingState.containsKey("targetPrice")
                            ? Double.parseDouble(existingState.get("targetPrice").toString()) : 0;
                        positionStateStore.save(symbol, posSide, absQty, resolvedAvg, setup, resolvedEntryTime, savedSl, savedTarget);
                        // Re-apply OCO order IDs if they were saved by monitorOCO
                        if (existingState != null) {
                            String savedSlId = existingState.getOrDefault("slOrderId", "").toString();
                            String savedTgtId = existingState.getOrDefault("targetOrderId", "").toString();
                            if (!savedSlId.isEmpty() && !savedTgtId.isEmpty()) {
                                positionStateStore.saveOcoState(symbol, savedSlId, savedTgtId, savedSl, savedTarget);
                            }
                        }
                        brokerSymbols.add(symbol);

                        // Auto-scan for SL/Target orders on newly detected positions without OCO monitor
                        if (isNewPosition && !ocoMonitoredSymbols.contains(symbol)) {
                            // Skip scan if OCO order IDs already exist on disk (restore will handle it)
                            String existSlId = existingState != null ? existingState.getOrDefault("slOrderId", "").toString() : "";
                            String existTgtId = existingState != null ? existingState.getOrDefault("targetOrderId", "").toString() : "";
                            if (existSlId.isEmpty() || existTgtId.isEmpty()) {
                                int exitSide = "LONG".equals(posSide) ? -1 : 1;
                                entryAvgBySymbol.put(symbol, resolvedAvg);
                                eventService.log("[INFO] New position detected for " + symbol + " — scanning for SL/Target orders");
                                scanForManualOCO(symbol, absQty, exitSide, posSide, setup, resolvedAvg);
                            }
                        }
                    }
                }
            }

            // Detect externally closed positions (per-symbol diff)
            if (justRestored && brokerSymbols.isEmpty()) {
                justRestored = false;
            } else {
                List<String> closedSymbols = new ArrayList<>();
                for (String symbol : new HashSet<>(cachedPositions.keySet())) {
                    if (!brokerSymbols.contains(symbol)) {
                        closedSymbols.add(symbol);
                    }
                }
                for (String symbol : closedSymbols) {
                    // Skip if OCO monitor is still active — let it handle the exit
                    if (ocoMonitoredSymbols.contains(symbol)) continue;
                    // Skip if OCO already handled this exit (SL/Target hit)
                    if (ocoHandledSymbols.remove(symbol)) {
                        cachedPositions.remove(symbol);
                        continue;
                    }
                    PositionsDTO prev = cachedPositions.get(symbol);
                    if (prev != null && !PositionManager.getPosition(symbol).equals("NONE")) {
                        // Cancel by known order IDs (fast) instead of fetching full order book
                        Map<String, Object> closedState = positionStateStore.load(symbol);
                        String cSlId  = closedState != null ? closedState.getOrDefault("slOrderId", "").toString() : "";
                        String cTgtId = closedState != null ? closedState.getOrDefault("targetOrderId", "").toString() : "";
                        if (!cSlId.isEmpty())  orderService.cancelOrder(cSlId);
                        if (!cTgtId.isEmpty()) orderService.cancelOrder(cTgtId);
                        double exitPrice = orderService.getExitPriceFromTradebook(symbol, prev.getSide());
                        double finalExit = exitPrice > 0 ? exitPrice : prev.getLtp();
                        double finalEntry = prev.getAvgPrice();
                        int absQty = Math.abs(prev.getQty());
                        double pnl = "LONG".equals(prev.getSide()) ? (finalExit - finalEntry) * absQty : (finalEntry - finalExit) * absQty;
                        String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                        eventService.log("[SUCCESS] Manual Square Off for " + symbol
                            + " initiated from Fyers Broker ( User / Automatic ) | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
                        tradeHistoryService.record(symbol, prev.getSide(), absQty,
                            finalEntry, finalExit,
                            "MANUAL", setupBySymbol.getOrDefault(symbol, ""), null, probabilityBySymbol.getOrDefault(symbol, ""));
                        clearSymbolState(symbol);
                    } else if (prev != null) {
                        // position already NONE (e.g. external square-off), just remove from cache
                        cachedPositions.remove(symbol);
                        setupBySymbol.remove(symbol);
                        entryTimeBySymbol.remove(symbol);
                    }
                }
            }

            lastSyncTime = java.time.LocalTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            connectionStatus = "POLLING";

        } catch (Exception e) {
            connectionStatus = "DISCONNECTED";
            log.error("[PollingService] Position sync error", e);
        }
    }

    // ── PUBLIC ACCESSORS ──────────────────────────────────────────────────────
    public void syncPositionOnce() { syncPosition(); }

    public List<PositionsDTO> fetchPositions() { return new ArrayList<>(cachedPositions.values()); }

    public String getConnectionStatus() {
        boolean orderWs = orderEventService.isConnected();
        boolean dataWs = marketDataService.isConnected();
        if (orderWs && dataWs) return "WS CONNECTED";
        if (orderWs && !dataWs) return "RECONNECTING (Data)";
        if (!orderWs && dataWs) return "RECONNECTING (Order)";
        if (marketDataService.isReconnecting() || orderEventService.isReconnecting()) return "RECONNECTING";
        // Initial post-login window: both WS clients running but not yet connected and no failed attempts yet.
        if (marketDataService.isConnecting() || orderEventService.isConnecting()) return "CONNECTING";
        return connectionStatus; // POLLING / SYNCING / DISCONNECTED
    }

    public String getLastSyncTime() { return lastSyncTime; }
    public int getOcoMonitorCount() { return ocoMonitoredSymbols.size(); }
    public Set<String> getOcoMonitoredSymbols() { return Collections.unmodifiableSet(ocoMonitoredSymbols); }
    public int getOpenPositionCount() { return cachedPositions.size(); }

    /** Update last sync time — called by WS handlers when positions are updated. */
    public void updateLastSyncTime() {
        lastSyncTime = java.time.LocalTime.now()
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    /** Returns setup for a specific symbol. */
    public String getCurrentSetup(String symbol) {
        return setupBySymbol.getOrDefault(symbol, "");
    }

    /** Returns first setup found — backward-compat for single-symbol callers. */
    public String getCurrentSetup() {
        return setupBySymbol.isEmpty() ? "" : setupBySymbol.values().iterator().next();
    }

    /** Returns probability for a specific symbol. */
    public String getProbability(String symbol) {
        return probabilityBySymbol.getOrDefault(symbol, "");
    }

    /** Sets probability for a symbol. Called from TradingController after signal processing. */
    public void setProbability(String symbol, String probability) {
        if (probability != null && !probability.isEmpty()) {
            probabilityBySymbol.put(symbol, probability);
            positionStateStore.saveProbability(symbol, probability);
        }
    }

    /** Removes the cached position for a specific symbol. */
    public void clearCachedPositions(String symbol) {
        cachedPositions.remove(symbol);
    }

    /** Clears all cached positions — used on mode switch. */
    public void clearCachedPositions() {
        cachedPositions.clear();
        setupBySymbol.clear();
        entryTimeBySymbol.clear();
        pendingEntrySymbols.clear();
        ocoMonitoredSymbols.clear();
    }
}
