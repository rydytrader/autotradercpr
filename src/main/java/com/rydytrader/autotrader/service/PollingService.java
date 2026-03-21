package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.dto.PositionsDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.mock.MockState;
import com.rydytrader.autotrader.store.ModeStore;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

@Service
public class PollingService {

    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final FyersClientRouter   fyersClient;
    private final OrderService        orderService;
    private final EventService        eventService;
    private final TradeHistoryService tradeHistoryService;
    private final PositionStateStore  positionStateStore;
    private final MockState           mockState;
    private final ModeStore           modeStore;
    private final RiskSettingsStore   riskSettings;
    private final TelegramService     telegramService;

    // ── per-symbol state ──────────────────────────────────────────────────────
    private final ConcurrentHashMap<String, PositionsDTO> cachedPositions  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String>       setupBySymbol    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String>       entryTimeBySymbol  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double>       entryAvgBySymbol   = new ConcurrentHashMap<>();
    private final Set<String> ocoHandledSymbols = ConcurrentHashMap.newKeySet();
    private final Set<String> ocoMonitoredSymbols = ConcurrentHashMap.newKeySet();
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
                          MockState mockState,
                          ModeStore modeStore,
                          RiskSettingsStore riskSettings,
                          TelegramService telegramService) {
        this.tokenStore          = tokenStore;
        this.fyersProperties     = fyersProperties;
        this.fyersClient         = fyersClient;
        this.orderService        = orderService;
        this.eventService        = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.positionStateStore  = positionStateStore;
        this.mockState           = mockState;
        this.modeStore           = modeStore;
        this.riskSettings        = riskSettings;
        this.telegramService     = telegramService;
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
                int    qty       = Integer.parseInt(saved.get("qty").toString());
                double avgPrice  = Double.parseDouble(saved.get("avgPrice").toString());
                String setup     = saved.getOrDefault("setup", "").toString();
                String entryTime = saved.getOrDefault("entryTime", "").toString();

                setupBySymbol.put(symbol, setup);
                entryTimeBySymbol.put(symbol, entryTime);
                entryAvgBySymbol.put(symbol, avgPrice);
                PositionManager.setPosition(symbol, side);
                cachedPositions.put(symbol, new PositionsDTO(symbol, qty, side, avgPrice, avgPrice, 0.0, setup, entryTime));

                if (modeStore.getMode() == ModeStore.Mode.SIMULATOR) {
                    mockState.restorePosition(symbol, side, qty, avgPrice);
                }

                // Restart OCO monitor if SL/Target order IDs were persisted
                String slOrderId     = saved.getOrDefault("slOrderId", "").toString();
                String targetOrderId = saved.getOrDefault("targetOrderId", "").toString();
                if (!slOrderId.isEmpty() && !targetOrderId.isEmpty()) {
                    double slPrice     = Double.parseDouble(saved.getOrDefault("slPrice", "0").toString());
                    double targetPrice = Double.parseDouble(saved.getOrDefault("targetPrice", "0").toString());
                    int exitSide       = "LONG".equals(side) ? -1 : 1;
                    monitorOCO(slOrderId, targetOrderId, symbol, qty, exitSide,
                        slPrice, targetPrice, side, setup, avgPrice);
                    System.out.println("[PollingService] Restored OCO monitor for " + symbol
                        + " — SL: " + slOrderId + " | Target: " + targetOrderId);
                } else {
                    // No saved OCO IDs — scan for manually placed SL/Target orders
                    int exitSide = "LONG".equals(side) ? -1 : 1;
                    System.out.println("[PollingService] Restored state on startup: " + side + " " + symbol
                        + " (no OCO IDs — scanning for manual SL/Target)");
                    scanForManualOCO(symbol, qty, exitSide, side, setup, avgPrice);
                }
            }
            justRestored = true;
        } catch (Exception e) {
            System.err.println("[PollingService] Failed to restore state: " + e.getMessage());
        }
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
        class Holder {
            ScheduledFuture<?> future;
            boolean positionSet    = false;  // set position state only once
            double  entryFillPrice = 0.0;    // captured at fill time, passed to OCO monitor
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
                        // Capture actual fill price from tradebook at entry time
                        int entrySide = exitSide == 1 ? -1 : 1;
                        holder.entryFillPrice = orderService.getFilledPriceFromTradebook(symbol, entrySide);
                        entryAvgBySymbol.put(symbol, holder.entryFillPrice);
                        pendingEntrySymbols.remove(symbol);
                        positionStateStore.save(symbol, position, quantity, holder.entryFillPrice,
                            setupBySymbol.get(symbol), entryTime);
                        eventService.log("[SUCCESS] " + (position.equals("LONG") ? "BUY" : "SELL")
                            + " order filled for " + symbol + " @ " + holder.entryFillPrice + " [ID: " + entry.getId() + "] — placing SL + Target");
                    }

                    holder.ocoRetries++;
                    OrderDTO slOrder     = orderService.placeStopLoss(symbol, quantity, exitSide, slPrice);
                    OrderDTO targetOrder = orderService.placeTarget(symbol, quantity, exitSide, targetPrice);

                    boolean slOk  = slOrder     != null && slOrder.getId()     != null && !slOrder.getId().isEmpty();
                    boolean tgtOk = targetOrder != null && targetOrder.getId() != null && !targetOrder.getId().isEmpty();

                    if (slOk && tgtOk) {
                        eventService.log("[SUCCESS] SL order placed for " + symbol + " at " + orderService.roundToTick(slPrice, symbol) + " [ID: " + slOrder.getId() + "]");
                        eventService.log("[SUCCESS] Target order placed for " + symbol + " at " + orderService.roundToTick(targetPrice, symbol) + " [ID: " + targetOrder.getId() + "]");
                        monitorOCO(slOrder.getId(), targetOrder.getId(), symbol, quantity,
                            exitSide, slPrice, targetPrice, position, setup, holder.entryFillPrice);
                        if (holder.future != null) holder.future.cancel(false);
                    } else {
                        // Cancel whichever succeeded to avoid a half-OCO state
                        if (slOk)  orderService.cancelOrder(slOrder.getId());
                        if (tgtOk) orderService.cancelOrder(targetOrder.getId());

                        if (!slOk)  eventService.log("[ERROR] SL order placement failed for " + symbol + " at " + slPrice);
                        if (!tgtOk) eventService.log("[ERROR] Target order placement failed for " + symbol + " at " + targetPrice);

                        if (holder.ocoRetries >= Holder.MAX_OCO_RETRIES) {
                            eventService.log("[WARNING] Could not place SL/Target for " + symbol
                                + " after " + Holder.MAX_OCO_RETRIES + " attempts — scanning for manually placed orders");
                            scanForManualOCO(symbol, quantity, exitSide, position, setup, holder.entryFillPrice);
                            if (holder.future != null) holder.future.cancel(false);
                        } else {
                            eventService.log("[WARNING] Retrying SL/Target placement for " + symbol
                                + " (attempt " + holder.ocoRetries + "/" + Holder.MAX_OCO_RETRIES + ")");
                            // do not cancel — next tick will retry
                        }
                    }

                } else if ("5".equals(status)) {
                    pendingEntrySymbols.remove(symbol);
                    eventService.log("[ERROR] " + (position.equals("LONG") ? "BUY" : "SELL")
                        + " order rejected for " + symbol + ": " + entry.getMessage());
                    if (holder.future != null) holder.future.cancel(false);
                }
            } catch (Exception e) { e.printStackTrace(); }
        }, 0, 2, TimeUnit.SECONDS);
    }

    // ── SCAN FOR MANUALLY PLACED SL/TARGET ────────────────────────────────────
    // After auto-placement fails, polls the order book every 10s looking for
    // a pending SL-M (type 3) and Limit (type 1) order on the same symbol/side.
    // Once both are found, starts the OCO monitor automatically.
    private void scanForManualOCO(String symbol, int qty, int exitSide,
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
        positionStateStore.saveOcoState(symbol, slId, targetId, slPrice, targetPrice);
        eventService.log("[INFO] OCO monitor started for " + symbol + " — SL: " + slId + " | Target: " + targetId);

        s.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (s.handled) return;
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
                    double exitPrice  = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = entryFillPrice > 0 ? entryFillPrice
                                      : orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    double finalEntry = entryPrice > 0 ? entryPrice : s.currentSlPrice;
                    double finalExit  = exitPrice  > 0 ? exitPrice  : s.currentSlPrice;
                    double pnl = "LONG".equals(positionSide) ? (finalExit - finalEntry) * qty : (finalEntry - finalExit) * qty;
                    String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                    eventService.log("[SUCCESS] SL triggered for " + symbol + " at " + finalExit
                        + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)) + " — cancelling target");
                    tradeHistoryService.record(symbol, positionSide, qty, finalEntry, finalExit, "SL", setup);
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
                    double exitPrice  = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = entryFillPrice > 0 ? entryFillPrice
                                      : orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    double finalEntry = entryPrice > 0 ? entryPrice : s.currentTargetPrice;
                    double finalExit  = exitPrice  > 0 ? exitPrice  : s.currentTargetPrice;
                    double pnl = "LONG".equals(positionSide) ? (finalExit - finalEntry) * qty : (finalEntry - finalExit) * qty;
                    String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                    eventService.log("[SUCCESS] Target hit for " + symbol + " at " + finalExit
                        + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)) + " — cancelling SL");
                    tradeHistoryService.record(symbol, positionSide, qty, finalEntry, finalExit, "TARGET", setup);
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
                e.printStackTrace();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /** Clears all in-memory and persisted state for a single symbol. */
    private void clearSymbolState(String symbol) {
        positionStateStore.clear(symbol);
        cachedPositions.remove(symbol);
        setupBySymbol.remove(symbol);
        entryTimeBySymbol.remove(symbol);
        entryAvgBySymbol.remove(symbol);
        PositionManager.setPosition(symbol, "NONE");
    }

    // ── ORDER STATUS (cached order book) ────────────────────────────────────
    private volatile JsonNode cachedOrderBook = null;
    private volatile long     orderBookFetchTime = 0;
    private static final long ORDER_BOOK_CACHE_MS = 4000; // 4 seconds

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
            if (order.has("id") && order.get("id").asText().equals(orderId)) {
                return order;
            }
        }
        return null;
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
            orderService.cancelAllPendingOrders(symbol);

            OrderDTO exitOrder = orderService.placeExitOrder(symbol, quantity, exitSide);
            if (exitOrder == null || !"ok".equals(exitOrder.getStatus())) {
                eventService.log("[ERROR] " + label + " failed for " + symbol + " — exit order rejected");
                return false;
            }

            double entryPrice = entryAvgBySymbol.getOrDefault(symbol, 0.0);
            if (entryPrice <= 0) {
                int entrySide = exitSide == 1 ? -1 : 1;
                entryPrice = orderService.getFilledPriceFromTradebook(symbol, entrySide);
            }
            double exitPrice = orderService.getExitPriceFromTradebook(symbol, position);
            String setup = setupBySymbol.getOrDefault(symbol, "");
            if (entryPrice > 0 && exitPrice > 0) {
                double pnl = "LONG".equals(position) ? (exitPrice - entryPrice) * quantity : (entryPrice - exitPrice) * quantity;
                String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                eventService.log("[SUCCESS] " + label + " executed for " + symbol + " at " + exitPrice
                    + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
                tradeHistoryService.record(symbol, position, quantity, entryPrice, exitPrice, exitReason, setup);
            } else {
                eventService.log("[SUCCESS] " + label + " executed for " + symbol + " — position closed");
                tradeHistoryService.record(symbol, position, quantity, entryPrice, exitPrice > 0 ? exitPrice : 0, exitReason, setup);
            }
            ocoHandledSymbols.add(symbol);
            clearSymbolState(symbol);
            return true;

        } catch (Exception e) {
            e.printStackTrace();
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
                System.err.println("[PollingService] Auto square off check error: " + e.getMessage());
            }
        }, 10, 15, TimeUnit.SECONDS);
    }

    // ── TELEGRAM END-OF-DAY SUMMARY ────────────────────────────────────────
    private volatile LocalDate lastEodSummaryDate = null;

    private void startEodSummaryScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LocalTime now = LocalTime.now();
                LocalDate today = LocalDate.now();
                if (today.equals(lastEodSummaryDate)) return;
                if (now.isBefore(LocalTime.of(15, 30))) return;

                lastEodSummaryDate = today;
                List<com.rydytrader.autotrader.dto.TradeRecord> trades = tradeHistoryService.getTrades();
                if (trades.isEmpty()) {
                    telegramService.sendMessage("=== Day Summary ===\n\nNo trades today.");
                    return;
                }

                int wins = 0, losses = 0;
                double totalPnl = 0, totalCharges = 0, totalNetPnl = 0;
                for (com.rydytrader.autotrader.dto.TradeRecord t : trades) {
                    if (t.getNetPnl() >= 0) wins++; else losses++;
                    totalPnl += t.getPnl();
                    totalCharges += t.getCharges();
                    totalNetPnl += t.getNetPnl();
                }

                StringBuilder sb = new StringBuilder();
                sb.append("=== Day Summary ===\n\n");
                sb.append("Total Trades: ").append(trades.size()).append("\n");
                sb.append("Wins: ").append(wins).append(" | Losses: ").append(losses).append("\n");
                sb.append("Win Rate: ").append(trades.size() > 0 ? String.format("%.0f", (wins * 100.0 / trades.size())) : "0").append("%\n\n");

                for (com.rydytrader.autotrader.dto.TradeRecord t : trades) {
                    String tag = t.getNetPnl() >= 0 ? "+" : "-";
                    sb.append(t.getSymbol().replace("NSE:", ""))
                      .append(" | ").append(t.getSide())
                      .append(" | ").append(t.getExitReason())
                      .append(" | ").append(tag).append("₹").append(String.format("%.2f", Math.abs(t.getNetPnl())))
                      .append("\n");
                }

                sb.append("\nGross P&L: ₹").append(String.format("%.2f", totalPnl));
                sb.append("\nCharges: ₹").append(String.format("%.2f", totalCharges));
                sb.append("\nNet P&L: ₹").append(String.format("%.2f", totalNetPnl));

                telegramService.sendMessage(sb.toString());
            } catch (Exception e) {
                System.err.println("[PollingService] EOD summary error: " + e.getMessage());
            }
        }, 15, 15, TimeUnit.SECONDS);
    }

    // ── TELEGRAM SUMMARY SCHEDULER ─────────────────────────────────────────
    private volatile long lastTelegramSummaryTime = 0;

    private void startTelegramSummaryScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (cachedPositions.isEmpty()) return;
                int freqSeconds = riskSettings.getTelegramAlertFrequency();
                if (freqSeconds <= 0) return; // disabled

                long now = System.currentTimeMillis();
                if ((now - lastTelegramSummaryTime) < freqSeconds * 1000L) return;
                lastTelegramSummaryTime = now;

                StringBuilder sb = new StringBuilder();
                sb.append("--- Portfolio Update ---\n\n");
                double totalUnrealizedPnl = 0;
                for (PositionsDTO pos : cachedPositions.values()) {
                    String arrow = pos.getPnl() >= 0 ? "+" : "-";
                    sb.append(pos.getSymbol().replace("NSE:", ""))
                      .append(" | ").append(pos.getSide())
                      .append(" x").append(pos.getQty())
                      .append(" @ ").append(String.format("%.2f", pos.getAvgPrice()))
                      .append(" -> ").append(String.format("%.2f", pos.getLtp()))
                      .append(" | ").append(arrow).append("₹").append(String.format("%.2f", Math.abs(pos.getPnl())))
                      .append("\n");
                    totalUnrealizedPnl += pos.getPnl();
                }

                List<com.rydytrader.autotrader.dto.TradeRecord> trades = tradeHistoryService.getTrades();
                int wins = 0, losses = 0;
                double realizedPnl = 0;
                for (com.rydytrader.autotrader.dto.TradeRecord t : trades) {
                    if (t.getNetPnl() >= 0) wins++; else losses++;
                    realizedPnl += t.getNetPnl();
                }

                sb.append("\nOpen: ").append(cachedPositions.size())
                  .append(" | Unrealized: ₹").append(String.format("%.2f", totalUnrealizedPnl));
                if (!trades.isEmpty()) {
                    sb.append("\nWins: ").append(wins)
                      .append(" | Losses: ").append(losses)
                      .append(" | Realized: ₹").append(String.format("%.2f", realizedPnl));
                }
                sb.append("\nNet: ₹").append(String.format("%.2f", realizedPnl + totalUnrealizedPnl));

                telegramService.sendMessage(sb.toString());
            } catch (Exception e) {
                System.err.println("[PollingService] Telegram summary error: " + e.getMessage());
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    /** Squares off all open positions. */
    public void squareOffAll() {
        List<PositionsDTO> positions = fetchPositions();
        if (positions.isEmpty()) {
            eventService.log("[AUTO] No open positions to square off");
            return;
        }
        for (PositionsDTO pos : positions) {
            String symbol = pos.getSymbol();
            int qty = Math.abs(pos.getQty());
            if (qty == 0) continue;
            try {
                if (!modeStore.isLive()) {
                    // Simulator mode
                    Map<String, Object> mockPos = mockState.getPosition(symbol);
                    if (mockPos == null) continue;
                    double entryPrice = Double.parseDouble(mockPos.get("netAvgPrice").toString());
                    double exitPrice  = mockState.getCurrentPrice(symbol);
                    String side = pos.getSide();
                    String setup = getCurrentSetup(symbol);
                    orderService.cancelAllPendingOrders(symbol);
                    PositionManager.setPosition(symbol, "NONE");
                    mockState.triggerManualSquareOff(symbol);
                    ocoHandledSymbols.add(symbol);
                    double pnl = "LONG".equals(side) ? (exitPrice - entryPrice) * qty : (entryPrice - exitPrice) * qty;
                    String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
                    tradeHistoryService.record(symbol, side, qty, entryPrice, exitPrice, "AUTO_SQUAREOFF", setup);
                    eventService.log("[SUCCESS] Auto square off for " + symbol + " at " + exitPrice
                        + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
                    clearSymbolState(symbol);
                } else {
                    // Live mode
                    boolean ok = squareOff(symbol, qty, "AUTO_SQUAREOFF");
                    if (!ok) {
                        eventService.log("[ERROR] Auto square off failed for " + symbol);
                    }
                }
            } catch (Exception e) {
                eventService.log("[ERROR] Auto square off error for " + symbol + ": " + e.getMessage());
            }
        }
    }

    // ── POSITION SYNC ─────────────────────────────────────────────────────────
    private volatile boolean positionSyncStarted = false;

    public void startPositionSync() {
        if (positionSyncStarted) return;
        positionSyncStarted = true;
        scheduler.scheduleAtFixedRate(() -> {
            if (tokenStore.getAccessToken() == null) return;
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
                        if (ocoHandledSymbols.contains(symbol)) continue; // OCO just closed this — don't re-add
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

                        // Prefer bot's recorded fill price over Fyers' potentially blended netAvgPrice
                        double resolvedAvg = avg;
                        double inMemAvg = entryAvgBySymbol.getOrDefault(symbol, 0.0);
                        if (inMemAvg > 0) {
                            resolvedAvg = inMemAvg;
                        } else if (saved != null && saved.containsKey("avgPrice")) {
                            double savedAvg = Double.parseDouble(saved.get("avgPrice").toString());
                            if (savedAvg > 0) resolvedAvg = savedAvg;
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
                        positionStateStore.save(symbol, posSide, absQty, resolvedAvg, setup, resolvedEntryTime);
                        brokerSymbols.add(symbol);

                        // Auto-scan for SL/Target orders on newly detected positions without OCO monitor
                        if (isNewPosition && !ocoMonitoredSymbols.contains(symbol)) {
                            int exitSide = "LONG".equals(posSide) ? -1 : 1;
                            entryAvgBySymbol.put(symbol, resolvedAvg);
                            eventService.log("[INFO] New position detected for " + symbol + " — scanning for SL/Target orders");
                            scanForManualOCO(symbol, absQty, exitSide, posSide, setup, resolvedAvg);
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
                        orderService.cancelAllPendingOrders(symbol);
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
                            "MANUAL", setupBySymbol.getOrDefault(symbol, ""));
                        clearSymbolState(symbol);
                    } else if (prev != null) {
                        // position already NONE (e.g. simulator square-off), just remove from cache
                        cachedPositions.remove(symbol);
                        setupBySymbol.remove(symbol);
                        entryTimeBySymbol.remove(symbol);
                    }
                }
            }

            lastSyncTime = java.time.LocalTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            connectionStatus = "CONNECTED";

        } catch (Exception e) {
            connectionStatus = "DISCONNECTED";
            e.printStackTrace();
        }
    }

    // ── PUBLIC ACCESSORS ──────────────────────────────────────────────────────
    public void syncPositionOnce() { syncPosition(); }

    public List<PositionsDTO> fetchPositions() { return new ArrayList<>(cachedPositions.values()); }

    public String getConnectionStatus() { return connectionStatus; }

    public String getLastSyncTime() { return lastSyncTime; }

    /** Returns setup for a specific symbol. */
    public String getCurrentSetup(String symbol) {
        return setupBySymbol.getOrDefault(symbol, "");
    }

    /** Returns first setup found — backward-compat for single-symbol callers. */
    public String getCurrentSetup() {
        return setupBySymbol.isEmpty() ? "" : setupBySymbol.values().iterator().next();
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
