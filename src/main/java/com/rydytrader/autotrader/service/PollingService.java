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
import com.rydytrader.autotrader.store.TokenStore;
import org.springframework.stereotype.Service;

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

    // ── per-symbol state ──────────────────────────────────────────────────────
    private final ConcurrentHashMap<String, PositionsDTO> cachedPositions  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String>       setupBySymbol    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String>       entryTimeBySymbol  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double>       entryAvgBySymbol   = new ConcurrentHashMap<>();
    private final Set<String> ocoHandledSymbols = ConcurrentHashMap.newKeySet();

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
                          ModeStore modeStore) {
        this.tokenStore          = tokenStore;
        this.fyersProperties     = fyersProperties;
        this.fyersClient         = fyersClient;
        this.orderService        = orderService;
        this.eventService        = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.positionStateStore  = positionStateStore;
        this.mockState           = mockState;
        this.modeStore           = modeStore;
        restoreStateOnStartup();
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
                PositionManager.setPosition(symbol, side);
                cachedPositions.put(symbol, new PositionsDTO(symbol, qty, side, avgPrice, avgPrice, 0.0, setup, entryTime));

                if (modeStore.getMode() == ModeStore.Mode.SIMULATOR) {
                    mockState.restorePosition(symbol, side, qty, avgPrice);
                }
                System.out.println("[PollingService] Restored state on startup: " + side + " " + symbol);
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
                        positionStateStore.save(symbol, position, quantity, holder.entryFillPrice,
                            setupBySymbol.get(symbol), entryTime);
                        eventService.log("[SUCCESS] " + (position.equals("LONG") ? "BUY" : "SELL")
                            + " order filled for " + symbol + " @ " + holder.entryFillPrice + " — placing SL + Target");
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
                                + " after " + Holder.MAX_OCO_RETRIES + " attempts — position is UNPROTECTED");
                            if (holder.future != null) holder.future.cancel(false);
                        } else {
                            eventService.log("[WARNING] Retrying SL/Target placement for " + symbol
                                + " (attempt " + holder.ocoRetries + "/" + Holder.MAX_OCO_RETRIES + ")");
                            // do not cancel — next tick will retry
                        }
                    }

                } else if ("5".equals(status)) {
                    eventService.log("[ERROR] " + (position.equals("LONG") ? "BUY" : "SELL")
                        + " order rejected for " + symbol + ": " + entry.getMessage());
                    if (holder.future != null) holder.future.cancel(false);
                }
            } catch (Exception e) { e.printStackTrace(); }
        }, 0, 2, TimeUnit.SECONDS);
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

        s.future = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (s.handled) return;
                s.ticks++;
                if (s.ticks > 4680) {
                    eventService.log("[WARNING] OCO monitor timeout for " + symbol + " after 6.5 hours");
                    if (s.future != null) s.future.cancel(false);
                    return;
                }

                JsonNode slNode     = getOrderNode(slId);
                JsonNode targetNode = getOrderNode(targetId);

                String slStatus     = slNode != null ? slNode.get("status").asText() : null;
                String targetStatus = targetNode != null ? targetNode.get("status").asText() : null;

                boolean slFilled     = "2".equals(slStatus);
                boolean targetFilled = "2".equals(targetStatus);
                boolean slCancelled  = "1".equals(slStatus);   // Fyers: 1 = cancelled
                boolean tgtCancelled = "1".equals(targetStatus);
                boolean slRejected   = "5".equals(slStatus);   // Fyers: 5 = rejected
                boolean tgtRejected  = "5".equals(targetStatus);

                // ── Detect manual SL/Target price modifications ──
                if (slNode != null && "6".equals(slStatus) && slNode.has("stopPrice")) {
                    double liveSlPrice = slNode.get("stopPrice").asDouble();
                    if (liveSlPrice > 0 && liveSlPrice != s.currentSlPrice) {
                        eventService.log("[SUCCESS] SL modified for " + symbol + ": " + s.currentSlPrice + " → " + liveSlPrice);
                        s.currentSlPrice = liveSlPrice;
                    }
                }
                if (targetNode != null && "6".equals(targetStatus) && targetNode.has("limitPrice")) {
                    double liveTargetPrice = targetNode.get("limitPrice").asDouble();
                    if (liveTargetPrice > 0 && liveTargetPrice != s.currentTargetPrice) {
                        eventService.log("[SUCCESS] Target modified for " + symbol + ": " + s.currentTargetPrice + " → " + liveTargetPrice);
                        s.currentTargetPrice = liveTargetPrice;
                    }
                }

                // SL hit
                if (slFilled && !s.handled) {
                    s.handled = true;
                    ocoHandledSymbols.add(symbol);
                    eventService.log("[SUCCESS] SL triggered for " + symbol + " at " + s.currentSlPrice + " — cancelling target");
                    orderService.cancelOrder(targetId);
                    double exitPrice  = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = entryFillPrice > 0 ? entryFillPrice
                                      : orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    tradeHistoryService.record(symbol, positionSide, qty,
                        entryPrice > 0 ? entryPrice : s.currentSlPrice,
                        exitPrice  > 0 ? exitPrice  : s.currentSlPrice, "SL", setup);
                    clearSymbolState(symbol);
                    if (s.future != null) s.future.cancel(false);
                }
                // Target hit
                else if (targetFilled && !s.handled) {
                    s.handled = true;
                    ocoHandledSymbols.add(symbol);
                    eventService.log("[SUCCESS] Target hit for " + symbol + " at " + s.currentTargetPrice + " — cancelling SL");
                    orderService.cancelOrder(slId);
                    double exitPrice  = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = entryFillPrice > 0 ? entryFillPrice
                                      : orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    tradeHistoryService.record(symbol, positionSide, qty,
                        entryPrice > 0 ? entryPrice : s.currentTargetPrice,
                        exitPrice  > 0 ? exitPrice  : s.currentTargetPrice, "TARGET", setup);
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
                    eventService.log("[WARNING] Both SL and Target cancelled for " + symbol + " — position unprotected");
                    if (s.future != null) s.future.cancel(false);
                }
                // SL or Target rejected by Fyers — position unprotected
                else if ((slRejected || tgtRejected) && !s.handled) {
                    s.handled = true;
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
                    if (s.future != null) s.future.cancel(false);
                }

            } catch (Exception e) { e.printStackTrace(); }
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

    // ── ORDER STATUS ──────────────────────────────────────────────────────────
    private String getOrderStatus(String orderId) {
        JsonNode node = getOrderNode(orderId);
        return node != null ? node.get("status").asText() : null;
    }

    /** Returns the full order node from orderBook for the given orderId, or null. */
    private JsonNode getOrderNode(String orderId) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getOrder(orderId, auth);
            for (JsonNode order : node.get("orderBook")) {
                if (order.get("id").asText().equals(orderId)) {
                    return order;
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
        return null;
    }

    // ── SQUARE OFF ────────────────────────────────────────────────────────────
    public boolean squareOff(String symbol, int quantity) {
        try {
            String position = PositionManager.getPosition(symbol);
            if ("NONE".equals(position)) return false;
            int exitSide = "LONG".equals(position) ? -1 : 1;

            eventService.log("[SUCCESS] Manual Square off for " + symbol + " — cancelling SL/Target orders");
            orderService.cancelAllPendingOrders(symbol);

            OrderDTO exitOrder = orderService.placeExitOrder(symbol, quantity, exitSide);
            if (exitOrder == null || !"ok".equals(exitOrder.getStatus())) {
                eventService.log("[ERROR] Manual Square off failed for " + symbol + " — exit order rejected");
                return false;
            }

            eventService.log("[SUCCESS] Manual Square off executed for " + symbol + " — position closed");
            ocoHandledSymbols.add(symbol);
            clearSymbolState(symbol);
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            eventService.log("[ERROR] Manual Square off error for " + symbol + ": " + e.getMessage());
            return false;
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
                        int absQty = Math.abs(qty);
                        // Compute unrealized P&L from our resolved avg, not Fyers' day-level pl
                        // which includes realized P&L from earlier closed trades on the same symbol
                        double unrealizedPl = "LONG".equals(posSide)
                            ? (ltp - resolvedAvg) * absQty
                            : (resolvedAvg - ltp) * absQty;
                        cachedPositions.put(symbol, new PositionsDTO(symbol, absQty, posSide, resolvedAvg, ltp, unrealizedPl, setup, resolvedEntryTime));
                        PositionManager.setPosition(symbol, posSide);
                        positionStateStore.save(symbol, posSide, absQty, resolvedAvg, setup, resolvedEntryTime);
                        brokerSymbols.add(symbol);
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
                    // Skip if OCO already handled this exit (SL/Target hit)
                    if (ocoHandledSymbols.remove(symbol)) {
                        cachedPositions.remove(symbol);
                        continue;
                    }
                    PositionsDTO prev = cachedPositions.get(symbol);
                    if (prev != null && !PositionManager.getPosition(symbol).equals("NONE")) {
                        eventService.log("[SUCCESS] Manual Square Off for " + symbol + " initiated from Fyers Broker ( User / Automatic )");
                        double exitPrice = orderService.getExitPriceFromTradebook(symbol, prev.getSide());
                        tradeHistoryService.record(symbol, prev.getSide(), Math.abs(prev.getQty()),
                            prev.getAvgPrice(), exitPrice > 0 ? exitPrice : prev.getLtp(),
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
    }
}
