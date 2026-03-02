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
    private volatile boolean justRestored = false;
    private volatile String lastSyncTime = "";
    private volatile String connectionStatus = "DISCONNECTED";
    private final PositionStateStore positionStateStore;
    private final MockState            mockState;
    private final ModeStore            modeStore;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    /**
     * Constructs the PollingService, injecting all dependencies and triggering
     * state restoration from the persisted position store on startup.
     */
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

    /**
     * Loads the last persisted position state from {@link PositionStateStore} and
     * restores in-memory state (cached positions, setup label, entry time, and
     * {@link PositionManager}) so the UI reflects the correct position immediately
     * after a restart. Also restores {@link MockState} when running in SIMULATOR mode.
     */
    private void restoreStateOnStartup() {
        java.util.Map<String, Object> saved = positionStateStore.load();
        if (saved == null) return;
        try {
            String symbol   = saved.get("symbol").toString();
            String side     = saved.get("side").toString();
            int    qty      = Integer.parseInt(saved.get("qty").toString());
            double avgPrice = Double.parseDouble(saved.get("avgPrice").toString());
            String setup    = saved.getOrDefault("setup", "").toString();
            String entryTime = saved.getOrDefault("entryTime", "").toString();
            // Restore in-memory state
            currentSetup     = setup;
            currentEntryTime = entryTime;
            PositionManager.setPosition(side);
            // Pre-populate cachedPositions so UI shows immediately
            cachedPositions = new java.util.ArrayList<>();
            cachedPositions.add(new PositionsDTO(symbol, qty, side, avgPrice, avgPrice, 0.0, setup, entryTime));
            // If simulator mode — restore MockState too
            if (modeStore.getMode() == ModeStore.Mode.SIMULATOR) {
                mockState.restorePosition(symbol, side, qty, avgPrice);
            }
            justRestored = true;
            System.out.println("[PollingService] Restored state on startup: " + side + " " + symbol);
        } catch (Exception e) {
            System.err.println("[PollingService] Failed to restore state: " + e.getMessage());
        }
    }

    // ── ENTRY MONITOR + OCO ───────────────────────────────────────────────────
    /**
     * Convenience overload of {@link #monitorEntryAndPlaceOCO(OrderDTO, String, int, String, int, double, double, String)}
     * that passes an empty setup label.
     */
    public void monitorEntryAndPlaceOCO(OrderDTO entry, String symbol,
                                        int quantity, String position,
                                        int exitSide, double slPrice, double targetPrice) {
        monitorEntryAndPlaceOCO(entry, symbol, quantity, position, exitSide, slPrice, targetPrice, "");
    }

    /**
     * Polls the entry order every 2 seconds until it is filled (status "2") or
     * rejected (status "5"). On fill, updates {@link PositionManager}, persists
     * position state, places SL and Target orders via {@link OrderService}, and
     * hands off to {@link #monitorOCO} for OCO management.
     *
     * @param entry       the entry order whose status will be polled
     * @param symbol      trading symbol (e.g. "NSE:NIFTY25FEB24000CE")
     * @param quantity    number of lots / quantity for the exit orders
     * @param position    "LONG" or "SHORT" — the resulting position side
     * @param exitSide    Fyers side code for the exit direction (1 = BUY, -1 = SELL)
     * @param slPrice     stop-loss limit price
     * @param targetPrice take-profit limit price
     * @param setup       optional label identifying the trade setup (e.g. "CPR Bounce")
     */
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
                    positionStateStore.save(symbol, position, quantity, 0.0, currentSetup, currentEntryTime);
                    eventService.log("[SUCCESS] " + (position.equals("LONG") ? "BUY" : "SELL") + " order filled — placing SL + Target");

                    OrderDTO slOrder     = orderService.placeStopLoss(symbol, quantity, exitSide, slPrice);
                    OrderDTO targetOrder = orderService.placeTarget(symbol, quantity, exitSide, targetPrice);

                    if (slOrder != null && slOrder.getId() != null)
                        eventService.log("[SUCCESS] SL order placed at " + slPrice + " [ID: " + slOrder.getId() + "]");
                    if (targetOrder != null && targetOrder.getId() != null)
                        eventService.log("[SUCCESS] Target order placed at " + targetPrice + " [ID: " + targetOrder.getId() + "]");

                    if (slOrder != null && targetOrder != null)
                        monitorOCO(slOrder.getId(), targetOrder.getId(), symbol, quantity, exitSide, slPrice, targetPrice, position, setup);

                    if (holder.future != null) holder.future.cancel(false);

                } else if ("5".equals(status)) {
                    eventService.log("[ERROR] " + (position.equals("LONG") ? "BUY" : "SELL") + " order rejected: " + entry.getMessage());
                    if (holder.future != null) holder.future.cancel(false);
                }
            } catch (Exception e) { e.printStackTrace(); }
        }, 0, 2, TimeUnit.SECONDS);
    }

    // ── OCO MONITOR ───────────────────────────────────────────────────────────
    /**
     * Polls both the SL and Target orders every 2 seconds (One-Cancels-Other logic).
     * When one side fills, the other is cancelled and the trade is recorded in
     * {@link TradeHistoryService}. Manual cancellations are detected and logged.
     * A 4-hour timeout stops the monitor if neither side fills. The monitor also
     * stops silently when the position is closed externally (e.g. simulator square-off).
     *
     * @param slId         order ID of the stop-loss order
     * @param targetId     order ID of the target order
     * @param symbol       trading symbol
     * @param qty          position quantity
     * @param exitSide     Fyers side code used for the exit orders
     * @param slPrice      stop-loss price (used as fallback exit price in trade record)
     * @param targetPrice  target price (used as fallback exit price in trade record)
     * @param positionSide "LONG" or "SHORT"
     * @param setup        trade setup label forwarded to the trade history record
     */
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
                    eventService.log("[SUCCESS] SL triggered at " + slPrice + " — cancelling target");
                    orderService.cancelOrder(targetId);
                    double exitPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    tradeHistoryService.record(symbol, positionSide, qty, entryPrice > 0 ? entryPrice : slPrice, exitPrice > 0 ? exitPrice : slPrice, "SL", setup);
                    positionStateStore.clear();
                    cachedPositions = new ArrayList<>();
                    PositionManager.setPosition("NONE");
                    if (s.future != null) s.future.cancel(false);
                }
                // Target hit
                else if (targetFilled && !s.handled) {
                    s.handled = true;
                    eventService.log("[SUCCESS] Target hit at " + targetPrice + " — cancelling SL");
                    orderService.cancelOrder(slId);
                    double exitPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide);
                    double entryPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide == 1 ? -1 : 1);
                    tradeHistoryService.record(symbol, positionSide, qty, entryPrice > 0 ? entryPrice : targetPrice, exitPrice > 0 ? exitPrice : targetPrice, "TARGET", setup);
                    positionStateStore.clear();
                    cachedPositions = new ArrayList<>();
                    PositionManager.setPosition("NONE");
                    if (s.future != null) s.future.cancel(false);
                }
                // SL manually cancelled — only log if position still open
                else if (slCancelled && !s.slManualCancelled && !PositionManager.getPosition().equals("NONE")) {
                    s.slManualCancelled = true;
                    if (!tgtCancelled) {
                        eventService.log("[WARNING] SL order manually cancelled — target still active");
                    } else {
                        eventService.log("[WARNING] SL order manually cancelled");
                    }
                }
                // Target manually cancelled — only log if position still open
                else if (tgtCancelled && !s.targetManualCancelled && !PositionManager.getPosition().equals("NONE")) {
                    s.targetManualCancelled = true;
                    if (!slCancelled) {
                        eventService.log("[WARNING] Target order manually cancelled — SL still active");
                    } else {
                        eventService.log("[WARNING] Target order manually cancelled");
                    }
                }
                // Both cancelled and position still open — unprotected warning
                else if (slCancelled && tgtCancelled && s.slManualCancelled && s.targetManualCancelled
                         && !PositionManager.getPosition().equals("NONE") && !s.handled) {
                    s.handled = true;
                    eventService.log("[WARNING] Both SL and Target cancelled — position unprotected");
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
    /**
     * Fetches the current status code of a single order from the Fyers order book.
     *
     * @param orderId the Fyers order ID to look up
     * @return the status string (e.g. "2" = filled, "5" = rejected/cancelled),
     *         or {@code null} if the order was not found or an error occurred
     */
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
    /** Returns the setup label associated with the current open position. */
    public String getCurrentSetup() { return currentSetup; }

    /** Clears the in-memory position cache (does not affect persisted state). */
    public void clearCachedPositions() { cachedPositions = new ArrayList<>(); }

    /** Returns the timestamp of the most recent successful position sync. */
    public String getLastSyncTime()     { return lastSyncTime; }

    /**
     * Manually squares off the current open position by cancelling all pending
     * SL/Target orders and placing a market exit order. Clears all in-memory and
     * persisted position state on success.
     *
     * @param symbol   trading symbol to square off
     * @param quantity quantity to exit
     * @return {@code true} if the exit order was accepted; {@code false} if there
     *         is no open position or the exit order was rejected
     */
    public boolean squareOff(String symbol, int quantity) {
        try {
            String position = PositionManager.getPosition();
            if ("NONE".equals(position)) return false;
            int exitSide = "LONG".equals(position) ? -1 : 1;

            // Cancel SL/Target orders first then place exit market order
            eventService.log("[SUCCESS] Manual Square off — cancelling SL/Target orders");
            orderService.cancelAllPendingOrders(symbol);

            OrderDTO exitOrder = orderService.placeExitOrder(symbol, quantity, exitSide);
            if (exitOrder == null || !"ok".equals(exitOrder.getStatus())) {
                eventService.log("[ERROR] Manual Square off failed — exit order rejected");
                return false;
            }

            eventService.log("[SUCCESS] Manual Square off executed — position closed");
            currentSetup = ""; currentEntryTime = "";
            positionStateStore.clear();
            cachedPositions = new ArrayList<>();
            PositionManager.setPosition("NONE");
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            eventService.log("[ERROR] Manual Square off error: " + e.getMessage());
            return false;
        }
    }

    private volatile boolean positionSyncStarted = false;

    /**
     * Starts a background scheduled task that calls {@link #syncPosition()} every
     * 10 seconds (after an initial 5-second delay). The sync is skipped when no
     * access token is available or when there is no active position and no persisted
     * state. Safe to call multiple times — only one sync loop is started.
     */
    public void startPositionSync() {
        if (positionSyncStarted) return;
        positionSyncStarted = true;
        scheduler.scheduleAtFixedRate(() -> {
            if (tokenStore.getAccessToken() == null) return;
            if (PositionManager.getPosition().equals("NONE") && positionStateStore.load() == null) return;
            syncPosition();
        }, 5, 10, TimeUnit.SECONDS);
    }

    /**
     * Fetches live net positions from the Fyers API and updates the in-memory
     * position cache, {@link PositionManager}, and the persisted state store.
     * Also detects when a position has been closed externally (manual square-off
     * via broker terminal) and records the resulting trade in {@link TradeHistoryService}.
     * Updates {@code connectionStatus} to CONNECTED or DISCONNECTED based on outcome.
     */
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
                        String resolvedEntryTime = !currentEntryTime.isEmpty() ? currentEntryTime : "";
                        if (resolvedEntryTime.isEmpty()) {
                            java.util.Map<String,Object> saved = positionStateStore.load();
                            if (saved != null && saved.containsKey("entryTime")) resolvedEntryTime = saved.get("entryTime").toString();
                        }
                        if (resolvedEntryTime.isEmpty()) resolvedEntryTime = eventService.getEntryTimeFromLogs();
                        if (currentEntryTime.isEmpty() && !resolvedEntryTime.isEmpty()) currentEntryTime = resolvedEntryTime;
                        updatedList.add(new PositionsDTO(
                            pos.get("symbol").asText(), qty, posSide, avg, ltp, pl, currentSetup, resolvedEntryTime
                        ));
                        PositionManager.setPosition(posSide);
                        positionStateStore.save(pos.get("symbol").asText(), posSide, qty, avg, currentSetup, currentEntryTime);
                    }
                }
            }

            // Detect manual square-off
            if (justRestored && updatedList.isEmpty()) { justRestored = false; }
            else if (updatedList.isEmpty() && !cachedPositions.isEmpty()
                    && !PositionManager.getPosition().equals("NONE")) {
                eventService.log("[INFO] Position closed externally — recording trade");
                PositionsDTO prev = cachedPositions.get(0);
                double exitPrice = orderService.getExitPriceFromTradebook(prev.getSymbol());
                tradeHistoryService.record(prev.getSymbol(), prev.getSide(), Math.abs(prev.getQty()),
                    prev.getAvgPrice(), exitPrice > 0 ? exitPrice : prev.getLtp(), "MANUAL", currentSetup);
                currentSetup = ""; currentEntryTime = "";
                positionStateStore.clear();
                PositionManager.setPosition("NONE");
            } else if (updatedList.isEmpty()) {
                currentSetup = ""; currentEntryTime = "";
                positionStateStore.clear();
                PositionManager.setPosition("NONE");
            }

            cachedPositions = updatedList;
            lastSyncTime = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            connectionStatus = "CONNECTED";

        } catch (Exception e) {
            connectionStatus = "DISCONNECTED";
            e.printStackTrace();
        }
    }

    /** Triggers a single immediate position sync outside the scheduled cycle. */
    public void syncPositionOnce()           { syncPosition(); }

    /** Returns the current in-memory list of open positions. */
    public List<PositionsDTO> fetchPositions(){ return cachedPositions; }

    /** Returns the current broker connection status: CONNECTED, SYNCING, or DISCONNECTED. */
    public String getConnectionStatus()       { return connectionStatus; }
}