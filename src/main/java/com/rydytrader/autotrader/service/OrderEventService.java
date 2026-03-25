package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.ModeStore;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.websocket.FyersOrderWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Handles Fyers Order Update WebSocket events.
 *
 * Tracks entry, SL, and target order IDs. When a push event arrives
 * for a tracked order, executes the appropriate action (place OCO,
 * cancel counterpart, record trade, clear state).
 *
 * Replaces the polling loops in PollingService for fill/cancel detection
 * when the WebSocket is connected. PollingService falls back to polling
 * when the WebSocket is down.
 */
@Service
public class OrderEventService implements FyersOrderWebSocket.OrderCallback {

    private static final Logger log = LoggerFactory.getLogger(OrderEventService.class);

    private final TokenStore         tokenStore;
    private final ModeStore          modeStore;
    private final FyersProperties    fyersProperties;
    private final OrderService       orderService;
    private final EventService       eventService;
    private final TradeHistoryService tradeHistoryService;
    private final PositionStateStore positionStateStore;
    private final MarketDataService  marketDataService;
    private final TelegramService    telegramService;

    // WebSocket client
    private volatile FyersOrderWebSocket wsClient;
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile boolean connected = false;
    private volatile int reconnectAttempts = 0;
    private static final int MAX_RECONNECT = 10;

    // ── Order tracking ──────────────────────────────────────────────────────────
    // Entry orders: orderId → context for handling fill
    private final ConcurrentHashMap<String, EntryContext> trackedEntries = new ConcurrentHashMap<>();
    // OCO orders: orderId → context for handling SL/target fill
    private final ConcurrentHashMap<String, OcoContext> trackedOcoOrders = new ConcurrentHashMap<>();

    /** Context for a tracked entry order. */
    public static class EntryContext {
        public final String symbol;
        public final int quantity;
        public final String position; // LONG or SHORT
        public final int exitSide;
        public final double slPrice;
        public final double targetPrice;
        public final String setup;
        public volatile boolean handled = false;

        public EntryContext(String symbol, int quantity, String position,
                           int exitSide, double slPrice, double targetPrice, String setup) {
            this.symbol = symbol;
            this.quantity = quantity;
            this.position = position;
            this.exitSide = exitSide;
            this.slPrice = slPrice;
            this.targetPrice = targetPrice;
            this.setup = setup;
        }
    }

    /** Context for a tracked OCO (SL or Target) order. */
    public static class OcoContext {
        public final String symbol;
        public final int quantity;
        public final String positionSide; // LONG or SHORT
        public final int exitSide;
        public final String counterpartOrderId; // cancel this when fill detected
        public final String type; // "SL" or "TARGET"
        public final String setup;
        public final double entryFillPrice;
        public volatile boolean handled = false;
        public volatile double currentPrice; // tracks current SL stop price or target limit price

        public OcoContext(String symbol, int quantity, String positionSide, int exitSide,
                         String counterpartOrderId, String type, String setup, double entryFillPrice) {
            this.symbol = symbol;
            this.quantity = quantity;
            this.positionSide = positionSide;
            this.exitSide = exitSide;
            this.counterpartOrderId = counterpartOrderId;
            this.type = type;
            this.setup = setup;
            this.entryFillPrice = entryFillPrice;
        }
    }

    // Reference to PollingService for shared state access (set via setter to avoid circular DI)
    private volatile PollingService pollingService;

    public OrderEventService(TokenStore tokenStore, ModeStore modeStore,
                              FyersProperties fyersProperties, OrderService orderService,
                              EventService eventService, TradeHistoryService tradeHistoryService,
                              PositionStateStore positionStateStore, MarketDataService marketDataService,
                              TelegramService telegramService) {
        this.tokenStore = tokenStore;
        this.modeStore = modeStore;
        this.fyersProperties = fyersProperties;
        this.orderService = orderService;
        this.eventService = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.positionStateStore = positionStateStore;
        this.marketDataService = marketDataService;
        this.telegramService = telegramService;
    }

    /** Set PollingService reference (called from PollingService constructor to avoid circular DI). */
    public void setPollingService(PollingService pollingService) {
        this.pollingService = pollingService;
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ────────────────────────────────────────────────────────────────────────────

    /** Start the Order WebSocket. Called after login or on startup (LIVE mode only). */
    public synchronized void start() {
        if (running) stop();
        if (!modeStore.isLive()) return; // Only for LIVE mode
        running = true;
        reconnectAttempts = 0;
        scheduler = Executors.newScheduledThreadPool(2);
        connectWebSocketSync(); // blocking — so restore can use WS immediately after
    }

    /** Stop the Order WebSocket. Called on logout or mode switch. */
    public synchronized void stop() {
        running = false;
        connected = false;
        if (wsClient != null) {
            try { wsClient.closeBlocking(); } catch (Exception ignored) {}
            wsClient = null;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        trackedEntries.clear();
        trackedOcoOrders.clear();
        log.info("[OrderEventSvc] Stopped");
    }

    /** Synchronous connect — blocks until connected or fails. Used on startup. */
    private void connectWebSocketSync() {
        try {
            String accessToken = tokenStore.getAccessToken();
            if (accessToken == null || accessToken.isEmpty()) {
                log.info("[OrderEventSvc] No access token — skipping WS connect");
                return;
            }

            // Fyers Order WS expects "clientId:accessToken" format
            String authToken = fyersProperties.getClientId() + ":" + accessToken;
            log.info("[OrderEventSvc] Connecting to Order WS (sync)... clientId={}", fyersProperties.getClientId());
            wsClient = new FyersOrderWebSocket(authToken, this);
            boolean ok = wsClient.connectBlocking(10, TimeUnit.SECONDS);
            if (ok) {
                log.info("[OrderEventSvc] Sync connect SUCCESS — isOpen={}", wsClient.isOpen());
                // Ping every 10s
                scheduler.scheduleAtFixedRate(() -> {
                    if (wsClient != null && wsClient.isOpen()) wsClient.sendPing();
                }, 10, 10, TimeUnit.SECONDS);
            } else {
                log.info("[OrderEventSvc] Sync connect FAILED — timed out after 10s");
                scheduleReconnect();
            }
        } catch (Exception e) {
            log.error("[OrderEventSvc] Sync connect error", e);
            scheduleReconnect();
        }
    }

    /** Async connect — used for reconnects. */
    private void connectWebSocket() {
        if (!running) return;
        scheduler.submit(() -> {
            try {
                String accessToken = tokenStore.getAccessToken();
                if (accessToken == null || accessToken.isEmpty()) return;

                // Fyers Order WS expects "clientId:accessToken" format
                String authToken = fyersProperties.getClientId() + ":" + accessToken;
                wsClient = new FyersOrderWebSocket(authToken, this);
                wsClient.connectBlocking();

                // Ping every 10s
                scheduler.scheduleAtFixedRate(() -> {
                    if (wsClient != null && wsClient.isOpen()) wsClient.sendPing();
                }, 10, 10, TimeUnit.SECONDS);

            } catch (Exception e) {
                log.error("[OrderEventSvc] Connect error: {}", e.getMessage());
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (!running || reconnectAttempts >= MAX_RECONNECT) {
            log.info("[OrderEventSvc] Max reconnect attempts, falling back to polling");
            connected = false;
            return;
        }
        reconnectAttempts++;
        long delay = Math.min(2L * (1L << reconnectAttempts), 30);
        log.info("[OrderEventSvc] Reconnecting in {}s (attempt {})", delay, reconnectAttempts);
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.schedule(this::connectWebSocket, delay, TimeUnit.SECONDS);
        }
    }

    /** Is the Order WebSocket connected? Used by PollingService to decide polling vs push. */
    public boolean isConnected() { return connected && wsClient != null && wsClient.isOpen(); }

    // ────────────────────────────────────────────────────────────────────────────
    // Order tracking — called by PollingService after placing orders
    // ────────────────────────────────────────────────────────────────────────────

    /** Register an entry order for WebSocket tracking. Returns true if WS is connected. */
    public boolean trackEntryOrder(String orderId, EntryContext ctx) {
        if (!connected || orderId == null || orderId.isEmpty()) return false;
        trackedEntries.put(orderId, ctx);
        log.info("[OrderEventSvc] Tracking entry order {} for {}", orderId, ctx.symbol);
        return true;
    }

    /** Register SL + Target orders for WebSocket tracking. Returns true if WS is connected. */
    public boolean trackOcoOrders(String slOrderId, String targetOrderId,
                                   String symbol, int qty, String positionSide, int exitSide,
                                   String setup, double entryFillPrice) {
        return trackOcoOrders(slOrderId, targetOrderId, symbol, qty, positionSide, exitSide, setup, entryFillPrice, 0, 0);
    }

    public boolean trackOcoOrders(String slOrderId, String targetOrderId,
                                   String symbol, int qty, String positionSide, int exitSide,
                                   String setup, double entryFillPrice,
                                   double slPrice, double targetPrice) {
        if (!connected) return false;
        OcoContext slCtx = new OcoContext(symbol, qty, positionSide, exitSide, targetOrderId, "SL", setup, entryFillPrice);
        slCtx.currentPrice = slPrice;
        OcoContext tgtCtx = new OcoContext(symbol, qty, positionSide, exitSide, slOrderId, "TARGET", setup, entryFillPrice);
        tgtCtx.currentPrice = targetPrice;
        trackedOcoOrders.put(slOrderId, slCtx);
        trackedOcoOrders.put(targetOrderId, tgtCtx);
        log.info("[OrderEventSvc] Tracking OCO for {} — SL: {} ({}) | Target: {} ({})", symbol, slOrderId, slPrice, targetOrderId, targetPrice);
        return true;
    }

    /** Untrack an order (e.g., on cancellation or squareoff). */
    public void untrack(String orderId) {
        trackedEntries.remove(orderId);
        trackedOcoOrders.remove(orderId);
    }

    /** Untrack all orders for a symbol. */
    public void untrackSymbol(String symbol) {
        trackedEntries.entrySet().removeIf(e -> symbol.equals(e.getValue().symbol));
        trackedOcoOrders.entrySet().removeIf(e -> symbol.equals(e.getValue().symbol));
    }

    // ────────────────────────────────────────────────────────────────────────────
    // WebSocket callbacks
    // ────────────────────────────────────────────────────────────────────────────

    @Override
    public void onWsConnected() {
        connected = true;
        reconnectAttempts = 0;
        log.info("[OrderEventSvc] Order WebSocket connected");
        eventService.log("[INFO] Order Update WebSocket connected — real-time fill detection active");
    }

    @Override
    public void onWsDisconnected(String reason) {
        connected = false;
        log.info("[OrderEventSvc] Order WebSocket disconnected: {}", reason);
        if (running) {
            eventService.log("[WARNING] Order Update WebSocket disconnected — falling back to polling");
            scheduleReconnect();
        }
    }

    @Override
    public void onOrderEvent(JsonNode order) {
        try {
            // Fyers internal field names (before mapping)
            String orderId = order.has("id") ? order.get("id").asText() : "";
            int status = order.has("org_ord_status") ? order.get("org_ord_status").asInt() : 0;
            String symbol = order.has("symbol") ? order.get("symbol").asText() : "";
            String tag = order.has("ordertag") ? order.get("ordertag").asText() : "";
            double tradedPrice = order.has("price_traded") ? order.get("price_traded").asDouble() : 0;
            double stopPrice = order.has("price_stop") ? order.get("price_stop").asDouble() : 0;
            double limitPrice = order.has("price_limit") ? order.get("price_limit").asDouble() : 0;
            String message = order.has("oms_msg") ? order.get("oms_msg").asText() : "";

            // Status: 2=Filled, 1=Cancelled, 5=Rejected, 6=Pending
            if (status == 2) {
                handleFill(orderId, symbol, tradedPrice, tag);
            } else if (status == 5) {
                handleRejection(orderId, symbol, tag, message);
            } else if (status == 1) {
                handleCancellation(orderId, symbol, tag);
            } else if (status == 6) {
                // Pending order update — detect manual SL/Target price modifications
                handleModification(orderId, symbol, stopPrice, limitPrice);
            }

        } catch (Exception e) {
            log.error("[OrderEventSvc] Error handling order event", e);
        }
    }

    @Override
    public void onTradeEvent(JsonNode trade) {
        // Trade events give exact fill prices — log for diagnostics
        try {
            String symbol = trade.has("symbol") ? trade.get("symbol").asText() : "";
            double price = trade.has("price_traded") ? trade.get("price_traded").asDouble() : 0;
            String tag = trade.has("ordertag") ? trade.get("ordertag").asText() : "";
            log.info("[OrderEventSvc] Trade fill: {} @ {} tag={}", symbol, price, tag);
        } catch (Exception e) {
            // Non-critical
        }
    }

    @Override
    public void onPositionEvent(JsonNode position) {
        try {
            String symbol = position.has("symbol") ? position.get("symbol").asText() : "";
            int netQty = position.has("net_qty") ? position.get("net_qty").asInt() : 0;
            double netAvg = position.has("net_avg") ? position.get("net_avg").asDouble() : 0;
            log.info("[OrderEventSvc] Position update: {} netQty={} netAvg={}", symbol, netQty, netAvg);

            if (symbol.isEmpty()) return;

            String currentPos = PositionManager.getPosition(symbol);
            boolean isTracked = !"NONE".equals(currentPos);

            if (netQty != 0 && !isTracked) {
                // ── New position detected (manual trade on Fyers) ──
                handleNewManualPosition(symbol, netQty, netAvg);
            } else if (netQty == 0 && isTracked) {
                // ── Position closed externally ──
                handleExternalClose(symbol, currentPos);
            }
        } catch (Exception e) {
            log.error("[OrderEventSvc] Error handling position event", e);
        }
    }

    /** Handle a new position detected via WebSocket that wasn't opened by the bot. */
    private void handleNewManualPosition(String symbol, int netQty, double netAvg) {
        String posSide = netQty > 0 ? "LONG" : "SHORT";
        int absQty = Math.abs(netQty);
        int exitSide = "LONG".equals(posSide) ? -1 : 1;
        String entryTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

        // Prefer actual fill price from tradebook over Fyers' blended netAvgPrice
        // (netAvg is day-level average across all trades on same symbol, not this trade's price)
        int entrySide = "LONG".equals(posSide) ? 1 : -1;
        double entryPrice = orderService.getFilledPriceFromTradebook(symbol, entrySide);
        if (entryPrice <= 0) entryPrice = netAvg; // fallback to Fyers' blended avg

        PositionManager.setPosition(symbol, posSide);
        positionStateStore.save(symbol, posSide, absQty, entryPrice, "", entryTime, 0, 0);

        if (pollingService != null) {
            pollingService.setSymbolState(symbol, "", entryTime, entryPrice);
            pollingService.addCachedPosition(symbol, absQty, posSide, entryPrice, "", entryTime);
            pollingService.updateLastSyncTime();
        }

        marketDataService.updateSubscriptions();

        eventService.log("[WS] New manual position detected: " + posSide + " " + absQty
            + " " + symbol + " @ " + entryPrice
            + (entryPrice != netAvg ? " (Fyers avg: " + netAvg + ")" : "")
            + " — scanning for SL/Target orders");

        // Scan for manually placed SL/Target orders and track via WebSocket
        if (pollingService != null) {
            pollingService.scanForManualOCO(symbol, absQty, exitSide, posSide, "", netAvg);
        }
    }

    /** Handle an externally closed position (manual close on Fyers terminal). */
    private void handleExternalClose(String symbol, String positionSide) {
        // Check if OCO orders are being tracked — if so, the fill handler will take care of it
        boolean hasTrackedOco = trackedOcoOrders.values().stream()
            .anyMatch(ctx -> symbol.equals(ctx.symbol) && !ctx.handled);
        if (hasTrackedOco) {
            log.info("[OrderEventSvc] Position closed for {} but OCO orders still tracked — letting fill handler process", symbol);
            return;
        }

        // External close — record trade and clear state
        double entryPrice = pollingService != null ? pollingService.getEntryAvg(symbol) : 0;
        Map<String, Object> state = positionStateStore.load(symbol);
        if (entryPrice <= 0 && state != null && state.containsKey("avgPrice")) {
            entryPrice = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString());
        }
        int qty = state != null ? Integer.parseInt(state.getOrDefault("qty", "0").toString()) : 0;

        // Cancel any tracked SL/Target orders
        if (state != null) {
            String slOid = state.getOrDefault("slOrderId", "").toString();
            String tgtOid = state.getOrDefault("targetOrderId", "").toString();
            if (!slOid.isEmpty()) { orderService.cancelOrder(slOid); untrack(slOid); }
            if (!tgtOid.isEmpty()) { orderService.cancelOrder(tgtOid); untrack(tgtOid); }
        }

        // Try to get exit price from tradebook
        double exitPrice = 0;
        try {
            int exitSide = "LONG".equals(positionSide) ? -1 : 1;
            exitPrice = orderService.getFilledPriceFromTradebook(symbol, exitSide);
        } catch (Exception ignored) {}

        if (qty > 0 && entryPrice > 0) {
            double finalExit = exitPrice > 0 ? exitPrice : entryPrice; // fallback
            double pnl = "LONG".equals(positionSide)
                ? (finalExit - entryPrice) * qty
                : (entryPrice - finalExit) * qty;
            String setup = pollingService != null ? pollingService.getCurrentSetup(symbol) : "";

            eventService.log("[WS] External close detected for " + symbol + " at " + finalExit
                + " | P&L: " + (pnl >= 0 ? "+" : "") + String.format("%.2f", pnl));

            tradeHistoryService.record(symbol, positionSide, qty, entryPrice, finalExit, "MANUAL", setup);

            try {
                telegramService.sendMessage("[WS] Manual close for " + symbol
                    + " | " + positionSide + " | P&L: " + (pnl >= 0 ? "+" : "") + String.format("%.2f", pnl));
            } catch (Exception ignored) {}
        } else {
            eventService.log("[WS] External close detected for " + symbol + " — could not determine P&L");
        }

        if (pollingService != null) {
            pollingService.clearSymbolStateFromWs(symbol);
            pollingService.updateLastSyncTime();
        }
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Event handlers
    // ────────────────────────────────────────────────────────────────────────────

    private void handleFill(String orderId, String symbol, double tradedPrice, String tag) {
        // Check if this is a tracked entry order
        EntryContext entry = trackedEntries.remove(orderId);
        if (entry != null && !entry.handled) {
            entry.handled = true;
            handleEntryFill(entry, orderId, tradedPrice);
            return;
        }

        // Check if this is a tracked SL/Target order
        OcoContext oco = trackedOcoOrders.remove(orderId);
        if (oco != null && !oco.handled) {
            oco.handled = true;
            handleOcoFill(oco, orderId, tradedPrice);
            return;
        }

        // Untracked fill — log it
        log.info("[OrderEventSvc] Untracked order filled: {} {} @ {} tag={}", orderId, symbol, tradedPrice, tag);
    }

    private void handleEntryFill(EntryContext ctx, String orderId, double fillPrice) {
        String symbol = ctx.symbol;
        double entryPrice = fillPrice > 0 ? fillPrice : ctx.slPrice; // fallback

        String entryTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        PositionManager.setPosition(symbol, ctx.position);

        if (pollingService != null) {
            pollingService.setSymbolState(symbol, ctx.setup, entryTime, entryPrice);
            pollingService.addCachedPosition(symbol, ctx.quantity, ctx.position, entryPrice, ctx.setup, entryTime);
            pollingService.updateLastSyncTime();
        }

        positionStateStore.save(symbol, ctx.position, ctx.quantity, entryPrice,
            ctx.setup, entryTime, ctx.slPrice, ctx.targetPrice);

        eventService.log("[SUCCESS] [WS] " + (ctx.position.equals("LONG") ? "BUY" : "SELL")
            + " order filled for " + symbol + " @ " + entryPrice + " [ID: " + orderId + "] — placing SL + Target");

        marketDataService.updateSubscriptions();

        // Place SL + Target
        placeOcoOrders(ctx, entryPrice);
    }

    private void placeOcoOrders(EntryContext ctx, double entryFillPrice) {
        String symbol = ctx.symbol;
        int retries = 0;
        final int MAX_RETRIES = 3;

        while (retries < MAX_RETRIES) {
            retries++;
            OrderDTO slOrder = orderService.placeStopLoss(symbol, ctx.quantity, ctx.exitSide, ctx.slPrice);
            OrderDTO targetOrder = orderService.placeTarget(symbol, ctx.quantity, ctx.exitSide, ctx.targetPrice);

            boolean slOk = slOrder != null && slOrder.getId() != null && !slOrder.getId().isEmpty();
            boolean tgtOk = targetOrder != null && targetOrder.getId() != null && !targetOrder.getId().isEmpty();

            if (slOk && tgtOk) {
                eventService.log("[SUCCESS] [WS] SL placed for " + symbol + " at " + orderService.roundToTick(ctx.slPrice, symbol) + " [ID: " + slOrder.getId() + "]");
                eventService.log("[SUCCESS] [WS] Target placed for " + symbol + " at " + orderService.roundToTick(ctx.targetPrice, symbol) + " [ID: " + targetOrder.getId() + "]");
                positionStateStore.saveOcoState(symbol, slOrder.getId(), targetOrder.getId(), ctx.slPrice, ctx.targetPrice);

                // Track SL + Target for fill detection via WebSocket
                trackOcoOrders(slOrder.getId(), targetOrder.getId(),
                    symbol, ctx.quantity, ctx.position, ctx.exitSide, ctx.setup, entryFillPrice,
                    ctx.slPrice, ctx.targetPrice);
                return;
            }

            // Cancel whichever succeeded
            if (slOk) orderService.cancelOrder(slOrder.getId());
            if (tgtOk) orderService.cancelOrder(targetOrder.getId());

            if (!slOk) eventService.log("[ERROR] SL placement failed for " + symbol + " (attempt " + retries + ")");
            if (!tgtOk) eventService.log("[ERROR] Target placement failed for " + symbol + " (attempt " + retries + ")");

            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
        }

        // All retries failed — fallback to manual OCO scan
        eventService.log("[WARNING] Could not place SL/Target for " + symbol + " after " + MAX_RETRIES + " attempts via WS handler");
        if (pollingService != null) {
            pollingService.scanForManualOCO(symbol, ctx.quantity, ctx.exitSide,
                ctx.position, ctx.setup, entryFillPrice);
        }
    }

    private void handleOcoFill(OcoContext ctx, String orderId, double tradedPrice) {
        String symbol = ctx.symbol;

        // Remove counterpart from tracking
        OcoContext counterpart = trackedOcoOrders.remove(ctx.counterpartOrderId);

        // Cancel the counterpart order
        if (!orderService.cancelOrder(ctx.counterpartOrderId)) {
            eventService.log("[WARNING] Failed to cancel " + ("SL".equals(ctx.type) ? "target" : "SL")
                + " order " + ctx.counterpartOrderId + " for " + symbol);
        }

        double exitPrice = tradedPrice > 0 ? tradedPrice : 0;
        if (exitPrice <= 0) {
            exitPrice = orderService.getFilledPriceByOrderId(orderId);
        }
        double entryPrice = ctx.entryFillPrice > 0 ? ctx.entryFillPrice : 0;
        if (entryPrice <= 0 && pollingService != null) {
            entryPrice = pollingService.getEntryAvg(symbol);
        }

        double finalEntry = entryPrice > 0 ? entryPrice : exitPrice;
        double finalExit = exitPrice > 0 ? exitPrice : 0;
        double pnl = "LONG".equals(ctx.positionSide) ? (finalExit - finalEntry) * ctx.quantity
                                                      : (finalEntry - finalExit) * ctx.quantity;
        String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";

        eventService.log("[SUCCESS] [WS] " + ctx.type + " triggered for " + symbol + " at " + finalExit
            + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl))
            + " — cancelling " + ("SL".equals(ctx.type) ? "target" : "SL"));

        tradeHistoryService.record(symbol, ctx.positionSide, ctx.quantity, finalEntry, finalExit, ctx.type, ctx.setup);

        // Telegram notification
        try {
            String pnlSign = pnl >= 0 ? "+" : "";
            telegramService.sendMessage("[WS] " + ctx.type + " hit for " + symbol
                + " | " + ctx.positionSide + " | Entry: " + String.format("%.2f", finalEntry)
                + " → Exit: " + String.format("%.2f", finalExit)
                + " | P&L: " + pnlSign + String.format("%.2f", pnl));
        } catch (Exception ignored) {}

        // Clear all state
        if (pollingService != null) {
            pollingService.clearSymbolStateFromWs(symbol);
            pollingService.updateLastSyncTime();
        }
    }

    private void handleRejection(String orderId, String symbol, String tag, String message) {
        EntryContext entry = trackedEntries.remove(orderId);
        if (entry != null) {
            eventService.log("[WS] " + (entry.position.equals("LONG") ? "BUY" : "SELL")
                + " order rejected for " + symbol + ": " + message);
            return;
        }

        OcoContext oco = trackedOcoOrders.remove(orderId);
        if (oco != null) {
            eventService.log("[WS] " + oco.type + " order rejected for " + symbol + ": " + message + " — position may be UNPROTECTED");
        }
    }

    private void handleCancellation(String orderId, String symbol, String tag) {
        // Entry cancellation
        EntryContext entry = trackedEntries.remove(orderId);
        if (entry != null) {
            eventService.log("[WS] Entry order cancelled for " + symbol);
            return;
        }

        // OCO cancellation — log warning but don't auto-handle
        // (user may have manually cancelled one leg)
        OcoContext oco = trackedOcoOrders.get(orderId); // Don't remove — counterpart may still be active
        if (oco != null) {
            eventService.log("[WS] " + oco.type + " order cancelled for " + symbol + " — manual action");
            trackedOcoOrders.remove(orderId);
            // Check if counterpart is also cancelled
            OcoContext counter = trackedOcoOrders.get(oco.counterpartOrderId);
            if (counter == null) {
                // Both legs gone — position unprotected
                eventService.log("[WARNING] Both SL and Target cancelled for " + symbol + " — position unprotected");
            }
        }
    }

    /** Detect manual SL/Target price modifications on pending orders. */
    private void handleModification(String orderId, String symbol, double stopPrice, double limitPrice) {
        OcoContext oco = trackedOcoOrders.get(orderId);
        if (oco == null || oco.handled) return;

        if ("SL".equals(oco.type) && stopPrice > 0) {
            // SL order — check if stopPrice changed
            if (oco.currentPrice > 0 && Math.abs(stopPrice - oco.currentPrice) > 0.10) {
                eventService.log("[SUCCESS] [WS] SL modified for " + symbol + ": "
                    + String.format("%.2f", oco.currentPrice) + " → " + String.format("%.2f", stopPrice));
                oco.currentPrice = stopPrice;
                // Update disk state — find counterpart to get target price
                OcoContext counter = trackedOcoOrders.get(oco.counterpartOrderId);
                double targetPrice = counter != null ? counter.currentPrice : 0;
                positionStateStore.saveOcoState(symbol, orderId, oco.counterpartOrderId, stopPrice, targetPrice);
            } else if (oco.currentPrice <= 0) {
                oco.currentPrice = stopPrice; // initial sync
            }
        } else if ("TARGET".equals(oco.type) && limitPrice > 0) {
            // Target order — check if limitPrice changed
            if (oco.currentPrice > 0 && Math.abs(limitPrice - oco.currentPrice) > 0.10) {
                eventService.log("[SUCCESS] [WS] Target modified for " + symbol + ": "
                    + String.format("%.2f", oco.currentPrice) + " → " + String.format("%.2f", limitPrice));
                oco.currentPrice = limitPrice;
                // Update disk state — find counterpart to get SL price
                OcoContext counter = trackedOcoOrders.get(oco.counterpartOrderId);
                double slPrice = counter != null ? counter.currentPrice : 0;
                positionStateStore.saveOcoState(symbol, oco.counterpartOrderId, orderId, slPrice, limitPrice);
            } else if (oco.currentPrice <= 0) {
                oco.currentPrice = limitPrice; // initial sync
            }
        }
    }
}
