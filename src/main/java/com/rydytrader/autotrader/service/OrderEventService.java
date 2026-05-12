package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.manager.PositionManager;
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
    private final FyersProperties    fyersProperties;
    private final OrderService       orderService;
    private final EventService       eventService;
    private final TradeHistoryService tradeHistoryService;
    private final PositionStateStore positionStateStore;
    private final MarketDataService  marketDataService;
    private final TelegramService    telegramService;
    private final com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;

    // WebSocket client
    private volatile FyersOrderWebSocket wsClient;
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile boolean connected = false;
    private volatile int reconnectAttempts = 0;
    private volatile String lastConnectTime = "";
    private volatile String lastDisconnectTime = "";
    private volatile int reconnectCountToday = 0;
    private static final int MAX_RECONNECT = 10;

    // ── Order tracking ──────────────────────────────────────────────────────────
    // Entry orders: orderId → context for handling fill
    private final ConcurrentHashMap<String, EntryContext> trackedEntries = new ConcurrentHashMap<>();
    // OCO orders: orderId → context for handling SL/target fill
    private final ConcurrentHashMap<String, OcoContext> trackedOcoOrders = new ConcurrentHashMap<>();
    // Recently handled symbols — prevents duplicate recording when position event arrives after OCO fill
    private final ConcurrentHashMap<String, Long> recentlyHandled = new ConcurrentHashMap<>();
    // Fills that arrived before trackEntryOrder was called (WS fill raced REST response).
    // Keyed by orderId → {price, tag, timestamp}. Replayed on trackEntryOrder; evicted after 10s.
    private final ConcurrentHashMap<String, PendingFill> pendingFills = new ConcurrentHashMap<>();
    private static final long PENDING_FILL_TTL_MS = 10_000;

    private static class PendingFill {
        final String symbol;
        final double price;
        final String tag;
        final long timestamp;
        PendingFill(String symbol, double price, String tag) {
            this.symbol = symbol;
            this.price = price;
            this.tag = tag;
            this.timestamp = System.currentTimeMillis();
        }
    }

    /** Context for a tracked entry order. */
    public static class EntryContext {
        public final String symbol;
        public final int quantity;
        public final String position; // LONG or SHORT
        public final int exitSide;
        public final double slPrice;  // initial SL from SignalProcessor (based on close)
        public final double targetPrice;
        public final String setup;
        public final double atr;
        public final double atrMultiplier;
        public final String description;
        public final boolean rescueShifted;
        public final boolean useStructuralSl;
        public final Double target1Price; // nullable — absolute T1 price when Target Rescue split the trade
        public volatile boolean handled = false;

        public EntryContext(String symbol, int quantity, String position,
                           int exitSide, double slPrice, double targetPrice, String setup) {
            this(symbol, quantity, position, exitSide, slPrice, targetPrice, setup, 0, 0, null, false, false, null);
        }

        public EntryContext(String symbol, int quantity, String position,
                           int exitSide, double slPrice, double targetPrice, String setup,
                           double atr, double atrMultiplier) {
            this(symbol, quantity, position, exitSide, slPrice, targetPrice, setup, atr, atrMultiplier, null, false, false, null);
        }

        public EntryContext(String symbol, int quantity, String position,
                           int exitSide, double slPrice, double targetPrice, String setup,
                           double atr, double atrMultiplier, String description) {
            this(symbol, quantity, position, exitSide, slPrice, targetPrice, setup, atr, atrMultiplier, description, false, false, null);
        }

        public EntryContext(String symbol, int quantity, String position,
                           int exitSide, double slPrice, double targetPrice, String setup,
                           double atr, double atrMultiplier, String description, boolean rescueShifted) {
            this(symbol, quantity, position, exitSide, slPrice, targetPrice, setup, atr, atrMultiplier, description, rescueShifted, false, null);
        }

        public EntryContext(String symbol, int quantity, String position,
                           int exitSide, double slPrice, double targetPrice, String setup,
                           double atr, double atrMultiplier, String description,
                           boolean rescueShifted, boolean useStructuralSl) {
            this(symbol, quantity, position, exitSide, slPrice, targetPrice, setup, atr, atrMultiplier, description, rescueShifted, useStructuralSl, null);
        }

        public EntryContext(String symbol, int quantity, String position,
                           int exitSide, double slPrice, double targetPrice, String setup,
                           double atr, double atrMultiplier, String description,
                           boolean rescueShifted, boolean useStructuralSl, Double target1Price) {
            this.symbol = symbol;
            this.quantity = quantity;
            this.position = position;
            this.exitSide = exitSide;
            this.slPrice = slPrice;
            this.targetPrice = targetPrice;
            this.setup = setup;
            this.atr = atr;
            this.atrMultiplier = atrMultiplier;
            this.description = description;
            this.rescueShifted = rescueShifted;
            this.useStructuralSl = useStructuralSl;
            this.target1Price = target1Price;
        }
    }

    /** Context for a tracked OCO (SL or Target) order. */
    public static class OcoContext {
        public final String symbol;
        public final int quantity;        // qty for THIS order (may be half for split targets)
        public final int totalQuantity;   // total entry qty (for reference)
        public final String positionSide; // LONG or SHORT
        public final int exitSide;
        public final String counterpartOrderId; // cancel this when fill detected (null for T1)
        public final String type; // "SL", "TARGET", "TARGET_1", "TARGET_2"
        public final String setup;
        public final double entryFillPrice;
        public volatile boolean handled = false;
        public volatile double currentPrice; // tracks current SL stop price or target limit price
        public volatile boolean trailed = false; // true if SL was moved by trailing logic
        // Split target: IDs of all related orders (for SL to cancel both T1+T2)
        public volatile String target1OrderId;
        public volatile String target2OrderId;
        public volatile String slOrderId;
        public volatile boolean t1Filled = false; // T1 already hit
        public volatile boolean slAtBreakeven = false; // post-T1 SL was moved to breakeven (vs. kept at original price)

        public OcoContext(String symbol, int quantity, String positionSide, int exitSide,
                         String counterpartOrderId, String type, String setup, double entryFillPrice) {
            this.symbol = symbol;
            this.quantity = quantity;
            this.totalQuantity = quantity;
            this.positionSide = positionSide;
            this.exitSide = exitSide;
            this.counterpartOrderId = counterpartOrderId;
            this.type = type;
            this.setup = setup;
            this.entryFillPrice = entryFillPrice;
        }

        public OcoContext(String symbol, int quantity, int totalQuantity, String positionSide, int exitSide,
                         String counterpartOrderId, String type, String setup, double entryFillPrice) {
            this.symbol = symbol;
            this.quantity = quantity;
            this.totalQuantity = totalQuantity;
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

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private BreakoutScanner breakoutScanner;

    public OrderEventService(TokenStore tokenStore,
                              FyersProperties fyersProperties, OrderService orderService,
                              EventService eventService, TradeHistoryService tradeHistoryService,
                              PositionStateStore positionStateStore, MarketDataService marketDataService,
                              TelegramService telegramService,
                              com.rydytrader.autotrader.store.RiskSettingsStore riskSettings) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.orderService = orderService;
        this.eventService = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.positionStateStore = positionStateStore;
        this.marketDataService = marketDataService;
        this.telegramService = telegramService;
        this.riskSettings = riskSettings;
    }

    /** Set PollingService reference (called from PollingService constructor to avoid circular DI). */
    public void setPollingService(PollingService pollingService) {
        this.pollingService = pollingService;
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ────────────────────────────────────────────────────────────────────────────

    /** Start the Order WebSocket. Called after login or on startup. */
    public synchronized void start() {
        if (running) stop();
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
                // Bounded connect — avoids scheduler-thread hangs if Fyers endpoint is briefly down.
                boolean connected = wsClient.connectBlocking(15, TimeUnit.SECONDS);
                if (!connected) {
                    log.warn("[OrderEventSvc] WS connectBlocking timed out after 15s — scheduling retry");
                    try { wsClient.close(); } catch (Exception ignored) {}
                    wsClient = null;
                    scheduleReconnect();
                    return;
                }

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
    public boolean isReconnecting() { return running && !isConnected() && reconnectAttempts > 0; }
    /** True when started post-login but WS not yet connected (initial connect OR retrying). */
    public boolean isConnecting() { return running && !isConnected(); }

    // ────────────────────────────────────────────────────────────────────────────
    // Order tracking — called by PollingService after placing orders
    // ────────────────────────────────────────────────────────────────────────────

    /** Register an entry order for WebSocket tracking. Returns true if WS is connected. */
    public boolean trackEntryOrder(String orderId, EntryContext ctx) {
        if (!connected || orderId == null || orderId.isEmpty()) return false;
        trackedEntries.put(orderId, ctx);
        log.info("[OrderEventSvc] Tracking entry order {} for {}", orderId, ctx.symbol);
        // Race recovery: WS fill event may have arrived before this tracking call
        // (ultra-fast market order on liquid stock where Fyers broadcasts fill
        // before REST order-place response returns). Replay the buffered fill.
        evictExpiredPendingFills();
        PendingFill buffered = pendingFills.remove(orderId);
        if (buffered != null) {
            log.info("[OrderEventSvc] Replaying buffered fill for {} — fill event beat REST response by {}ms",
                ctx.symbol, System.currentTimeMillis() - buffered.timestamp);
            eventService.log("[INFO] " + ctx.symbol + " — replaying buffered fill (WS beat REST response)");
            handleFill(orderId, buffered.symbol, buffered.price, buffered.tag);
        }
        return true;
    }

    private void evictExpiredPendingFills() {
        long now = System.currentTimeMillis();
        pendingFills.entrySet().removeIf(e -> now - e.getValue().timestamp > PENDING_FILL_TTL_MS);
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

    /** Register SL + T1 + T2 split target orders for WebSocket tracking. */
    public boolean trackSplitOcoOrders(String slOrderId, String target1OrderId, String target2OrderId,
                                       String symbol, int totalQty, int t1Qty, int t2Qty,
                                       String positionSide, int exitSide,
                                       String setup, double entryFillPrice,
                                       double slPrice, double target1Price, double target2Price) {
        if (!connected) return false;

        // SL context: cancels both T1 and T2 on fill
        OcoContext slCtx = new OcoContext(symbol, totalQty, totalQty, positionSide, exitSide, null, "SL", setup, entryFillPrice);
        slCtx.currentPrice = slPrice;
        slCtx.target1OrderId = target1OrderId;
        slCtx.target2OrderId = target2OrderId;
        slCtx.slOrderId = slOrderId;

        // T1 context: does NOT cancel anything on fill (T2 and SL stay alive, SL gets replaced)
        OcoContext t1Ctx = new OcoContext(symbol, t1Qty, totalQty, positionSide, exitSide, null, "TARGET_1", setup, entryFillPrice);
        t1Ctx.currentPrice = target1Price;
        t1Ctx.target1OrderId = target1OrderId;
        t1Ctx.target2OrderId = target2OrderId;
        t1Ctx.slOrderId = slOrderId;

        // T2 context: cancels SL on fill
        OcoContext t2Ctx = new OcoContext(symbol, t2Qty, totalQty, positionSide, exitSide, null, "TARGET_2", setup, entryFillPrice);
        t2Ctx.currentPrice = target2Price;
        t2Ctx.target1OrderId = target1OrderId;
        t2Ctx.target2OrderId = target2OrderId;
        t2Ctx.slOrderId = slOrderId;

        trackedOcoOrders.put(slOrderId, slCtx);
        trackedOcoOrders.put(target1OrderId, t1Ctx);
        trackedOcoOrders.put(target2OrderId, t2Ctx);
        log.info("[OrderEventSvc] Tracking split OCO for {} — SL: {} ({}) | T1: {} ({}) qty={} | T2: {} ({}) qty={}",
            symbol, slOrderId, slPrice, target1OrderId, target1Price, t1Qty, target2OrderId, target2Price, t2Qty);
        return true;
    }

    /** Untrack an order (e.g., on cancellation or squareoff). */
    public void untrack(String orderId) {
        trackedEntries.remove(orderId);
        trackedOcoOrders.remove(orderId);
    }

    /** Mark a tracked SL order as trailed (exit reason will be TRAILING_SL instead of SL). */
    public void markAsTrailed(String slOrderId) {
        OcoContext ctx = trackedOcoOrders.get(slOrderId);
        if (ctx != null) ctx.trailed = true;
    }

    /** Mark a symbol as recently handled (prevents WS position events from recording duplicate). */
    public void markRecentlyHandled(String symbol) {
        recentlyHandled.put(symbol, System.currentTimeMillis());
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
        lastConnectTime = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        log.info("[OrderEventSvc] Order WebSocket connected");
        eventService.log("[INFO] Order Update WebSocket connected — real-time fill detection active");
        // Hand off any polling OCO monitors to WebSocket tracking
        if (pollingService != null) {
            pollingService.handoffOcoToWebSocket();
        }
    }

    @Override
    public void onWsDisconnected(String reason) {
        connected = false;
        lastDisconnectTime = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        reconnectCountToday++;
        log.info("[OrderEventSvc] Order WebSocket disconnected: {}", reason);
        if (running) {
            eventService.log("[WARNING] Order Update WebSocket disconnected — falling back to polling");
            scheduleReconnect();
        }
    }

    // Monitoring accessors
    public String getLastConnectTime() { return lastConnectTime; }
    public String getLastDisconnectTime() { return lastDisconnectTime; }
    public int getReconnectCountToday() { return reconnectCountToday; }
    public int getTrackedOcoCount() { return (int) trackedOcoOrders.values().stream().filter(c -> !c.handled).count(); }

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
                // Skip if recently handled by entry fill (bot-placed order)
                Long entryHandledAt = recentlyHandled.get(symbol);
                if (entryHandledAt != null && (System.currentTimeMillis() - entryHandledAt) < 30_000) {
                    log.info("[OrderEventSvc] Position event for {} skipped — recently handled by entry fill", symbol);
                    return;
                }
                // Skip if entry order is currently being tracked (fill event hasn't arrived yet)
                boolean hasTrackedEntry = trackedEntries.values().stream()
                    .anyMatch(ctx -> symbol.equals(ctx.symbol) && !ctx.handled);
                if (hasTrackedEntry) {
                    log.info("[OrderEventSvc] Position event for {} skipped — entry order still tracked", symbol);
                    return;
                }
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
        // Defer processing by 5 seconds — gives time for order fill event to arrive first
        // (Fyers doesn't guarantee order of position vs order events)
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.schedule(() -> handleExternalCloseDeferred(symbol, positionSide), 5, TimeUnit.SECONDS);
        }
    }

    private void handleExternalCloseDeferred(String symbol, String positionSide) {
        // Check if position was already cleared by OCO fill handler
        if ("NONE".equals(PositionManager.getPosition(symbol))) {
            log.info("[OrderEventSvc] Position for {} already cleared — skipping external close", symbol);
            return;
        }

        // Check if this symbol was recently handled by OCO fill (within 30s)
        Long handledAt = recentlyHandled.get(symbol);
        if (handledAt != null && (System.currentTimeMillis() - handledAt) < 30_000) {
            log.info("[OrderEventSvc] Position event for {} skipped — recently handled by OCO fill", symbol);
            return;
        }

        // Check if OCO orders are being tracked — if so, the fill handler will take care of it
        boolean hasTrackedOco = trackedOcoOrders.values().stream()
            .anyMatch(ctx -> symbol.equals(ctx.symbol) && !ctx.handled);
        if (hasTrackedOco) {
            log.info("[OrderEventSvc] Position closed for {} but OCO orders still tracked — letting fill handler process", symbol);
            return;
        }

        // Check if this is a bot-managed position (has saved SL/target order IDs)
        // If so, the order fill event will handle it — not a manual close
        Map<String, Object> savedState = positionStateStore.load(symbol);
        if (savedState != null) {
            Object slId = savedState.get("slOrderId");
            Object tgtId = savedState.get("targetOrderId");
            if ((slId != null && !slId.toString().isEmpty()) || (tgtId != null && !tgtId.toString().isEmpty())) {
                log.info("[OrderEventSvc] Position for {} has saved OCO IDs — not a manual close, waiting for order event", symbol);
                // Re-defer once more to give order fill event time
                if (scheduler != null && !scheduler.isShutdown()) {
                    scheduler.schedule(() -> handleExternalCloseFinal(symbol, positionSide), 5, TimeUnit.SECONDS);
                }
                return;
            }
        }

        handleExternalCloseFinal(symbol, positionSide);
    }

    /** Final attempt — only records MANUAL if position is truly unhandled after all delays. */
    private void handleExternalCloseFinal(String symbol, String positionSide) {
        // Final check: position already cleared?
        if ("NONE".equals(PositionManager.getPosition(symbol))) {
            log.info("[OrderEventSvc] Position for {} already cleared (final check) — skipping", symbol);
            return;
        }
        // Final check: recently handled?
        Long handledAt = recentlyHandled.get(symbol);
        if (handledAt != null && (System.currentTimeMillis() - handledAt) < 30_000) {
            log.info("[OrderEventSvc] Position for {} skipped (final check) — recently handled", symbol);
            return;
        }

        // Check if OCO orders are still tracked — use their context for exit reason
        // (handles race where position event arrives before order fill event)
        OcoContext ocoCtx = trackedOcoOrders.values().stream()
            .filter(ctx -> symbol.equals(ctx.symbol) && !ctx.handled)
            .findFirst().orElse(null);

        // Genuinely external close — record trade and clear state
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

        // Determine exit reason: use OCO context if available (SL/TARGET/TRAILING_SL), else MANUAL
        String exitReason = "MANUAL";
        if (ocoCtx != null) {
            // Position closed while OCO was tracked — likely SL/target fill whose order event was delayed
            exitReason = ("SL".equals(ocoCtx.type) && ocoCtx.trailed) ? "TRAILING_SL" : ocoCtx.type;
            log.info("[OrderEventSvc] Using OCO context for exit reason: {} (trailed={})", exitReason, ocoCtx.trailed);
            ocoCtx.handled = true;
        }

        if (qty > 0 && entryPrice > 0) {
            double finalExit = exitPrice > 0 ? exitPrice : entryPrice; // fallback
            double pnl = "LONG".equals(positionSide)
                ? (finalExit - entryPrice) * qty
                : (entryPrice - finalExit) * qty;
            String setup = pollingService != null ? pollingService.getCurrentSetup(symbol) : "";

            eventService.log("[WS] " + (!"MANUAL".equals(exitReason) ? exitReason + " detected" : "External close detected")
                + " for " + symbol + " at " + finalExit
                + " | P&L: " + (pnl >= 0 ? "+" : "") + String.format("%.2f", pnl));

            positionStateStore.appendDescription(symbol,
                "[EXIT] " + exitReason + " @ " + String.format("%.2f", finalExit)
                + " | " + (pnl >= 0 ? "PROFIT" : "LOSS") + " ₹" + String.format("%.2f", Math.abs(pnl)));
            String desc = positionStateStore.getDescription(symbol);
            String prob = pollingService != null ? pollingService.getProbability(symbol) : "";
            tradeHistoryService.record(symbol, positionSide, qty, entryPrice, finalExit, exitReason, setup, desc, prob);

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
        log.info("[OrderEventSvc] handleFill called: orderId={} symbol={} price={} tag={} trackedOco={} trackedEntry={}",
            orderId, symbol, tradedPrice, tag, trackedOcoOrders.containsKey(orderId), trackedEntries.containsKey(orderId));

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

        // Untracked fill — buffer it. This handles the race where Fyers broadcasts
        // the fill event before the OrderService REST call returns and trackEntryOrder
        // gets called. trackEntryOrder replays the buffered fill when it registers.
        // Only buffer AutoTrader-tagged fills to avoid stashing unrelated manual orders.
        if (tag != null && tag.contains("AutoTrader")) {
            pendingFills.put(orderId, new PendingFill(symbol, tradedPrice, tag));
            log.info("[OrderEventSvc] Buffered untracked fill for replay: {} {} @ {} tag={}", orderId, symbol, tradedPrice, tag);
        } else {
            log.info("[OrderEventSvc] Untracked order filled: {} {} @ {} tag={}", orderId, symbol, tradedPrice, tag);
        }
    }

    @org.springframework.beans.factory.annotation.Autowired
    private LatencyTracker latencyTracker;

    private void handleEntryFill(EntryContext ctx, String orderId, double fillPrice) {
        String symbol = ctx.symbol;
        if (latencyTracker != null) latencyTracker.mark(symbol, ctx.setup, LatencyTracker.Stage.ORDER_FILLED);
        double entryPrice = fillPrice > 0 ? fillPrice : ctx.slPrice; // fallback

        // Mark as recently handled to prevent duplicate from position event
        recentlyHandled.put(symbol, System.currentTimeMillis());

        // Recalculate SL from actual fill price (always ATR-based at entry), unless structural.
        // Structural SL is anchored to the S/R level — keep as-is regardless of fill price.
        // Chandelier Exit only adjusts SL later on candle-close events, not at initial placement.
        double adjustedSl = ctx.slPrice;
        if (ctx.useStructuralSl) {
            adjustedSl = orderService.roundToTick(adjustedSl, symbol);
        } else if (entryPrice > 0 && ctx.atr > 0 && ctx.atrMultiplier > 0) {
            adjustedSl = "LONG".equals(ctx.position) ? entryPrice - ctx.atr * ctx.atrMultiplier : entryPrice + ctx.atr * ctx.atrMultiplier;
            adjustedSl = orderService.roundToTick(adjustedSl, symbol);
        }

        String entryTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        PositionManager.setPosition(symbol, ctx.position);

        if (pollingService != null) {
            pollingService.setSymbolState(symbol, ctx.setup, entryTime, entryPrice);
            pollingService.addCachedPosition(symbol, ctx.quantity, ctx.position, entryPrice, ctx.setup, entryTime);
            pollingService.updateLastSyncTime();
        }

        double roundedTarget = orderService.roundToTick(ctx.targetPrice, symbol);
        positionStateStore.save(symbol, ctx.position, ctx.quantity, entryPrice,
            ctx.setup, entryTime, adjustedSl, roundedTarget);

        // Persist the probability cached in PollingService onto the entity that was just
        // created. Without this, the prob column stays empty in the DB and the positions
        // table loses prob after a server restart.
        String pendingProb = pollingService.getProbability(symbol);
        if (pendingProb != null && !pendingProb.isEmpty()) {
            positionStateStore.saveProbability(symbol, pendingProb);
        }

        // NIFTY HTF Hurdle break-guard — if BreakoutScanner captured a guard for this symbol
        // when checkNiftyHurdle gated the trade, persist it onto the position record so the
        // NiftyHurdleExitService can defend the level on subsequent NIFTY 5-min closes.
        if (breakoutScanner != null) {
            BreakoutScanner.NiftyHurdleGuard guard = breakoutScanner.consumePendingHurdleGuard(symbol);
            if (guard != null) {
                positionStateStore.saveNiftyHurdleGuard(symbol, guard.low(), guard.high());
            }
        }

        eventService.log("[SUCCESS] [WS] " + (ctx.position.equals("LONG") ? "BUY" : "SELL")
            + " order filled for " + symbol + " @ " + entryPrice + " [ID: " + orderId + "] — placing SL + Target");

        // Append [FILL] to description
        if (ctx.description != null && !ctx.description.isEmpty()) {
            positionStateStore.appendDescription(symbol, ctx.description);
        }
        String ts = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        positionStateStore.appendDescription(symbol,
            ts + " [FILL] " + (ctx.position.equals("LONG") ? "BUY" : "SELL") + " filled @ "
            + String.format("%.2f", entryPrice) + ". SL recalculated → " + String.format("%.2f", adjustedSl) + ".");

        marketDataService.updateSubscriptions();

        // Place SL + Target (using adjusted SL based on actual fill price)
        placeOcoOrders(ctx, entryPrice, adjustedSl);
    }

    private void placeOcoOrders(EntryContext ctx, double entryFillPrice, double slPrice) {
        String symbol = ctx.symbol;
        int retries = 0;
        final int MAX_RETRIES = 3;
        // Fibonacci trailing always requires a target to compute range — no-target mode removed.
        boolean skipTarget = false;

        // Determine if we should split targets
        double targetDist = Math.abs(ctx.targetPrice - entryFillPrice);
        double atr = ctx.atr > 0 ? ctx.atr : 1; // safety
        // Target Rescue: SignalProcessor already produced an absolute T1 price → force a split
        // regardless of enableSplitTarget. Otherwise fall back to the normal toggle.
        boolean rescueSplit = ctx.target1Price != null && ctx.target1Price > 0;
        boolean shouldSplit = rescueSplit
            || (riskSettings.isEnableSplitTarget() && !skipTarget
                && ctx.quantity >= 4 // need at least 4 for even split
                && (riskSettings.getSplitMinDistanceAtr() <= 0 || targetDist > atr * riskSettings.getSplitMinDistanceAtr()));

        if (shouldSplit) {
            placeSplitOcoOrders(ctx, entryFillPrice, slPrice);
            return;
        }

        // Apply target tolerance: structural CPR target → discounted price slightly inside the level.
        // Always rounds AWAY from the level. Only used for the actual order; ctx.targetPrice (the
        // original structural value) is preserved for logging and downstream consumers.
        boolean isBuyForTol = "LONG".equals(ctx.position);
        double placedTargetPrice = orderService.applyTargetTolerance(ctx.targetPrice, isBuyForTol, ctx.atr, symbol);

        while (retries < MAX_RETRIES) {
            retries++;
            OrderDTO slOrder = orderService.placeStopLoss(symbol, ctx.quantity, ctx.exitSide, slPrice);

            OrderDTO targetOrder = null;
            if (!skipTarget) {
                targetOrder = orderService.placeTarget(symbol, ctx.quantity, ctx.exitSide, placedTargetPrice);
            }

            boolean slOk = slOrder != null && slOrder.getId() != null && !slOrder.getId().isEmpty();
            boolean tgtOk = skipTarget || (targetOrder != null && targetOrder.getId() != null && !targetOrder.getId().isEmpty());

            if (slOk && tgtOk) {
                double roundedSl = orderService.roundToTick(slPrice, symbol);
                String tgtId = skipTarget ? "" : targetOrder.getId();
                double tgtPrice = skipTarget ? 0 : placedTargetPrice;

                eventService.log("[SUCCESS] [WS] SL placed for " + symbol + " at " + roundedSl + " [ID: " + slOrder.getId() + "]");
                if (skipTarget) {
                    eventService.log("[INFO] [WS] No fixed target for " + symbol + " — trailing SL will close the trade");
                } else {
                    if (Math.abs(placedTargetPrice - ctx.targetPrice) > 0.001) {
                        eventService.log("[SUCCESS] [WS] Target placed for " + symbol + " at " + String.format("%.2f", placedTargetPrice)
                            + " (structural " + String.format("%.2f", ctx.targetPrice) + " - tolerance) [ID: " + tgtId + "]");
                    } else {
                        eventService.log("[SUCCESS] [WS] Target placed for " + symbol + " at " + String.format("%.2f", placedTargetPrice) + " [ID: " + tgtId + "]");
                    }
                    String ts2 = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                    positionStateStore.appendDescription(symbol,
                        ts2 + " [TGT_PLACED] @ " + String.format("%.2f", placedTargetPrice) + " [" + tgtId + "]");
                }
                positionStateStore.saveOcoState(symbol, slOrder.getId(), tgtId, slPrice, tgtPrice);
                String ts3 = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                positionStateStore.appendDescription(symbol,
                    ts3 + " [SL_PLACED] @ " + String.format("%.2f", roundedSl) + " [" + slOrder.getId() + "]");

                // Track SL + Target for fill detection via WebSocket
                trackOcoOrders(slOrder.getId(), tgtId,
                    symbol, ctx.quantity, ctx.position, ctx.exitSide, ctx.setup, entryFillPrice,
                    slPrice, tgtPrice);
                return;
            }

            // Cancel whichever succeeded
            if (slOk) orderService.cancelOrder(slOrder.getId());
            if (!skipTarget && targetOrder != null && targetOrder.getId() != null && !targetOrder.getId().isEmpty())
                orderService.cancelOrder(targetOrder.getId());

            if (!slOk) eventService.log("[ERROR] SL placement failed for " + symbol + " (attempt " + retries + ")");
            if (!skipTarget && !tgtOk) eventService.log("[ERROR] Target placement failed for " + symbol + " (attempt " + retries + ")");

            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
        }

        // All retries failed — square off immediately to avoid unprotected position
        eventService.log("[ERROR] Could not place SL/Target for " + symbol + " after " + MAX_RETRIES
            + " attempts — squaring off to avoid unprotected position");
        if (pollingService != null) {
            boolean closed = pollingService.squareOff(symbol, ctx.quantity, "SL_TARGET_FAILED");
            if (closed) {
                eventService.log("[SUCCESS] Emergency squareoff for " + symbol + " — unprotected position closed");
            } else {
                eventService.log("[ERROR] Emergency squareoff FAILED for " + symbol + " — POSITION UNPROTECTED, manual intervention needed");
            }
        }
    }

    /** Place split target orders: SL (full qty) + T1 (half qty) + T2 (half qty). */
    private void placeSplitOcoOrders(EntryContext ctx, double entryFillPrice, double slPrice) {
        String symbol = ctx.symbol;
        int totalQty = ctx.quantity;
        int t1Qty = (totalQty / 4) * 2; // half, rounded to even
        int t2Qty = totalQty - t1Qty;

        boolean isBuy = "LONG".equals(ctx.position);
        double t1Price;
        if (ctx.target1Price != null && ctx.target1Price > 0) {
            // Rescue-driven split: SignalProcessor provided absolute T1 (the original structural target).
            t1Price = orderService.roundToTick(ctx.target1Price, symbol);
        } else {
            // Normal split: T1 at percentage between entry and the structural target.
            double t1Pct = riskSettings.getT1DistancePct() / 100.0;
            t1Price = isBuy
                ? entryFillPrice + t1Pct * (ctx.targetPrice - entryFillPrice)
                : entryFillPrice - t1Pct * (entryFillPrice - ctx.targetPrice);
            t1Price = orderService.roundToTick(t1Price, symbol);
        }
        // T2 uses the DISCOUNTED target (tolerance applied) — T2 sits at the CPR level itself
        double t2Price = orderService.applyTargetTolerance(ctx.targetPrice, isBuy, ctx.atr, symbol);

        int retries = 0;
        final int MAX_RETRIES = 3;

        while (retries < MAX_RETRIES) {
            retries++;

            OrderDTO slOrder = orderService.placeStopLoss(symbol, totalQty, ctx.exitSide, slPrice);
            OrderDTO t1Order = orderService.placeTarget(symbol, t1Qty, ctx.exitSide, t1Price);
            OrderDTO t2Order = orderService.placeTarget(symbol, t2Qty, ctx.exitSide, t2Price);

            boolean slOk = slOrder != null && slOrder.getId() != null && !slOrder.getId().isEmpty();
            boolean t1Ok = t1Order != null && t1Order.getId() != null && !t1Order.getId().isEmpty();
            boolean t2Ok = t2Order != null && t2Order.getId() != null && !t2Order.getId().isEmpty();

            if (slOk && t1Ok && t2Ok) {
                double roundedSl = orderService.roundToTick(slPrice, symbol);
                String ts = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));

                eventService.log("[SUCCESS] [WS] Split targets for " + symbol
                    + " — SL: " + roundedSl + " [" + slOrder.getId() + "]"
                    + " | T1: " + t1Price + " qty=" + t1Qty + " [" + t1Order.getId() + "]"
                    + " | T2: " + t2Price + " qty=" + t2Qty + " [" + t2Order.getId() + "]");

                positionStateStore.appendDescription(symbol,
                    ts + " [SPLIT_TARGETS] T1 @ " + String.format("%.2f", t1Price) + " qty=" + t1Qty
                    + " | T2 @ " + String.format("%.2f", t2Price) + " qty=" + t2Qty);
                positionStateStore.appendDescription(symbol,
                    ts + " [SL_PLACED] @ " + String.format("%.2f", roundedSl) + " qty=" + totalQty + " [" + slOrder.getId() + "]");

                positionStateStore.saveSplitOcoState(symbol, slOrder.getId(),
                    t1Order.getId(), t2Order.getId(), slPrice, t1Price, t2Price);

                trackSplitOcoOrders(slOrder.getId(), t1Order.getId(), t2Order.getId(),
                    symbol, totalQty, t1Qty, t2Qty,
                    ctx.position, ctx.exitSide, ctx.setup, entryFillPrice,
                    slPrice, t1Price, t2Price);
                return;
            }

            // Cancel whichever succeeded
            if (slOk) orderService.cancelOrder(slOrder.getId());
            if (t1Ok) orderService.cancelOrder(t1Order.getId());
            if (t2Ok) orderService.cancelOrder(t2Order.getId());

            eventService.log("[ERROR] Split target placement failed for " + symbol + " (attempt " + retries
                + ") SL=" + slOk + " T1=" + t1Ok + " T2=" + t2Ok);

            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
        }

        eventService.log("[WARNING] Could not place split SL/T1/T2 for " + symbol + " — falling back to single target");
        // Fallback: place single target instead of failing completely
        placeOcoOrders(ctx, entryFillPrice, slPrice);
    }

    private void handleOcoFill(OcoContext ctx, String orderId, double tradedPrice) {
        // Route to split handler if this is a T1/T2 or split SL
        if ("TARGET_1".equals(ctx.type)) {
            handleT1Fill(ctx, orderId, tradedPrice);
            return;
        }
        if ("TARGET_2".equals(ctx.type)) {
            handleT2Fill(ctx, orderId, tradedPrice);
            return;
        }
        if ("SL".equals(ctx.type) && ctx.target1OrderId != null) {
            handleSplitSlFill(ctx, orderId, tradedPrice);
            return;
        }

        // ── Standard single-target OCO fill (unchanged) ──
        String symbol = ctx.symbol;
        recentlyHandled.put(symbol, System.currentTimeMillis());

        OcoContext counterpart = trackedOcoOrders.remove(ctx.counterpartOrderId);
        if (!orderService.cancelOrder(ctx.counterpartOrderId)) {
            eventService.log("[WARNING] Failed to cancel " + ("SL".equals(ctx.type) ? "target" : "SL")
                + " order " + ctx.counterpartOrderId + " for " + symbol);
        }

        double exitPrice = tradedPrice > 0 ? tradedPrice : 0;
        if (exitPrice <= 0) exitPrice = orderService.getFilledPriceByOrderId(orderId);
        double entryPrice = ctx.entryFillPrice > 0 ? ctx.entryFillPrice : 0;
        if (entryPrice <= 0 && pollingService != null) entryPrice = pollingService.getEntryAvg(symbol);

        double finalEntry = entryPrice > 0 ? entryPrice : exitPrice;
        double finalExit = exitPrice > 0 ? exitPrice : 0;
        double pnl = "LONG".equals(ctx.positionSide) ? (finalExit - finalEntry) * ctx.quantity
                                                      : (finalEntry - finalExit) * ctx.quantity;
        String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
        String exitReason = ("SL".equals(ctx.type) && ctx.trailed) ? "TRAILING_SL" : ctx.type;

        eventService.log("[SUCCESS] [WS] " + exitReason + " triggered for " + symbol + " at " + finalExit
            + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl))
            + " — cancelling " + ("SL".equals(ctx.type) ? "target" : "SL"));

        String tsExit = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        positionStateStore.appendDescription(symbol,
            tsExit + " [EXIT] " + exitReason + " @ " + String.format("%.2f", finalExit)
            + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
        String desc = positionStateStore.getDescription(symbol);

        String prob = pollingService != null ? pollingService.getProbability(symbol) : "";
        tradeHistoryService.record(symbol, ctx.positionSide, ctx.quantity, finalEntry, finalExit, exitReason, ctx.setup, desc, prob);

        if (pollingService != null) {
            pollingService.clearSymbolStateFromWs(symbol);
            pollingService.updateLastSyncTime();
        }
    }

    // ── Split target T1 fill: partial exit; SL is re-placed for remaining qty.
    // If trailing SL is enabled, the new SL is placed at breakeven (entry). Otherwise
    // the new SL stays at the original SL price — we just resize it to the remaining qty. ──
    private void handleT1Fill(OcoContext ctx, String orderId, double tradedPrice) {
        String symbol = ctx.symbol;
        recentlyHandled.put(symbol, System.currentTimeMillis());

        boolean moveToBreakeven = riskSettings.isEnableTrailingSl();
        log.info("[OrderEventSvc] T1 fill for {} — cancelling SL, placing {} SL",
            symbol, moveToBreakeven ? "breakeven" : "resized original-price");

        double exitPrice = tradedPrice > 0 ? tradedPrice : orderService.getFilledPriceByOrderId(orderId);
        double entryPrice = ctx.entryFillPrice > 0 ? ctx.entryFillPrice : 0;
        if (entryPrice <= 0 && pollingService != null) entryPrice = pollingService.getEntryAvg(symbol);
        double finalEntry = entryPrice > 0 ? entryPrice : exitPrice;
        double finalExit = exitPrice > 0 ? exitPrice : 0;

        double pnl = "LONG".equals(ctx.positionSide) ? (finalExit - finalEntry) * ctx.quantity
                                                      : (finalEntry - finalExit) * ctx.quantity;

        // Record partial trade for T1 qty
        String tsExit = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        positionStateStore.appendDescription(symbol,
            tsExit + " [T1_HIT] @ " + String.format("%.2f", finalExit)
            + " | qty=" + ctx.quantity + " | P&L ₹" + String.format("%.2f", pnl));

        String desc = positionStateStore.getDescription(symbol);
        String prob = pollingService != null ? pollingService.getProbability(symbol) : "";
        tradeHistoryService.record(symbol, ctx.positionSide, ctx.quantity, finalEntry, finalExit, "TARGET_1", ctx.setup, desc, prob);

        // Capture original SL price before removing from tracking so we can re-use it
        // when trailing is disabled.
        OcoContext oldSlCtx = trackedOcoOrders.get(ctx.slOrderId);
        double originalSlPrice = oldSlCtx != null ? oldSlCtx.currentPrice : 0;

        // Cancel existing SL (full qty)
        trackedOcoOrders.remove(ctx.slOrderId);
        orderService.cancelOrder(ctx.slOrderId);

        // Remove T1 from tracking
        trackedOcoOrders.remove(orderId);

        // Place new SL for remaining qty — at breakeven (if trailing) or at original SL price.
        int remainingQty = ctx.totalQuantity - ctx.quantity;
        double newSlPrice = moveToBreakeven
            ? orderService.roundToTick(finalEntry, symbol)
            : (originalSlPrice > 0 ? orderService.roundToTick(originalSlPrice, symbol) : orderService.roundToTick(finalEntry, symbol));
        int exitSide = ctx.exitSide;

        OrderDTO newSlOrder = orderService.placeStopLoss(symbol, remainingQty, exitSide, newSlPrice);
        String newSlId = (newSlOrder != null && newSlOrder.getId() != null) ? newSlOrder.getId() : "";

        if (!newSlId.isEmpty()) {
            String slLabel = moveToBreakeven ? "breakeven" : "original price";
            eventService.log("[SUCCESS] [WS] T1 hit for " + symbol + " — SL re-placed at " + slLabel + " "
                + String.format("%.2f", newSlPrice) + " for remaining qty=" + remainingQty + " [" + newSlId + "]");
            String descTag = moveToBreakeven ? "[SL_BREAKEVEN]" : "[SL_RESIZED]";
            positionStateStore.appendDescription(symbol,
                tsExit + " " + descTag + " New SL @ " + String.format("%.2f", newSlPrice) + " qty=" + remainingQty);

            // Update position state
            positionStateStore.saveT1FilledState(symbol, newSlId, newSlPrice, remainingQty);

            // Update in-memory position cache to reflect reduced qty
            if (pollingService != null) {
                pollingService.updateCachedPositionQty(symbol, remainingQty);
            }

            // Re-track: new SL + existing T2
            OcoContext t2Ctx = trackedOcoOrders.get(ctx.target2OrderId);
            OcoContext newSlCtx = new OcoContext(symbol, remainingQty, ctx.totalQuantity, ctx.positionSide, exitSide,
                null, "SL", ctx.setup, ctx.entryFillPrice);
            newSlCtx.currentPrice = newSlPrice;
            newSlCtx.target1OrderId = ctx.target1OrderId; // marker so SL fill routes to handleSplitSlFill
            newSlCtx.target2OrderId = ctx.target2OrderId;
            newSlCtx.slOrderId = newSlId;
            newSlCtx.t1Filled = true;
            newSlCtx.slAtBreakeven = moveToBreakeven;
            trackedOcoOrders.put(newSlId, newSlCtx);

            // Update T2 context with new SL order ID
            if (t2Ctx != null) {
                t2Ctx.slOrderId = newSlId;
                t2Ctx.t1Filled = true;
                t2Ctx.slAtBreakeven = moveToBreakeven;
            }
        } else {
            eventService.log("[ERROR] [WS] T1 hit for " + symbol + " but failed to place breakeven SL — position UNPROTECTED");
        }

        telegramService.notifyT1Hit(symbol, ctx.positionSide, ctx.quantity, finalExit, pnl, newSlPrice, remainingQty, moveToBreakeven);
    }

    // ── Split target T2 fill: final exit, cancel SL, clear position ──
    private void handleT2Fill(OcoContext ctx, String orderId, double tradedPrice) {
        String symbol = ctx.symbol;
        recentlyHandled.put(symbol, System.currentTimeMillis());

        double exitPrice = tradedPrice > 0 ? tradedPrice : orderService.getFilledPriceByOrderId(orderId);
        double entryPrice = ctx.entryFillPrice > 0 ? ctx.entryFillPrice : 0;
        if (entryPrice <= 0 && pollingService != null) entryPrice = pollingService.getEntryAvg(symbol);
        double finalEntry = entryPrice > 0 ? entryPrice : exitPrice;
        double finalExit = exitPrice > 0 ? exitPrice : 0;

        double pnl = "LONG".equals(ctx.positionSide) ? (finalExit - finalEntry) * ctx.quantity
                                                      : (finalEntry - finalExit) * ctx.quantity;

        // Cancel SL
        String slId = ctx.slOrderId;
        trackedOcoOrders.remove(slId);
        trackedOcoOrders.remove(orderId);
        orderService.cancelOrder(slId);

        String tsExit = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        positionStateStore.appendDescription(symbol,
            tsExit + " [T2_HIT] @ " + String.format("%.2f", finalExit)
            + " | qty=" + ctx.quantity + " | P&L ₹" + String.format("%.2f", pnl));
        String desc = positionStateStore.getDescription(symbol);

        String prob = pollingService != null ? pollingService.getProbability(symbol) : "";
        tradeHistoryService.record(symbol, ctx.positionSide, ctx.quantity, finalEntry, finalExit, "TARGET_2", ctx.setup, desc, prob);

        eventService.log("[SUCCESS] [WS] T2 hit for " + symbol + " at " + finalExit
            + " | P&L ₹" + String.format("%.2f", pnl) + " — position fully closed");

        try {
            telegramService.notifyT2Hit(symbol, ctx.positionSide, ctx.quantity, finalExit, pnl);
        } catch (Exception ignored) {}

        if (pollingService != null) {
            pollingService.clearSymbolStateFromWs(symbol);
            pollingService.updateLastSyncTime();
        }
    }

    // ── Split SL fill: cancel remaining targets, record trade ──
    private void handleSplitSlFill(OcoContext ctx, String orderId, double tradedPrice) {
        String symbol = ctx.symbol;
        recentlyHandled.put(symbol, System.currentTimeMillis());

        double exitPrice = tradedPrice > 0 ? tradedPrice : orderService.getFilledPriceByOrderId(orderId);
        double entryPrice = ctx.entryFillPrice > 0 ? ctx.entryFillPrice : 0;
        if (entryPrice <= 0 && pollingService != null) entryPrice = pollingService.getEntryAvg(symbol);
        double finalEntry = entryPrice > 0 ? entryPrice : exitPrice;
        double finalExit = exitPrice > 0 ? exitPrice : 0;

        int exitQty = ctx.t1Filled ? (ctx.totalQuantity - ctx.quantity) : ctx.totalQuantity;
        // After T1, SL qty = remaining qty. Before T1, SL qty = total qty.
        exitQty = ctx.quantity; // use the actual SL order qty

        double pnl = "LONG".equals(ctx.positionSide) ? (finalExit - finalEntry) * exitQty
                                                      : (finalEntry - finalExit) * exitQty;
        // Post-T1 SL hit: only label as SL_BREAKEVEN if the SL was actually moved to
        // breakeven (trailing SL enabled). If trailing is off, the new SL stayed at the
        // original price — it's a plain SL hit for the remaining qty.
        String exitReason;
        if (ctx.t1Filled) {
            exitReason = ctx.slAtBreakeven ? "SL_BREAKEVEN" : "SL";
        } else {
            exitReason = ctx.trailed ? "TRAILING_SL" : "SL";
        }

        // Cancel remaining targets
        if (!ctx.t1Filled && ctx.target1OrderId != null) {
            trackedOcoOrders.remove(ctx.target1OrderId);
            orderService.cancelOrder(ctx.target1OrderId);
        }
        if (ctx.target2OrderId != null) {
            trackedOcoOrders.remove(ctx.target2OrderId);
            orderService.cancelOrder(ctx.target2OrderId);
        }
        trackedOcoOrders.remove(orderId);

        String pnlTag = pnl >= 0 ? "PROFIT" : "LOSS";
        String tsExit = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        positionStateStore.appendDescription(symbol,
            tsExit + " [EXIT] " + exitReason + " @ " + String.format("%.2f", finalExit)
            + " | qty=" + exitQty + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl)));
        String desc = positionStateStore.getDescription(symbol);

        String prob = pollingService != null ? pollingService.getProbability(symbol) : "";
        tradeHistoryService.record(symbol, ctx.positionSide, exitQty, finalEntry, finalExit, exitReason, ctx.setup, desc, prob);

        eventService.log("[SUCCESS] [WS] " + exitReason + " triggered for " + symbol + " at " + finalExit
            + " | qty=" + exitQty + " | " + pnlTag + " ₹" + String.format("%.2f", Math.abs(pnl))
            + (ctx.t1Filled ? " (after T1, breakeven SL)" : " (before T1, full loss)"));

        telegramService.notifySlHit(symbol, ctx.positionSide, ctx.quantity, finalExit, pnl, exitReason);

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
            positionStateStore.appendDescription(symbol,
                ("SL".equals(oco.type) ? "[SL_CANCELLED]" : "[TGT_CANCELLED]") + " Manual cancellation.");
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
        if (orderId == null) return;
        OcoContext oco = trackedOcoOrders.get(orderId);
        if (oco == null || oco.handled) return;

        if ("SL".equals(oco.type) && stopPrice > 0) {
            // SL order — check if stopPrice changed
            if (oco.currentPrice > 0 && Math.abs(stopPrice - oco.currentPrice) > 0.10) {
                eventService.log("[SUCCESS] [WS] SL modified for " + symbol + ": "
                    + String.format("%.2f", oco.currentPrice) + " → " + String.format("%.2f", stopPrice));
                positionStateStore.appendDescription(symbol,
                    "[SL_MODIFIED] " + String.format("%.2f", oco.currentPrice) + " → " + String.format("%.2f", stopPrice));
                oco.currentPrice = stopPrice;
                // Update disk state — find counterpart to get target price (split SL has null counterpart, use T1/T2)
                String counterpartId = oco.counterpartOrderId != null ? oco.counterpartOrderId
                    : (oco.target1OrderId != null ? oco.target1OrderId : oco.target2OrderId);
                OcoContext counter = counterpartId != null ? trackedOcoOrders.get(counterpartId) : null;
                double targetPrice = counter != null ? counter.currentPrice : 0;
                positionStateStore.saveOcoState(symbol, orderId, counterpartId, stopPrice, targetPrice);
            } else if (oco.currentPrice <= 0) {
                oco.currentPrice = stopPrice; // initial sync
            }
        } else if ("TARGET".equals(oco.type) && limitPrice > 0) {
            // Target order — check if limitPrice changed
            if (oco.currentPrice > 0 && Math.abs(limitPrice - oco.currentPrice) > 0.10) {
                eventService.log("[SUCCESS] [WS] Target modified for " + symbol + ": "
                    + String.format("%.2f", oco.currentPrice) + " → " + String.format("%.2f", limitPrice));
                positionStateStore.appendDescription(symbol,
                    "[TGT_MODIFIED] " + String.format("%.2f", oco.currentPrice) + " → " + String.format("%.2f", limitPrice));
                oco.currentPrice = limitPrice;
                // Update disk state — find counterpart to get SL price
                String counterpartId = oco.counterpartOrderId != null ? oco.counterpartOrderId : oco.slOrderId;
                OcoContext counter = counterpartId != null ? trackedOcoOrders.get(counterpartId) : null;
                double slPrice = counter != null ? counter.currentPrice : 0;
                positionStateStore.saveOcoState(symbol, counterpartId, orderId, slPrice, limitPrice);
            } else if (oco.currentPrice <= 0) {
                oco.currentPrice = limitPrice; // initial sync
            }
        }
    }
}
