package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.websocket.FyersOrderWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;

/**
 * Lean Order WebSocket service for the options-selling bot.
 *
 * <p>Connects to {@code wss://socket.fyers.in/trade/v3}. On every fill push, looks up the
 * tracked {@link EntryContext} (registered by {@link RollingStraddleService} before placement)
 * and updates the local position state.
 *
 * <p>OCO (SL+Target) tracking from the equity pipeline has been removed — the straddle places
 * no SL/Target legs, so we just record entry-side fills and let the strategy service handle
 * lifecycle.
 */
@Service
public class OrderEventService implements FyersOrderWebSocket.OrderCallback {

    private static final Logger log = LoggerFactory.getLogger(OrderEventService.class);

    private final TokenStore       tokenStore;
    private final FyersProperties  fyersProperties;
    private final EventService     eventService;
    private final PositionStateStore positionStateStore;
    @SuppressWarnings("unused")
    private final MarketDataService marketDataService;
    @SuppressWarnings("unused")
    private final TelegramService   telegramService;

    @org.springframework.beans.factory.annotation.Autowired
    @Lazy
    private PollingService pollingService;

    /** Context for a tracked entry order — registered before placement, consumed on fill. */
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

    private final ConcurrentHashMap<String, EntryContext> trackedEntries = new ConcurrentHashMap<>();

    private volatile FyersOrderWebSocket wsClient;
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile boolean connected = false;
    private volatile int reconnectAttempts = 0;
    private volatile String lastConnectTime = "";
    private volatile String lastDisconnectTime = "";
    private volatile int reconnectCountToday = 0;

    public OrderEventService(TokenStore tokenStore,
                              FyersProperties fyersProperties,
                              EventService eventService,
                              PositionStateStore positionStateStore,
                              MarketDataService marketDataService,
                              TelegramService telegramService) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.eventService = eventService;
        this.positionStateStore = positionStateStore;
        this.marketDataService = marketDataService;
        this.telegramService = telegramService;
    }

    /** Setter used by callers wanting to avoid construction-time circular DI (legacy). */
    public void setPollingService(PollingService pollingService) {
        this.pollingService = pollingService;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public synchronized void start() {
        if (running) stop();
        running = true;
        reconnectAttempts = 0;
        scheduler = Executors.newScheduledThreadPool(2);
        connectWebSocket();
    }

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
    }

    private void connectWebSocket() {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            wsClient = new FyersOrderWebSocket(auth, this);
            boolean ok = wsClient.connectBlocking(15, TimeUnit.SECONDS);
            if (!ok) {
                scheduleReconnect();
            }
        } catch (Exception e) {
            log.error("[OrderEventSvc] WS connect error: {}", e.getMessage());
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!running) return;
        reconnectAttempts++;
        long delay = Math.min(2L * (1L << Math.min(reconnectAttempts, 5)), 60);
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.schedule(this::connectWebSocket, delay, TimeUnit.SECONDS);
        }
    }

    // ── OrderCallback ─────────────────────────────────────────────────────────

    @Override
    public void onOrderEvent(JsonNode order) {
        if (order == null) return;
        int status = order.path("status").asInt(0);
        // 2 = filled
        if (status != 2) return;
        String orderId = order.path("id").asText("");
        if (orderId.isEmpty()) return;
        EntryContext ctx = trackedEntries.remove(orderId);
        if (ctx == null || ctx.handled) return;
        ctx.handled = true;
        double fillPrice = order.path("tradedPrice").asDouble(0);
        if (fillPrice <= 0) fillPrice = order.path("limitPrice").asDouble(0);
        handleEntryFill(ctx, orderId, fillPrice);
    }

    @Override
    public void onTradeEvent(JsonNode trade) {
        // No special handling required for the options-only bot; per-leg trades are recorded
        // by the strategy service via TradeHistoryService once that's wired back up.
    }

    @Override
    public void onPositionEvent(JsonNode position) {
        // PollingService syncs every 10s; this is informational only.
    }

    @Override
    public void onWsConnected() {
        markConnected();
    }

    @Override
    public void onWsDisconnected(String reason) {
        markDisconnected(reason);
    }

    private void handleEntryFill(EntryContext ctx, String orderId, double fillPrice) {
        String entryTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        double entryPrice = fillPrice > 0 ? fillPrice : ctx.slPrice;

        PositionManager.setPosition(ctx.symbol, ctx.position);
        positionStateStore.save(ctx.symbol, ctx.position, ctx.quantity, entryPrice,
            ctx.setup, entryTime, ctx.slPrice, ctx.targetPrice);
        if (pollingService != null) {
            pollingService.addCachedPosition(ctx.symbol, ctx.quantity, ctx.position, entryPrice,
                ctx.setup, entryTime);
            pollingService.updateLastSyncTime();
        }
        eventService.log("[SUCCESS] [WS] " + ("LONG".equals(ctx.position) ? "BUY" : "SELL")
            + " filled for " + ctx.symbol + " @ " + entryPrice + " [ID: " + orderId + "]");
    }

    // ── Tracking API ──────────────────────────────────────────────────────────

    public boolean trackEntryOrder(String orderId, EntryContext ctx) {
        if (orderId == null || orderId.isEmpty() || ctx == null) return false;
        trackedEntries.put(orderId, ctx);
        return true;
    }

    public void untrack(String orderId) {
        trackedEntries.remove(orderId);
    }

    // ── Status accessors ──────────────────────────────────────────────────────

    public boolean isConnected()    { return connected && wsClient != null && wsClient.isOpen(); }
    public boolean isReconnecting() { return running && !isConnected() && reconnectAttempts > 0; }
    public boolean isConnecting()   { return running && !isConnected(); }
    public String  getLastConnectTime()    { return lastConnectTime; }
    public String  getLastDisconnectTime() { return lastDisconnectTime; }
    public int     getReconnectCountToday() { return reconnectCountToday; }
    public int     getTrackedOcoCount() { return 0; }

    /** Called by FyersOrderWebSocket on connection events. */
    public void markConnected() {
        connected = true;
        reconnectAttempts = 0;
        lastConnectTime = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        log.info("[OrderEventSvc] WebSocket connected");
        if (eventService != null) eventService.log("[WS] Order WebSocket connected");
    }

    public void markDisconnected(String reason) {
        connected = false;
        reconnectCountToday++;
        lastDisconnectTime = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        log.info("[OrderEventSvc] WebSocket disconnected: {}", reason);
        if (eventService != null) eventService.log("[WS] Order WebSocket disconnected: " + reason);
        if (running) scheduleReconnect();
    }
}
