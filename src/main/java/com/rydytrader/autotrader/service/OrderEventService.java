package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.websocket.FyersOrderWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;

/**
 * Lean Order WebSocket service for the options-selling bot.
 *
 * <p>Connects to {@code wss://socket.fyers.in/trade/v3}. On every fill push, looks up the
 * tracked {@link EntryContext} (registered by the strategy service before placement) and
 * updates the local position state.
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
    @SuppressWarnings("unused")
    private final MarketDataService marketDataService;
    @SuppressWarnings("unused")
    private final TelegramService   telegramService;

    @org.springframework.beans.factory.annotation.Autowired
    @Lazy
    private PollingService pollingService;

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
                              MarketDataService marketDataService,
                              TelegramService telegramService) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.eventService = eventService;
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
        // Order fills for straddle legs are tracked by each Strategy via its own orderId
        // state — this WS callback is informational only.
    }

    @Override
    public void onTradeEvent(JsonNode trade) {
        // Per-leg trades recorded by the strategy service itself.
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
