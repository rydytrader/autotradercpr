package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.websocket.FyersOrderWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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

    /** Consecutive 429 disconnects from Fyers' Order WS handshake. Fyers returns 429 on the
     *  WS path when the auth token is dead (in addition to genuine rate limits). After
     *  {@link #AUTH_FAIL_THRESHOLD} in a row we clear the in-memory token — the existing
     *  {@code !tokenStore.isTokenAvailable()} guard in {@link #scheduleReconnect} then stops
     *  the loop until the next morning's login. */
    private volatile int consecutive429s = 0;
    private static final int AUTH_FAIL_THRESHOLD = 2;

    /** Market-window guard so the reconnect loop doesn't churn overnight. NSE F&O hours are
     *  09:15–15:30 IST; we use a small buffer either side for pre-market connect and any
     *  post-close order events still being pushed. */
    private static final ZoneId   IST          = ZoneId.of("Asia/Kolkata");
    private static final LocalTime MARKET_OPEN  = LocalTime.of(9, 10);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 45);

    /** Fill prices keyed by Fyers orderId, populated by {@link #onOrderEvent} as Fyers pushes
     *  filled-status events. ShortStraddle reads from here for any post-hoc race recovery; the
     *  primary correction path is the {@link FillListener} notification below. */
    private final java.util.concurrent.ConcurrentMap<String, Double> fillPriceByOrderId =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** Notified asynchronously the moment Fyers pushes a status=2 (Filled) event. Strategies
     *  register a listener in their bootstrap() and use the callback to correct their
     *  estimated entry / close prices the moment the broker confirms — no blocking on the
     *  hot-path of order placement. Listeners must be lightweight; the WS thread invokes
     *  them synchronously. */
    @FunctionalInterface
    public interface FillListener {
        void onFill(String orderId, double price);
    }

    private final java.util.List<FillListener> fillListeners =
        new java.util.concurrent.CopyOnWriteArrayList<>();

    public void addFillListener(FillListener l) {
        if (l != null && !fillListeners.contains(l)) fillListeners.add(l);
    }

    public void removeFillListener(FillListener l) {
        if (l != null) fillListeners.remove(l);
    }

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
        // Mirrors MarketDataService — once the in-memory token has been cleared (after
        // MarketDataService detected repeated 401s from /data/symbol-token), there's no
        // point hammering the order WS handshake. Resume on the next start() call which
        // ViewController.fyersCallback triggers after a fresh Fyers login.
        if (!tokenStore.isTokenAvailable()) {
            log.info("[OrderEventSvc] Reconnect paused — no access token. Waiting for re-login.");
            return;
        }
        // Skip overnight / weekend churn — Fyers tokens roll over around early morning IST
        // and any in-memory token is dead long before the next market window opens. Without
        // this guard the order WS handshake retries every ~60s through the night, each one
        // logged as a 429. We resume reconnects at the next market window.
        if (isOutsideMarketWindow()) {
            log.info("[OrderEventSvc] Reconnect paused — outside market hours. Waiting for next session.");
            return;
        }
        reconnectAttempts++;
        long delay = Math.min(2L * (1L << Math.min(reconnectAttempts, 5)), 60);
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.schedule(this::connectWebSocket, delay, TimeUnit.SECONDS);
        }
    }

    /** True when the wall clock falls outside the NSE F&O session window (with a small
     *  buffer either side) or it's a weekend. Holidays aren't checked — the worst case is
     *  a single day of log noise that the 429-counter fallback also catches. */
    private boolean isOutsideMarketWindow() {
        ZonedDateTime now = ZonedDateTime.now(IST);
        DayOfWeek dow = now.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) return true;
        LocalTime t = now.toLocalTime();
        return t.isBefore(MARKET_OPEN) || !t.isBefore(MARKET_CLOSE);
    }

    // ── OrderCallback ─────────────────────────────────────────────────────────

    @Override
    public void onOrderEvent(JsonNode order) {
        // Mirrors the equity bot's pattern: when an order reports status=2 (Filled), cache
        // its actual traded price keyed by orderId so the strategy can read the broker-confirmed
        // fill instead of using LTP (which can drift a tick or two between placement and the
        // next quote). Race protection: the strategy may not have called getFillPrice yet, so
        // we just keep the value sitting in the map — the strategy polls it briefly and
        // recovers if the REST response beat the WS push.
        try {
            String orderId = order.has("id") ? order.get("id").asText() : "";
            int status     = order.has("org_ord_status") ? order.get("org_ord_status").asInt() : 0;
            double price   = order.has("price_traded")   ? order.get("price_traded").asDouble(0) : 0;
            // Fyers status: 1=Cancelled, 2=Filled, 5=Rejected, 6=Pending.
            if (status == 2 && !orderId.isEmpty() && price > 0) {
                fillPriceByOrderId.put(orderId, price);
                log.info("[OrderEventSvc] Fill captured for {}: price={}", orderId, price);
                for (FillListener l : fillListeners) {
                    try { l.onFill(orderId, price); }
                    catch (Exception ex) { log.warn("[OrderEventSvc] FillListener threw for {}: {}", orderId, ex.getMessage()); }
                }
            }
        } catch (Exception e) {
            log.error("[OrderEventSvc] Error parsing order event for fill", e);
        }
    }

    /** Returns the broker-confirmed fill price for {@code orderId} once Fyers' order WS has
     *  pushed the filled-status event, or {@code null} if it hasn't landed yet. ShortStraddle
     *  polls this immediately after placement; if it's still null after a short wait, falls
     *  back to a tradebook REST lookup. */
    public Double getFillPrice(String orderId) {
        return orderId == null || orderId.isEmpty() ? null : fillPriceByOrderId.get(orderId);
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
        consecutive429s   = 0;
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
        if (!running) return;
        // Track consecutive 429s separately from other disconnect reasons. Fyers returns 429
        // on the WS handshake when the token is dead — after AUTH_FAIL_THRESHOLD in a row
        // we clear the token and let the reconnect-guard stop the loop until re-login.
        if (reason != null && reason.contains("429")) {
            consecutive429s++;
            log.warn("[OrderEventSvc] Auth-likely 429 disconnect {}/{}", consecutive429s, AUTH_FAIL_THRESHOLD);
            if (consecutive429s >= AUTH_FAIL_THRESHOLD) {
                log.error("[OrderEventSvc] Repeated 429s — clearing in-memory token. Re-login required.");
                if (eventService != null) {
                    eventService.log("[ERROR] [WS] Order WS — repeated 429s, token cleared. Please re-login.");
                }
                tokenStore.setAccessToken("");
                return; // !tokenStore.isTokenAvailable() now blocks scheduleReconnect anyway
            }
        } else {
            consecutive429s = 0;
        }
        scheduleReconnect();
    }
}
