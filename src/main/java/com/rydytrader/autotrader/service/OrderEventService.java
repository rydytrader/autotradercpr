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

    /** Consecutive auth-failure disconnects from Fyers' Order WS handshake. Fyers commonly
     *  returns 401 / 403 / 429 on the WS path when the access token is dead. (403 in particular
     *  is what we see first when the token expires — Fyers stops accepting it before its
     *  rate-limiter starts replying with 429.) After {@link #AUTH_FAIL_THRESHOLD} in a row we
     *  clear the in-memory token — the existing {@code !tokenStore.isTokenAvailable()} guard
     *  in {@link #scheduleReconnect} then stops the loop until the next morning's login. */
    private volatile int consecutiveAuthErrors = 0;
    private static final int AUTH_FAIL_THRESHOLD = 2;

    /** True when the reconnect loop is idle because we're outside market hours / on a weekend.
     *  Set by the {@link #scheduleReconnect} guard, cleared on {@link #start()} and
     *  {@link #markConnected()}. {@link #isConnecting()} and {@link #isReconnecting()} both
     *  read this so the UI status doesn't perpetually show "RECONNECTING" when the loop is
     *  actually paused. */
    private volatile boolean pausedOutsideMarketWindow = false;

    /** True once we've had at least one successful WS handshake since the last {@link #start()}
     *  — i.e. we've proved the in-memory token can authenticate. Used to scope the outside-
     *  market-hours pause guard: if we've never connected (bot booted pre-market with a stale
     *  token), pause and wait for the morning re-login. If we HAVE connected (fresh login at
     *  07:00 followed by a transient WS drop), reconnect normally — token is good. */
    private volatile boolean hasConnectedSinceStart = false;

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
        consecutiveAuthErrors = 0;
        pausedOutsideMarketWindow = false;
        hasConnectedSinceStart    = false;
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
        // Once the in-memory token has been cleared (e.g. LoginService cleared it
        // on repeated 401s from order-side calls), there's no point hammering the
        // order WS handshake. Resume on the next start() call which
        // ViewController.fyersCallback triggers after a fresh Fyers login.
        if (!tokenStore.isTokenAvailable()) {
            log.info("[OrderEventSvc] Reconnect paused — no access token. Waiting for re-login.");
            return;
        }
        // Skip overnight / weekend churn — Fyers tokens roll over around early morning IST
        // and any in-memory token is dead long before the next market window opens. The
        // guard is scoped to "we have never proved this token works" so a fresh login at
        // 07:00 followed by a transient WS drop at 07:30 still reconnects normally (token
        // is good). The 429 counter independently catches mid-day token revocations.
        if (isOutsideMarketWindow() && !hasConnectedSinceStart) {
            if (!pausedOutsideMarketWindow) {
                log.info("[OrderEventSvc] Reconnect paused — outside market hours, no successful connect this run. Waiting for re-login.");
            }
            pausedOutsideMarketWindow = true;
            reconnectAttempts = 0;
            return;
        }
        pausedOutsideMarketWindow = false;
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
    public boolean isReconnecting() { return running && !isPaused() && !isConnected() && reconnectAttempts > 0; }
    public boolean isConnecting()   { return running && !isPaused() && !isConnected(); }
    /** True while the reconnect loop is intentionally paused — outside market hours with no
     *  prior connect this run, OR the in-memory token has been cleared (after repeated 429s
     *  or never set). The scheduleReconnect path returns early in both cases, so we should
     *  not report the service as "RECONNECTING" or "CONNECTING" — it's idle waiting for a
     *  fresh login. Drives the "WAITING FOR LOGIN" status indicator on the UI. */
    public boolean isPaused()       { return running && !isConnected()
                                          && (pausedOutsideMarketWindow || !tokenStore.isTokenAvailable()); }
    public String  getLastConnectTime()    { return lastConnectTime; }
    public String  getLastDisconnectTime() { return lastDisconnectTime; }
    public int     getReconnectCountToday() { return reconnectCountToday; }
    public int     getTrackedOcoCount() { return 0; }

    /** Called by FyersOrderWebSocket on connection events. */
    public void markConnected() {
        connected = true;
        reconnectAttempts = 0;
        consecutiveAuthErrors = 0;
        pausedOutsideMarketWindow = false;
        hasConnectedSinceStart    = true;
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
        // Track consecutive auth-failure disconnects (401 / 403 / 429) separately from
        // other disconnect reasons. Fyers' Order WS hands back 403 first when the access
        // token expires (the token is rejected before the rate-limiter sees the request),
        // then 429s start once retries keep coming. Either way, after AUTH_FAIL_THRESHOLD
        // in a row we clear the in-memory token and let the reconnect-guard stop the loop
        // until re-login.
        if (isAuthFailureReason(reason)) {
            consecutiveAuthErrors++;
            log.warn("[OrderEventSvc] Auth-failure disconnect {} {}/{}",
                reason, consecutiveAuthErrors, AUTH_FAIL_THRESHOLD);
            if (consecutiveAuthErrors >= AUTH_FAIL_THRESHOLD) {
                log.error("[OrderEventSvc] Repeated auth failures — clearing in-memory token. Re-login required.");
                if (eventService != null) {
                    eventService.log("[ERROR] [WS] Order WS — repeated auth failures (" + reason
                        + "), token cleared. Please re-login.");
                }
                tokenStore.setAccessToken("");
                return; // !tokenStore.isTokenAvailable() now blocks scheduleReconnect anyway
            }
        } else {
            consecutiveAuthErrors = 0;
        }
        scheduleReconnect();
    }

    /** True when the disconnect reason looks like an auth failure rather than a transient
     *  network drop. Matches 401 / 403 / 429 anywhere in the reason text. */
    private static boolean isAuthFailureReason(String reason) {
        if (reason == null) return false;
        return reason.contains("401") || reason.contains("403") || reason.contains("429");
    }
}
