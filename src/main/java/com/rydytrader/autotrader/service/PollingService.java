package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.PositionsDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

/**
 * Lean polling service for the options-selling bot.
 *
 * <p>Two jobs:
 * <ol>
 *   <li><b>Position sync</b> — every 10s, fetch the broker positions list and reconcile with
 *       the in-memory cache.</li>
 *   <li><b>Manual squareoff</b> — close a position at market on user request.</li>
 * </ol>
 *
 * <p>The equity entry-monitor + OCO-monitor + trailing-SL loops from the old service have all
 * been removed. The Rolling Straddle runs its own scheduler and uses {@link OrderEventService}
 * (or this service's fallback fill-detection) to handle leg fills.
 */
@Service
public class PollingService {

    private static final Logger log = LoggerFactory.getLogger(PollingService.class);

    private final TokenStore       tokenStore;
    private final FyersProperties  fyersProperties;
    private final FyersClientRouter fyersClient;
    private final OrderService     orderService;
    private final EventService     eventService;
    private final MarketDataService  marketDataService;

    @org.springframework.beans.factory.annotation.Autowired
    @Lazy
    private OrderEventService orderEventService;

    private final ConcurrentHashMap<String, PositionsDTO> cachedPositions = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;
    private volatile String connectionStatus = "DISCONNECTED";
    private volatile String lastSyncTime = "";
    private volatile boolean running = false;

    public PollingService(TokenStore tokenStore,
                           FyersProperties fyersProperties,
                           FyersClientRouter fyersClient,
                           OrderService orderService,
                           EventService eventService,
                           MarketDataService marketDataService) {
        this.tokenStore        = tokenStore;
        this.fyersProperties   = fyersProperties;
        this.fyersClient       = fyersClient;
        this.orderService      = orderService;
        this.eventService      = eventService;
        this.marketDataService = marketDataService;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public synchronized void startPositionSync() {
        if (running) return;
        running = true;
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::syncPosition, 0, 10, TimeUnit.SECONDS);
        log.info("[Polling] Position sync started (10s interval)");
    }

    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        cachedPositions.clear();
    }

    /** Sync broker positions into the local cache. Called by the scheduler + manually. */
    public void syncPosition() {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getPositions(auth);
            if (root == null || !"ok".equals(root.path("s").asText())) {
                connectionStatus = "DISCONNECTED";
                return;
            }
            connectionStatus = "POLLING";
            JsonNode netPositions = root.path("netPositions");
            Map<String, PositionsDTO> fresh = new LinkedHashMap<>();
            Set<String> openSymbols = new LinkedHashSet<>();

            if (netPositions != null && netPositions.isArray()) {
                for (JsonNode p : netPositions) {
                    int netQty = p.path("netQty").asInt(0);
                    if (netQty == 0) continue; // closed position
                    String symbol = p.path("symbol").asText("");
                    if (symbol.isEmpty()) continue;
                    // Options-only bot: silently ignore non-option positions the user holds at
                    // the broker (manual equity / index / futures trades). They stay invisible
                    // to PositionManager so the data WS doesn't subscribe to them and the bot's
                    // squareoff / kill-switch logic doesn't get confused by their P&L.
                    if (!isOptionSymbol(symbol)) continue;
                    String side  = netQty > 0 ? "LONG" : "SHORT";
                    double avg   = p.path("netAvg").asDouble(0);
                    double ltp   = p.path("ltp").asDouble(0);
                    double pnl   = p.path("pl").asDouble(0);
                    int qty      = Math.abs(netQty);
                    PositionsDTO dto = new PositionsDTO(symbol, qty, side, avg, ltp, pnl, "", "");
                    fresh.put(symbol, dto);
                    openSymbols.add(symbol);
                    PositionManager.setPosition(symbol, side);
                }
            }

            // Remove positions that closed externally
            for (String sym : new HashSet<>(cachedPositions.keySet())) {
                if (!fresh.containsKey(sym)) {
                    cachedPositions.remove(sym);
                    PositionManager.setPosition(sym, "NONE");
                }
            }
            cachedPositions.putAll(fresh);
            updateLastSyncTime();

            // Push subscription updates if open position set changed.
            marketDataService.updateSubscriptions();
        } catch (Exception e) {
            log.error("[Polling] syncPosition error: {}", e.getMessage());
            connectionStatus = "DISCONNECTED";
        }
    }

    public void syncPositionOnce() { syncPosition(); }

    // ── Manual squareoff ──────────────────────────────────────────────────────

    public boolean squareOff(String symbol, int quantity) {
        return squareOff(symbol, quantity, "MANUAL");
    }

    public boolean squareOff(String symbol, int quantity, String reason) {
        try {
            // Cancel any pending orders for this symbol first (SL/Target legs etc.).
            orderService.cancelAllPendingOrders(symbol);
            // Read current position side to know which side to close.
            String side = PositionManager.getPosition(symbol);
            if ("NONE".equals(side) || side == null) {
                log.info("[Polling] squareOff({}) — no open position to close", symbol);
                return false;
            }
            int exitSide = "LONG".equals(side) ? -1 : 1;
            var resp = orderService.placeExitOrder(symbol, quantity, exitSide);
            if (resp == null || resp.getId() == null || resp.getId().isEmpty()) {
                eventService.log("[ERROR] Squareoff failed for " + symbol + " (" + reason + ")");
                return false;
            }
            eventService.log("[INFO] Squareoff placed for " + symbol + " qty=" + quantity + " (" + reason + ")");
            // Refresh positions shortly after to reflect the close.
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.schedule(this::syncPosition, 2, TimeUnit.SECONDS);
            }
            return true;
        } catch (Exception e) {
            log.error("[Polling] squareOff error for {}: {}", symbol, e.getMessage());
            return false;
        }
    }

    /** Close every open position at market. Called by the auto-squareoff scheduler / kill switch. */
    public void squareOffAll() {
        for (PositionsDTO p : new ArrayList<>(cachedPositions.values())) {
            squareOff(p.getSymbol(), p.getQty(), "SQUAREOFF_ALL");
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public List<PositionsDTO> fetchPositions() {
        return new ArrayList<>(cachedPositions.values());
    }

    public String getConnectionStatus() {
        boolean orderWs = orderEventService != null && orderEventService.isConnected();
        boolean dataWs  = marketDataService.isConnected();
        if (orderWs && dataWs)  return "WS CONNECTED";
        if (orderWs && !dataWs) return "RECONNECTING (Data)";
        if (!orderWs && dataWs) return "RECONNECTING (Order)";
        if (marketDataService.isReconnecting() || (orderEventService != null && orderEventService.isReconnecting())) return "RECONNECTING";
        if (marketDataService.isConnecting()   || (orderEventService != null && orderEventService.isConnecting()))   return "CONNECTING";
        return connectionStatus;
    }

    public String getLastSyncTime() { return lastSyncTime; }
    public int    getOpenPositionCount() { return cachedPositions.size(); }
    public void   updateLastSyncTime() {
        lastSyncTime = java.time.LocalTime.now()
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    /** Add a position to the local cache without re-fetching from broker. Used by
     *  {@link OrderEventService} to surface freshly-filled entries immediately. */
    public void addCachedPosition(String symbol, int qty, String side, double avgPrice, String setup, String entryTime) {
        cachedPositions.put(symbol, new PositionsDTO(symbol, qty, side, avgPrice, 0, 0, setup, entryTime));
        PositionManager.setPosition(symbol, side);
        updateLastSyncTime();
    }

    /** True when the Fyers symbol is an option (CE/PE on any underlying), false for equity,
     *  index, futures, mutual funds, etc. Done by exclusion of the well-known non-option
     *  segment suffixes. Examples accepted: NSE:NIFTY25527C24350, NSE:BANKNIFTY...P50000.
     *  Rejected: NSE:INFY-EQ, NSE:NIFTY50-INDEX, NSE:NIFTY25MAYFUT. */
    private static boolean isOptionSymbol(String fyersSymbol) {
        if (fyersSymbol == null || fyersSymbol.isEmpty()) return false;
        String upper = fyersSymbol.toUpperCase();
        if (upper.endsWith("-EQ") || upper.endsWith("-INDEX") || upper.endsWith("-MF")
            || upper.endsWith("-BE") || upper.endsWith("-BL") || upper.endsWith("-SM")
            || upper.endsWith("FUT")) return false;
        // Must end in C<digits> or P<digits> (strike price following the option type letter)
        return upper.matches(".*[CP]\\d+$");
    }
}
