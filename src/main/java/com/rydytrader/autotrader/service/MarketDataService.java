package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.controller.MarketTickerController;
import com.rydytrader.autotrader.dto.TickData;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.websocket.FyersDataWebSocket;
import com.rydytrader.autotrader.websocket.HsmBinaryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * Central orchestrator for real-time market data.
 *
 * Connects to Fyers HSM WebSocket, receives binary ticks,
 * pushes updates to browser via SSE.
 *
 * Maintains a ConcurrentHashMap of current TickData per symbol and
 * flushes dirty ticks to all SSE emitters every 500ms.
 */
@Service
public class MarketDataService implements FyersDataWebSocket.TickCallback, CandleAggregator.CandleCloseListener, CandleAggregator.DailyResetListener {

    private static final Logger log = LoggerFactory.getLogger(MarketDataService.class);

    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final PositionStateStore  positionStateStore;
    private final TradeHistoryService tradeHistoryService;
    private final OrderService        orderService;
    private final EventService        eventService;
    private final RiskSettingsStore   riskSettings;
    private final CandleAggregator    candleAggregator;
    private final AtrService          atrService;
    private final EmaService          emaService;
    private final WeeklyCprService    weeklyCprService;
    private final BreakoutScanner     breakoutScanner;
    private final BhavcopyService     bhavcopyService;
    private final TelegramService     telegramService;
    private final HtfEmaService       htfEmaService;
    private final ObjectMapper        mapper = new ObjectMapper();
    private CandleAggregator          htfAggregator; // higher timeframe (75-min) for weekly trend

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private OrderEventService orderEventService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private PollingService pollingService;

    // Trailing SL: track symbols where SL has already been trailed
    private final Set<String> trailedSymbols = ConcurrentHashMap.newKeySet();
    // Peak/trough price since entry for ATR trailing SL
    private final ConcurrentHashMap<String, Double> peakPrice = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> troughPrice = new ConcurrentHashMap<>();

    // Current tick state per Fyers symbol
    private final ConcurrentHashMap<String, TickData> currentTicks = new ConcurrentHashMap<>();

    // SSE emitters (browser connections)
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    // WebSocket client
    private volatile FyersDataWebSocket wsClient;

    // Schedulers
    private ScheduledExecutorService scheduler;

    // Opening refresh: track last run date so we only fire once per day
    private volatile java.time.LocalDate lastOpeningRefreshDate;
    // Opening refresh validation results (set by validateOpeningRefresh, read by status API)
    private volatile int validationPass = -1;   // -1 = not run yet
    private volatile int validationFail = -1;
    private volatile int validationTotal = -1;

    // Symbol mappings
    private final Map<String, String> hsmToFyersSymbol = new ConcurrentHashMap<>();
    private final Map<String, String> fyersToHsmToken  = new ConcurrentHashMap<>();
    private final Set<String> subscribedHsmTokens = ConcurrentHashMap.newKeySet();

    // State flags
    private volatile boolean running = false;
    private volatile boolean dirty = false;
    private volatile int reconnectAttempts = 0;

    // Monitoring: connection timing
    private volatile String lastConnectTime = "";
    private volatile String lastDisconnectTime = "";
    private volatile int reconnectCountToday = 0;
    private volatile long connectSinceMs = 0;
    private static final int MAX_RECONNECT = 5;
    private static final int CHANNEL_NUM = 11;

    // Index symbol → display name mapping (for HSM token resolution)
    private static final Map<String, String> INDEX_DICT = Map.ofEntries(
        Map.entry("NSE:NIFTY50-INDEX", "Nifty 50"),
        Map.entry("NSE:NIFTYBANK-INDEX", "Nifty Bank"),
        Map.entry("NSE:NIFTYIT-INDEX", "Nifty IT"),
        Map.entry("NSE:FINNIFTY-INDEX", "Nifty Fin Service"),
        Map.entry("NSE:MIDCPNIFTY-INDEX", "NIFTY MID SELECT"),
        Map.entry("NSE:NIFTYNEXT50-INDEX", "Nifty Next 50"),
        Map.entry("NSE:INDIAVIX-INDEX", "India VIX")
    );

    // Exchange segment code → segment name
    private static final Map<String, String> EXCH_SEG_DICT = Map.of(
        "1010", "nse_cm",
        "1011", "nse_fo",
        "1120", "mcx_fo",
        "1210", "bse_cm",
        "1012", "cde_fo",
        "1211", "bse_fo"
    );

    // Index symbol → exchange token (from Fyers map.json)
    private static final Map<String, String> INDEX_TOKEN_MAP = Map.ofEntries(
        Map.entry("NSE:NIFTY50-INDEX", "Nifty 50"),
        Map.entry("NSE:NIFTYBANK-INDEX", "Nifty Bank"),
        Map.entry("NSE:NIFTYIT-INDEX", "Nifty IT"),
        Map.entry("NSE:FINNIFTY-INDEX", "Nifty Fin Service"),
        Map.entry("NSE:MIDCPNIFTY-INDEX", "NIFTY MID SELECT"),
        Map.entry("NSE:NIFTYNEXT50-INDEX", "Nifty Next 50"),
        Map.entry("NSE:INDIAVIX-INDEX", "India VIX")
    );

    public MarketDataService(TokenStore tokenStore,
                              FyersProperties fyersProperties,
                              PositionStateStore positionStateStore,
                              TradeHistoryService tradeHistoryService,
                              OrderService orderService,
                              EventService eventService,
                              RiskSettingsStore riskSettings,
                              CandleAggregator candleAggregator,
                              AtrService atrService,
                              WeeklyCprService weeklyCprService,
                              BreakoutScanner breakoutScanner,
                              BhavcopyService bhavcopyService,
                              EmaService emaService,
                              TelegramService telegramService,
                              HtfEmaService htfEmaService) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.positionStateStore = positionStateStore;
        this.tradeHistoryService = tradeHistoryService;
        this.orderService = orderService;
        this.eventService = eventService;
        this.riskSettings = riskSettings;
        this.candleAggregator = candleAggregator;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.breakoutScanner = breakoutScanner;
        this.bhavcopyService = bhavcopyService;
        this.emaService = emaService;
        this.telegramService = telegramService;
        this.htfEmaService = htfEmaService;
        candleAggregator.addListener(this);
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ────────────────────────────────────────────────────────────────────────────

    /** Start the market data feed. Called after login or mode switch. */
    public synchronized void start() {
        if (running) stop();
        running = true;
        reconnectAttempts = 0;
        scheduler = Executors.newScheduledThreadPool(3);

        // SSE flush every 500ms
        scheduler.scheduleAtFixedRate(this::flushSse, 500, 500, TimeUnit.MILLISECONDS);

        // SSE keepalive every 15s
        scheduler.scheduleAtFixedRate(this::sendKeepalive, 15, 15, TimeUnit.SECONDS);

        // Opening refresh check every 30s — re-seeds candles from Fyers history after
        // 9:20 to correct wrong live-tick-built first candles (Fyers WS is unreliable 9:15-9:25)
        scheduler.scheduleAtFixedRate(this::checkOpeningRefresh, 30, 30, TimeUnit.SECONDS);

        // Register candle close listeners
        candleAggregator.setTimeframe(riskSettings.getScannerTimeframe());
        candleAggregator.addListener(atrService);
        candleAggregator.addListener(emaService);
        candleAggregator.addListener(weeklyCprService);
        candleAggregator.addListener(breakoutScanner);
        candleAggregator.start();

        // Higher timeframe aggregator for weekly trend (e.g. 75-min candles)
        htfAggregator = new CandleAggregator(riskSettings);
        htfAggregator.setTimeframe(riskSettings.getHigherTimeframe());
        htfAggregator.addListener((symbol, candle) ->
            weeklyCprService.onHigherTimeframeCandleClose(symbol, candle.open, candle.high, candle.low, candle.close));
        htfAggregator.addListener(htfEmaService);
        htfAggregator.start();
        log.info("[MarketData] Higher timeframe aggregator started: {}min (listeners: WeeklyCpr, HtfEma)", riskSettings.getHigherTimeframe());

        // Schedule scanner pre-market data fetch
        scheduleScannerInit();

        startLiveWebSocket();
        log.info("[MarketData] Started in LIVE mode");
    }

    /** Stop the market data feed. Called on logout or mode switch. */
    public synchronized void stop() {
        running = false;
        if (wsClient != null) {
            try { wsClient.closeBlocking(); } catch (Exception ignored) {}
            wsClient = null;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        candleAggregator.stop();
        currentTicks.clear();
        hsmToFyersSymbol.clear();
        fyersToHsmToken.clear();
        subscribedHsmTokens.clear();
        // Don't clear emitters — browsers may reconnect
        log.info("[MarketData] Stopped");
    }

    // ────────────────────────────────────────────────────────────────────────────
    // LIVE mode: WebSocket
    // ────────────────────────────────────────────────────────────────────────────

    private void startLiveWebSocket() {
        scheduler.submit(() -> {
            try {
                String accessToken = tokenStore.getAccessToken();
                if (accessToken == null || accessToken.isEmpty()) {
                    log.info("[MarketData] No access token, skipping WS connect");
                    return;
                }

                // 1. Extract hsm_key from JWT
                String hsmKey = extractHsmKey(accessToken);
                if (hsmKey == null) {
                    log.info("[MarketData] Failed to extract hsm_key from token");
                    return;
                }

                // 2. Build symbol list
                List<String> fyersSymbols = buildSymbolList();

                // 3. Resolve HSM tokens via API
                resolveSymbolTokens(fyersSymbols, accessToken);

                if (fyersToHsmToken.isEmpty()) {
                    log.info("[MarketData] No HSM tokens resolved, falling back to REST");
                    return;
                }

                List<String> hsmTokens = new ArrayList<>(fyersToHsmToken.values());
                subscribedHsmTokens.addAll(hsmTokens);

                // 4. Connect WebSocket
                wsClient = new FyersDataWebSocket(hsmKey, hsmTokens, hsmToFyersSymbol,
                    false, CHANNEL_NUM, this);
                // Diagnostic: route HSM parser drop-count messages to the event log
                wsClient.setEventLogSink(msg -> {
                    if (eventService != null) eventService.log(msg);
                });
                wsClient.connectBlocking();

                // 5. Start ping scheduler (10s)
                scheduler.scheduleAtFixedRate(() -> {
                    if (wsClient != null && wsClient.isOpen()) {
                        wsClient.sendPing();
                    }
                }, 10, 10, TimeUnit.SECONDS);

            } catch (Exception e) {
                log.error("[MarketData] WS connect error: {}", e.getMessage());
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (!running || reconnectAttempts >= MAX_RECONNECT) {
            log.info("[MarketData] Max reconnect attempts reached, using REST fallback");
            return;
        }
        reconnectAttempts++;
        long delay = Math.min(2L * (1L << reconnectAttempts), 30);
        log.info("[MarketData] Reconnecting in {}s (attempt {})", delay, reconnectAttempts);
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.schedule(this::startLiveWebSocket, delay, TimeUnit.SECONDS);
        }
    }

    // ────────────────────────────────────────────────────────────────────────────
    // WebSocket callback (TickCallback interface)
    // ────────────────────────────────────────────────────────────────────────────

    @Override
    public void onTick(HsmBinaryParser.RawTick raw) {
        if (raw.fyersSymbol == null || raw.fyersSymbol.isEmpty()) return;

        TickData tick = currentTicks.computeIfAbsent(raw.fyersSymbol, k -> new TickData());
        tick.setFyersSymbol(raw.fyersSymbol);
        if (tick.getShortName() == null || tick.getShortName().isEmpty()) {
            tick.setShortName(deriveShortName(raw.fyersSymbol));
        }
        tick.setLtp(raw.ltp);
        if (raw.prevClose > 0) tick.setPrevClose(raw.prevClose);
        if (raw.open > 0) tick.setOpen(raw.open);
        if (raw.high > 0) tick.setHigh(raw.high);
        if (raw.low > 0) tick.setLow(raw.low);
        tick.recalcChange();
        tick.setHasPosition(PositionManager.getAllSymbols().contains(raw.fyersSymbol));

        // Chandelier Exit trailing SL is handled via onCandleClose callback (not per-tick)

        // Route tick to candle aggregators (trading TF + higher TF)
        candleAggregator.onTick(raw);
        if (htfAggregator != null) htfAggregator.onTick(raw);

        // Track peak/trough for ATR trailing SL
        if (raw.ltp > 0 && !"NONE".equals(PositionManager.getPosition(raw.fyersSymbol))) {
            peakPrice.merge(raw.fyersSymbol, raw.ltp, Math::max);
            troughPrice.merge(raw.fyersSymbol, raw.ltp, Math::min);
        }

        dirty = true;
    }

    // ── ATR TRAILING SL (called on every candle close) ─────────────────────────

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (!riskSettings.isEnableTrailingSl()) return;
        String position = PositionManager.getPosition(fyersSymbol);
        if ("NONE".equals(position)) return;

        Map<String, Object> state = positionStateStore.load(fyersSymbol);
        if (state == null) return;

        String slOrderId = state.getOrDefault("slOrderId", "").toString();
        if (slOrderId.isEmpty()) return;

        String side = state.getOrDefault("side", "").toString();
        double currentSlPrice = 0;
        try { currentSlPrice = Double.parseDouble(state.getOrDefault("slPrice", "0").toString()); }
        catch (NumberFormatException ignored) {}

        // Only start trailing after price has moved activation × ATR in the right direction
        double avgPrice = 0;
        try { avgPrice = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString()); }
        catch (NumberFormatException ignored) {}
        double atr = atrService.getAtr(fyersSymbol);
        double ltp = getLtp(fyersSymbol);
        if (avgPrice > 0 && atr > 0 && ltp > 0) {
            double moveFromEntry = "LONG".equals(side) ? ltp - avgPrice : avgPrice - ltp;
            double activationThreshold = atr * riskSettings.getTrailingSlActivationAtr();
            if (moveFromEntry < activationThreshold) return; // not enough profit yet
        }

        // ATR trailing: SL = peak/trough - ATR × trailing multiplier
        double multiplier = riskSettings.getTrailingSlAtrMultiplier();
        if (atr <= 0) return;

        double newSl;
        if ("LONG".equals(side)) {
            double peak = peakPrice.getOrDefault(fyersSymbol, ltp);
            newSl = peak - atr * multiplier;
        } else {
            double trough = troughPrice.getOrDefault(fyersSymbol, ltp);
            newSl = trough + atr * multiplier;
        }
        if (newSl <= 0) return;

        newSl = orderService.roundToTick(newSl, fyersSymbol);

        // Only tighten — LONG: SL moves UP, SHORT: SL moves DOWN
        if ("LONG".equals(side) && newSl <= currentSlPrice) return;
        if ("SHORT".equals(side) && newSl >= currentSlPrice) return;

        log.info("[TrailingSL] {} {} — new SL={} current={} peak={} atr={}",
            fyersSymbol, side, String.format("%.2f", newSl), String.format("%.2f", currentSlPrice),
            String.format("%.2f", "LONG".equals(side) ? peakPrice.getOrDefault(fyersSymbol, 0.0) : troughPrice.getOrDefault(fyersSymbol, 0.0)),
            String.format("%.2f", atr));

        int modResult = orderService.modifySlOrder(slOrderId, newSl, fyersSymbol);
        boolean ok = modResult == 1;
        if (modResult == -1) {
            // SL order is gone (already filled/cancelled). Trigger position sync to clean up local state.
            eventService.log("[INFO] Trailing SL skipped for " + fyersSymbol
                + " — SL order already filled/cancelled, syncing position state");
            if (pollingService != null) {
                pollingService.syncPositionOnce();
            }
            return;
        }
        if (ok) {
            trailedSymbols.add(fyersSymbol);

            String targetOrderId = state.get("targetOrderId") != null ? state.get("targetOrderId").toString() : "";
            double currentTarget = state.get("targetPrice") != null ? Double.parseDouble(state.get("targetPrice").toString()) : 0;
            positionStateStore.saveOcoState(fyersSymbol, slOrderId, targetOrderId, newSl, currentTarget);

            if (orderEventService != null) orderEventService.markAsTrailed(slOrderId);

            String tsTrail = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            positionStateStore.appendDescription(fyersSymbol,
                tsTrail + " [TRAILING_SL] SL → " + String.format("%.2f", newSl)
                + " (ATR=" + String.format("%.2f", atr) + " mult=" + multiplier + ")");

            String direction = "LONG".equals(side) ? "up" : "down";
            eventService.log("[SUCCESS] Trailing SL moved " + direction + " for " + fyersSymbol + ": "
                + String.format("%.2f", currentSlPrice) + " → " + String.format("%.2f", newSl));
            int qty = 0;
            try { qty = Integer.parseInt(state.getOrDefault("qty", "0").toString()); } catch (NumberFormatException ignored) {}
            double peak = "LONG".equals(side) ? peakPrice.getOrDefault(fyersSymbol, ltp) : troughPrice.getOrDefault(fyersSymbol, ltp);
            telegramService.notifySlModified(fyersSymbol, side, qty, newSl, currentSlPrice, peak);
        } else {
            eventService.log("[ERROR] Trailing SL failed for " + fyersSymbol
                + " — could not modify SL to " + String.format("%.2f", newSl));
        }
    }

    /** Check if a symbol's SL has been trailed. */
    public boolean isTrailed(String symbol) {
        return trailedSymbols.contains(symbol);
    }

    /** Clear trailing SL flag + peak/trough when a position is closed. */
    public void clearTrailedFlag(String symbol) {
        trailedSymbols.remove(symbol);
        peakPrice.remove(symbol);
        troughPrice.remove(symbol);
    }

    /** Daily reset — called by CandleAggregator on date change. Clears day-specific state
     *  so the dashboard, trailing SL, and opening refresh start fresh for the new day. */
    @Override
    public void onDailyReset() {
        trailedSymbols.clear();
        peakPrice.clear();
        troughPrice.clear();
        lastOpeningRefreshDate = null;
        validationPass = -1;
        validationFail = -1;
        validationTotal = -1;
        // Reload today's trades from DB (clears yesterday's in-memory list, loads any trades already recorded today)
        tradeHistoryService.reloadForCurrentMode();
        // Clear auth token so user must re-login for the new day (Fyers tokens are day-scoped)
        tokenStore.setAccessToken(null);
        log.info("[MarketData] Daily reset — cleared trailing state, opening refresh, auth token, reloaded trade history");
        eventService.log("[INFO] Daily reset — dashboard, trailing SL, trade history, and auth token cleared for new day. Please re-login.");
    }

    @Override
    public void onConnected() {
        reconnectAttempts = 0;
        lastConnectTime = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        connectSinceMs = System.currentTimeMillis();
        log.info("[MarketData] WebSocket fully connected and subscribed");
    }

    @Override
    public void onDisconnected(String reason) {
        lastDisconnectTime = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        reconnectCountToday++;
        connectSinceMs = 0;
        if (running) {
            scheduleReconnect();
        }
    }

    // Monitoring accessors
    public String getLastConnectTime() { return lastConnectTime; }
    public String getLastDisconnectTime() { return lastDisconnectTime; }
    public int getReconnectCountToday() { return reconnectCountToday; }

    @Override
    public void onAuthResult(boolean success, int ackCount) {
        if (!success) {
            log.error("[MarketData] Auth failed, will not reconnect");
        }
    }

    // ────────────────────────────────────────────────────────────────────────────
    // SSE management
    // ────────────────────────────────────────────────────────────────────────────

    public void addEmitter(SseEmitter emitter) {
        emitters.add(emitter);
    }

    public void removeEmitter(SseEmitter emitter) {
        emitters.remove(emitter);
    }

    /** Send current snapshot to a single emitter (on initial SSE connection). */
    public void sendSnapshot(SseEmitter emitter) {
        try {
            List<Map<String, Object>> payload = buildPayload();
            if (!payload.isEmpty()) {
                emitter.send(SseEmitter.event()
                    .name("ticker")
                    .data(mapper.writeValueAsString(payload)));
            }
            // Also send position snapshot if positions exist
            Set<String> posSymbols = PositionManager.getAllSymbols();
            if (!posSymbols.isEmpty()) {
                Map<String, Object> posPayload = buildPositionPayload(posSymbols);
                emitter.send(SseEmitter.event()
                    .name("positions")
                    .data(mapper.writeValueAsString(posPayload)));
            }
        } catch (Exception e) {
            removeEmitter(emitter);
        }
    }

    private volatile boolean boundaryCheckerWarned = false;

    private void flushSse() {
        // Health check: restart candle boundary checker if it died
        if (!candleAggregator.isBoundaryCheckerAlive() && !boundaryCheckerWarned) {
            boundaryCheckerWarned = true;
            log.error("[MarketData] Candle boundary checker DIED — restarting!");
            eventService.log("[ERROR] Candle boundary checker died — restarting. Signals may have been missed.");
            candleAggregator.start();
        } else if (candleAggregator.isBoundaryCheckerAlive()) {
            boundaryCheckerWarned = false;
        }

        if (!dirty || emitters.isEmpty()) return;
        dirty = false;

        // 1. Ticker event
        List<Map<String, Object>> tickerPayload = buildPayload();
        String tickerJson = null;
        if (!tickerPayload.isEmpty()) {
            try { tickerJson = mapper.writeValueAsString(tickerPayload); } catch (Exception e) { /* skip */ }
        }

        // 2. Positions event (only if there are open positions)
        String posJson = null;
        Set<String> posSymbols = PositionManager.getAllSymbols();
        if (!posSymbols.isEmpty()) {
            try { posJson = mapper.writeValueAsString(buildPositionPayload(posSymbols)); } catch (Exception e) { /* skip */ }
        }

        // 3. Watchlist event (lightweight LTP + change% + volume for scanner page)
        String watchlistJson = null;
        List<String> wl = getWatchlist();
        if (!wl.isEmpty()) {
            try {
                Map<String, Map<String, Object>> wlPayload = new LinkedHashMap<>();
                for (String sym : wl) {
                    double ltp = candleAggregator.getLtp(sym);
                    if (ltp <= 0) continue;
                    Map<String, Object> d = new LinkedHashMap<>();
                    d.put("ltp", Math.round(ltp * 100.0) / 100.0);
                    d.put("changePercent", Math.round(candleAggregator.getChangePct(sym) * 100.0) / 100.0);
                    d.put("candleVolume", candleAggregator.getCurrentCandleVolume(sym));
                    CandleAggregator.CandleBar cb = candleAggregator.getCurrentCandle(sym);
                    if (cb != null) {
                        d.put("candleOpen", Math.round(cb.open * 100.0) / 100.0);
                        d.put("candleHigh", Math.round(cb.high * 100.0) / 100.0);
                        d.put("candleLow", Math.round(cb.low * 100.0) / 100.0);
                    }
                    wlPayload.put(sym, d);
                }
                if (!wlPayload.isEmpty()) {
                    watchlistJson = mapper.writeValueAsString(wlPayload);
                }
            } catch (Exception e) { /* skip */ }
        }

        // Collect dead emitters to remove after iteration
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                if (tickerJson != null) {
                    emitter.send(SseEmitter.event().name("ticker").data(tickerJson));
                }
                if (posJson != null) {
                    emitter.send(SseEmitter.event().name("positions").data(posJson));
                }
                if (watchlistJson != null) {
                    emitter.send(SseEmitter.event().name("watchlist").data(watchlistJson));
                }
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        for (SseEmitter d : dead) {
            try { d.complete(); } catch (Exception ignored) {}
            removeEmitter(d);
        }
    }

    /** Build position payload with live LTP and recalculated P&L. */
    private Map<String, Object> buildPositionPayload(Set<String> posSymbols) {
        List<Map<String, Object>> positions = new ArrayList<>();
        double unrealizedPnl = 0;

        for (String symbol : posSymbols) {
            Map<String, Object> state = positionStateStore.load(symbol);
            if (state == null) continue;

            String side = state.getOrDefault("side", "").toString();
            int qty = 0;
            double avgPrice = 0;
            try {
                qty = Integer.parseInt(state.getOrDefault("qty", "0").toString());
                avgPrice = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString());
            } catch (NumberFormatException ignored) {}

            double ltp = getLtp(symbol);
            if (ltp <= 0) continue; // no tick data yet

            double pnl = "LONG".equals(side)
                ? (ltp - avgPrice) * qty
                : (avgPrice - ltp) * qty;
            unrealizedPnl += pnl;

            Map<String, Object> pos = new LinkedHashMap<>();
            pos.put("symbol", symbol);
            pos.put("ltp", Math.round(ltp * 100.0) / 100.0);
            pos.put("pnl", Math.round(pnl * 100.0) / 100.0);
            pos.put("avgPrice", Math.round(avgPrice * 100.0) / 100.0);
            pos.put("qty", qty);
            pos.put("side", side);
            pos.put("setup", state.getOrDefault("setup", ""));
            pos.put("entryTime", state.getOrDefault("entryTime", ""));
            try { pos.put("slPrice", Double.parseDouble(state.getOrDefault("slPrice", "0").toString())); } catch (NumberFormatException e) { pos.put("slPrice", 0.0); }
            try { pos.put("targetPrice", Double.parseDouble(state.getOrDefault("targetPrice", "0").toString())); } catch (NumberFormatException e) { pos.put("targetPrice", 0.0); }
            pos.put("slTrailed", isTrailed(symbol));
            positions.add(pos);
        }

        double realizedPnl = tradeHistoryService.getTrades().stream()
            .mapToDouble(t -> t.getNetPnl()).sum();
        double netDayPnl = realizedPnl + unrealizedPnl;

        String lastUpdate = java.time.LocalTime.now()
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("positions", positions);
        result.put("unrealizedPnl", Math.round(unrealizedPnl * 100.0) / 100.0);
        result.put("realizedPnl", Math.round(realizedPnl * 100.0) / 100.0);
        result.put("netDayPnl", Math.round(netDayPnl * 100.0) / 100.0);
        result.put("lastSync", lastUpdate);
        return result;
    }

    private void sendKeepalive() {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().comment("keepalive"));
            } catch (Exception e) {
                removeEmitter(emitter);
            }
        }
    }

    private List<Map<String, Object>> buildPayload() {
        List<Map<String, Object>> indices = new ArrayList<>();
        List<Map<String, Object>> stocks = new ArrayList<>();
        for (TickData tick : currentTicks.values()) {
            Map<String, Object> item = new LinkedHashMap<>();
            String shortName = tick.getShortName() != null ? tick.getShortName() : tick.getFyersSymbol();
            item.put("symbol", shortName);
            item.put("lp", Math.round(tick.getLtp() * 100.0) / 100.0);
            item.put("ch", Math.round(tick.getChange() * 100.0) / 100.0);
            item.put("chp", Math.round(tick.getChangePercent() * 100.0) / 100.0);
            item.put("position", tick.isHasPosition());
            // Indices first, then stocks
            if (tick.getFyersSymbol() != null && tick.getFyersSymbol().endsWith("-INDEX")) {
                indices.add(item);
            } else {
                stocks.add(item);
            }
        }
        // Sort indices in preferred order: Nifty 50, Bank Nifty, Nifty IT, rest alphabetical
        java.util.Map<String, Integer> indexOrder = java.util.Map.of(
            "Nifty 50", 1, "NIFTY 50", 1,
            "Nifty Bank", 2, "BANK NIFTY", 2,
            "Nifty IT", 3, "NIFTY IT", 3
        );
        indices.sort((a, b) -> {
            int oa = indexOrder.getOrDefault(a.get("symbol"), 99);
            int ob = indexOrder.getOrDefault(b.get("symbol"), 99);
            if (oa != ob) return Integer.compare(oa, ob);
            return ((String) a.get("symbol")).compareTo((String) b.get("symbol"));
        });
        stocks.sort((a, b) -> ((String) a.get("symbol")).compareTo((String) b.get("symbol")));
        List<Map<String, Object>> result = new ArrayList<>(indices);
        result.addAll(stocks);
        return result;
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Subscription updates (when positions open/close)
    // ────────────────────────────────────────────────────────────────────────────

    /** Called when position symbols change. Subscribes/unsubscribes delta. */
    public void updateSubscriptions() {
        if (!running || wsClient == null || !wsClient.isOpen()) {
            // WebSocket not connected — just update position flags
            for (TickData tick : currentTicks.values()) {
                tick.setHasPosition(PositionManager.getAllSymbols().contains(tick.getFyersSymbol()));
            }
            dirty = true;
            return;
        }

        Set<String> wantedFyers = new LinkedHashSet<>();
        for (String s : MarketTickerController.getBaseSymbols()) {
            wantedFyers.add(s);
        }
        wantedFyers.addAll(PositionManager.getAllSymbols());

        // Find new symbols not yet subscribed
        List<String> toSubscribe = new ArrayList<>();
        for (String fyers : wantedFyers) {
            String hsm = fyersToHsmToken.get(fyers);
            if (hsm != null && !subscribedHsmTokens.contains(hsm)) {
                toSubscribe.add(hsm);
                subscribedHsmTokens.add(hsm);
            }
        }

        // Resolve any new symbols that don't have HSM tokens yet
        List<String> unresolved = new ArrayList<>();
        for (String fyers : wantedFyers) {
            if (!fyersToHsmToken.containsKey(fyers)) {
                unresolved.add(fyers);
            }
        }
        if (!unresolved.isEmpty()) {
            try {
                resolveSymbolTokens(unresolved, tokenStore.getAccessToken());
                for (String fyers : unresolved) {
                    String hsm = fyersToHsmToken.get(fyers);
                    if (hsm != null && !subscribedHsmTokens.contains(hsm)) {
                        toSubscribe.add(hsm);
                        subscribedHsmTokens.add(hsm);
                    }
                }
            } catch (Exception e) {
                log.error("[MarketData] Failed to resolve new symbols: {}", e.getMessage());
            }
        }

        if (!toSubscribe.isEmpty()) {
            wsClient.subscribeSymbols(toSubscribe);
            log.info("[MarketData] Subscribed {} new symbols", toSubscribe.size());
        }

        // Update position flags
        for (TickData tick : currentTicks.values()) {
            tick.setHasPosition(PositionManager.getAllSymbols().contains(tick.getFyersSymbol()));
        }
        dirty = true;
    }

    // ────────────────────────────────────────────────────────────────────────────
    // JWT / Symbol resolution helpers
    // ────────────────────────────────────────────────────────────────────────────

    /** Decode JWT access token and extract hsm_key from payload. */
    private String extractHsmKey(String accessToken) {
        try {
            // Token may be prefixed with clientId: — strip it
            String token = accessToken;
            if (token.contains(":")) {
                token = token.split(":")[1];
            }
            String[] parts = token.split("\\.");
            if (parts.length < 2) return null;

            String payload = parts[1];
            // Add padding
            int pad = 4 - (payload.length() % 4);
            if (pad != 4) payload += "=".repeat(pad);

            byte[] decoded = Base64.getUrlDecoder().decode(payload);
            JsonNode json = mapper.readTree(decoded);
            return json.has("hsm_key") ? json.get("hsm_key").asText() : null;
        } catch (Exception e) {
            log.error("[MarketData] JWT decode error: {}", e.getMessage());
            return null;
        }
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Scanner initialization
    // ────────────────────────────────────────────────────────────────────────────

    /**
     * Schedule pre-market scanner data fetch.
     * If already past 9:00 AM, runs immediately (restart scenario).
     */
    private void scheduleScannerInit() {
        // Always initialize scanner data (watchlist, ATR, trends) for the dashboard page.
        // Signal generation is gated by signalSource check in BreakoutScanner.onCandleClose().
        scheduler.submit(() -> {
            try {
                initScanner();
            } catch (Exception e) {
                log.error("[MarketData] Scanner init failed: {}", e.getMessage());
            }
        });
    }

    /**
     * Initialize scanner: fetch ATR + weekly data, subscribe watchlist symbols.
     */
    private void initScanner() {
        List<String> watchlist = buildWatchlist();
        if (watchlist.isEmpty()) {
            log.warn("[MarketData] No watchlist symbols (narrow/inside CPR lists empty)");
            return;
        }

        log.info("[MarketData] Initializing scanner with {} watchlist symbols", watchlist.size());
        breakoutScanner.setWatchlistSymbols(watchlist);

        // Always include NIFTY 50 index for the index alignment filter — fetched and subscribed
        // alongside the watchlist but NOT passed to the breakout scanner (it's reference data only).
        List<String> watchlistWithIndex = new ArrayList<>(watchlist);
        if (!watchlistWithIndex.contains("NSE:NIFTY50-INDEX")) {
            watchlistWithIndex.add("NSE:NIFTY50-INDEX");
        }

        // Fetch ATR + seed EMA. When EmaService was restored from disk cache, pass the full
        // watchlist so AtrService routes cached symbols through its 1-day catch-up branch
        // (and new symbols through the full 14-day seed). Otherwise drop already-loaded symbols.
        List<String> needsAtr = emaService.isSeededFromCache()
            ? watchlistWithIndex
            : watchlistWithIndex.stream()
                .filter(s -> atrService.getAtr(s) <= 0)
                .collect(java.util.stream.Collectors.toList());
        if (!needsAtr.isEmpty()) {
            log.info("[MarketData] Fetching ATR for {} symbols (cache-seeded={})",
                needsAtr.size(), emaService.isSeededFromCache());
            atrService.fetchAtrForSymbols(needsAtr);
        } else {
            log.info("[MarketData] ATR already loaded for all {} symbols — skipping fetch", watchlistWithIndex.size());
        }
        if (weeklyCprService.getLoadedCount() == 0) {
            weeklyCprService.fetchWeeklyTrends(watchlistWithIndex);
        } else {
            log.info("[MarketData] Weekly trends already loaded ({} symbols) — skipping fetch", weeklyCprService.getLoadedCount());
        }
        // Seed HTF (60-min) EMAs. Same cache-aware routing as ATR above.
        if (htfEmaService.isSeededFromCache()) {
            atrService.fetchHtfEmaForSymbols(watchlistWithIndex, htfEmaService);
        } else if (htfEmaService.getLoadedCount() == 0) {
            atrService.fetchHtfEmaForSymbols(watchlistWithIndex, htfEmaService);
        } else {
            log.info("[MarketData] HTF EMAs already loaded ({} symbols) — skipping fetch", htfEmaService.getLoadedCount());
        }

        // Subscribe watchlist + NIFTY to WebSocket (after API calls give WS time to connect)
        subscribeWatchlist(watchlistWithIndex);

        // Verify all symbols subscribed — retry if some failed
        int subscribed = 0;
        for (String fyers : watchlist) {
            String hsm = fyersToHsmToken.get(fyers);
            if (hsm != null && subscribedHsmTokens.contains(hsm)) subscribed++;
        }
        if (subscribed < watchlist.size()) {
            log.warn("[MarketData] Only {}/{} symbols subscribed, retrying in 3s...", subscribed, watchlist.size());
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
            subscribeWatchlist(watchlist);
        }

        double minPrice = riskSettings.getScanMinPrice();
        double narrowMax = riskSettings.getNarrowCprMaxWidth();
        double insideMax = riskSettings.getInsideCprMaxWidth();
        eventService.log("[INFO] Scanner initialized: " + watchlist.size() + " symbols"
            + " (filters: narrow<" + narrowMax + "%, inside<" + insideMax + "%, price≥₹" + (int)minPrice + ")");
        eventService.log("[SUCCESS] All prerequisites loaded — system ready for trading");

        int narrowCount = (int) bhavcopyService.getNarrowCprStocks().size();
        int insideCount = (int) bhavcopyService.getInsideCprStocks().size();
        int atrLoaded = atrService.getLoadedCount();
        int weeklyCount = weeklyCprService.getLoadedCount();
        telegramService.notifyBotReady(watchlist.size(), narrowCount, insideCount,
            atrLoaded, weeklyCount, riskSettings.getHigherTimeframe());
    }

    /**
     * Build watchlist from narrow + inside CPR stocks.
     * Returns Fyers symbols (e.g., "NSE:RELIANCE-EQ").
     */
    private List<String> buildWatchlist() {
        Set<String> symbols = new LinkedHashSet<>();
        double narrowMax = riskSettings.getNarrowCprMaxWidth();
        double insideMax = riskSettings.getInsideCprMaxWidth();

        for (var cpr : bhavcopyService.getAllCprLevels().values()) {
            if (cpr.getCprWidthPct() < narrowMax && passesWatchlistFilters(cpr)) {
                symbols.add("NSE:" + cpr.getSymbol() + "-EQ");
            }
        }
        for (var cpr : bhavcopyService.getInsideCprStocks()) {
            if ((insideMax <= 0 || cpr.getCprWidthPct() < insideMax) && passesWatchlistFilters(cpr)) {
                symbols.add("NSE:" + cpr.getSymbol() + "-EQ");
            }
        }
        return new ArrayList<>(symbols);
    }

    public boolean passesWatchlistFilters(com.rydytrader.autotrader.dto.CprLevels cpr) {
        double minPrice = riskSettings.getScanMinPrice();
        double maxPrice = riskSettings.getScanMaxPrice();
        if (minPrice > 0 && cpr.getClose() < minPrice) return false;
        if (maxPrice > 0 && cpr.getClose() > maxPrice) return false;
        double minTurnover = riskSettings.getScanMinTurnover();
        if (minTurnover > 0 && cpr.getAvgTurnover20() > 0 && cpr.getAvgTurnover20() / 1e7 < minTurnover) return false;
        long minVolume = riskSettings.getScanMinVolume();
        if (minVolume > 0 && cpr.getAvgVolume20() > 0 && cpr.getAvgVolume20() < minVolume) return false;
        double minBeta = riskSettings.getScanMinBeta();
        double maxBeta = riskSettings.getScanMaxBeta();
        if (minBeta > 0 && cpr.getBeta() > 0 && cpr.getBeta() < minBeta) return false;
        if (maxBeta > 0 && cpr.getBeta() > 0 && cpr.getBeta() > maxBeta) return false;
        String capFilter = riskSettings.getScanCapFilter();
        if (capFilter != null && !capFilter.isEmpty() && !"ALL".equals(capFilter)) {
            String cap = cpr.getCapCategory();
            if (cap == null) cap = "SMALL";
            boolean match = false;
            for (String allowed : capFilter.split(",")) {
                if (allowed.trim().equals(cap)) { match = true; break; }
            }
            if (!match) return false;
        }
        return true;
    }

    /**
     * Subscribe watchlist symbols to the HSM WebSocket.
     */
    private void subscribeWatchlist(List<String> fyersSymbols) {
        if (wsClient == null || !wsClient.isOpen()) {
            log.warn("[MarketData] WebSocket not connected, cannot subscribe watchlist");
            return;
        }

        // Resolve HSM tokens for watchlist symbols
        List<String> unresolved = new ArrayList<>();
        for (String fyers : fyersSymbols) {
            if (!fyersToHsmToken.containsKey(fyers)) {
                unresolved.add(fyers);
            }
        }
        if (!unresolved.isEmpty()) {
            try {
                resolveSymbolTokens(unresolved, tokenStore.getAccessToken());
            } catch (Exception e) {
                log.error("[MarketData] Failed to resolve watchlist tokens: {}", e.getMessage());
            }
        }

        // Subscribe new tokens
        List<String> toSubscribe = new ArrayList<>();
        List<String> unresolvedSymbols = new ArrayList<>();
        for (String fyers : fyersSymbols) {
            String hsm = fyersToHsmToken.get(fyers);
            if (hsm != null && !subscribedHsmTokens.contains(hsm)) {
                toSubscribe.add(hsm);
                subscribedHsmTokens.add(hsm);
            } else if (hsm == null) {
                unresolvedSymbols.add(fyers);
            }
        }

        if (!toSubscribe.isEmpty()) {
            wsClient.subscribeSymbols(toSubscribe);
        }
        int totalSubscribed = 0;
        for (String fyers : fyersSymbols) {
            String hsm = fyersToHsmToken.get(fyers);
            if (hsm != null && subscribedHsmTokens.contains(hsm)) totalSubscribed++;
        }
        log.info("[MarketData] Watchlist WebSocket: {}/{} subscribed ({} new)", totalSubscribed, fyersSymbols.size(), toSubscribe.size());
        if (!unresolvedSymbols.isEmpty()) {
            log.warn("[MarketData] Failed to resolve HSM tokens for: {}", unresolvedSymbols);
        }
    }

    /** Get watchlist for external use (scanner dashboard). */
    public List<String> getWatchlist() {
        return buildWatchlist();
    }

    /**
     * Periodic check (every 30s) — triggers the opening refresh exactly once per trading day,
     * at or shortly after the configured time (default 09:25 IST). Re-fetches today's candles
     * from Fyers /data/history for every watchlist symbol and re-seeds firstCandleClose,
     * dayOpen, openingRangeHigh/Low, EMA, ATR, and the completedCandles deque. This corrects
     * any wrong values from the unreliable live tick stream during 9:15-9:25 (Fyers-documented
     * issue — their live WS can't deliver ticks fast enough for all subscribed symbols in the
     * first 5-10 minutes).
     */
    private void checkOpeningRefresh() {
        try {
            if (!riskSettings.isEnableOpeningRefresh()) return;

            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            java.time.LocalDate today = java.time.ZonedDateTime.now(ist).toLocalDate();

            // Skip weekends and holidays
            java.time.DayOfWeek dow = today.getDayOfWeek();
            if (dow == java.time.DayOfWeek.SATURDAY || dow == java.time.DayOfWeek.SUNDAY) return;

            // Only once per day
            if (today.equals(lastOpeningRefreshDate)) return;

            // Parse configured refresh time
            String timeStr = riskSettings.getOpeningRefreshTime();
            if (timeStr == null || timeStr.isEmpty()) return;
            java.time.LocalTime refreshTime;
            try {
                refreshTime = java.time.LocalTime.parse(timeStr);
            } catch (Exception e) {
                log.warn("[OpeningRefresh] Invalid time format '{}', expected HH:mm", timeStr);
                return;
            }

            java.time.LocalTime now = java.time.ZonedDateTime.now(ist).toLocalTime();
            if (now.isBefore(refreshTime)) return;

            // Fire the refresh
            List<String> watchlist = buildWatchlist();
            if (watchlist.isEmpty()) {
                log.warn("[OpeningRefresh] Empty watchlist, skipping");
                return;
            }
            List<String> withIndex = new ArrayList<>(watchlist);
            if (!withIndex.contains("NSE:NIFTY50-INDEX")) withIndex.add("NSE:NIFTY50-INDEX");

            eventService.log("[INFO] Opening refresh triggered — refetching today's candles from Fyers /data/history for "
                + withIndex.size() + " symbols (fixes wrong live-tick-built first candles)");

            // Reuse existing seeding path — fetchAtrForSymbols re-fetches history,
            // re-computes ATR, re-seeds candleAggregator.completedCandles (today-only filter
            // picks the correct 9:15-9:20 bar from history), and re-seeds EMA from the raw list.
            // Also re-writes firstCandleClose, dayOpen, OR via seedCandles' direct put calls.
            atrService.fetchAtrForSymbols(withIndex);

            lastOpeningRefreshDate = today;
            eventService.log("[SUCCESS] Opening refresh complete — firstCandleClose, dayOpen, OR, EMA, ATR all re-seeded from authoritative Fyers history");

            // Diagnostic: log pre-snapshot drop summary captured by the HSM parser between
            // subscribe-time and now. Tells us whether the 9:15-9:20 data loss came from
            // updates arriving before their snapshot (parser-side drop) or from Fyers not
            // sending any data at all (server-side rate limit).
            if (wsClient != null) {
                String summary = wsClient.buildPreSnapshotDropSummary();
                eventService.log("[INFO] [HsmParser] " + summary);
            }

            // Schedule validation 30 seconds later — gives the refresh a moment to settle,
            // then re-fetches 5 random watchlist symbols fresh from Fyers and diffs them
            // against the in-memory values to confirm the refresh worked.
            scheduler.schedule(() -> validateOpeningRefresh(watchlist), 30, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.error("[OpeningRefresh] Error: {}", t.getMessage(), t);
        }
    }

    /** Validation: re-fetch 5 random watchlist symbols fresh from Fyers and compare
     *  against the in-memory state populated by the opening refresh. Logs each check
     *  to the event log so we can confirm the refresh produced clean data. */
    private void validateOpeningRefresh(List<String> watchlist) {
        try {
            if (watchlist == null || watchlist.isEmpty()) return;
            List<String> pool = new ArrayList<>(watchlist);
            pool.removeIf(s -> s == null || s.contains("INDEX"));
            Collections.shuffle(pool);
            int n = Math.min(5, pool.size());
            List<String> sample = pool.subList(0, n);

            eventService.log("[INFO] [VALIDATE] Spot-checking " + n + " random symbols against fresh Fyers history...");
            int pass = 0, fail = 0;
            List<String> failLines = new ArrayList<>();
            for (String symbol : sample) {
                List<CandleAggregator.CandleBar> fyersBars = atrService.fetchTodayCandles(symbol);
                if (fyersBars.isEmpty()) {
                    eventService.log("[WARNING] [VALIDATE] " + symbol + " — Fyers history empty, skipping");
                    continue;
                }
                // Filter to today's bars only
                long todayStartEpoch = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
                    .toLocalDate().atStartOfDay(java.time.ZoneId.of("Asia/Kolkata")).toEpochSecond();
                List<CandleAggregator.CandleBar> todayBars = new ArrayList<>();
                for (CandleAggregator.CandleBar b : fyersBars) {
                    if (b.epochSec >= todayStartEpoch) todayBars.add(b);
                }
                if (todayBars.isEmpty()) {
                    eventService.log("[WARNING] [VALIDATE] " + symbol + " — no today bars from Fyers, skipping");
                    continue;
                }

                // Reference: the two morning bars from Fyers (authoritative)
                CandleAggregator.CandleBar fyersBar1 = todayBars.get(0); // 9:15-9:20
                CandleAggregator.CandleBar fyersBar2 = todayBars.size() >= 2 ? todayBars.get(1) : null; // 9:20-9:25

                // Bot state: firstCandleClose + dayOpen maps, plus the actual bars in completedCandles
                double actualFirstClose = candleAggregator.getFirstCandleClose(symbol);
                double actualDayOpen    = candleAggregator.getDayOpen(symbol);
                List<CandleAggregator.CandleBar> botBars = candleAggregator.getCompletedCandles(symbol);
                CandleAggregator.CandleBar botBar1 = botBars.size() >= 1 ? botBars.get(0) : null;
                CandleAggregator.CandleBar botBar2 = botBars.size() >= 2 ? botBars.get(1) : null;

                List<String> failures = new ArrayList<>();

                // Check 1: firstCandleClose map
                if (Math.abs(actualFirstClose - fyersBar1.close) >= 0.01) {
                    failures.add("firstClose bot=" + String.format("%.2f", actualFirstClose) + " vs Fyers=" + String.format("%.2f", fyersBar1.close));
                }
                // Check 2: dayOpen map
                if (Math.abs(actualDayOpen - fyersBar1.open) >= 0.01) {
                    failures.add("dayOpen bot=" + String.format("%.2f", actualDayOpen) + " vs Fyers=" + String.format("%.2f", fyersBar1.open));
                }
                // Check 3: 9:15-9:20 bar full OHLC in completedCandles
                if (botBar1 == null) {
                    failures.add("bar1 missing from completedCandles");
                } else {
                    if (Math.abs(botBar1.open  - fyersBar1.open)  >= 0.01) failures.add("bar1.O bot=" + String.format("%.2f", botBar1.open)  + " vs Fyers=" + String.format("%.2f", fyersBar1.open));
                    if (Math.abs(botBar1.high  - fyersBar1.high)  >= 0.01) failures.add("bar1.H bot=" + String.format("%.2f", botBar1.high)  + " vs Fyers=" + String.format("%.2f", fyersBar1.high));
                    if (Math.abs(botBar1.low   - fyersBar1.low)   >= 0.01) failures.add("bar1.L bot=" + String.format("%.2f", botBar1.low)   + " vs Fyers=" + String.format("%.2f", fyersBar1.low));
                    if (Math.abs(botBar1.close - fyersBar1.close) >= 0.01) failures.add("bar1.C bot=" + String.format("%.2f", botBar1.close) + " vs Fyers=" + String.format("%.2f", fyersBar1.close));
                }
                // Check 4: 9:20-9:25 bar full OHLC (if available from both sides)
                if (fyersBar2 != null && botBar2 != null) {
                    if (Math.abs(botBar2.open  - fyersBar2.open)  >= 0.01) failures.add("bar2.O bot=" + String.format("%.2f", botBar2.open)  + " vs Fyers=" + String.format("%.2f", fyersBar2.open));
                    if (Math.abs(botBar2.high  - fyersBar2.high)  >= 0.01) failures.add("bar2.H bot=" + String.format("%.2f", botBar2.high)  + " vs Fyers=" + String.format("%.2f", fyersBar2.high));
                    if (Math.abs(botBar2.low   - fyersBar2.low)   >= 0.01) failures.add("bar2.L bot=" + String.format("%.2f", botBar2.low)   + " vs Fyers=" + String.format("%.2f", fyersBar2.low));
                    if (Math.abs(botBar2.close - fyersBar2.close) >= 0.01) failures.add("bar2.C bot=" + String.format("%.2f", botBar2.close) + " vs Fyers=" + String.format("%.2f", fyersBar2.close));
                }

                if (failures.isEmpty()) {
                    eventService.log("[SUCCESS] [VALIDATE] " + symbol + " PASS — bar1 O=" + String.format("%.2f", fyersBar1.open)
                        + " H=" + String.format("%.2f", fyersBar1.high)
                        + " L=" + String.format("%.2f", fyersBar1.low)
                        + " C=" + String.format("%.2f", fyersBar1.close)
                        + (fyersBar2 != null ? " | bar2 C=" + String.format("%.2f", fyersBar2.close) : "")
                        + " (matches Fyers history)");
                    pass++;
                } else {
                    String failLine = symbol + " FAIL — " + String.join(" | ", failures);
                    eventService.log("[ERROR] [VALIDATE] " + failLine);
                    failLines.add(failLine);
                    fail++;
                }
            }
            // Store results for status API
            validationPass = pass;
            validationFail = fail;
            validationTotal = n;

            String summary = "[INFO] [VALIDATE] Done — " + pass + " pass, " + fail + " fail out of " + n + " sampled";
            eventService.log(fail == 0 ? "[SUCCESS] " + summary.substring(6) : summary);

            // Telegram alert
            StringBuilder tg = new StringBuilder();
            tg.append(fail == 0 ? "OPENING REFRESH VALIDATION — OK\n" : "OPENING REFRESH VALIDATION — ISSUES\n");
            tg.append("Sampled: ").append(n).append("\n");
            tg.append("Pass: ").append(pass).append("\n");
            tg.append("Fail: ").append(fail).append("\n");
            if (!failLines.isEmpty()) {
                tg.append("\nFailures:\n");
                for (String f : failLines) tg.append("- ").append(f).append("\n");
            }
            telegramService.sendMessage(tg.toString());
        } catch (Throwable t) {
            log.error("[Validate] Error: {}", t.getMessage(), t);
            eventService.log("[ERROR] [VALIDATE] Exception: " + t.getMessage());
            telegramService.sendMessage("OPENING REFRESH VALIDATION — EXCEPTION\n" + t.getMessage());
        }
    }

    /** Rebuild watchlist from current settings without full re-init. Subscribes new symbols. */
    public int rebuildWatchlist() {
        List<String> watchlist = buildWatchlist();
        if (watchlist.isEmpty()) {
            log.warn("[MarketData] Rebuild: no watchlist symbols after filter");
            return 0;
        }
        breakoutScanner.setWatchlistSymbols(watchlist);
        List<String> withIndex = new ArrayList<>(watchlist);
        if (!withIndex.contains("NSE:NIFTY50-INDEX")) withIndex.add("NSE:NIFTY50-INDEX");
        subscribeWatchlist(withIndex);
        log.info("[MarketData] Watchlist rebuilt: {} symbols", watchlist.size());
        eventService.log("[INFO] Watchlist rebuilt: " + watchlist.size() + " symbols (filters updated)");
        return watchlist.size();
    }

    /** Build the full symbol list (base + position symbols). */
    private List<String> buildSymbolList() {
        Set<String> symbols = new LinkedHashSet<>();
        for (String s : MarketTickerController.getBaseSymbols()) {
            symbols.add(s);
        }
        symbols.addAll(PositionManager.getAllSymbols());
        return new ArrayList<>(symbols);
    }

    /**
     * Call Fyers symbol-token API to convert Fyers symbols to HSM tokens.
     * Populates fyersToHsmToken and hsmToFyersSymbol maps.
     */
    private void resolveSymbolTokens(List<String> fyersSymbols, String accessToken) throws Exception {
        String token = accessToken;
        if (token.contains(":")) {
            token = token.split(":")[1];
        }

        String jsonBody = mapper.writeValueAsString(Map.of("symbols", fyersSymbols));

        URL url = new URL("https://api-t1.fyers.in/data/symbol-token");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Authorization", token);
        conn.setDoOutput(true);
        conn.getOutputStream().write(jsonBody.getBytes(StandardCharsets.UTF_8));
        conn.getOutputStream().close();

        InputStream is = conn.getResponseCode() < 400 ? conn.getInputStream() : conn.getErrorStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sb.append(line);
        br.close();

        JsonNode resp = mapper.readTree(sb.toString());
        if (resp.has("s") && "ok".equals(resp.get("s").asText()) && resp.has("validSymbol")) {
            JsonNode valid = resp.get("validSymbol");
            Iterator<Map.Entry<String, JsonNode>> fields = valid.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String fyersSymbol = entry.getKey();
                String fyToken = entry.getValue().asText();

                String hsmToken = convertToHsmToken(fyersSymbol, fyToken);
                if (hsmToken != null) {
                    fyersToHsmToken.put(fyersSymbol, hsmToken);
                    hsmToFyersSymbol.put(hsmToken, fyersSymbol);
                }
            }
        }

        log.info("[MarketData] Resolved {} symbol tokens", fyersToHsmToken.size());
    }

    /**
     * Convert a fyToken to HSM token format.
     * Scrips: "sf|{segment}|{token}" where segment = exch_seg_dict[fytoken[:4]], token = fytoken[10:]
     * Index:  "if|{segment}|{index_name}" where index_name from INDEX_TOKEN_MAP
     */
    private String convertToHsmToken(String fyersSymbol, String fyToken) {
        if (fyToken.length() < 10) return null;

        String exSg = fyToken.substring(0, 4);
        String segment = EXCH_SEG_DICT.get(exSg);
        if (segment == null) return null;

        // Check if it's an index symbol
        if (fyersSymbol.endsWith("-INDEX")) {
            String indexName = INDEX_TOKEN_MAP.get(fyersSymbol);
            if (indexName != null) {
                return "if|" + segment + "|" + indexName;
            }
            // Fallback: use symbol part
            String parts = fyersSymbol.split(":")[1].split("-")[0];
            return "if|" + segment + "|" + parts;
        }

        // Regular scrip
        String exchToken = fyToken.substring(10);
        return "sf|" + segment + "|" + exchToken;
    }

    private String deriveShortName(String fyersSymbol) {
        // "NSE:NIFTY50-INDEX" → check INDEX_DICT first
        String display = INDEX_DICT.get(fyersSymbol);
        if (display != null) return display;
        try {
            // "NSE:RELIANCE-EQ" → "RELIANCE"
            // "NSE:BAJAJ-AUTO-EQ" → "BAJAJ-AUTO"
            String afterColon = fyersSymbol.split(":")[1];
            // Strip known suffixes: -EQ, -INDEX, -MF, -BE, etc.
            return afterColon.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        } catch (Exception e) {
            return fyersSymbol;
        }
    }

    /** Get live LTP for a symbol. Returns 0 if no tick data available. */
    public double getLtp(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getLtp() > 0) ? tick.getLtp() : 0;
    }

    /** Get today's day open for a symbol. */
    public double getDayOpen(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getOpen() > 0) ? tick.getOpen() : 0;
    }

    /** Get today's session high for a symbol. */
    public double getDayHigh(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getHigh() > 0) ? tick.getHigh() : 0;
    }

    /** Get today's session low for a symbol. */
    public double getDayLow(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getLow() > 0) ? tick.getLow() : 0;
    }

    /** Check if currently connected to WebSocket. */
    public boolean isConnected() {
        return wsClient != null && wsClient.isOpen();
    }

    public boolean isReconnecting() {
        return running && !isConnected() && reconnectAttempts > 0;
    }

    /**
     * True when the service has been started (login complete) but the WS isn't connected yet.
     * Covers both the initial post-login connection attempt AND post-disconnect retries.
     * Used by status display to show "CONNECTING" instead of "DISCONNECTED" while we wait.
     */
    public boolean isConnecting() {
        return running && !isConnected();
    }

    public int getEmitterCount() { return emitters.size(); }

    /** Opening refresh validation results (-1 = not run yet). */
    public int getValidationPass()  { return validationPass; }
    public int getValidationFail()  { return validationFail; }
    public int getValidationTotal() { return validationTotal; }

    /** Get current tick count (for debug/status). */
    public int getTickCount() {
        return currentTicks.size();
    }
}
