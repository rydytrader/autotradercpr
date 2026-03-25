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
public class MarketDataService implements FyersDataWebSocket.TickCallback {

    private static final Logger log = LoggerFactory.getLogger(MarketDataService.class);

    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final PositionStateStore  positionStateStore;
    private final TradeHistoryService tradeHistoryService;
    private final OrderService        orderService;
    private final EventService        eventService;
    private final RiskSettingsStore   riskSettings;
    private final ObjectMapper        mapper = new ObjectMapper();

    // Trailing SL: track symbols where SL has already been trailed (one-time per position)
    private final Set<String> trailedSymbols = ConcurrentHashMap.newKeySet();

    // Current tick state per Fyers symbol
    private final ConcurrentHashMap<String, TickData> currentTicks = new ConcurrentHashMap<>();

    // SSE emitters (browser connections)
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    // WebSocket client
    private volatile FyersDataWebSocket wsClient;

    // Schedulers
    private ScheduledExecutorService scheduler;

    // Symbol mappings
    private final Map<String, String> hsmToFyersSymbol = new ConcurrentHashMap<>();
    private final Map<String, String> fyersToHsmToken  = new ConcurrentHashMap<>();
    private final Set<String> subscribedHsmTokens = ConcurrentHashMap.newKeySet();

    // State flags
    private volatile boolean running = false;
    private volatile boolean dirty = false;
    private volatile int reconnectAttempts = 0;
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
                              RiskSettingsStore riskSettings) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.positionStateStore = positionStateStore;
        this.tradeHistoryService = tradeHistoryService;
        this.orderService = orderService;
        this.eventService = eventService;
        this.riskSettings = riskSettings;
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
                    true, CHANNEL_NUM, this);
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

        // Check trailing SL for position symbols
        if (tick.isHasPosition()) {
            checkTrailingSl(raw.fyersSymbol, raw.ltp);
        }

        dirty = true;
    }

    /**
     * Trailing SL: when price moves 75% from entry toward target,
     * move SL to entry + 10% of the range (one-time per position).
     */
    private void checkTrailingSl(String symbol, double ltp) {
        if (trailedSymbols.contains(symbol)) return;

        Map<String, Object> state = positionStateStore.load(symbol);
        if (state == null) return;

        String side = state.getOrDefault("side", "").toString();
        String slOrderId = state.getOrDefault("slOrderId", "").toString();
        if (slOrderId.isEmpty()) return;

        double entryPrice = 0, targetPrice = 0;
        try {
            entryPrice = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString());
            targetPrice = Double.parseDouble(state.getOrDefault("targetPrice", "0").toString());
        } catch (NumberFormatException ignored) {}

        if (entryPrice <= 0 || targetPrice <= 0) return;

        double range = Math.abs(targetPrice - entryPrice);
        if (range <= 0) return;

        double triggerPct = riskSettings.getTrailTriggerPct() / 100.0; // e.g. 75 → 0.75
        double slPct      = riskSettings.getTrailSlPct() / 100.0;      // e.g. 50 → 0.50
        if (triggerPct <= 0 || slPct <= 0) return; // disabled if set to 0

        double triggerLevel, newSl;
        if ("LONG".equals(side)) {
            triggerLevel = entryPrice + range * triggerPct;
            newSl = entryPrice + range * slPct;
            if (ltp < triggerLevel) return;
        } else if ("SHORT".equals(side)) {
            triggerLevel = entryPrice - range * triggerPct;
            newSl = entryPrice - range * slPct;
            if (ltp > triggerLevel) return;
        } else {
            return;
        }

        // Trail! Mark as done first to prevent duplicate attempts
        trailedSymbols.add(symbol);

        boolean ok = orderService.modifySlOrder(slOrderId, newSl, symbol);
        if (ok) {
            // Update saved SL price on disk
            double currentTarget = Double.parseDouble(state.getOrDefault("targetPrice", "0").toString());
            String targetOrderId = state.getOrDefault("targetOrderId", "").toString();
            positionStateStore.saveOcoState(symbol, slOrderId, targetOrderId, newSl, currentTarget);

            eventService.log("[SUCCESS] Trailing SL for " + symbol + ": moved to "
                + String.format("%.2f", newSl) + " (locking 50% profit)"
                + " — LTP " + String.format("%.2f", ltp) + " crossed 75% level " + String.format("%.2f", triggerLevel));
        } else {
            eventService.log("[ERROR] Failed to trail SL for " + symbol + " to " + String.format("%.2f", newSl));
            trailedSymbols.remove(symbol); // allow retry
        }
    }

    /** Clear trailing SL flag when a position is closed. Called from clearSymbolState flow. */
    public void clearTrailedFlag(String symbol) {
        trailedSymbols.remove(symbol);
    }

    @Override
    public void onConnected() {
        reconnectAttempts = 0;
        log.info("[MarketData] WebSocket fully connected and subscribed");
    }

    @Override
    public void onDisconnected(String reason) {
        if (running) {
            scheduleReconnect();
        }
    }

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

    private void flushSse() {
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
        List<Map<String, Object>> result = new ArrayList<>();
        for (TickData tick : currentTicks.values()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("symbol", tick.getShortName() != null ? tick.getShortName() : tick.getFyersSymbol());
            item.put("lp", Math.round(tick.getLtp() * 100.0) / 100.0);
            item.put("ch", Math.round(tick.getChange() * 100.0) / 100.0);
            item.put("chp", Math.round(tick.getChangePercent() * 100.0) / 100.0);
            item.put("position", tick.isHasPosition());
            result.add(item);
        }
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

    /** Check if currently connected to WebSocket. */
    public boolean isConnected() {
        return wsClient != null && wsClient.isOpen();
    }

    /** Get current tick count (for debug/status). */
    public int getTickCount() {
        return currentTicks.size();
    }
}
