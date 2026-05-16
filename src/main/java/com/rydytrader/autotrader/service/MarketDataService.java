package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.controller.MarketTickerController;
import com.rydytrader.autotrader.dto.CprLevels;
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
    private final SmaService          smaService;
    private final WeeklyCprService    weeklyCprService;
    private final BreakoutScanner     breakoutScanner;
    private final BhavcopyService     bhavcopyService;
    private final TelegramService     telegramService;
    private final SmaCrossExitService smaCrossExitService;
    @org.springframework.beans.factory.annotation.Autowired
    private SymbolMasterService       symbolMasterService;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService      marketHolidayService;
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

    // Index symbol → exchange "Symbol" string used in the HSM binary feed. The hsm_key
    // generated by convertToHsmToken is "if|{segment}|{index_name}" — without an entry here
    // the symbol's HSM token can't be built, so the WS never subscribes and getLtp returns 0.
    // Names match the strings published in NSE's bhavcopy "Index Name" column (case-sensitive).
    private static final Map<String, String> INDEX_TOKEN_MAP = Map.ofEntries(
        Map.entry("NSE:NIFTY50-INDEX",          "Nifty 50"),
        Map.entry("NSE:NIFTYBANK-INDEX",        "Nifty Bank"),
        Map.entry("NSE:NIFTYIT-INDEX",          "Nifty IT"),
        Map.entry("NSE:FINNIFTY-INDEX",         "Nifty Fin Service"),
        Map.entry("NSE:NIFTYPHARMA-INDEX",      "Nifty Pharma"),
        Map.entry("NSE:NIFTYAUTO-INDEX",        "Nifty Auto"),
        Map.entry("NSE:NIFTYFMCG-INDEX",        "Nifty FMCG"),
        Map.entry("NSE:NIFTYMETAL-INDEX",       "Nifty Metal"),
        Map.entry("NSE:NIFTYENERGY-INDEX",      "Nifty Energy"),
        // HSM name per Fyers Python SDK index_dict (FyersWebsocket/map.json): uppercase
        // "NIFTY HEALTHCARE". Differs from the bhavcopy CSV column ("Nifty Healthcare Index").
        Map.entry("NSE:NIFTYHEALTHCARE-INDEX",  "NIFTY HEALTHCARE"),
        Map.entry("NSE:NIFTYREALTY-INDEX",      "Nifty Realty"),
        Map.entry("NSE:NIFTYMEDIA-INDEX",       "Nifty Media"),
        // HSM name per Fyers Python SDK index_dict: uppercase "NIFTY OIL AND GAS".
        Map.entry("NSE:NIFTYOILANDGAS-INDEX",   "NIFTY OIL AND GAS"),
        // HSM name per Fyers Python SDK: abbreviated "NIFTY CONSR DURBL" (not the
        // bhavcopy CSV's "Nifty Consumer Durables").
        Map.entry("NSE:NIFTYCONSRDURBL-INDEX",  "NIFTY CONSR DURBL"),
        // Broader thematic indices used as fallback for industries without a direct sectoral index.
        // HSM name per Fyers Python SDK: "Nifty Serv Sector" (abbreviated "Serv").
        Map.entry("NSE:NIFTYSERVSECTOR-INDEX",  "Nifty Serv Sector"),
        Map.entry("NSE:NIFTYCONSUMPTION-INDEX", "Nifty Consumption"),
        // HSM publishes this as "Nifty Infra" (short form, matches the Fyers symbol
        // "NIFTYINFRA"), not the bhavcopy CSV's "Nifty Infrastructure".
        Map.entry("NSE:NIFTYINFRA-INDEX",       "Nifty Infra"),
        Map.entry("NSE:NIFTYCOMMODITIES-INDEX", "Nifty Commodities"),
        // Speculative: NIFTY Chemicals isn't in the Fyers equity symbol master and is
        // also absent from the Fyers Python SDK's index_dict (no HSM publication).
        // Subscription silently fails — Chemicals chip stays neutral.
        Map.entry("NSE:NIFTYCHEM-INDEX",        "Nifty Chemicals"),
        // NOTE: NIFTYMSITTELCM (MidSmall IT & Telecom) is also absent from Fyers' index_dict
        // — HSM doesn't publish it. Telecom chip now points at NIFTYSERVSECTOR (BHARTIARTL
        // is a Services Sector constituent) as the live-LTP proxy. See SECTOR_TO_INDEX in
        // BhavcopyService.
        Map.entry("NSE:MIDCPNIFTY-INDEX",       "NIFTY MID SELECT"),
        Map.entry("NSE:NIFTYNEXT50-INDEX",      "Nifty Next 50"),
        Map.entry("NSE:INDIAVIX-INDEX",         "India VIX")
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
                              SmaService smaService,
                              TelegramService telegramService,
                              SmaCrossExitService smaCrossExitService) {
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
        this.smaService = smaService;
        this.telegramService = telegramService;
        this.smaCrossExitService = smaCrossExitService;
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
        candleAggregator.addListener(smaService);
        candleAggregator.addListener(weeklyCprService);
        candleAggregator.addListener(breakoutScanner);
        // Defensive exits — must register AFTER smaService so candle.sma20 is populated.
        // Reads candle's post-close completed-only SMA snapshot.
        candleAggregator.addListener(smaCrossExitService);
        candleAggregator.start();

        // Higher timeframe aggregator for weekly trend (e.g. 75-min candles)
        htfAggregator = new CandleAggregator(riskSettings);
        htfAggregator.setTimeframe(riskSettings.getHigherTimeframe());
        htfAggregator.addListener((symbol, candle) ->
            weeklyCprService.onHigherTimeframeCandleClose(symbol, candle.open, candle.high, candle.low, candle.close));
        htfAggregator.start();
        log.info("[MarketData] Higher timeframe aggregator started: {}min (listener: WeeklyCpr)", riskSettings.getHigherTimeframe());

        // Schedule scanner pre-market data fetch
        scheduleScannerInit();

        startLiveWebSocket();
        log.info("[MarketData] Started in LIVE mode");

        // One-shot diagnostic: 90s after WS start, list any subscribed indices that
        // haven't received a single tick. This identifies HSM index-name mismatches
        // (we built "if|nse_cm|X" but the server publishes under a different X).
        scheduler.schedule(this::logUntickedIndices, 90, TimeUnit.SECONDS);
    }

    /** Diagnostic — log any -INDEX symbols subscribed via HSM but with no tick in currentTicks. */
    private void logUntickedIndices() {
        try {
            List<String> missing = new ArrayList<>();
            for (Map.Entry<String, String> e : fyersToHsmToken.entrySet()) {
                String fyersSymbol = e.getKey();
                if (!fyersSymbol.endsWith("-INDEX")) continue;
                TickData tick = currentTicks.get(fyersSymbol);
                if (tick == null || tick.getLtp() <= 0) {
                    String hsmName = INDEX_TOKEN_MAP.getOrDefault(fyersSymbol, "<no-map>");
                    missing.add(fyersSymbol + " (subscribed as \"" + hsmName + "\")");
                }
            }
            if (missing.isEmpty()) {
                log.info("[MarketData] Diagnostic: all subscribed indices have received ticks");
            } else {
                log.warn("[MarketData] Diagnostic: {} subscribed index symbol(s) have no ticks after 90s — likely HSM name mismatch: {}",
                    missing.size(), missing);
            }
        } catch (Exception ex) {
            log.warn("[MarketData] Unticked-indices diagnostic failed: {}", ex.getMessage());
        }
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
                    log.info("[MarketData] No access token — waiting for re-login before WS can connect");
                    if (eventService != null) eventService.log("[WS] No access token — please re-login (Fyers tokens expire daily)");
                    scheduleReconnect();
                    return;
                }

                // 1. Extract hsm_key from JWT
                String hsmKey = extractHsmKey(accessToken);
                if (hsmKey == null) {
                    log.info("[MarketData] Failed to extract hsm_key from token — token may be malformed");
                    if (eventService != null) eventService.log("[WS] Failed to extract hsm_key — token may be malformed; re-login required");
                    scheduleReconnect();
                    return;
                }

                // 2. Build symbol list
                List<String> fyersSymbols = buildSymbolList();

                // 3. Resolve HSM tokens via API
                resolveSymbolTokens(fyersSymbols, accessToken);

                if (fyersToHsmToken.isEmpty()) {
                    log.info("[MarketData] No HSM tokens resolved (token likely expired) — will retry with backoff");
                    if (eventService != null) eventService.log("[WS] HSM token resolve failed — Fyers token likely expired; please re-login");
                    scheduleReconnect();
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
                // Bounded connect — without a timeout, connectBlocking can hang indefinitely
                // if Fyers' WS endpoint is briefly unavailable (e.g. ~8 AM maintenance),
                // blocking the scheduler thread and stalling future reconnects.
                boolean connected = wsClient.connectBlocking(15, TimeUnit.SECONDS);
                if (!connected) {
                    log.warn("[MarketData] WS connectBlocking timed out after 15s — scheduling retry");
                    try { wsClient.close(); } catch (Exception ignored) {}
                    wsClient = null;
                    scheduleReconnect();
                    return;
                }

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
        if (!running) return;
        reconnectAttempts++;
        // Escalating backoff capped at 60s — never give up during a trading session.
        // If Fyers/token remain unreachable, we keep retrying every 60s indefinitely so
        // the moment a fresh token / network is back, WS comes alive without a server restart.
        long delay = Math.min(2L * (1L << Math.min(reconnectAttempts, 5)), 60);
        if (reconnectAttempts == 1 || reconnectAttempts % 10 == 0) {
            log.info("[MarketData] Reconnect attempt #{} in {}s", reconnectAttempts, delay);
            if (reconnectAttempts >= 10 && eventService != null) {
                eventService.log("[WS] Reconnect attempt #" + reconnectAttempts + " — still trying (every "
                    + delay + "s). If stuck, check Fyers login / network.");
            }
        }
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.schedule(this::startLiveWebSocket, delay, TimeUnit.SECONDS);
        }
    }

    // ────────────────────────────────────────────────────────────────────────────
    // WebSocket callback (TickCallback interface)
    // ────────────────────────────────────────────────────────────────────────────

    // Diagnostic: ticks arriving with an HSM topic name not in hsmToFyersSymbol
    // (e.g. an index whose name in INDEX_TOKEN_MAP doesn't match what HSM publishes).
    // Log each unmapped token once so we can identify the exact name HSM uses without
    // flooding logs on every tick. Cleared per-process; reset on restart.
    private final java.util.concurrent.ConcurrentHashMap<String, Boolean> loggedUnmappedHsmTokens =
        new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void onTick(HsmBinaryParser.RawTick raw) {
        if (raw.fyersSymbol == null || raw.fyersSymbol.isEmpty()) {
            if (raw.hsmToken != null && loggedUnmappedHsmTokens.putIfAbsent(raw.hsmToken, Boolean.TRUE) == null) {
                log.info("[MarketData] Unmapped HSM tick: token=\"{}\" ltp={} — add to INDEX_TOKEN_MAP if this is a sectoral index", raw.hsmToken, raw.ltp);
            }
            return;
        }

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
        tick.setLastTickDate(java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString());
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

    // ── BREAKEVEN SL (called on every candle close) ──────────────────────────
    // Single-stage move. Range = |target - entry|. When the peak (long) / trough (short)
    // reaches breakevenTriggerPct% of that range, SL is moved to entry ± breakevenSlAtrMult
    // × ATR. Once moved, the never-widen guard keeps it there for the remainder of the trade.
    // Split-target trades: suppressed until T1 fills; then activates on remaining qty.

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
        if (!"LONG".equals(side) && !"SHORT".equals(side)) return;

        // Split-target gate: wait for T1 fill before breakeven kicks in.
        Object t1Obj = state.get("target1OrderId");
        String target1OrderId = t1Obj != null ? t1Obj.toString() : "";
        boolean isSplit = !target1OrderId.isEmpty() && !"null".equalsIgnoreCase(target1OrderId);
        boolean t1Filled = Boolean.TRUE.equals(state.get("t1Filled"));
        if (isSplit && !t1Filled) return;

        double currentSl = 0;
        try { currentSl = Double.parseDouble(state.getOrDefault("slPrice", "0").toString()); }
        catch (NumberFormatException ignored) {}
        double entry = 0;
        try { entry = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString()); }
        catch (NumberFormatException ignored) {}
        double target = 0;
        try { target = Double.parseDouble(state.getOrDefault("targetPrice", "0").toString()); }
        catch (NumberFormatException ignored) {}

        double atr = atrService.getAtr(fyersSymbol);
        double ltp = getLtp(fyersSymbol);
        if (entry <= 0 || target <= 0 || atr <= 0 || ltp <= 0) return;

        double range = Math.abs(target - entry);
        if (range <= 0) return;

        boolean isBuy = "LONG".equals(side);
        int sign = isBuy ? 1 : -1;
        double triggerPct = riskSettings.getBreakevenTriggerPct() / 100.0;
        double slAtrMult  = riskSettings.getBreakevenSlAtrMult();
        double triggerPx  = entry + sign * triggerPct * range;
        double desiredSl  = entry + sign * slAtrMult * atr;

        double extreme = isBuy
            ? peakPrice.getOrDefault(fyersSymbol, ltp)
            : troughPrice.getOrDefault(fyersSymbol, ltp);

        // Stage not reached yet — wait.
        if (isBuy  && extreme < triggerPx) return;
        if (!isBuy && extreme > triggerPx) return;

        String stageLabel = String.format("%.1f%% of range → SL at entry%s%.2f ATR",
            riskSettings.getBreakevenTriggerPct(),
            slAtrMult >= 0 ? "+" : "-", Math.abs(slAtrMult));

        desiredSl = orderService.roundToTick(desiredSl, fyersSymbol);

        // Never-widen guard
        if (isBuy && desiredSl <= currentSl) return;
        if (!isBuy && desiredSl >= currentSl) return;

        log.info("[BreakevenSL] {} {} — {}: desiredSl={} currentSl={} entry={} target={} trigger={} extreme={} atr={}",
            fyersSymbol, side, stageLabel,
            String.format("%.2f", desiredSl), String.format("%.2f", currentSl),
            String.format("%.2f", entry), String.format("%.2f", target),
            String.format("%.2f", triggerPx),
            String.format("%.2f", extreme), String.format("%.2f", atr));

        int modResult = orderService.modifySlOrder(slOrderId, desiredSl, fyersSymbol);
        if (modResult == -1) {
            eventService.log("[INFO] Breakeven SL skipped for " + fyersSymbol
                + " — SL order already filled/cancelled, syncing position state");
            if (pollingService != null) pollingService.syncPositionOnce();
            return;
        }
        if (modResult != 1) {
            eventService.log("[ERROR] Breakeven SL failed for " + fyersSymbol
                + " — could not modify SL to " + String.format("%.2f", desiredSl));
            return;
        }

        trailedSymbols.add(fyersSymbol);
        String targetOrderId = state.get("targetOrderId") != null ? state.get("targetOrderId").toString() : "";
        positionStateStore.saveOcoState(fyersSymbol, slOrderId, targetOrderId, desiredSl, target);
        if (orderEventService != null) orderEventService.markAsTrailed(slOrderId);

        String tsTrail = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        positionStateStore.appendDescription(fyersSymbol,
            tsTrail + " [BREAKEVEN_SL] SL → " + String.format("%.2f", desiredSl)
            + " (" + stageLabel + "; entry=" + String.format("%.2f", entry)
            + " target=" + String.format("%.2f", target) + ")");

        String direction = isBuy ? "up" : "down";
        eventService.log("[SUCCESS] " + fyersSymbol + " " + side + " SL moved " + direction
            + ": " + String.format("%.2f", currentSl) + " → " + String.format("%.2f", desiredSl)
            + " (" + stageLabel + ", range " + String.format("%.2f", entry) + "→" + String.format("%.2f", target) + ")");

        int qty = 0;
        try { qty = Integer.parseInt(state.getOrDefault("qty", "0").toString()); } catch (NumberFormatException ignored) {}
        telegramService.notifySlModified(fyersSymbol, side, qty, desiredSl, currentSl, extreme);
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
            log.error("[MarketData] Auth failed — Fyers token likely expired; will keep retrying until re-login");
            if (eventService != null) {
                eventService.log("[WS] Fyers auth failed — please re-login. Bot will auto-reconnect once a fresh token is set.");
            }
            // Keep retrying rather than giving up: the user may re-login at any moment and
            // we want WS to come back without a server restart.
            scheduleReconnect();
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
        // Add NIFTY 50 index to the SSE payload so the scanner's NIFTY card can update
        // ltp/change/sma in real time. buildWatchlist() excludes indices because they're not
        // tradable, but the dashboard still needs NIFTY's tick data.
        List<String> ssePayloadSymbols = new ArrayList<>(wl);
        ssePayloadSymbols.add(IndexTrendService.NIFTY_SYMBOL);
        if (!ssePayloadSymbols.isEmpty()) {
            try {
                Map<String, Map<String, Object>> wlPayload = new LinkedHashMap<>();
                for (String sym : ssePayloadSymbols) {
                    double ltp = candleAggregator.getLtp(sym);
                    if (ltp <= 0) ltp = getLtp(sym);  // fallback to TickData (same source as scrolling ticker)
                    if (ltp <= 0) continue;
                    Map<String, Object> d = new LinkedHashMap<>();
                    d.put("ltp", Math.round(ltp * 100.0) / 100.0);
                    // change% fallback: candleAggregator's latestChangePct map skips ticks with
                    // changePercent==0 so it can be empty for quiet symbols. TickData.changePercent
                    // is recomputed from prev close on every tick (recalcChange) — authoritative.
                    double chPct = candleAggregator.getChangePct(sym);
                    if (chPct == 0) chPct = getChangePercent(sym);
                    d.put("changePercent", Math.round(chPct * 100.0) / 100.0);
                    // Absolute change in points (ltp - prev close), pre-computed by TickData.
                    double ch = getChange(sym);
                    d.put("change", Math.round(ch * 100.0) / 100.0);
                    d.put("candleVolume", candleAggregator.getCurrentCandleVolume(sym));
                    CandleAggregator.CandleBar cb = candleAggregator.getCurrentCandle(sym);
                    if (cb != null) {
                        d.put("candleOpen", Math.round(cb.open * 100.0) / 100.0);
                        d.put("candleHigh", Math.round(cb.high * 100.0) / 100.0);
                        d.put("candleLow", Math.round(cb.low * 100.0) / 100.0);
                    }
                    // Live SMA values — both compute lazily with current LTP blending,
                    // so the UI sees tick-fresh values instead of waiting for the 15s full refresh.
                    // Rounded to 2 decimals only (TV does not snap SMAs to tick size — keeping
                    // raw value rounded to ₹0.01 lets the bot's display match TV exactly).
                    double s20  = smaService.getSma(sym);
                    if (s20  > 0) d.put("sma",      Math.round(s20  * 100.0) / 100.0);
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

    /** Round a derived price to the symbol's tick size. Falls back to 2-decimal rounding
     *  if tick is unknown so the value is still readable. Display only — filter math uses
     *  the raw value via SmaService. */
    private static double roundToTick(double value, double tick) {
        if (tick <= 0) return Math.round(value * 100.0) / 100.0;
        return Math.round(value / tick) * tick;
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
            int remainingQty = 0;
            double avgPrice = 0;
            try {
                qty = Integer.parseInt(state.getOrDefault("qty", "0").toString());
                remainingQty = Integer.parseInt(state.getOrDefault("remainingQty", "0").toString());
                avgPrice = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString());
            } catch (NumberFormatException ignored) {}

            // Open-qty source of truth: after T1 fills on a split-target trade, the original
            // qty field still holds the entry quantity (saveT1FilledState does not touch it),
            // but remainingQty is set atomically alongside t1Filled. Prefer remainingQty when
            // t1Filled — this keeps live P&L stable through the syncPosition reconciliation
            // window where Fyers' position API can briefly return either full or remaining qty,
            // causing the displayed P&L to flicker.
            boolean t1Filled = Boolean.TRUE.equals(state.get("t1Filled"));
            int openQty = (t1Filled && remainingQty > 0) ? remainingQty : qty;

            double ltp = getLtp(symbol);
            if (ltp <= 0) continue; // no tick data yet

            double pnl = "LONG".equals(side)
                ? (ltp - avgPrice) * openQty
                : (avgPrice - ltp) * openQty;
            unrealizedPnl += pnl;

            Map<String, Object> pos = new LinkedHashMap<>();
            pos.put("symbol", symbol);
            pos.put("ltp", Math.round(ltp * 100.0) / 100.0);
            pos.put("pnl", Math.round(pnl * 100.0) / 100.0);
            pos.put("avgPrice", Math.round(avgPrice * 100.0) / 100.0);
            pos.put("qty", openQty);
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
        // Stale-day guard: if a tick's lastTickDate is from a previous IST trading date,
        // zero out lp / ch / chp so the scrolling ticker (and any consumer that overwrites
        // card values from this stream, like the NIFTY card SSE handler) doesn't render
        // yesterday's values on a new trading day before the first new-day tick lands.
        String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString();
        for (TickData tick : currentTicks.values()) {
            Map<String, Object> item = new LinkedHashMap<>();
            String shortName = tick.getShortName() != null ? tick.getShortName() : tick.getFyersSymbol();
            item.put("symbol", shortName);
            boolean staleDay = tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate());
            double lp  = staleDay ? 0 : tick.getLtp();
            double ch  = staleDay ? 0 : tick.getChange();
            double chp = staleDay ? 0 : tick.getChangePercent();
            item.put("lp", Math.round(lp * 100.0) / 100.0);
            item.put("ch", Math.round(ch * 100.0) / 100.0);
            item.put("chp", Math.round(chp * 100.0) / 100.0);
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
        // Near-month NIFTY futures — diagnostic for VWAP/ATP availability. Indices don't have
        // traded volume so spot ATP is always 0; futures trade with real volume → real ATP.
        String niftyFut = computeNearMonthNiftyFuturesSymbol(
            java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")));
        if (!watchlistWithIndex.contains(niftyFut)) {
            watchlistWithIndex.add(niftyFut);
        }

        // Prune indicator caches to the FULL NIFTY 50 universe (not the filtered watchlist).
        // Filter changes can re-include stocks; we want their indicator data preserved. Pruning
        // only removes data for non-NIFTY-50 stocks (e.g., legacy positions that closed).
        List<String> seedUniverse = buildSeedUniverse(watchlistWithIndex);
        atrService.pruneTo(seedUniverse);
        smaService.pruneTo(seedUniverse);
        candleAggregator.pruneTo(seedUniverse);

        // Pre-seed ATR / SMA / weekly for ALL NIFTY 50 stocks (not just the filtered watchlist
        // subset). Filter changes mid-day can bring previously-excluded stocks into the
        // watchlist; pre-seeding ensures their indicators are ready instantly. Cost is one
        // historical fetch per stock at startup vs. lazy seeding on every rebuild.
        log.info("[MarketData] Seeding ATR + 5-min SMA for {} symbols (full NIFTY 50)", seedUniverse.size());
        atrService.fetchAtrForSymbols(seedUniverse);
        log.info("[MarketData] Seeding weekly trends for {} symbols", seedUniverse.size());
        weeklyCprService.fetchWeeklyTrends(seedUniverse);

        // Subscribe ALL 50 NIFTY stocks + indices to WebSocket — needed for live LTP on the
        // full universe so NIFTY breadth (advancers vs decliners) reflects the entire 50,
        // not just the filtered watchlist subset. Bandwidth cost negligible (~50 symbols).
        subscribeWatchlist(seedUniverse);

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

        int narrowCount = (int) bhavcopyService.getNarrowCprStocks().size();
        int insideCount = (int) bhavcopyService.getInsideCprStocks().size();
        // Watchlist-scoped counts — avoid misleading totals that include stale or non-watched symbols.
        int atrLoaded = atrService.getLoadedCountFor(watchlist);
        int weeklyCount = weeklyCprService.getLoadedCountFor(watchlist);
        int cprCount = bhavcopyService.getLoadedCountFor(watchlist);
        int e20 = smaService.getLoadedCountFor(watchlist);
        int total = watchlist.size();
        int htfMins = riskSettings.getHigherTimeframe();

        eventService.log("[INFO] All prerequisites loaded"
            + " (watchlist=" + total
            + ", narrow=" + narrowCount + "/inside=" + insideCount
            + ", CPR=" + cprCount + ", ATR=" + atrLoaded
            + ", weekly-trend=" + weeklyCount
            + ", 5m SMA 20=" + e20 + ")");
        eventService.log("[SUCCESS] System ready for trading");

        telegramService.notifyBotReady(total, narrowCount, insideCount,
            atrLoaded, weeklyCount, cprCount,
            e20, htfMins);
    }

    /**
     * Build watchlist from narrow + inside CPR stocks.
     * Returns Fyers symbols (e.g., "NSE:RELIANCE-EQ").
     */
    private List<String> buildWatchlist() {
        // BhavcopyService now filters to NIFTY 50 at the parse stage (cpr cache only contains
        // NIFTY 50 stocks when the list is available; full FNO as a fallback). No extra filter
        // needed here — width / price / volume / cap / NS-NL toggles still apply.
        Set<String> symbols = new LinkedHashSet<>();
        double narrowMax = riskSettings.getNarrowCprMaxWidth();
        double insideMax = riskSettings.getInsideCprMaxWidth();

        for (var cpr : bhavcopyService.getAllCprLevels().values()) {
            if (bhavcopyService.isIndex(cpr.getSymbol())) continue; // NIFTY50 etc. are not tradable stocks
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

    /** Build the indicator-seeding universe: ALL stocks in the configured scan universe
     *  (NIFTY 50 or NIFTY 100) + NIFTY index + everything in the filtered watchlist
     *  (which is a subset already, but include for safety). Used at startup and on
     *  watchlist rebuild so filter changes don't strand stocks without ATR/SMA. */
    private List<String> buildSeedUniverse(List<String> filteredWatchlistWithIndex) {
        Set<String> universe = new LinkedHashSet<>(filteredWatchlistWithIndex);
        for (var cpr : bhavcopyService.getAllCprLevels().values()) {
            if (bhavcopyService.isIndex(cpr.getSymbol())) continue;
            if (bhavcopyService.isInScanUniverse(cpr.getSymbol())) {
                universe.add("NSE:" + cpr.getSymbol() + "-EQ");
            }
        }
        return new ArrayList<>(universe);
    }

    public boolean passesWatchlistFilters(com.rydytrader.autotrader.dto.CprLevels cpr) {
        double minPrice = riskSettings.getScanMinPrice();
        double maxPrice = riskSettings.getScanMaxPrice();
        if (minPrice > 0 && cpr.getClose() < minPrice) return false;
        if (maxPrice > 0 && cpr.getClose() > maxPrice) return false;
        // Turnover / volume / beta / cap filters removed — universe is NIFTY 50.
        return true;
    }

    /**
     * Subscribe watchlist symbols to the HSM WebSocket.
     */
    private void subscribeWatchlist(List<String> fyersSymbols) {
        // Wait up to 5s for the WS to come up. Startup orders the seeding/catch-up before
        // wsClient.connect(), and now that catch-up is fast (no-op when cache is current),
        // subscribeWatchlist can land here a few hundred ms before the WS handshake completes.
        // Brief poll keeps the call site clean and avoids the noisy 3s outer retry.
        long deadline = System.currentTimeMillis() + 5000L;
        while ((wsClient == null || !wsClient.isOpen()) && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(100); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); break; }
        }
        if (wsClient == null || !wsClient.isOpen()) {
            log.warn("[MarketData] WebSocket not connected after 5s wait, cannot subscribe watchlist");
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
     * dayOpen, openingRangeHigh/Low, SMA, ATR, and the completedCandles deque. This corrects
     * any wrong values from the unreliable live tick stream during 9:15-9:25 (Fyers-documented
     * issue — their live WS can't deliver ticks fast enough for all subscribed symbols in the
     * first 5-10 minutes).
     */
    /**
     * Compute the near-month NIFTY 50 futures symbol for a given date.
     *
     * Expiry rule: last Thursday of the month, walked back to the prior trading day if that
     * Thursday is a holiday (NSE shifts expiry to the previous business day in that case).
     *
     * Roll-over: roll on expiry day itself ({@code today >= expiry}), not after. NSE settles
     * the contract at 3:30 PM on expiry day; after settlement Fyers stops accepting subscribe
     * requests for the expiring symbol (HTTP 422). Rolling on expiry day morning sacrifices
     * intraday signal continuity for the AM session in exchange for stable subscription.
     * Volume shifts to the next-month contract heavily through expiry day anyway.
     *
     * Format matches Fyers symbol-master output: NSE:NIFTY{YY}{MMM}FUT,
     * e.g. NSE:NIFTY26MARFUT.
     */
    public String computeNearMonthNiftyFuturesSymbol(java.time.LocalDate today) {
        java.time.LocalDate expiry = computeMonthlyExpiry(today.getYear(), today.getMonthValue());
        boolean rollToNext = !today.isBefore(expiry); // today >= expiry → roll
        java.time.YearMonth target = rollToNext
            ? java.time.YearMonth.of(today.getYear(), today.getMonthValue()).plusMonths(1)
            : java.time.YearMonth.of(today.getYear(), today.getMonthValue());
        String yy  = String.format("%02d", target.getYear() % 100);
        String mmm = target.getMonth().getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH).toUpperCase();
        return "NSE:NIFTY" + yy + mmm + "FUT";
    }

    /**
     * NSE monthly F&amp;O expiry: last Thursday of the month, shifted earlier if that day is a
     * holiday. Walks back one day at a time skipping weekends and holidays — handles the rare
     * case of consecutive holiday days too. Returns the actual trading day on which the
     * monthly contract expires.
     */
    private java.time.LocalDate computeMonthlyExpiry(int year, int month) {
        java.time.LocalDate d = java.time.YearMonth.of(year, month).atEndOfMonth();
        while (d.getDayOfWeek() != java.time.DayOfWeek.THURSDAY) {
            d = d.minusDays(1);
        }
        // Walk back while the day is a non-trading day (weekend or holiday).
        // marketHolidayService.isHoliday() may return true for both weekends and holidays
        // depending on its impl; check explicitly for safety.
        while (true) {
            boolean weekend = d.getDayOfWeek() == java.time.DayOfWeek.SATURDAY
                           || d.getDayOfWeek() == java.time.DayOfWeek.SUNDAY;
            boolean holiday = marketHolidayService != null && marketHolidayService.isHoliday(d);
            if (!weekend && !holiday) break;
            d = d.minusDays(1);
        }
        return d;
    }

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

            // Skip when started after market close — there are no live-tick-built first candles
            // to correct. Saves the redundant Fyers re-fetch (was ~4s on watchlist subset) when
            // the bot is restarted in the evening for the next trading day.
            int nowMin = now.getHour() * 60 + now.getMinute();
            if (nowMin > MarketHolidayService.MARKET_CLOSE_MINUTE) {
                lastOpeningRefreshDate = today;  // mark done so we don't keep re-checking
                return;
            }

            // Fire the refresh
            List<String> watchlist = buildWatchlist();
            if (watchlist.isEmpty()) {
                log.warn("[OpeningRefresh] Empty watchlist, skipping");
                return;
            }
            List<String> withIndex = new ArrayList<>(watchlist);
            if (!withIndex.contains("NSE:NIFTY50-INDEX")) withIndex.add("NSE:NIFTY50-INDEX");
            String niftyFutRefresh = computeNearMonthNiftyFuturesSymbol(
                java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")));
            if (!withIndex.contains(niftyFutRefresh)) withIndex.add(niftyFutRefresh);

            eventService.log("[INFO] Opening refresh triggered — refetching today's candles from Fyers /data/history for "
                + withIndex.size() + " symbols (fixes wrong live-tick-built first candles)");

            // Reuse existing seeding path — fetchAtrForSymbols re-fetches history,
            // re-computes ATR, re-seeds candleAggregator.completedCandles (today-only filter
            // picks the correct 9:15-9:20 bar from history), and re-seeds SMA from the raw list.
            // Also re-writes firstCandleClose, dayOpen, OR via seedCandles' direct put calls.
            // forceFetch=true bypasses the catch-up's "skip when current" optimization — the
            // whole point of the opening refresh is to overwrite live-tick-built bars even if
            // the cache claims they're up-to-date.
            atrService.fetchAtrForSymbols(withIndex, true);

            lastOpeningRefreshDate = today;
            eventService.log("[SUCCESS] Opening refresh complete — firstCandleClose, dayOpen, OR, SMA, ATR all re-seeded from authoritative Fyers history");

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

    /** Rebuild watchlist from current settings without full re-init. Universe is fixed
     *  (NIFTY 50 stocks seeded once at startup via initScanner) so rebuild only re-applies
     *  filters and prunes — no reseeding needed. Every symbol in the rebuilt watchlist was
     *  already seeded on startup. */
    public int rebuildWatchlist() {
        List<String> watchlist = buildWatchlist();
        if (watchlist.isEmpty()) {
            log.warn("[MarketData] Rebuild: no watchlist symbols after filter");
            return 0;
        }
        breakoutScanner.setWatchlistSymbols(watchlist);
        List<String> withIndex = new ArrayList<>(watchlist);
        if (!withIndex.contains("NSE:NIFTY50-INDEX")) withIndex.add("NSE:NIFTY50-INDEX");
        String niftyFutRebuild = computeNearMonthNiftyFuturesSymbol(
            java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")));
        if (!withIndex.contains(niftyFutRebuild)) withIndex.add(niftyFutRebuild);
        // Subscribe newly-filtered stocks to WS (subscribeWatchlist is idempotent for already-
        // subscribed symbols). No re-seeding of indicators needed — startup pre-seeded all 50
        // NIFTY stocks and we never prune them. Filter changes are just a WS-subscription +
        // scanner-target update.
        // Subscribe full universe (all 50 NIFTY) so breadth and non-filtered LTPs stay live.
        subscribeWatchlist(buildSeedUniverse(withIndex));
        log.info("[MarketData] Watchlist rebuilt: {} symbols (filters updated)", watchlist.size());
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

    /**
     * Returns the currently-forming 1h HTF candle for a symbol, or null if none yet (e.g. very
     * first ticks of a new 1h bucket, or pre-market). Used by the HTF Candle direction filter
     * in BreakoutScanner — read-only snapshot of open/high/low/close.
     */
    public CandleAggregator.CandleBar getInProgressHtfCandle(String fyersSymbol) {
        return htfAggregator != null ? htfAggregator.getCurrentCandle(fyersSymbol) : null;
    }

    /** Get live LTP for a symbol. Returns 0 if no tick data available. */
    public double getLtp(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null || tick.getLtp() <= 0) return 0;
        // Stale-day guard: yesterday's cached LTP is not today's "last value"
        String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString();
        if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        return tick.getLtp();
    }

    /** Get today's change% for a symbol — same source as the scrolling ticker (currentTicks).
     *  Survives across the whole bot lifetime; used as a fallback when CandleAggregator's
     *  per-tick latestChangePct is empty (e.g. after market close + restart).
     *
     *  Stale-day guard: if the last tick on this symbol was from a previous trading day
     *  (bot kept running overnight, no new-day tick has landed yet), return 0 so the card
     *  doesn't show yesterday's session move on a fresh trading day. The cached changePercent
     *  self-corrects on the first new-day tick. */
    public double getChangePercent(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null) return 0;
        String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString();
        if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        return tick.getChangePercent();
    }

    /** Absolute change vs prev close in points. Same stale-day guard as getChangePercent. */
    public double getChange(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null) return 0;
        String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString();
        if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        return tick.getChange();
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
