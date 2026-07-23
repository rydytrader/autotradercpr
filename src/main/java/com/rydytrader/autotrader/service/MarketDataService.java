package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.controller.MarketTickerController;
import com.rydytrader.autotrader.dto.TickData;
import com.rydytrader.autotrader.manager.PositionManager;
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
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;

/**
 * Lean market data service for the options-selling bot.
 *
 * <p>Single responsibility: maintain a live LTP feed via the Fyers HSM WebSocket and broadcast
 * tick updates to browser SSE clients. No candle aggregation, no scanner orchestration, no
 * per-stock indicators — those services were removed in the strip-to-options refactor.
 *
 * <p>Subscription set on the WebSocket = {@link MarketTickerController#getBaseSymbols()}
 * (NIFTY + sectoral indices for the ticker bar) ∪ open position symbols ∪ ad-hoc subscriptions
 * (e.g. straddle option legs registered via {@link #subscribeAdditional(java.util.Collection)}).
 *
 * <p>Reconnect: exponential backoff capped at 60s. Pings every 10s while open.
 */
@Service
public class MarketDataService implements FyersDataWebSocket.TickCallback {

    private static final Logger log = LoggerFactory.getLogger(MarketDataService.class);

    private final TokenStore       tokenStore;
    private final FyersProperties  fyersProperties;
    private final EventService     eventService;
    @SuppressWarnings("unused")
    private final RiskSettingsStore riskSettings;
    private final ObjectMapper     mapper = new ObjectMapper();

    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService   marketHolidayService;

    // Tick state
    private final ConcurrentHashMap<String, TickData> currentTicks = new ConcurrentHashMap<>();
    /** Per-symbol last exchange dissemination timestamp (epoch seconds). Populated from
     *  every RawTick where the parser could extract {@code exch_feed_time}. Used by
     *  {@link CandleAggregator} to bucket by exchange time (not local receive time), so a
     *  tick stamped 09:16:59 that arrives at 09:17:00.1 lands in the 09:15→09:17 bar. */
    private final ConcurrentHashMap<String, Long> lastExchFeedTimeSec = new ConcurrentHashMap<>();
    /** Latest exchange-dissemination timestamp (epoch seconds) observed from an
     *  ALTERNATE feed (GDFL). Tracked separately from {@link #lastExchFeedTimeSec} so
     *  the chart countdown can prefer the alternate feed's clock when available —
     *  Fyers's exch_feed_time can round up second boundaries in a way that puts our
     *  countdown 1-2 s out of sync with what the alternate vendor is showing. Updated
     *  only via {@link #pushLtpTick}. */
    private volatile long lastAltFeedExchFeedTimeSec = 0;

    /** Last exchange-dissemination timestamp (epoch seconds) observed for {@code fyersSymbol}.
     *  Returns 0 when no tick has been seen or the parser couldn't extract the timestamp
     *  (older HSM frames occasionally omit it). Callers should fall back to wall-clock
     *  time in that case. */
    public long getLastExchFeedTime(String fyersSymbol) {
        if (fyersSymbol == null) return 0;
        Long v = lastExchFeedTimeSec.get(fyersSymbol);
        return v == null ? 0 : v;
    }

    /** Latest exchange-dissemination timestamp (epoch seconds) — the best available
     *  "exchange now" reference on the server side. Used by the chart page to run its
     *  2-min bar countdown on exchange time instead of the local wall clock. Prefers
     *  the alternate feed's clock (GDFL {@code ServerTime}) when available — that's
     *  the same tape TradingView aligns to on the option side. Falls back to the max
     *  across Fyers-tracked symbols when no alternate feed has landed a tick yet.
     *  Returns 0 if no ticks have arrived from either feed. */
    public long getLatestExchFeedTimeSec() {
        if (lastAltFeedExchFeedTimeSec > 0) return lastAltFeedExchFeedTimeSec;
        long max = 0;
        for (Long v : lastExchFeedTimeSec.values()) {
            if (v != null && v > max) max = v;
        }
        return max;
    }

    // SSE emitters
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    // OI listeners — side channel for open interest ticks on option symbols. Non-blocking,
    // fanned out to each registered consumer on every option tick that carries a positive
    // OI value. Used by OptionOiTracker to maintain cumulative-since-baseline aggregates.
    /** Raw open interest event. {@code exchFeedTimeSec} is the exchange dissemination
     *  time in epoch seconds (0 when the parser couldn't extract it).
     *  {@code oiChange} is the exchange-published incremental OI change carried on the
     *  tick — currently populated only by the GDFL feed ({@code OpenInterestChange}
     *  field on {@code RealtimeResult}); Fyers ticks pass 0. Not consumed today —
     *  {@code OptionOiTracker} still computes its own delta against baseline — kept for
     *  future filters (e.g. instantaneous OI surge detection). */
    public record OiTick(String fyersSymbol, long oi, long exchFeedTimeSec, long oiChange) {}
    private final CopyOnWriteArrayList<java.util.function.Consumer<OiTick>> oiListeners = new CopyOnWriteArrayList<>();
    /** Registers a listener that receives every option-symbol OI tick. Callers must not
     *  block — listeners run inline on the WebSocket callback thread. */
    public void addOiListener(java.util.function.Consumer<OiTick> l) {
        if (l != null) oiListeners.add(l);
    }

    /** Raw LTP tick — fanned out on every WS snapshot that carries a positive LTP.
     *  {@code atp} is the session VWAP (Fyers' avg_trade_price), 0 for index symbols and
     *  pre-first-tick option symbols. {@code exchFeedTimeSec} is the Fyers dissemination
     *  time in epoch seconds. {@code lastTradedTimeSec} is the exchange's own trade
     *  timestamp — prefer this for bucket boundary decisions (closer to TradingView).
     *  Either or both may be 0 if the parser couldn't extract them; consumers should
     *  fall back {@code LTT → EFT → wall-clock} in that order. */
    public record LtpTick(String fyersSymbol, double ltp, double atp,
                          long exchFeedTimeSec, long lastTradedTimeSec) {}
    private final CopyOnWriteArrayList<java.util.function.Consumer<LtpTick>> ltpListeners = new CopyOnWriteArrayList<>();
    /** Registers a listener that receives every LTP tick — this is the pub-sub sibling of
     *  {@link #addOiListener}. Use it instead of polling {@link #getLtp} on a scheduler
     *  when you need to see every one of Fyers' ~4 snapshots/sec (poll-based sampling
     *  at 1 Hz drops ~3/4 of them). Callers must not block — listeners run inline on
     *  the WebSocket callback thread. */
    public void addLtpListener(java.util.function.Consumer<LtpTick> l) {
        if (l != null) ltpListeners.add(l);
    }

    /** Symbols for which an alternate feed (typically GDFL) owns the tick stream — Fyers
     *  ticks for these symbols are dropped entirely at {@link #onTick} ingress (no
     *  currentTicks update, no LTP fan-out, no exchFeedTime write). Registered by
     *  {@link com.rydytrader.autotrader.gdfl.GdflService} on successful SubscribeRealtime;
     *  cleared on day rollover so tomorrow's ATM (which may differ) doesn't inherit
     *  yesterday's ownership. */
    private final Set<String> altFeedOwnedSymbols = ConcurrentHashMap.newKeySet();

    /** Marks {@code fyersSymbol} as owned by an alternate feed. Idempotent. */
    public void addAltFeedOwnedSymbol(String fyersSymbol) {
        if (fyersSymbol != null && !fyersSymbol.isBlank()) altFeedOwnedSymbols.add(fyersSymbol);
    }

    /** Releases {@code fyersSymbol} back to Fyers-fed ingress. */
    public void removeAltFeedOwnedSymbol(String fyersSymbol) {
        if (fyersSymbol != null) altFeedOwnedSymbols.remove(fyersSymbol);
    }

    /** Wipes the ownership set — called on day rollover from the alternate-feed service. */
    public void clearAltFeedOwnedSymbols() {
        altFeedOwnedSymbols.clear();
    }

    /** True when Fyers ticks for {@code fyersSymbol} should be dropped in favour of an
     *  alternate feed's stream (e.g. GDFL). */
    public boolean isAltFeedOwned(String fyersSymbol) {
        return fyersSymbol != null && altFeedOwnedSymbols.contains(fyersSymbol);
    }

    /** Fans an externally-sourced OI tick to every registered OI listener. Symmetric
     *  to {@link #pushLtpTick} — used by alternate feed clients (e.g. GDFL) so their
     *  OI updates reach {@link OptionOiTracker} through the same pub-sub chain the
     *  Fyers WS uses. {@code oiChange} is forwarded verbatim on the {@link OiTick}
     *  record for future consumers; pass 0 when the source feed doesn't publish it. */
    public void pushOiTick(String fyersSymbol, long oi, long exchFeedTimeSec, long oiChange) {
        if (fyersSymbol == null || fyersSymbol.isBlank() || oi <= 0) return;
        if (oiListeners.isEmpty()) return;
        OiTick evt = new OiTick(fyersSymbol, oi, exchFeedTimeSec, oiChange);
        for (var l : oiListeners) {
            try { l.accept(evt); } catch (Exception ignored) {}
        }
    }

    /** Fans an externally-sourced LtpTick to every registered listener. Used by
     *  alternate feed clients (e.g. {@code GdflDataWebSocket}) so ticks from a
     *  non-Fyers source flow through the same {@link CandleAggregator} pipeline as
     *  the native Fyers ticks. Also updates {@code currentTicks} and
     *  {@code lastExchFeedTimeSec} so downstream code that reads those (chart
     *  countdown, ticker, LTP getters) sees consistent state regardless of feed. */
    public void pushLtpTick(LtpTick evt) {
        if (evt == null || evt.ltp() <= 0) return;
        String fyersSymbol = evt.fyersSymbol();
        if (fyersSymbol == null || fyersSymbol.isBlank()) return;
        TickData tick = currentTicks.computeIfAbsent(fyersSymbol, k -> {
            TickData t = new TickData();
            t.setFyersSymbol(k);
            t.setShortName(deriveShortName(k));
            return t;
        });
        tick.setLtp(evt.ltp());
        if (evt.atp() > 0) tick.setVwap(evt.atp());
        String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
        tick.setLastTickDate(today);
        tick.recalcChange();
        if (evt.exchFeedTimeSec() > 0) {
            lastExchFeedTimeSec.put(fyersSymbol, evt.exchFeedTimeSec());
            // Advance the alt-feed clock — chart countdown prefers this over the
            // Fyers-tracked map because GDFL's ServerTime is what TradingView-style
            // clients on the option side align to.
            if (evt.exchFeedTimeSec() > lastAltFeedExchFeedTimeSec) {
                lastAltFeedExchFeedTimeSec = evt.exchFeedTimeSec();
            }
        }
        dirty = true;
        for (var l : ltpListeners) {
            try { l.accept(evt); } catch (Exception ignored) {}
        }
    }

    // WS state
    private volatile FyersDataWebSocket wsClient;
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile boolean dirty   = false;
    private volatile int reconnectAttempts = 0;
    /** Consecutive HTTP 401/403 responses from {@code /data/symbol-token}. After
     *  {@link #AUTH_FAIL_THRESHOLD}, the in-memory token is cleared and reconnects pause
     *  until the operator logs in again — prevents the daily-token-expiry 429 storm where
     *  the bot used to retry the bootstrap every 60 s indefinitely. */
    private volatile int consecutive401s = 0;
    private static final int AUTH_FAIL_THRESHOLD = 2;
    private volatile String lastConnectTime = "";
    private volatile String lastDisconnectTime = "";
    private volatile int reconnectCountToday = 0;
    private static final int CHANNEL_NUM = 11;

    // HSM token mappings
    private final Map<String, String> hsmToFyersSymbol = new ConcurrentHashMap<>();
    private final Map<String, String> fyersToHsmToken  = new ConcurrentHashMap<>();
    private final Set<String> subscribedHsmTokens     = ConcurrentHashMap.newKeySet();

    // Ad-hoc subscription set (e.g. open option legs from the rolling straddle)
    private final Set<String> adHocSymbols = ConcurrentHashMap.newKeySet();

    // Equity-only flags kept as no-ops for API compatibility with callers we haven't
    // yet stripped (PollingService etc.). Returns false / does nothing.
    private final Set<String> trailedSymbols = ConcurrentHashMap.newKeySet();

    // Exchange segment + index name dictionaries (subset retained for option + index symbols)
    private static final Map<String, String> EXCH_SEG_DICT = Map.of(
        "1010", "nse_cm",
        "1011", "nse_fo",
        "1120", "mcx_fo",
        "1210", "bse_cm",
        "1012", "cde_fo",
        "1211", "bse_fo"
    );

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
        Map.entry("NSE:NIFTYHEALTHCARE-INDEX",  "NIFTY HEALTHCARE"),
        Map.entry("NSE:NIFTYREALTY-INDEX",      "Nifty Realty"),
        Map.entry("NSE:NIFTYMEDIA-INDEX",       "Nifty Media"),
        Map.entry("NSE:NIFTYOILANDGAS-INDEX",   "NIFTY OIL AND GAS"),
        Map.entry("NSE:NIFTYCONSRDURBL-INDEX",  "NIFTY CONSR DURBL"),
        Map.entry("NSE:NIFTYSERVSECTOR-INDEX",  "Nifty Serv Sector"),
        Map.entry("NSE:NIFTYCONSUMPTION-INDEX", "Nifty Consumption"),
        Map.entry("NSE:NIFTYINFRA-INDEX",       "Nifty Infra"),
        Map.entry("NSE:NIFTYCOMMODITIES-INDEX", "Nifty Commodities"),
        Map.entry("NSE:MIDCPNIFTY-INDEX",       "NIFTY MID SELECT"),
        Map.entry("NSE:NIFTYNEXT50-INDEX",      "Nifty Next 50"),
        Map.entry("NSE:INDIAVIX-INDEX",         "India VIX")
    );

    public MarketDataService(TokenStore tokenStore,
                              FyersProperties fyersProperties,
                              EventService eventService,
                              RiskSettingsStore riskSettings) {
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
        this.eventService    = eventService;
        this.riskSettings    = riskSettings;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public synchronized void start() {
        if (running) stop();
        running = true;
        reconnectAttempts = 0;
        consecutive401s  = 0;
        scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(this::flushSse, 500, 500, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::sendKeepalive, 15, 15, TimeUnit.SECONDS);
        startLiveWebSocket();
        log.info("[MarketData] Started");
    }

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
        log.info("[MarketData] Stopped");
    }

    private void startLiveWebSocket() {
        scheduler.submit(() -> {
            try {
                String accessToken = tokenStore.getAccessToken();
                if (accessToken == null || accessToken.isEmpty()) {
                    log.info("[MarketData] No access token — waiting for login");
                    if (eventService != null) eventService.log("[WS] No access token — please re-login");
                    scheduleReconnect();
                    return;
                }
                String hsmKey = extractHsmKey(accessToken);
                if (hsmKey == null) {
                    log.info("[MarketData] Failed to extract hsm_key");
                    scheduleReconnect();
                    return;
                }
                List<String> fyersSymbols = buildSymbolList();
                resolveSymbolTokens(fyersSymbols, accessToken);
                if (fyersToHsmToken.isEmpty()) {
                    log.info("[MarketData] No HSM tokens resolved");
                    scheduleReconnect();
                    return;
                }
                List<String> hsmTokens = new ArrayList<>(fyersToHsmToken.values());
                subscribedHsmTokens.addAll(hsmTokens);

                wsClient = new FyersDataWebSocket(hsmKey, hsmTokens, hsmToFyersSymbol,
                    false, CHANNEL_NUM, this);
                wsClient.setEventLogSink(msg -> { if (eventService != null) eventService.log(msg); });
                boolean connected = wsClient.connectBlocking(15, TimeUnit.SECONDS);
                if (!connected) {
                    log.warn("[MarketData] WS connect timeout — retrying");
                    try { wsClient.close(); } catch (Exception ignored) {}
                    wsClient = null;
                    scheduleReconnect();
                    return;
                }
                scheduler.scheduleAtFixedRate(() -> {
                    if (wsClient != null && wsClient.isOpen()) wsClient.sendPing();
                }, 10, 10, TimeUnit.SECONDS);
            } catch (AuthExpiredException ae) {
                handleAuthFailure(ae);
            } catch (Exception e) {
                log.error("[MarketData] WS connect error: {}", e.getMessage());
                scheduleReconnect();
            }
        });
    }

    /** Track a 401/403 from the symbol-token endpoint. After {@link #AUTH_FAIL_THRESHOLD}
     *  consecutive failures, clear the in-memory token and stop scheduling reconnects.
     *  The operator's next Fyers login calls {@link #start()} which resets the counter
     *  and resumes connection. A single spurious 401 (Fyers rate-spike edge case) doesn't
     *  lock the operator out — only repeated failures do. */
    private void handleAuthFailure(AuthExpiredException ae) {
        consecutive401s++;
        log.warn("[MarketData] Auth failure {}/{}: {}", consecutive401s, AUTH_FAIL_THRESHOLD, ae.getMessage());
        if (eventService != null) {
            eventService.log("[WARNING] [WS] Fyers auth failure " + consecutive401s + "/" + AUTH_FAIL_THRESHOLD
                + (consecutive401s < AUTH_FAIL_THRESHOLD ? " — retrying" : ""));
        }
        if (consecutive401s >= AUTH_FAIL_THRESHOLD) {
            log.error("[MarketData] Repeated auth failures — clearing in-memory token. Re-login required.");
            if (eventService != null) {
                eventService.log("[ERROR] [WS] Fyers token expired — reconnect paused. Please re-login.");
            }
            tokenStore.setAccessToken("");
            // No scheduleReconnect — the token is gone; the scheduleReconnect guard will
            // refuse to schedule any further bootstrap attempt until a new token lands.
            return;
        }
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (!running) return;
        // Don't hammer Fyers with the bootstrap loop when there's no token to authenticate
        // with — caused the daily 429 storm. Resume the moment a fresh token is set via
        // /fyers/callback (which calls start() and re-arms the counters).
        if (!tokenStore.isTokenAvailable()) {
            log.info("[MarketData] Reconnect paused — no access token. Waiting for re-login.");
            return;
        }
        reconnectAttempts++;
        long delay = Math.min(2L * (1L << Math.min(reconnectAttempts, 5)), 60);
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.schedule(this::startLiveWebSocket, delay, TimeUnit.SECONDS);
        }
    }

    /** Marker thrown by {@code resolveSymbolTokensBatch} when Fyers returns HTTP 401/403,
     *  so the connect bootstrap can distinguish auth expiry from a transient connect failure. */
    private static class AuthExpiredException extends RuntimeException {
        AuthExpiredException(String msg) { super(msg); }
    }

    // ── TickCallback ──────────────────────────────────────────────────────────

    @Override
    public void onTick(HsmBinaryParser.RawTick raw) {
        String fyersSymbol = raw.fyersSymbol != null ? raw.fyersSymbol : hsmToFyersSymbol.get(raw.hsmToken);
        if (fyersSymbol == null) return;
        // Alternate-feed ownership — GDFL is the SOLE source for these symbols. Drop
        // Fyers ticks entirely (LTP, OHLC, OI, everything). GDFL supplies OI via
        // pushOiTick so OptionOiTracker still gets per-strike updates for the ATM legs.
        // The non-ATM strikes in the ±7 OI window remain Fyers-fed through this method
        // as they're never in altFeedOwnedSymbols. Each strike's OI is thus sourced by
        // exactly ONE vendor, avoiding cross-feed timestamp drift inside the tracker.
        if (altFeedOwnedSymbols.contains(fyersSymbol)) return;
        String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
        TickData tick = currentTicks.computeIfAbsent(fyersSymbol, k -> {
            TickData t = new TickData();
            t.setFyersSymbol(k);
            t.setShortName(deriveShortName(k));
            return t;
        });
        if (raw.ltp > 0) tick.setLtp(raw.ltp);
        if (raw.open > 0) tick.setOpen(raw.open);
        if (raw.high > 0) tick.setHigh(raw.high);
        if (raw.low > 0) tick.setLow(raw.low);
        if (raw.prevClose > 0) tick.setPrevClose(raw.prevClose);
        // Fyers' avg_trade_price = session VWAP since market open. Stored as-is for the
        // UI's per-strike VWAP display; no client-side accumulation needed.
        if (raw.atp > 0) tick.setVwap(raw.atp);
        tick.setLastTickDate(today);
        tick.recalcChange();
        if (raw.exchFeedTime > 0) lastExchFeedTimeSec.put(fyersSymbol, raw.exchFeedTime);
        dirty = true;

        // LTP fan-out — CandleAggregator (and other consumers) receive every WS snapshot
        // rather than sampling from currentTicks on a 1 Hz poll. Fyers throttles ~4
        // snapshots/sec per symbol, so this path catches all 4 vs the poll's 1.
        if (raw.ltp > 0 && !ltpListeners.isEmpty()) {
            LtpTick evt = new LtpTick(fyersSymbol, raw.ltp, raw.atp,
                raw.exchFeedTime, raw.lastTradedTime);
            for (var l : ltpListeners) {
                try { l.accept(evt); } catch (Exception ignored) {}
            }
        }

        // OI side channel — options only, positive OI only. Fan out to registered
        // listeners so OptionOiTracker can maintain cumulative-since-baseline aggregates
        // without polluting the TickData path used by the scrolling ticker. Fyers's HSM
        // frame doesn't carry an OI-change field, so oiChange is 0 here — populated by
        // the GDFL feed only.
        if (raw.oi > 0 && !oiListeners.isEmpty() && isOptionSymbol(fyersSymbol)) {
            OiTick evt = new OiTick(fyersSymbol, raw.oi, raw.exchFeedTime, 0L);
            for (var l : oiListeners) {
                try { l.accept(evt); } catch (Exception ignored) {}
            }
        }
    }

    @Override
    public void onConnected() {
        reconnectAttempts = 0;
        consecutive401s  = 0;
        lastConnectTime = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        log.info("[MarketData] WebSocket connected");
        if (eventService != null) eventService.log("[WS] Data WebSocket connected");
    }

    @Override
    public void onDisconnected(String reason) {
        lastDisconnectTime = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        reconnectCountToday++;
        log.info("[MarketData] WebSocket disconnected: {}", reason);
        if (eventService != null) eventService.log("[WS] Data WebSocket disconnected: " + reason);
        if (running) scheduleReconnect();
    }

    @Override
    public void onAuthResult(boolean success, int ackCount) {
        log.info("[MarketData] WS auth: success={} acks={}", success, ackCount);
    }

    // ── SSE ───────────────────────────────────────────────────────────────────

    public void addEmitter(SseEmitter emitter) {
        emitters.add(emitter);
    }

    public void removeEmitter(SseEmitter emitter) {
        emitters.remove(emitter);
    }

    public void sendSnapshot(SseEmitter emitter) {
        try {
            List<Map<String, Object>> payload = buildTickerPayload();
            if (!payload.isEmpty()) {
                emitter.send(SseEmitter.event().name("ticker").data(mapper.writeValueAsString(payload)));
            }
        } catch (Exception e) {
            removeEmitter(emitter);
        }
    }

    private void flushSse() {
        if (!dirty || emitters.isEmpty()) return;
        dirty = false;
        List<Map<String, Object>> tickerPayload = buildTickerPayload();
        String tickerJson = null;
        if (!tickerPayload.isEmpty()) {
            try { tickerJson = mapper.writeValueAsString(tickerPayload); } catch (Exception ignored) {}
        }
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                if (tickerJson != null) emitter.send(SseEmitter.event().name("ticker").data(tickerJson));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        for (SseEmitter d : dead) {
            try { d.complete(); } catch (Exception ignored) {}
            removeEmitter(d);
        }
    }

    private List<Map<String, Object>> buildTickerPayload() {
        List<Map<String, Object>> out = new ArrayList<>();
        Set<String> posSymbols = PositionManager.getAllSymbols();
        for (TickData tick : currentTicks.values()) {
            if (tick.getLtp() <= 0) continue;
            // Hide option legs from the scrolling ticker — those are subscribed for OI bias
            // tracking (and for any live position legs) but the ticker is meant to show only
            // indices + the NIFTY-style base set. Open positions on options still get visibility
            // through the Live Positions card.
            if (isOptionSymbol(tick.getFyersSymbol())) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            // Field names match ticker.js renderTicker expectations + the REST /api/market-ticker
            // payload: symbol = short display name, lp = last price, ch = absolute change,
            // chp = change %.
            m.put("symbol", tick.getShortName() != null && !tick.getShortName().isEmpty()
                ? tick.getShortName() : tick.getFyersSymbol());
            // Full Fyers symbol — needed by the manual options terminal modal which subscribes
            // to specific CE/PE legs and must match by exact symbol, not display short-name.
            m.put("fyers", tick.getFyersSymbol());
            m.put("lp",  Math.round(tick.getLtp() * 100.0) / 100.0);
            m.put("ch",  Math.round(tick.getChange() * 100.0) / 100.0);
            m.put("chp", Math.round(tick.getChangePercent() * 100.0) / 100.0);
            m.put("position", posSymbols.contains(tick.getFyersSymbol()));
            out.add(m);
        }
        return out;
    }

    private void sendKeepalive() {
        if (emitters.isEmpty()) return;
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("keepalive").data("ping"));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        for (SseEmitter d : dead) {
            try { d.complete(); } catch (Exception ignored) {}
            removeEmitter(d);
        }
    }

    // ── Subscriptions ─────────────────────────────────────────────────────────

    /** Build the desired subscription symbol set. */
    private List<String> buildSymbolList() {
        Set<String> symbols = new LinkedHashSet<>();
        for (String s : MarketTickerController.getBaseSymbols()) symbols.add(s);
        symbols.addAll(PositionManager.getAllSymbols());
        symbols.addAll(adHocSymbols);
        return new ArrayList<>(symbols);
    }

    /** Public: refresh the subscription set against the WS — call when positions change or
     *  ad-hoc subscriptions are added/removed. Idempotent. */
    public void updateSubscriptions() {
        if (!running || wsClient == null || !wsClient.isOpen()) return;
        Set<String> wantedFyers = new LinkedHashSet<>(buildSymbolList());
        List<String> toSubscribe = new ArrayList<>();
        for (String fyers : wantedFyers) {
            String hsm = fyersToHsmToken.get(fyers);
            if (hsm != null && !subscribedHsmTokens.contains(hsm)) {
                toSubscribe.add(hsm);
                subscribedHsmTokens.add(hsm);
            }
        }
        List<String> unresolved = new ArrayList<>();
        for (String fyers : wantedFyers) {
            if (!fyersToHsmToken.containsKey(fyers)) unresolved.add(fyers);
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
            } catch (AuthExpiredException ae) {
                // Token died mid-session (rare — usually expires only at day boundary). Route
                // through the same handler the bootstrap uses so consecutive401s + token clear
                // + re-login pause are all one code path.
                handleAuthFailure(ae);
            } catch (Exception e) {
                log.error("[MarketData] Failed to resolve new symbols: {}", e.getMessage());
            }
        }
        if (!toSubscribe.isEmpty()) {
            wsClient.subscribeSymbols(toSubscribe);
            log.info("[MarketData] Subscribed {} new symbols", toSubscribe.size());
        }
    }

    /** Add symbols to the ad-hoc subscription set (e.g. straddle CE/PE legs at entry) and
     *  immediately push them to the WS. Symbols stay subscribed until {@link #unsubscribeAdditional}. */
    public void subscribeAdditional(Collection<String> symbols) {
        if (symbols == null || symbols.isEmpty()) return;
        adHocSymbols.addAll(symbols);
        updateSubscriptions();
    }

    /** Remove symbols from the ad-hoc subscription set. (HSM unsubscription is deferred —
     *  symbols stay on the wire until the next reconnect; harmless for short-lived option legs.) */
    public void unsubscribeAdditional(Collection<String> symbols) {
        if (symbols == null || symbols.isEmpty()) return;
        adHocSymbols.removeAll(symbols);
    }

    // ── HSM token resolution (copied from the old service) ───────────────────

    private void resolveSymbolTokens(List<String> fyersSymbols, String accessToken) throws Exception {
        if (fyersSymbols == null || fyersSymbols.isEmpty()) return;
        String token = accessToken;
        if (token != null && token.contains(":")) token = token.split(":")[1];
        final int BATCH_SIZE = 50;
        int totalResolved = 0;
        for (int i = 0; i < fyersSymbols.size(); i += BATCH_SIZE) {
            List<String> batch = fyersSymbols.subList(i, Math.min(i + BATCH_SIZE, fyersSymbols.size()));
            totalResolved += resolveSymbolTokensBatch(batch, token);
        }
        log.info("[MarketData] Resolved {} symbol tokens across {} batches (universe size {})",
            totalResolved, (fyersSymbols.size() + BATCH_SIZE - 1) / BATCH_SIZE, fyersSymbols.size());
    }

    private int resolveSymbolTokensBatch(List<String> fyersSymbols, String token) throws Exception {
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
        int responseCode = conn.getResponseCode();
        InputStream is = responseCode < 400 ? conn.getInputStream() : conn.getErrorStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sb.append(line);
        br.close();
        // 401/403 = token rejected by Fyers. Propagate up so the connect bootstrap can stop
        // hammering — without this, the bot retried every 60 s after daily token expiry and
        // Fyers' rate-limiter would respond with continuous 429s.
        if (responseCode == 401 || responseCode == 403) {
            throw new AuthExpiredException("HTTP " + responseCode + " from /data/symbol-token: " + sb);
        }
        int batchResolved = 0;
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
                    batchResolved++;
                }
            }
        }
        return batchResolved;
    }

    private String convertToHsmToken(String fyersSymbol, String fyToken) {
        if (fyToken == null || fyToken.length() < 10) return null;
        String segment = EXCH_SEG_DICT.get(fyToken.substring(0, 4));
        if (segment == null) return null;
        if (fyersSymbol.endsWith("-INDEX")) {
            String indexName = INDEX_TOKEN_MAP.get(fyersSymbol);
            if (indexName != null) return "if|" + segment + "|" + indexName;
            String parts = fyersSymbol.split(":")[1].split("-")[0];
            return "if|" + segment + "|" + parts;
        }
        return "sf|" + segment + "|" + fyToken.substring(10);
    }

    private String extractHsmKey(String accessToken) {
        try {
            String token = accessToken;
            if (token.contains(":")) token = token.split(":")[1];
            String[] parts = token.split("\\.");
            if (parts.length < 2) return null;
            String payload = parts[1];
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

    private String deriveShortName(String fyersSymbol) {
        try {
            String afterColon = fyersSymbol.split(":")[1];
            return afterColon.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        } catch (Exception e) {
            return fyersSymbol;
        }
    }

    /** True when the Fyers symbol is an option leg (CE/PE on any underlying), false for
     *  equity, index, futures, mutual funds. Used to exclude OI-tracker subscriptions and
     *  position legs from the scrolling ticker which only shows indices.
     *  Accepts both option-symbol formats Fyers has used:
     *    A) NSE:NIFTY2660223850CE / PE          (current — CE/PE suffix after digits)
     *    B) NSE:NIFTY25527C24350 / P24350       (older — C/P letter followed by strike) */
    private static boolean isOptionSymbol(String fyersSymbol) {
        if (fyersSymbol == null || fyersSymbol.isEmpty()) return false;
        String upper = fyersSymbol.toUpperCase();
        if (upper.endsWith("-EQ") || upper.endsWith("-INDEX") || upper.endsWith("-MF")
            || upper.endsWith("-BE") || upper.endsWith("-BL") || upper.endsWith("-SM")
            || upper.endsWith("FUT")) return false;
        return upper.matches(".*[CP]\\d+$")        // format B: ends in C/P + digits
            || upper.matches(".*\\d+(CE|PE)$");    // format A: ends in digits + CE/PE
    }

    // ── Public accessors ──────────────────────────────────────────────────────

    public double getLtp(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null || tick.getLtp() <= 0) return 0;
        String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
        if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        return tick.getLtp();
    }

    /** Display-only LTP: returns the last known LTP regardless of session date. Use for UI
     *  fields where stale-but-visible beats blank (e.g. Hero NIFTY pre-market). Trading
     *  logic must continue to use {@link #getLtp} which guards on today's session. */
    public double getDisplayLtp(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return tick == null ? 0 : tick.getLtp();
    }
    public double getDisplayChange(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return tick == null ? 0 : tick.getChange();
    }
    public double getDisplayChangePct(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return tick == null ? 0 : tick.getChangePercent();
    }

    /** Session VWAP for a symbol — Fyers' {@code avg_trade_price} field accumulated
     *  by NSE from market open. Returns 0 when no full-mode tick has populated it
     *  yet, or when the cached tick is from a prior session. */
    public double getVwap(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null || tick.getVwap() <= 0) return 0;
        String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
        if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        return tick.getVwap();
    }

    /** Seed prev-close + LTP for a symbol the WS hasn't ticked yet. Used by strategies to
     *  populate change/% display on fresh option subscriptions where the WS often skips
     *  prev_close in its first tick. */
    public void seedTickData(String fyersSymbol, double ltp, double prevClose) {
        if (fyersSymbol == null || fyersSymbol.isEmpty()) return;
        TickData tick = currentTicks.computeIfAbsent(fyersSymbol, k -> {
            TickData t = new TickData();
            t.setFyersSymbol(k);
            t.setShortName(deriveShortName(k));
            return t;
        });
        if (ltp > 0) tick.setLtp(ltp);
        if (prevClose > 0) tick.setPrevClose(prevClose);
        tick.setLastTickDate(java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString());
        tick.recalcChange();
        dirty = true;
    }

    public double getChangePercent(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null) return 0;
        boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();
        if (tradingDay) {
            String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
            if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        }
        return tick.getChangePercent();
    }

    public double getChange(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null) return 0;
        boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();
        if (tradingDay) {
            String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
            if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        }
        return tick.getChange();
    }

    public double getDayOpen(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getOpen() > 0) ? tick.getOpen() : 0;
    }

    public double getDayHigh(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getHigh() > 0) ? tick.getHigh() : 0;
    }

    public double getDayLow(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getLow() > 0) ? tick.getLow() : 0;
    }

    public boolean isConnected()    { return wsClient != null && wsClient.isOpen(); }
    public boolean isReconnecting() { return running && !isConnected() && reconnectAttempts > 0 && tokenStore.isTokenAvailable(); }
    public boolean isConnecting()   { return running && !isConnected() && tokenStore.isTokenAvailable(); }
    /** True while the reconnect loop is intentionally paused waiting for a fresh access
     *  token (the {@link #scheduleReconnect} bailout when {@code !tokenStore.isTokenAvailable()}).
     *  Drives the "WAITING FOR LOGIN" status indicator on the UI. */
    public boolean isPaused()       { return running && !isConnected() && !tokenStore.isTokenAvailable(); }
    public int     getEmitterCount() { return emitters.size(); }
    public String  getLastConnectTime()    { return lastConnectTime; }
    public String  getLastDisconnectTime() { return lastDisconnectTime; }
    public int     getReconnectCountToday() { return reconnectCountToday; }
    public int     getTickCount() { return currentTicks.size(); }

    // ── Legacy no-ops kept for callers we haven't stripped yet ────────────────

    /** Equity trailing-SL flag — always false now. */
    public boolean isTrailed(String fyersSymbol) {
        return trailedSymbols.contains(fyersSymbol);
    }

    /** Equity trailing-SL flag — no-op now. */
    public void clearTrailedFlag(String fyersSymbol) {
        trailedSymbols.remove(fyersSymbol);
    }
}
