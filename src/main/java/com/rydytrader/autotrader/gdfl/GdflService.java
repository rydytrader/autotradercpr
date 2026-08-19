package com.rydytrader.autotrader.gdfl;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.strategy.OptionBuying;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Spring lifecycle owner for the GDFL WebSocket client.
 *
 * <p><b>Dynamic subscription flow</b> — the operator does NOT hand-enter today's ATM
 * symbols. Instead:
 * <ol>
 *   <li>{@link #boot()} opens the WS connection at Spring startup (if
 *       {@code gdfl.enabled=true}) and completes the {@code Authenticate} handshake.</li>
 *   <li>An in-process poller runs every {@code gdfl.atmPollIntervalSeconds} seconds.
 *       As soon as {@link OptionBuying#getCeSymbol()} + {@link OptionBuying#getPeSymbol()} are
 *       non-blank (i.e. OptionBuying has resolved the day's ATM at ~09:18 IST — either from
 *       its own first-bar close or from a mid-day operator override), the poller
 *       converts the two Fyers-format symbols to GDFL contractwise identifiers via
 *       {@link GdflSymbolMapper}, sends {@code SubscribeRealtime} for each, and stops
 *       polling for the day.</li>
 *   <li>Ticks stream in as {@code RealtimeResult} frames → {@link #onGdflTick} maps
 *       the GDFL identifier back to the Fyers symbol and hands off to
 *       {@link MarketDataService#pushLtpTick}.</li>
 *   <li>Day rollover: the poller detects it, clears the mapper, and re-arms for
 *       tomorrow's ATM.</li>
 * </ol>
 *
 * <p>Requires the operator to keep {@code gdfl.expiry-date} up to date weekly (format
 * {@code DDMMMYY}, e.g. {@code 28JUL26}). Everything else is automatic.
 */
@Service
@EnableConfigurationProperties(GdflProperties.class)
public class GdflService {

    private static final Logger log = LoggerFactory.getLogger(GdflService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final GdflProperties     props;
    private final GdflSymbolMapper   mapper;
    private final MarketDataService  marketDataService;
    private final CandleAggregator   candleAggregator;
    private final EventService       eventService;
    private final ObjectProvider<OptionBuying> optionBuyingProvider;
    private final ScheduledExecutorService executor;

    private volatile GdflDataWebSocket wsClient;
    /** Which GDFL identifiers we've already sent SubscribeRealtime for TODAY. Reset at
     *  day rollover AND on every fresh connect (each new WS starts with zero
     *  live subscriptions). */
    private final Set<String> subscribedGdflSymbols = new HashSet<>();
    /** Which GDFL identifiers we've already sent SubscribeSnapshot for TODAY.
     *  Snapshot delivers canonical 1-min OHLC bars from GDFL's server (which
     *  sees every trade on the tape), pushed ~1.5-2.5s after each minute
     *  boundary. Used to correct our 1Hz-throttled tick-aggregated bars for
     *  opening-burst OHLC accuracy. Reset on day rollover. */
    private final Set<String> snapshotSubscribedGdflSymbols = new HashSet<>();
    /** Fyers symbols we've already logged a "FIRST LIVE TICK" line for. Used only
     *  for diagnostic visibility — once a symbol is in here, subsequent live ticks
     *  go through the silent hot path. Not persisted; reset on process restart
     *  (that's intentional — each restart should re-log the first live tick). */
    private final Set<String> firstLiveTickSeen = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** Day-key of the last successful subscribe pass, so day rollover clears state. */
    private volatile String subscribedDayKey = "";
    /** Flipped in {@link #shutdown} — {@link #scheduleReconnect} and
     *  {@link #connect} short-circuit once true to avoid reconnect storms during
     *  app teardown. */
    private volatile boolean shuttingDown = false;
    /** Monotonic counter bumped on every {@link #connect}. The onDisconnect
     *  callback captures the generation of the client it was attached to; when
     *  the callback fires, it checks whether that generation is still current.
     *  If not, the callback came from a client we already replaced — no
     *  reconnect is scheduled. Without this guard, closing an old leaked client
     *  during connect() would cascade into another reconnect and a fresh
     *  client-per-close spin loop. */
    private volatile int wsGeneration = 0;

    public GdflService(GdflProperties props,
                       GdflSymbolMapper mapper,
                       MarketDataService marketDataService,
                       CandleAggregator candleAggregator,
                       EventService eventService,
                       ObjectProvider<OptionBuying> optionBuyingProvider) {
        this.props             = props;
        this.mapper            = mapper;
        this.marketDataService = marketDataService;
        this.candleAggregator  = candleAggregator;
        this.eventService      = eventService;
        this.optionBuyingProvider   = optionBuyingProvider;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gdfl-lifecycle");
            t.setDaemon(true);
            return t;
        });
    }

    @PostConstruct
    public void boot() {
        if (!props.isEnabled()) {
            log.info("[Gdfl] disabled (gdfl.enabled=false) — no WS connection opened");
            return;
        }
        // Credentials come from env vars only (no defaults in application.properties)
        // so missing them is a hard startup failure — fails safe rather than
        // silently disabling GDFL and letting the strategy run without the
        // vendor it was configured to use.
        if (props.getApiKey() == null || props.getApiKey().isBlank()) {
            throw new IllegalStateException(
                "GDFL_API_KEY environment variable is not set. Either export "
                + "GDFL_API_KEY before starting the server, or set "
                + "gdfl.enabled=false in application.properties.");
        }
        if (props.getEndpoint() == null || props.getEndpoint().isBlank()) {
            throw new IllegalStateException(
                "GDFL_ENDPOINT environment variable is not set. Either export "
                + "GDFL_ENDPOINT (e.g. wss://prod.your-vendor.example:443) "
                + "before starting the server, or set gdfl.enabled=false in "
                + "application.properties.");
        }
        connect();

        // Poll for ATM resolution every configured interval. As soon as CE + PE are
        // resolved, subscribe. Idempotent — won't re-subscribe the same symbol.
        int pollSec = Math.max(1, props.getAtmPollIntervalSeconds());
        executor.scheduleWithFixedDelay(this::checkAtmAndSubscribe, pollSec, pollSec, TimeUnit.SECONDS);
    }

    /** Public health-check for UI status widgets. Values:
     *  <ul>
     *    <li>{@code DISABLED} — {@code gdfl.enabled=false}, no attempt to connect.</li>
     *    <li>{@code CONNECTED} — WS is open and authentication handshake succeeded.</li>
     *    <li>{@code CONNECTING} — {@code boot()} ran but the WS isn't authenticated yet
     *        (initial handshake in flight, or reconnect scheduled).</li>
     *  </ul> */
    public String connectionStatus() {
        if (!props.isEnabled()) return "DISABLED";
        return wsClient != null && wsClient.isAuthenticated() ? "CONNECTED" : "CONNECTING";
    }

    /** Diagnostic — sends an arbitrary GDFL message payload over the current WS.
     *  Returns {@code false} when disabled, disconnected, or the send failed.
     *  See {@link GdflDataWebSocket#sendRawMessage}. */
    public boolean sendRaw(String jsonPayload) {
        if (wsClient == null) return false;
        return wsClient.sendRawMessage(jsonPayload);
    }

    /** Diagnostic — recent GDFL frames whose MessageType wasn't a well-known
     *  ticker / OHLC / ACK type. Response frames for request-response calls
     *  (e.g. GetInstrumentsOnSearch) land here. Newest last. */
    public List<String> recentUnknownFrames() {
        if (wsClient == null) return List.of();
        return wsClient.getRecentUnknownFrames();
    }

    @PreDestroy
    public void shutdown() {
        // Set BEFORE closing the socket so the onDisconnect callback
        // (fired from onClose) short-circuits instead of scheduling a
        // pointless reconnect against a shutting-down executor.
        shuttingDown = true;
        executor.shutdownNow();
        if (wsClient != null) {
            try { wsClient.close(); } catch (Exception ignored) {}
        }
    }

    private synchronized void connect() {
        if (shuttingDown) return;
        URI endpoint;
        try { endpoint = URI.create(props.getEndpoint()); }
        catch (Exception e) {
            log.warn("[Gdfl] invalid gdfl.endpoint '{}': {}", props.getEndpoint(), e.getMessage());
            return;
        }
        // Close any prior client BEFORE creating the new one. Skipping this
        // used to leak the previous socket — the vendor eventually dropped it
        // (duplicate-connection kick or idle timeout), its onClose fired our
        // reconnect callback, and we'd cascade into a client-per-close spin.
        // The generation bump below neutralises the leaked client's callback
        // so this local close() doesn't itself trigger another connect().
        int myGeneration = ++wsGeneration;
        GdflDataWebSocket prior = wsClient;
        if (prior != null) {
            try { prior.close(); } catch (Exception ignored) {}
        }
        // Fresh WS instance — no subscriptions active on it yet. Clear the
        // dedup set so the atm-check loop re-issues SubscribeRealtime for
        // every symbol in today's OI window. Without this, subscribeOne
        // would think each symbol is "already subscribed" (from the previous
        // WS) and skip, leaving the new WS silent.
        subscribedGdflSymbols.clear();
        snapshotSubscribedGdflSymbols.clear();
        // The onDisconnect lambda captures myGeneration; when it fires, it
        // reconnects only if we haven't already replaced this client. That
        // way the old leaked client's onClose becomes a no-op instead of
        // stacking additional reconnects.
        Runnable onDisconnect = () -> {
            if (myGeneration != wsGeneration) return;
            scheduleReconnect();
        };
        // Pre-populate the initial-subscribe list with NIFTY-I so the WS fires the
        // SubscribeRealtime immediately on AuthenticateResult — no waiting for the
        // 5s checkAtmAndSubscribe poll. Also seed the reverse mapper + altFeed
        // ownership + subscribedGdflSymbols set so the poll's subscribeOne
        // recognises the symbol as already-handled and skips re-sending.
        List<String> initialSubs = new ArrayList<>();
        initialSubs.add(GdflSymbolMapper.GDFL_NIFTY_FUTURES);
        subscribedGdflSymbols.add(GdflSymbolMapper.GDFL_NIFTY_FUTURES);
        // Registers gdflToFyers mapping and marks altFeed-owned so pushLtpTick fans
        // to CandleAggregator without Fyers-side collision.
        mapper.fyersToGdfl(GdflSymbolMapper.FYERS_NIFTY_FUTURES);
        marketDataService.addAltFeedOwnedSymbol(GdflSymbolMapper.FYERS_NIFTY_FUTURES);

        wsClient = new GdflDataWebSocket(endpoint, props.getApiKey(), props.getExchange(),
            initialSubs, this::onGdflTick, this::onCanonicalMinuteBar, onDisconnect);
        wsClient.setConnectionLostTimeout(30);
        try {
            boolean connected = wsClient.connectBlocking(15, TimeUnit.SECONDS);
            if (!connected) {
                log.warn("[Gdfl] connect timed out — will retry in {}s", props.getReconnectDelaySeconds());
                scheduleReconnect();
            } else {
                log.info("[Gdfl] WS connect returned true — waiting for AuthenticateResult (gen={})", myGeneration);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private void scheduleReconnect() {
        if (shuttingDown) return;
        if (executor.isShutdown()) return;
        int delaySec = Math.max(1, props.getReconnectDelaySeconds());
        log.info("[Gdfl] scheduling reconnect in {}s", delaySec);
        try {
            executor.schedule(this::connect, delaySec, TimeUnit.SECONDS);
        } catch (java.util.concurrent.RejectedExecutionException ex) {
            log.warn("[Gdfl] reconnect not scheduled — executor rejected (shutdown?)");
        }
    }

    /** Fires every {@code gdfl.atmPollIntervalSeconds}. Discovers today's ATM CE + PE
     *  from OptionBuying and issues SubscribeRealtime for each — once and only once per day. */
    private void checkAtmAndSubscribe() {
        try {
            // Window opens at 09:10 IST — aligned with OptionBuying.warmupIfDue's
            // pre-market subscribe window. Subscribing the ±10 pre-warm strikes to
            // GDFL BEFORE 09:15 means the exchange's first tick lands in an
            // already-subscribed slot (no first-bar-partial). Closes at 15:41
            // (~1 min past market close — NSE extended session to 15:40 effective
            // 2026-08-03) to catch the final flush.
            LocalTime now = ZonedDateTime.now(IST).toLocalTime();
            if (now.isBefore(LocalTime.of(9, 10)) || now.isAfter(LocalTime.of(15, 41))) return;

            // Day rollover — clear yesterday's subscriptions and reverse map + release
            // any alternate-feed ownership on Fyers's side so yesterday's ATM symbol
            // resumes Fyers ingress if it happens to re-appear in the wild.
            //
            // NIFTY-I is EXCLUDED from the wipe — it's GDFL's continuous
            // current-month identifier, subscribed at boot via initialSubs and
            // still live on the WS across day boundaries (GDFL rotates the
            // underlying contract server-side). Re-adding it to every state
            // set after the clear prevents subscribeOne from re-firing a
            // duplicate SubscribeRealtime at 09:10:01 every morning.
            String today = LocalDate.now(IST).toString();
            if (!today.equals(subscribedDayKey)) {
                subscribedGdflSymbols.clear();
                snapshotSubscribedGdflSymbols.clear();
                mapper.clear();
                marketDataService.clearAltFeedOwnedSymbols();
                // Re-seed the Realtime dedupe set that connect()'s boot-time
                // subscribe put in — mapper reverse-mapping, altFeed ownership.
                subscribedGdflSymbols.add(GdflSymbolMapper.GDFL_NIFTY_FUTURES);
                mapper.fyersToGdfl(GdflSymbolMapper.FYERS_NIFTY_FUTURES);
                marketDataService.addAltFeedOwnedSymbol(GdflSymbolMapper.FYERS_NIFTY_FUTURES);
                subscribedDayKey = today;
            }

            // WS must be up + authenticated.
            if (wsClient == null || !wsClient.isAuthenticated()) return;

            // NIFTY current-month futures — always subscribed independent of the
            // strategy pre-warm. Uses GDFL's continuous "NIFTY-I" identifier which
            // auto-rolls at expiry server-side, so no local expiry math / holiday
            // walkback / rollover code needed. Idempotent — subscribeOne skips
            // once it's in subscribedGdflSymbols.
            subscribeOne(GdflSymbolMapper.FYERS_NIFTY_FUTURES);

            // Canonical 1-min OHLC push — GDFL's server-side aggregation of every
            // trade on the exchange tape, delivered ~1.5-2 s after each 5-min
            // close. Feeds the aggregator's history ring directly — bar OHLC
            // matches TradingView by construction (no tick-side aggregation).
            // Idempotent per day via snapshotSubscribedGdflSymbols set.
            if (snapshotSubscribedGdflSymbols.add(GdflSymbolMapper.GDFL_NIFTY_FUTURES)) {
                boolean sent = wsClient.subscribeSnapshot(
                    GdflSymbolMapper.GDFL_NIFTY_FUTURES, "MINUTE", 1);
                if (sent) {
                    log.info("[Gdfl] SubscribeSnapshot fired for NIFTY-I (MINUTE, 1) — canonical 1-min bars feed the chart, aggregator emits 5-min aggregate to strategy every 5th minute");
                } else {
                    snapshotSubscribedGdflSymbols.remove(GdflSymbolMapper.GDFL_NIFTY_FUTURES);
                    log.warn("[Gdfl] SubscribeSnapshot for NIFTY-I failed to send — will retry next poll");
                }
            }

            // Pre-warm window — OptionBuying.warmupIfDue populates ±10 strikes
            // (42 CE + PE symbols) at 09:10 IST. Subscribe them all on GDFL so
            // the 09:15 → 09:16 first 1-min bar has tick data for whichever
            // strike ends up being the ATM anchor. Comfortably under GDFL's
            // 50-symbol cap. subscribeOne is idempotent so the 5 s poll doesn't
            // re-send SubscribeRealtime.
            //
            // Once OptionBuying.trimWarmingSet narrows to just the 1 ITM
            // CE + PE pair (at 09:16), getPreWarmSymbols() returns those two.
            OptionBuying strategy = optionBuyingProvider.getIfAvailable();
            if (strategy != null) {
                for (String sym : strategy.getPreWarmSymbols()) {
                    subscribeOne(sym);
                }
                // Always cover the resolved ATM pair (defence in depth — pre-warm
                // list is empty once trimmed on some code paths).
                subscribeOne(strategy.getCeSymbol());
                subscribeOne(strategy.getPeSymbol());
            }

        } catch (Exception e) {
            log.warn("[Gdfl] atm-check loop threw: {}", e.getMessage());
        }
    }

    /** Public wrapper — a strategy that resolves its target Fyers symbol at
     *  runtime (e.g. the OPTION BUYING FSM picks today's OTM strike at 09:20)
     *  calls this to have GDFL subscribe the strike on demand. Delegates to the
     *  same idempotent {@link #subscribeOne} path the atm-check loop uses, so
     *  no double-subscribe. Safe to call from any thread; the underlying WS
     *  send is guarded by the GDFL client. Silently no-ops when the WS isn't
     *  up yet — the caller is expected to retry (bar-close or scheduler tick).
     *  <p>Returns {@code true} when the symbol is now subscribed (either
     *  because this call issued the subscribe or an earlier call already did);
     *  {@code false} on translation failure / WS not authenticated. */
    public boolean subscribeSymbolOnDemand(String fyersSym) {
        if (fyersSym == null || fyersSym.isBlank()) return false;
        if (wsClient == null || !wsClient.isAuthenticated()) return false;
        String gdflSym = mapper.fyersToGdfl(fyersSym);
        if (gdflSym == null) return false;
        if (subscribedGdflSymbols.contains(gdflSym)) return true;
        subscribeOne(fyersSym);
        return subscribedGdflSymbols.contains(gdflSym);
    }

    /** Idempotent per-symbol subscribe. Translates the Fyers symbol to GDFL contractwise
     *  format, sends SubscribeRealtime, and takes altFeed ownership so subsequent Fyers
     *  ticks for this symbol are dropped at ingress. Silently no-ops on second call for
     *  the same symbol or when the symbol can't be translated. */
    private void subscribeOne(String fyersSym) {
        if (fyersSym == null || fyersSym.isBlank()) return;
        String gdflSym = mapper.fyersToGdfl(fyersSym);
        if (gdflSym == null) {
            log.warn("[Gdfl] can't translate {} to GDFL identifier", fyersSym);
            return;
        }
        if (subscribedGdflSymbols.contains(gdflSym)) return;
        if (wsClient.subscribeSymbol(gdflSym)) {
            subscribedGdflSymbols.add(gdflSym);
            marketDataService.addAltFeedOwnedSymbol(fyersSym);
            log.info("[Gdfl] subscribed {} (Fyers={})", gdflSym, fyersSym);
        }
    }

    /** Translates one GDFL RealtimeResult frame into a
     *  {@link MarketDataService.LtpTick} and hands it to the same listener chain Fyers
     *  ticks flow through.
     *
     *  <p>GDFL {@code LastTradeTime} = exchange trade timestamp (goes into
     *  {@code lastTradedTimeSec}); {@code ServerTime} = GDFL dissemination time (goes
     *  into {@code exchFeedTimeSec}). {@code AverageTradedPrice} maps to VWAP. */
    private void onGdflTick(JsonNode root) {
        String gdflSym = root.path("InstrumentIdentifier").asText("");
        if (gdflSym.isBlank()) return;
        String fyersSym = mapper.gdflToFyers(gdflSym);
        if (fyersSym == null || fyersSym.isBlank()) {
            // Bumped from DEBUG to INFO so diagnostic probe subscribes (via
            // /api/gdfl/diag/send-raw with a SubscribeRealtime payload) surface
            // their arriving ticks in the console — we can confirm a candidate
            // GDFL identifier is valid the moment the first tick prints. Revert
            // to DEBUG once probing is done if this becomes noisy.
            log.info("[Gdfl] tick for unmapped symbol {} ltp={} — ignored (probe echo?)",
                gdflSym, root.path("LastTradePrice").asDouble(0));
            return;
        }

        double ltp = root.path("LastTradePrice").asDouble(0);
        if (ltp <= 0) return;

        double atp = root.path("AverageTradedPrice").asDouble(0);
        long   ltt = root.path("LastTradeTime").asLong(0);   // exchange trade time (epoch sec)
        long   svt = root.path("ServerTime").asLong(0);      // GDFL dissemination time (epoch sec)
        // GDFL docs (function-subscriberealtime): "Close (previous Day's Close)".
        // Present on every RealtimeResult frame — we pass it to MarketDataService
        // so change / % change vs prev close is always available for the header
        // + hero tiles, without a separate daily-history request.
        double prevClose = root.path("Close").asDouble(0);
        // Exchange-cumulative session volume (Σ qty since 09:15) and turnover
        // (Σ price × qty since 09:15). Used by CandleAggregator to capture a
        // one-shot SessionVwapSeed on the first tick per day — powers the
        // restart-tolerant pandas_ta VWAP calc on the chart so a mid-day boot
        // shows an anchored-at-09:15 VWAP instead of an anchored-at-boot one.
        long   sessionVolume    = root.path("TotalQtyTraded").asLong(0);
        double sessionTurnover  = root.path("Value").asDouble(0);

        // Market-hours gate — drop pre-market / post-market / stale-day ticks BEFORE
        // they reach MarketDataService. Uses ServerTime primarily (populated on every
        // frame); falls back to LastTradeTime, then wall-clock if neither is set.
        long tickSec = svt > 0 ? svt : (ltt > 0 ? ltt : System.currentTimeMillis() / 1000);
        ZonedDateTime tickZdt = Instant.ofEpochSecond(tickSec).atZone(IST);
        LocalDate today   = LocalDate.now(IST);
        LocalDate tickDay = tickZdt.toLocalDate();
        LocalTime tickTime = tickZdt.toLocalTime();

        // Two gates apply BEFORE aggregator dispatch:
        //   1. Wrong-day OR out-of-market-hours → cache LTP only, skip listener
        //      fanout. Post-close / pre-open / after-restart, GDFL often pushes
        //      a "last known" snapshot tick on SubscribeRealtime — that seeds
        //      the UI cache without polluting bar aggregation with stale-
        //      timestamped bars. Values older than 5 trading days are rejected
        //      entirely as clearly stale.
        //   2. Same-day AND within 09:15-15:41 → normal live path (aggregator +
        //      listeners).
        long daysAgo = java.time.temporal.ChronoUnit.DAYS.between(tickDay, today);
        boolean withinMarketHours = !tickTime.isBefore(LocalTime.of(9, 15))
                                 && !tickTime.isAfter(LocalTime.of(15, 41));
        boolean isLive = today.equals(tickDay) && withinMarketHours;

        if (!isLive) {
            if (daysAgo > 5) {
                log.info("[Gdfl] rejecting truly-stale tick for {} ltp={} (tickDay={}, {}d ago)",
                    fyersSym, ltp, tickDay, daysAgo);
                return;
            }
            // Cache-only path — populates currentTicks so getDisplayLtp returns the
            // value, but no ltpListeners fire so CandleAggregator stays clean.
            // Also seeds prevClose from the tick's Close field so the header
            // change chip works pre-market / post-close too.
            marketDataService.seedTickData(fyersSym, ltp, prevClose);
            log.info("[Gdfl] out-of-hours/stale-day tick for {} ltp={} prevClose={} — cached ({} {})",
                fyersSym, ltp, prevClose, tickDay, tickTime);
            return;
        }

        // Log first live tick per symbol so we can visually confirm the pipeline works.
        // Subsequent live ticks go DEBUG to avoid flooding.
        if (firstLiveTickSeen.add(fyersSym)) {
            log.info("[Gdfl] FIRST LIVE TICK for {} ltp={} atp={} prevClose={} ({} {})",
                fyersSym, ltp, atp, prevClose, tickDay, tickTime);
            try {
                eventService.log(String.format("[Gdfl] First live tick — %s ltp=%.2f atp=%.2f (%s)",
                    fyersSym, ltp, atp, tickTime));
            } catch (Exception ignored) {}
        }
        MarketDataService.LtpTick evt = new MarketDataService.LtpTick(
            fyersSym, ltp, atp, svt, ltt, prevClose, sessionVolume, sessionTurnover);
        marketDataService.pushLtpTick(evt);
    }

    /** Handler for every OHLC frame GDFL pushes on our SubscribeSnapshot channel.
     *  Each frame is a canonical 1-min bar for NIFTY-I, delivered ~1.5-2 s after
     *  the minute boundary. Passed straight to
     *  {@link CandleAggregator#appendOneMinBar} — chart uses the 1-min bar
     *  directly; the aggregator emits a synthetic 5-min bar to strategy
     *  listeners every 5th minute.
     *
     *  <p>Frame schema (verified 2026-08-08 empirically):
     *  <ul>
     *    <li>{@code Exchange}, {@code InstrumentIdentifier}</li>
     *    <li>{@code LastTradeTime} — bar START epoch (seconds), NOT close</li>
     *    <li>{@code Open}, {@code High}, {@code Low}, {@code Close}</li>
     *    <li>{@code TradedQty} — trades in this 1-min window (not cumulative)</li>
     *    <li>{@code Periodicity} = "MINUTE", {@code Period} = 1</li>
     *  </ul> */
    private void onCanonicalMinuteBar(JsonNode root) {
        String gdflSym = root.path("InstrumentIdentifier").asText("");
        if (gdflSym.isBlank()) return;
        String fyersSym = mapper.gdflToFyers(gdflSym);
        // Continuous-alias echo: GDFL sometimes echoes the specific contract
        // (NIFTY27AUG26FUT) instead of the "NIFTY-I" alias we subscribed with.
        // Same underlying — route to the futures symbol so the append lands.
        if ((fyersSym == null || fyersSym.isBlank())
                && gdflSym.startsWith("NIFTY")
                && (gdflSym.endsWith("FUT") || gdflSym.contains("-I"))) {
            fyersSym = GdflSymbolMapper.FYERS_NIFTY_FUTURES;
        }
        if (fyersSym == null || fyersSym.isBlank()) return;
        // Only interested in NIFTY futures for now; other snapshots (if any) ignored.
        if (!GdflSymbolMapper.FYERS_NIFTY_FUTURES.equals(fyersSym)) return;

        long ltSec  = root.path("LastTradeTime").asLong(0);
        double open  = root.path("Open").asDouble(0);
        double high  = root.path("High").asDouble(0);
        double low   = root.path("Low").asDouble(0);
        double close = root.path("Close").asDouble(0);
        long volume  = root.path("TradedQty").asLong(0);
        if (ltSec <= 0 || open <= 0 || high <= 0 || low <= 0 || close <= 0) {
            log.warn("[Gdfl] canonical 1-min bar MALFORMED for {} — {}", fyersSym, root);
            return;
        }
        long startMs = ltSec * 1000L;
        Candle bar = new Candle(open, high, low, close, volume, startMs, 0.0);
        log.info("[Gdfl] canonical 1-min bar for {} — startMs={} o={} h={} l={} c={} v={}",
            fyersSym, startMs, open, high, low, close, volume);
        candleAggregator.appendOneMinBar(fyersSym, bar);
    }

}
