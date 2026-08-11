package com.rydytrader.autotrader.gdfl;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.service.CandleAggregator;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
    /** How long the canonical dispatcher waits after the wall-clock bar close
     *  for a GDFL {@code SnapshotResult} frame before falling back to the
     *  tick-aggregated bar. GDFL typically pushes snapshots ~200-300ms after
     *  the wall-clock close, so 1500ms leaves generous headroom on a healthy
     *  session and still guarantees the strategy sees a bar on a bad-network
     *  day (fires WARN so operators can spot the miss). */
    private static final long CANONICAL_FALLBACK_MS = 1500L;

    private final GdflProperties     props;
    private final GdflSymbolMapper   mapper;
    private final MarketDataService  marketDataService;
    private final CandleAggregator   candleAggregator;
    private final ObjectProvider<OptionBuying> optionBuyingProvider;
    private final ScheduledExecutorService executor;
    /** GDFL symbols we've already sent SubscribeSnapshot for TODAY. Prevents the
     *  ATM-check poll from re-subscribing every 5 seconds. Reset on day rollover
     *  + fresh connect. Currently used for the NIFTY-I hybrid subscribe (Realtime
     *  for LTP + Snapshot for canonical 5-min OHLC); previously held the trading
     *  pair (ATM CE + PE) — that path is disabled but the set is reused here. */
    private final Set<String> snapshotSubscribedGdflSymbols = new HashSet<>();
    /** Fyers symbol → canonical-bar listeners. Fired once per 5-min bar for
     *  the futures leg — with either GDFL snapshot values (~200-300ms after
     *  wall-clock close) or, if the snapshot doesn't arrive within
     *  {@link #CANONICAL_FALLBACK_MS}, the tick-aggregated bar as a fallback.
     *  {@link CopyOnWriteArrayList} so subscribes can happen while dispatch is
     *  iterating without ConcurrentModificationException. */
    private final Map<String, CopyOnWriteArrayList<Consumer<Candle>>>
        canonicalBarListeners = new ConcurrentHashMap<>();
    /** Per-symbol last bar {@code startMillis} that fired the canonical dispatch
     *  — dedupe so snapshot + fallback don't both fire for the same bar. */
    private final Map<String, Long> lastCanonicalFiredBarStartMs = new ConcurrentHashMap<>();
    /** Pending {@link #CANONICAL_FALLBACK_MS} fallback tasks — cancelled the
     *  instant a snapshot frame lands (snapshot won the race). */
    private final Map<String, ScheduledFuture<?>> pendingFallbackTasks = new ConcurrentHashMap<>();
    /** GDFL symbols we've already fired GetHistory for TODAY (backfilling the
     *  09:15 → 09:16 first bar for the trading pair). */
    private final Set<String> historyFetchedGdflSymbols = new HashSet<>();

    private volatile GdflDataWebSocket wsClient;
    /** Which GDFL identifiers we've already sent SubscribeRealtime for TODAY. Reset at
     *  day rollover AND on every fresh connect (each new WS starts with zero
     *  live subscriptions). */
    private final Set<String> subscribedGdflSymbols = new HashSet<>();
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
                       ObjectProvider<OptionBuying> optionBuyingProvider) {
        this.props             = props;
        this.mapper            = mapper;
        this.marketDataService = marketDataService;
        this.candleAggregator  = candleAggregator;
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

        // Fallback timer REMOVED — strategy now fires ONLY when a GDFL
        // SubscribeSnapshot frame lands. If snapshot never arrives for a bar,
        // the strategy skips that bar entirely. Trade-off: accuracy over
        // guaranteed coverage. Per operator ask 2026-08-11.

        // Poll for ATM resolution every configured interval. As soon as CE + PE are
        // resolved, subscribe. Idempotent — won't re-subscribe the same symbol.
        int pollSec = Math.max(1, props.getAtmPollIntervalSeconds());
        executor.scheduleWithFixedDelay(this::checkAtmAndSubscribe, pollSec, pollSec, TimeUnit.SECONDS);
    }

    /** Register a listener fired once per 5-min bar for {@code fyersSymbol}
     *  with the CANONICAL bar values — either from a GDFL
     *  {@code SubscribeSnapshot} frame (~200-300ms after wall-clock bar close)
     *  or a {@link #CANONICAL_FALLBACK_MS} fallback using the tick-aggregated
     *  bar when the snapshot doesn't arrive.
     *
     *  <p>The strategy trigger (OPTIONS BUYING + OPTIONS SELLING onFuturesBar)
     *  registers here so its OHLC matches TradingView. The aggregator's own
     *  close event is not used on the trigger path — the aggregator only
     *  buckets ticks for chart / VWAP display. */
    public void addCanonicalBarListener(String fyersSymbol, Consumer<Candle> listener) {
        if (fyersSymbol == null || listener == null) return;
        canonicalBarListeners
            .computeIfAbsent(fyersSymbol, k -> new CopyOnWriteArrayList<>())
            .add(listener);
        log.info("[Canonical] registered canonical-bar listener for {}", fyersSymbol);
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
            initialSubs, this::onGdflTick, this::onGdflOhlcBar, onDisconnect);
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
            // already-subscribed slot (no first-bar-partial). Closes at 15:31
            // (~1 min past market close) to catch the final flush.
            LocalTime now = ZonedDateTime.now(IST).toLocalTime();
            if (now.isBefore(LocalTime.of(9, 10)) || now.isAfter(LocalTime.of(15, 31))) return;

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
                historyFetchedGdflSymbols.clear();
                mapper.clear();
                marketDataService.clearAltFeedOwnedSymbols();
                // Re-seed the Realtime dedupe set that connect()'s boot-time
                // subscribe put in — mapper reverse-mapping, altFeed ownership.
                subscribedGdflSymbols.add(GdflSymbolMapper.GDFL_NIFTY_FUTURES);
                // Snapshot: intentionally NOT pre-added to the dedupe set here.
                // The rollover branch also fires on FRESH process boot (when
                // subscribedDayKey starts as ""), and pre-adding would block the
                // first-ever SubscribeSnapshot send from firing. Let
                // checkAtmAndSubscribe below fire the subscribe naturally on
                // every fresh process — GDFL accepts duplicate SubscribeSnapshot
                // silently, so a re-fire on true day rollover (rare — bot
                // running > 24h) is harmless.
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

            // HYBRID subscribe — Realtime (above) gives us live LTP for the
            // header / hero / live P&L; Snapshot below gives us GDFL-side
            // aggregated 5-min OHLC that matches TradingView. Snapshot frames
            // land in onGdflOhlcBar and drive the canonical-bar dispatch that
            // fires the strategy trigger. Idempotent — the snapshot dedupe
            // set stops repeat sends within the session. GDFL-side subscription
            // persists across day boundaries (see the day-rollover re-add).
            if (snapshotSubscribedGdflSymbols.add(GdflSymbolMapper.GDFL_NIFTY_FUTURES)) {
                boolean sent = wsClient.subscribeSnapshot(
                    GdflSymbolMapper.GDFL_NIFTY_FUTURES, "MINUTE", CandleAggregator.BUCKET_MINUTES);
                if (sent) {
                    log.info("[Canonical] SubscribeSnapshot fired for NIFTY-I ({}m bars)",
                        CandleAggregator.BUCKET_MINUTES);
                } else {
                    snapshotSubscribedGdflSymbols.remove(GdflSymbolMapper.GDFL_NIFTY_FUTURES);
                    log.warn("[Canonical] SubscribeSnapshot for NIFTY-I failed to send");
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
                // SubscribeSnapshot + GetHistory backfill for the trading pair
                // temporarily DISABLED — operator wants to test whether the 3
                // client-side aggregation fixes (OPEN by earliest-LTT, CLOSE
                // by latest-LTT, grace window 1500ms) alone are enough to
                // match TradingView. Re-enable by uncommenting.
                // subscribeSnapshotForTradingPair();
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
        //   2. Same-day AND within 09:15-15:31 → normal live path (aggregator +
        //      listeners).
        long daysAgo = java.time.temporal.ChronoUnit.DAYS.between(tickDay, today);
        boolean withinMarketHours = !tickTime.isBefore(LocalTime.of(9, 15))
                                 && !tickTime.isAfter(LocalTime.of(15, 31));
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
            log.info("[Gdfl] FIRST LIVE TICK for {} ltp={} prevClose={} ({} {})",
                fyersSym, ltp, prevClose, tickDay, tickTime);
        }
        MarketDataService.LtpTick evt = new MarketDataService.LtpTick(
            fyersSym, ltp, atp, svt, ltt, prevClose);
        marketDataService.pushLtpTick(evt);
    }

    /** Fires for each GDFL server-side aggregated OHLC bar — one on every bar
     *  close for symbols we've SubscribeSnapshot'd, and each row of a GetHistory
     *  response. Frame schema per GDFL docs:
     *  <ul>
     *    <li>{@code Exchange}, {@code InstrumentIdentifier} — stamped by
     *        {@link GdflDataWebSocket#onMessage} even when the row itself
     *        omits them (GetHistory nests them at the top level).</li>
     *    <li>{@code LastTradeTime} — bar CLOSE epoch (seconds). We subtract
     *        the bar length to derive startMillis.</li>
     *    <li>{@code Open}, {@code High}, {@code Low}, {@code Close},
     *        {@code TradedQty}, {@code OpenInterest}.</li>
     *  </ul>
     *  We map the GDFL symbol back to its Fyers form (that's the key
     *  {@link CandleAggregator} uses) and hand the canonical bar to
     *  {@link CandleAggregator#overwriteBar} which replaces whatever
     *  tick-aggregated bar we already had for that timestamp. */
    private void onGdflOhlcBar(JsonNode root) {
        String gdflSym = root.path("InstrumentIdentifier").asText("");
        if (gdflSym.isBlank()) {
            log.info("[Gdfl] OHLC bar received with BLANK InstrumentIdentifier — dropping. Raw: {}", root);
            return;
        }
        // TEMPORARY DIAGNOSTIC — log EVERY OHLC bar arrival at INFO with the
        // raw InstrumentIdentifier so we can see exactly what GDFL sends for
        // continuous NIFTY-I snapshots (they may echo the specific contract
        // like NIFTY27AUG26FUT instead of the alias). Bump to DEBUG once the
        // mapping situation is resolved.
        log.info("[Gdfl] OHLC bar arrival — GDFL sym='{}' periodicity='{}' period={} closeSec={}",
            gdflSym, root.path("Periodicity").asText(""), root.path("Period").asInt(0),
            root.path("LastTradeTime").asLong(0));

        String fyersSym = mapper.gdflToFyers(gdflSym);
        // Special-case: any OHLC bar on a NIFTY futures identifier that isn't in
        // the mapper (e.g. GDFL echoing NIFTY27AUG26FUT instead of NIFTY-I when
        // we subscribed with the continuous alias) — treat it as the futures
        // symbol we care about. Same underlying instrument, same canonical bar.
        if ((fyersSym == null || fyersSym.isBlank())
                && gdflSym.startsWith("NIFTY") && (gdflSym.endsWith("FUT") || gdflSym.contains("-I"))) {
            fyersSym = GdflSymbolMapper.FYERS_NIFTY_FUTURES;
            log.info("[Gdfl] OHLC bar for '{}' auto-mapped to NIFTY-I futures (continuous-alias echo)", gdflSym);
        }
        if (fyersSym == null || fyersSym.isBlank()) {
            log.info("[Gdfl] OHLC bar for unmapped symbol '{}' — ignored", gdflSym);
            return;
        }
        long ltSec = root.path("LastTradeTime").asLong(0);
        double open   = root.path("Open").asDouble(0);
        double high   = root.path("High").asDouble(0);
        double low    = root.path("Low").asDouble(0);
        double close  = root.path("Close").asDouble(0);
        long   volume = root.path("TradedQty").asLong(0);
        if (ltSec <= 0 || open <= 0 || high <= 0 || low <= 0 || close <= 0) {
            log.info("[Gdfl] OHLC bar for {} malformed — skipping ({})", fyersSym, root);
            return;
        }
        // Empirical (verified 2026-08-11 with operator): GDFL's LastTradeTime on
        // RealtimeSnapshotResult frames is the bar's OPEN/START time — NOT the
        // close as our earlier assumption. Snapshot for a bar closing at
        // 12:55:00 arrives with LastTradeTime=1786432800 which decodes to
        // 12:50:00 (start of 12:50-12:55 bar). Our aggregator keys bars by
        // startMillis, so use LastTradeTime directly.
        long startMs = ltSec * 1000L;
        Candle canonical = new Candle(open, high, low, close, volume, startMs, 0.0);
        candleAggregator.overwriteBar(fyersSym, canonical);

        // Fan the canonical bar out to registered strategy listeners for the
        // futures leg. Match by the RESOLVED fyersSym so continuous-alias
        // echoes (GDFL sending NIFTY27AUG26FUT instead of NIFTY-I) still route
        // correctly — the check on snapshotSubscribedGdflSymbols would miss
        // those since it stores what WE sent ("NIFTY-I"), not what GDFL echoes.
        if (GdflSymbolMapper.FYERS_NIFTY_FUTURES.equals(fyersSym)) {
            dispatchCanonicalBar(fyersSym, canonical);
        }
    }

    /** Called from {@link #onGdflOhlcBar} for every snapshot-subscribed symbol.
     *  Dedupes against the fallback path via {@link #lastCanonicalFiredBarStartMs}
     *  and cancels any pending fallback timer for this symbol. Logs the arrival
     *  time + delay from wall-clock bar close. */
    private synchronized void dispatchCanonicalBar(String fyersSymbol, Candle bar) {
        Long prev = lastCanonicalFiredBarStartMs.get(fyersSymbol);
        if (prev != null && prev == bar.startMillis()) {
            // Fallback already fired for this bar — snapshot arrived late.
            log.info("[Canonical] {} snapshot arrived LATE (fallback already fired) — bar startMs={}",
                fyersSymbol, bar.startMillis());
            return;
        }
        lastCanonicalFiredBarStartMs.put(fyersSymbol, bar.startMillis());

        long barCloseMs = bar.startMillis() + CandleAggregator.BUCKET_MINUTES * 60_000L;
        long nowMs = System.currentTimeMillis();
        long delayMs = nowMs - barCloseMs;
        log.info("[Canonical] SNAPSHOT ARRIVED for {} — bar startMs={} closeMs={} nowMs={} delayMs={} (O={} H={} L={} C={})",
            fyersSymbol, bar.startMillis(), barCloseMs, nowMs, delayMs,
            bar.open(), bar.high(), bar.low(), bar.close());

        fireCanonicalListeners(fyersSymbol, bar);
    }

    /** Invoke every registered canonical listener for {@code fyersSymbol}. A
     *  throwing listener is logged and swallowed — one strategy's bug must not
     *  cascade into another's dispatch. */
    private void fireCanonicalListeners(String fyersSymbol, Candle bar) {
        List<Consumer<Candle>> ls = canonicalBarListeners.get(fyersSymbol);
        if (ls == null || ls.isEmpty()) return;
        for (Consumer<Candle> l : ls) {
            try { l.accept(bar); }
            catch (Exception e) {
                log.warn("[Canonical] listener for {} threw: {}", fyersSymbol, e.getMessage());
            }
        }
    }

    /** Called by the 5 s ATM-check poll once OptionBuying has locked the
     *  trading pair (CE + PE symbols non-blank). Sends {@code SubscribeSnapshot}
     *  for each leg — GDFL pushes {@code SnapshotResult} frames on every bar
     *  close, giving us canonical server-side aggregated OHLC that matches
     *  TradingView. Also fires a one-shot {@code GetHistory} per leg to
     *  backfill the 09:15 → 09:16 first bar which the snapshot push misses
     *  (we subscribe AFTER it closes). Idempotent — the sent-today dedupe
     *  sets stop repeats within the session. */
    private void subscribeSnapshotForTradingPair() {
        if (wsClient == null || !wsClient.isAuthenticated()) return;
        OptionBuying strategy = optionBuyingProvider.getIfAvailable();
        if (strategy == null) return;
        String ceFy = strategy.getCeSymbol();
        String peFy = strategy.getPeSymbol();
        subscribeSnapshotAndBackfill(ceFy);
        subscribeSnapshotAndBackfill(peFy);
    }

    private void subscribeSnapshotAndBackfill(String fyersSym) {
        if (fyersSym == null || fyersSym.isBlank()) return;
        String gdflSym = mapper.fyersToGdfl(fyersSym);
        if (gdflSym == null || gdflSym.isBlank()) return;
        if (!snapshotSubscribedGdflSymbols.contains(gdflSym)) {
            if (wsClient.subscribeSnapshot(gdflSym, "MINUTE", CandleAggregator.BUCKET_MINUTES)) {
                snapshotSubscribedGdflSymbols.add(gdflSym);
            }
        }
        if (!historyFetchedGdflSymbols.contains(gdflSym)) {
            // Ask for the last 5 bars — covers the just-closed first bar plus
            // a small buffer in case the caller ran a minute or two late.
            if (wsClient.getHistory(gdflSym, "MINUTE", CandleAggregator.BUCKET_MINUTES, 5)) {
                historyFetchedGdflSymbols.add(gdflSym);
            }
        }
    }
}
