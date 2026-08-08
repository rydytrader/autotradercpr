package com.rydytrader.autotrader.gdfl;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.strategy.OptionScalping;
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
 *       As soon as {@link OptionScalping#getCeSymbol()} + {@link OptionScalping#getPeSymbol()} are
 *       non-blank (i.e. OptionScalping has resolved the day's ATM at ~09:18 IST — either from
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
    private final ObjectProvider<OptionScalping> optionScalpingProvider;
    private final ScheduledExecutorService executor;
    /** GDFL symbols we've already sent SubscribeSnapshot for TODAY (the trading
     *  pair — ATM CE + PE resolved at 09:16). Prevents the ATM-check poll from
     *  re-subscribing every 5 seconds. Reset on day rollover + fresh connect. */
    private final Set<String> snapshotSubscribedGdflSymbols = new HashSet<>();
    /** GDFL symbols we've already fired GetHistory for TODAY (backfilling the
     *  09:15 → 09:16 first bar for the trading pair). */
    private final Set<String> historyFetchedGdflSymbols = new HashSet<>();

    private volatile GdflDataWebSocket wsClient;
    /** Which GDFL identifiers we've already sent SubscribeRealtime for TODAY. Reset at
     *  day rollover AND on every fresh connect (each new WS starts with zero
     *  live subscriptions). */
    private final Set<String> subscribedGdflSymbols = new HashSet<>();
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
                       ObjectProvider<OptionScalping> optionScalpingProvider) {
        this.props             = props;
        this.mapper            = mapper;
        this.marketDataService = marketDataService;
        this.candleAggregator  = candleAggregator;
        this.optionScalpingProvider   = optionScalpingProvider;
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
        // The onDisconnect lambda captures myGeneration; when it fires, it
        // reconnects only if we haven't already replaced this client. That
        // way the old leaked client's onClose becomes a no-op instead of
        // stacking additional reconnects.
        Runnable onDisconnect = () -> {
            if (myGeneration != wsGeneration) return;
            scheduleReconnect();
        };
        wsClient = new GdflDataWebSocket(endpoint, props.getApiKey(), props.getExchange(),
            new ArrayList<>(), this::onGdflTick, this::onGdflOhlcBar, onDisconnect);
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
     *  from OptionScalping and issues SubscribeRealtime for each — once and only once per day. */
    private void checkAtmAndSubscribe() {
        try {
            // Window opens at 09:10 IST — aligned with OptionScalping.warmupIfDue's
            // pre-market subscribe window. Subscribing the ±10 pre-warm strikes to
            // GDFL BEFORE 09:15 means the exchange's first tick lands in an
            // already-subscribed slot (no first-bar-partial). Closes at 15:31
            // (~1 min past market close) to catch the final flush.
            LocalTime now = ZonedDateTime.now(IST).toLocalTime();
            if (now.isBefore(LocalTime.of(9, 10)) || now.isAfter(LocalTime.of(15, 31))) return;

            // Day rollover — clear yesterday's subscriptions and reverse map + release
            // any alternate-feed ownership on Fyers's side so yesterday's ATM symbol
            // resumes Fyers ingress if it happens to re-appear in the wild.
            String today = LocalDate.now(IST).toString();
            if (!today.equals(subscribedDayKey)) {
                subscribedGdflSymbols.clear();
                snapshotSubscribedGdflSymbols.clear();
                historyFetchedGdflSymbols.clear();
                mapper.clear();
                marketDataService.clearAltFeedOwnedSymbols();
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

            // Pre-warm window — OptionScalping.warmupIfDue populates ±10 strikes
            // (42 CE + PE symbols) at 09:10 IST. Subscribe them all on GDFL so
            // the 09:15 → 09:16 first 1-min bar has tick data for whichever
            // strike ends up being the ATM anchor. Comfortably under GDFL's
            // 50-symbol cap. subscribeOne is idempotent so the 5 s poll doesn't
            // re-send SubscribeRealtime.
            //
            // Once OptionScalping.trimWarmingSet narrows to just the 1 ITM
            // CE + PE pair (at 09:16), getPreWarmSymbols() returns those two.
            OptionScalping strategy = optionScalpingProvider.getIfAvailable();
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

        // Market-hours gate — drop pre-market / post-market / stale-day ticks BEFORE
        // they reach MarketDataService. Uses ServerTime primarily (populated on every
        // frame); falls back to LastTradeTime, then wall-clock if neither is set.
        long tickSec = svt > 0 ? svt : (ltt > 0 ? ltt : System.currentTimeMillis() / 1000);
        ZonedDateTime tickZdt = Instant.ofEpochSecond(tickSec).atZone(IST);
        if (!LocalDate.now(IST).equals(tickZdt.toLocalDate())) {
            log.debug("[Gdfl] dropping stale-day tick for {} (tickDay={})", fyersSym, tickZdt.toLocalDate());
            return;
        }
        LocalTime tickTime = tickZdt.toLocalTime();
        if (tickTime.isBefore(LocalTime.of(9, 15)) || tickTime.isAfter(LocalTime.of(15, 31))) {
            log.debug("[Gdfl] dropping out-of-hours tick for {} (tickTime={})", fyersSym, tickTime);
            return;
        }

        MarketDataService.LtpTick evt = new MarketDataService.LtpTick(
            fyersSym, ltp, atp, svt, ltt);
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
        if (gdflSym.isBlank()) return;
        String fyersSym = mapper.gdflToFyers(gdflSym);
        if (fyersSym == null || fyersSym.isBlank()) {
            log.debug("[Gdfl] OHLC bar for unmapped symbol {} — ignored", gdflSym);
            return;
        }
        long closeSec = root.path("LastTradeTime").asLong(0);
        double open   = root.path("Open").asDouble(0);
        double high   = root.path("High").asDouble(0);
        double low    = root.path("Low").asDouble(0);
        double close  = root.path("Close").asDouble(0);
        long   volume = root.path("TradedQty").asLong(0);
        if (closeSec <= 0 || open <= 0 || high <= 0 || low <= 0 || close <= 0) {
            log.debug("[Gdfl] OHLC bar for {} malformed — skipping ({})", fyersSym, root);
            return;
        }
        // LastTradeTime is the bar CLOSE. Our aggregator keys bars by OPEN
        // (startMillis) — subtract one bar length to convert.
        long barLengthSec = 60L * CandleAggregator.BUCKET_MINUTES;
        long startMs = (closeSec - barLengthSec) * 1000L;
        Candle canonical = new Candle(open, high, low, close, volume, startMs, 0.0);
        candleAggregator.overwriteBar(fyersSym, canonical);
    }

    /** Called by the 5 s ATM-check poll once OptionScalping has locked the
     *  trading pair (CE + PE symbols non-blank). Sends {@code SubscribeSnapshot}
     *  for each leg — GDFL pushes {@code SnapshotResult} frames on every bar
     *  close, giving us canonical server-side aggregated OHLC that matches
     *  TradingView. Also fires a one-shot {@code GetHistory} per leg to
     *  backfill the 09:15 → 09:16 first bar which the snapshot push misses
     *  (we subscribe AFTER it closes). Idempotent — the sent-today dedupe
     *  sets stop repeats within the session. */
    private void subscribeSnapshotForTradingPair() {
        if (wsClient == null || !wsClient.isAuthenticated()) return;
        OptionScalping strategy = optionScalpingProvider.getIfAvailable();
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
