package com.rydytrader.autotrader.gdfl;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OptionOiTracker;
import com.rydytrader.autotrader.service.strategy.AtmVwap;
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
 *       As soon as {@link AtmVwap#getCeSymbol()} + {@link AtmVwap#getPeSymbol()} are
 *       non-blank (i.e. AtmVwap has resolved the day's ATM at ~09:17 IST — either from
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
    private final ObjectProvider<AtmVwap> atmVwapProvider;
    private final ObjectProvider<OptionOiTracker> oiTrackerProvider;
    private final ScheduledExecutorService executor;

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

    public GdflService(GdflProperties props,
                       GdflSymbolMapper mapper,
                       MarketDataService marketDataService,
                       ObjectProvider<AtmVwap> atmVwapProvider,
                       ObjectProvider<OptionOiTracker> oiTrackerProvider) {
        this.props             = props;
        this.mapper            = mapper;
        this.marketDataService = marketDataService;
        this.atmVwapProvider   = atmVwapProvider;
        this.oiTrackerProvider = oiTrackerProvider;
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
        // Fresh WS instance — no subscriptions active on it yet. Clear the
        // dedup set so the atm-check loop re-issues SubscribeRealtime for
        // every symbol in today's OI window. Without this, subscribeOne
        // would think each symbol is "already subscribed" (from the previous
        // WS) and skip, leaving the new WS silent.
        subscribedGdflSymbols.clear();
        // Empty subscribe list at connect time — the poller will send SubscribeRealtime
        // per symbol once AtmVwap has today's ATM. onDisconnect wires this same class
        // back into scheduleReconnect, so a mid-day remote drop auto-heals.
        wsClient = new GdflDataWebSocket(endpoint, props.getApiKey(), props.getExchange(),
            new ArrayList<>(), this::onGdflTick, this::scheduleReconnect);
        wsClient.setConnectionLostTimeout(30);
        try {
            boolean connected = wsClient.connectBlocking(15, TimeUnit.SECONDS);
            if (!connected) {
                log.warn("[Gdfl] connect timed out — will retry in {}s", props.getReconnectDelaySeconds());
                scheduleReconnect();
            } else {
                log.info("[Gdfl] WS connect returned true — waiting for AuthenticateResult");
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
     *  from AtmVwap and issues SubscribeRealtime for each — once and only once per day. */
    private void checkAtmAndSubscribe() {
        try {
            // Only makes sense during market hours + a small warm-up window.
            LocalTime now = ZonedDateTime.now(IST).toLocalTime();
            if (now.isBefore(LocalTime.of(9, 15)) || now.isAfter(LocalTime.of(15, 31))) return;

            // Day rollover — clear yesterday's subscriptions and reverse map + release
            // any alternate-feed ownership on Fyers's side so yesterday's ATM symbol
            // resumes Fyers ingress if it happens to re-appear in the wild.
            String today = LocalDate.now(IST).toString();
            if (!today.equals(subscribedDayKey)) {
                subscribedGdflSymbols.clear();
                mapper.clear();
                marketDataService.clearAltFeedOwnedSymbols();
                subscribedDayKey = today;
            }

            // WS must be up + authenticated.
            if (wsClient == null || !wsClient.isAuthenticated()) return;

            // OI-tracker window — populated as early as 09:15 pre-warm by
            // OptionOiSubscriber.onPreWarm. Subscribing here (not gated on ATM
            // resolution) means every strike's OI baseline is captured from GDFL
            // at 09:15, not source-swapped from Fyers at 09:17. Pre-warm ±10 = 42
            // symbols, comfortably under GDFL's 50-symbol cap. subscribeOne is
            // idempotent — the every-5-s poll doesn't re-send SubscribeRealtime.
            //
            // The ATM CE + PE aggregation legs are always inside the ±10 window,
            // so they're subscribed here too — no separate aggregation-legs block
            // is needed (the earlier gdfl.subscribe-side filter was retired once
            // the strategy locked into GDFL-owned CE + PE + OI end-to-end).
            OptionOiTracker oiTracker = oiTrackerProvider.getIfAvailable();
            if (oiTracker != null) {
                for (OptionOiTracker.StrikeSymbols ss : oiTracker.activeWindow()) {
                    subscribeOne(ss.ceSymbol());
                    subscribeOne(ss.peSymbol());
                }
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
            log.debug("[Gdfl] tick for unmapped symbol {} — ignored", gdflSym);
            return;
        }

        double ltp = root.path("LastTradePrice").asDouble(0);
        if (ltp <= 0) return;

        double atp = root.path("AverageTradedPrice").asDouble(0);
        long   ltt = root.path("LastTradeTime").asLong(0);   // exchange trade time (epoch sec)
        long   svt = root.path("ServerTime").asLong(0);      // GDFL dissemination time (epoch sec)

        // Market-hours gate — drop pre-market / post-market / stale-day ticks BEFORE
        // they reach MarketDataService. Pre-market frames could otherwise seed
        // OptionOiTracker with yesterday-EOD baselines and advance the countdown clock
        // with a pre-09:15 timestamp. Uses ServerTime primarily (populated on every
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

        // OI side-channel — same tick carries OpenInterest + OpenInterestChange. Fan
        // out via pushOiTick so OptionOiTracker sees per-strike OI updates from GDFL
        // (for altFeed-owned symbols, Fyers's OI is dropped in MarketDataService.onTick,
        // so this is the only source). OpenInterestChange is captured on the OiTick for
        // future consumers; the tracker itself still computes cumulative-since-baseline
        // deltas independently, so a zero OpenInterestChange is harmless.
        long oi       = root.path("OpenInterest").asLong(0);
        long oiChange = root.path("OpenInterestChange").asLong(0);
        if (oi > 0) marketDataService.pushOiTick(fyersSym, oi, svt, oiChange);
    }
}
