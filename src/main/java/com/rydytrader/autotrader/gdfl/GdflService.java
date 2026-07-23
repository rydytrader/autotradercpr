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
     *  day rollover. */
    private final Set<String> subscribedGdflSymbols = new HashSet<>();
    /** Day-key of the last successful subscribe pass, so day rollover clears state. */
    private volatile String subscribedDayKey = "";

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
        if (props.getApiKey() == null || props.getApiKey().isBlank()) {
            log.warn("[Gdfl] gdfl.enabled=true but GDFL_API_KEY env var is not set — skipping");
            return;
        }
        if (props.getEndpoint() == null || props.getEndpoint().isBlank()) {
            log.warn("[Gdfl] gdfl.enabled=true but gdfl.endpoint is not set — skipping");
            return;
        }
        connect();
        // Poll for ATM resolution every configured interval. As soon as CE + PE are
        // resolved, subscribe. Idempotent — won't re-subscribe the same symbol.
        int pollSec = Math.max(1, props.getAtmPollIntervalSeconds());
        executor.scheduleWithFixedDelay(this::checkAtmAndSubscribe, pollSec, pollSec, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
        if (wsClient != null) {
            try { wsClient.close(); } catch (Exception ignored) {}
        }
    }

    private synchronized void connect() {
        URI endpoint;
        try { endpoint = URI.create(props.getEndpoint()); }
        catch (Exception e) {
            log.warn("[Gdfl] invalid gdfl.endpoint '{}': {}", props.getEndpoint(), e.getMessage());
            return;
        }
        // Empty subscribe list at connect time — the poller will send SubscribeRealtime
        // per symbol once AtmVwap has today's ATM.
        wsClient = new GdflDataWebSocket(endpoint, props.getApiKey(), props.getExchange(),
            new ArrayList<>(), this::onGdflTick);
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
        int delaySec = Math.max(1, props.getReconnectDelaySeconds());
        executor.schedule(this::connect, delaySec, TimeUnit.SECONDS);
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

            // 1) OI-tracker window — populated as early as 09:15 pre-warm by
            //    OptionOiSubscriber.onPreWarm. Subscribing here (not gated on ATM
            //    resolution) means every strike's OI baseline is captured from GDFL
            //    at 09:15, not source-swapped from Fyers at 09:17. Pre-warm ±10 = 42
            //    symbols, comfortably under GDFL's 50-symbol cap. subscribeOne is
            //    idempotent — the every-5-s poll doesn't re-send SubscribeRealtime.
            OptionOiTracker oiTracker = oiTrackerProvider.getIfAvailable();
            if (oiTracker != null) {
                for (OptionOiTracker.StrikeSymbols ss : oiTracker.activeWindow()) {
                    subscribeOne(ss.ceSymbol());
                    subscribeOne(ss.peSymbol());
                }
            }

            // 2) Aggregation legs (LTP + OHLC → CandleAggregator + FSM). Only known
            //    once AtmVwap resolves the ATM at 09:17. Usually a no-op because the
            //    aggregation legs are ALREADY inside the pre-warm ±10 window and
            //    subscribeOne saw them at 09:15 — this block just enforces the
            //    subscribeSide filter and handles the (rare) drift case where the
            //    resolved ATM sits outside the pre-warm window.
            AtmVwap atmVwap = atmVwapProvider.getIfAvailable();
            if (atmVwap == null) return;
            String ceFyers = atmVwap.getCeSymbol();
            String peFyers = atmVwap.getPeSymbol();
            if (ceFyers == null || ceFyers.isBlank()) return;
            if (peFyers == null || peFyers.isBlank()) return;

            String side = props.getSubscribeSide() == null ? "BOTH"
                : props.getSubscribeSide().trim().toUpperCase();
            if ("CE".equals(side) || "BOTH".equals(side)) subscribeOne(ceFyers);
            if ("PE".equals(side) || "BOTH".equals(side)) subscribeOne(peFyers);
            if (!"CE".equals(side) && !"PE".equals(side) && !"BOTH".equals(side)) {
                log.warn("[Gdfl] unrecognised gdfl.subscribe-side={} (allowed: CE, PE, BOTH)", side);
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
