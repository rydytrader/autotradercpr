package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lifecycle owner for the OI bias tracker's 42-symbol WebSocket subscription.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Listens to {@link AtmTracker} for ATM changes.</li>
 *   <li>On each change, resolves the 21 strikes around the new ATM (ATM ± 10) into
 *       42 Fyers option symbols via a single option-chain REST fetch.</li>
 *   <li>Hands the resolved (strike → ceSymbol / peSymbol) window to
 *       {@link OptionOiTracker#setActiveWindow(List)} so the tracker knows which
 *       symbols count toward the bias sum.</li>
 *   <li>Calls {@link MarketDataService#subscribeAdditional(java.util.Collection)} on the
 *       new symbols entering the window.</li>
 * </ul>
 *
 * <p>Old strikes that drift out of the window stay subscribed on the WebSocket (cheaper
 * than maintaining an unsubscribe path), but the tracker ignores their OI ticks because
 * they're no longer in the active window.
 */
@Service
public class OptionOiSubscriber {

    private static final Logger log = LoggerFactory.getLogger(OptionOiSubscriber.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String NIFTY_SYMBOL  = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_STEP   = 50L;
    private static final int    STRIKES_PER_SIDE = 15;
    private static final int    CHAIN_FETCH_STRIKES = 35;
    /** A successful subscription places at least this many strike pairs into the active window.
     *  Full window is 2 × STRIKES_PER_SIDE + 1 = 31 pairs at STRIKES_PER_SIDE=15; some
     *  far-edge strikes may be missing legs near boot, so we accept ~70% as healthy. */
    private static final int    HEALTHY_WINDOW_MIN_STRIKES = 22;

    private volatile boolean subscriptionsHealthy = false;

    private final FyersClientRouter fyersClient;
    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;
    private final AtmTracker        atmTracker;
    private final MarketDataService marketDataService;
    private final OptionOiTracker   oiTracker;

    public OptionOiSubscriber(FyersClientRouter fyersClient,
                              TokenStore        tokenStore,
                              FyersProperties   fyersProperties,
                              AtmTracker        atmTracker,
                              MarketDataService marketDataService,
                              OptionOiTracker   oiTracker) {
        this.fyersClient       = fyersClient;
        this.tokenStore        = tokenStore;
        this.fyersProperties   = fyersProperties;
        this.atmTracker        = atmTracker;
        this.marketDataService = marketDataService;
        this.oiTracker         = oiTracker;
    }

    @PostConstruct
    public void boot() {
        // Bridge MarketDataService OI ticks → OptionOiTracker. Registered once, fires for
        // every symbol's OI updates (tracker filters by active window).
        marketDataService.addOiListener(tick -> oiTracker.onOiTick(tick.fyersSymbol(), tick.oi(), tick.exchFeedTimeSec()));
        // Listen for ATM changes — first AtmChange after market open resolves the initial window.
        atmTracker.addListener(this::onAtmChange);
        log.info("[OptionOiSubscriber] booted — waiting for first ATM resolution");
    }

    public synchronized void onAtmChange(AtmTracker.AtmChange ev) {
        long atm = ev.newAtm();
        if (atm <= 0) return;
        if (!tokenStore.isTokenAvailable()) return;

        // Resolve 21 strikes × CE+PE symbols via one chain fetch.
        List<OptionOiTracker.StrikeSymbols> window = resolveWindowSymbols(atm);
        if (window.size() < HEALTHY_WINDOW_MIN_STRIKES) {
            // Common at 09:15:0x — Fyers can return an empty or partial chain right at market
            // open. Mark unhealthy so {@link #retryIfSubscriptionsMissing} keeps re-attempting
            // every minute until the chain comes back full, instead of waiting 30 min for the
            // next ATM drift heartbeat.
            subscriptionsHealthy = false;
            log.warn("[OptionOiSubscriber] could not resolve full symbol set around ATM={} (got {} strikes, need {}). Will retry.",
                atm, window.size(), HEALTHY_WINDOW_MIN_STRIKES);
            return;
        }

        // Tell tracker which strikes count + the symbol→strike mapping for tick routing.
        List<String> newSymbols = oiTracker.setActiveWindow(window);

        if (!newSymbols.isEmpty()) {
            marketDataService.subscribeAdditional(newSymbols);
            log.info("[OptionOiSubscriber] subscribed to {} new OI symbols around ATM={} (window={} strikes)",
                newSymbols.size(), atm, window.size());
        } else {
            log.info("[OptionOiSubscriber] ATM={} window unchanged ({} strikes already subscribed)",
                atm, window.size());
        }
        subscriptionsHealthy = true;
    }

    /** Retry loop — runs every minute during market hours. If the initial subscription attempt
     *  at 09:15 failed (typical: Fyers chain endpoint is empty for the first few seconds after
     *  market open), this catches up so we don't sit idle for the whole session. The ATM
     *  itself is locked at open by AtmTracker and never drifts. */
    @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
    public void retryIfSubscriptionsMissing() {
        if (subscriptionsHealthy) return;
        LocalTime t = ZonedDateTime.now(IST).toLocalTime();
        if (t.isBefore(LocalTime.of(9, 15)) || t.isAfter(LocalTime.of(15, 30))) return;
        long atm = atmTracker.getCurrentAtm();
        if (atm <= 0) return;
        log.info("[OptionOiSubscriber] retry — attempting to resolve symbols around ATM={}", atm);
        onAtmChange(new AtmTracker.AtmChange(atm, atm));
    }

    /** One chain fetch → walk strikes ATM − 500 … ATM + 500 (step 50) → 21 strikes,
     *  each with its CE and PE Fyers symbol (skipping strikes where one leg is missing). */
    private List<OptionOiTracker.StrikeSymbols> resolveWindowSymbols(long atm) {
        List<OptionOiTracker.StrikeSymbols> out = new ArrayList<>();
        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
        JsonNode root;
        try {
            root = fyersClient.getOptionChain(NIFTY_SYMBOL, CHAIN_FETCH_STRIKES, auth);
        } catch (Exception e) {
            log.warn("[OptionOiSubscriber] chain fetch failed: {}", e.getMessage());
            return out;
        }
        if (root == null) return out;

        JsonNode data  = root.has("data") ? root.get("data") : null;
        JsonNode chain = data != null && data.has("optionsChain") ? data.get("optionsChain")
            : (root.has("optionsChain") ? root.get("optionsChain") : null);
        if (chain == null || !chain.isArray()) return out;

        Map<Long, String[]> byStrike = new LinkedHashMap<>();  // strike → [ceSym, peSym]
        for (JsonNode row : chain) {
            double strikeD = row.has("strike_price") ? row.get("strike_price").asDouble()
                : row.has("strikePrice") ? row.get("strikePrice").asDouble() : 0;
            String optType = row.has("option_type") ? row.get("option_type").asText("")
                : row.has("optionType") ? row.get("optionType").asText("") : "";
            String sym = row.has("symbol") ? row.get("symbol").asText("") : "";
            if (strikeD <= 0 || optType.isEmpty() || sym.isEmpty()) continue;
            long strike = Math.round(strikeD);
            String[] pair = byStrike.computeIfAbsent(strike, k -> new String[2]);
            if ("CE".equalsIgnoreCase(optType))      pair[0] = sym;
            else if ("PE".equalsIgnoreCase(optType)) pair[1] = sym;
        }

        long lo = atm - STRIKES_PER_SIDE * STRIKE_STEP;
        long hi = atm + STRIKES_PER_SIDE * STRIKE_STEP;
        for (long s = lo; s <= hi; s += STRIKE_STEP) {
            String[] pair = byStrike.get(s);
            if (pair == null || pair[0] == null || pair[1] == null) continue;
            out.add(new OptionOiTracker.StrikeSymbols(s, pair[0], pair[1]));
        }
        return out;
    }
}
