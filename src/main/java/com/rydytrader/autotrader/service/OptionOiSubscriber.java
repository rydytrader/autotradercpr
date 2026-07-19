package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lifecycle owner for the OI tracker's option-chain WebSocket subscription.
 *
 * <p>Trigger: {@link #onAtmSelected(long)} is called once per session by
 * {@code AtmVwap.resolveAtmFromFirstBar} right after it locks the day's ATM (~09:17
 * IST). Resolves ±N strikes around the ATM into Fyers CE + PE symbols via a single
 * option-chain REST fetch, hands them to {@link OptionOiTracker#setActiveWindow(long,
 * List)}, and calls {@link MarketDataService#subscribeAdditional} on the net-new
 * symbols. The strike count each side is configurable via
 * {@code riskSettings.getAtmVwapOiStrikesEachSide()} (default 15).
 *
 * <p>Also wires the MarketDataService OI listener → tracker on boot.
 *
 * <p>No retry loop — one shot per ATM selection. AtmVwap only fires when it has a
 * valid strike, so a failed chain fetch will simply leave the tracker idle for the
 * day. The health of the subscription is inspectable via
 * {@link OptionOiTracker#snapshot()} ({@code activeStrikeCount}).
 */
@Service
public class OptionOiSubscriber {

    private static final Logger log = LoggerFactory.getLogger(OptionOiSubscriber.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_STEP  = 50L;
    /** Number of strikes above AND below the ATM to subscribe for OI tracking.
     *  Hard-coded (not configurable) — the OI chart's ±15 window is fixed by design. */
    private static final int    STRIKES_EACH_SIDE = 15;
    /** How many extra strikes to request from Fyers on each side of the target range —
     *  small buffer so the chain covers the ATM ± N window even when Fyers rounds the
     *  requested count. */
    private static final int    CHAIN_BUFFER_STRIKES = 5;

    private final FyersClientRouter    fyersClient;
    private final TokenStore           tokenStore;
    private final FyersProperties      fyersProperties;
    private final MarketDataService    marketDataService;
    private final OptionOiTracker      oiTracker;

    public OptionOiSubscriber(FyersClientRouter fyersClient,
                              TokenStore        tokenStore,
                              FyersProperties   fyersProperties,
                              MarketDataService marketDataService,
                              OptionOiTracker   oiTracker) {
        this.fyersClient       = fyersClient;
        this.tokenStore        = tokenStore;
        this.fyersProperties   = fyersProperties;
        this.marketDataService = marketDataService;
        this.oiTracker         = oiTracker;
    }

    @PostConstruct
    public void boot() {
        // Bridge MarketDataService OI ticks → OptionOiTracker. Registered once; the tracker
        // filters by active window so pre-baseline ticks are safely ignored.
        marketDataService.addOiListener(tick ->
            oiTracker.onOiTick(tick.fyersSymbol(), tick.oi(), tick.exchFeedTimeSec()));
        log.info("[OptionOiSubscriber] booted — waiting for AtmVwap to select today's ATM");
    }

    /** Called by AtmVwap.resolveAtmFromFirstBar right after the day's ATM is locked.
     *  Idempotent — safe to call multiple times per day (tracker's setActiveWindow is a
     *  no-op when the window hasn't changed). */
    public synchronized void onAtmSelected(long atm) {
        if (atm <= 0) return;
        if (!tokenStore.isTokenAvailable()) {
            log.warn("[OptionOiSubscriber] ATM={} but Fyers token unavailable — skipping OI window setup", atm);
            return;
        }

        List<OptionOiTracker.StrikeSymbols> window = resolveWindowSymbols(atm, STRIKES_EACH_SIDE);
        if (window.isEmpty()) {
            log.warn("[OptionOiSubscriber] chain fetch resolved 0 strikes around ATM={} — OI tracker will stay idle today", atm);
            return;
        }

        List<String> newSymbols = oiTracker.setActiveWindow(atm, window);
        if (!newSymbols.isEmpty()) {
            marketDataService.subscribeAdditional(newSymbols);
        }
        log.info("[OptionOiSubscriber] OI window ATM={} ±{}, resolved {} strikes, subscribed {} net-new symbols",
            atm, STRIKES_EACH_SIDE, window.size(), newSymbols.size());
    }

    /** One chain fetch → walk strikes in [ATM − N·50, ATM + N·50] step 50 → pair each
     *  strike with its CE and PE Fyers symbol from the chain response. Strikes where
     *  either leg is missing are skipped. */
    private List<OptionOiTracker.StrikeSymbols> resolveWindowSymbols(long atm, int strikesEachSide) {
        List<OptionOiTracker.StrikeSymbols> out = new ArrayList<>();
        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
        int chainFetchCount = strikesEachSide + CHAIN_BUFFER_STRIKES;
        JsonNode root;
        try {
            root = fyersClient.getOptionChain(NIFTY_SYMBOL, chainFetchCount, auth);
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

        long lo = atm - (long) strikesEachSide * STRIKE_STEP;
        long hi = atm + (long) strikesEachSide * STRIKE_STEP;
        for (long s = lo; s <= hi; s += STRIKE_STEP) {
            String[] pair = byStrike.get(s);
            if (pair == null || pair[0] == null || pair[1] == null) continue;
            out.add(new OptionOiTracker.StrikeSymbols(s, pair[0], pair[1]));
        }
        return out;
    }
}
