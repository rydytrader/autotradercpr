package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lifecycle owner for the OI tracker's option-chain WebSocket subscription.
 *
 * <p>Trigger: {@link #onAtmSelected(long)} is called once per session by
 * {@code AtmVwap.resolveAtmFromFirstBar} right after it locks the day's ATM (~09:18
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
    private static final ZoneId IST          = ZoneId.of("Asia/Kolkata");
    /** Number of strikes above AND below the ATM to subscribe for OI tracking.
     *  ±7 → 15 strikes total (ATM ±7). Kept narrow so the ΔCE / ΔPE cumulative
     *  reflects near-the-money OI flow only; deep OTM strikes have small OI
     *  swings that mostly add noise. Hard-coded (not configurable) by design. */
    private static final int    STRIKES_EACH_SIDE = 7;
    /** How many extra strikes to request from Fyers on each side of the target range —
     *  small buffer so the chain covers the ATM ± N window even when Fyers rounds the
     *  requested count. */
    private static final int    CHAIN_BUFFER_STRIKES = 5;

    /** Per-day idempotency guard — remember the ATM already provisioned so repeated
     *  calls from AtmVwap's re-entry paths (every 3-min NIFTY candle close hits the
     *  same-day early-return branch that also fires notifyOiWindow) don't burn a chain
     *  fetch and spam the log. Reset when the day rolls. */
    private volatile long   lastAtmSubscribed = 0;
    private volatile String lastDayKey        = "";

    private final FyersClientRouter    fyersClient;
    private final TokenStore           tokenStore;
    private final FyersProperties      fyersProperties;
    private final MarketDataService    marketDataService;
    private final OptionOiTracker      oiTracker;
    private final MarketHolidayService marketHolidayService;
    private final org.springframework.beans.factory.ObjectProvider<com.rydytrader.autotrader.gdfl.GdflProperties> gdflPropertiesProvider;

    public OptionOiSubscriber(FyersClientRouter fyersClient,
                              TokenStore        tokenStore,
                              FyersProperties   fyersProperties,
                              MarketDataService marketDataService,
                              OptionOiTracker   oiTracker,
                              MarketHolidayService marketHolidayService,
                              org.springframework.beans.factory.ObjectProvider<com.rydytrader.autotrader.gdfl.GdflProperties> gdflPropertiesProvider) {
        this.fyersClient          = fyersClient;
        this.tokenStore           = tokenStore;
        this.fyersProperties      = fyersProperties;
        this.marketDataService    = marketDataService;
        this.oiTracker            = oiTracker;
        this.marketHolidayService = marketHolidayService;
        this.gdflPropertiesProvider = gdflPropertiesProvider;
    }

    /** True when the alternate feed (GDFL) is enabled and owns the option-tick
     *  stream. In that mode Fyers-side WS subscribes for CE/PE are dead weight
     *  — MarketDataService.onTick drops those ticks at ingress. */
    private boolean gdflOwnsOptionTicks() {
        var props = gdflPropertiesProvider == null ? null : gdflPropertiesProvider.getIfAvailable();
        return props != null && props.isEnabled();
    }

    @PostConstruct
    public void boot() {
        // Bridge MarketDataService OI ticks → OptionOiTracker. Registered once; the tracker
        // filters by active window so pre-baseline ticks are safely ignored.
        marketDataService.addOiListener(tick ->
            oiTracker.onOiTick(tick.fyersSymbol(), tick.oi(), tick.exchFeedTimeSec()));
        log.info("[OptionOiSubscriber] booted — waiting for AtmVwap to select today's ATM");
    }

    /** Called by {@code AtmVwap.warmupIfDue} at 09:15 IST — right after pre-warm has
     *  subscribed the ±15-strike WS symbols. Hands that same window to the tracker so
     *  per-strike OI baselines are captured from the very first tick (09:15) instead of
     *  waiting for the 09:18 ATM lock.
     *
     *  <p>Does NOT set {@link #lastAtmSubscribed} / {@link #lastDayKey} — those track the
     *  final post-ATM-lock provisioning path. When {@link #onAtmSelected} fires at 09:18
     *  it will narrow the window from ±15 down to ±7, and {@code setActiveWindow}
     *  transparently drops the outer strikes while preserving baselines + latest OI for
     *  the ones that stay. Also does NOT call {@code subscribeAdditional} — pre-warm has
     *  already pushed the symbols to the WS.
     *
     *  <p>Safe to call multiple times a day; only the first non-empty call per day
     *  actually captures 09:15 baselines because rolloverIfNewDay + on-disk state means
     *  a second call would just re-issue the same routing map. */
    public synchronized void onPreWarm(long baseAtm, List<OptionOiTracker.StrikeSymbols> window) {
        if (window == null || window.isEmpty()) return;
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;
        oiTracker.setActiveWindow(baseAtm, window);
        log.info("[OptionOiSubscriber] pre-warm OI window baseAtm={} strikes={} — baselines will be taken from first 09:15 tick",
            baseAtm, window.size());
    }

    /** Called by AtmVwap.resolveAtmFromFirstBar right after the day's ATM is locked.
     *  Idempotent — safe to call multiple times per day (tracker's setActiveWindow is a
     *  no-op when the window hasn't changed).
     *
     *  <p>Operator rule (2026-07-31): the OI bias tracker stays pinned to the
     *  PRE-WARM ATM (locked at 09:15) rather than shifting to the resolved
     *  ATM at 09:18. If {@link #onPreWarm} already established an active
     *  window today, this call short-circuits — no chain re-fetch, no
     *  window narrowing, no un-crediting of strikes that would have left
     *  the ±7 window. The wider ±10 pre-warm window remains the basis for
     *  cumulative CE/PE change and bias evaluation all day. */
    public synchronized void onAtmSelected(long atm) {
        if (atm <= 0) return;
        // NSE closed — weekend or listed holiday. AtmVwap's boot-time re-entry into
        // resolveAtmFromFirstBar can still fire this hook from a persisted state file,
        // but there's no OI feed to consume today so no need to burn a chain fetch or
        // spam subscribe calls at the WS.
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) {
            log.info("[OptionOiSubscriber] skipping OI window setup — not a trading day");
            return;
        }
        // Pre-warm-pinned OI window. If onPreWarm already set an active window,
        // leave it in place — the operator wants OI bias computed against the
        // pre-warm ATM's ±10 strikes, not narrowed to the resolved ATM's ±7.
        // Fallback path (pre-warm didn't fire because bot booted after 09:15,
        // or LTP was unavailable) still provisions the ±7 window below.
        if (oiTracker.getActiveAtmStrike() > 0) {
            log.info("[OptionOiSubscriber] onAtmSelected({}) — pre-warm window at ATM={} already active, keeping it (OI bias stays pinned to pre-warm ATM)",
                atm, oiTracker.getActiveAtmStrike());
            return;
        }
        // Idempotency guard — AtmVwap fires notifyOiWindow from three code paths:
        //   1. resolveAtmFromFirstBar fast path (first 3-min close)
        //   2. resolveAtmFromFirstBar slow path
        //   3. same-day re-entry (every subsequent 3-min NIFTY close)
        // Path 3 fires ~180 times a day with an unchanged ATM. Skip silently after the
        // first successful provisioning per day. Day rollover re-arms the check.
        String today = LocalDate.now(IST).toString();
        if (atm == lastAtmSubscribed && today.equals(lastDayKey)) {
            log.debug("[OptionOiSubscriber] OI window ATM={} already provisioned for {}, skipping", atm, today);
            return;
        }
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
        // Push the FULL window at Fyers MarketDataService — but ONLY when GDFL
        // isn't the option-side feed. In GDFL mode these strikes are altFeed-owned
        // (GdflService.subscribeOne marks them at subscribe time), so a Fyers WS
        // subscribe here would be dead weight — ticks would be dropped at
        // MarketDataService.onTick's altFeedOwnedSymbols guard.
        //
        // subscribeAdditional dedups internally, so in Fyers-only mode this is a
        // no-op for already-subscribed symbols but essential after a mid-day
        // restart (the tracker's routing map is restored from disk so
        // newSymbols comes back empty, but the fresh MarketDataService WS has
        // never subscribed these option legs — relying on newSymbols alone
        // starved the OI feed post-restart → ΔCE / ΔPE frozen).
        List<String> allSymbols = new ArrayList<>();
        for (OptionOiTracker.StrikeSymbols ss : window) {
            if (ss.ceSymbol() != null && !ss.ceSymbol().isBlank()) allSymbols.add(ss.ceSymbol());
            if (ss.peSymbol() != null && !ss.peSymbol().isBlank()) allSymbols.add(ss.peSymbol());
        }
        if (!allSymbols.isEmpty() && !gdflOwnsOptionTicks()) {
            marketDataService.subscribeAdditional(allSymbols);
        }
        lastAtmSubscribed = atm;
        lastDayKey        = today;
        log.info("[OptionOiSubscriber] OI window ATM={} ±{}, resolved {} strikes ({} WS symbols pushed, {} net-new to tracker)",
            atm, STRIKES_EACH_SIDE, window.size(), allSymbols.size(), newSymbols.size());
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
