package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.TelegramService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.strategy.LegSlStateStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * Short ATM straddle on NIFTY weekly options with PER-LEG SL and no rolls.
 *
 * <p>Lifecycle (gated by {@code strategies.leg-sl.enabled}):
 * <ol>
 *   <li>At {@code entryTime}: SELL ATM CE + ATM PE on the current weekly expiry → OPEN_BOTH.</li>
 *   <li>While OPEN_BOTH: on every tick check each leg's live LTP against its individual SL trigger
 *       (entry × (1 + legSlPct/100)). When one leg breaches, close only that leg →
 *       OPEN_CE_ONLY or OPEN_PE_ONLY (depending on which leg remains). No re-entry.</li>
 *   <li>While OPEN_*: the surviving leg keeps running. Its SL is still active. If it hits SL,
 *       close it and park DONE_FOR_DAY. Daily max-loss is also checked.</li>
 *   <li>At {@code squareOffTime}: close any leg that's still open → DONE_FOR_DAY.</li>
 * </ol>
 *
 * <p>Independent of {@code RollingStraddleService} — separate state file, separate sessions row,
 * separate dashboard. Both can run in parallel on the same ATM strike; Fyers nets the broker
 * position but each strategy tracks its own logical qty and places orders against that own qty.
 */
@Service
@Order(1)
public class LegSlStrategy implements Strategy {

    public static final String STRATEGY_ID = "leg-sl";

    private static final Logger log = LoggerFactory.getLogger(LegSlStrategy.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int    NIFTY_LOT_SIZE = 65;
    private static final String NIFTY_SYMBOL   = "NSE:NIFTY50-INDEX";
    private static final int    STRIKE_STEP    = 50;

    /** Charge constants (NIFTY weekly options, FY 2025-26) — same as RollingStraddleService. */
    private static final double STT_SELL_PCT   = 0.000625;
    private static final double EXCH_TXN_PCT   = 0.0003503;
    private static final double GST_PCT        = 0.18;
    private static final double SEBI_PER_CRORE = 10.0;
    private static final double STAMP_BUY_PCT  = 0.00003;

    public enum LifecycleState { IDLE, OPEN_BOTH, OPEN_PE_ONLY, OPEN_CE_ONLY, DONE_FOR_DAY }

    // ── Dependencies ───────────────────────────────────────────────────────────
    private final RiskSettingsStore riskSettings;
    private final LegSlStateStore stateStore;
    private final EventService eventService;
    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final FyersClientRouter fyersClient;

    @Autowired @Lazy private MarketDataService marketDataService;
    @Autowired @Lazy private OrderService orderService;
    @Autowired @Lazy private MarketHolidayService marketHolidayService;
    @Autowired @Lazy private TelegramService telegramService;
    @Autowired @Lazy private com.rydytrader.autotrader.repository.StraddleSessionRepository sessionRepo;
    @Autowired @Lazy private com.rydytrader.autotrader.repository.StraddleTradeRepository tradeRepo;

    // ── In-memory state ───────────────────────────────────────────────────────
    private volatile LifecycleState state = LifecycleState.IDLE;
    private volatile String dayKey   = "";
    private volatile String ceSymbol = "";
    private volatile String peSymbol = "";
    private volatile int    ceQty    = 0;
    private volatile int    peQty    = 0;
    private volatile String ceOrderId = "";
    private volatile String peOrderId = "";
    private volatile double ceEntryPremium = 0;
    private volatile double peEntryPremium = 0;
    private volatile long   ceClosedAtMillis = 0;
    private volatile long   peClosedAtMillis = 0;
    private volatile double realisedPnlToday = 0;
    private volatile double sellPremiumTurnoverToday = 0;
    private volatile double buyPremiumTurnoverToday  = 0;
    private volatile int    orderCountToday = 0;
    private volatile String currentWeeklyExpiry = "";

    private final java.util.Deque<CycleEvent> recentEvents = new java.util.ArrayDeque<>();
    private final java.util.List<java.util.Map<String, Object>> combinedPremiumSamples =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    public static record CycleEvent(String time, String event, double nifty,
                                    String ce, String pe, double pnl) {}

    public LegSlStrategy(RiskSettingsStore riskSettings,
                         LegSlStateStore stateStore,
                         EventService eventService,
                         TokenStore tokenStore,
                         FyersProperties fyersProperties,
                         FyersClientRouter fyersClient) {
        this.riskSettings = riskSettings;
        this.stateStore = stateStore;
        this.eventService = eventService;
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.fyersClient = fyersClient;
    }

    // ── Strategy interface ─────────────────────────────────────────────────────
    @Override public String id()           { return STRATEGY_ID; }
    @Override public String displayName()  { return "Leg-wise SL"; }
    @Override public String description()  { return "ATM straddle on NIFTY weekly · per-leg SL · no rolls, surviving leg runs to squareoff"; }
    @Override public String currentState() { return state.name(); }
    @Override public String navIcon()      { return "L"; }
    @Override public boolean forceClose(String reason) { return forceCloseAll(reason); }
    @Override public String currentWeeklyExpiry() { return currentWeeklyExpiry; }

    /** Live net day P&L (realised + open MTM − charges). Used by the portfolio kill switch. */
    @Override
    public double liveNetPnlToday() {
        if (marketDataService == null) return realisedPnlToday;
        double ceLtp = (isCeOpen() && !ceSymbol.isEmpty()) ? marketDataService.getLtp(ceSymbol) : 0;
        double peLtp = (isPeOpen() && !peSymbol.isEmpty()) ? marketDataService.getLtp(peSymbol) : 0;
        double ceMtm = (isCeOpen() && ceEntryPremium > 0 && ceLtp > 0 && ceQty > 0) ? (ceEntryPremium - ceLtp) * ceQty : 0;
        double peMtm = (isPeOpen() && peEntryPremium > 0 && peLtp > 0 && peQty > 0) ? (peEntryPremium - peLtp) * peQty : 0;
        double charges = computeChargesBreakdown().getOrDefault("total", 0.0);
        return realisedPnlToday + ceMtm + peMtm - charges;
    }

    @Override
    public java.util.List<java.util.Map<String, Object>> getSettingsSchema() {
        java.util.List<java.util.Map<String, Object>> s = new java.util.ArrayList<>();
        s.add(field("enabled",       "boolean", false,   "Enable Strategy",
            "Bot only fires entries / SL checks when enabled. Disable to pause without losing today's state."));
        s.add(field("entryTime",     "time",    "09:20", "Entry Time (HH:mm IST)", null));
        s.add(field("squareOffTime", "time",    "15:15", "Squareoff Time (HH:mm IST)", null));
        s.add(field("lotsPerLeg",    "int",      1,      "Lots per Leg",
            "Qty = lots × NIFTY lot size (65). Independent from the combined-roll strategy."));
        s.add(field("legSlPct",      "percent",  50,     "Per-leg SL (%)",
            "Close that leg when its LTP rises by this % from entry. Other leg keeps running."));
        return s;
    }

    private static java.util.Map<String, Object> field(String key, String type, Object def, String label, String hint) {
        java.util.Map<String, Object> f = new java.util.LinkedHashMap<>();
        f.put("key", key); f.put("type", type); f.put("default", def); f.put("label", label);
        if (hint != null) f.put("hint", hint);
        return f;
    }

    @Override
    public java.util.Map<String, Object> getSettingsValues() {
        java.util.Map<String, Object> v = new java.util.LinkedHashMap<>();
        v.put("enabled",       riskSettings.getStrategyBool(STRATEGY_ID,   "enabled",       false));
        v.put("entryTime",     riskSettings.getStrategyString(STRATEGY_ID, "entryTime",     "09:20"));
        v.put("squareOffTime", riskSettings.getStrategyString(STRATEGY_ID, "squareOffTime", "15:15"));
        v.put("lotsPerLeg",    riskSettings.getStrategyInt(STRATEGY_ID,    "lotsPerLeg",    1));
        v.put("legSlPct",      riskSettings.getStrategyDouble(STRATEGY_ID, "legSlPct",      50));
        return v;
    }

    @Override
    public void saveSettings(java.util.Map<String, Object> values) {
        if (values == null) return;
        if (values.containsKey("enabled"))       riskSettings.setStrategySetting(STRATEGY_ID, "enabled",       asBool(values.get("enabled")));
        if (values.containsKey("entryTime"))     riskSettings.setStrategySetting(STRATEGY_ID, "entryTime",     String.valueOf(values.get("entryTime")));
        if (values.containsKey("squareOffTime")) riskSettings.setStrategySetting(STRATEGY_ID, "squareOffTime", String.valueOf(values.get("squareOffTime")));
        if (values.containsKey("lotsPerLeg"))    riskSettings.setStrategySetting(STRATEGY_ID, "lotsPerLeg",    asInt(values.get("lotsPerLeg"), 1));
        if (values.containsKey("legSlPct"))      riskSettings.setStrategySetting(STRATEGY_ID, "legSlPct",      asDouble(values.get("legSlPct"), 50));
        riskSettings.saveFor("live");
    }

    // ── Boot resume ────────────────────────────────────────────────────────────
    @PostConstruct
    public void init() {
        LegSlStateStore.State p = stateStore.get();
        if (p != null && p.state != null) {
            try { this.state = LifecycleState.valueOf(p.state); }
            catch (IllegalArgumentException ex) { this.state = LifecycleState.IDLE; }
            this.dayKey         = p.dayKey != null ? p.dayKey : "";
            this.ceSymbol       = p.ceSymbol != null ? p.ceSymbol : "";
            this.peSymbol       = p.peSymbol != null ? p.peSymbol : "";
            this.ceQty          = p.ceQty;
            this.peQty          = p.peQty;
            this.ceOrderId      = p.ceOrderId != null ? p.ceOrderId : "";
            this.peOrderId      = p.peOrderId != null ? p.peOrderId : "";
            this.ceEntryPremium = p.ceEntryPremium;
            this.peEntryPremium = p.peEntryPremium;
            this.ceClosedAtMillis = p.ceClosedAtMillis;
            this.peClosedAtMillis = p.peClosedAtMillis;
            this.realisedPnlToday = p.realisedPnlToday;
            this.sellPremiumTurnoverToday = p.sellPremiumTurnoverToday;
            this.buyPremiumTurnoverToday  = p.buyPremiumTurnoverToday;
            this.orderCountToday = p.orderCountToday;
            this.currentWeeklyExpiry = p.currentWeeklyExpiry != null ? p.currentWeeklyExpiry : "";
            if (p.combinedPremiumSamples != null) {
                this.combinedPremiumSamples.clear();
                this.combinedPremiumSamples.addAll(p.combinedPremiumSamples);
            }
            if (p.recentEvents != null) {
                this.recentEvents.clear();
                for (java.util.Map<String, Object> r : p.recentEvents) {
                    this.recentEvents.addLast(new CycleEvent(
                        String.valueOf(r.getOrDefault("time", "")),
                        String.valueOf(r.getOrDefault("event", "")),
                        ((Number) r.getOrDefault("nifty", 0)).doubleValue(),
                        String.valueOf(r.getOrDefault("ce", "")),
                        String.valueOf(r.getOrDefault("pe", "")),
                        ((Number) r.getOrDefault("pnl", 0)).doubleValue()
                    ));
                }
            }
            log.info("[leg-sl] Resumed state={} dayKey={} ce={} pe={} ceEntry={} peEntry={} realised={}",
                state, dayKey, ceSymbol, peSymbol, ceEntryPremium, peEntryPremium, realisedPnlToday);
        }
        rolloverIfNewDay();
        // Re-subscribe still-open legs to the WS.
        if (state == LifecycleState.OPEN_BOTH || state == LifecycleState.OPEN_CE_ONLY
                || state == LifecycleState.OPEN_PE_ONLY) {
            java.util.List<String> resub = new java.util.ArrayList<>();
            if (isCeOpen() && !ceSymbol.isEmpty()) resub.add(ceSymbol);
            if (isPeOpen() && !peSymbol.isEmpty()) resub.add(peSymbol);
            if (!resub.isEmpty()) {
                try {
                    marketDataService.subscribeAdditional(resub);
                    log.info("[leg-sl] Re-subscribed open legs to data WS after restart: {}", resub);
                    for (String s : resub) seedLegQuote(s);
                } catch (Exception e) {
                    log.warn("[leg-sl] Re-subscribe failed: {}", e.getMessage());
                }
            }
        }
    }

    // ── 1-min combined-premium sampler (drives the dashboard chart) ───────────
    @Scheduled(cron = "0 * * * * MON-FRI", zone = "Asia/Kolkata")
    public void sampleCombinedPremium() {
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;
        if (state != LifecycleState.OPEN_BOTH && state != LifecycleState.OPEN_CE_ONLY
                && state != LifecycleState.OPEN_PE_ONLY) return;
        LocalTime now = LocalTime.now(IST);
        LocalTime entryTime     = parseTime(getEntryTime(),     "09:20");
        LocalTime squareOffTime = parseTime(getSquareOffTime(), "15:15");
        if (now.isBefore(entryTime) || now.isAfter(squareOffTime.plusMinutes(5))) return;
        double ceLtp = (isCeOpen() && !ceSymbol.isEmpty()) ? marketDataService.getLtp(ceSymbol) : 0;
        double peLtp = (isPeOpen() && !peSymbol.isEmpty()) ? marketDataService.getLtp(peSymbol) : 0;
        double total = (ceLtp > 0 ? ceLtp : 0) + (peLtp > 0 ? peLtp : 0);
        if (total <= 0) return;
        // Sample includes per-leg fields so the leg-sl chart can plot CE and PE as separate
        // side-by-side lines. The summed {v} is also kept for any reader that only needs the
        // combined value.
        java.util.Map<String, Object> sample = new java.util.LinkedHashMap<>();
        sample.put("t", now.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm")));
        sample.put("v",  Math.round(total * 100.0) / 100.0);
        sample.put("ce", ceLtp > 0 ? Math.round(ceLtp * 100.0) / 100.0 : null);
        sample.put("pe", peLtp > 0 ? Math.round(peLtp * 100.0) / 100.0 : null);
        combinedPremiumSamples.add(sample);
        persist();
    }

    // ── Main scheduler tick ────────────────────────────────────────────────────
    @Scheduled(fixedDelay = 5000)
    public void tick() {
        rolloverIfNewDay();
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        // Enabled-flag guard. Default false — operator opts in via Settings → LEG SL.
        if (!riskSettings.getStrategyBool(STRATEGY_ID, "enabled", false)) return;
        // Late-recover entry premium from tradebook if init() couldn't (no token at boot).
        tryRecoverEntryPremiumFromTradebook();

        LocalTime now = LocalTime.now(IST);
        LocalTime entryTime     = parseTime(getEntryTime(),     "09:20");
        LocalTime squareOffTime = parseTime(getSquareOffTime(), "15:15");
        if (!squareOffTime.isAfter(entryTime)) {
            log.warn("[leg-sl] squareOffTime {} must be after entryTime {} — skipping tick",
                squareOffTime, entryTime);
            return;
        }

        switch (state) {
            case IDLE -> {
                if (afterOrAt(now, entryTime) && now.isBefore(squareOffTime)) {
                    doInitialEntry();
                }
            }
            case OPEN_BOTH, OPEN_CE_ONLY, OPEN_PE_ONLY -> checkLegSlOrSquareoff(now, squareOffTime);
            case DONE_FOR_DAY -> { /* idle until tomorrow */ }
        }
    }

    // ── Initial entry ──────────────────────────────────────────────────────────
    private void doInitialEntry() {
        double niftyLtp = marketDataService.getLtp(NIFTY_SYMBOL);
        if (niftyLtp <= 0) {
            log.info("[leg-sl] Skipping entry — NIFTY LTP unavailable (waiting for first tick)");
            return;
        }
        long atmStrike = Math.round(niftyLtp / STRIKE_STEP) * (long) STRIKE_STEP;
        String[] symbols = resolveAtmSymbols(atmStrike);
        if (symbols == null) {
            log.warn("[leg-sl] Could not resolve ATM CE+PE symbols for strike {} — aborting day", atmStrike);
            eventService.log("[ERROR] [leg-sl] entry aborted — failed to resolve ATM symbols for strike " + atmStrike);
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        String resolvedCe = symbols[0], resolvedPe = symbols[1];
        int qty = Math.max(1, riskSettings.getStrategyInt(STRATEGY_ID, "lotsPerLeg", 1)) * NIFTY_LOT_SIZE;

        OrderDTO ceResp = orderService.placeOrder(resolvedCe, qty, -1, 0);
        orderCountToday++;
        if (ceResp == null || ceResp.getId() == null || ceResp.getId().isEmpty() || !"ok".equals(ceResp.getStatus())) {
            log.error("[leg-sl] CE leg rejected: {}", ceResp);
            eventService.log("[ERROR] [leg-sl] CE leg rejected — aborting day");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        OrderDTO peResp = orderService.placeOrder(resolvedPe, qty, -1, 0);
        orderCountToday++;
        if (peResp == null || peResp.getId() == null || peResp.getId().isEmpty() || !"ok".equals(peResp.getStatus())) {
            log.error("[leg-sl] PE leg rejected after CE filled — flattening CE: {}", peResp);
            eventService.log("[ERROR] [leg-sl] PE leg rejected — buying back CE to flatten");
            try { orderService.placeOrder(resolvedCe, qty, 1, 0); orderCountToday++; } catch (Exception ignored) {}
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }

        this.ceSymbol = resolvedCe;
        this.peSymbol = resolvedPe;
        this.ceQty = qty;
        this.peQty = qty;
        this.ceOrderId = ceResp.getId();
        this.peOrderId = peResp.getId();
        this.ceClosedAtMillis = 0;
        this.peClosedAtMillis = 0;
        try { marketDataService.subscribeAdditional(java.util.Arrays.asList(resolvedCe, resolvedPe)); }
        catch (Exception ignored) {}
        this.ceEntryPremium = readEntryPremium(resolvedCe);
        this.peEntryPremium = readEntryPremium(resolvedPe);
        this.currentWeeklyExpiry = parseExpiryFromSymbol(resolvedCe);
        this.sellPremiumTurnoverToday += (ceEntryPremium * qty) + (peEntryPremium * qty);
        transitionTo(LifecycleState.OPEN_BOTH);

        double niftyAtEntry = niftyLtp;
        pushEvent("ENTRY", niftyAtEntry, resolvedCe, resolvedPe, 0);

        String msg = "leg-sl armed @ ATM " + atmStrike + " (NIFTY " + String.format("%.2f", niftyLtp)
            + ") qty=" + qty + " ce=" + ceSymbol + " pe=" + peSymbol;
        log.info("[leg-sl] {}", msg);
        eventService.log("[INFO] [leg-sl] " + msg);
        notifyTelegram(msg);
    }

    // ── Per-leg SL + timed squareoff ───────────────────────────────────────────
    private void checkLegSlOrSquareoff(LocalTime now, LocalTime squareOffTime) {
        if (afterOrAt(now, squareOffTime)) {
            closeRemainingLegs("TIMED_SQUAREOFF");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        if (checkMaxLossKillSwitch()) return;
        // SL triggers are also evaluated by the 500ms fast scheduler — calling here covers
        // the case where fastSlCheck is disabled / paused and ensures the 5s path is still
        // protective. Both paths funnel through the synchronized closeLeg/transition so a
        // race is fine.
        checkLegSlTriggers();
    }

    /** Pure SL-trigger check — fast path, run by both the 5s scheduler and the 500ms scheduler.
     *  No squareoff time check, no max-loss check (those stay on the 5s path). Just live LTP
     *  vs per-leg trigger; fires close on breach. */
    private synchronized void checkLegSlTriggers() {
        if (state != LifecycleState.OPEN_BOTH
                && state != LifecycleState.OPEN_CE_ONLY
                && state != LifecycleState.OPEN_PE_ONLY) return;
        double legSlPct = riskSettings.getStrategyDouble(STRATEGY_ID, "legSlPct", 50);
        if (isCeOpen() && ceEntryPremium > 0 && !ceSymbol.isEmpty()) {
            double ceLtp = marketDataService.getLtp(ceSymbol);
            if (ceLtp > 0) {
                double trigger = ceEntryPremium * (1.0 + legSlPct / 100.0);
                if (ceLtp >= trigger) {
                    double consumedPct = ((ceLtp - ceEntryPremium) / ceEntryPremium) * 100.0;
                    String msg = String.format("CE leg SL hit — entry %.2f, live %.2f (+%.2f%%, threshold %.2f%%). Closing CE only.",
                        ceEntryPremium, ceLtp, consumedPct, legSlPct);
                    log.info("[leg-sl] {}", msg);
                    eventService.log("[INFO] [leg-sl] " + msg);
                    notifyTelegram(msg);
                    closeLeg("CE", "CE_SL_HIT");
                    return;
                }
            }
        }
        if (isPeOpen() && peEntryPremium > 0 && !peSymbol.isEmpty()) {
            double peLtp = marketDataService.getLtp(peSymbol);
            if (peLtp > 0) {
                double trigger = peEntryPremium * (1.0 + legSlPct / 100.0);
                if (peLtp >= trigger) {
                    double consumedPct = ((peLtp - peEntryPremium) / peEntryPremium) * 100.0;
                    String msg = String.format("PE leg SL hit — entry %.2f, live %.2f (+%.2f%%, threshold %.2f%%). Closing PE only.",
                        peEntryPremium, peLtp, consumedPct, legSlPct);
                    log.info("[leg-sl] {}", msg);
                    eventService.log("[INFO] [leg-sl] " + msg);
                    notifyTelegram(msg);
                    closeLeg("PE", "PE_SL_HIT");
                    return;
                }
            }
        }
        if (!isCeOpen() && !isPeOpen()) {
            transitionTo(LifecycleState.DONE_FOR_DAY);
        }
    }

    /** Fast tick — runs every 500ms during market hours, only does the per-leg SL trigger
     *  check. Detection latency drops from ~5s (slow tick) to ~500ms. Cheap: just reads
     *  LTPs from the in-memory tick cache and compares to per-leg thresholds. */
    @Scheduled(fixedDelay = 500)
    public void fastSlCheck() {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        if (!riskSettings.getStrategyBool(STRATEGY_ID, "enabled", false)) return;
        checkLegSlTriggers();
    }

    /** Close just the named leg ("CE" or "PE"), update state to the surviving-leg state, persist. */
    private void closeLeg(String which, String reason) {
        boolean isCe = "CE".equalsIgnoreCase(which);
        String symbol = isCe ? ceSymbol : peSymbol;
        int qty       = isCe ? ceQty    : peQty;
        double entry  = isCe ? ceEntryPremium : peEntryPremium;
        if (symbol == null || symbol.isEmpty() || qty <= 0) return;

        double closeLtp = marketDataService.getLtp(symbol);
        double pnl = (entry > 0 && closeLtp > 0) ? (entry - closeLtp) * qty : 0;
        realisedPnlToday += pnl;
        if (closeLtp > 0) buyPremiumTurnoverToday += closeLtp * qty;
        double niftyAtClose = marketDataService.getLtp(NIFTY_SYMBOL);
        String closedCe = isCe ? symbol : "";
        String closedPe = isCe ? "" : symbol;

        placeCloseRetry(symbol, qty, which, reason);
        // Unsubscribe — the other leg keeps its WS sub.
        try { marketDataService.unsubscribeAdditional(java.util.Collections.singletonList(symbol)); }
        catch (Exception ignored) {}

        pushEvent("CLOSE_" + reason, niftyAtClose, closedCe, closedPe, pnl);
        String msg = which + " leg closed (" + reason + "): " + symbol + " qty=" + qty
            + " pnl=" + String.format("%.2f", pnl);
        log.info("[leg-sl] {}", msg);
        eventService.log("[INFO] [leg-sl] " + msg);

        long nowMs = System.currentTimeMillis();
        if (isCe) {
            this.ceClosedAtMillis = nowMs;
            this.ceQty = 0;
            this.ceOrderId = "";
            // CE closed → if PE still open we move to OPEN_PE_ONLY; if PE already closed we're done.
            transitionTo(isPeOpen() ? LifecycleState.OPEN_PE_ONLY : LifecycleState.DONE_FOR_DAY);
        } else {
            this.peClosedAtMillis = nowMs;
            this.peQty = 0;
            this.peOrderId = "";
            transitionTo(isCeOpen() ? LifecycleState.OPEN_CE_ONLY : LifecycleState.DONE_FOR_DAY);
        }
    }

    /** Close every leg still open. Used by timed squareoff + manual squareoff. */
    private void closeRemainingLegs(String reason) {
        java.util.List<String> unsubAfter = new java.util.ArrayList<>();
        double niftyAtClose = marketDataService.getLtp(NIFTY_SYMBOL);
        double totalPnl = 0;
        if (isCeOpen() && !ceSymbol.isEmpty() && ceQty > 0) {
            double ltp = marketDataService.getLtp(ceSymbol);
            double pnl = (ceEntryPremium > 0 && ltp > 0) ? (ceEntryPremium - ltp) * ceQty : 0;
            realisedPnlToday += pnl;
            totalPnl += pnl;
            if (ltp > 0) buyPremiumTurnoverToday += ltp * ceQty;
            placeCloseRetry(ceSymbol, ceQty, "CE", reason);
            unsubAfter.add(ceSymbol);
            this.ceClosedAtMillis = System.currentTimeMillis();
            this.ceQty = 0;
            this.ceOrderId = "";
        }
        if (isPeOpen() && !peSymbol.isEmpty() && peQty > 0) {
            double ltp = marketDataService.getLtp(peSymbol);
            double pnl = (peEntryPremium > 0 && ltp > 0) ? (peEntryPremium - ltp) * peQty : 0;
            realisedPnlToday += pnl;
            totalPnl += pnl;
            if (ltp > 0) buyPremiumTurnoverToday += ltp * peQty;
            placeCloseRetry(peSymbol, peQty, "PE", reason);
            unsubAfter.add(peSymbol);
            this.peClosedAtMillis = System.currentTimeMillis();
            this.peQty = 0;
            this.peOrderId = "";
        }
        if (!unsubAfter.isEmpty()) {
            try { marketDataService.unsubscribeAdditional(unsubAfter); } catch (Exception ignored) {}
        }
        if (!unsubAfter.isEmpty()) {
            pushEvent("CLOSE_" + reason, niftyAtClose, ceSymbol, peSymbol, totalPnl);
            String msg = "leg-sl remaining legs closed (" + reason + "): " + String.join(", ", unsubAfter)
                + " pnl=" + String.format("%.2f", totalPnl);
            log.info("[leg-sl] {}", msg);
            eventService.log("[INFO] [leg-sl] " + msg);
            notifyTelegram(msg);
        }
        persist();
    }

    /** BUY close order with one retry. */
    private void placeCloseRetry(String symbol, int qty, String legTag, String reason) {
        try {
            OrderDTO resp = orderService.placeOrder(symbol, qty, 1, 0);
            orderCountToday++;
            if (resp != null && resp.getId() != null && !resp.getId().isEmpty() && "ok".equals(resp.getStatus())) return;
            log.warn("[leg-sl] First close attempt failed for {} {} ({}): {} — retrying in 2s",
                legTag, symbol, reason, resp);
            try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            OrderDTO retry = orderService.placeOrder(symbol, qty, 1, 0);
            orderCountToday++;
            if (retry == null || retry.getId() == null || retry.getId().isEmpty() || !"ok".equals(retry.getStatus())) {
                log.error("[leg-sl] CLOSE FAILED for {} {} qty={} ({}): {}",
                    legTag, symbol, qty, reason, retry);
                eventService.log("[ERROR] [leg-sl] CLOSE FAILED for " + legTag + " " + symbol
                    + " qty=" + qty + " — manual intervention required");
            }
        } catch (Exception e) {
            log.error("[leg-sl] Exception closing {} {}: {}", legTag, symbol, e.getMessage());
        }
    }

    // ── Max loss kill switch ──────────────────────────────────────────────────
    private boolean checkMaxLossKillSwitch() {
        // Derived from portfolioMaxDailyLoss × this strategy's allocation %.
        double maxLoss = riskSettings.getStrategyMaxDailyLoss(STRATEGY_ID);
        if (maxLoss <= 0) return false;
        double ceLtp = isCeOpen() && !ceSymbol.isEmpty() ? marketDataService.getLtp(ceSymbol) : 0;
        double peLtp = isPeOpen() && !peSymbol.isEmpty() ? marketDataService.getLtp(peSymbol) : 0;
        double ceMtm = (isCeOpen() && ceEntryPremium > 0 && ceLtp > 0 && ceQty > 0) ? (ceEntryPremium - ceLtp) * ceQty : 0;
        double peMtm = (isPeOpen() && peEntryPremium > 0 && peLtp > 0 && peQty > 0) ? (peEntryPremium - peLtp) * peQty : 0;
        double charges = computeChargesBreakdown().get("total");
        double netPnl = realisedPnlToday + ceMtm + peMtm - charges;
        if (netPnl < -maxLoss) {
            String msg = String.format("Daily max-loss hit (net %.2f < -%.2f) — flattening remaining legs",
                netPnl, maxLoss);
            log.warn("[leg-sl] {}", msg);
            eventService.log("[ERROR] [leg-sl] " + msg);
            notifyTelegram(msg);
            closeRemainingLegs("MAX_LOSS_HIT");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return true;
        }
        return false;
    }

    // ── Charges ────────────────────────────────────────────────────────────────
    private java.util.Map<String, Double> computeChargesBreakdown() {
        int projectedOrders = orderCountToday;
        double projectedBuyT = buyPremiumTurnoverToday;
        if (marketDataService != null) {
            if (isCeOpen() && !ceSymbol.isEmpty() && ceQty > 0) {
                double ltp = marketDataService.getLtp(ceSymbol);
                if (ltp > 0) projectedBuyT += ltp * ceQty;
                projectedOrders++;
            }
            if (isPeOpen() && !peSymbol.isEmpty() && peQty > 0) {
                double ltp = marketDataService.getLtp(peSymbol);
                if (ltp > 0) projectedBuyT += ltp * peQty;
                projectedOrders++;
            }
        }
        double brokerage = projectedOrders * riskSettings.getBrokeragePerOrder();
        double sellT = sellPremiumTurnoverToday;
        double buyT  = projectedBuyT;
        double totalT = sellT + buyT;
        double stt        = sellT * STT_SELL_PCT;
        double exchange   = totalT * EXCH_TXN_PCT;
        double sebi       = (totalT / 10_000_000.0) * SEBI_PER_CRORE;
        double stamp      = buyT * STAMP_BUY_PCT;
        double gst        = (brokerage + exchange) * GST_PCT;
        double total      = brokerage + stt + exchange + sebi + stamp + gst;
        java.util.Map<String, Double> b = new java.util.LinkedHashMap<>();
        b.put("brokerage", round2(brokerage));
        b.put("stt",       round2(stt));
        b.put("exchange",  round2(exchange));
        b.put("sebi",      round2(sebi));
        b.put("stamp",     round2(stamp));
        b.put("gst",       round2(gst));
        b.put("total",     round2(total));
        b.put("sellTurnover", round2(sellT));
        b.put("buyTurnover",  round2(buyT));
        return b;
    }

    // ── Dashboard payload (leg-sl shape) ───────────────────────────────────────
    @Override
    public java.util.Map<String, Object> getDashboard() {
        rolloverIfNewDay();
        if (currentWeeklyExpiry == null || currentWeeklyExpiry.isEmpty()) {
            tryResolveWeeklyExpiry();
        }
        java.util.Map<String, Object> m = getStatus();
        m.put("dashboardShape",  "leg-sl");
        m.put("weeklyExpiry",    currentWeeklyExpiry);
        m.put("daysToExpiry",    tradingDaysToExpiry(currentWeeklyExpiry));
        synchronized (combinedPremiumSamples) {
            m.put("combinedPremiumSamples", new java.util.ArrayList<>(combinedPremiumSamples));
        }
        if (marketDataService != null) {
            m.put("niftyDisplayLtp", round2(marketDataService.getDisplayLtp(NIFTY_SYMBOL)));
            m.put("niftyChange",     round2(marketDataService.getDisplayChange(NIFTY_SYMBOL)));
            m.put("niftyChangePct",  round2(marketDataService.getDisplayChangePct(NIFTY_SYMBOL)));
            String vix = "NSE:INDIAVIX-INDEX";
            m.put("vixDisplayLtp",   round2(marketDataService.getDisplayLtp(vix)));
            m.put("vixChange",       round2(marketDataService.getDisplayChange(vix)));
            m.put("vixChangePct",    round2(marketDataService.getDisplayChangePct(vix)));
            if (ceSymbol != null && !ceSymbol.isEmpty()) {
                m.put("ceChange",    round2(marketDataService.getDisplayChange(ceSymbol)));
                m.put("ceChangePct", round2(marketDataService.getDisplayChangePct(ceSymbol)));
            }
            if (peSymbol != null && !peSymbol.isEmpty()) {
                m.put("peChange",    round2(marketDataService.getDisplayChange(peSymbol)));
                m.put("peChangePct", round2(marketDataService.getDisplayChangePct(peSymbol)));
            }
        }
        double niftyLtp = marketDataService != null ? marketDataService.getLtp(NIFTY_SYMBOL) : 0;
        m.put("niftyLtp", niftyLtp);

        double ceLtp = (!ceSymbol.isEmpty()) ? marketDataService.getLtp(ceSymbol) : 0;
        double peLtp = (!peSymbol.isEmpty()) ? marketDataService.getLtp(peSymbol) : 0;
        m.put("ceLtp", ceLtp);
        m.put("peLtp", peLtp);
        m.put("ceEntryPremium", ceEntryPremium);
        m.put("peEntryPremium", peEntryPremium);
        m.put("ceClosed", !isCeOpen());
        m.put("peClosed", !isPeOpen());
        m.put("ceClosedAtMillis", ceClosedAtMillis);
        m.put("peClosedAtMillis", peClosedAtMillis);

        double ceMtm = (isCeOpen() && ceEntryPremium > 0 && ceLtp > 0 && ceQty > 0) ? (ceEntryPremium - ceLtp) * ceQty : 0;
        double peMtm = (isPeOpen() && peEntryPremium > 0 && peLtp > 0 && peQty > 0) ? (peEntryPremium - peLtp) * peQty : 0;
        m.put("ceMtm", round2(ceMtm));
        m.put("peMtm", round2(peMtm));
        m.put("combinedMtm", round2(ceMtm + peMtm));
        m.put("realisedPnlToday", round2(realisedPnlToday));
        m.put("totalPnlToday", round2(realisedPnlToday + ceMtm + peMtm));

        // Per-leg SL triggers + consumed % (replaces combined SL in the leg-sl dashboard).
        double legSlPct = riskSettings.getStrategyDouble(STRATEGY_ID, "legSlPct", 50);
        m.put("legSlPct", legSlPct);
        // Worst-case loss for the currently-running straddle if both legs hit their SLs. Per-leg
        // loss at SL = entryPremium × legSlPct/100 × qty. Total = sum of both legs (qty same for
        // each leg). Skip closed legs — their loss is realised, not future.
        double maxLossPerStraddle = 0;
        int legQty = Math.max(ceQty, peQty);
        if (legSlPct > 0 && legQty > 0) {
            if (isCeOpen() && ceEntryPremium > 0) maxLossPerStraddle += ceEntryPremium * (legSlPct / 100.0) * legQty;
            if (isPeOpen() && peEntryPremium > 0) maxLossPerStraddle += peEntryPremium * (legSlPct / 100.0) * legQty;
        }
        m.put("maxLossPerStraddle", round2(maxLossPerStraddle));
        if (ceEntryPremium > 0) {
            double ceTrigger = ceEntryPremium * (1.0 + legSlPct / 100.0);
            m.put("ceSlTrigger", round2(ceTrigger));
            double consumed = isCeOpen() && ceLtp > 0
                ? ((ceLtp - ceEntryPremium) / (ceTrigger - ceEntryPremium)) * 100.0 : 0;
            m.put("ceSlConsumedPct", round2(consumed));
        } else {
            m.put("ceSlTrigger", 0.0);
            m.put("ceSlConsumedPct", 0.0);
        }
        if (peEntryPremium > 0) {
            double peTrigger = peEntryPremium * (1.0 + legSlPct / 100.0);
            m.put("peSlTrigger", round2(peTrigger));
            double consumed = isPeOpen() && peLtp > 0
                ? ((peLtp - peEntryPremium) / (peTrigger - peEntryPremium)) * 100.0 : 0;
            m.put("peSlConsumedPct", round2(consumed));
        } else {
            m.put("peSlTrigger", 0.0);
            m.put("peSlConsumedPct", 0.0);
        }

        java.util.Map<String, Double> charges = computeChargesBreakdown();
        m.put("charges", charges);
        m.put("netPnlToday", round2(realisedPnlToday - charges.get("total")));

        java.util.List<java.util.Map<String, Object>> events = new java.util.ArrayList<>();
        for (CycleEvent e : recentEvents) {
            java.util.Map<String, Object> rm = new java.util.LinkedHashMap<>();
            rm.put("time",  e.time());
            rm.put("event", e.event());
            rm.put("nifty", e.nifty());
            rm.put("ce",    e.ce());
            rm.put("pe",    e.pe());
            rm.put("pnl",   round2(e.pnl()));
            events.add(rm);
        }
        m.put("recentRolls", events); // reuse the same key so the existing UI table just works
        return m;
    }

    @Override
    public java.util.Map<String, Object> getStatus() {
        java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("state",         state.name());
        m.put("dayKey",        dayKey);
        m.put("ceSymbol",      ceSymbol);
        m.put("peSymbol",      peSymbol);
        m.put("ceQty",         ceQty);
        m.put("peQty",         peQty);
        m.put("ceOrderId",     ceOrderId);
        m.put("peOrderId",     peOrderId);
        m.put("entryTime",     getEntryTime());
        m.put("squareOffTime", getSquareOffTime());
        m.put("legSlPct",      riskSettings.getStrategyDouble(STRATEGY_ID, "legSlPct", 50));
        m.put("lotsPerLeg",    riskSettings.getStrategyInt(STRATEGY_ID, "lotsPerLeg", 1));
        m.put("maxDailyLoss",  riskSettings.getStrategyMaxDailyLoss(STRATEGY_ID));
        m.put("lotSize",       NIFTY_LOT_SIZE);
        m.put("enabled",       riskSettings.getStrategyBool(STRATEGY_ID, "enabled", false));
        return m;
    }

    // ── Manual controls ────────────────────────────────────────────────────────
    public synchronized boolean forceCloseAll(String reason) {
        if (state != LifecycleState.OPEN_BOTH && state != LifecycleState.OPEN_CE_ONLY
                && state != LifecycleState.OPEN_PE_ONLY) {
            log.info("[leg-sl] forceClose ignored — state={}", state);
            return false;
        }
        eventService.log("[INFO] [leg-sl] Manual squareoff (" + reason + ") — flattening any open legs");
        closeRemainingLegs(reason);
        transitionTo(LifecycleState.DONE_FOR_DAY);
        return true;
    }

    /** Hard-stop for the day. Closes any open legs AND parks DONE_FOR_DAY regardless of
     *  state, so an IDLE leg-sl that hasn't entered yet won't fire its entry later. */
    @Override
    public synchronized void parkDoneForDay(String reason) {
        if (state == LifecycleState.DONE_FOR_DAY) return;
        boolean hadOpen = (state == LifecycleState.OPEN_BOTH
                          || state == LifecycleState.OPEN_CE_ONLY
                          || state == LifecycleState.OPEN_PE_ONLY);
        if (hadOpen) {
            eventService.log("[INFO] [leg-sl] Portfolio kill (" + reason + ") — flattening + parking");
            closeRemainingLegs(reason);
        } else {
            eventService.log("[INFO] [leg-sl] Portfolio kill (" + reason + ") — parking from state=" + state + " (no open position)");
        }
        transitionTo(LifecycleState.DONE_FOR_DAY);
    }

    @Override
    public synchronized void resetToIdle(String reason) {
        log.info("[leg-sl] Manual reset from {} → IDLE ({})", state, reason);
        eventService.log("[INFO] [leg-sl] state reset to IDLE (" + reason + ")");
        this.ceSymbol = ""; this.peSymbol = "";
        this.ceQty = 0; this.peQty = 0;
        this.ceOrderId = ""; this.peOrderId = "";
        this.ceClosedAtMillis = 0; this.peClosedAtMillis = 0;
        transitionTo(LifecycleState.IDLE);
    }

    // ── Day rollover + session persistence ────────────────────────────────────
    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(dayKey)) return;
        if (state == LifecycleState.OPEN_BOTH || state == LifecycleState.OPEN_CE_ONLY
                || state == LifecycleState.OPEN_PE_ONLY) {
            log.warn("[leg-sl] Stale state {} from {} detected at startup — flattening before reset",
                state, dayKey);
            eventService.log("[WARNING] [leg-sl] stale " + state + " from " + dayKey + " — flattening");
            closeRemainingLegs("STALE_DAY_RESET");
        }
        if (dayKey != null && !dayKey.isEmpty() && realisedPnlToday != 0) {
            try { persistSessionFor(dayKey); }
            catch (Exception e) { log.warn("[leg-sl] Failed to persist session row for {}: {}", dayKey, e.getMessage()); }
        }
        this.dayKey = today;
        this.ceSymbol = ""; this.peSymbol = "";
        this.ceQty = 0; this.peQty = 0;
        this.ceOrderId = ""; this.peOrderId = "";
        this.ceEntryPremium = 0; this.peEntryPremium = 0;
        this.ceClosedAtMillis = 0; this.peClosedAtMillis = 0;
        this.realisedPnlToday = 0;
        this.sellPremiumTurnoverToday = 0;
        this.buyPremiumTurnoverToday = 0;
        this.orderCountToday = 0;
        this.currentWeeklyExpiry = "";
        this.recentEvents.clear();
        this.combinedPremiumSamples.clear();
        transitionTo(LifecycleState.IDLE);
    }

    private void persistSessionFor(String date) {
        if (sessionRepo == null) return;
        java.util.Map<String, Double> chargesBreakdown = computeChargesBreakdown();
        double charges = chargesBreakdown.get("total");
        double gross   = realisedPnlToday;
        double net     = gross - charges;
        com.rydytrader.autotrader.entity.StraddleSessionEntity row =
            sessionRepo.findByStrategyIdAndSessionDate(STRATEGY_ID, date)
                       .orElseGet(com.rydytrader.autotrader.entity.StraddleSessionEntity::new);
        row.setStrategyId(STRATEGY_ID);
        row.setSessionDate(date);
        row.setEntries(ceEntryPremium > 0 || peEntryPremium > 0 || realisedPnlToday != 0 ? 1 : 0);
        row.setRolls(0); // leg-sl never rolls
        row.setFinalState(state.name());
        row.setPremiumCollected(round2(sellPremiumTurnoverToday));
        row.setPremiumPaidBack(round2(buyPremiumTurnoverToday));
        row.setGrossPnl(gross);
        row.setCharges(charges);
        row.setNetPnl(net);
        if (row.getCreatedAt() == 0) row.setCreatedAt(System.currentTimeMillis());
        sessionRepo.save(row);
        log.info("[leg-sl] Persisted session row for {}: gross={} net={}", date, gross, net);
    }

    // ── Helpers — symbol resolution + premium reads + persistence ─────────────
    private String[] resolveAtmSymbols(long atmStrike) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getOptionChain(NIFTY_SYMBOL, 30, auth);
            if (root == null) return null;
            JsonNode data = root.has("data") ? root.get("data") : null;
            JsonNode chain = data != null && data.has("optionsChain") ? data.get("optionsChain")
                : (root.has("optionsChain") ? root.get("optionsChain") : null);
            if (chain == null || !chain.isArray()) return null;
            String ce = null, pe = null;
            for (JsonNode row : chain) {
                double strike = row.has("strike_price") ? row.get("strike_price").asDouble()
                    : row.has("strikePrice") ? row.get("strikePrice").asDouble() : 0;
                if (Math.round(strike) != atmStrike) continue;
                String optType = row.has("option_type") ? row.get("option_type").asText()
                    : row.has("optionType") ? row.get("optionType").asText() : "";
                String sym = row.has("symbol") ? row.get("symbol").asText() : "";
                if (sym.isEmpty()) continue;
                if ("CE".equalsIgnoreCase(optType)) ce = sym;
                else if ("PE".equalsIgnoreCase(optType)) pe = sym;
            }
            if (ce == null || pe == null) {
                log.warn("[leg-sl] Could not find both CE and PE for ATM strike {} (ce={}, pe={})", atmStrike, ce, pe);
                return null;
            }
            return new String[]{ ce, pe };
        } catch (Exception e) {
            log.error("[leg-sl] Option chain fetch failed: {}", e.getMessage());
            return null;
        }
    }

    private void tryResolveWeeklyExpiry() {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            if (auth == null || auth.endsWith(":") || auth.endsWith(":null")) return;
            JsonNode root = fyersClient.getOptionChain(NIFTY_SYMBOL, 4, auth);
            if (root == null) return;
            JsonNode data = root.has("data") ? root.get("data") : null;
            JsonNode chain = data != null && data.has("optionsChain") ? data.get("optionsChain")
                : (root.has("optionsChain") ? root.get("optionsChain") : null);
            if (chain == null || !chain.isArray()) return;
            for (JsonNode row : chain) {
                String sym = row.has("symbol") ? row.get("symbol").asText() : "";
                String exp = parseExpiryFromSymbol(sym);
                if (!exp.isEmpty()) { this.currentWeeklyExpiry = exp; return; }
            }
        } catch (Exception ignored) {}
    }

    private int tradingDaysToExpiry(String expiryIso) {
        if (expiryIso == null || expiryIso.isEmpty()) return -1;
        try {
            LocalDate expiry = LocalDate.parse(expiryIso);
            LocalDate today  = LocalDate.now(IST);
            if (expiry.isBefore(today)) return -1;
            int count = 0;
            LocalDate cursor = today.plusDays(1);
            while (!cursor.isAfter(expiry)) {
                if (marketHolidayService == null || marketHolidayService.isTradingDay(cursor)) count++;
                cursor = cursor.plusDays(1);
            }
            return count;
        } catch (Exception e) { return -1; }
    }

    static String parseExpiryFromSymbol(String fyersSymbol) {
        if (fyersSymbol == null) return "";
        try {
            int hash = fyersSymbol.indexOf("NIFTY");
            if (hash < 0) return "";
            String tail = fyersSymbol.substring(hash + 5);
            if (tail.length() < 5) return "";
            int yr = Integer.parseInt(tail.substring(0, 2));
            char monthCh = tail.charAt(2);
            int month;
            if (monthCh >= '1' && monthCh <= '9') month = monthCh - '0';
            else if (monthCh == 'O') month = 10;
            else if (monthCh == 'N') month = 11;
            else if (monthCh == 'D') month = 12;
            else return "";
            int day = Integer.parseInt(tail.substring(3, 5));
            return LocalDate.of(2000 + yr, month, day).toString();
        } catch (Exception e) { return ""; }
    }

    private double readEntryPremium(String symbol) {
        try {
            double ltp = marketDataService.getLtp(symbol);
            if (ltp > 0) return ltp;
        } catch (Exception ignored) {}
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getQuotes(symbol, auth);
            if (root != null && root.has("d") && root.get("d").isArray() && root.get("d").size() > 0) {
                JsonNode v = root.get("d").get(0).path("v");
                double lp        = v.path("lp").asDouble(0);
                double prevClose = v.path("prev_close_price").asDouble(0);
                if (lp > 0) {
                    marketDataService.seedTickData(symbol, lp, prevClose);
                    log.info("[leg-sl] Entry premium for {} captured via REST quote: lp={} prevClose={}",
                        symbol, lp, prevClose);
                    return lp;
                }
            }
        } catch (Exception e) {
            log.warn("[leg-sl] REST quote fallback failed for {}: {}", symbol, e.getMessage());
        }
        return 0;
    }

    private void seedLegQuote(String symbol) {
        if (symbol == null || symbol.isEmpty()) return;
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getQuotes(symbol, auth);
            if (root != null && root.has("d") && root.get("d").isArray() && root.get("d").size() > 0) {
                JsonNode v = root.get("d").get(0).path("v");
                double lp = v.path("lp").asDouble(0);
                double prevClose = v.path("prev_close_price").asDouble(0);
                if (lp > 0) marketDataService.seedTickData(symbol, lp, prevClose);
            }
        } catch (Exception e) {
            log.warn("[leg-sl] Seed leg quote failed for {}: {}", symbol, e.getMessage());
        }
    }

    private void tryRecoverEntryPremiumFromTradebook() {
        if (state != LifecycleState.OPEN_BOTH && state != LifecycleState.OPEN_CE_ONLY
                && state != LifecycleState.OPEN_PE_ONLY) return;
        if (ceEntryPremium > 0 && peEntryPremium > 0) return;
        if (tokenStore.getAccessToken() == null || tokenStore.getAccessToken().isEmpty()) return;
        boolean changed = false;
        if (ceEntryPremium == 0 && ceOrderId != null && !ceOrderId.isEmpty()) {
            try {
                double fill = orderService.getFilledPriceByOrderId(ceOrderId);
                if (fill > 0) {
                    ceEntryPremium = fill;
                    log.info("[leg-sl] Recovered CE entry premium from tradebook: {} (orderId={})", fill, ceOrderId);
                    changed = true;
                }
            } catch (Exception ignored) {}
        }
        if (peEntryPremium == 0 && peOrderId != null && !peOrderId.isEmpty()) {
            try {
                double fill = orderService.getFilledPriceByOrderId(peOrderId);
                if (fill > 0) {
                    peEntryPremium = fill;
                    log.info("[leg-sl] Recovered PE entry premium from tradebook: {} (orderId={})", fill, peOrderId);
                    changed = true;
                }
            } catch (Exception ignored) {}
        }
        if (changed) {
            if (sellPremiumTurnoverToday == 0 && ceEntryPremium > 0 && peEntryPremium > 0
                    && ceQty > 0 && peQty > 0) {
                sellPremiumTurnoverToday = (ceEntryPremium * ceQty) + (peEntryPremium * peQty);
            }
            if (orderCountToday == 0) orderCountToday = 2;
            persist();
        }
    }

    private void pushEvent(String evt, double nifty, String ce, String pe, double pnl) {
        String ts = LocalTime.now(IST).format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        recentEvents.addFirst(new CycleEvent(ts, evt, nifty, ce, pe, pnl));
        while (recentEvents.size() > 20) recentEvents.removeLast();
    }

    private void transitionTo(LifecycleState next) {
        LifecycleState prev = this.state;
        this.state = next;
        // First entry into DONE_FOR_DAY for today → straddle just finished. Write one
        // straddle_trades row capturing the day's realisedPnlToday + accumulated charges.
        if (next == LifecycleState.DONE_FOR_DAY && prev != LifecycleState.DONE_FOR_DAY) {
            persistStraddleTrade();
        }
        persist();
    }

    /** Write a single straddle row for today's straddle. Called once when the state first
     *  transitions to DONE_FOR_DAY. Skipped if there was no activity (no entry — realised P&L
     *  and turnover both ~0). */
    private void persistStraddleTrade() {
        if (tradeRepo == null) return;
        if (Math.abs(realisedPnlToday) < 0.01 && sellPremiumTurnoverToday < 0.01) return;
        try {
            double charges = computeCycleCharges(sellPremiumTurnoverToday, buyPremiumTurnoverToday, orderCountToday);
            com.rydytrader.autotrader.entity.StraddleTradeEntity t =
                new com.rydytrader.autotrader.entity.StraddleTradeEntity();
            t.setStrategyId(STRATEGY_ID);
            t.setSessionDate(dayKey != null && !dayKey.isEmpty() ? dayKey : LocalDate.now(IST).toString());
            t.setClosedAtMillis(System.currentTimeMillis());
            // leg-sl uses whichever leg's qty is non-zero, or the last known. Both legs share qty
            // at entry so this is approximate when one leg was closed earlier.
            int qty = Math.max(ceQty, peQty);
            if (qty == 0) qty = Math.max(1, riskSettings.getStrategyInt(STRATEGY_ID, "lotsPerLeg", 1)) * NIFTY_LOT_SIZE;
            t.setQty(qty);
            t.setGrossPnl(round2(realisedPnlToday));
            t.setCharges(round2(charges));
            t.setNetPnl(round2(realisedPnlToday - charges));
            t.setCloseReason("DONE_FOR_DAY");
            tradeRepo.save(t);
        } catch (Exception e) {
            log.warn("[leg-sl] Failed to persist straddle_trades row: {}", e.getMessage());
        }
    }

    /** Same formula as the session-level breakdown but applied to a specific cycle's totals. */
    private double computeCycleCharges(double sellPrem, double buyPrem, int orders) {
        double brokerage = orders * riskSettings.getBrokeragePerOrder();
        double totalPrem = sellPrem + buyPrem;
        double stt       = sellPrem * STT_SELL_PCT;
        double exchange  = totalPrem * EXCH_TXN_PCT;
        double sebi      = (totalPrem / 10_000_000.0) * SEBI_PER_CRORE;
        double stamp     = buyPrem * STAMP_BUY_PCT;
        double gst       = (brokerage + exchange) * GST_PCT;
        return round2(brokerage + stt + exchange + sebi + stamp + gst);
    }

    private void persist() {
        LegSlStateStore.State s = new LegSlStateStore.State();
        s.dayKey = this.dayKey;
        s.state = this.state.name();
        s.ceSymbol = this.ceSymbol;
        s.peSymbol = this.peSymbol;
        s.ceQty = this.ceQty;
        s.peQty = this.peQty;
        s.ceOrderId = this.ceOrderId;
        s.peOrderId = this.peOrderId;
        s.ceEntryPremium = this.ceEntryPremium;
        s.peEntryPremium = this.peEntryPremium;
        s.ceClosedAtMillis = this.ceClosedAtMillis;
        s.peClosedAtMillis = this.peClosedAtMillis;
        s.realisedPnlToday = this.realisedPnlToday;
        s.sellPremiumTurnoverToday = this.sellPremiumTurnoverToday;
        s.buyPremiumTurnoverToday  = this.buyPremiumTurnoverToday;
        s.orderCountToday = this.orderCountToday;
        s.currentWeeklyExpiry = this.currentWeeklyExpiry;
        synchronized (combinedPremiumSamples) {
            s.combinedPremiumSamples = new java.util.ArrayList<>(combinedPremiumSamples);
        }
        java.util.List<java.util.Map<String, Object>> events = new java.util.ArrayList<>();
        for (CycleEvent e : recentEvents) {
            java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("time", e.time());
            m.put("event", e.event());
            m.put("nifty", e.nifty());
            m.put("ce", e.ce());
            m.put("pe", e.pe());
            m.put("pnl", e.pnl());
            events.add(m);
        }
        s.recentEvents = events;
        stateStore.update(s);
    }

    private void notifyTelegram(String msg) {
        try { if (telegramService != null) telegramService.sendMessage("[leg-sl] " + msg); }
        catch (Exception ignored) {}
    }

    private String getEntryTime()     { return riskSettings.getStrategyString(STRATEGY_ID, "entryTime",     "09:20"); }
    private String getSquareOffTime() { return riskSettings.getStrategyString(STRATEGY_ID, "squareOffTime", "15:15"); }

    private boolean isCeOpen() {
        return state == LifecycleState.OPEN_BOTH || state == LifecycleState.OPEN_CE_ONLY;
    }
    private boolean isPeOpen() {
        return state == LifecycleState.OPEN_BOTH || state == LifecycleState.OPEN_PE_ONLY;
    }

    private static LocalTime parseTime(String hhmm, String fallback) {
        try { return LocalTime.parse((hhmm == null || hhmm.isBlank()) ? fallback : hhmm.trim()); }
        catch (Exception e) {
            log.warn("[leg-sl] Failed to parse time \"{}\" — falling back to {}", hhmm, fallback);
            return LocalTime.parse(fallback);
        }
    }
    private static boolean afterOrAt(LocalTime a, LocalTime b) { return !a.isBefore(b); }
    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private static int asInt(Object o, int def) {
        if (o == null) return def;
        try { return Integer.parseInt(String.valueOf(o).trim()); } catch (NumberFormatException e) { return def; }
    }
    private static double asDouble(Object o, double def) {
        if (o == null) return def;
        try { return Double.parseDouble(String.valueOf(o).trim()); } catch (NumberFormatException e) { return def; }
    }
    private static boolean asBool(Object o) {
        if (o == null) return false;
        if (o instanceof Boolean b) return b;
        return Boolean.parseBoolean(String.valueOf(o).trim());
    }
}
