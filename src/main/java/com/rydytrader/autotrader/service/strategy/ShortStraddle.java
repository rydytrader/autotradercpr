package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StraddleInstanceEntity;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.TelegramService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.strategy.ShortStraddleStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * Short ATM straddle on NIFTY weekly options with PER-LEG SL and no rolls. Instances of this
 * class are constructed at runtime by {@code StraddleInstanceManager} — one per row of the
 * {@code STRADDLE_INSTANCES} table. NOT a Spring bean: every dependency comes via the
 * constructor; the manager owns the {@code @Scheduled} fan-out via {@code StraddleScheduler}.
 *
 * <p>Lifecycle (gated by {@code strategies.<instanceId>.enabled}):
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
 * <p>State file, sessions row and dashboard are scoped to this strategy via its {@code id()},
 * which is {@code "inst-" + entity.getId()}.
 */
public class ShortStraddle implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(ShortStraddle.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int    NIFTY_LOT_SIZE = 65;
    private static final String NIFTY_SYMBOL   = "NSE:NIFTY50-INDEX";
    private static final int    STRIKE_STEP    = 50;

    /** Charge constants (NIFTY weekly options, FY 2025-26). */
    private static final double STT_SELL_PCT   = 0.000625;
    private static final double EXCH_TXN_PCT   = 0.0003503;
    private static final double GST_PCT        = 0.18;
    private static final double SEBI_PER_CRORE = 10.0;
    private static final double STAMP_BUY_PCT  = 0.00003;

    public enum LifecycleState { IDLE, OPEN_BOTH, OPEN_PE_ONLY, OPEN_CE_ONLY, DONE_FOR_DAY }

    // ── Instance identity (mutable via syncFromEntity()) ──────────────────────
    private final String instanceId;            // "inst-<entityId>" — never changes after construction
    private volatile String displayName;
    private volatile String description;
    private volatile String shortCode;

    // ── Dependencies ───────────────────────────────────────────────────────────
    private final RiskSettingsStore riskSettings;
    private final ShortStraddleStateStore stateStore;
    private final EventService eventService;
    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final FyersClientRouter fyersClient;
    private final MarketDataService marketDataService;
    private final OrderService orderService;
    private final MarketHolidayService marketHolidayService;
    private final TelegramService telegramService;
    private final com.rydytrader.autotrader.repository.StraddleSessionRepository sessionRepo;
    private final com.rydytrader.autotrader.repository.StraddleTradeRepository tradeRepo;
    private final OrderEventService orderEventService;
    private final BalancedAtmSelector atmSelector;

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
    /** Per-leg realised P&L frozen at the moment the leg closed. Surfaced on the dashboard's
     *  CE/PE leg card as the leg's "MTM" once the leg is closed (so the card shows the loss
     *  taken on that SL hit instead of resetting to 0). 0 while leg is still open. */
    private volatile double ceLegPnl = 0;
    private volatile double peLegPnl = 0;
    /** LTP captured at the moment the leg closed (SL hit / squareoff / etc.). Surfaced on the
     *  dashboard leg card so a closed leg shows "Entry / Exit" with the actual exit price
     *  instead of "Entry / LTP" with a stale live tick. 0 while leg is still open. */
    private volatile double ceClosePremium = 0;
    private volatile double peClosePremium = 0;
    private volatile double realisedPnlToday = 0;
    private volatile double sellPremiumTurnoverToday = 0;
    private volatile double buyPremiumTurnoverToday  = 0;
    private volatile int    orderCountToday = 0;
    /** Number of per-leg SL hits today (0, 1 or 2 per cycle). Increments when closeLeg fires
     *  with CE_SL_HIT or PE_SL_HIT, reset on day rollover. Cumulative across all cycles in
     *  a multi-straddle day; per-cycle delta is what gets persisted on each trade row. */
    private volatile int    slHitsToday = 0;

    // Per-cycle snapshots — captured at the start of every entry (scheduler or manual restart)
    // so persistStraddleTrade writes that cycle's delta rather than the cumulative day total.
    // Multi-cycle days produce one straddle_trades row per cycle.
    private volatile double cycleStartRealisedPnl    = 0;
    private volatile double cycleStartSellTurnover   = 0;
    private volatile double cycleStartBuyTurnover    = 0;
    private volatile int    cycleStartOrderCount     = 0;
    private volatile int    cycleStartSlHits         = 0;

    /** Day-level Consumed Risk — sum of every leg's realised P&L (signed) across every cycle
     *  today. Dashboard displays it only when negative (UI convention preserved from the old
     *  per-cycle closedLegsPnl). Reset on day rollover, persisted in the JSON state. */
    private volatile double consumedRiskToday        = 0;

    private volatile String currentWeeklyExpiry = "";
    /** NIFTY LTP captured at the moment this day's straddle was entered — surfaced on the
     *  dashboard's Hero "Last Entry" tile. 0 until first entry, cleared on day rollover. */
    private volatile double lastEntryNifty = 0;

    /** True once at least one tick has run today with the scheduler observing the pre-entry
     *  window (state==IDLE, now<entryTime). Reset on every day rollover. Used to detect a
     *  late start: when tick() first runs already past entry time without ever having seen
     *  the pre-entry window, the bot won't auto-fire — the operator must explicitly hit
     *  + NEW STRADDLE. Same gate also catches the case where the operator paused before
     *  entry time and unpaused after it. */
    private volatile boolean observedPreEntryWindow = false;

    private final java.util.Deque<CycleEvent> recentEvents = new java.util.ArrayDeque<>();
    private final java.util.List<java.util.Map<String, Object>> combinedPremiumSamples =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    public static record CycleEvent(String time, String event, double nifty,
                                    String ce, String pe, double pnl) {}

    /** Tracks which legs are awaiting a WS fill confirmation. State transitions happen
     *  synchronously; cumulative P&L / turnover updates happen ONLY when the WS fires the
     *  filled-status event. Equity-bot pattern: register the orderId + context, do the
     *  bookkeeping when the callback arrives. */
    private enum PendingType { ENTRY_CE, ENTRY_PE, CLOSE_CE, CLOSE_PE }
    private static record PendingFill(PendingType type, int qty, double entryRef) {}
    private final java.util.concurrent.ConcurrentMap<String, PendingFill> pendingFills =
        new java.util.concurrent.ConcurrentHashMap<>();
    /** True once {@link #bootstrap()} has wired the FillListener. */
    private volatile boolean fillListenerWired = false;

    // ── Balanced-ATM state ───────────────────────────────────────────────────
    /** Last selection result — surfaced on the dashboard so the operator sees the strike
     *  the synthetic-futures method picked vs the naïve spot/50 round. {@code null} pre-entry. */
    private volatile BalancedAtmSelector.AtmSelection lastAtmSelection = null;
    /** Cached pre-entry preview — populated lazily by {@link #getAtmPreview()} and refreshed
     *  every {@link #ATM_PREVIEW_TTL_MS}. Lets the dashboard publish the projected balanced
     *  ATM without re-fetching the option chain on every poll. */
    private volatile BalancedAtmSelector.AtmSelection cachedAtmPreview = null;
    private volatile long                              cachedAtmPreviewMs = 0;
    private static final long ATM_PREVIEW_TTL_MS = 30_000L;

    public ShortStraddle(StraddleInstanceEntity entity,
                         RiskSettingsStore riskSettings,
                         ShortStraddleStateStore stateStore,
                         EventService eventService,
                         TokenStore tokenStore,
                         FyersProperties fyersProperties,
                         FyersClientRouter fyersClient,
                         MarketDataService marketDataService,
                         OrderService orderService,
                         MarketHolidayService marketHolidayService,
                         TelegramService telegramService,
                         com.rydytrader.autotrader.repository.StraddleSessionRepository sessionRepo,
                         com.rydytrader.autotrader.repository.StraddleTradeRepository tradeRepo,
                         OrderEventService orderEventService,
                         BalancedAtmSelector atmSelector) {
        this.instanceId = entity.strategyId();
        this.displayName = entity.getName();
        this.description = entity.getDescription();
        this.shortCode = entity.getShortCode();
        this.riskSettings = riskSettings;
        this.stateStore = stateStore;
        this.eventService = eventService;
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.fyersClient = fyersClient;
        this.marketDataService = marketDataService;
        this.orderService = orderService;
        this.marketHolidayService = marketHolidayService;
        this.telegramService = telegramService;
        this.sessionRepo = sessionRepo;
        this.tradeRepo = tradeRepo;
        this.orderEventService = orderEventService;
        this.atmSelector = atmSelector;
    }

    /** Called by {@code StraddleInstanceManager} immediately after construction to load
     *  on-disk state, replay open-leg WS subs, and roll the day if needed. Mirrors the old
     *  {@code @PostConstruct init()} but is now invoked explicitly. */
    public void bootstrap() {
        init();
        // Register the async fill-correction listener so the order WS push can update our
        // estimated entry / close prices the moment the broker confirms. ShortStraddle is
        // long-lived (instance lifetime); no removeFillListener needed.
        if (!fillListenerWired) {
            orderEventService.addFillListener(this::onActualFill);
            fillListenerWired = true;
        }
    }

    /** Invoked on the WS thread the moment Fyers pushes an order's status=2 (Filled) event.
     *  All P&L / turnover bookkeeping happens here — synchronous placement code only handles
     *  state transitions. No delta math; the values are computed from scratch using the
     *  broker-confirmed fill price. */
    private synchronized void onActualFill(String orderId, double price) {
        PendingFill p = pendingFills.remove(orderId);
        if (p == null || price <= 0) return;
        switch (p.type()) {
            case ENTRY_CE -> {
                ceEntryPremium = price;
                sellPremiumTurnoverToday += price * p.qty();
                log.info("[short-straddle] CE entry filled @ {}", String.format("%.2f", price));
                eventService.log("[INFO] [short-straddle] CE entry filled @ " + String.format("%.2f", price));
            }
            case ENTRY_PE -> {
                peEntryPremium = price;
                sellPremiumTurnoverToday += price * p.qty();
                log.info("[short-straddle] PE entry filled @ {}", String.format("%.2f", price));
                eventService.log("[INFO] [short-straddle] PE entry filled @ " + String.format("%.2f", price));
            }
            case CLOSE_CE -> {
                double pnl = (p.entryRef() - price) * p.qty();
                ceLegPnl = pnl;
                ceClosePremium = price;
                realisedPnlToday += pnl;
                consumedRiskToday += pnl;
                buyPremiumTurnoverToday += price * p.qty();
                log.info("[short-straddle] CE close filled @ {} pnl={}", String.format("%.2f", price), String.format("%.2f", pnl));
                eventService.log("[INFO] [short-straddle] CE close filled @ " + String.format("%.2f", price) + " pnl=" + String.format("%.2f", pnl));
            }
            case CLOSE_PE -> {
                double pnl = (p.entryRef() - price) * p.qty();
                peLegPnl = pnl;
                peClosePremium = price;
                realisedPnlToday += pnl;
                consumedRiskToday += pnl;
                buyPremiumTurnoverToday += price * p.qty();
                log.info("[short-straddle] PE close filled @ {} pnl={}", String.format("%.2f", price), String.format("%.2f", pnl));
                eventService.log("[INFO] [short-straddle] PE close filled @ " + String.format("%.2f", price) + " pnl=" + String.format("%.2f", pnl));
            }
        }
        persist();
    }

    /** Register an orderId for fill-callback notification. Race protection: if the WS push
     *  already cached the fill before this call (rare — Fyers WS sometimes beats the REST
     *  response on liquid options), apply it immediately. */
    private void registerPendingFill(String orderId, PendingType type, int qty, double entryRef) {
        if (orderId == null || orderId.isEmpty()) return;
        pendingFills.put(orderId, new PendingFill(type, qty, entryRef));
        Double already = orderEventService.getFillPrice(orderId);
        if (already != null && already > 0) onActualFill(orderId, already);
    }

    /** Refresh display fields when the operator renames the instance via the Straddles tab. */
    public void syncFromEntity(StraddleInstanceEntity entity) {
        this.displayName = entity.getName();
        this.description = entity.getDescription();
        this.shortCode   = entity.getShortCode();
    }

    // ── Strategy interface ─────────────────────────────────────────────────────
    @Override public String id()           { return instanceId; }
    @Override public String displayName()  { return displayName; }
    @Override public String description()  { return description; }
    @Override public String shortCode()    { return shortCode; }
    @Override public String currentState() { return state.name(); }
    @Override public String navIcon()      { return shortCode != null && !shortCode.isEmpty() ? shortCode : "∧"; }
    @Override public boolean forceClose(String reason) { return forceCloseAll(reason); }

    /** Public entry point for the dashboard's per-leg Close buttons. Synchronized — funnels
     *  through the same close path the scheduler / SL check uses, so concurrent ticks won't
     *  race. Returns true when the requested leg was open and a close order was placed. */
    @Override
    public synchronized boolean closeOneLeg(String leg, String reason) {
        if (leg == null) return false;
        boolean isCe = "CE".equalsIgnoreCase(leg);
        boolean isPe = "PE".equalsIgnoreCase(leg);
        if (!isCe && !isPe) return false;
        if (isCe && !isCeOpen()) return false;
        if (isPe && !isPeOpen()) return false;
        String tag = isCe ? "CE_MANUAL" : "PE_MANUAL";
        eventService.log("[INFO] [" + instanceId + "] Manual " + (isCe ? "CE" : "PE")
            + " leg close from dashboard");
        closeLeg(isCe ? "CE" : "PE", tag);
        return true;
    }

    /** Public entry point for the dashboard's {@code + NEW STRADDLE} button. Validates every
     *  scheduler precondition except the {@code state == IDLE} gate (which is the whole
     *  point of this path), then funnels through the same {@link #performEntryNow} that the
     *  scheduler's initial entry uses. The frontend mirrors these gates client-side for the
     *  button's enable / disable state but the server is authoritative — a 409 from this
     *  method names the exact block. */
    @Override
    public String restartFromDoneForDay(String reason) {
        synchronized (this) {
            if (state != LifecycleState.DONE_FOR_DAY)        return "NOT_DONE_FOR_DAY";
            if (!isTodayDayEnabled())                         return "DAY_DISABLED";
            if (riskSettings.getStrategyBool(instanceId, "tradingPaused", false))
                                                              return "TRADING_PAUSED";
            LocalTime now = LocalTime.now(IST);
            LocalTime entryT     = parseTime(getEntryTime(),     "09:20");
            LocalTime squareOffT = parseTime(getSquareOffTime(), "15:15");
            if (now.isBefore(entryT))             return "BEFORE_ENTRY_TIME";
            if (!now.isBefore(squareOffT))        return "AFTER_SQUAREOFF_TIME";
            if (!pendingFills.isEmpty())          return "PENDING_FILLS";
            eventService.log("[INFO] [" + instanceId + "] Manual + NEW STRADDLE restart from dashboard ("
                + reason + ")");
            performEntryNow("ENTRY_MANUAL");
        }
        return "OK";
    }

    @Override public String currentWeeklyExpiry() { return currentWeeklyExpiry; }
    @Override public boolean isEnabled() {
        return riskSettings.getStrategyBool(instanceId, "enabled", false);
    }

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

    /** Total accrued + projected charges for today — sum of the breakdown's {@code total} key.
     *  Surfaced so the analytics live overlay can split today's totalCharges across closed
     *  live trade rows + the open-position synthetic row so per-day Charges + Gross match
     *  the dashboard exactly. */
    @Override
    public double liveChargesToday() {
        return computeChargesBreakdown().getOrDefault("total", 0.0);
    }

    /** Day-of-week trading slots, ordered by DTE (4 → 0). NIFTY weekly options expire Tuesday;
     *  each weekday entry has its own toggle and SL%. Keys are the 3-letter day codes used in
     *  the settings table — {@code strategies.<instanceId>.day.<DAY>.enabled / legSlPct}. */
    private static final java.util.List<String[]> WEEK_DAYS = java.util.List.of(
        new String[]{"WED", "4"},
        new String[]{"THU", "3"},
        new String[]{"FRI", "2"},
        new String[]{"MON", "1"},
        new String[]{"TUE", "0"}
    );

    @Override
    public java.util.List<java.util.Map<String, Object>> getSettingsSchema() {
        java.util.List<java.util.Map<String, Object>> s = new java.util.ArrayList<>();
        // 'enabled' is intentionally NOT a per-instance settings field. The Straddles tab in
        // the global Settings modal owns enable/disable so the operator has one consistent
        // place to flip an instance on or off; the per-instance ⚙ Settings dialog is for
        // trading config only (entry time, lots, per-day SL %, squareoff).
        s.add(field("entryTime",     "time",    "09:20", "Entry Time (HH:mm IST)", null));
        s.add(field("squareOffTime", "time",    "15:15", "Squareoff Time (HH:mm IST)", null));
        s.add(field("lotsPerLeg",    "int",      1,      "Lots per Leg",
            "Qty = lots × NIFTY lot size (65)."));
        // Dropdown — INTRADAY (Fyers MIS) or OVERNIGHT (Fyers MARGIN / NRML).
        java.util.Map<String, Object> orderTypeFld = new java.util.LinkedHashMap<>();
        orderTypeFld.put("key", "orderType");
        orderTypeFld.put("type", "select");
        orderTypeFld.put("default", "INTRADAY");
        orderTypeFld.put("label", "Order Type");
        orderTypeFld.put("options", java.util.List.of("INTRADAY", "OVERNIGHT"));
        s.add(orderTypeFld);
        // Per-day enable + SL %. Ordered by DTE (4 → 3 → 2 → 1 → 0). Defaults: every day on
        // at 50 %, matching the previous single-SL behaviour.
        for (String[] dk : WEEK_DAYS) {
            String day = dk[0]; String dte = dk[1];
            s.add(field("day." + day + ".enabled",  "boolean", true, day + " — " + dte + " DTE — Enable",
                "Enter straddle on " + day + " using the SL below. Disabled days are skipped."));
            s.add(field("day." + day + ".legSlPct",    "percent", 50, day + " — " + dte + " DTE — SL %", null));
            s.add(field("day." + day + ".legSlPoints", "double",  0,  day + " — " + dte + " DTE — SL Points",
                "Direct premium points added to entry. Takes precedence over SL % when > 0."));
        }
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
        v.put("entryTime",     riskSettings.getStrategyString(instanceId, "entryTime",     "09:20"));
        v.put("squareOffTime", riskSettings.getStrategyString(instanceId, "squareOffTime", "15:15"));
        v.put("lotsPerLeg",    riskSettings.getStrategyInt(instanceId,    "lotsPerLeg",    1));
        v.put("orderType",     riskSettings.getStrategyString(instanceId, "orderType",     "INTRADAY"));
        for (String[] dk : WEEK_DAYS) {
            String day = dk[0];
            v.put("day." + day + ".enabled",     riskSettings.getStrategyBool(instanceId,   "day." + day + ".enabled",     true));
            v.put("day." + day + ".legSlPct",    riskSettings.getStrategyDouble(instanceId, "day." + day + ".legSlPct",    50));
            v.put("day." + day + ".legSlPoints", riskSettings.getStrategyDouble(instanceId, "day." + day + ".legSlPoints", 0));
        }
        return v;
    }

    @Override
    public void saveSettings(java.util.Map<String, Object> values) {
        if (values == null) return;
        // Soft-pause toggle. Surfaced as a switch in the Today pane header. Saved through the
        // same generic settings endpoint so a single PATCH-ish POST flips it without a new route.
        // Event-log on actual transition so a no-op POST (no key, or same value) doesn't spam.
        if (values.containsKey("tradingPaused")) {
            boolean prior = riskSettings.getStrategyBool(instanceId, "tradingPaused", false);
            boolean next  = Boolean.parseBoolean(String.valueOf(values.get("tradingPaused")));
            if (prior != next) {
                riskSettings.setStrategySetting(instanceId, "tradingPaused", next);
                String msg = next
                    ? "Trading PAUSED — no auto-entry today, + NEW STRADDLE disabled"
                    : "Trading RESUMED — auto-entry re-enabled";
                eventService.log("[INFO] [" + instanceId + "] " + msg);
                log.info("[short-straddle] [{}] {}", instanceId, msg);
                notifyTelegram(msg);
            }
        }
        if (values.containsKey("entryTime"))     riskSettings.setStrategySetting(instanceId, "entryTime",     String.valueOf(values.get("entryTime")));
        if (values.containsKey("squareOffTime")) riskSettings.setStrategySetting(instanceId, "squareOffTime", String.valueOf(values.get("squareOffTime")));
        if (values.containsKey("lotsPerLeg"))    riskSettings.setStrategySetting(instanceId, "lotsPerLeg",    asInt(values.get("lotsPerLeg"), 1));
        if (values.containsKey("orderType")) {
            String ot = String.valueOf(values.get("orderType")).trim().toUpperCase();
            if (!"INTRADAY".equals(ot) && !"OVERNIGHT".equals(ot)) ot = "INTRADAY";
            riskSettings.setStrategySetting(instanceId, "orderType", ot);
        }
        for (String[] dk : WEEK_DAYS) {
            String day = dk[0];
            String enKey = "day." + day + ".enabled";
            String slKey = "day." + day + ".legSlPct";
            String ptKey = "day." + day + ".legSlPoints";
            if (values.containsKey(enKey)) riskSettings.setStrategySetting(instanceId, enKey, Boolean.parseBoolean(String.valueOf(values.get(enKey))));
            if (values.containsKey(slKey)) riskSettings.setStrategySetting(instanceId, slKey, asDouble(values.get(slKey), 50));
            if (values.containsKey(ptKey)) riskSettings.setStrategySetting(instanceId, ptKey, asDouble(values.get(ptKey), 0));
        }
        riskSettings.saveFor("live");
    }

    /** Fyers product type for this instance's orders. Mapped from the operator-facing
     *  {@code orderType} dropdown: {@code INTRADAY} → Fyers {@code INTRADAY} (MIS,
     *  auto-squareoff at exchange EOD); {@code OVERNIGHT} → Fyers {@code MARGIN}
     *  (NRML / held overnight). Defaults to INTRADAY. */
    private String productType() {
        String ot = riskSettings.getStrategyString(instanceId, "orderType", "INTRADAY");
        return "OVERNIGHT".equalsIgnoreCase(ot) ? "MARGIN" : "INTRADAY";
    }

    /** Returns today's day code (MON/TUE/WED/THU/FRI), or empty on weekends. */
    private String todayKey() {
        return switch (java.time.LocalDate.now(IST).getDayOfWeek()) {
            case MONDAY    -> "MON";
            case TUESDAY   -> "TUE";
            case WEDNESDAY -> "WED";
            case THURSDAY  -> "THU";
            case FRIDAY    -> "FRI";
            default        -> "";
        };
    }

    /** True when today's day-of-week is enabled in this instance's per-day settings. Returns
     *  false on weekends (matches the marketHolidayService check elsewhere). */
    private boolean isTodayDayEnabled() {
        String k = todayKey();
        if (k.isEmpty()) return false;
        return riskSettings.getStrategyBool(instanceId, "day." + k + ".enabled", true);
    }

    /** Per-leg SL % for the current weekday, or {@code null} when no day code applies
     *  (weekends — operator-facing UI renders this as "—" rather than a misleading 50 %).
     *  Internal callers that need a usable number on a weekend should fall back themselves. */
    private Double todayLegSlPct() {
        String k = todayKey();
        if (k.isEmpty()) return null;
        return riskSettings.getStrategyDouble(instanceId, "day." + k + ".legSlPct", 50);
    }

    /** Per-leg SL in absolute premium points for the current weekday, or {@code null} on a
     *  weekend. When > 0, points takes precedence over {@link #todayLegSlPct()} — the trigger
     *  is computed as {@code entryPremium + points} (linear) instead of
     *  {@code entryPremium × (1 + pct/100)} (multiplicative). */
    private Double todayLegSlPoints() {
        String k = todayKey();
        if (k.isEmpty()) return null;
        return riskSettings.getStrategyDouble(instanceId, "day." + k + ".legSlPoints", 0);
    }

    /** Computes the per-leg trigger price for {@code entryPremium}. Points-mode wins when
     *  set; falls back to pct-mode. Returns 0 when neither is configured (weekend / unset). */
    private double computeLegTrigger(double entryPremium) {
        if (entryPremium <= 0) return 0;
        Double pts = todayLegSlPoints();
        if (pts != null && pts > 0) return entryPremium + pts;
        Double pct = todayLegSlPct();
        if (pct != null && pct > 0) return entryPremium * (1.0 + pct / 100.0);
        return 0;
    }

    // ── Boot resume — called by StraddleInstanceManager via bootstrap() ───────
    private void init() {
        ShortStraddleStateStore.State p = stateStore.get(instanceId);
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
            this.lastEntryNifty = p.lastEntryNifty;
            this.ceEntryPremium = p.ceEntryPremium;
            this.peEntryPremium = p.peEntryPremium;
            this.ceClosedAtMillis = p.ceClosedAtMillis;
            this.peClosedAtMillis = p.peClosedAtMillis;
            this.ceLegPnl = p.ceLegPnl;
            this.peLegPnl = p.peLegPnl;
            this.ceClosePremium = p.ceClosePremium;
            this.peClosePremium = p.peClosePremium;
            this.realisedPnlToday = p.realisedPnlToday;
            this.sellPremiumTurnoverToday = p.sellPremiumTurnoverToday;
            this.buyPremiumTurnoverToday  = p.buyPremiumTurnoverToday;
            this.orderCountToday = p.orderCountToday;
            this.slHitsToday     = p.slHitsToday;
            this.cycleStartRealisedPnl   = p.cycleStartRealisedPnl;
            this.cycleStartSellTurnover  = p.cycleStartSellTurnover;
            this.cycleStartBuyTurnover   = p.cycleStartBuyTurnover;
            this.cycleStartOrderCount    = p.cycleStartOrderCount;
            this.cycleStartSlHits        = p.cycleStartSlHits;
            this.consumedRiskToday       = p.consumedRiskToday;
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
            log.info("[short-straddle] Resumed state={} dayKey={} ce={} pe={} ceEntry={} peEntry={} realised={}",
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
                    log.info("[short-straddle] Re-subscribed open legs to data WS after restart: {}", resub);
                    for (String s : resub) seedLegQuote(s);
                } catch (Exception e) {
                    log.warn("[short-straddle] Re-subscribe failed: {}", e.getMessage());
                }
            }
        }
    }

    // ── 1-min combined-premium sampler (drives the dashboard chart) ───────────
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
    public void tick() {
        rolloverIfNewDay();
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        // Enabled-flag guard. Default false — operator opts in via Settings → LEG SL.
        if (!riskSettings.getStrategyBool(instanceId, "enabled", false)) return;
        // Late-recover entry premium from tradebook if init() couldn't (no token at boot).
        tryRecoverEntryPremiumFromTradebook();

        LocalTime now = LocalTime.now(IST);
        LocalTime entryTime     = parseTime(getEntryTime(),     "09:20");
        LocalTime squareOffTime = parseTime(getSquareOffTime(), "15:15");
        if (!squareOffTime.isAfter(entryTime)) {
            log.warn("[short-straddle] squareOffTime {} must be after entryTime {} — skipping tick",
                squareOffTime, entryTime);
            return;
        }

        switch (state) {
            case IDLE -> {
                // Record that we saw the scheduler running BEFORE entry time today. Used by
                // the auto-entry gate below to distinguish "natural 9:20 fire" from "started
                // late / unpaused late". When paused we still observe the window — pause is
                // about firing, not about whether the scheduler exists.
                if (now.isBefore(entryTime)) {
                    observedPreEntryWindow = true;
                    return;
                }
                if (now.isBefore(squareOffTime)) {
                    // Per-day toggle. If today is disabled we never enter; legs already open
                    // from a previous day rollover would have been flattened by rolloverIfNewDay.
                    if (!isTodayDayEnabled()) {
                        return;
                    }
                    // Soft pause past the scheduled entry time → park DONE_FOR_DAY. Same
                    // applies if the bot was simply started after entry time and never saw
                    // the pre-entry window. In both cases the 9:20 setup is stale — auto-
                    // firing 40 min late at a different ATM doesn't match the operator's
                    // intent. + NEW STRADDLE stays available so the operator can fire
                    // explicitly at the current ATM whenever they choose.
                    boolean paused = riskSettings.getStrategyBool(instanceId, "tradingPaused", false);
                    if (paused || !observedPreEntryWindow) {
                        String why = paused ? "paused" : "started late (after entry time)";
                        eventService.log("[INFO] [" + instanceId + "] " + why
                            + " — auto-entry skipped, parked DONE_FOR_DAY. Use + NEW STRADDLE "
                            + "to fire manually.");
                        transitionTo(LifecycleState.DONE_FOR_DAY);
                        return;
                    }
                    doInitialEntry();
                }
            }
            case OPEN_BOTH, OPEN_CE_ONLY, OPEN_PE_ONLY -> checkLegSlOrSquareoff(now, squareOffTime);
            case DONE_FOR_DAY -> { /* idle until tomorrow */ }
        }
    }

    // ── Initial entry ──────────────────────────────────────────────────────────
    /** Scheduler entry path — gates already checked by caller (state IDLE, entry time
     *  reached, day enabled). Delegates to {@link #performEntryNow} which does the actual
     *  placement and is also reused by the manual {@code + NEW STRADDLE} restart path. */
    private void doInitialEntry() {
        performEntryNow("ENTRY");
    }

    /** Atomic placement-and-state-mutation block. Snapshots day-level accumulators at the
     *  start of the cycle so {@code persistStraddleTrade} can write per-cycle deltas instead
     *  of cumulative day totals (multi-cycle days produce one trade row per cycle). Fully
     *  resets per-leg state so a manual restart from DONE_FOR_DAY doesn't carry stale
     *  closed-leg values into the new straddle. */
    private synchronized void performEntryNow(String entryEventTag) {
        this.cycleStartRealisedPnl   = realisedPnlToday;
        this.cycleStartSellTurnover  = sellPremiumTurnoverToday;
        this.cycleStartBuyTurnover   = buyPremiumTurnoverToday;
        this.cycleStartOrderCount    = orderCountToday;
        this.cycleStartSlHits        = slHitsToday;
        this.ceSymbol = "";  this.peSymbol = "";
        this.ceQty = 0;      this.peQty = 0;
        this.ceOrderId = ""; this.peOrderId = "";
        this.ceEntryPremium = 0;   this.peEntryPremium = 0;
        this.ceClosePremium = 0;   this.peClosePremium = 0;
        this.ceLegPnl = 0;         this.peLegPnl = 0;
        this.ceClosedAtMillis = 0; this.peClosedAtMillis = 0;
        this.combinedPremiumSamples.clear();

        double niftyLtp = marketDataService.getLtp(NIFTY_SYMBOL);
        if (niftyLtp <= 0) {
            log.info("[short-straddle] Skipping entry — NIFTY LTP unavailable (waiting for first tick)");
            return;
        }
        // Balanced-ATM selection — single-method (put-call parity / synthetic futures).
        BalancedAtmSelector.AtmSelection sel = atmSelector.select(niftyLtp);
        this.lastAtmSelection = sel;
        if (sel == null) {
            log.warn("[short-straddle] Balanced-ATM selection failed (chain unavailable) — aborting day");
            eventService.log("[ERROR] [short-straddle] entry aborted — ATM-selector chain fetch failed");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        long atmStrike = sel.chosenAtm();
        String resolvedCe = sel.ceSymbolAtChosen();
        String resolvedPe = sel.peSymbolAtChosen();
        if (resolvedCe == null || resolvedCe.isEmpty() || resolvedPe == null || resolvedPe.isEmpty()) {
            log.warn("[short-straddle] Selector returned chosenAtm={} but missing CE/PE symbols — aborting day",
                atmStrike);
            eventService.log("[ERROR] [short-straddle] entry aborted — selector missing CE/PE symbols for "
                + atmStrike);
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        int qty = Math.max(1, riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1)) * NIFTY_LOT_SIZE;
        String product = productType();

        OrderDTO ceResp = orderService.placeOrder(resolvedCe, qty, -1, 0, product);
        orderCountToday++;
        if (ceResp == null || ceResp.getId() == null || ceResp.getId().isEmpty() || !"ok".equals(ceResp.getStatus())) {
            log.error("[short-straddle] CE leg rejected: {}", ceResp);
            eventService.log("[ERROR] [short-straddle] CE leg rejected — aborting day");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        OrderDTO peResp = orderService.placeOrder(resolvedPe, qty, -1, 0, product);
        orderCountToday++;
        if (peResp == null || peResp.getId() == null || peResp.getId().isEmpty() || !"ok".equals(peResp.getStatus())) {
            log.error("[short-straddle] PE leg rejected after CE filled — flattening CE: {}", peResp);
            eventService.log("[ERROR] [short-straddle] PE leg rejected — buying back CE to flatten");
            try { orderService.placeOrder(resolvedCe, qty, 1, 0, product); orderCountToday++; } catch (Exception ignored) {}
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
        this.ceLegPnl = 0;
        this.peLegPnl = 0;
        this.ceClosePremium = 0;
        this.peClosePremium = 0;
        try { marketDataService.subscribeAdditional(java.util.Arrays.asList(resolvedCe, resolvedPe)); }
        catch (Exception ignored) {}
        // Set immediate LTP-based estimates so the state transition + chart sample happen
        // synchronously — no blocking. The async FillListener (onActualFill) overwrites these
        // with the broker-confirmed fill the moment the order WS pushes status=2, typically
        // within 100–200 ms of placement.
        // Seed display values from LTP so the leg cards / chart show something while we
        // wait for the WS fill confirmation (~100-200 ms). The WS callback overwrites these
        // with the broker-confirmed fill and does ALL turnover / P&L bookkeeping.
        this.ceEntryPremium = readEntryPremium(resolvedCe);
        this.peEntryPremium = readEntryPremium(resolvedPe);
        this.currentWeeklyExpiry = parseExpiryFromSymbol(resolvedCe);
        registerPendingFill(ceResp.getId(), PendingType.ENTRY_CE, qty, 0);
        registerPendingFill(peResp.getId(), PendingType.ENTRY_PE, qty, 0);
        transitionTo(LifecycleState.OPEN_BOTH);

        double niftyAtEntry = niftyLtp;
        this.lastEntryNifty = niftyAtEntry;
        // Seed the chart with an entry-point sample so the leftmost line value matches the
        // CE/PE leg cards' Entry premiums. Without this, the first chart sample is whatever
        // the LTPs are at the NEXT minute boundary (up to 60s after entry) — never the entry
        // premium itself — and the chart appears to start somewhere other than entry.
        java.util.Map<String, Object> entrySample = new java.util.LinkedHashMap<>();
        entrySample.put("t",  LocalTime.now(IST).format(java.time.format.DateTimeFormatter.ofPattern("HH:mm")));
        entrySample.put("v",  round2(ceEntryPremium + peEntryPremium));
        entrySample.put("ce", round2(ceEntryPremium));
        entrySample.put("pe", round2(peEntryPremium));
        combinedPremiumSamples.add(entrySample);
        pushEvent(entryEventTag, niftyAtEntry, resolvedCe, resolvedPe, 0);

        String msg = "leg-sl armed @ ATM " + atmStrike + " (NIFTY " + String.format("%.2f", niftyLtp)
            + ") qty=" + qty + " ce=" + ceSymbol + " pe=" + peSymbol;
        log.info("[short-straddle] {}", msg);
        eventService.log("[INFO] [short-straddle] " + msg);
        notifyTelegram(msg);
    }

    // ── Per-leg SL + timed squareoff ───────────────────────────────────────────
    private void checkLegSlOrSquareoff(LocalTime now, LocalTime squareOffTime) {
        if (afterOrAt(now, squareOffTime)) {
            closeRemainingLegs("TIMED_SQUAREOFF");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        // Per-strategy max-loss kill switch removed — the portfolio-wide kill switch
        // (PortfolioRiskService) is now the sole limit: aggregate (realised + open MTM)
        // across every enabled strategy is checked against portfolio risk every 5 s and
        // flattens everything when crossed.
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
        Double todayPct = todayLegSlPct();
        Double todayPts = todayLegSlPoints();
        if (todayPct == null && todayPts == null) return; // weekend → no SL check
        boolean pointsMode = todayPts != null && todayPts > 0;
        String triggerDesc = pointsMode
            ? String.format("+%.2f pts", todayPts)
            : (todayPct != null ? String.format("%.0f%%", todayPct) : "—");
        if (isCeOpen() && ceEntryPremium > 0 && !ceSymbol.isEmpty()) {
            double ceLtp = marketDataService.getLtp(ceSymbol);
            double trigger = computeLegTrigger(ceEntryPremium);
            if (ceLtp > 0 && trigger > 0 && ceLtp >= trigger) {
                double consumedPts = ceLtp - ceEntryPremium;
                String msg = String.format("CE leg SL hit — entry %.2f, live %.2f (+%.2f pts, threshold %s). Closing CE only.",
                    ceEntryPremium, ceLtp, consumedPts, triggerDesc);
                log.info("[short-straddle] {}", msg);
                eventService.log("[INFO] [short-straddle] " + msg);
                notifyTelegram(msg);
                closeLeg("CE", "CE_SL_HIT");
                return;
            }
        }
        if (isPeOpen() && peEntryPremium > 0 && !peSymbol.isEmpty()) {
            double peLtp = marketDataService.getLtp(peSymbol);
            double trigger = computeLegTrigger(peEntryPremium);
            if (peLtp > 0 && trigger > 0 && peLtp >= trigger) {
                double consumedPts = peLtp - peEntryPremium;
                String msg = String.format("PE leg SL hit — entry %.2f, live %.2f (+%.2f pts, threshold %s). Closing PE only.",
                    peEntryPremium, peLtp, consumedPts, triggerDesc);
                log.info("[short-straddle] {}", msg);
                eventService.log("[INFO] [short-straddle] " + msg);
                notifyTelegram(msg);
                closeLeg("PE", "PE_SL_HIT");
                return;
            }
        }
        if (!isCeOpen() && !isPeOpen()) {
            transitionTo(LifecycleState.DONE_FOR_DAY);
        }
    }

    /** Fast tick — only does the per-leg SL trigger check. Detection latency drops from ~5s
     *  (slow tick) to ~500ms. Cheap: just reads LTPs from the in-memory tick cache and compares
     *  to per-leg thresholds. */
    public void fastSlCheck() {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        if (!riskSettings.getStrategyBool(instanceId, "enabled", false)) return;
        checkLegSlTriggers();
    }

    /** Close just the named leg ("CE" or "PE"), update state to the surviving-leg state, persist. */
    private void closeLeg(String which, String reason) {
        boolean isCe = "CE".equalsIgnoreCase(which);
        String symbol = isCe ? ceSymbol : peSymbol;
        int qty       = isCe ? ceQty    : peQty;
        double entry  = isCe ? ceEntryPremium : peEntryPremium;
        if (symbol == null || symbol.isEmpty() || qty <= 0) return;
        // SL-hit counter — only the per-leg SL paths bump it; TIMED_SQUAREOFF / MANUAL /
        // MAX_LOSS_HIT close the leg too but don't count as an SL day for analytics.
        if ("CE_SL_HIT".equals(reason) || "PE_SL_HIT".equals(reason)) slHitsToday++;

        // Seed display values from LTP so the leg card shows a reasonable Exit price during
        // the ~100-200 ms gap before the WS push lands. The WS callback (onActualFill)
        // overwrites ceClosePremium / peClosePremium with the broker-confirmed fill and does
        // ALL P&L / turnover bookkeeping — closeLeg does NOT touch realisedPnlToday or
        // buyPremiumTurnoverToday.
        double quotedLtp = marketDataService.getLtp(symbol);
        double niftyAtClose = marketDataService.getLtp(NIFTY_SYMBOL);
        String closedCe = isCe ? symbol : "";
        String closedPe = isCe ? "" : symbol;

        String closeOrderId = placeCloseRetry(symbol, qty, which, reason);

        double pnl = (entry > 0 && quotedLtp > 0) ? (entry - quotedLtp) * qty : 0;
        if (isCe) { ceLegPnl = pnl; ceClosePremium = quotedLtp; }
        else      { peLegPnl = pnl; peClosePremium = quotedLtp; }
        registerPendingFill(closeOrderId, isCe ? PendingType.CLOSE_CE : PendingType.CLOSE_PE,
            qty, entry);

        // Unsubscribe — the other leg keeps its WS sub.
        try { marketDataService.unsubscribeAdditional(java.util.Collections.singletonList(symbol)); }
        catch (Exception ignored) {}

        pushEvent("CLOSE_" + reason, niftyAtClose, closedCe, closedPe, pnl);
        String msg = which + " leg closed (" + reason + "): " + symbol + " qty=" + qty
            + " pnl=" + String.format("%.2f", pnl);
        log.info("[short-straddle] {}", msg);
        eventService.log("[INFO] [short-straddle] " + msg);

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
        // Treat risk-event force-closes (per-strategy max-loss kill, portfolio kill) as SL
        // hits for analytics — the bot didn't choose to ride out to squareoff, the loss
        // budget did. Timed squareoff and stale-day reset do NOT count.
        boolean countAsSl = reason != null && reason.contains("MAX_LOSS");
        if (isCeOpen() && !ceSymbol.isEmpty() && ceQty > 0) {
            double ltp = marketDataService.getLtp(ceSymbol);
            double pnl = (ceEntryPremium > 0 && ltp > 0) ? (ceEntryPremium - ltp) * ceQty : 0;
            ceLegPnl = pnl;
            ceClosePremium = ltp;
            totalPnl += pnl;
            String ceCloseId = placeCloseRetry(ceSymbol, ceQty, "CE", reason);
            registerPendingFill(ceCloseId, PendingType.CLOSE_CE, ceQty, ceEntryPremium);
            unsubAfter.add(ceSymbol);
            this.ceClosedAtMillis = System.currentTimeMillis();
            this.ceQty = 0;
            this.ceOrderId = "";
            if (countAsSl) slHitsToday++;
        }
        if (isPeOpen() && !peSymbol.isEmpty() && peQty > 0) {
            double ltp = marketDataService.getLtp(peSymbol);
            double pnl = (peEntryPremium > 0 && ltp > 0) ? (peEntryPremium - ltp) * peQty : 0;
            peLegPnl = pnl;
            peClosePremium = ltp;
            totalPnl += pnl;
            String peCloseId = placeCloseRetry(peSymbol, peQty, "PE", reason);
            registerPendingFill(peCloseId, PendingType.CLOSE_PE, peQty, peEntryPremium);
            unsubAfter.add(peSymbol);
            this.peClosedAtMillis = System.currentTimeMillis();
            this.peQty = 0;
            this.peOrderId = "";
            if (countAsSl) slHitsToday++;
        }
        if (!unsubAfter.isEmpty()) {
            try { marketDataService.unsubscribeAdditional(unsubAfter); } catch (Exception ignored) {}
        }
        if (!unsubAfter.isEmpty()) {
            pushEvent("CLOSE_" + reason, niftyAtClose, ceSymbol, peSymbol, totalPnl);
            String msg = "leg-sl remaining legs closed (" + reason + "): " + String.join(", ", unsubAfter)
                + " pnl=" + String.format("%.2f", totalPnl);
            log.info("[short-straddle] {}", msg);
            eventService.log("[INFO] [short-straddle] " + msg);
            notifyTelegram(msg);
        }
        persist();
    }

    /** BUY close order with one retry. Returns the orderId of the successful placement,
     *  or empty string if both attempts failed. Caller passes the orderId to
     *  {@link #readFilledPriceWithRetry} to capture the broker-confirmed fill price. */
    private String placeCloseRetry(String symbol, int qty, String legTag, String reason) {
        try {
            String product = productType();
            OrderDTO resp = orderService.placeOrder(symbol, qty, 1, 0, product);
            orderCountToday++;
            if (resp != null && resp.getId() != null && !resp.getId().isEmpty() && "ok".equals(resp.getStatus())) return resp.getId();
            log.warn("[short-straddle] First close attempt failed for {} {} ({}): {} — retrying in 2s",
                legTag, symbol, reason, resp);
            try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            OrderDTO retry = orderService.placeOrder(symbol, qty, 1, 0, product);
            orderCountToday++;
            if (retry != null && retry.getId() != null && !retry.getId().isEmpty() && "ok".equals(retry.getStatus())) return retry.getId();
            log.error("[short-straddle] CLOSE FAILED for {} {} qty={} ({}): {}",
                legTag, symbol, qty, reason, retry);
            eventService.log("[ERROR] [short-straddle] CLOSE FAILED for " + legTag + " " + symbol
                + " qty=" + qty + " — manual intervention required");
        } catch (Exception e) {
            log.error("[short-straddle] Exception closing {} {}: {}", legTag, symbol, e.getMessage());
        }
        return "";
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
        double gst        = (brokerage + exchange + sebi) * GST_PCT;
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

    /** Pre-entry preview of the balanced ATM selection, cached for {@link #ATM_PREVIEW_TTL_MS}
     *  to avoid hammering the option chain on every dashboard poll. Returns the cached
     *  selection when still warm and the strategy isn't currently holding open legs (no
     *  point recomputing — the legs are already on the chosen strike). Returns {@code null}
     *  when no NIFTY LTP is available yet. */
    public BalancedAtmSelector.AtmSelection getAtmPreview() {
        long now = System.currentTimeMillis();
        if (cachedAtmPreview != null && (now - cachedAtmPreviewMs) < ATM_PREVIEW_TTL_MS) {
            return cachedAtmPreview;
        }
        double niftyLtp = marketDataService != null ? marketDataService.getLtp(NIFTY_SYMBOL) : 0;
        if (niftyLtp <= 0) return cachedAtmPreview;
        BalancedAtmSelector.AtmSelection fresh = atmSelector.select(niftyLtp);
        if (fresh != null) {
            cachedAtmPreview   = fresh;
            cachedAtmPreviewMs = now;
        }
        return cachedAtmPreview;
    }

    // ── Dashboard payload (leg-sl shape) ───────────────────────────────────────
    @Override
    public java.util.Map<String, Object> getDashboard() {
        rolloverIfNewDay();
        if (currentWeeklyExpiry == null || currentWeeklyExpiry.isEmpty()) {
            tryResolveWeeklyExpiry();
        }
        java.util.Map<String, Object> m = getStatus();
        m.put("dashboardShape",  "short-straddle");
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

        // Balanced-ATM projection — drives the projected strike shown on the CE/PE leg cards
        // pre-entry AND the disagreement banner on the + NEW STRADDLE confirm modal. Pre-entry
        // we compute live (cached 30 s); post-entry we surface the selection captured at the
        // time of the actual entry placement so the UI reflects what was actually traded.
        BalancedAtmSelector.AtmSelection atmInfo;
        boolean preEntry = (state == LifecycleState.IDLE) || (state == LifecycleState.DONE_FOR_DAY);
        if (preEntry && niftyLtp > 0) {
            atmInfo = getAtmPreview();
        } else {
            atmInfo = lastAtmSelection;
        }
        if (atmInfo != null) {
            // Pre-entry leg cards show ~24950 + the projected leg's LTP in muted text so
            // the operator can see where the strikes are currently trading. To get real-
            // time LTP (not the 30 s option-chain cache), subscribe the projected CE/PE
            // symbols to MarketDataService once and read live values. The subscription is
            // idempotent — subscribeAdditional dedupes on its end.
            String preCeSym = atmInfo.ceSymbolAtChosen();
            String prePeSym = atmInfo.peSymbolAtChosen();
            if (preEntry && marketDataService != null
                    && preCeSym != null && !preCeSym.isEmpty()
                    && prePeSym != null && !prePeSym.isEmpty()) {
                try { marketDataService.subscribeAdditional(java.util.Arrays.asList(preCeSym, prePeSym)); }
                catch (Exception ignored) {}
            }
            double preCeLtp = (preCeSym != null && !preCeSym.isEmpty() && marketDataService != null)
                ? marketDataService.getLtp(preCeSym) : 0;
            double prePeLtp = (prePeSym != null && !prePeSym.isEmpty() && marketDataService != null)
                ? marketDataService.getLtp(prePeSym) : 0;
            // Live WS LTP first; fall back to the option-chain snapshot from the selector
            // until the first tick lands on the freshly-subscribed symbol.
            if (preCeLtp <= 0) preCeLtp = atmInfo.ceLtpAtChosen();
            if (prePeLtp <= 0) prePeLtp = atmInfo.peLtpAtChosen();
            m.put("projectedAtm",      atmInfo.chosenAtm());
            m.put("projectedAtmSpot",  atmInfo.spotAtm());
            m.put("projectedAtmCeSym", preCeSym);
            m.put("projectedAtmPeSym", prePeSym);
            m.put("projectedAtmCeLtp", round2(preCeLtp));
            m.put("projectedAtmPeLtp", round2(prePeLtp));
            m.put("projectedAtmGap",   round2(Math.abs(preCeLtp - prePeLtp)));
        }

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
        m.put("ceClosePremium", round2(ceClosePremium));
        m.put("peClosePremium", round2(peClosePremium));

        double ceMtm = (isCeOpen() && ceEntryPremium > 0 && ceLtp > 0 && ceQty > 0) ? (ceEntryPremium - ceLtp) * ceQty : 0;
        double peMtm = (isPeOpen() && peEntryPremium > 0 && peLtp > 0 && peQty > 0) ? (peEntryPremium - peLtp) * peQty : 0;
        // Leg-card display values — when a leg is closed, freeze the realised P&L so the
        // card shows the loss taken instead of resetting to 0. The Hero's Open MTM stays
        // clean (sums live MTMs only) via combinedMtm below.
        double ceCardMtm = isCeOpen() ? ceMtm : ceLegPnl;
        double peCardMtm = isPeOpen() ? peMtm : peLegPnl;
        m.put("ceMtm", round2(ceCardMtm));
        m.put("peMtm", round2(peCardMtm));
        m.put("combinedMtm", round2(ceMtm + peMtm)); // Hero "Open MTM" — live legs only
        m.put("realisedPnlToday", round2(realisedPnlToday));
        m.put("totalPnlToday", round2(realisedPnlToday + ceMtm + peMtm));

        // Per-leg SL triggers + consumed % (replaces combined SL in the leg-sl dashboard).
        // legSlPct / legSlPoints come from the per-day config — null on weekends, where the
        // Risk Band renders "—" rather than a misleading 50 % fallback. Points takes
        // precedence in the trigger formula when set.
        Double legSlPctBoxed    = todayLegSlPct();
        Double legSlPointsBoxed = todayLegSlPoints();
        m.put("legSlPct",    legSlPctBoxed);
        m.put("legSlPoints", legSlPointsBoxed);
        // Effective per-leg loss at SL — uses points if set, else (entryPremium × pct/100).
        java.util.function.DoubleUnaryOperator legLossAtSl = (entry) -> {
            if (entry <= 0) return 0;
            if (legSlPointsBoxed != null && legSlPointsBoxed > 0) return legSlPointsBoxed;
            if (legSlPctBoxed    != null && legSlPctBoxed    > 0) return entry * (legSlPctBoxed / 100.0);
            return 0;
        };
        // Worst-case loss for the currently-OPEN legs if they hit SL (Active Risk on the
        // dashboard). Closed legs are excluded — their loss has already been realised.
        double maxLossPerStraddle = 0;
        int legQty = Math.max(ceQty, peQty);
        if (legQty > 0) {
            if (isCeOpen() && ceEntryPremium > 0) maxLossPerStraddle += legLossAtSl.applyAsDouble(ceEntryPremium) * legQty;
            if (isPeOpen() && peEntryPremium > 0) maxLossPerStraddle += legLossAtSl.applyAsDouble(peEntryPremium) * legQty;
        }
        m.put("maxLossPerStraddle", round2(maxLossPerStraddle));
        // Realised P&L from legs already closed in the CURRENT cycle — kept for backward
        // compatibility with anything still reading closedLegsPnl. Multi-straddle days
        // should read consumedRiskToday instead (below) since that aggregates across cycles.
        double closedLegsPnl = 0;
        if (!isCeOpen() && ceLegPnl != 0) closedLegsPnl += ceLegPnl;
        if (!isPeOpen() && peLegPnl != 0) closedLegsPnl += peLegPnl;
        m.put("closedLegsPnl", round2(closedLegsPnl));
        // Day-level Consumed Risk — sum of every closed leg's realised P&L across every
        // straddle today (cumulative across manual restarts). UI gates display on < 0.
        m.put("consumedRiskToday", round2(consumedRiskToday));
        double ceTrigger = computeLegTrigger(ceEntryPremium);
        if (ceTrigger > 0) {
            m.put("ceSlTrigger", round2(ceTrigger));
            double consumed = isCeOpen() && ceLtp > 0
                ? ((ceLtp - ceEntryPremium) / (ceTrigger - ceEntryPremium)) * 100.0 : 0;
            m.put("ceSlConsumedPct", round2(consumed));
        } else {
            m.put("ceSlTrigger", 0.0);
            m.put("ceSlConsumedPct", 0.0);
        }
        double peTrigger = computeLegTrigger(peEntryPremium);
        if (peTrigger > 0) {
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
        m.put("lastEntryNifty", lastEntryNifty);
        m.put("ceSymbol",      ceSymbol);
        m.put("peSymbol",      peSymbol);
        m.put("ceQty",         ceQty);
        m.put("peQty",         peQty);
        m.put("ceOrderId",     ceOrderId);
        m.put("peOrderId",     peOrderId);
        m.put("entryTime",     getEntryTime());
        m.put("squareOffTime", getSquareOffTime());
        m.put("legSlPct",      todayLegSlPct());
        // Per-day toggle + active SL — dashboard market clock shows the day's status.
        String dayKey = todayKey();
        m.put("todayDayKey",   dayKey);
        m.put("todayDayEnabled", isTodayDayEnabled());
        java.util.Map<String, Object> dayMap = new java.util.LinkedHashMap<>();
        for (String[] dk : WEEK_DAYS) {
            java.util.Map<String, Object> e = new java.util.LinkedHashMap<>();
            e.put("dte",         Integer.parseInt(dk[1]));
            e.put("enabled",     riskSettings.getStrategyBool(instanceId,   "day." + dk[0] + ".enabled",     true));
            e.put("legSlPct",    riskSettings.getStrategyDouble(instanceId, "day." + dk[0] + ".legSlPct",    50));
            e.put("legSlPoints", riskSettings.getStrategyDouble(instanceId, "day." + dk[0] + ".legSlPoints", 0));
            dayMap.put(dk[0], e);
        }
        m.put("dayConfig", dayMap);
        m.put("lotsPerLeg",    riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1));
        m.put("lotSize",       NIFTY_LOT_SIZE);
        m.put("enabled",       riskSettings.getStrategyBool(instanceId, "enabled", false));
        // Soft-pause flag — surfaced in the Today pane header. When true, scheduler skips
        // the IDLE → entry transition AND restartFromDoneForDay returns TRADING_PAUSED so
        // the + NEW STRADDLE button stays disabled. Open positions continue to be managed.
        m.put("tradingPaused", riskSettings.getStrategyBool(instanceId, "tradingPaused", false));
        return m;
    }

    // ── Manual controls ────────────────────────────────────────────────────────
    public synchronized boolean forceCloseAll(String reason) {
        if (state != LifecycleState.OPEN_BOTH && state != LifecycleState.OPEN_CE_ONLY
                && state != LifecycleState.OPEN_PE_ONLY) {
            log.info("[short-straddle] forceClose ignored — state={}", state);
            return false;
        }
        eventService.log("[INFO] [short-straddle] Manual squareoff (" + reason + ") — flattening any open legs");
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
            eventService.log("[INFO] [short-straddle] Portfolio kill (" + reason + ") — flattening + parking");
            closeRemainingLegs(reason);
        } else {
            eventService.log("[INFO] [short-straddle] Portfolio kill (" + reason + ") — parking from state=" + state + " (no open position)");
        }
        transitionTo(LifecycleState.DONE_FOR_DAY);
    }

    @Override
    public synchronized void resetToIdle(String reason) {
        log.info("[short-straddle] Manual reset from {} → IDLE ({})", state, reason);
        eventService.log("[INFO] [short-straddle] state reset to IDLE (" + reason + ")");
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
            log.warn("[short-straddle] Stale state {} from {} detected at startup — flattening before reset",
                state, dayKey);
            eventService.log("[WARNING] [short-straddle] stale " + state + " from " + dayKey + " — flattening");
            closeRemainingLegs("STALE_DAY_RESET");
        }
        if (dayKey != null && !dayKey.isEmpty() && realisedPnlToday != 0) {
            try { persistSessionFor(dayKey); }
            catch (Exception e) { log.warn("[short-straddle] Failed to persist session row for {}: {}", dayKey, e.getMessage()); }
        }
        this.dayKey = today;
        this.lastEntryNifty = 0;
        this.ceSymbol = ""; this.peSymbol = "";
        this.ceQty = 0; this.peQty = 0;
        this.ceOrderId = ""; this.peOrderId = "";
        this.ceEntryPremium = 0; this.peEntryPremium = 0;
        this.ceClosedAtMillis = 0; this.peClosedAtMillis = 0;
        this.ceLegPnl = 0; this.peLegPnl = 0;
        this.ceClosePremium = 0; this.peClosePremium = 0;
        this.realisedPnlToday = 0;
        this.sellPremiumTurnoverToday = 0;
        this.buyPremiumTurnoverToday = 0;
        this.orderCountToday = 0;
        this.slHitsToday = 0;
        this.cycleStartRealisedPnl = 0;
        this.cycleStartSellTurnover = 0;
        this.cycleStartBuyTurnover = 0;
        this.cycleStartOrderCount = 0;
        this.cycleStartSlHits = 0;
        this.consumedRiskToday = 0;
        this.pendingFills.clear();
        this.currentWeeklyExpiry = "";
        this.recentEvents.clear();
        this.combinedPremiumSamples.clear();
        this.lastAtmSelection = null;
        this.observedPreEntryWindow = false;
        transitionTo(LifecycleState.IDLE);
    }

    private void persistSessionFor(String date) {
        if (sessionRepo == null) return;
        java.util.Map<String, Double> chargesBreakdown = computeChargesBreakdown();
        double charges = chargesBreakdown.get("total");
        double gross   = realisedPnlToday;
        double net     = gross - charges;
        com.rydytrader.autotrader.entity.StraddleSessionEntity row =
            sessionRepo.findByStrategyIdAndSessionDate(instanceId, date)
                       .orElseGet(com.rydytrader.autotrader.entity.StraddleSessionEntity::new);
        row.setStrategyId(instanceId);
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
        log.info("[short-straddle] Persisted session row for {}: gross={} net={}", date, gross, net);
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
                log.warn("[short-straddle] Could not find both CE and PE for ATM strike {} (ce={}, pe={})", atmStrike, ce, pe);
                return null;
            }
            return new String[]{ ce, pe };
        } catch (Exception e) {
            log.error("[short-straddle] Option chain fetch failed: {}", e.getMessage());
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

    public static String parseExpiryFromSymbol(String fyersSymbol) {
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

    /** Look up the actual broker-confirmed fill price for {@code orderId}. Three-step lookup
     *  mirroring the old equity bot's pattern, fastest path first:
     *  <ol>
     *    <li>Order WS cache (populated by {@link OrderEventService#onOrderEvent} on status=2).
     *        Typically lands within 100–200 ms of the REST place-order response. Polled 5 ×
     *        150 ms.</li>
     *    <li>Tradebook REST lookup with cache invalidation. Covers the case where the order
     *        WS is disconnected. 3 × 500 ms.</li>
     *    <li>Returns 0 — caller falls back to LTP, and {@link #tryRecoverEntryPremiumFromTradebook}
     *        repairs on the next tick.</li>
     *  </ol>
     *  Worst-case wall time: 750 ms + 1500 ms = 2.25 s. */
    private double readFilledPriceWithRetry(String orderId) {
        if (orderId == null || orderId.isEmpty()) return 0;
        // 1. Order WS push (fast path — instant once it lands)
        for (int i = 0; i < 5; i++) {
            Double cached = orderEventService.getFillPrice(orderId);
            if (cached != null && cached > 0) {
                log.info("[short-straddle] WS fill for {} on poll {}: {}", orderId, i + 1, cached);
                return cached;
            }
            if (i < 4) {
                try { Thread.sleep(150); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); return 0; }
            }
        }
        // 2. Tradebook fallback (WS down or slow)
        for (int i = 0; i < 3; i++) {
            try {
                orderService.invalidateTradebookCache();
                double fill = orderService.getFilledPriceByOrderId(orderId);
                if (fill > 0) {
                    log.info("[short-straddle] Tradebook fill for {} on attempt {}: {}", orderId, i + 1, fill);
                    return fill;
                }
            } catch (Exception e) {
                log.warn("[short-straddle] tradebook lookup attempt {} for {}: {}", i + 1, orderId, e.getMessage());
            }
            if (i < 2) {
                try { Thread.sleep(500); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); return 0; }
            }
        }
        log.info("[short-straddle] No fill price for {} within 2.25 s — falling back to LTP (next tick recovery will repair)", orderId);
        return 0;
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
                    log.info("[short-straddle] Entry premium for {} captured via REST quote: lp={} prevClose={}",
                        symbol, lp, prevClose);
                    return lp;
                }
            }
        } catch (Exception e) {
            log.warn("[short-straddle] REST quote fallback failed for {}: {}", symbol, e.getMessage());
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
            log.warn("[short-straddle] Seed leg quote failed for {}: {}", symbol, e.getMessage());
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
                    log.info("[short-straddle] Recovered CE entry premium from tradebook: {} (orderId={})", fill, ceOrderId);
                    changed = true;
                }
            } catch (Exception ignored) {}
        }
        if (peEntryPremium == 0 && peOrderId != null && !peOrderId.isEmpty()) {
            try {
                double fill = orderService.getFilledPriceByOrderId(peOrderId);
                if (fill > 0) {
                    peEntryPremium = fill;
                    log.info("[short-straddle] Recovered PE entry premium from tradebook: {} (orderId={})", fill, peOrderId);
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

    /** Write one {@code straddle_trades} row per cycle. Called when the state first transitions
     *  to DONE_FOR_DAY — for the initial scheduler-driven straddle and again for every manual
     *  {@code + NEW STRADDLE} restart cycle on the same day. Writes per-cycle deltas (current
     *  day totals minus the snapshot captured at {@link #performEntryNow}) rather than
     *  cumulative day totals so the cycle's contribution is recorded faithfully. Session row
     *  ({@link #persistSessionFor}) keeps aggregating cumulatively. */
    private void persistStraddleTrade() {
        if (tradeRepo == null) return;
        // Fold in any in-flight close fills. When closeRemainingLegs runs (timed squareoff /
        // portfolio kill / max-loss), it places close orders, sets leg estimates, and lets
        // transitionTo → persistStraddleTrade run synchronously — but the broker WS push that
        // actually updates realisedPnlToday + buyPremiumTurnoverToday arrives 100-200 ms
        // later. Without this overlay, the trade row would miss whatever legs were forcibly
        // closed in the same tick (most visible on portfolio-SL: row shows only the prior
        // SL'd leg's loss). onActualFill will re-overwrite these same fields when the WS
        // push lands; the trade row was already written so the values it captured are this
        // cycle's best estimate at the moment of close.
        double inFlightPnl = 0;
        double inFlightBuyTurnover = 0;
        for (PendingFill pf : pendingFills.values()) {
            if (pf.type() == PendingType.CLOSE_CE) {
                inFlightPnl         += ceLegPnl;
                inFlightBuyTurnover += ceClosePremium * pf.qty();
            } else if (pf.type() == PendingType.CLOSE_PE) {
                inFlightPnl         += peLegPnl;
                inFlightBuyTurnover += peClosePremium * pf.qty();
            }
        }
        double cycleGross = (realisedPnlToday          + inFlightPnl)         - cycleStartRealisedPnl;
        double cycleSellT = sellPremiumTurnoverToday                          - cycleStartSellTurnover;
        double cycleBuyT  = (buyPremiumTurnoverToday   + inFlightBuyTurnover) - cycleStartBuyTurnover;
        int    cycleOrders= orderCountToday                                   - cycleStartOrderCount;
        int    cycleSls   = slHitsToday                                       - cycleStartSlHits;
        if (Math.abs(cycleGross) < 0.01 && cycleSellT < 0.01) return;
        try {
            double charges = computeCycleCharges(cycleSellT, cycleBuyT, cycleOrders);
            com.rydytrader.autotrader.entity.StraddleTradeEntity t =
                new com.rydytrader.autotrader.entity.StraddleTradeEntity();
            t.setStrategyId(instanceId);
            t.setSessionDate(dayKey != null && !dayKey.isEmpty() ? dayKey : LocalDate.now(IST).toString());
            t.setClosedAtMillis(System.currentTimeMillis());
            int qty = Math.max(ceQty, peQty);
            if (qty == 0) qty = Math.max(1, riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1)) * NIFTY_LOT_SIZE;
            t.setQty(qty);
            t.setGrossPnl(round2(cycleGross));
            t.setCharges(round2(charges));
            t.setNetPnl(round2(cycleGross - charges));
            t.setCloseReason("DONE_FOR_DAY");
            t.setSlHitCount(cycleSls);
            tradeRepo.save(t);
        } catch (Exception e) {
            log.warn("[short-straddle] Failed to persist straddle_trades row: {}", e.getMessage());
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
        double gst       = (brokerage + exchange + sebi) * GST_PCT;
        return round2(brokerage + stt + exchange + sebi + stamp + gst);
    }

    private void persist() {
        ShortStraddleStateStore.State s = new ShortStraddleStateStore.State();
        s.dayKey = this.dayKey;
        s.state = this.state.name();
        s.ceSymbol = this.ceSymbol;
        s.peSymbol = this.peSymbol;
        s.ceQty = this.ceQty;
        s.peQty = this.peQty;
        s.ceOrderId = this.ceOrderId;
        s.peOrderId = this.peOrderId;
        s.lastEntryNifty = this.lastEntryNifty;
        s.ceEntryPremium = this.ceEntryPremium;
        s.peEntryPremium = this.peEntryPremium;
        s.ceClosedAtMillis = this.ceClosedAtMillis;
        s.peClosedAtMillis = this.peClosedAtMillis;
        s.ceLegPnl = this.ceLegPnl;
        s.peLegPnl = this.peLegPnl;
        s.ceClosePremium = this.ceClosePremium;
        s.peClosePremium = this.peClosePremium;
        s.realisedPnlToday = this.realisedPnlToday;
        s.sellPremiumTurnoverToday = this.sellPremiumTurnoverToday;
        s.buyPremiumTurnoverToday  = this.buyPremiumTurnoverToday;
        s.orderCountToday = this.orderCountToday;
        s.slHitsToday     = this.slHitsToday;
        s.cycleStartRealisedPnl   = this.cycleStartRealisedPnl;
        s.cycleStartSellTurnover  = this.cycleStartSellTurnover;
        s.cycleStartBuyTurnover   = this.cycleStartBuyTurnover;
        s.cycleStartOrderCount    = this.cycleStartOrderCount;
        s.cycleStartSlHits        = this.cycleStartSlHits;
        s.consumedRiskToday       = this.consumedRiskToday;
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
        stateStore.update(instanceId, s);
    }

    private void notifyTelegram(String msg) {
        try { if (telegramService != null) telegramService.sendMessage("[short-straddle] " + msg); }
        catch (Exception ignored) {}
    }

    private String getEntryTime()     { return riskSettings.getStrategyString(instanceId, "entryTime",     "09:20"); }
    private String getSquareOffTime() { return riskSettings.getStrategyString(instanceId, "squareOffTime", "15:15"); }

    private boolean isCeOpen() {
        return state == LifecycleState.OPEN_BOTH || state == LifecycleState.OPEN_CE_ONLY;
    }
    private boolean isPeOpen() {
        return state == LifecycleState.OPEN_BOTH || state == LifecycleState.OPEN_PE_ONLY;
    }

    private static LocalTime parseTime(String hhmm, String fallback) {
        try { return LocalTime.parse((hhmm == null || hhmm.isBlank()) ? fallback : hhmm.trim()); }
        catch (Exception e) {
            log.warn("[short-straddle] Failed to parse time \"{}\" — falling back to {}", hhmm, fallback);
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
}
