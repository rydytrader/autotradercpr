package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyInstanceEntity;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

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
    // Instrument constants (index symbol, lot size, strike step, weekly expiry day) now come
    // from the per-instance {@code underlying} setting via {@link #underlying()}. Old NIFTY-
    // only constants removed — every call site reads the current underlying at fire time.

    /** Charge constants (NIFTY weekly options, FY 2025-26). */
    /** Fallback STT rate (0.15% on sell-side premium, post 2026 hike) used
     *  when the operator zero'd out the CHARGES setting. Live value comes
     *  from {@link RiskSettingsStore#getSttRate()} (percent → /100 to apply). */
    private static final double STT_SELL_PCT_FALLBACK = 0.0015;
    /** Fallback exchange transaction rate (0.03553%) — same fallback pattern
     *  as STT. Live value from {@link RiskSettingsStore#getExchangeRate()}. */
    private static final double EXCH_TXN_PCT_FALLBACK = 0.0003553;
    /** Fallback stamp duty rate (0.003% on buy side) — same pattern.
     *  Live value from {@link RiskSettingsStore#getStampDutyRate()}. */
    private static final double STAMP_BUY_PCT_FALLBACK = 0.00003;
    private static final double GST_PCT        = 0.18;
    private static final double SEBI_PER_CRORE = 10.0;

    public enum LifecycleState { ARMED, OPEN_BOTH, OPEN_PE_ONLY, OPEN_CE_ONLY, DONE_FOR_DAY }

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
    private final com.rydytrader.autotrader.repository.StrategySessionRepository sessionRepo;
    private final com.rydytrader.autotrader.repository.StrategyTradeRepository tradeRepo;
    private final OrderEventService orderEventService;
    private final BalancedAtmSelector atmSelector;

    // ── In-memory state ───────────────────────────────────────────────────────
    private volatile LifecycleState state = LifecycleState.ARMED;
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

    /** Per-leg "SL moved to cost" markers. Flipped to true when the OTHER leg hits SL and the
     *  per-instance {@code moveSlToCostOnFirstLegHit} setting is on — from that point the
     *  surviving leg's SL trigger drops from {@code entry + threshold} down to its entry
     *  premium itself, locking break-even on that leg. In-memory, reset on day rollover and
     *  on every new cycle entry (multi-straddle days start each cycle fresh). */
    private volatile boolean ceSlMovedToCost = false;
    private volatile boolean peSlMovedToCost = false;

    /** Re-entry-on-SL counters: bumped in {@link #closeLeg} when reason is CE_SL_HIT / PE_SL_HIT
     *  AND re-entry is enabled AND cap not yet reached. Reset on day rollover. */
    private volatile int  ceReEntriesCount = 0;
    private volatile int  peReEntriesCount = 0;
    /** Wall-clock millis after which a scheduled re-entry may fire. 0 = no pending re-entry.
     *  Set in {@link #closeLeg} SL path; cleared once the re-entry is placed (or skipped past
     *  cutoff / cap). Checked in {@link #tick}. */
    private volatile long pendingCeReEntryAtMillis = 0;
    private volatile long pendingPeReEntryAtMillis = 0;

    /** True once at least one tick has run today with the scheduler observing the pre-entry
     *  window (state==ARMED, now<entryTime). Reset on every day rollover. Used to detect a
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
    /** {@code provisionalPnl} and {@code provisionalBuyTurnover} are the values
     *  booked synchronously against realisedPnlToday / buyPremiumTurnoverToday
     *  at close-order-placement time (LTP-based). The WS fill callback later
     *  applies the delta between actual and provisional so the dashboard is
     *  honest even if the WS push never arrives. Zero for entries. */
    private static record PendingFill(PendingType type, int qty, double entryRef,
                                       double provisionalPnl, double provisionalBuyTurnover) {}
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
    private final    AtomicBoolean                     atmRefreshInFlight = new AtomicBoolean(false);
    private final    AtomicBoolean                     expiryRefreshInFlight = new AtomicBoolean(false);
    private static final long ATM_PREVIEW_TTL_MS = 30_000L;

    public ShortStraddle(StrategyInstanceEntity entity,
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
                         com.rydytrader.autotrader.repository.StrategySessionRepository sessionRepo,
                         com.rydytrader.autotrader.repository.StrategyTradeRepository tradeRepo,
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
                double actualPnl  = (p.entryRef() - price) * p.qty();
                double actualBuyT = price * p.qty();
                // Delta refinement over the LTP-based provisional booked at
                // close-order placement — see closeRemainingLegs / closeLeg.
                realisedPnlToday        += (actualPnl  - p.provisionalPnl());
                consumedRiskToday       += (actualPnl  - p.provisionalPnl());
                buyPremiumTurnoverToday += (actualBuyT - p.provisionalBuyTurnover());
                ceLegPnl = actualPnl;
                ceClosePremium = price;
                log.info("[short-straddle] CE close filled @ {} pnl={} (delta {})",
                    String.format("%.2f", price),
                    String.format("%.2f", actualPnl),
                    String.format("%+.2f", actualPnl - p.provisionalPnl()));
                eventService.log("[INFO] [short-straddle] CE close filled @ " + String.format("%.2f", price) + " pnl=" + String.format("%.2f", actualPnl));
            }
            case CLOSE_PE -> {
                double actualPnl  = (p.entryRef() - price) * p.qty();
                double actualBuyT = price * p.qty();
                realisedPnlToday        += (actualPnl  - p.provisionalPnl());
                consumedRiskToday       += (actualPnl  - p.provisionalPnl());
                buyPremiumTurnoverToday += (actualBuyT - p.provisionalBuyTurnover());
                peLegPnl = actualPnl;
                peClosePremium = price;
                log.info("[short-straddle] PE close filled @ {} pnl={} (delta {})",
                    String.format("%.2f", price),
                    String.format("%.2f", actualPnl),
                    String.format("%+.2f", actualPnl - p.provisionalPnl()));
                eventService.log("[INFO] [short-straddle] PE close filled @ " + String.format("%.2f", price) + " pnl=" + String.format("%.2f", actualPnl));
            }
        }
        persist();
    }

    /** Register an orderId for fill-callback notification. Race protection: if the WS push
     *  already cached the fill before this call (rare — Fyers WS sometimes beats the REST
     *  response on liquid options), apply it immediately. */
    private void registerPendingFill(String orderId, PendingType type, int qty, double entryRef,
                                      double provisionalPnl, double provisionalBuyTurnover) {
        if (orderId == null || orderId.isEmpty()) return;
        pendingFills.put(orderId, new PendingFill(type, qty, entryRef,
            provisionalPnl, provisionalBuyTurnover));
        Double already = orderEventService.getFillPrice(orderId);
        if (already != null && already > 0) onActualFill(orderId, already);
    }
    /** Entry orders never book provisional P&L — the WS fill supplies the entry
     *  premium the strategy has no LTP-based estimate for. */
    private void registerPendingFill(String orderId, PendingType type, int qty, double entryRef) {
        registerPendingFill(orderId, type, qty, entryRef, 0, 0);
    }

    /** Refresh display fields when the operator renames the instance via the Straddles tab. */
    public void syncFromEntity(StrategyInstanceEntity entity) {
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

    /** Public accessor for the per-instance underlying — used by the sidebar chip to show
     *  the instrument (NIFTY / SENSEX) as a subtitle under the short code. */
    public String underlyingName() { return underlying().name(); }
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
     *  scheduler precondition except the {@code state == ARMED} gate (which is the whole
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

    /** DTE-keyed trading slots, ordered top-down (4 → 0) to match the form layout. Each entry
     *  has its own enable toggle and SL %. Lookup happens via {@link #todayDteKey()} which
     *  computes the live DTE against the current weekly expiry — so a "0 DTE → 50 %" rule
     *  fires on whatever calendar day expiry actually lands on, even if NSE shifts it off
     *  Tuesday for a holiday. Settings keys: {@code strategies.<id>.dte.<N>.enabled /
     *  legSlPct / legSlPoints}. */
    private static final java.util.List<String> DTE_LEVELS = java.util.List.of("4", "3", "2", "1", "0");

    @Override
    public java.util.List<java.util.Map<String, Object>> getSettingsSchema() {
        java.util.List<java.util.Map<String, Object>> s = new java.util.ArrayList<>();
        // 'enabled' is intentionally NOT a per-instance settings field. The Straddles tab in
        // the global Settings modal owns enable/disable so the operator has one consistent
        // place to flip an instance on or off; the per-instance ⚙ Settings dialog is for
        // trading config only (entry time, lots, per-day SL %, squareoff).
        // Underlying — NIFTY (NSE, lot 65, step 50) or SENSEX (BSE, lot 20, step 100).
        // Drives every downstream lookup: option-chain fetch symbol, lot size, strike step,
        // and the weekly-expiry day fallback in nextExpectedWeeklyExpiry.
        java.util.Map<String, Object> underlyingFld = new java.util.LinkedHashMap<>();
        underlyingFld.put("key", "underlying");
        underlyingFld.put("type", "select");
        underlyingFld.put("default", "NIFTY");
        underlyingFld.put("label", "Underlying");
        underlyingFld.put("options", java.util.List.of("NIFTY", "SENSEX"));
        s.add(underlyingFld);
        s.add(field("entryTime",     "time",    "09:20", "Entry Time (HH:mm IST)", null));
        s.add(field("squareOffTime", "time",    "15:15", "Squareoff Time (HH:mm IST)", null));
        s.add(field("lotsPerLeg",    "int",      1,      "Lots per Leg", null));
        // Dropdown — INTRADAY (Fyers MIS) or OVERNIGHT (Fyers MARGIN / NRML).
        java.util.Map<String, Object> orderTypeFld = new java.util.LinkedHashMap<>();
        orderTypeFld.put("key", "orderType");
        orderTypeFld.put("type", "select");
        orderTypeFld.put("default", "INTRADAY");
        orderTypeFld.put("label", "Order Type");
        orderTypeFld.put("options", java.util.List.of("INTRADAY", "OVERNIGHT"));
        s.add(orderTypeFld);
        // Move-to-cost option. When ON, the moment one leg's SL fires the OTHER leg's SL
        // trigger collapses from "entry + threshold" down to its entry premium — locking
        // break-even on the surviving leg. Off by default; opt in per instance.
        s.add(field("moveSlToCostOnFirstLegHit", "boolean", false,
            "Move SL to Cost on First Leg SL Hit",
            "When one leg's SL fires, drops the surviving leg's SL trigger from entry + threshold down to its entry premium — locking break-even on the surviving leg."));
        // Re-entry on SL — helps recover on V-shape / inverted-V days. When a
        // leg's SL fires, wait {@code reEntryDelaySeconds}, then re-enter the
        // same side at the current ATM. Capped at {@code maxReEntriesPerLeg}
        // per side per day. No new re-entries after {@code reEntryLatestTime}.
        // Off by default.
        s.add(field("reEntryOnSlEnabled",  "boolean", false, "Re-entry on SL Enabled",
            "When one leg's SL fires, wait the delay below and re-enter the same side at the current ATM. Capped by max re-entries and latest time. Off by default."));
        s.add(field("maxReEntriesPerLeg",  "int",     1,     "Max Re-entries per Leg",
            "How many times each side (CE / PE) can be re-entered per day after its SL fires. 1 = at most one recovery entry per side."));
        s.add(field("reEntryDelaySeconds", "int",     300,   "Re-entry Delay (seconds)",
            "Wait this many seconds after the SL fires before re-entering — filters out mid-move wick stops from immediate re-entry into the same wave."));
        s.add(field("reEntryLatestTime",   "time",    "14:30", "Re-entry Latest Time (HH:mm IST)",
            "No new re-entries after this IST time — not enough runway before the timed squareoff for a fresh entry."));
        // Per-DTE enable + SL %. Rows ordered 4 → 3 → 2 → 1 → 0 so the layout reads top-down
        // away-from-expiry → expiry-day. Defaults: every level on at 50 %, matching the
        // previous single-SL behaviour. Keys live under {@code dte.<N>.*} so the lookup
        // follows the actual expiry instead of the weekday.
        for (String n : DTE_LEVELS) {
            s.add(field("dte." + n + ".enabled",     "boolean", true, n + " DTE — Enable",     null));
            s.add(field("dte." + n + ".legSlPct",    "percent", 50,   n + " DTE — SL %",       null));
            s.add(field("dte." + n + ".legSlPoints", "double",  0,    n + " DTE — SL Points",  null));
        }
        // Bucket each field into a UI tab. Risk = per-leg SL + move-to-cost.
        // Re-entry = re-entry-on-SL group. Everything else = basic.
        for (java.util.Map<String, Object> f : s) {
            String k = String.valueOf(f.get("key"));
            boolean risk    = "moveSlToCostOnFirstLegHit".equals(k) || k.startsWith("dte.");
            boolean reentry = k.startsWith("reEntry") || "maxReEntriesPerLeg".equals(k);
            String tab = reentry ? "reentry" : (risk ? "risk" : "basic");
            f.put("tab", tab);
            // Long-hint fields span both grid columns so the hint stays on one line.
            if ("moveSlToCostOnFirstLegHit".equals(k)) f.put("wide", true);
            if ("reEntryOnSlEnabled".equals(k))        f.put("wide", true);
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
        v.put("underlying",    riskSettings.getStrategyString(instanceId, "underlying",    "NIFTY"));
        v.put("entryTime",     riskSettings.getStrategyString(instanceId, "entryTime",     "09:20"));
        v.put("squareOffTime", riskSettings.getStrategyString(instanceId, "squareOffTime", "15:15"));
        v.put("lotsPerLeg",    riskSettings.getStrategyInt(instanceId,    "lotsPerLeg",    1));
        v.put("orderType",     riskSettings.getStrategyString(instanceId, "orderType",     "INTRADAY"));
        v.put("moveSlToCostOnFirstLegHit",
            riskSettings.getStrategyBool(instanceId, "moveSlToCostOnFirstLegHit", false));
        v.put("reEntryOnSlEnabled",  riskSettings.getStrategyBool(instanceId,   "reEntryOnSlEnabled",  false));
        v.put("maxReEntriesPerLeg",  riskSettings.getStrategyInt(instanceId,    "maxReEntriesPerLeg",  1));
        v.put("reEntryDelaySeconds", riskSettings.getStrategyInt(instanceId,    "reEntryDelaySeconds", 300));
        v.put("reEntryLatestTime",   riskSettings.getStrategyString(instanceId, "reEntryLatestTime",   "14:30"));
        for (String n : DTE_LEVELS) {
            v.put("dte." + n + ".enabled",     riskSettings.getStrategyBool(instanceId,   "dte." + n + ".enabled",     true));
            v.put("dte." + n + ".legSlPct",    riskSettings.getStrategyDouble(instanceId, "dte." + n + ".legSlPct",    50));
            v.put("dte." + n + ".legSlPoints", riskSettings.getStrategyDouble(instanceId, "dte." + n + ".legSlPoints", 0));
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
        if (values.containsKey("moveSlToCostOnFirstLegHit")) {
            riskSettings.setStrategySetting(instanceId, "moveSlToCostOnFirstLegHit",
                Boolean.parseBoolean(String.valueOf(values.get("moveSlToCostOnFirstLegHit"))));
        }
        if (values.containsKey("reEntryOnSlEnabled")) {
            riskSettings.setStrategySetting(instanceId, "reEntryOnSlEnabled",
                Boolean.parseBoolean(String.valueOf(values.get("reEntryOnSlEnabled"))));
        }
        if (values.containsKey("maxReEntriesPerLeg")) {
            int n = asInt(values.get("maxReEntriesPerLeg"), 1);
            if (n < 0)  n = 0;
            if (n > 10) n = 10;
            riskSettings.setStrategySetting(instanceId, "maxReEntriesPerLeg", n);
        }
        if (values.containsKey("reEntryDelaySeconds")) {
            int n = asInt(values.get("reEntryDelaySeconds"), 300);
            if (n < 0)    n = 0;
            if (n > 3600) n = 3600;
            riskSettings.setStrategySetting(instanceId, "reEntryDelaySeconds", n);
        }
        if (values.containsKey("reEntryLatestTime")) {
            riskSettings.setStrategySetting(instanceId, "reEntryLatestTime",
                String.valueOf(values.get("reEntryLatestTime")));
        }
        if (values.containsKey("orderType")) {
            String ot = String.valueOf(values.get("orderType")).trim().toUpperCase();
            if (!"INTRADAY".equals(ot) && !"OVERNIGHT".equals(ot)) ot = "INTRADAY";
            riskSettings.setStrategySetting(instanceId, "orderType", ot);
        }
        if (values.containsKey("underlying")) {
            String u = String.valueOf(values.get("underlying")).trim().toUpperCase();
            if (!"NIFTY".equals(u) && !"SENSEX".equals(u)) u = "NIFTY";
            riskSettings.setStrategySetting(instanceId, "underlying", u);
        }
        for (String n : DTE_LEVELS) {
            String enKey = "dte." + n + ".enabled";
            String slKey = "dte." + n + ".legSlPct";
            String ptKey = "dte." + n + ".legSlPoints";
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

    /** Reads the per-instance {@code underlying} setting and returns the matching
     *  {@link Underlying} enum. Defaults to NIFTY when unset or unknown. Every symbol /
     *  lot / strike-step / expiry-day lookup in the strategy funnels through this. */
    private Underlying underlying() {
        return Underlying.fromName(riskSettings.getStrategyString(instanceId, "underlying", "NIFTY"));
    }

    /** Today's DTE row key as a digit string ("0".."4"), or empty when no row applies.
     *  Empty cases: weekend (today is past the prior expiry) and bootstrap before
     *  {@code currentWeeklyExpiry} is resolved. DTE > 4 caps at 4 so the bot keeps using
     *  the highest-defined row if NSE ever widens the cycle. */
    private String todayDteKey() {
        int dte = tradingDaysToExpiry(currentWeeklyExpiry);
        if (dte < 0) return "";
        if (dte > 4) dte = 4;
        return String.valueOf(dte);
    }

    /** True when today's DTE row is enabled in this instance's per-instance settings. Returns
     *  false when no row applies (weekend / expiry unresolved) — matches the
     *  marketHolidayService check elsewhere. */
    private boolean isTodayDayEnabled() {
        String k = todayDteKey();
        if (k.isEmpty()) return false;
        return riskSettings.getStrategyBool(instanceId, "dte." + k + ".enabled", true);
    }

    /** Per-leg SL % for today's DTE row, or {@code null} when no row applies (weekend /
     *  expiry unresolved — operator-facing UI renders this as "—" rather than a misleading
     *  50 %). Internal callers that need a usable number on a weekend should fall back
     *  themselves. */
    private Double todayLegSlPct() {
        String k = todayDteKey();
        if (k.isEmpty()) return null;
        return riskSettings.getStrategyDouble(instanceId, "dte." + k + ".legSlPct", 50);
    }

    /** Per-leg SL in absolute premium points for today's DTE row, or {@code null} when no
     *  row applies. When > 0, points takes precedence over {@link #todayLegSlPct()} — the
     *  trigger is computed as {@code entryPremium + points} (linear) instead of
     *  {@code entryPremium × (1 + pct/100)} (multiplicative). */
    private Double todayLegSlPoints() {
        String k = todayDteKey();
        if (k.isEmpty()) return null;
        return riskSettings.getStrategyDouble(instanceId, "dte." + k + ".legSlPoints", 0);
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

    /** Per-leg trigger price taking the "moved to cost" override into account. When the
     *  surviving leg has been flagged after the other leg's SL fired (and the setting is
     *  on), the trigger collapses to the entry premium itself — i.e. close the moment the
     *  leg gives back its remaining premium and goes flat instead of waiting for the full
     *  SL distance. */
    private double effectiveLegTrigger(boolean isCe) {
        boolean moved = isCe ? ceSlMovedToCost : peSlMovedToCost;
        double entry  = isCe ? ceEntryPremium  : peEntryPremium;
        if (moved && entry > 0) return entry;
        return computeLegTrigger(entry);
    }

    /** Called from {@link #checkLegSlTriggers} right after one leg's SL fires while the other
     *  is still open. When the per-instance {@code moveSlToCostOnFirstLegHit} setting is on,
     *  flips the surviving leg's "moved to cost" flag so its SL trigger drops to its entry
     *  premium. No-op if the setting is off, the flag is already set, or the entry premium
     *  isn't known yet (defensive — should always be set by the time SL fires). */
    private void maybeMoveSurvivorToCost(boolean survivorIsCe, String triggerLeg) {
        if (!riskSettings.getStrategyBool(instanceId, "moveSlToCostOnFirstLegHit", false)) return;
        double entry = survivorIsCe ? ceEntryPremium : peEntryPremium;
        if (entry <= 0) return;
        boolean already = survivorIsCe ? ceSlMovedToCost : peSlMovedToCost;
        if (already) return;
        if (survivorIsCe) ceSlMovedToCost = true; else peSlMovedToCost = true;
        // Persist immediately — closeLeg() above wrote state BEFORE the flag flipped, so we
        // need another save here. Otherwise a mid-day restart between this line and the
        // next state-mutating event would revert the survivor's trigger to the wider
        // entry × (1 + slPct/100) value and defeat the break-even guarantee.
        persist();
        String which = survivorIsCe ? "CE" : "PE";
        String msg = String.format("%s SL moved to COST (entry %.2f) after %s SL hit — leg now closes on any retrace to entry.",
            which, entry, triggerLeg);
        log.info("[short-straddle] {}", msg);
        eventService.log("[INFO] [short-straddle] " + msg);
        notifyTelegram(msg);
    }

    /** One-time migration from the legacy weekday-keyed schema (day.MON.* … day.FRI.*) to the
     *  DTE-keyed schema (dte.0.* … dte.4.*). Idempotent via a {@code dteSettingsMigrated}
     *  sentinel — runs once per instance the first time the bot boots after the schema
     *  switch, then never again. Operators who had a 60 % SL on TUE see it under the
     *  new 0 DTE row; everyone who never touched the form lands on the same defaults
     *  the new schema's getters would have served anyway. Legacy weekday keys are left in
     *  the JSON untouched — nothing reads them anymore, and deletion would risk a slip-up
     *  if migration ever needs to be re-run. */
    private void migrateLegacyDaySettings() {
        if (riskSettings.getStrategyBool(instanceId, "dteSettingsMigrated", false)) return;
        // Mapping reflects the operator's assumption when they set the weekday rows: TUE = 0
        // DTE (expiry day), MON = 1, FRI = 2, THU = 3, WED = 4.
        java.util.Map<String, String> wdToDte = java.util.Map.of(
            "TUE", "0", "MON", "1", "FRI", "2", "THU", "3", "WED", "4");
        for (java.util.Map.Entry<String, String> e : wdToDte.entrySet()) {
            String wd = e.getKey(), n = e.getValue();
            boolean en  = riskSettings.getStrategyBool  (instanceId, "day." + wd + ".enabled",     true);
            double  pct = riskSettings.getStrategyDouble(instanceId, "day." + wd + ".legSlPct",    50);
            double  pts = riskSettings.getStrategyDouble(instanceId, "day." + wd + ".legSlPoints", 0);
            riskSettings.setStrategySetting(instanceId, "dte." + n + ".enabled",     en);
            riskSettings.setStrategySetting(instanceId, "dte." + n + ".legSlPct",    pct);
            riskSettings.setStrategySetting(instanceId, "dte." + n + ".legSlPoints", pts);
        }
        riskSettings.setStrategySetting(instanceId, "dteSettingsMigrated", true);
        riskSettings.saveFor("live");
        eventService.log("[INFO] [" + instanceId + "] Migrated weekday SL settings → DTE-based schema");
        log.info("[short-straddle] [{}] Migrated weekday SL settings → DTE-based schema", instanceId);
    }

    // ── Boot resume — called by StraddleInstanceManager via bootstrap() ───────
    private void init() {
        migrateLegacyDaySettings();
        ShortStraddleStateStore.State p = stateStore.get(instanceId);
        if (p != null && p.state != null) {
            try { this.state = LifecycleState.valueOf(p.state); }
            catch (IllegalArgumentException ex) { this.state = LifecycleState.ARMED; }
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
            this.ceSlMovedToCost = p.ceSlMovedToCost;
            this.peSlMovedToCost = p.peSlMovedToCost;
            this.ceReEntriesCount = p.ceReEntriesCount;
            this.peReEntriesCount = p.peReEntriesCount;
            this.pendingCeReEntryAtMillis = p.pendingCeReEntryAtMillis;
            this.pendingPeReEntryAtMillis = p.pendingPeReEntryAtMillis;
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
            case ARMED -> {
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
            case OPEN_BOTH, OPEN_CE_ONLY, OPEN_PE_ONLY -> {
                processPendingReEntries();
                checkLegSlOrSquareoff(now, squareOffTime);
            }
            case DONE_FOR_DAY -> {
                // Even if both legs closed (both SL'd), a scheduled re-entry may still fire.
                if (pendingCeReEntryAtMillis > 0 || pendingPeReEntryAtMillis > 0) {
                    processPendingReEntries();
                }
            }
        }
    }

    // ── Initial entry ──────────────────────────────────────────────────────────
    /** Scheduler entry path — gates already checked by caller (state ARMED, entry time
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
        this.ceSlMovedToCost = false;
        this.peSlMovedToCost = false;
        this.combinedPremiumSamples.clear();

        double niftyLtp = marketDataService.getLtp(underlying().indexSymbol());
        if (niftyLtp <= 0) {
            log.info("[short-straddle] Skipping entry — NIFTY LTP unavailable (waiting for first tick)");
            return;
        }
        // Balanced-ATM selection — single-method (put-call parity / synthetic futures).
        BalancedAtmSelector.AtmSelection sel = atmSelector.select(underlying(), niftyLtp);
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
        int qty = Math.max(1, riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1)) * underlying().lotSize();
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
        this.currentWeeklyExpiry = parseExpiryFromSymbol(resolvedCe, underlying());
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
            double trigger = effectiveLegTrigger(true);
            if (ceLtp > 0 && trigger > 0 && ceLtp >= trigger) {
                double consumedPts = ceLtp - ceEntryPremium;
                String thr = ceSlMovedToCost ? "MOVED TO COST" : triggerDesc;
                String msg = String.format("CE leg SL hit — entry %.2f, live %.2f (+%.2f pts, threshold %s). Closing CE only.",
                    ceEntryPremium, ceLtp, consumedPts, thr);
                log.info("[short-straddle] {}", msg);
                eventService.log("[INFO] [short-straddle] " + msg);
                notifyTelegram(msg);
                boolean peWasOpen = isPeOpen();
                closeLeg("CE", "CE_SL_HIT");
                if (peWasOpen) maybeMoveSurvivorToCost(false, "CE");
                return;
            }
        }
        if (isPeOpen() && peEntryPremium > 0 && !peSymbol.isEmpty()) {
            double peLtp = marketDataService.getLtp(peSymbol);
            double trigger = effectiveLegTrigger(false);
            if (peLtp > 0 && trigger > 0 && peLtp >= trigger) {
                double consumedPts = peLtp - peEntryPremium;
                String thr = peSlMovedToCost ? "MOVED TO COST" : triggerDesc;
                String msg = String.format("PE leg SL hit — entry %.2f, live %.2f (+%.2f pts, threshold %s). Closing PE only.",
                    peEntryPremium, peLtp, consumedPts, thr);
                log.info("[short-straddle] {}", msg);
                eventService.log("[INFO] [short-straddle] " + msg);
                notifyTelegram(msg);
                boolean ceWasOpen = isCeOpen();
                closeLeg("PE", "PE_SL_HIT");
                if (ceWasOpen) maybeMoveSurvivorToCost(true, "PE");
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

        // Seed display values from LTP for the ~100-200 ms gap before the WS push lands.
        // We ALSO book the LTP-based P&L provisionally against realisedPnlToday +
        // buyPremiumTurnoverToday so the dashboard is honest even if the WS fill event
        // never arrives. The WS callback (onActualFill) later applies the actual-vs-
        // provisional delta so numbers converge to broker-confirmed values.
        double quotedLtp = marketDataService.getLtp(symbol);
        double niftyAtClose = marketDataService.getLtp(underlying().indexSymbol());
        String closedCe = isCe ? symbol : "";
        String closedPe = isCe ? "" : symbol;

        String closeOrderId = placeCloseRetry(symbol, qty, which, reason);

        double pnl  = (entry > 0 && quotedLtp > 0) ? (entry - quotedLtp) * qty : 0;
        double buyT = quotedLtp > 0 ? quotedLtp * qty : 0;
        if (isCe) { ceLegPnl = pnl; ceClosePremium = quotedLtp; }
        else      { peLegPnl = pnl; peClosePremium = quotedLtp; }
        if (closeOrderId != null && !closeOrderId.isEmpty()) {
            realisedPnlToday        += pnl;
            consumedRiskToday       += pnl;
            buyPremiumTurnoverToday += buyT;
        }
        registerPendingFill(closeOrderId, isCe ? PendingType.CLOSE_CE : PendingType.CLOSE_PE,
            qty, entry, pnl, buyT);

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
        // Re-entry on SL — schedule a re-entry timestamp; the scheduler tick() picks it up.
        if ("CE_SL_HIT".equals(reason) || "PE_SL_HIT".equals(reason)) {
            scheduleReEntryIfEligible(isCe);
        }
    }

    /** Called from closeLeg on CE_SL_HIT / PE_SL_HIT. Sets a pending re-entry timestamp
     *  if the feature is enabled, the per-leg cap hasn't been reached, and the current
     *  time is before the configured cutoff. The tick() loop performs the actual re-entry
     *  once wall clock reaches the pending timestamp. */
    private void scheduleReEntryIfEligible(boolean isCe) {
        if (!riskSettings.getStrategyBool(instanceId, "reEntryOnSlEnabled", false)) return;
        int maxPerLeg = Math.max(0, riskSettings.getStrategyInt(instanceId, "maxReEntriesPerLeg", 1));
        int used = isCe ? ceReEntriesCount : peReEntriesCount;
        if (used >= maxPerLeg) {
            log.info("[short-straddle] Re-entry skipped for {} — cap reached ({}/{}).",
                isCe ? "CE" : "PE", used, maxPerLeg);
            return;
        }
        LocalTime latest = parseTime(
            riskSettings.getStrategyString(instanceId, "reEntryLatestTime", "14:30"), "14:30");
        if (LocalTime.now(IST).isAfter(latest)) {
            log.info("[short-straddle] Re-entry skipped for {} — past latest time {}.",
                isCe ? "CE" : "PE", latest);
            return;
        }
        int delaySec = Math.max(0, riskSettings.getStrategyInt(instanceId, "reEntryDelaySeconds", 300));
        long fireAt = System.currentTimeMillis() + delaySec * 1000L;
        if (isCe) pendingCeReEntryAtMillis = fireAt;
        else      pendingPeReEntryAtMillis = fireAt;
        String msg = (isCe ? "CE" : "PE") + " re-entry scheduled in " + delaySec + "s (attempt "
            + (used + 1) + "/" + maxPerLeg + ")";
        log.info("[short-straddle] {}", msg);
        eventService.log("[INFO] [short-straddle] " + msg);
        notifyTelegram(msg);
        // Persist so a mid-delay restart preserves the pending re-entry.
        persist();
    }

    /** Scheduler-invoked hook. Runs on every tick — cheap ints/longs. If a pending re-entry
     *  timestamp has elapsed for either side, place a fresh sell at the current ATM for that
     *  side. Skips gracefully if the state has since closed the day or the other leg has
     *  concurrently re-opened (unlikely). */
    private synchronized void processPendingReEntries() {
        long now = System.currentTimeMillis();
        boolean fired = false;
        if (pendingCeReEntryAtMillis > 0 && now >= pendingCeReEntryAtMillis) {
            pendingCeReEntryAtMillis = 0;
            performReEntry(true);
            fired = true;
        }
        if (pendingPeReEntryAtMillis > 0 && now >= pendingPeReEntryAtMillis) {
            pendingPeReEntryAtMillis = 0;
            performReEntry(false);
            fired = true;
        }
        if (fired) persist();
    }

    /** Place a fresh SELL for the given side at the current ATM. Re-uses the balanced ATM
     *  selector so the re-entry strike matches whatever the pipeline would pick fresh right
     *  now (may differ from the earlier SL'd strike if NIFTY has moved). Does NOT reset any
     *  day-level accumulators — this is not a new cycle. */
    private synchronized void performReEntry(boolean isCe) {
        // Re-check gates in case config changed or cutoff passed between schedule and fire.
        if (!riskSettings.getStrategyBool(instanceId, "reEntryOnSlEnabled", false)) return;
        LocalTime latest = parseTime(
            riskSettings.getStrategyString(instanceId, "reEntryLatestTime", "14:30"), "14:30");
        LocalTime nowT = LocalTime.now(IST);
        if (nowT.isAfter(latest)) {
            log.info("[short-straddle] Re-entry aborted for {} — past latest time {} at fire.",
                isCe ? "CE" : "PE", latest);
            return;
        }
        // Squareoff time is an absolute stop — no re-entries after the day's forced flat time.
        LocalTime squareOff = parseTime(getSquareOffTime(), "15:15");
        if (!nowT.isBefore(squareOff)) {
            log.info("[short-straddle] Re-entry aborted for {} — past squareoff time {}.",
                isCe ? "CE" : "PE", squareOff);
            return;
        }
        // Refuse to re-enter a side that is already open (defensive — should not happen because
        // scheduling only fires when the leg just SL'd).
        if (isCe && isCeOpen()) return;
        if (!isCe && isPeOpen()) return;

        double niftyLtp = marketDataService.getLtp(underlying().indexSymbol());
        if (niftyLtp <= 0) {
            log.warn("[short-straddle] Re-entry aborted for {} — NIFTY LTP unavailable.", isCe ? "CE" : "PE");
            return;
        }
        BalancedAtmSelector.AtmSelection sel = atmSelector.select(underlying(), niftyLtp);
        if (sel == null) {
            log.warn("[short-straddle] Re-entry aborted for {} — ATM selector failed.", isCe ? "CE" : "PE");
            return;
        }
        String sym = isCe ? sel.ceSymbolAtChosen() : sel.peSymbolAtChosen();
        if (sym == null || sym.isEmpty()) {
            log.warn("[short-straddle] Re-entry aborted for {} — selector missing symbol.", isCe ? "CE" : "PE");
            return;
        }
        int qty = Math.max(1, riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1)) * underlying().lotSize();
        String product = productType();

        OrderDTO resp = orderService.placeOrder(sym, qty, -1, 0, product);
        orderCountToday++;
        if (resp == null || resp.getId() == null || resp.getId().isEmpty() || !"ok".equals(resp.getStatus())) {
            log.error("[short-straddle] Re-entry rejected for {}: {}", isCe ? "CE" : "PE", resp);
            eventService.log("[ERROR] [short-straddle] " + (isCe ? "CE" : "PE") + " re-entry rejected");
            return;
        }

        try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(sym)); }
        catch (Exception ignored) {}

        double entryLtp = readEntryPremium(sym);
        if (isCe) {
            this.ceSymbol = sym;
            this.ceQty = qty;
            this.ceOrderId = resp.getId();
            this.ceEntryPremium = entryLtp;
            this.ceClosedAtMillis = 0;
            this.ceLegPnl = 0;
            this.ceClosePremium = 0;
            this.ceSlMovedToCost = false;
            ceReEntriesCount++;
            registerPendingFill(resp.getId(), PendingType.ENTRY_CE, qty, 0);
            transitionTo(isPeOpen() ? LifecycleState.OPEN_BOTH : LifecycleState.OPEN_CE_ONLY);
        } else {
            this.peSymbol = sym;
            this.peQty = qty;
            this.peOrderId = resp.getId();
            this.peEntryPremium = entryLtp;
            this.peClosedAtMillis = 0;
            this.peLegPnl = 0;
            this.peClosePremium = 0;
            this.peSlMovedToCost = false;
            peReEntriesCount++;
            registerPendingFill(resp.getId(), PendingType.ENTRY_PE, qty, 0);
            transitionTo(isCeOpen() ? LifecycleState.OPEN_BOTH : LifecycleState.OPEN_PE_ONLY);
        }
        String msg = (isCe ? "CE" : "PE") + " re-entered @ " + sym + " qty=" + qty
            + " ltp=" + String.format("%.2f", entryLtp) + " (NIFTY " + String.format("%.2f", niftyLtp) + ")";
        log.info("[short-straddle] {}", msg);
        eventService.log("[INFO] [short-straddle] " + msg);
        notifyTelegram(msg);
        pushEvent("RE_ENTRY_" + (isCe ? "CE" : "PE"), niftyLtp, isCe ? sym : "", isCe ? "" : sym, 0);
    }

    /** Close every leg still open. Used by timed squareoff + manual squareoff. */
    private void closeRemainingLegs(String reason) {
        java.util.List<String> unsubAfter = new java.util.ArrayList<>();
        double niftyAtClose = marketDataService.getLtp(underlying().indexSymbol());
        double totalPnl = 0;
        // Treat risk-event force-closes (per-strategy max-loss kill, portfolio kill) as SL
        // hits for analytics — the bot didn't choose to ride out to squareoff, the loss
        // budget did. Timed squareoff and stale-day reset do NOT count.
        boolean countAsSl = reason != null && reason.contains("MAX_LOSS");
        if (isCeOpen() && !ceSymbol.isEmpty() && ceQty > 0) {
            double ltp = marketDataService.getLtp(ceSymbol);
            double pnl  = (ceEntryPremium > 0 && ltp > 0) ? (ceEntryPremium - ltp) * ceQty : 0;
            double buyT = ltp > 0 ? ltp * ceQty : 0;
            ceLegPnl = pnl;
            ceClosePremium = ltp;
            totalPnl += pnl;
            String ceCloseId = placeCloseRetry(ceSymbol, ceQty, "CE", reason);
            // Provisional book — only when the close order was accepted. Keeps
            // the display honest if the WS fill push never lands. onActualFill
            // later applies the actual-vs-provisional delta.
            if (ceCloseId != null && !ceCloseId.isEmpty()) {
                realisedPnlToday        += pnl;
                consumedRiskToday       += pnl;
                buyPremiumTurnoverToday += buyT;
            }
            registerPendingFill(ceCloseId, PendingType.CLOSE_CE, ceQty, ceEntryPremium, pnl, buyT);
            unsubAfter.add(ceSymbol);
            this.ceClosedAtMillis = System.currentTimeMillis();
            this.ceQty = 0;
            this.ceOrderId = "";
            if (countAsSl) slHitsToday++;
        }
        if (isPeOpen() && !peSymbol.isEmpty() && peQty > 0) {
            double ltp = marketDataService.getLtp(peSymbol);
            double pnl  = (peEntryPremium > 0 && ltp > 0) ? (peEntryPremium - ltp) * peQty : 0;
            double buyT = ltp > 0 ? ltp * peQty : 0;
            peLegPnl = pnl;
            peClosePremium = ltp;
            totalPnl += pnl;
            String peCloseId = placeCloseRetry(peSymbol, peQty, "PE", reason);
            if (peCloseId != null && !peCloseId.isEmpty()) {
                realisedPnlToday        += pnl;
                consumedRiskToday       += pnl;
                buyPremiumTurnoverToday += buyT;
            }
            registerPendingFill(peCloseId, PendingType.CLOSE_PE, peQty, peEntryPremium, pnl, buyT);
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
        // All rate settings are stored as percent (e.g. 0.15 for 0.15%) —
        // divide by 100 to apply as a multiplier. Zero'd-out settings fall
        // back to the compile-time constants so charges never silently drop
        // to zero on a misconfiguration.
        double exchRateFrac  = riskSettings.getExchangeRate()  > 0 ? riskSettings.getExchangeRate()  / 100.0 : EXCH_TXN_PCT_FALLBACK;
        double sttRateFrac   = riskSettings.getSttRate()       > 0 ? riskSettings.getSttRate()       / 100.0 : STT_SELL_PCT_FALLBACK;
        double stampRateFrac = riskSettings.getStampDutyRate() > 0 ? riskSettings.getStampDutyRate() / 100.0 : STAMP_BUY_PCT_FALLBACK;
        double stt        = sellT * sttRateFrac;
        double exchange   = totalT * exchRateFrac;
        double sebi       = (totalT / 10_000_000.0) * SEBI_PER_CRORE;
        double stamp      = buyT * stampRateFrac;
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

    /** Pre-entry preview of the balanced ATM selection, cached for {@link #ATM_PREVIEW_TTL_MS}.
     *  NON-BLOCKING — returns the currently-cached value (possibly null on cold start) and
     *  triggers an async chain fetch when the cache is stale. The next dashboard poll picks
     *  up the freshly-cached value. Without this the first page load on each instance waited
     *  ~1-2 s for Fyers' option chain endpoint, freezing the entire dashboard payload —
     *  including the live positions table — behind the chain fetch. */
    public BalancedAtmSelector.AtmSelection getAtmPreview() {
        long now = System.currentTimeMillis();
        boolean fresh = cachedAtmPreview != null && (now - cachedAtmPreviewMs) < ATM_PREVIEW_TTL_MS;
        if (!fresh && atmRefreshInFlight.compareAndSet(false, true)) {
            CompletableFuture.runAsync(() -> {
                try {
                    double niftyLtp = marketDataService != null ? marketDataService.getLtp(underlying().indexSymbol()) : 0;
                    if (niftyLtp <= 0) return;
                    BalancedAtmSelector.AtmSelection picked = atmSelector.select(underlying(), niftyLtp);
                    if (picked != null) {
                        cachedAtmPreview   = picked;
                        cachedAtmPreviewMs = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    log.warn("[short-straddle] async ATM refresh failed: {}", e.getMessage());
                } finally {
                    atmRefreshInFlight.set(false);
                }
            });
        }
        return cachedAtmPreview;
    }

    // ── Dashboard payload (leg-sl shape) ───────────────────────────────────────
    @Override
    public java.util.Map<String, Object> getDashboard() {
        rolloverIfNewDay();
        // Detect a stale currentWeeklyExpiry — happens when the operator switches the
        // underlying (NIFTY ↔ SENSEX) mid-day. A value populated by a prior underlying
        // will land on the wrong weekday, so we clear it and force a refresh.
        if (currentWeeklyExpiry != null && !currentWeeklyExpiry.isEmpty()) {
            try {
                java.time.DayOfWeek expected = underlying().expiryDayOfWeek();
                if (LocalDate.parse(currentWeeklyExpiry).getDayOfWeek() != expected) {
                    this.currentWeeklyExpiry = "";
                }
            } catch (Exception ignored) {}
        }
        if (currentWeeklyExpiry == null || currentWeeklyExpiry.isEmpty()) {
            tryResolveWeeklyExpiry();
        }
        java.util.Map<String, Object> m = getStatus();
        m.put("dashboardShape",  "short-straddle");
        // Fallback to the underlying's deterministic next-expiry when the broker-confirmed
        // value has not resolved yet (first day of new expiry, pre-market, cold page load).
        String displayExpiry = (currentWeeklyExpiry != null && !currentWeeklyExpiry.isEmpty())
            ? currentWeeklyExpiry
            : nextExpectedWeeklyExpiry();
        m.put("weeklyExpiry",    displayExpiry);
        m.put("daysToExpiry",    tradingDaysToExpiry(displayExpiry));
        m.put("underlying",      underlying().name());
        synchronized (combinedPremiumSamples) {
            m.put("combinedPremiumSamples", new java.util.ArrayList<>(combinedPremiumSamples));
        }
        if (marketDataService != null) {
            m.put("niftyDisplayLtp", round2(marketDataService.getDisplayLtp(underlying().indexSymbol())));
            m.put("niftyChange",     round2(marketDataService.getDisplayChange(underlying().indexSymbol())));
            m.put("niftyChangePct",  round2(marketDataService.getDisplayChangePct(underlying().indexSymbol())));
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
        double niftyLtp = marketDataService != null ? marketDataService.getLtp(underlying().indexSymbol()) : 0;
        m.put("niftyLtp", niftyLtp);

        // Balanced-ATM projection — drives the projected strike shown on the CE/PE leg cards
        // pre-entry AND the disagreement banner on the + NEW STRADDLE confirm modal. Pre-entry
        // we compute live (cached 30 s); post-entry we surface the selection captured at the
        // time of the actual entry placement so the UI reflects what was actually traded.
        BalancedAtmSelector.AtmSelection atmInfo;
        boolean preEntry = (state == LifecycleState.ARMED) || (state == LifecycleState.DONE_FOR_DAY);
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

        // Per-leg greeks — computed via Black-Scholes on the fly. Skipped (fields become
        // null on the wire → dashboard renders "—") when the leg is closed, the option
        // hasn't ticked yet, or the IV inversion falls through (deep ITM in thin markets).
        double spotForGreeks = marketDataService != null ? marketDataService.getLtp(underlying().indexSymbol()) : 0;
        java.util.Map<String, Object> ceGreeks = isCeOpen()
            ? computeLegGreeks(ceSymbol, spotForGreeks, ceLtp, true)
            : emptyGreeks();
        java.util.Map<String, Object> peGreeks = isPeOpen()
            ? computeLegGreeks(peSymbol, spotForGreeks, peLtp, false)
            : emptyGreeks();
        m.put("ceDelta", ceGreeks.get("delta"));
        m.put("ceTheta", ceGreeks.get("theta"));
        m.put("ceVega",  ceGreeks.get("vega"));
        m.put("ceGamma", ceGreeks.get("gamma"));
        m.put("ceIv",    ceGreeks.get("iv"));
        m.put("peDelta", peGreeks.get("delta"));
        m.put("peTheta", peGreeks.get("theta"));
        m.put("peVega",  peGreeks.get("vega"));
        m.put("peGamma", peGreeks.get("gamma"));
        m.put("peIv",    peGreeks.get("iv"));

        // Per-leg SL triggers + consumed % (replaces combined SL in the leg-sl dashboard).
        // legSlPct / legSlPoints come from the per-day config — null on weekends, where the
        // Risk Band renders "—" rather than a misleading 50 % fallback. Points takes
        // precedence in the trigger formula when set.
        Double legSlPctBoxed    = todayLegSlPct();
        Double legSlPointsBoxed = todayLegSlPoints();
        m.put("legSlPct",    legSlPctBoxed);
        m.put("legSlPoints", legSlPointsBoxed);
        // Effective per-leg loss at SL — uses points if set, else (entryPremium × pct/100).
        // Fallback: when neither points nor pct is populated (weekend / DTE row unset /
        // currentWeeklyExpiry unresolved) but a leg is genuinely open, use 50 % so the
        // Active Risk tile still shows a meaningful number instead of "—".
        java.util.function.DoubleUnaryOperator legLossAtSl = (entry) -> {
            if (entry <= 0) return 0;
            if (legSlPointsBoxed != null && legSlPointsBoxed > 0) return legSlPointsBoxed;
            if (legSlPctBoxed    != null && legSlPctBoxed    > 0) return entry * (legSlPctBoxed / 100.0);
            return entry * 0.50;
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
        double ceTrigger = effectiveLegTrigger(true);
        if (ceTrigger > 0) {
            m.put("ceSlTrigger", round2(ceTrigger));
            double denom = ceTrigger - ceEntryPremium;
            double consumed = isCeOpen() && ceLtp > 0 && Math.abs(denom) > 0.0001
                ? ((ceLtp - ceEntryPremium) / denom) * 100.0 : 0;
            m.put("ceSlConsumedPct", round2(consumed));
        } else {
            m.put("ceSlTrigger", 0.0);
            m.put("ceSlConsumedPct", 0.0);
        }
        double peTrigger = effectiveLegTrigger(false);
        if (peTrigger > 0) {
            m.put("peSlTrigger", round2(peTrigger));
            double denom = peTrigger - peEntryPremium;
            double consumed = isPeOpen() && peLtp > 0 && Math.abs(denom) > 0.0001
                ? ((peLtp - peEntryPremium) / denom) * 100.0 : 0;
            m.put("peSlConsumedPct", round2(consumed));
        } else {
            m.put("peSlTrigger", 0.0);
            m.put("peSlConsumedPct", 0.0);
        }
        // Surface the "moved to cost" markers so the UI can render a badge next to the
        // CE/PE SL Trigger rows on the Risk Band card.
        m.put("ceSlMovedToCost", ceSlMovedToCost);
        m.put("peSlMovedToCost", peSlMovedToCost);

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
        // displayState — UI-facing label. When ARMED on a weekend / NSE holiday
        // the strategy isn't going to fire, so surface IDLE (with the reason
        // exposed separately for a tooltip). Internal state stays ARMED — this
        // is display-only.
        boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();
        String displayState = (state == LifecycleState.ARMED && !tradingDay) ? "IDLE" : state.name();
        m.put("displayState", displayState);
        if ("IDLE".equals(displayState)) {
            LocalDate today = LocalDate.now(IST);
            String reason;
            java.time.DayOfWeek dow = today.getDayOfWeek();
            if (dow == java.time.DayOfWeek.SATURDAY || dow == java.time.DayOfWeek.SUNDAY) {
                reason = "Market closed — weekend (" + dow.getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH) + "). Strategy is armed for the next trading day.";
            } else {
                reason = "Market closed — NSE holiday. Strategy is armed for the next trading day.";
            }
            m.put("displayStateReason", reason);
        }
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
        // Per-DTE toggle + active SL — dashboard market clock shows today's DTE row status.
        String dteKey = todayDteKey();
        m.put("todayDte",      dteKey.isEmpty() ? null : Integer.parseInt(dteKey));
        m.put("todayDayEnabled", isTodayDayEnabled());
        java.util.Map<String, Object> dteMap = new java.util.LinkedHashMap<>();
        for (String n : DTE_LEVELS) {
            java.util.Map<String, Object> e = new java.util.LinkedHashMap<>();
            e.put("enabled",     riskSettings.getStrategyBool(instanceId,   "dte." + n + ".enabled",     true));
            e.put("legSlPct",    riskSettings.getStrategyDouble(instanceId, "dte." + n + ".legSlPct",    50));
            e.put("legSlPoints", riskSettings.getStrategyDouble(instanceId, "dte." + n + ".legSlPoints", 0));
            dteMap.put(n, e);
        }
        m.put("dteConfig", dteMap);
        m.put("lotsPerLeg",    riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1));
        m.put("lotSize",       underlying().lotSize());
        m.put("enabled",       riskSettings.getStrategyBool(instanceId, "enabled", false));
        // Soft-pause flag — surfaced in the Today pane header. When true, scheduler skips
        // the ARMED → entry transition AND restartFromDoneForDay returns TRADING_PAUSED so
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
     *  state, so an ARMED leg-sl that hasn't entered yet won't fire its entry later. */
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
    public synchronized boolean resetToIdle(String reason) {
        // Refuse when legs are still open at the broker. Resetting would clear
        // ceSymbol/peSymbol/qty from memory but leave the broker holding the
        // shorts — instant orphan. Operator must squareoff first.
        if (state == LifecycleState.OPEN_BOTH
                || state == LifecycleState.OPEN_CE_ONLY
                || state == LifecycleState.OPEN_PE_ONLY) {
            log.warn("[short-straddle] resetToIdle refused — legs open (state={})", state);
            eventService.log("[WARNING] [short-straddle] RESET refused — legs are still open. Squareoff first.");
            return false;
        }
        log.info("[short-straddle] Manual reset from {} → ARMED ({})", state, reason);
        eventService.log("[INFO] [short-straddle] state reset to ARMED (" + reason + ")");
        this.ceSymbol = ""; this.peSymbol = "";
        this.ceQty = 0; this.peQty = 0;
        this.ceOrderId = ""; this.peOrderId = "";
        this.ceClosedAtMillis = 0; this.peClosedAtMillis = 0;
        transitionTo(LifecycleState.ARMED);
        return true;
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
        // Clear the pre-entry ATM preview cache — otherwise the dashboard keeps
        // showing yesterday's expiry's strikes until the 30s TTL naturally expires.
        this.cachedAtmPreview   = null;
        this.cachedAtmPreviewMs = 0;
        this.observedPreEntryWindow = false;
        this.ceSlMovedToCost = false;
        this.peSlMovedToCost = false;
        this.ceReEntriesCount = 0;
        this.peReEntriesCount = 0;
        this.pendingCeReEntryAtMillis = 0;
        this.pendingPeReEntryAtMillis = 0;
        transitionTo(LifecycleState.ARMED);
    }

    private void persistSessionFor(String date) {
        if (sessionRepo == null) return;
        java.util.Map<String, Double> chargesBreakdown = computeChargesBreakdown();
        double charges = chargesBreakdown.get("total");
        double gross   = realisedPnlToday;
        double net     = gross - charges;
        com.rydytrader.autotrader.entity.StrategySessionEntity row =
            sessionRepo.findByStrategyIdAndSessionDate(instanceId, date)
                       .orElseGet(com.rydytrader.autotrader.entity.StrategySessionEntity::new);
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
            JsonNode root = fyersClient.getOptionChain(underlying().indexSymbol(), 30, auth);
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

    /** Non-blocking: kicks off the Fyers chain fetch on a background thread when the cached
     *  weekly expiry is empty. Without this, the very first dashboard poll on each cold page
     *  load waited ~1-2 s for Fyers before any payload (including positions) could return.
     *  Gated by {@link #expiryRefreshInFlight} so concurrent polls don't pile up parallel
     *  fetches. The next poll picks up the freshly-resolved value. */
    private void tryResolveWeeklyExpiry() {
        if (!expiryRefreshInFlight.compareAndSet(false, true)) return;
        CompletableFuture.runAsync(() -> {
            try {
                String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
                if (auth == null || auth.endsWith(":") || auth.endsWith(":null")) return;
                JsonNode root = fyersClient.getOptionChain(underlying().indexSymbol(), 4, auth);
                if (root == null) return;
                JsonNode data = root.has("data") ? root.get("data") : null;
                JsonNode chain = data != null && data.has("optionsChain") ? data.get("optionsChain")
                    : (root.has("optionsChain") ? root.get("optionsChain") : null);
                if (chain == null || !chain.isArray()) return;
                Underlying u = underlying();
                for (JsonNode row : chain) {
                    String sym = row.has("symbol") ? row.get("symbol").asText() : "";
                    String exp = parseExpiryFromSymbol(sym, u);
                    if (!exp.isEmpty()) { this.currentWeeklyExpiry = exp; return; }
                }
            } catch (Exception e) {
                log.warn("[short-straddle] async expiry refresh failed: {}", e.getMessage());
            } finally {
                expiryRefreshInFlight.set(false);
            }
        });
    }

    /** Deterministic fallback used when {@link #currentWeeklyExpiry} is still empty —
     *  typically on the first day of a new expiry week, pre-market, before any leg has
     *  been placed AND before the async Fyers chain fetch has resolved. Walks forward day
     *  by day looking for the next trading Tuesday (NIFTY weekly expiry day as of 2026),
     *  skipping holidays via {@link MarketHolidayService}. Returns {@code ""} on failure. */
    private String nextExpectedWeeklyExpiry() {
        try {
            java.time.DayOfWeek expiryDow = underlying().expiryDayOfWeek();
            LocalDate cursor = LocalDate.now(IST);
            for (int i = 0; i < 14; i++) {
                if (cursor.getDayOfWeek() == expiryDow
                        && (marketHolidayService == null || marketHolidayService.isTradingDay(cursor))) {
                    return cursor.toString();
                }
                cursor = cursor.plusDays(1);
            }
        } catch (Exception ignored) {}
        return "";
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

    // ── Greeks — real-time Black-Scholes for leg cards ───────────────────────
    private static final java.util.regex.Pattern STRIKE_PATTERN =
        java.util.regex.Pattern.compile("(\\d{4,6})(?:CE|PE)$", java.util.regex.Pattern.CASE_INSENSITIVE);

    /** Extracts the strike price from a Fyers option symbol. Returns 0 when the tail
     *  doesn't match the expected {@code <strike>CE|PE} pattern. */
    private static long parseStrikeFromSymbol(String sym) {
        if (sym == null) return 0;
        java.util.regex.Matcher m = STRIKE_PATTERN.matcher(sym);
        return m.find() ? Long.parseLong(m.group(1)) : 0;
    }

    /** Precise time-to-expiry in years — includes the fractional hours remaining today
     *  vs 15:30 IST on the expiry date. Falls back to a 1-hour floor so BSM inputs stay
     *  well-defined on expiry-day afternoons. Returns 0 when expiry is unresolved. */
    private double yearsToExpiryPrecise() {
        if (currentWeeklyExpiry == null || currentWeeklyExpiry.isEmpty()) return 0;
        try {
            LocalDate expiry = LocalDate.parse(currentWeeklyExpiry);
            java.time.ZonedDateTime expiryClose =
                expiry.atTime(15, 30).atZone(IST);
            java.time.ZonedDateTime now = java.time.ZonedDateTime.now(IST);
            long seconds = java.time.temporal.ChronoUnit.SECONDS.between(now, expiryClose);
            if (seconds <= 0) return 1.0 / (365.0 * 24.0);
            return seconds / (365.0 * 24.0 * 3600.0);
        } catch (Exception e) { return 0; }
    }

    /** Returns a map of greeks (delta / theta / vega / gamma / iv) for one leg. All
     *  fields are {@code null} when the leg is closed, the option hasn't ticked yet,
     *  or the IV inversion fails (deep ITM in thin markets). {@code iv} is the implied
     *  vol expressed as a percentage. */
    private java.util.Map<String, Object> computeLegGreeks(String symbol, double spot,
                                                            double optionLtp, boolean isCall) {
        java.util.Map<String, Object> g = emptyGreeks();
        if (spot <= 0 || optionLtp <= 0 || symbol == null || symbol.isEmpty()) return g;
        long strike = parseStrikeFromSymbol(symbol);
        if (strike <= 0) return g;
        double T = yearsToExpiryPrecise();
        if (T <= 0) return g;
        double r = com.rydytrader.autotrader.util.BlackScholes.DEFAULT_RISK_FREE_RATE;
        double iv = com.rydytrader.autotrader.util.BlackScholes.impliedVol(spot, strike, T, r, optionLtp, isCall);
        if (iv <= 0) return g;
        g.put("delta", round4(com.rydytrader.autotrader.util.BlackScholes.delta(spot, strike, T, r, iv, isCall)));
        g.put("theta", round2(com.rydytrader.autotrader.util.BlackScholes.theta(spot, strike, T, r, iv, isCall)));
        g.put("vega",  round2(com.rydytrader.autotrader.util.BlackScholes.vega (spot, strike, T, r, iv)));
        g.put("gamma", round4(com.rydytrader.autotrader.util.BlackScholes.gamma(spot, strike, T, r, iv)));
        g.put("iv",    round2(iv * 100.0));
        return g;
    }

    private static java.util.Map<String, Object> emptyGreeks() {
        java.util.Map<String, Object> g = new java.util.LinkedHashMap<>();
        g.put("delta", null); g.put("theta", null); g.put("vega", null);
        g.put("gamma", null); g.put("iv", null);
        return g;
    }

    private static double round4(double v) { return Math.round(v * 10000.0) / 10000.0; }

    /** Decodes the tail of a Fyers option symbol into an ISO date. Handles both underlyings
     *  (NIFTY / SENSEX) and both encodings:
     *  <ul>
     *    <li>WEEKLY {@code YYMDD} (e.g. {@code 26929} → 2026-09-29, M is 1-9/O/N/D).</li>
     *    <li>MONTHLY {@code YYMON} (e.g. {@code 26SEP}) — walks back from the last day of
     *        the month to the underlying's own weekly expiry day.</li>
     *  </ul>
     *  Fyers uses the monthly format when a week's expiry day IS the monthly expiry too. */
    public static String parseExpiryFromSymbol(String fyersSymbol) {
        return parseExpiryFromSymbol(fyersSymbol, null);
    }

    /** Variant that takes the {@link Underlying} so the monthly-format decode knows which
     *  weekday to walk back to (Tuesday for NIFTY, or whatever SENSEX ends up being). When
     *  {@code u} is null (backward-compat call from legacy callers), defaults to Tuesday. */
    public static String parseExpiryFromSymbol(String fyersSymbol, Underlying u) {
        if (fyersSymbol == null) return "";
        try {
            int hash = -1;
            String matched = null;
            for (String p : new String[]{"NIFTY", "SENSEX"}) {
                int idx = fyersSymbol.indexOf(p);
                if (idx >= 0) { hash = idx; matched = p; break; }
            }
            if (hash < 0 || matched == null) return "";
            String tail = fyersSymbol.substring(hash + matched.length());
            if (tail.length() < 5) return "";
            int yr = Integer.parseInt(tail.substring(0, 2));
            String maybeMonth3 = tail.substring(2, 5).toUpperCase();
            int monthlyIdx = "JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC".indexOf(maybeMonth3);
            if (monthlyIdx >= 0 && monthlyIdx % 3 == 0) {
                int month = monthlyIdx / 3 + 1;
                java.time.DayOfWeek expiryDow;
                if (u != null) expiryDow = u.expiryDayOfWeek();
                else expiryDow = "SENSEX".equals(matched)
                    ? java.time.DayOfWeek.THURSDAY
                    : java.time.DayOfWeek.TUESDAY;
                LocalDate last = LocalDate.of(2000 + yr, month, 1)
                    .withDayOfMonth(java.time.YearMonth.of(2000 + yr, month).lengthOfMonth());
                while (last.getDayOfWeek() != expiryDow) last = last.minusDays(1);
                return last.toString();
            }
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
            com.rydytrader.autotrader.entity.StrategyTradeEntity t =
                new com.rydytrader.autotrader.entity.StrategyTradeEntity();
            t.setStrategyId(instanceId);
            t.setSessionDate(dayKey != null && !dayKey.isEmpty() ? dayKey : LocalDate.now(IST).toString());
            t.setClosedAtMillis(System.currentTimeMillis());
            int qty = Math.max(ceQty, peQty);
            if (qty == 0) qty = Math.max(1, riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1)) * underlying().lotSize();
            t.setQty(qty);
            t.setGrossPnl(round2(cycleGross));
            t.setCharges(round2(charges));
            t.setNetPnl(round2(cycleGross - charges));
            t.setCloseReason("DONE_FOR_DAY");
            t.setSlHitCount(cycleSls);
            tradeRepo.save(t);
            // Re-baseline cycle-start counters to the current values. Without this, a next
            // cycle in the same day (e.g. a scheduled re-entry that fires after this
            // DONE_FOR_DAY and ends in another DONE_FOR_DAY at squareoff) would compute
            // its cycleGross = cumulativeRealised - originalCycleStart(0), double-counting
            // this cycle's P&L on the next row. Snapshot post-write so the next cycle's
            // row carries only the delta from now onward.
            this.cycleStartRealisedPnl   = realisedPnlToday          + inFlightPnl;
            this.cycleStartSellTurnover  = sellPremiumTurnoverToday;
            this.cycleStartBuyTurnover   = buyPremiumTurnoverToday   + inFlightBuyTurnover;
            this.cycleStartOrderCount    = orderCountToday;
            this.cycleStartSlHits        = slHitsToday;
        } catch (Exception e) {
            log.warn("[short-straddle] Failed to persist straddle_trades row: {}", e.getMessage());
        }
    }

    /** Same formula as the session-level breakdown but applied to a specific cycle's totals. */
    private double computeCycleCharges(double sellPrem, double buyPrem, int orders) {
        double brokerage = orders * riskSettings.getBrokeragePerOrder();
        double totalPrem = sellPrem + buyPrem;
        double exchRateFrac  = riskSettings.getExchangeRate()  > 0 ? riskSettings.getExchangeRate()  / 100.0 : EXCH_TXN_PCT_FALLBACK;
        double sttRateFrac   = riskSettings.getSttRate()       > 0 ? riskSettings.getSttRate()       / 100.0 : STT_SELL_PCT_FALLBACK;
        double stampRateFrac = riskSettings.getStampDutyRate() > 0 ? riskSettings.getStampDutyRate() / 100.0 : STAMP_BUY_PCT_FALLBACK;
        double stt       = sellPrem * sttRateFrac;
        double exchange  = totalPrem * exchRateFrac;
        double sebi      = (totalPrem / 10_000_000.0) * SEBI_PER_CRORE;
        double stamp     = buyPrem * stampRateFrac;
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
        s.ceSlMovedToCost = this.ceSlMovedToCost;
        s.peSlMovedToCost = this.peSlMovedToCost;
        s.ceReEntriesCount = this.ceReEntriesCount;
        s.peReEntriesCount = this.peReEntriesCount;
        s.pendingCeReEntryAtMillis = this.pendingCeReEntryAtMillis;
        s.pendingPeReEntryAtMillis = this.pendingPeReEntryAtMillis;
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
