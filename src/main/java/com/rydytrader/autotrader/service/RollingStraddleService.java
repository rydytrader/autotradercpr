package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.StraddleStateStore;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * Rolling Short Straddle on NIFTY weekly options.
 *
 * <p>Self-contained options-selling strategy that runs in parallel with the equity breakout
 * pipeline. See {@code memory/project_rolling_straddle.md} for the full design and
 * {@code C:\Users\RYDYTRADER\.claude\plans\steady-humming-clock.md} for the implementation plan.
 *
 * <p>Lifecycle per trading day (gated by {@code enableRollingStraddle}):
 * <ol>
 *   <li>At {@code straddleEntryTime}: SELL ATM CE + ATM PE on the current weekly expiry.</li>
 *   <li>On every 5s tick while OPEN: if {@code |nifty - lastEntryNifty| / lastEntryNifty &times; 100 &ge; movePctTrigger},
 *       close both legs and re-enter fresh ATM CE+PE. Each roll resets {@code lastEntryNifty}.</li>
 *   <li>After {@code rollCount &gt;= maxRolls} the next trigger keeps the open straddle to the
 *       timed squareoff for max theta (state {@code MAX_ROLLS_HOLD}).</li>
 *   <li>At {@code straddleSquareOffTime}: close any open legs at market. State {@code DONE_FOR_DAY}.</li>
 * </ol>
 */
@Service
public class RollingStraddleService implements Strategy {

    /** Stable identifier for this strategy. Used in URLs, settings keys, log prefixes,
     *  order tags, and the session-row strategy_id column. */
    public static final String STRATEGY_ID = "combined-sl-roll";

    @Override public String id()          { return STRATEGY_ID; }
    @Override public String displayName() { return "Combined SL + Roll"; }
    @Override public String description() { return "ATM straddle on NIFTY weekly · combined-premium SL · re-enter on roll"; }
    @Override public String currentState(){ return state.name(); }
    @Override public String navIcon()     { return "C"; }
    @Override public boolean forceClose(String reason) { return forceCloseBothLegs(reason); }
    @Override public String currentWeeklyExpiry() { return currentWeeklyExpiry; }

    /** Live net day P&L (realised + open MTM − charges). Used by the portfolio kill switch. */
    @Override
    public double liveNetPnlToday() {
        if (marketDataService == null) return realisedPnlToday;
        double ceLtp = (ceSymbol != null && !ceSymbol.isEmpty()) ? marketDataService.getLtp(ceSymbol) : 0;
        double peLtp = (peSymbol != null && !peSymbol.isEmpty()) ? marketDataService.getLtp(peSymbol) : 0;
        double ceMtm = (ceEntryPremium > 0 && ceLtp > 0 && ceQty > 0) ? (ceEntryPremium - ceLtp) * ceQty : 0;
        double peMtm = (peEntryPremium > 0 && peLtp > 0 && peQty > 0) ? (peEntryPremium - peLtp) * peQty : 0;
        double charges = computeChargesBreakdown().getOrDefault("total", 0.0);
        return realisedPnlToday + ceMtm + peMtm - charges;
    }

    /** Today's CLOSE_* roll events from the in-memory ring, mapped to the Analytics live-overlay
     *  shape. {@code recentRolls} is cleared on day rollover so anything present here is from
     *  the current trading day. Skips ENTRY / ROLL marker events (no close P&L yet). */
    @Override
    public java.util.List<java.util.Map<String, Object>> todayClosedTrades() {
        java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>();
        for (RollEvent r : recentRolls) {
            if (r == null || r.event() == null || !r.event().startsWith("CLOSE_")) continue;
            java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("grossPnl", r.pnl());
            m.put("closedAtMillis", System.currentTimeMillis()); // ring lacks epoch; approximate
            m.put("closeReason", r.event().substring("CLOSE_".length()));
            out.add(m);
        }
        return out;
    }

    @Override
    public java.util.List<java.util.Map<String, Object>> getSettingsSchema() {
        java.util.List<java.util.Map<String, Object>> s = new java.util.ArrayList<>();
        s.add(field("enabled",              "boolean", true,    "Enable Strategy",
            "Bot only fires entries / SL checks when enabled. Disable to pause without losing today's state."));
        s.add(field("entryTime",            "time",    "09:20", "Entry Time (HH:mm IST)", null));
        s.add(field("squareOffTime",        "time",    "15:15", "Squareoff Time (HH:mm IST)", null));
        s.add(field("rollWaitMin",          "int",     15,      "Roll Wait (minutes)",
            "Wait this many minutes after an SL hit before re-entering. Avoids whipsaw. 0 = roll immediately."));
        s.add(field("rollCutoffTime",       "time",    "14:30", "Roll Cutoff Time (HH:mm IST)",
            "If SL fires after this time (or the wait would push past it), bot closes and parks DONE_FOR_DAY."));
        s.add(field("maxRolls",             "int",     3,       "Max Rolls",
            "Number of rolls before holding to squareoff."));
        s.add(field("lotsPerLeg",           "int",     1,       "Lots per Leg",
            "Qty = lots × NIFTY lot size (65)."));
        s.add(field("combinedTargetPct",    "percent", 30,      "Combined Premium Target (%)",
            "Book profit when (CE + PE LTP) falls below entry combined premium by this %. 0 disables."));
        s.add(field("combinedSlPct",        "percent", 30,      "Combined Premium SL (%)",
            "Roll when (CE + PE LTP) exceeds entry combined premium by this %."));
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
        v.put("enabled",             riskSettings.isEnableRollingStraddle());
        v.put("entryTime",           riskSettings.getStraddleEntryTime());
        v.put("squareOffTime",       riskSettings.getStraddleSquareOffTime());
        v.put("rollWaitMin",         riskSettings.getStraddleRollWaitMin());
        v.put("rollCutoffTime",      riskSettings.getStraddleRollCutoffTime());
        v.put("maxRolls",            riskSettings.getStraddleMaxRolls());
        v.put("lotsPerLeg",          riskSettings.getStraddleLotsPerLeg());
        v.put("combinedTargetPct",   riskSettings.getStraddleCombinedTargetPct());
        v.put("combinedSlPct",       riskSettings.getStraddleCombinedSlPct());
        return v;
    }

    @Override
    public void saveSettings(java.util.Map<String, Object> values) {
        if (values == null) return;
        // Legacy field setters — these continue to drive the running strategy. The keys here
        // match the schema entries' "key" field so the UI is symmetric for read/write.
        if (values.containsKey("enabled"))           riskSettings.setEnableRollingStraddle(asBool(values.get("enabled")));
        if (values.containsKey("entryTime"))         riskSettings.setStraddleEntryTime(String.valueOf(values.get("entryTime")));
        if (values.containsKey("squareOffTime"))     riskSettings.setStraddleSquareOffTime(String.valueOf(values.get("squareOffTime")));
        if (values.containsKey("rollWaitMin"))       riskSettings.setStraddleRollWaitMin(asInt(values.get("rollWaitMin"), 15));
        if (values.containsKey("rollCutoffTime"))    riskSettings.setStraddleRollCutoffTime(String.valueOf(values.get("rollCutoffTime")));
        if (values.containsKey("maxRolls"))          riskSettings.setStraddleMaxRolls(asInt(values.get("maxRolls"), 3));
        if (values.containsKey("lotsPerLeg"))        riskSettings.setStraddleLotsPerLeg(asInt(values.get("lotsPerLeg"), 1));
        if (values.containsKey("combinedTargetPct")) riskSettings.setStraddleCombinedTargetPct(asDouble(values.get("combinedTargetPct"), 30));
        if (values.containsKey("combinedSlPct"))     riskSettings.setStraddleCombinedSlPct(asDouble(values.get("combinedSlPct"), 30));
        riskSettings.saveFor("live");
    }

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

    private static final Logger log = LoggerFactory.getLogger(RollingStraddleService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int NIFTY_LOT_SIZE = 65; // bump if NSE changes the contract size
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final int STRIKE_STEP = 50;    // NIFTY strikes increment by 50

    public enum LifecycleState { IDLE, OPEN, WAITING_TO_ROLL, MAX_ROLLS_HOLD, DONE_FOR_DAY }

    private final RiskSettingsStore riskSettings;
    private final StraddleStateStore stateStore;
    private final EventService eventService;
    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final FyersClientRouter fyersClient;

    // Lazy-wired to avoid the MarketDataService ↔ * boot cycles and to allow the service to
    // come up even when these aren't fully initialized at construction time.
    @Autowired @Lazy private MarketDataService marketDataService;
    @Autowired @Lazy private OrderService orderService;
    @Autowired @Lazy private MarketHolidayService marketHolidayService;
    @Autowired @Lazy private TelegramService telegramService;
    @Autowired @Lazy private com.rydytrader.autotrader.repository.StraddleSessionRepository sessionRepo;
    @Autowired @Lazy private com.rydytrader.autotrader.repository.StraddleTradeRepository tradeRepo;

    // ── In-memory state (mirrors StraddleStateStore.State; persisted on every transition) ──
    private volatile LifecycleState state = LifecycleState.IDLE;
    private volatile String  dayKey         = "";
    private volatile double  lastEntryNifty = 0;
    private volatile String  ceSymbol       = "";
    private volatile String  peSymbol       = "";
    private volatile int     ceQty          = 0;
    private volatile int     peQty          = 0;
    private volatile int     rollCount      = 0;
    private volatile String  ceOrderId      = "";
    private volatile String  peOrderId      = "";
    /** Premium collected on each leg's SELL fill (used for live MTM). Volatile primitives;
     *  set once at entry, read on every dashboard fetch. */
    private volatile double  ceEntryPremium = 0;
    private volatile double  peEntryPremium = 0;
    /** Today's roll-event ring buffer for the Home dashboard "Recent rolls" table. */
    private final java.util.Deque<RollEvent> recentRolls = new java.util.ArrayDeque<>();
    /** Cumulative realised P&L for the current day (sum of premium collected - premium paid back
     *  across all closed legs today). Reset on day rollover. */
    private volatile double  realisedPnlToday = 0;
    /** Premium-turnover trackers for accurate charge computation (STT, exchange, SEBI all scale
     *  with premium × qty). Sell side accumulates on each entry/roll-entry; buy side on each
     *  close/roll-exit. Reset on day rollover alongside realisedPnlToday. */
    private volatile double  sellPremiumTurnoverToday = 0;
    private volatile double  buyPremiumTurnoverToday  = 0;
    /** Actual count of orders placed today. Drives the brokerage component of charges, so pre-
     *  entry the dashboard shows ₹0 charges instead of the previous formula's assumed cycle. */
    private volatile int     orderCountToday          = 0;
    /** Cached current weekly expiry date (ISO yyyy-MM-dd). Set from {@code ceSymbol} on entry;
     *  lazy-fetched from the Fyers option chain when IDLE so the dashboard can show it without
     *  needing an open position. Reset on day rollover; re-resolved on next dashboard hit. */
    private volatile String  currentWeeklyExpiry = "";
    /** Epoch millis when the combined-premium SL last fired. The bot sits in WAITING_TO_ROLL
     *  until now - slHitTimeMillis >= straddleRollWaitMin minutes, then attempts re-entry
     *  (unless we're past the roll cutoff time, in which case it parks DONE_FOR_DAY). */
    private volatile long    slHitTimeMillis     = 0;
    /** 1-minute combined-premium samples for the dashboard's straddle chart. List of
     *  {t: "HH:mm", v: ceLtp + peLtp}. Sloping DOWN = good for short straddle (premiums
     *  decaying). Cleared on day rollover, persisted to disk. */
    private final java.util.List<java.util.Map<String, Object>> combinedPremiumSamples =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    public static record RollEvent(String time, String event, double nifty,
                                    String ce, String pe, double pnl) {}

    public RollingStraddleService(RiskSettingsStore riskSettings,
                                   StraddleStateStore stateStore,
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

    @PostConstruct
    public void init() {
        // Resume from disk so a mid-day restart picks up open legs.
        StraddleStateStore.State persisted = stateStore.get();
        if (persisted != null && persisted.state != null) {
            try {
                this.state = LifecycleState.valueOf(persisted.state);
            } catch (IllegalArgumentException ex) {
                this.state = LifecycleState.IDLE;
            }
            this.dayKey         = persisted.dayKey != null ? persisted.dayKey : "";
            this.lastEntryNifty = persisted.lastEntryNifty;
            this.ceSymbol       = persisted.ceSymbol != null ? persisted.ceSymbol : "";
            this.peSymbol       = persisted.peSymbol != null ? persisted.peSymbol : "";
            this.ceQty          = persisted.ceQty;
            this.peQty          = persisted.peQty;
            this.rollCount      = persisted.rollCount;
            this.ceOrderId      = persisted.ceOrderId != null ? persisted.ceOrderId : "";
            this.peOrderId      = persisted.peOrderId != null ? persisted.peOrderId : "";
            this.ceEntryPremium = persisted.ceEntryPremium;
            this.peEntryPremium = persisted.peEntryPremium;
            this.realisedPnlToday = persisted.realisedPnlToday;
            this.sellPremiumTurnoverToday = persisted.sellPremiumTurnoverToday;
            this.buyPremiumTurnoverToday = persisted.buyPremiumTurnoverToday;
            this.orderCountToday = persisted.orderCountToday;
            this.currentWeeklyExpiry = persisted.currentWeeklyExpiry != null ? persisted.currentWeeklyExpiry : "";
            this.slHitTimeMillis = persisted.slHitTimeMillis;
            if (persisted.combinedPremiumSamples != null) {
                this.combinedPremiumSamples.clear();
                this.combinedPremiumSamples.addAll(persisted.combinedPremiumSamples);
            }
            if (persisted.recentRolls != null) {
                this.recentRolls.clear();
                // Persisted as List<Map>; rebuild RollEvent records. Order in JSON is most-recent-
                // first (matches the ring); add directly so addFirst order matches existing pattern.
                for (java.util.Map<String, Object> r : persisted.recentRolls) {
                    String time  = String.valueOf(r.getOrDefault("time", ""));
                    String event = String.valueOf(r.getOrDefault("event", ""));
                    double nifty = ((Number) r.getOrDefault("nifty", 0)).doubleValue();
                    String ce    = String.valueOf(r.getOrDefault("ce", ""));
                    String pe    = String.valueOf(r.getOrDefault("pe", ""));
                    double pnl   = ((Number) r.getOrDefault("pnl", 0)).doubleValue();
                    this.recentRolls.addLast(new RollEvent(time, event, nifty, ce, pe, pnl));
                }
            }
            log.info("[Straddle] Resumed state={} dayKey={} rollCount={} lastEntryNifty={} ce={} pe={} ceEntry={} peEntry={} realised={}",
                state, dayKey, rollCount, lastEntryNifty, ceSymbol, peSymbol,
                ceEntryPremium, peEntryPremium, realisedPnlToday);
        }
        // Stale-day handling: any non-IDLE state from a prior trading day → reset.
        rolloverIfNewDay();
        // Re-subscribe the data WS to any legs still considered open after rollover. Without
        // this, getLtp would stay at 0 for the legs and MTM would never tick post-restart.
        if ((state == LifecycleState.OPEN || state == LifecycleState.MAX_ROLLS_HOLD)
                && ceSymbol != null && !ceSymbol.isEmpty()
                && peSymbol != null && !peSymbol.isEmpty()) {
            try {
                marketDataService.subscribeAdditional(java.util.Arrays.asList(ceSymbol, peSymbol));
                log.info("[Straddle] Re-subscribed open legs to data WS after restart: ce={} pe={}",
                    ceSymbol, peSymbol);
                // Seed LTP + prev close from REST quote so dashboard MTM and change badges
                // populate immediately, without waiting for the first WS tick to arrive.
                seedLegQuote(ceSymbol);
                seedLegQuote(peSymbol);
            } catch (Exception e) {
                log.warn("[Straddle] Re-subscribe failed: {}", e.getMessage());
            }
        }
    }

    /** Attempted on every tick while open legs are missing entry premium (typical on first
     *  restart after an upgrade — Fyers token isn't available at @PostConstruct time, so we
     *  defer until trading hours when it is). Idempotent: silently no-ops once recovered. */
    private void tryRecoverEntryPremiumFromTradebook() {
        if (state != LifecycleState.OPEN && state != LifecycleState.MAX_ROLLS_HOLD) return;
        if (ceEntryPremium > 0 && peEntryPremium > 0) return; // already recovered
        if (tokenStore.getAccessToken() == null || tokenStore.getAccessToken().isEmpty()) return;
        boolean changed = false;
        if (ceEntryPremium == 0 && ceOrderId != null && !ceOrderId.isEmpty()) {
            try {
                double fill = orderService.getFilledPriceByOrderId(ceOrderId);
                if (fill > 0) {
                    ceEntryPremium = fill;
                    log.info("[Straddle] Recovered CE entry premium from tradebook: {} (orderId={})",
                        fill, ceOrderId);
                    changed = true;
                }
            } catch (Exception ignored) {}
        }
        if (peEntryPremium == 0 && peOrderId != null && !peOrderId.isEmpty()) {
            try {
                double fill = orderService.getFilledPriceByOrderId(peOrderId);
                if (fill > 0) {
                    peEntryPremium = fill;
                    log.info("[Straddle] Recovered PE entry premium from tradebook: {} (orderId={})",
                        fill, peOrderId);
                    changed = true;
                }
            } catch (Exception ignored) {}
        }
        if (changed) {
            if (sellPremiumTurnoverToday == 0 && ceEntryPremium > 0 && peEntryPremium > 0
                    && ceQty > 0 && peQty > 0) {
                sellPremiumTurnoverToday = (ceEntryPremium * ceQty) + (peEntryPremium * peQty);
            }
            if (orderCountToday == 0) orderCountToday = 2 * (rollCount + 1);
            persist();
        }
    }

    /** One-shot REST quote → seeds TickData for the given leg symbol. Used on restart and
     *  whenever we want the dashboard to display fresh change/% without waiting for WS. */
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
            log.warn("[Straddle] Seed leg quote failed for {}: {}", symbol, e.getMessage());
        }
    }

    /** Captures one combined-premium sample (ceLtp + peLtp) every minute during trading hours
     *  for the dashboard's straddle chart. Only samples when both legs are alive (OPEN or
     *  MAX_ROLLS_HOLD) — between SL hit and re-entry, or post-squareoff, we don't have
     *  meaningful CE/PE LTPs to chart. */
    @Scheduled(cron = "0 * * * * MON-FRI", zone = "Asia/Kolkata")
    public void sampleCombinedPremium() {
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;
        if (state != LifecycleState.OPEN && state != LifecycleState.MAX_ROLLS_HOLD) return;
        LocalTime now = LocalTime.now(IST);
        LocalTime entryTime     = parseTime(riskSettings.getStraddleEntryTime(),     "09:20");
        LocalTime squareOffTime = parseTime(riskSettings.getStraddleSquareOffTime(), "15:15");
        if (now.isBefore(entryTime) || now.isAfter(squareOffTime.plusMinutes(5))) return;
        if (ceSymbol == null || ceSymbol.isEmpty() || peSymbol == null || peSymbol.isEmpty()) return;
        double ceLtp = marketDataService.getLtp(ceSymbol);
        double peLtp = marketDataService.getLtp(peSymbol);
        if (ceLtp <= 0 || peLtp <= 0) return;
        java.util.Map<String, Object> sample = new java.util.LinkedHashMap<>();
        sample.put("t", now.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm")));
        sample.put("v", Math.round((ceLtp + peLtp) * 100.0) / 100.0);
        combinedPremiumSamples.add(sample);
        persist();
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        // Day rollover runs BEFORE the enable + market-open guards so weekend/holiday/pre-market
        // ticks still reset yesterday's P&L cleanly and persist the prior session row.
        rolloverIfNewDay();
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        // Enabled-flag guard. Default true — operator can pause entries without restart via
        // Settings → COMBINED ROLL → Enable Strategy.
        if (!riskSettings.isEnableRollingStraddle()) return;
        // Late-recover entry premium from tradebook if init() couldn't (no token at boot).
        tryRecoverEntryPremiumFromTradebook();

        LocalTime now = LocalTime.now(IST);
        LocalTime entryTime     = parseTime(riskSettings.getStraddleEntryTime(),     "09:20");
        LocalTime squareOffTime = parseTime(riskSettings.getStraddleSquareOffTime(), "15:15");
        if (!squareOffTime.isAfter(entryTime)) {
            log.warn("[Straddle] squareOffTime {} must be after entryTime {} — skipping tick",
                squareOffTime, entryTime);
            return;
        }

        switch (state) {
            case IDLE -> {
                if (afterOrAt(now, entryTime) && now.isBefore(squareOffTime)) {
                    doInitialEntry();
                }
            }
            case OPEN -> checkCombinedSlOrSquareoff(now, squareOffTime);
            case WAITING_TO_ROLL -> checkRollWait(now, squareOffTime);
            case MAX_ROLLS_HOLD -> {
                if (checkMaxLossKillSwitch()) return;
                if (afterOrAt(now, squareOffTime)) {
                    closeBothLegs("TIMED_SQUAREOFF");
                    transitionTo(LifecycleState.DONE_FOR_DAY);
                }
            }
            case DONE_FOR_DAY -> { /* idle until tomorrow's rollover */ }
        }
    }

    // ── Initial entry ──────────────────────────────────────────────────────────
    private void doInitialEntry() {
        String niftySym = NIFTY_SYMBOL;
        double niftyLtp = marketDataService.getLtp(niftySym);
        if (niftyLtp <= 0) {
            log.info("[Straddle] Skipping entry — NIFTY LTP unavailable (waiting for first tick)");
            return;
        }
        long atmStrike = Math.round(niftyLtp / STRIKE_STEP) * (long) STRIKE_STEP;
        String[] symbols = resolveAtmSymbols(atmStrike);
        if (symbols == null) {
            log.warn("[Straddle] Could not resolve ATM CE+PE symbols for strike {} — aborting entry for today", atmStrike);
            eventService.log("[ERROR] Straddle entry aborted — failed to resolve ATM symbols for strike " + atmStrike);
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        String resolvedCe = symbols[0];
        String resolvedPe = symbols[1];

        int qty = Math.max(1, riskSettings.getStraddleLotsPerLeg()) * NIFTY_LOT_SIZE;
        // SELL = -1 in Fyers (we collect premium). No SL / no target.
        OrderDTO ceResp = orderService.placeOrder(resolvedCe, qty, -1, 0);
        orderCountToday++;
        if (ceResp == null || ceResp.getId() == null || ceResp.getId().isEmpty() || !"ok".equals(ceResp.getStatus())) {
            log.error("[Straddle] CE leg rejected: {}", ceResp);
            eventService.log("[ERROR] Straddle CE leg rejected — aborting day");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        OrderDTO peResp = orderService.placeOrder(resolvedPe, qty, -1, 0);
        orderCountToday++;
        if (peResp == null || peResp.getId() == null || peResp.getId().isEmpty() || !"ok".equals(peResp.getStatus())) {
            log.error("[Straddle] PE leg rejected after CE filled — flattening CE: {}", peResp);
            eventService.log("[ERROR] Straddle PE leg rejected — buying back CE immediately to flatten");
            // BUY back the CE to flatten — never leave one leg unhedged.
            try { orderService.placeOrder(resolvedCe, qty, 1, 0); orderCountToday++; } catch (Exception ignored) {}
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }

        this.ceSymbol       = resolvedCe;
        this.peSymbol       = resolvedPe;
        this.ceQty          = qty;
        this.peQty          = qty;
        this.ceOrderId      = ceResp.getId();
        this.peOrderId      = peResp.getId();
        this.lastEntryNifty = niftyLtp;
        // Subscribe BEFORE reading entry premium so the WS has a chance to push the first tick.
        // Even with that head start the WS round-trip is ~500ms-2s, so readEntryPremium falls
        // back to a one-shot REST quote when the in-memory LTP is still zero.
        try {
            marketDataService.subscribeAdditional(java.util.Arrays.asList(resolvedCe, resolvedPe));
        } catch (Exception ignored) {}
        this.ceEntryPremium = readEntryPremium(ceResp, resolvedCe);
        this.peEntryPremium = readEntryPremium(peResp, resolvedPe);
        // Capture current weekly expiry from the resolved option symbol (cheaper than re-fetching).
        this.currentWeeklyExpiry = parseExpiryFromSymbol(resolvedCe);
        // Accumulate sell-side premium turnover for STT/exchange/SEBI charge calc.
        this.sellPremiumTurnoverToday += (ceEntryPremium * qty) + (peEntryPremium * qty);
        // rollCount is only reset by rolloverIfNewDay on initial-day entry. Subsequent rolls
        // increment it in checkMoveTriggerOrSquareoff before calling back into doInitialEntry.
        transitionTo(LifecycleState.OPEN);

        String evtName = rollCount == 0 ? "ENTRY" : "ROLL";
        pushRollEvent(evtName, niftyLtp, resolvedCe, resolvedPe, 0);

        String msg = "Straddle armed @ ATM " + atmStrike + " (NIFTY " + String.format("%.2f", niftyLtp)
            + ") qty=" + qty + " roll=" + rollCount + "/" + riskSettings.getStraddleMaxRolls()
            + " ce=" + ceSymbol + " pe=" + peSymbol;
        log.info("[Straddle] {}", msg);
        eventService.log("[INFO] " + msg);
        notifyTelegram(msg);
    }

    /** Best-effort entry premium read — Fyers `OrderDTO` doesn't carry the fill price, so we
     *  fall back to the leg's current LTP on the data WS (close to the actual fill for market
     *  orders). When neither is available, returns 0 — MTM will be 0 until the WS has a tick. */
    /** Capture the entry premium of a freshly-placed leg. Tries the in-memory WS tick first
     *  (cheapest); if the leg hasn't received its first tick yet (subscribe + ack takes time),
     *  falls back to a synchronous Fyers /data/quotes REST call so we always have a non-zero
     *  baseline for MTM. Also seeds prev_close into the leg's TickData so the dashboard's
     *  change/% display starts populating right away (the WS often skips prev_close in its
     *  first ticks for newly-subscribed option symbols). Returns 0 only as a last resort. */
    private double readEntryPremium(OrderDTO resp, String symbol) {
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
                    log.info("[Straddle] Entry premium for {} captured via REST quote: lp={} prevClose={}",
                        symbol, lp, prevClose);
                    return lp;
                }
            }
        } catch (Exception e) {
            log.warn("[Straddle] REST quote fallback failed for {}: {}", symbol, e.getMessage());
        }
        return 0;
    }

    /** Save (or update) the {@code straddle_sessions} row for the given trading day. Idempotent —
     *  re-runs on the same day overwrite the row. Premium collected/paid back are best-effort
     *  approximations computed from the in-memory roll log + realised P&L. */
    private void persistSessionFor(String date) {
        if (sessionRepo == null) return;
        double charges = estimateDailyCharges();
        double grossPnl = realisedPnlToday;
        double netPnl = grossPnl - charges;
        com.rydytrader.autotrader.entity.StraddleSessionEntity row =
            sessionRepo.findByStrategyIdAndSessionDate(STRATEGY_ID, date)
                       .orElseGet(com.rydytrader.autotrader.entity.StraddleSessionEntity::new);
        row.setStrategyId(STRATEGY_ID);
        row.setSessionDate(date);
        row.setEntries(rollCount > 0 || ceSymbol != null && !ceSymbol.isEmpty() ? 1 : 0);
        row.setRolls(rollCount);
        row.setFinalState(state.name());
        row.setPremiumCollected(round2(sellPremiumTurnoverToday));
        row.setPremiumPaidBack(round2(buyPremiumTurnoverToday));
        row.setGrossPnl(grossPnl);
        row.setCharges(charges);
        row.setNetPnl(netPnl);
        if (row.getCreatedAt() == 0) row.setCreatedAt(System.currentTimeMillis());
        sessionRepo.save(row);
        log.info("[Straddle] Persisted session row for {}: rolls={} grossPnl={} netPnl={}",
            date, rollCount, grossPnl, netPnl);
    }

    /** Index-options charge constants (NIFTY weekly, as of FY 2025-26).
     *  STT on sell-side premium (hiked Oct 2024 budget), exchange txn on premium both sides,
     *  GST 18% on (brokerage + exchange), SEBI ₹10/crore, stamp duty buy-side only. */
    private static final double STT_SELL_PCT      = 0.000625;   // 0.0625%
    private static final double EXCH_TXN_PCT      = 0.0003503;  // 0.03503% NSE F&O options
    private static final double GST_PCT           = 0.18;       // 18% on brokerage + exchange
    private static final double SEBI_PER_CRORE    = 10.0;       // ₹10 / ₹1cr turnover
    private static final double STAMP_BUY_PCT     = 0.00003;    // 0.003% buy-side

    /** Proper end-of-day charge computation. Brokerage is per-order from settings; the rest are
     *  statutory and scale with premium turnover that {@code doInitialEntry}/{@code closeBothLegs}
     *  accumulate live. Returned breakdown is also surfaced to the dashboard via
     *  {@link #getChargesBreakdown()}. */
    private double estimateDailyCharges() {
        return computeChargesBreakdown().get("total");
    }

    /** Compute the full charge breakdown using accumulated premium turnover. Each cycle (entry +
     *  close) produces 4 orders: 2 sell (entry) + 2 buy (close). Sell-side premium turnover feeds
     *  STT + half the SEBI/exchange; buy-side feeds the other half + stamp duty.
     *
     *  <p>When legs are currently OPEN, the close-side hasn't fired yet — so the realised buy-side
     *  turnover is 0 and the realised order count omits the impending close orders. We PROJECT
     *  those using each open leg's current LTP × qty and add a +1 order per open leg, so the
     *  Hero's Net Day P&L reflects the true take-home if you flat-closed right now. */
    private java.util.Map<String, Double> computeChargesBreakdown() {
        int projectedOrders = orderCountToday;
        double projectedBuyT = buyPremiumTurnoverToday;
        if (marketDataService != null) {
            if (ceSymbol != null && !ceSymbol.isEmpty() && ceQty > 0) {
                double ltp = marketDataService.getLtp(ceSymbol);
                if (ltp > 0) projectedBuyT += ltp * ceQty;
                projectedOrders++;
            }
            if (peSymbol != null && !peSymbol.isEmpty() && peQty > 0) {
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

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    /** Public accessor for dashboard / history endpoints. */
    public java.util.Map<String, Double> getChargesBreakdown() { return computeChargesBreakdown(); }

    /** Charges for a single straddle cycle (one entry-close pair). Same formula as the session
     *  breakdown but applied to this cycle's premium turnover and order count. */
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

    /** Persist one straddle_trades row capturing this cycle's gross P&L, allocated charges,
     *  and close reason. Best-effort — failure to write the row never blocks the close path. */
    private void persistStraddleTrade(int qty, double grossPnl, double charges, String reason) {
        if (tradeRepo == null) return;
        try {
            com.rydytrader.autotrader.entity.StraddleTradeEntity t =
                new com.rydytrader.autotrader.entity.StraddleTradeEntity();
            t.setStrategyId(STRATEGY_ID);
            t.setSessionDate(dayKey != null && !dayKey.isEmpty() ? dayKey : LocalDate.now(IST).toString());
            t.setClosedAtMillis(System.currentTimeMillis());
            t.setQty(qty);
            t.setGrossPnl(round2(grossPnl));
            t.setCharges(round2(charges));
            t.setNetPnl(round2(grossPnl - charges));
            t.setCloseReason(reason);
            tradeRepo.save(t);
        } catch (Exception e) {
            log.warn("[Straddle] Failed to persist straddle_trades row: {}", e.getMessage());
        }
    }

    private void pushRollEvent(String evt, double nifty, String ce, String pe, double pnl) {
        String ts = LocalTime.now(IST).format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        recentRolls.addFirst(new RollEvent(ts, evt, nifty, ce, pe, pnl));
        while (recentRolls.size() > 20) recentRolls.removeLast();
    }

    /** If today's net P&L (realised + open MTM − projected charges) is worse than the configured
     *  daily max-loss, flatten both legs and park DONE_FOR_DAY. Returns true when triggered.
     *  Same Net definition the Hero shows, so the kill point matches what the operator sees. */
    private boolean checkMaxLossKillSwitch() {
        // Derived from portfolioMaxDailyLoss × this strategy's allocation %.
        // Disabled when Portfolio Max Daily Risk is 0.
        double maxLoss = riskSettings.getStrategyMaxDailyLoss(STRATEGY_ID);
        if (maxLoss <= 0) return false; // disabled
        double ceLtp = ceSymbol != null && !ceSymbol.isEmpty() ? marketDataService.getLtp(ceSymbol) : 0;
        double peLtp = peSymbol != null && !peSymbol.isEmpty() ? marketDataService.getLtp(peSymbol) : 0;
        double ceMtm = (ceEntryPremium > 0 && ceLtp > 0 && ceQty > 0) ? (ceEntryPremium - ceLtp) * ceQty : 0;
        double peMtm = (peEntryPremium > 0 && peLtp > 0 && peQty > 0) ? (peEntryPremium - peLtp) * peQty : 0;
        double charges = computeChargesBreakdown().get("total");
        double netPnl = realisedPnlToday + ceMtm + peMtm - charges;
        if (netPnl < -maxLoss) {
            String msg = String.format("Daily max-loss hit (net %.2f < -%.2f) — flattening and parking DONE_FOR_DAY",
                netPnl, maxLoss);
            log.warn("[Straddle] {}", msg);
            eventService.log("[ERROR] " + msg);
            notifyTelegram(msg);
            closeBothLegs("MAX_LOSS_HIT");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return true;
        }
        return false;
    }

    // ── Combined-premium SL trigger / squareoff while OPEN ────────────────────
    /** Fires when the live (CE + PE LTP) has risen by {@code straddleCombinedSlPct}% above the
     *  entry combined premium. On hit:
     *    1. Close both legs immediately
     *    2. Park in WAITING_TO_ROLL for {@code straddleRollWaitMin} minutes
     *    3. If we're past {@code straddleRollCutoffTime} when the wait expires, park DONE_FOR_DAY
     *       and don't re-enter. Otherwise call doInitialEntry() at a fresh ATM. */
    private void checkCombinedSlOrSquareoff(LocalTime now, LocalTime squareOffTime) {
        if (afterOrAt(now, squareOffTime)) {
            closeBothLegs("TIMED_SQUAREOFF");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        // Daily max-loss kill switch — checked before SL trigger so a runaway loss never gets
        // a chance to roll into a fresh straddle.
        if (checkMaxLossKillSwitch()) return;

        if (ceEntryPremium <= 0 || peEntryPremium <= 0
            || ceSymbol == null || ceSymbol.isEmpty()
            || peSymbol == null || peSymbol.isEmpty()) return;

        double ceLtp = marketDataService.getLtp(ceSymbol);
        double peLtp = marketDataService.getLtp(peSymbol);
        if (ceLtp <= 0 || peLtp <= 0) return;

        double entryCombined = ceEntryPremium + peEntryPremium;
        double liveCombined  = ceLtp + peLtp;

        // Combined-premium TARGET check (profit booking). Triggered when live combined has
        // fallen below entry by the configured target %. On hit: close legs, park
        // DONE_FOR_DAY (no new straddles same day).
        double targetPct = riskSettings.getStraddleCombinedTargetPct();
        if (targetPct > 0) {
            double targetTrigger = entryCombined * (1.0 - targetPct / 100.0);
            if (liveCombined <= targetTrigger) {
                double profitPct = ((entryCombined - liveCombined) / entryCombined) * 100.0;
                String msg = String.format("Combined premium TARGET hit — entry %.2f, live %.2f (-%.2f%%, threshold %.2f%%). Booking profit, parking DONE_FOR_DAY.",
                    entryCombined, liveCombined, profitPct, targetPct);
                log.info("[Straddle] {}", msg);
                eventService.log("[SUCCESS] " + msg);
                notifyTelegram(msg);
                closeBothLegs("TARGET_HIT");
                transitionTo(LifecycleState.DONE_FOR_DAY);
                return;
            }
        }

        double slPct = riskSettings.getStraddleCombinedSlPct();
        double trigger = entryCombined * (1.0 + slPct / 100.0);
        if (liveCombined < trigger) return;

        double consumedPct = ((liveCombined - entryCombined) / entryCombined) * 100.0;
        int maxRolls = riskSettings.getStraddleMaxRolls();
        if (rollCount + 1 > maxRolls) {
            transitionTo(LifecycleState.MAX_ROLLS_HOLD);
            String msg = "Straddle max rolls reached (" + maxRolls + ") — holding open legs to timed squareoff";
            log.info("[Straddle] {}", msg);
            eventService.log("[INFO] " + msg);
            notifyTelegram(msg);
            return;
        }

        // Pre-check: if the configured wait would push the re-entry past the roll cutoff,
        // skip the wait entirely and park DONE_FOR_DAY immediately. The wait exists to avoid
        // whipsaw re-entry; if we can't actually roll afterward, there's no benefit to sitting
        // in WAITING_TO_ROLL doing nothing for N minutes.
        int waitMin = riskSettings.getStraddleRollWaitMin();
        LocalTime cutoff = parseTime(riskSettings.getStraddleRollCutoffTime(), "14:30");
        LocalTime projectedReentry = now.plusMinutes(waitMin);
        if (afterOrAt(projectedReentry, cutoff)) {
            String parkMsg = String.format("Combined premium SL hit — entry %.2f, live %.2f (+%.2f%%, threshold %.2f%%). Wait of %d min would push re-entry past cutoff %s — parking DONE_FOR_DAY without wait.",
                entryCombined, liveCombined, consumedPct, slPct, waitMin, cutoff);
            log.info("[Straddle] {}", parkMsg);
            eventService.log("[INFO] " + parkMsg);
            notifyTelegram(parkMsg);
            closeBothLegs("SL_HIT");
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }

        String slMsg = String.format("Combined premium SL hit — entry %.2f, live %.2f (+%.2f%%, threshold %.2f%%). Closing legs and waiting %d min before re-entry.",
            entryCombined, liveCombined, consumedPct, slPct, waitMin);
        log.info("[Straddle] {}", slMsg);
        eventService.log("[INFO] " + slMsg);
        notifyTelegram(slMsg);

        closeBothLegs("SL_HIT");
        rollCount++;
        slHitTimeMillis = System.currentTimeMillis();
        transitionTo(LifecycleState.WAITING_TO_ROLL);
    }

    /** Called every tick while WAITING_TO_ROLL. Compares elapsed time since slHitTimeMillis
     *  against the configured wait. Once the wait expires, re-enters at the new ATM — unless
     *  we've crossed the roll cutoff time, in which case we park DONE_FOR_DAY. */
    private void checkRollWait(LocalTime now, LocalTime squareOffTime) {
        if (afterOrAt(now, squareOffTime)) {
            // Past squareoff while waiting — done for day
            transitionTo(LifecycleState.DONE_FOR_DAY);
            return;
        }
        if (slHitTimeMillis <= 0) {
            // Defensive: shouldn't be in WAITING_TO_ROLL without an slHitTimeMillis
            transitionTo(LifecycleState.IDLE);
            return;
        }
        long elapsedMin = (System.currentTimeMillis() - slHitTimeMillis) / 60_000L;
        int waitMin = riskSettings.getStraddleRollWaitMin();
        if (elapsedMin < waitMin) return; // still waiting

        // Wait elapsed — check roll cutoff time before re-entering
        LocalTime cutoff = parseTime(riskSettings.getStraddleRollCutoffTime(), "14:30");
        if (afterOrAt(now, cutoff)) {
            String msg = "Roll wait expired but past cutoff " + cutoff + " — parking DONE_FOR_DAY without re-entry";
            log.info("[Straddle] {}", msg);
            eventService.log("[INFO] " + msg);
            notifyTelegram(msg);
            transitionTo(LifecycleState.DONE_FOR_DAY);
            slHitTimeMillis = 0;
            return;
        }
        // OK to re-enter
        slHitTimeMillis = 0;
        doInitialEntry();
    }

    // ── Close both legs (BUY since we're short) ────────────────────────────────
    private void closeBothLegs(String reason) {
        // Snapshot leg state + compute realised P&L before clearing.
        double ceClose = ceSymbol != null && !ceSymbol.isEmpty() ? marketDataService.getLtp(ceSymbol) : 0;
        double peClose = peSymbol != null && !peSymbol.isEmpty() ? marketDataService.getLtp(peSymbol) : 0;
        int q = ceQty;
        double cePnl = (ceEntryPremium > 0 && ceClose > 0) ? (ceEntryPremium - ceClose) * q : 0;
        double pePnl = (peEntryPremium > 0 && peClose > 0) ? (peEntryPremium - peClose) * q : 0;
        double pairPnl = cePnl + pePnl;
        realisedPnlToday += pairPnl;
        // Per-straddle charges — this cycle alone: 4 orders (entry sells + close buys) +
        // statutory percentages applied to this cycle's premium turnover. Persisted to the
        // straddle_trades row so the Analytics page has exact per-straddle net P&L.
        double cycleSellPrem = (ceEntryPremium * q) + (peEntryPremium * q);
        double cycleBuyPrem  = (ceClose * q) + (peClose * q);
        double cycleCharges  = computeCycleCharges(cycleSellPrem, cycleBuyPrem, 4);
        // Accumulate buy-side premium turnover (we BUY back on close) for session charge calc.
        if (ceClose > 0 && q > 0) buyPremiumTurnoverToday += ceClose * q;
        if (peClose > 0 && q > 0) buyPremiumTurnoverToday += peClose * q;
        String closedCe = ceSymbol;
        String closedPe = peSymbol;
        double niftyAtClose = marketDataService.getLtp(NIFTY_SYMBOL);
        persistStraddleTrade(q, pairPnl, cycleCharges, reason);

        if (ceSymbol != null && !ceSymbol.isEmpty() && ceQty > 0) {
            placeCloseRetry(ceSymbol, ceQty, "CE", reason);
        }
        if (peSymbol != null && !peSymbol.isEmpty() && peQty > 0) {
            placeCloseRetry(peSymbol, peQty, "PE", reason);
        }
        // Unsubscribe the closed legs from the data WS.
        try {
            if (closedCe != null && !closedCe.isEmpty() && closedPe != null && !closedPe.isEmpty()) {
                marketDataService.unsubscribeAdditional(java.util.Arrays.asList(closedCe, closedPe));
            }
        } catch (Exception ignored) {}
        String msg = "Straddle legs closed (" + reason + "): ce=" + ceSymbol + " pe=" + peSymbol
            + " qty=" + ceQty + " roll=" + rollCount + " pnl=" + String.format("%.2f", pairPnl);
        log.info("[Straddle] {}", msg);
        eventService.log("[INFO] " + msg);
        notifyTelegram(msg);
        pushRollEvent("CLOSE_" + reason, niftyAtClose, closedCe, closedPe, pairPnl);
        this.ceSymbol = "";
        this.peSymbol = "";
        this.ceQty = 0;
        this.peQty = 0;
        this.ceOrderId = "";
        this.peOrderId = "";
        this.ceEntryPremium = 0;
        this.peEntryPremium = 0;
        persist();
    }

    /** Submit a BUY close order. One retry after 2s on failure. */
    private void placeCloseRetry(String symbol, int qty, String legTag, String reason) {
        try {
            OrderDTO resp = orderService.placeOrder(symbol, qty, 1, 0);
            orderCountToday++;
            if (resp != null && resp.getId() != null && !resp.getId().isEmpty() && "ok".equals(resp.getStatus())) {
                return;
            }
            log.warn("[Straddle] First close attempt failed for {} {} ({}): {} — retrying in 2s",
                legTag, symbol, reason, resp);
            try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            OrderDTO retry = orderService.placeOrder(symbol, qty, 1, 0);
            orderCountToday++;
            if (retry == null || retry.getId() == null || retry.getId().isEmpty() || !"ok".equals(retry.getStatus())) {
                log.error("[Straddle] CLOSE FAILED for {} {} qty={} ({}): {}",
                    legTag, symbol, qty, reason, retry);
                eventService.log("[ERROR] Straddle CLOSE FAILED for " + legTag + " " + symbol
                    + " qty=" + qty + " — manual intervention required");
            }
        } catch (Exception e) {
            log.error("[Straddle] Exception closing {} {}: {}", legTag, symbol, e.getMessage());
        }
    }

    // ── Symbol resolution from Fyers options chain ─────────────────────────────
    /** @return [ceSymbol, peSymbol] or null if either is missing. */
    private String[] resolveAtmSymbols(long atmStrike) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getOptionChain(NIFTY_SYMBOL, 30, auth);
            if (root == null) return null;
            JsonNode data = root.has("data") ? root.get("data") : null;
            JsonNode chain = data != null && data.has("optionsChain") ? data.get("optionsChain")
                : (root.has("optionsChain") ? root.get("optionsChain") : null);
            if (chain == null || !chain.isArray()) {
                log.warn("[Straddle] Option chain response missing 'optionsChain' array. Root keys: {}",
                    fieldNames(root));
                return null;
            }
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
                log.warn("[Straddle] Could not find both CE and PE for ATM strike {} (ce={}, pe={})",
                    atmStrike, ce, pe);
                return null;
            }
            return new String[]{ ce, pe };
        } catch (Exception e) {
            log.error("[Straddle] Option chain fetch failed: {}", e.getMessage());
            return null;
        }
    }

    /** Best-effort lazy resolution of the current weekly expiry when no straddle is open yet.
     *  One option-chain call, grab the first symbol, parse expiry. Failures are silent — the
     *  dashboard just shows "—" until entry succeeds (which sets it explicitly). */
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

    /** Trading days remaining until expiry, excluding weekends and NSE holidays.
     *  Today is counted as "0" if it's expiry day; "1" if expiry is the next trading session;
     *  and so on. Returns -1 when the expiry string is empty or unparseable. */
    private int tradingDaysToExpiry(String expiryIso) {
        if (expiryIso == null || expiryIso.isEmpty()) return -1;
        try {
            java.time.LocalDate expiry = java.time.LocalDate.parse(expiryIso);
            java.time.LocalDate today = java.time.LocalDate.now(IST);
            if (expiry.isBefore(today)) return -1; // stale cache, won't happen after day rollover
            int count = 0;
            java.time.LocalDate cursor = today.plusDays(1); // exclusive of today
            while (!cursor.isAfter(expiry)) {
                if (marketHolidayService == null || marketHolidayService.isTradingDay(cursor)) {
                    count++;
                }
                cursor = cursor.plusDays(1);
            }
            return count;
        } catch (Exception e) {
            return -1;
        }
    }

    /** Parse the weekly expiry date from a Fyers option symbol.
     *  Format: {@code NSE:NIFTY<YY><M><DD><C|P><STRIKE>}, where M is 1-9 for Jan-Sep,
     *  O for Oct, N for Nov, D for Dec. Example: {@code NSE:NIFTY25527C24350} → 2025-05-27.
     *  Returns empty string on any parse failure (caller treats it as "unknown"). */
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
            return java.time.LocalDate.of(2000 + yr, month, day).toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static String fieldNames(JsonNode node) {
        if (node == null) return "<null>";
        StringBuilder sb = new StringBuilder("[");
        java.util.Iterator<String> it = node.fieldNames();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) sb.append(",");
        }
        return sb.append("]").toString();
    }

    // ── Day rollover ───────────────────────────────────────────────────────────
    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(dayKey)) return;
        // New trading day or first boot
        if (state == LifecycleState.OPEN || state == LifecycleState.MAX_ROLLS_HOLD) {
            log.warn("[Straddle] Stale state {} from {} detected at startup — forcing close before reset",
                state, dayKey);
            eventService.log("[WARNING] Straddle stale " + state + " from " + dayKey
                + " — flattening legs and resetting");
            closeBothLegs("STALE_DAY_RESET");
        }
        // Persist yesterday's session for the history page before resetting state.
        if (dayKey != null && !dayKey.isEmpty() && (rollCount > 0 || realisedPnlToday != 0)) {
            try { persistSessionFor(dayKey); } catch (Exception e) {
                log.warn("[Straddle] Failed to persist session row for {}: {}", dayKey, e.getMessage());
            }
        }
        this.dayKey = today;
        this.rollCount = 0;
        this.lastEntryNifty = 0;
        this.ceSymbol = "";
        this.peSymbol = "";
        this.ceQty = 0;
        this.peQty = 0;
        this.ceOrderId = "";
        this.peOrderId = "";
        this.ceEntryPremium = 0;
        this.peEntryPremium = 0;
        this.realisedPnlToday = 0;
        this.sellPremiumTurnoverToday = 0;
        this.buyPremiumTurnoverToday = 0;
        this.orderCountToday = 0;
        this.currentWeeklyExpiry = "";
        this.slHitTimeMillis = 0;
        this.recentRolls.clear();
        this.combinedPremiumSamples.clear();
        transitionTo(LifecycleState.IDLE);
    }

    private void transitionTo(LifecycleState next) {
        this.state = next;
        persist();
    }

    private void persist() {
        StraddleStateStore.State s = new StraddleStateStore.State();
        s.dayKey = this.dayKey;
        s.state = this.state.name();
        s.lastEntryNifty = this.lastEntryNifty;
        s.ceSymbol = this.ceSymbol;
        s.peSymbol = this.peSymbol;
        s.ceQty = this.ceQty;
        s.peQty = this.peQty;
        s.rollCount = this.rollCount;
        s.ceOrderId = this.ceOrderId;
        s.peOrderId = this.peOrderId;
        s.ceEntryPremium = this.ceEntryPremium;
        s.peEntryPremium = this.peEntryPremium;
        s.realisedPnlToday = this.realisedPnlToday;
        s.sellPremiumTurnoverToday = this.sellPremiumTurnoverToday;
        s.buyPremiumTurnoverToday = this.buyPremiumTurnoverToday;
        s.orderCountToday = this.orderCountToday;
        s.currentWeeklyExpiry = this.currentWeeklyExpiry;
        s.slHitTimeMillis = this.slHitTimeMillis;
        synchronized (combinedPremiumSamples) {
            s.combinedPremiumSamples = new java.util.ArrayList<>(combinedPremiumSamples);
        }
        // Snapshot today's roll events as maps for JSON persistence
        java.util.List<java.util.Map<String, Object>> rolls = new java.util.ArrayList<>();
        for (RollEvent r : recentRolls) {
            java.util.Map<String, Object> m2 = new java.util.LinkedHashMap<>();
            m2.put("time",  r.time());
            m2.put("event", r.event());
            m2.put("nifty", r.nifty());
            m2.put("ce",    r.ce());
            m2.put("pe",    r.pe());
            m2.put("pnl",   r.pnl());
            rolls.add(m2);
        }
        s.recentRolls = rolls;
        stateStore.update(s);
    }

    private void notifyTelegram(String msg) {
        try {
            if (telegramService != null) telegramService.sendMessage("[Straddle] " + msg);
        } catch (Exception ignored) {}
    }

    private static LocalTime parseTime(String hhmm, String fallback) {
        try {
            return LocalTime.parse((hhmm == null || hhmm.isBlank()) ? fallback : hhmm.trim());
        } catch (Exception e) {
            log.warn("[Straddle] Failed to parse time \"{}\" (expected HH:mm with colon) — falling back to {}",
                hhmm, fallback);
            return LocalTime.parse(fallback);
        }
    }

    private static boolean afterOrAt(LocalTime a, LocalTime b) {
        return !a.isBefore(b);
    }

    /**
     * Manual close — flatten both legs at market and park the day. Used by the dashboard
     * "Squareoff Straddle" button. After this returns, state = DONE_FOR_DAY and no further
     * entries fire today (the timed-squareoff path is a no-op when state is already terminal).
     */
    public synchronized boolean forceCloseBothLegs(String reason) {
        if (state != LifecycleState.OPEN && state != LifecycleState.MAX_ROLLS_HOLD) {
            log.info("[Straddle] forceClose ignored — state={}", state);
            return false;
        }
        eventService.log("[INFO] Manual squareoff (" + reason + ") — flattening both legs");
        closeBothLegs(reason);
        transitionTo(LifecycleState.DONE_FOR_DAY);
        return true;
    }

    /** Hard-stop for the day — closes legs if open AND parks DONE_FOR_DAY regardless of
     *  current state. So a strategy in IDLE / WAITING_TO_ROLL also gets parked, preventing
     *  fresh entries or roll re-entries later in the session. */
    @Override
    public synchronized void parkDoneForDay(String reason) {
        if (state == LifecycleState.DONE_FOR_DAY) return; // already parked
        if (state == LifecycleState.OPEN || state == LifecycleState.MAX_ROLLS_HOLD) {
            eventService.log("[INFO] Portfolio kill (" + reason + ") — flattening + parking combined-sl-roll");
            closeBothLegs(reason);
        } else {
            eventService.log("[INFO] Portfolio kill (" + reason + ") — parking combined-sl-roll from state=" + state + " (no open position)");
        }
        // Clear roll-wait so a stale WAITING_TO_ROLL doesn't re-enter on the next tick.
        slHitTimeMillis = 0;
        transitionTo(LifecycleState.DONE_FOR_DAY);
    }

    /**
     * Manual recovery — flips in-memory state back to IDLE for the current day. Useful when
     * a same-day rejection (e.g. lot-size mismatch) parked the service in DONE_FOR_DAY but
     * the operator has fixed the underlying issue and wants to re-arm without restarting.
     * Never invokes order placement directly; the next scheduler tick will re-enter if the
     * configured entry time is satisfied.
     */
    public synchronized void resetToIdle(String reason) {
        log.info("[Straddle] Manual reset from {} → IDLE ({})", state, reason);
        eventService.log("[INFO] Straddle state reset to IDLE (" + reason + ")");
        this.lastEntryNifty = 0;
        this.ceSymbol = "";
        this.peSymbol = "";
        this.ceQty = 0;
        this.peQty = 0;
        this.ceOrderId = "";
        this.peOrderId = "";
        this.rollCount = 0;
        transitionTo(LifecycleState.IDLE);
    }

    /** Consolidated payload for the Home dashboard — state + leg legs + MTM + day stats +
     *  recent rolls + trigger band. Polled by the UI every 5 seconds. */
    public java.util.Map<String, Object> getDashboard() {
        // Roll over the day if the scheduler hasn't done it yet (e.g. user opens the dashboard
        // before the first tick of the trading day fires). Idempotent.
        rolloverIfNewDay();
        // Lazy-resolve current weekly expiry when we don't have it yet (IDLE state, no entry yet
        // today). Cheap: same option-chain call we'd make on entry; cached for the rest of the day.
        if (currentWeeklyExpiry == null || currentWeeklyExpiry.isEmpty()) {
            tryResolveWeeklyExpiry();
        }
        java.util.Map<String, Object> m = getStatus();
        m.put("dashboardShape", "combined-sl-roll");
        m.put("weeklyExpiry", currentWeeklyExpiry);
        m.put("daysToExpiry", tradingDaysToExpiry(currentWeeklyExpiry));
        synchronized (combinedPremiumSamples) {
            m.put("combinedPremiumSamples", new java.util.ArrayList<>(combinedPremiumSamples));
        }
        // NIFTY + VIX change + change% for the Hero stat. Uses display getters so pre-market shows
        // the last known value + prev-close-relative change (matches the ticker bar behaviour).
        if (marketDataService != null) {
            m.put("niftyDisplayLtp", Math.round(marketDataService.getDisplayLtp(NIFTY_SYMBOL) * 100.0) / 100.0);
            m.put("niftyChange",     Math.round(marketDataService.getDisplayChange(NIFTY_SYMBOL) * 100.0) / 100.0);
            m.put("niftyChangePct",  Math.round(marketDataService.getDisplayChangePct(NIFTY_SYMBOL) * 100.0) / 100.0);
            String vixSymbol = "NSE:INDIAVIX-INDEX";
            m.put("vixDisplayLtp",   Math.round(marketDataService.getDisplayLtp(vixSymbol) * 100.0) / 100.0);
            m.put("vixChange",       Math.round(marketDataService.getDisplayChange(vixSymbol) * 100.0) / 100.0);
            m.put("vixChangePct",    Math.round(marketDataService.getDisplayChangePct(vixSymbol) * 100.0) / 100.0);
            // Per-leg change + change% (vs prev close) for the leg cards
            if (ceSymbol != null && !ceSymbol.isEmpty()) {
                m.put("ceChange",     Math.round(marketDataService.getDisplayChange(ceSymbol) * 100.0) / 100.0);
                m.put("ceChangePct",  Math.round(marketDataService.getDisplayChangePct(ceSymbol) * 100.0) / 100.0);
            }
            if (peSymbol != null && !peSymbol.isEmpty()) {
                m.put("peChange",     Math.round(marketDataService.getDisplayChange(peSymbol) * 100.0) / 100.0);
                m.put("peChangePct",  Math.round(marketDataService.getDisplayChangePct(peSymbol) * 100.0) / 100.0);
            }
        }
        // Current NIFTY
        double niftyLtp = marketDataService != null ? marketDataService.getLtp(NIFTY_SYMBOL) : 0;
        m.put("niftyLtp", niftyLtp);
        // Live leg MTM
        double ceLtp = ceSymbol != null && !ceSymbol.isEmpty() ? marketDataService.getLtp(ceSymbol) : 0;
        double peLtp = peSymbol != null && !peSymbol.isEmpty() ? marketDataService.getLtp(peSymbol) : 0;
        m.put("ceLtp", ceLtp);
        m.put("peLtp", peLtp);
        m.put("ceEntryPremium", ceEntryPremium);
        m.put("peEntryPremium", peEntryPremium);
        // Short positions: P&L = (entryPremium - currentLtp) × qty
        double ceMtm = (ceEntryPremium > 0 && ceLtp > 0 && ceQty > 0) ? (ceEntryPremium - ceLtp) * ceQty : 0;
        double peMtm = (peEntryPremium > 0 && peLtp > 0 && peQty > 0) ? (peEntryPremium - peLtp) * peQty : 0;
        m.put("ceMtm", Math.round(ceMtm * 100.0) / 100.0);
        m.put("peMtm", Math.round(peMtm * 100.0) / 100.0);
        m.put("combinedMtm", Math.round((ceMtm + peMtm) * 100.0) / 100.0);
        m.put("realisedPnlToday", Math.round(realisedPnlToday * 100.0) / 100.0);
        m.put("totalPnlToday", Math.round((realisedPnlToday + ceMtm + peMtm) * 100.0) / 100.0);
        // Risk Band — combined-premium SL tracking
        double entryCombined = ceEntryPremium + peEntryPremium;
        double liveCombined  = (ceLtp > 0 ? ceLtp : 0) + (peLtp > 0 ? peLtp : 0);
        double combinedSlPct = riskSettings.getStraddleCombinedSlPct();
        m.put("entryCombined", round2(entryCombined));
        m.put("liveCombined",  round2(liveCombined));
        m.put("combinedSlPct", combinedSlPct);
        // Worst-case loss for the currently-running straddle cycle, if the combined SL fires
        // exactly at threshold. = entryCombined × slPct/100 × qty. 0 when no straddle is open.
        double maxLossPerStraddle = 0;
        if (entryCombined > 0 && combinedSlPct > 0 && ceQty > 0) {
            maxLossPerStraddle = entryCombined * (combinedSlPct / 100.0) * ceQty;
        }
        m.put("maxLossPerStraddle", round2(maxLossPerStraddle));
        if (entryCombined > 0) {
            double slTrigger = entryCombined * (1.0 + combinedSlPct / 100.0);
            m.put("slTrigger", round2(slTrigger));
            // % of way to SL: 0% = at entry, 100% = at trigger.
            double moveFromEntry = liveCombined - entryCombined;
            double allowedMove   = slTrigger - entryCombined;
            double slConsumedPct = (allowedMove > 0) ? (moveFromEntry / allowedMove) * 100.0 : 0;
            m.put("slConsumedPct", round2(slConsumedPct));
        } else {
            m.put("slTrigger", 0.0);
            m.put("slConsumedPct", 0.0);
        }
        // Combined-premium TARGET tracking
        double combinedTargetPct = riskSettings.getStraddleCombinedTargetPct();
        m.put("combinedTargetPct", combinedTargetPct);
        if (entryCombined > 0 && combinedTargetPct > 0) {
            double targetTrigger = entryCombined * (1.0 - combinedTargetPct / 100.0);
            m.put("targetTrigger", round2(targetTrigger));
        } else {
            m.put("targetTrigger", 0.0);
        }
        // Roll wait countdown — only meaningful when WAITING_TO_ROLL
        if (state == LifecycleState.WAITING_TO_ROLL && slHitTimeMillis > 0) {
            int waitMin = riskSettings.getStraddleRollWaitMin();
            long elapsedSec = (System.currentTimeMillis() - slHitTimeMillis) / 1000L;
            long remainingSec = Math.max(0, waitMin * 60L - elapsedSec);
            m.put("rollWaitRemainingSec", remainingSec);
        }
        m.put("rollCutoffTime", riskSettings.getStraddleRollCutoffTime());
        // Live charges estimate + breakdown for the dashboard.
        java.util.Map<String, Double> charges = computeChargesBreakdown();
        m.put("charges", charges);
        m.put("netPnlToday", round2(realisedPnlToday - charges.get("total")));
        // Recent rolls
        java.util.List<java.util.Map<String, Object>> rolls = new java.util.ArrayList<>();
        for (RollEvent r : recentRolls) {
            java.util.Map<String, Object> rm = new java.util.LinkedHashMap<>();
            rm.put("time", r.time());
            rm.put("event", r.event());
            rm.put("nifty", r.nifty());
            rm.put("ce", r.ce());
            rm.put("pe", r.pe());
            rm.put("pnl", Math.round(r.pnl() * 100.0) / 100.0);
            rolls.add(rm);
        }
        m.put("recentRolls", rolls);
        return m;
    }

    // ── Status for UI ──────────────────────────────────────────────────────────
    public java.util.Map<String, Object> getStatus() {
        java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("state",           state.name());
        m.put("dayKey",          dayKey);
        m.put("lastEntryNifty",  lastEntryNifty);
        m.put("ceSymbol",        ceSymbol);
        m.put("peSymbol",        peSymbol);
        m.put("ceQty",           ceQty);
        m.put("peQty",           peQty);
        m.put("rollCount",       rollCount);
        m.put("maxRolls",        riskSettings.getStraddleMaxRolls());
        m.put("maxDailyLoss",    riskSettings.getStrategyMaxDailyLoss(STRATEGY_ID));
        m.put("ceOrderId",       ceOrderId);
        m.put("peOrderId",       peOrderId);
        m.put("entryTime",       riskSettings.getStraddleEntryTime());
        m.put("squareOffTime",   riskSettings.getStraddleSquareOffTime());
        m.put("combinedSlPct",     riskSettings.getStraddleCombinedSlPct());
        m.put("combinedTargetPct", riskSettings.getStraddleCombinedTargetPct());
        m.put("rollWaitMin",       riskSettings.getStraddleRollWaitMin());
        m.put("rollCutoffTime",    riskSettings.getStraddleRollCutoffTime());
        m.put("lotsPerLeg",      riskSettings.getStraddleLotsPerLeg());
        m.put("lotSize",         NIFTY_LOT_SIZE);
        return m;
    }
}
