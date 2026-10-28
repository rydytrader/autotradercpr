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
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.strategy.ShortStrangleStateStore;
import com.rydytrader.autotrader.store.strategy.ShortStrangleStateStore.Leg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manual multi-strike position manager on weekly index options (NIFTY / SENSEX).
 *
 * <p>There are no instance settings. The operator opens every leg from the dashboard and each
 * open dialog collects everything the leg needs: strike, Fyers product (INTRADAY / MARGIN), lots
 * and, for short legs, the SL % of entry. The bot then automates only:
 * <ul>
 *   <li>per-leg SL on short legs (the leg's own %, operator +/− / inline edit, the COST button);</li>
 *   <li>day end: INTRADAY legs are closed at the global intraday squareoff time; MARGIN legs are carried
 *       overnight and settled in-memory at the {@link #MARKET_CLOSE_TIME} close on expiry day;</li>
 *   <li>charges, realised / open P&L, per-cycle trade rows and per-day session rows;</li>
 *   <li>broker fill reconciliation over the order WebSocket.</li>
 * </ul>
 * The only instance-level attribute is the underlying (NIFTY / SENSEX), changed from the page
 * header while nothing is open.
 *
 * <p>Lifecycle: {@code ARMED} (no legs today) → {@code OPEN} (any leg open) → {@code DONE_FOR_DAY}
 * (every leg closed). Opening a leg from {@code DONE_FOR_DAY} starts a new cycle on the same day.
 */
public class ShortStrangle implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(ShortStrangle.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private static final double STT_SELL_PCT_FALLBACK = 0.0015;
    private static final double EXCH_TXN_PCT_FALLBACK = 0.0003553;
    private static final double STAMP_BUY_PCT_FALLBACK = 0.00003;
    private static final double GST_PCT        = 0.18;
    private static final double SEBI_PER_CRORE = 10.0;

    /** NSE/BSE F&O close — options expire at this wall-clock time on expiry day. */
    private static final LocalTime MARKET_CLOSE_TIME = LocalTime.of(15, 40);
    /** Fallback bot squareoff for INTRADAY (MIS) legs when the global setting is blank / invalid. */
    private static final LocalTime INTRADAY_SQUAREOFF_DEFAULT = LocalTime.of(15, 20);
    /** Default pick for the hedge dialog: the strike nearest this premium. */
    private static final double HEDGE_DEFAULT_PREMIUM = 10;
    private static final int MAX_SAMPLES_PER_LEG = 500;
    private static final String PRODUCT_INTRADAY = "INTRADAY";
    private static final String PRODUCT_MARGIN   = "MARGIN";

    public enum LifecycleState { ARMED, OPEN, DONE_FOR_DAY }

    // ── Instance identity ─────────────────────────────────────────────────────
    private final String instanceId;
    private volatile String displayName;
    private volatile String description;
    private volatile String shortCode;

    // ── Dependencies ──────────────────────────────────────────────────────────
    private final RiskSettingsStore riskSettings;
    private final ShortStrangleStateStore stateStore;
    private final EventService eventService;
    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final FyersClientRouter fyersClient;
    private final MarketDataService marketDataService;
    private final OrderService orderService;
    private final MarketHolidayService marketHolidayService;
    private final com.rydytrader.autotrader.repository.StrategySessionRepository sessionRepo;
    private final com.rydytrader.autotrader.repository.StrategyTradeRepository tradeRepo;
    private final OrderEventService orderEventService;
    private final BalancedAtmSelector atmSelector;

    // ── In-memory state ───────────────────────────────────────────────────────
    private volatile LifecycleState state = LifecycleState.ARMED;
    private volatile String dayKey = "";
    private final List<Leg> legs = new CopyOnWriteArrayList<>();
    private volatile int legSeq = 0;
    private volatile double realisedPnlToday = 0;
    private volatile double sellPremiumTurnoverToday = 0;
    private volatile double buyPremiumTurnoverToday  = 0;
    private volatile int    orderCountToday = 0;
    private volatile int    slHitsToday = 0;
    private volatile double cycleStartRealisedPnl   = 0;
    private volatile double cycleStartSellTurnover  = 0;
    private volatile double cycleStartBuyTurnover   = 0;
    private volatile int    cycleStartOrderCount    = 0;
    private volatile int    cycleStartSlHits        = 0;
    private volatile double consumedRiskToday       = 0;
    private volatile String currentWeeklyExpiry = "";
    private volatile double lastEntryNifty = 0;

    private final java.util.Deque<CycleEvent> recentEvents = new java.util.ArrayDeque<>();

    public static record CycleEvent(String time, String event, double nifty,
                                    String ce, String pe, double pnl) {}

    private enum PendingType { ENTRY, CLOSE, ADD, PART }
    private static record PendingFill(String legId, PendingType type, int qty, double entryRef,
                                       double provisionalPnl, double provisionalTurnover, int prevQty) {}
    private final java.util.concurrent.ConcurrentMap<String, PendingFill> pendingFills =
        new java.util.concurrent.ConcurrentHashMap<>();
    private volatile boolean fillListenerWired = false;
    private final AtomicBoolean expiryRefreshInFlight = new AtomicBoolean(false);
    // Overview tiles: payoff maths over the open legs + Fyers span margin (cached, refreshed async).
    private final PayoffService payoff = new PayoffService();
    private static final long MARGIN_CACHE_MS = 120_000L;
    private volatile double cachedMargin = 0;
    private volatile String cachedMarginKey = "";
    private volatile long cachedMarginAt = 0;
    private final AtomicBoolean marginInFlight = new AtomicBoolean(false);

    public ShortStrangle(StrategyInstanceEntity entity,
                         RiskSettingsStore riskSettings,
                         ShortStrangleStateStore stateStore,
                         EventService eventService,
                         TokenStore tokenStore,
                         FyersProperties fyersProperties,
                         FyersClientRouter fyersClient,
                         MarketDataService marketDataService,
                         OrderService orderService,
                         MarketHolidayService marketHolidayService,
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
        this.sessionRepo = sessionRepo;
        this.tradeRepo = tradeRepo;
        this.orderEventService = orderEventService;
        this.atmSelector = atmSelector;
    }

    protected String tag() { return "STRADDLE".equals(strategyType()) ? "short-straddle" : "short-strangle"; }
    private void info(String msg)  { log.info("[{}] {}", tag(), msg);  eventService.log("[INFO] [" + tag() + "] " + msg); }
    private void warn(String msg)  { log.warn("[{}] {}", tag(), msg);  eventService.log("[WARNING] [" + tag() + "] " + msg); }
    private void error(String msg) { log.error("[{}] {}", tag(), msg); eventService.log("[ERROR] [" + tag() + "] " + msg); }

    public void bootstrap() {
        init();
        if (!fillListenerWired) {
            orderEventService.addFillListener(this::onActualFill);
            fillListenerWired = true;
        }
    }

    public void syncFromEntity(StrategyInstanceEntity entity) {
        this.displayName = entity.getName();
        this.description = entity.getDescription();
        this.shortCode   = entity.getShortCode();
    }

    // ── Fill reconciliation (order WS thread) ─────────────────────────────────
    private synchronized void onActualFill(String orderId, double price) {
        PendingFill p = pendingFills.remove(orderId);
        if (p == null || price <= 0) return;
        Leg leg = findLeg(p.legId());
        if (leg == null) return;
        boolean shortLeg = isShort(leg);
        String label = legLabel(leg);
        switch (p.type()) {
            case ENTRY -> {
                leg.entryPremium = price;
                if (shortLeg) sellPremiumTurnoverToday += price * p.qty();
                else          buyPremiumTurnoverToday  += price * p.qty() - p.provisionalTurnover();
                info(label + " entry filled @ " + String.format("%.2f", price));
            }
            case ADD -> {
                int newQty = p.prevQty() + p.qty();
                double avg = newQty > 0 ? (p.entryRef() * p.prevQty() + price * p.qty()) / newQty : price;
                leg.entryPremium = avg;
                if (shortLeg) sellPremiumTurnoverToday += price * p.qty();
                else          buyPremiumTurnoverToday  += price * p.qty() - p.provisionalTurnover();
                info(label + " add-lot filled @ " + String.format("%.2f", price)
                    + " → avg entry " + String.format("%.2f", avg) + " qty=" + newQty);
            }
            case CLOSE, PART -> {
                double actualPnl = shortLeg ? (p.entryRef() - price) * p.qty() : (price - p.entryRef()) * p.qty();
                double actualT   = price * p.qty();
                realisedPnlToday  += (actualPnl - p.provisionalPnl());
                consumedRiskToday += (actualPnl - p.provisionalPnl());
                if (shortLeg) buyPremiumTurnoverToday  += (actualT - p.provisionalTurnover());
                else          sellPremiumTurnoverToday += (actualT - p.provisionalTurnover());
                if (p.type() == PendingType.CLOSE) { leg.pnl = actualPnl; leg.closePremium = price; }
                info(label + (p.type() == PendingType.CLOSE ? " close" : " exit-lot") + " filled @ "
                    + String.format("%.2f", price) + " pnl=" + String.format("%.2f", actualPnl)
                    + " (delta " + String.format("%+.2f", actualPnl - p.provisionalPnl()) + ")");
            }
        }
        persist();
    }

    private void registerPendingFill(String orderId, PendingFill fill) {
        if (orderId == null || orderId.isEmpty()) return;
        pendingFills.put(orderId, fill);
        Double already = orderEventService.getFillPrice(orderId);
        if (already != null && already > 0) onActualFill(orderId, already);
    }

    // ── Strategy interface — identity ─────────────────────────────────────────
    @Override public String id()           { return instanceId; }
    @Override public String displayName()  { return displayName; }
    @Override public String description()  { return description; }
    @Override public String shortCode()    { return shortCode; }
    @Override public String currentState() { return state.name(); }
    @Override public String navIcon()      { return shortCode != null && !shortCode.isEmpty() ? shortCode : "∧"; }
    @Override public String strategyType() { return "STRANGLE"; }
    @Override public String underlyingName() { return underlying().name(); }
    @Override public String currentWeeklyExpiry() { return currentWeeklyExpiry; }
    @Override public boolean isEnabled() { return riskSettings.getStrategyBool(instanceId, "enabled", false); }
    @Override public boolean forceClose(String reason) { return forceCloseAll(reason); }

    // ── Leg helpers ───────────────────────────────────────────────────────────
    private static boolean isShort(Leg l) { return "SHORT".equalsIgnoreCase(l.kind); }
    private static boolean isCe(Leg l)    { return "CE".equalsIgnoreCase(l.side); }
    private static boolean isOpen(Leg l)  { return l != null && l.closedAtMillis == 0 && l.qty > 0; }
    private static boolean isIntraday(Leg l) { return !PRODUCT_MARGIN.equalsIgnoreCase(l.product); }
    private static String legLabel(Leg l) { return (isShort(l) ? "SHORT " : "HEDGE ") + l.side + " " + l.symbol; }
    private static String normaliseProduct(String p) {
        if (p == null) return PRODUCT_INTRADAY;
        String u = p.trim().toUpperCase();
        return ("MARGIN".equals(u) || "OVERNIGHT".equals(u) || "NRML".equals(u) || "POSITIONAL".equals(u)) ? PRODUCT_MARGIN : PRODUCT_INTRADAY;
    }

    private Leg findLeg(String legId) {
        if (legId == null) return null;
        for (Leg l : legs) if (legId.equalsIgnoreCase(l.id)) return l;
        return null;
    }
    private List<Leg> openLegs() {
        List<Leg> out = new ArrayList<>();
        for (Leg l : legs) if (isOpen(l)) out.add(l);
        return out;
    }
    private boolean hasOpenLegs() { for (Leg l : legs) if (isOpen(l)) return true; return false; }
    private boolean hasOpenIntraday() { for (Leg l : legs) if (isOpen(l) && isIntraday(l)) return true; return false; }
    private boolean hasOpenOvernight() { for (Leg l : legs) if (isOpen(l) && !isIntraday(l)) return true; return false; }
    private String newLegId() { legSeq++; return "L" + legSeq; }

    /** Open leg of the same kind, contract and product (a repeat pick adds lots to that row). */
    private Leg findOpenLeg(String kind, String symbol, String product) {
        for (Leg l : legs)
            if (isOpen(l) && kind.equalsIgnoreCase(l.kind) && symbol.equals(l.symbol)
                    && normaliseProduct(l.product).equals(product)) return l;
        return null;
    }

    private double ltp(String sym) {
        if (sym == null || sym.isEmpty() || marketDataService == null) return 0;
        return marketDataService.getLtp(sym);
    }
    private double spot() { return ltp(underlying().indexSymbol()); }

    // ── Operator actions ──────────────────────────────────────────────────────
    @Override
    public synchronized String openOneLeg(String side, String symbol, String product, int lots, double slPct) {
        return openLeg(side, "SHORT", symbol, normaliseProduct(product), Math.max(1, lots), Math.max(0, slPct));
    }

    @Override
    public synchronized String openHedgeLeg(String side, String symbol, String product, int lots) {
        return openLeg(side, "HEDGE", symbol, normaliseProduct(product), Math.max(1, lots), 0);
    }

    @Override
    public synchronized boolean closeOneLeg(String legId, String reason) {
        Leg leg = findLeg(legId);
        if (!isOpen(leg)) return false;
        info("Manual close from dashboard — " + legLabel(leg));
        closeLegInternal(leg, reason == null ? "MANUAL" : reason);
        afterLegsChanged();
        return true;
    }

    @Override
    public synchronized String closeHedgeLeg(String legId) {
        Leg leg = findLeg(legId);
        if (!isOpen(leg)) return "NOT_OPEN";
        closeLegInternal(leg, "MANUAL");
        afterLegsChanged();
        return isOpen(leg) ? "REJECTED" : "OK";
    }

    @Override
    public synchronized String reopenLeg(String legId, String product, int lots, double slPct) {
        Leg leg = findLeg(legId);
        if (leg == null) return "NOT_FOUND";
        if (isOpen(leg)) return "ALREADY_OPEN";
        String exp = parseExpiryFromSymbol(leg.symbol, underlying());
        if (!exp.isEmpty()) {
            try { if (LocalDate.parse(exp).isBefore(LocalDate.now(IST))) return "EXPIRED"; } catch (Exception ignored) {}
        }
        int lot = underlying().lotSize();
        int n = lots > 0 ? lots : Math.max(1, lot > 0 ? leg.qty / lot : 1);
        String prod = (product == null || product.trim().isEmpty()) ? normaliseProduct(leg.product) : normaliseProduct(product);
        double sl = isShort(leg) ? (slPct >= 0 ? slPct : Math.max(0, leg.slPct)) : 0;
        info(String.format(java.util.Locale.US, "REOPEN %s — %d lot(s) %s%s", legLabel(leg), n,
            PRODUCT_INTRADAY.equals(prod) ? "MIS" : "NRML", isShort(leg) ? (sl > 0 ? String.format(java.util.Locale.US, " SL %.0f%%", sl) : " no SL") : ""));
        return openLeg(leg.side, leg.kind, leg.symbol, prod, n, sl);
    }

    @Override
    public synchronized String adjustLegSl(String legId, int dir) {
        Leg leg = findLeg(legId);
        if (!isOpen(leg)) return "NOT_OPEN";
        if (!isShort(leg)) return "NO_TRIGGER";
        if (dir == 0) { leg.slManual = 0; persist(); return "OK"; }
        double cur = effectiveLegTrigger(leg);
        if (cur <= 0) return "NO_TRIGGER";
        double next = Math.round((cur + dir * 1.0) * 100.0) / 100.0;
        double l = ltp(leg.symbol);
        if (next <= 0) return "INVALID";
        if (l > 0 && next <= l) return "BELOW_LTP";
        leg.slManual = next;
        persist();
        log.info("[{}] {} SL manually set to {} (was {}, LTP {})", tag(), legLabel(leg),
            String.format("%.2f", next), String.format("%.2f", cur), String.format("%.2f", l));
        return "OK";
    }

    @Override
    public synchronized String setLegSl(String legId, double price) {
        Leg leg = findLeg(legId);
        if (!isOpen(leg)) return "NOT_OPEN";
        if (!isShort(leg)) return "NO_TRIGGER";
        if (Double.isNaN(price) || Double.isInfinite(price) || price < 0) return "INVALID";
        if (price == 0) {
            double had = effectiveLegTrigger(leg);
            leg.slPct = 0; leg.slManual = 0; leg.slMovedToCost = false;
            persist();
            info(String.format(java.util.Locale.US, "%s SL removed (was %.2f) — this leg has no stop", legLabel(leg), had));
            return "CLEARED";
        }
        double next = Math.round(price * 100.0) / 100.0;
        double l = ltp(leg.symbol);
        if (l > 0 && next <= l) return "BELOW_LTP";
        double cur = effectiveLegTrigger(leg);
        leg.slManual = next;
        persist();
        log.info("[{}] {} SL set to {} (was {}, LTP {})", tag(), legLabel(leg),
            String.format("%.2f", next), String.format("%.2f", cur), String.format("%.2f", l));
        return "OK";
    }

    @Override
    public synchronized String toggleLegCost(String legId) {
        Leg leg = findLeg(legId);
        if (!isOpen(leg)) return "NOT_OPEN";
        if (!isShort(leg)) return "NO_TRIGGER";
        if (leg.entryPremium <= 0) return "NO_TRIGGER";
        if (leg.slMovedToCost) {
            leg.slMovedToCost = false;
            persist();
            info(legLabel(leg) + " SL back to its configured trigger (COST off)");
            return "OK";
        }
        double l = ltp(leg.symbol);
        if (l > 0 && l >= leg.entryPremium) return "LTP_ABOVE_ENTRY";
        leg.slMovedToCost = true;
        leg.slManual = 0;
        persist();
        info(String.format(java.util.Locale.US, "%s SL moved to COST (entry %.2f) — closes on any retrace to entry",
            legLabel(leg), leg.entryPremium));
        return "OK";
    }

    @Override
    public synchronized String adjustLegLots(String legId, int dir) {
        Leg leg = findLeg(legId);
        if (!isOpen(leg)) return "NOT_OPEN";
        if (dir == 0) return "INVALID";
        if (isIntraday(leg) && dir > 0 && !LocalTime.now(IST).isBefore(intradaySquareoff())) return "PAST_SQUAREOFF";
        int lot = underlying().lotSize();
        if (dir < 0 && leg.qty <= lot) {
            closeLegInternal(leg, "MANUAL");
            afterLegsChanged();
            return isOpen(leg) ? "REJECTED" : "CLOSED";
        }
        String rc = dir > 0 ? addLots(leg, 1) : exitLots(leg, 1);
        persist();
        return rc;
    }

    private String addLots(Leg leg, int lots) {
        boolean shortLeg = isShort(leg);
        int lot = underlying().lotSize();
        int addQty = lots * lot;
        double l = ltp(leg.symbol);
        OrderDTO resp = orderService.placeOrder(leg.symbol, addQty, shortLeg ? -1 : 1, 0, normaliseProduct(leg.product));
        orderCountToday++;
        if (!accepted(resp)) {
            error(legLabel(leg) + " add-lot order rejected: " + resp);
            return "REJECTED";
        }
        int prevQty = leg.qty;
        double entryBefore = leg.entryPremium;
        int newQty = prevQty + addQty;
        double avg = (entryBefore > 0 && l > 0) ? (entryBefore * prevQty + l * addQty) / newQty : (l > 0 ? l : entryBefore);
        leg.qty = newQty;
        leg.entryPremium = avg;
        leg.orderId = resp.getId();
        double provT = 0;
        if (!shortLeg && l > 0) { provT = l * addQty; buyPremiumTurnoverToday += provT; }
        registerPendingFill(resp.getId(), new PendingFill(leg.id, PendingType.ADD, addQty, entryBefore, 0, provT, prevQty));
        info(String.format(java.util.Locale.US, "%s +%d lot %s @ ~%.2f → qty %d, avg entry %.2f",
            legLabel(leg), lots, shortLeg ? "SOLD" : "BOUGHT", l, newQty, avg));
        pushEvent("ADD_LOT_" + leg.side, spot(), isCe(leg) ? leg.symbol : "", isCe(leg) ? "" : leg.symbol, 0);
        return "OK";
    }

    private String exitLots(Leg leg, int lots) {
        boolean shortLeg = isShort(leg);
        int lot = underlying().lotSize();
        int exitQty = lots * lot;
        double l = ltp(leg.symbol);
        OrderDTO resp = orderService.placeOrder(leg.symbol, exitQty, shortLeg ? 1 : -1, 0, normaliseProduct(leg.product));
        orderCountToday++;
        if (!accepted(resp)) {
            error(legLabel(leg) + " exit-lot order rejected: " + resp);
            return "REJECTED";
        }
        double pnl = (leg.entryPremium > 0 && l > 0)
            ? (shortLeg ? (leg.entryPremium - l) * exitQty : (l - leg.entryPremium) * exitQty) : 0;
        double t = l > 0 ? l * exitQty : 0;
        realisedPnlToday  += pnl;
        consumedRiskToday += pnl;
        if (shortLeg) buyPremiumTurnoverToday += t; else sellPremiumTurnoverToday += t;
        leg.qty -= exitQty;
        registerPendingFill(resp.getId(), new PendingFill(leg.id, PendingType.PART, exitQty, leg.entryPremium, pnl, t, 0));
        info(String.format(java.util.Locale.US, "%s -%d lot %s @ ~%.2f pnl=%.2f → qty %d",
            legLabel(leg), lots, shortLeg ? "BOUGHT BACK" : "SOLD", l, pnl, leg.qty));
        pushEvent("EXIT_LOT_" + leg.side, spot(), isCe(leg) ? leg.symbol : "", isCe(leg) ? "" : leg.symbol, pnl);
        return "OK";
    }

    /** Both sides of each strike, ATM ± 10 steps, for the straddle / strangle dialogs. */
    @Override
    public Map<String, Object> pairCandidates() { return pairCandidates(null); }

    @Override
    public Map<String, Object> pairCandidates(String expiry) { return pairCandidates(expiry, 10); }

    private Map<String, Object> pairCandidates(String expiry, int steps) {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Map<String, Object>> expiries = expiryOptions();
        out.put("expiries", expiries);
        double sp = spot();
        if (sp <= 0) { out.put("rows", List.of()); return out; }
        long step = underlying().strikeStep();
        long atm = Math.round(sp / step) * step;
        long ts = resolveExpiryTs(expiry, expiries);
        List<Map<String, Object>> rows = new ArrayList<>();
        String firstSym = null;
        for (Map<String, Object> row : atmSelector.chainWindow(underlying(), atm - steps * step, atm + steps * step, ts)) {
            String ceSym = String.valueOf(row.get("ceSymbol"));
            String peSym = String.valueOf(row.get("peSymbol"));
            if (ceSym == null || ceSym.isEmpty() || "null".equals(ceSym) || peSym == null || peSym.isEmpty() || "null".equals(peSym)) continue;
            long strike = ((Number) row.get("strike")).longValue();
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("strike", strike);
            r.put("ceSymbol", ceSym);
            r.put("ceLtp", round2(((Number) row.get("ceLtp")).doubleValue()));
            r.put("ceMoneyness", moneynessLabel(ceSym, true));
            r.put("peSymbol", peSym);
            r.put("peLtp", round2(((Number) row.get("peLtp")).doubleValue()));
            r.put("peMoneyness", moneynessLabel(peSym, false));
            r.put("isAtm", strike == atm);
            rows.add(r);
            if (firstSym == null) firstSym = ceSym;
        }
        out.put("expiry", selectedExpiry(expiry, firstSym, expiries));
        out.put("spot", round2(sp));
        out.put("atm", atm);
        out.put("step", step);
        out.put("lotSize", underlying().lotSize());
        out.put("rows", rows);
        return out;
    }

    // ── SIMULATE modal ────────────────────────────────────────────────────────
    @Override
    public Map<String, Object> simContext() {
        Map<String, Object> out = new LinkedHashMap<>();
        double sp = spot();
        int lot = underlying().lotSize();
        double r = com.rydytrader.autotrader.util.BlackScholes.DEFAULT_RISK_FREE_RATE;
        out.put("underlying", underlying().name());
        out.put("spot", round2(sp));
        out.put("lotSize", lot);
        out.put("riskFree", r);
        out.put("expiries", expiryOptions());
        out.put("defaultProduct", defaultProduct());
        out.put("defaultSlPct", defaultSlPct());
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Leg l : openLegs()) {
            boolean ce = isCe(l);
            double lp = ltp(l.symbol);
            double T = yearsToExpiryPrecise(l.symbol);
            long k = parseStrikeFromSymbol(l.symbol, sp);
            double iv = (sp > 0 && lp > 0 && T > 0 && k > 0)
                ? com.rydytrader.autotrader.util.BlackScholes.impliedVol(sp, k, T, r, lp, ce) : 0;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("legId", l.id);
            m.put("tag", "LIVE");
            m.put("side", isShort(l) ? "SELL" : "BUY");
            m.put("type", ce ? "CE" : "PE");
            m.put("strike", k);
            m.put("expiry", parseExpiryFromSymbol(l.symbol, underlying()));
            m.put("lots", Math.max(1, lot > 0 ? l.qty / lot : 1));
            m.put("qty", l.qty);
            m.put("price", round2(l.entryPremium));
            m.put("ltp", round2(lp));
            m.put("iv", round4(iv));
            m.put("symbol", l.symbol);
            m.put("include", true);
            rows.add(m);
        }
        out.put("legs", rows);
        return out;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> simChain(String expiry) {
        Map<String, Object> out = pairCandidates(expiry, 25);
        double sp = spot();
        double r = com.rydytrader.autotrader.util.BlackScholes.DEFAULT_RISK_FREE_RATE;
        Object rowsObj = out.get("rows");
        if (rowsObj instanceof List<?> rows) {
            for (Object o : rows) {
                if (!(o instanceof Map)) continue;
                Map<String, Object> row = (Map<String, Object>) o;
                double k = ((Number) row.get("strike")).doubleValue();
                double T = yearsToExpiryPrecise(String.valueOf(row.get("ceSymbol")));
                double ceLtp = ((Number) row.get("ceLtp")).doubleValue();
                double peLtp = ((Number) row.get("peLtp")).doubleValue();
                row.put("ceIv", round4(sp > 0 && T > 0 && ceLtp > 0 ? com.rydytrader.autotrader.util.BlackScholes.impliedVol(sp, k, T, r, ceLtp, true) : 0));
                row.put("peIv", round4(sp > 0 && T > 0 && peLtp > 0 ? com.rydytrader.autotrader.util.BlackScholes.impliedVol(sp, k, T, r, peLtp, false) : 0));
            }
        }
        return out;
    }

    /** Open legs as simulator legs: SHORT → SELL, HEDGE → BUY, price = entry, iv implied from LTP. */
    private List<PayoffService.SimLeg> openLegsAsSim(double sp) {
        List<PayoffService.SimLeg> out = new ArrayList<>();
        int lot = underlying().lotSize();
        double r = com.rydytrader.autotrader.util.BlackScholes.DEFAULT_RISK_FREE_RATE;
        for (Leg l : openLegs()) {
            boolean ce = isCe(l);
            double lp = ltp(l.symbol);
            double T = yearsToExpiryPrecise(l.symbol);
            long k = parseStrikeFromSymbol(l.symbol, sp);
            PayoffService.SimLeg sl = new PayoffService.SimLeg();
            sl.side = isShort(l) ? "SELL" : "BUY";
            sl.type = ce ? "CE" : "PE";
            sl.strike = k;
            sl.expiry = parseExpiryFromSymbol(l.symbol, underlying());
            sl.lots = Math.max(1, lot > 0 ? l.qty / lot : 1);
            sl.price = l.entryPremium;
            sl.iv = (sp > 0 && lp > 0 && T > 0 && k > 0) ? com.rydytrader.autotrader.util.BlackScholes.impliedVol(sp, k, T, r, lp, ce) : 0;
            sl.symbol = l.symbol; sl.legId = l.id; sl.tag = "LIVE"; sl.include = true;
            out.add(sl);
        }
        return out;
    }

    /** Max profit / max loss / POP / breakevens of the open book for the Overview tiles. */
    private Map<String, Object> openPayoffStats(double sp) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (!hasOpenLegs() || sp <= 0) return out;
        try {
            PayoffService.Request req = new PayoffService.Request();
            req.legs = openLegsAsSim(sp);
            Map<String, Object> res = payoff.compute(req, sp, underlying().lotSize());
            if (Boolean.TRUE.equals(res.get("empty"))) return out;
            for (String k : new String[]{"maxProfit", "maxProfitUnlimited", "maxLoss", "maxLossUnlimited", "pop", "breakevens", "netCredit", "nearestExpiry"}) out.put(k, res.get(k));
        } catch (Exception e) {
            log.debug("[{}] payoff stats failed: {}", tag(), e.getMessage());
        }
        return out;
    }

    /** Fyers span margin for the open book. Cached per leg-set for {@value #MARGIN_CACHE_MS} ms and
     *  refreshed off-thread so the 1 s dashboard poll never waits on the broker. 0 = not known yet. */
    private double currentSpanMargin(double sp) {
        List<Leg> open = openLegs();
        if (open.isEmpty()) { cachedMargin = 0; cachedMarginKey = ""; return 0; }
        StringBuilder kb = new StringBuilder();
        boolean anyOvernight = false;
        for (Leg l : open) { kb.append(l.symbol).append(':').append(l.qty).append(':').append(l.product).append(';'); if (!isIntraday(l)) anyOvernight = true; }
        String key = kb.toString();
        long now = System.currentTimeMillis();
        boolean stale = !key.equals(cachedMarginKey) || now - cachedMarginAt > MARGIN_CACHE_MS;
        if (stale && marginInFlight.compareAndSet(false, true)) {
            final String product = anyOvernight ? PRODUCT_MARGIN : PRODUCT_INTRADAY;
            final List<PayoffService.SimLeg> legsNow = openLegsAsSim(sp);
            CompletableFuture.runAsync(() -> {
                try {
                    Map<String, Object> m = simMargin(legsNow, product);
                    if (m.get("margin") instanceof Number n && n.doubleValue() > 0) {
                        cachedMargin = n.doubleValue(); cachedMarginKey = key; cachedMarginAt = System.currentTimeMillis();
                    } else {
                        cachedMarginAt = System.currentTimeMillis();   // back off; keep the last good value
                        cachedMarginKey = key;
                    }
                } finally { marginInFlight.set(false); }
            });
        }
        return key.equals(cachedMarginKey) ? cachedMargin : 0;
    }

    @Override
    public Map<String, Object> simMargin(List<PayoffService.SimLeg> legs, String product) {
        Map<String, Object> out = new LinkedHashMap<>();
        String prod = normaliseProduct(product == null || product.isEmpty() ? defaultProduct() : product);
        int lot = underlying().lotSize();
        StringBuilder data = new StringBuilder();
        int n = 0;
        for (PayoffService.SimLeg l : legs == null ? List.<PayoffService.SimLeg>of() : legs) {
            if (l == null || !l.include || l.lots <= 0 || l.symbol == null || l.symbol.isEmpty()) continue;
            if (n++ > 0) data.append(',');
            data.append(String.format(java.util.Locale.US,
                "{\"symbol\":\"%s\",\"qty\":%d,\"side\":%d,\"type\":2,\"productType\":\"%s\",\"limitPrice\":0,\"stopLoss\":0}",
                l.symbol, l.lots * lot, "BUY".equalsIgnoreCase(l.side) ? 1 : -1, prod));
        }
        out.put("product", prod);
        out.put("legs", n);
        if (n == 0) { out.put("error", "NO_SYMBOLS"); return out; }
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.spanMargin("{\"data\":[" + data + "]}", auth);
            JsonNode d = root != null && root.has("data") ? root.get("data") : root;
            double margin = 0, avail = 0;
            if (d != null) {
                for (String k : new String[]{"total", "margin_total", "margin_new_order", "total_margin"}) {
                    if (d.has(k) && d.get(k).isNumber()) { margin = d.get(k).asDouble(); break; }
                }
                for (String k : new String[]{"margin_avail", "available", "margin_available"}) {
                    if (d.has(k) && d.get(k).isNumber()) { avail = d.get(k).asDouble(); break; }
                }
            }
            if (margin <= 0) {
                log.warn("[{}] span_margin response had no usable total: {}", tag(), root);
                out.put("error", root != null && root.has("message") ? root.get("message").asText() : "NO_TOTAL");
                return out;
            }
            out.put("margin", round2(margin));
            out.put("available", round2(avail));
        } catch (Exception e) {
            log.warn("[{}] span_margin failed: {}", tag(), e.getMessage());
            out.put("error", String.valueOf(e.getMessage()));
        }
        return out;
    }

    @Override
    public synchronized List<Map<String, Object>> simExecute(List<PayoffService.SimLeg> legs, String product, double slPct) {
        List<Map<String, Object>> out = new ArrayList<>();
        String prod = normaliseProduct(product == null || product.isEmpty() ? defaultProduct() : product);
        double sl = slPct >= 0 ? slPct : defaultSlPct();
        int lot = underlying().lotSize();
        for (PayoffService.SimLeg l : legs == null ? List.<PayoffService.SimLeg>of() : legs) {
            if (l == null || !l.include || l.lots <= 0 || "LIVE".equalsIgnoreCase(l.tag)) continue;
            boolean buy = "BUY".equalsIgnoreCase(l.side);
            String side = "PE".equalsIgnoreCase(l.type) ? "PE" : "CE";
            String label = (buy ? "BUY " : "SELL ") + l.lots + " lot " + side + " " + (long) l.strike + " " + (l.expiry == null ? "" : l.expiry);
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("label", label);
            String code;
            if (l.symbol == null || l.symbol.isEmpty()) code = "NO_SYMBOL";
            else {
                // A BUY against an open short of the same contract is an exit, not a hedge (and vice versa).
                Leg opposite = null;
                for (Leg x : openLegs()) {
                    if (!l.symbol.equalsIgnoreCase(x.symbol)) continue;
                    if ((buy && isShort(x)) || (!buy && !isShort(x))) { opposite = x; break; }
                }
                if (opposite != null) {
                    int openLots = Math.max(1, lot > 0 ? opposite.qty / lot : 1);
                    if (l.lots >= openLots) {
                        info("SIM execute — exit " + legLabel(opposite) + " (" + label + ")");
                        closeLegInternal(opposite, "MANUAL");
                        afterLegsChanged();
                        code = isOpen(opposite) ? "REJECTED" : "CLOSED";
                    } else {
                        info("SIM execute — exit " + l.lots + " lot(s) of " + legLabel(opposite));
                        code = exitLots(opposite, l.lots);
                        persist();
                        if ("OK".equals(code)) code = "REDUCED";
                    }
                } else if (buy) {
                    code = openHedgeLeg(side, l.symbol, prod, l.lots);
                } else {
                    code = openOneLeg(side, l.symbol, prod, l.lots, sl);
                }
            }
            r.put("code", code);
            r.put("ok", "OK".equals(code) || "CLOSED".equals(code) || "REDUCED".equals(code));
            out.add(r);
        }
        return out;
    }

    /** CE then PE under one lock so the two legs land as one action. */
    @Override
    public synchronized Map<String, String> openPair(String ceSymbol, String peSymbol, String product, int lots, double slPct) {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("ce", openLeg("CE", "SHORT", ceSymbol, normaliseProduct(product), Math.max(1, lots), Math.max(0, slPct)));
        out.put("pe", openLeg("PE", "SHORT", peSymbol, normaliseProduct(product), Math.max(1, lots), Math.max(0, slPct)));
        return out;
    }

    /** Strike picker for "+ OPEN CE/PE": ATM ± 10 steps. */
    @Override
    public Map<String, Object> openLegCandidates(String side) { return openLegCandidates(side, null); }

    @Override
    public Map<String, Object> openLegCandidates(String side, String expiry) {
        boolean ce = "CE".equalsIgnoreCase(side);
        double sp = spot();
        String def = sp > 0 ? atmSymbol(ce, sp) : null;
        Map<String, Object> out = candidates(ce, sp, def, 10, expiry, false);
        out.put("lotSize", underlying().lotSize());
        out.put("intradaySquareoff", intradaySquareoff().toString());
        return out;
    }

    /** Strike picker for "+ CE/PE HEDGE": the ≈₹10 pick ± 20 steps. */
    @Override
    public Map<String, Object> hedgeCandidates(String side) { return hedgeCandidates(side, null); }

    @Override
    public Map<String, Object> hedgeCandidates(String side, String expiry) {
        boolean ce = "CE".equalsIgnoreCase(side);
        double sp = spot();
        String def = sp > 0 ? defaultHedgeSymbol(ce, sp) : null;
        Map<String, Object> out = candidates(ce, sp, def, 20, expiry, true);
        out.put("lotSize", underlying().lotSize());
        out.put("intradaySquareoff", intradaySquareoff().toString());
        return out;
    }

    private Map<String, Object> candidates(boolean ce, double sp, String def, int steps, String expiry, boolean hedge) {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Map<String, Object>> expiries = expiryOptions();
        out.put("expiries", expiries);
        if (sp <= 0) { out.put("candidates", List.of()); return out; }
        long step = underlying().strikeStep();
        long atm = Math.round(sp / step) * step;
        long centre = parseStrikeFromSymbol(def, sp);
        if (centre <= 0) centre = atm;
        long ts = resolveExpiryTs(expiry, expiries);
        List<Map<String, Object>> cands = new ArrayList<>();
        for (Map<String, Object> row : atmSelector.chainWindow(underlying(), centre - steps * step, centre + steps * step, ts)) {
            String sym = String.valueOf(ce ? row.get("ceSymbol") : row.get("peSymbol"));
            if (sym == null || sym.isEmpty() || "null".equals(sym)) continue;
            double l = ((Number) (ce ? row.get("ceLtp") : row.get("peLtp"))).doubleValue();
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("symbol", sym);
            c.put("strike", row.get("strike"));
            c.put("ltp", round2(l));
            c.put("moneyness", moneynessLabel(sym, ce));
            c.put("isDefault", ts > 0 ? false : sym.equals(def));
            cands.add(c);
        }
        if (ts > 0 && !cands.isEmpty()) {
            // Another expiry: the near-expiry default symbol doesn't exist here. Short → same strike;
            // hedge → the strike priced nearest the ₹target in this series.
            Map<String, Object> pick = null;
            double best = Double.MAX_VALUE;
            for (Map<String, Object> c : cands) {
                double d = hedge ? Math.abs(((Number) c.get("ltp")).doubleValue() - HEDGE_DEFAULT_PREMIUM)
                                 : Math.abs(((Number) c.get("strike")).longValue() - centre);
                if (hedge && !(((Number) c.get("ltp")).doubleValue() > 0)) continue;
                if (d < best) { best = d; pick = c; }
            }
            if (pick != null) { pick.put("isDefault", true); def = String.valueOf(pick.get("symbol")); }
        }
        out.put("expiry", selectedExpiry(expiry, cands.isEmpty() ? null : String.valueOf(cands.get(0).get("symbol")), expiries));
        out.put("spot", round2(sp));
        out.put("defaultSymbol", def);
        out.put("candidates", cands);
        return out;
    }

    // ── Expiry choices for the open dialogs ───────────────────────────────────
    private static final int EXPIRY_CHOICES_MAX = 10;

    /** Every expiry up to and including the next monthly one — Fyers's own list (holiday-shifted by
     *  the exchange) when reachable, else the weekly schedule shifted back over holidays from the
     *  calendar. When the nearest expiry is itself the monthly, the list runs on to the following
     *  month's. {@code [{date, ts, dte, monthly, label}]}. */
    public List<Map<String, Object>> expiryOptions() {
        LocalDate today = LocalDate.now(IST);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> e : atmSelector.expiryList(underlying())) {
            LocalDate d;
            try { d = LocalDate.parse(String.valueOf(e.get("date"))); } catch (Exception ex) { continue; }
            if (d.isBefore(today)) continue;
            out.add(expiryRow(d, ((Number) e.get("ts")).longValue()));
            if (stopAfter(out)) break;
        }
        if (out.isEmpty()) {
            java.time.DayOfWeek dow = underlying().expiryDayOfWeek();
            LocalDate from = today;
            while (!stopAfter(out)) {
                LocalDate d;
                if (marketHolidayService != null) d = marketHolidayService.weeklyExpiryOnOrAfter(from, dow);
                else { d = from; while (d.getDayOfWeek() != dow) d = d.plusDays(1); }
                out.add(expiryRow(d, d.atStartOfDay(IST).toEpochSecond()));
                from = d.plusDays(1);
            }
        }
        return out;
    }

    /** Stop once a monthly expiry is in the list (unless it is the only entry) or at the cap. */
    private static boolean stopAfter(List<Map<String, Object>> out) {
        if (out.size() >= EXPIRY_CHOICES_MAX) return true;
        return out.size() >= 2 && Boolean.TRUE.equals(out.get(out.size() - 1).get("monthly"));
    }

    private Map<String, Object> expiryRow(LocalDate d, long ts) {
        Map<String, Object> m = new LinkedHashMap<>();
        int dte = tradingDaysToExpiry(d.toString());
        boolean monthly = isMonthlyExpiry(d);
        String dow = d.getDayOfWeek().getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH);
        String mon = d.getMonth().getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH);
        String when = dte == 0 ? "today" : dte == 1 ? "1 day" : dte + " days";
        m.put("date", d.toString());
        m.put("ts", ts);
        m.put("dte", dte);
        m.put("monthly", monthly);
        m.put("label", String.format(java.util.Locale.US, "%s %02d %s · %s%s", dow, d.getDayOfMonth(), mon, when, monthly ? " · MONTHLY" : ""));
        return m;
    }

    /** Last scheduled expiry weekday of the month, shifted back over holidays. */
    private boolean isMonthlyExpiry(LocalDate d) {
        java.time.DayOfWeek dow = underlying().expiryDayOfWeek();
        LocalDate last = d.withDayOfMonth(d.lengthOfMonth());
        while (last.getDayOfWeek() != dow) last = last.minusDays(1);
        if (marketHolidayService != null) while (!marketHolidayService.isTradingDay(last)) last = last.minusDays(1);
        return d.equals(last);
    }

    /** 0 = let the chain serve its current expiry; otherwise the Fyers timestamp for that date. */
    private long resolveExpiryTs(String expiry, List<Map<String, Object>> expiries) {
        if (expiry == null || expiry.trim().isEmpty()) return 0;
        String want = expiry.trim();
        for (Map<String, Object> e : expiries) if (want.equals(e.get("date"))) return ((Number) e.get("ts")).longValue();
        try { return LocalDate.parse(want).atStartOfDay(IST).toEpochSecond(); } catch (Exception ex) { return 0; }
    }

    /** Which expiry the returned rows belong to: the request's, else parsed from a row symbol, else the nearest. */
    private String selectedExpiry(String requested, String anySymbol, List<Map<String, Object>> expiries) {
        if (requested != null && !requested.trim().isEmpty()) return requested.trim();
        String fromSym = anySymbol == null ? "" : parseExpiryFromSymbol(anySymbol, underlying());
        if (!fromSym.isEmpty()) return fromSym;
        return expiries.isEmpty() ? "" : String.valueOf(expiries.get(0).get("date"));
    }

    /** ATM CE / PE symbol for the current spot. */
    protected String atmSymbol(boolean isCe, double sp) {
        long step = underlying().strikeStep();
        long atm = Math.round(sp / step) * step;
        BalancedAtmSelector.StrikeSymbols s = atmSelector.resolveStrikeSymbols(underlying(), atm, atm);
        if (s == null) return null;
        return isCe ? s.ceSymbol() : s.peSymbol();
    }

    private String defaultHedgeSymbol(boolean isCe, double sp) {
        long step = underlying().strikeStep();
        long atm = Math.round(sp / step) * step;
        BalancedAtmSelector.StrikeSymbols wings = atmSelector.resolveStrikeSymbolsByPremium(underlying(), atm, HEDGE_DEFAULT_PREMIUM);
        if (wings == null) return null;
        return isCe ? wings.ceSymbol() : wings.peSymbol();
    }

    // ── Core: open / close ────────────────────────────────────────────────────
    private String openLeg(String side, String kind, String symbol, String product, int lots, double slPct) {
        boolean ce = "CE".equalsIgnoreCase(side);
        if (!ce && !"PE".equalsIgnoreCase(side)) return "INVALID";
        boolean shortLeg = "SHORT".equals(kind);
        boolean intraday = PRODUCT_INTRADAY.equals(product);
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return "MARKET_CLOSED";
        LocalTime nowT = LocalTime.now(IST);
        if (intraday && !nowT.isBefore(intradaySquareoff())) return "PAST_SQUAREOFF";
        if (!nowT.isBefore(MARKET_CLOSE_TIME)) return "MARKET_CLOSED";
        double sp = spot();
        if (sp <= 0) return "ABORTED";
        String sym = symbol;
        if (sym == null || sym.isEmpty()) sym = shortLeg ? atmSymbol(ce, sp) : defaultHedgeSymbol(ce, sp);
        if (sym == null || sym.isEmpty()) {
            warn((shortLeg ? "open " : "hedge ") + side + " aborted — could not resolve a strike");
            return "ABORTED";
        }
        String suffix = ce ? "CE" : "PE";
        if (!sym.toUpperCase().endsWith(suffix)) return "INVALID";
        Leg existing = findOpenLeg(kind, sym, product);
        if (existing != null) {
            String rc = addLots(existing, lots);
            if ("OK".equals(rc) && shortLeg && slPct > 0 && existing.slManual <= 0) existing.slPct = slPct;
            persist();
            return rc;
        }
        int qty = lots * underlying().lotSize();
        OrderDTO resp = orderService.placeOrder(sym, qty, shortLeg ? -1 : 1, 0, product);
        orderCountToday++;
        if (!accepted(resp)) {
            error((shortLeg ? "SHORT " : "HEDGE ") + side + " order rejected: " + sym + " qty=" + qty + " → " + resp);
            return "REJECTED";
        }
        try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(sym)); } catch (Exception ignored) {}
        double entry = readEntryPremium(sym);
        if (!hasOpenLegs()) startCycle();
        Leg leg = new Leg();
        leg.id = newLegId();
        leg.side = suffix;
        leg.kind = kind;
        leg.symbol = sym;
        leg.qty = qty;
        leg.orderId = resp.getId();
        leg.product = product;
        leg.slPct = shortLeg ? slPct : 0;
        leg.entryPremium = entry;
        leg.openedAtMillis = System.currentTimeMillis();
        leg.samples = new ArrayList<>();
        if (entry > 0) leg.samples.add(sample(entry));
        legs.add(leg);
        this.lastEntryNifty = sp;
        double provT = 0;
        if (!shortLeg) { provT = entry * qty; buyPremiumTurnoverToday += provT; }
        registerPendingFill(resp.getId(), new PendingFill(leg.id, PendingType.ENTRY, qty, 0, 0, provT, 0));
        if (currentWeeklyExpiry == null || currentWeeklyExpiry.isEmpty()) {
            String exp = parseExpiryFromSymbol(sym, underlying());
            String nearest = nextExpectedWeeklyExpiry();
            if (!exp.isEmpty() && (nearest.isEmpty() || nearest.equals(exp))) currentWeeklyExpiry = exp;
        }
        long strike = parseStrikeFromSymbol(sym, sp);
        String mny = moneynessLabel(sym, ce);
        info(String.format(java.util.Locale.US, "%s %s %s %d (%s) @ ~%.2f qty=%d %s%s (%s %.2f)",
            shortLeg ? "SHORT" : "HEDGE", side, shortLeg ? "SOLD" : "BOUGHT", strike,
            mny == null ? "?" : mny, entry, qty, intraday ? "MIS" : "NRML",
            shortLeg ? (slPct > 0 ? String.format(java.util.Locale.US, " SL %.0f%%", slPct) : " NO SL") : "",
            underlying().name(), sp));
        pushEvent((shortLeg ? "OPEN_" : "HEDGE_OPEN_") + suffix, sp, ce ? sym : "", ce ? "" : sym, 0);
        transitionTo(LifecycleState.OPEN);
        return "OK";
    }

    private static boolean accepted(OrderDTO resp) {
        return resp != null && resp.getId() != null && !resp.getId().isEmpty() && "ok".equals(resp.getStatus());
    }

    private void startCycle() {
        this.cycleStartRealisedPnl  = realisedPnlToday;
        this.cycleStartSellTurnover = sellPremiumTurnoverToday;
        this.cycleStartBuyTurnover  = buyPremiumTurnoverToday;
        this.cycleStartOrderCount   = orderCountToday;
        this.cycleStartSlHits       = slHitsToday;
    }

    private void closeLegInternal(Leg leg, String reason) {
        if (!isOpen(leg)) return;
        boolean shortLeg = isShort(leg);
        int qty = leg.qty;
        double entry = leg.entryPremium;
        boolean slHit = reason != null && reason.endsWith("SL_HIT");
        if (slHit) slHitsToday++;
        double l = ltp(leg.symbol);
        double sp = spot();
        String closeId = placeOrderRetry(leg.symbol, qty, shortLeg ? 1 : -1, normaliseProduct(leg.product), legLabel(leg), reason);
        double pnl = (entry > 0 && l > 0) ? (shortLeg ? (entry - l) * qty : (l - entry) * qty) : 0;
        double t = l > 0 ? l * qty : 0;
        leg.pnl = pnl;
        leg.closePremium = l;
        if (closeId != null && !closeId.isEmpty()) {
            realisedPnlToday  += pnl;
            consumedRiskToday += pnl;
            if (shortLeg) buyPremiumTurnoverToday += t; else sellPremiumTurnoverToday += t;
            registerPendingFill(closeId, new PendingFill(leg.id, PendingType.CLOSE, qty, entry, pnl, t, 0));
        }
        try { marketDataService.unsubscribeAdditional(java.util.Collections.singletonList(leg.symbol)); } catch (Exception ignored) {}
        leg.closedAtMillis = System.currentTimeMillis();
        leg.closeReason = reason;
        leg.orderId = "";
        leg.slManual = 0;
        pushEvent((shortLeg ? "CLOSE_" : "HEDGE_CLOSE_") + reason, sp, isCe(leg) ? leg.symbol : "", isCe(leg) ? "" : leg.symbol, pnl);
        info(legLabel(leg) + " closed (" + reason + ") qty=" + qty + " @ " + String.format("%.2f", l)
            + " pnl=" + String.format("%.2f", pnl));
    }

    private void closeAllLegs(String reason) {
        boolean countAsSl = reason != null && reason.contains("MAX_LOSS");
        for (Leg l : openLegs()) {
            closeLegInternal(l, reason);
            if (countAsSl) slHitsToday++;
        }
        afterLegsChanged();
    }

    private void closeIntradayLegs(String reason) {
        for (Leg l : openLegs()) if (isIntraday(l)) closeLegInternal(l, reason);
        afterLegsChanged();
    }

    /** Book still-open legs as expired at their last LTP — no orders. */
    /** A leg whose contract expires on or before {@code day}. Falls back to the instance's weekly
     *  expiry when the symbol can't be parsed. */
    private boolean legExpiredBy(Leg l, LocalDate day) {
        String exp = parseExpiryFromSymbol(l.symbol, underlying());
        if (exp.isEmpty()) exp = currentWeeklyExpiry == null ? "" : currentWeeklyExpiry;
        if (exp.isEmpty()) return false;
        try { return !LocalDate.parse(exp).isAfter(day); } catch (Exception e) { return false; }
    }
    private boolean anyOpenLegExpiredBy(LocalDate day) { for (Leg l : openLegs()) if (legExpiredBy(l, day)) return true; return false; }

    /** Settle (no close orders) every open leg whose expiry is on or before {@code asOf}; others keep running. */
    private void settleExpiredLegs(LocalDate asOf) {
        double sp = spot();
        double total = 0;
        List<String> unsub = new ArrayList<>();
        long nowMs = System.currentTimeMillis();
        for (Leg leg : openLegs()) {
            if (!legExpiredBy(leg, asOf)) continue;
            double l = Math.max(0, ltp(leg.symbol));
            double pnl = leg.entryPremium > 0
                ? (isShort(leg) ? (leg.entryPremium - l) * leg.qty : (l - leg.entryPremium) * leg.qty) : 0;
            leg.pnl = pnl; leg.closePremium = l; leg.closedAtMillis = nowMs; leg.closeReason = "EXPIRED"; leg.orderId = "";
            realisedPnlToday += pnl; consumedRiskToday += pnl; total += pnl;
            unsub.add(leg.symbol);
        }
        if (!unsub.isEmpty()) {
            try { marketDataService.unsubscribeAdditional(unsub); } catch (Exception ignored) {}
            pushEvent("CLOSE_EXPIRED", sp, "", "", total);
            info("legs EXPIRED (held to expiry, no close orders): " + String.join(", ", unsub)
                + " pnl=" + String.format("%.2f", total));
        }
        afterLegsChanged();
    }

    private void afterLegsChanged() {
        if (hasOpenLegs()) transitionTo(LifecycleState.OPEN);
        else if (!legs.isEmpty()) transitionTo(LifecycleState.DONE_FOR_DAY);
        else persist();
    }

    private String placeOrderRetry(String symbol, int qty, int side, String product, String legTag, String reason) {
        try {
            OrderDTO resp = orderService.placeOrder(symbol, qty, side, 0, product);
            orderCountToday++;
            if (accepted(resp)) return resp.getId();
            log.warn("[{}] First close attempt failed for {} ({}): {} — retrying in 2s", tag(), legTag, reason, resp);
            try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            OrderDTO retry = orderService.placeOrder(symbol, qty, side, 0, product);
            orderCountToday++;
            if (accepted(retry)) return retry.getId();
            error("CLOSE FAILED for " + legTag + " qty=" + qty + " (" + reason + ") — manual intervention required");
        } catch (Exception e) {
            log.error("[{}] Exception closing {}: {}", tag(), legTag, e.getMessage());
        }
        return "";
    }

    // ── Scheduler ─────────────────────────────────────────────────────────────
    @Override
    public void tick() {
        rolloverIfNewDay();
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        if (!isEnabled()) return;
        tryRecoverEntryPremiumFromTradebook();
        if (!hasOpenLegs()) return;
        checkDayEnd(LocalTime.now(IST));
        checkLegSlTriggers();
    }

    @Override
    public void fastSlCheck() {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        if (!isEnabled()) return;
        checkLegSlTriggers();
    }

    /** INTRADAY legs: bot squareoff at 15:20. MARGIN legs: settled at the 15:40 close on expiry day. */
    private synchronized void checkDayEnd(LocalTime now) {
        if (!hasOpenLegs()) return;
        if (hasOpenIntraday() && afterOrAt(now, intradaySquareoff())) {
            info("Intraday squareoff at " + intradaySquareoff() + " — closing MIS leg(s)");
            closeIntradayLegs("TIMED_SQUAREOFF");
        }
        if (hasOpenLegs() && afterOrAt(now, MARKET_CLOSE_TIME)) {
            LocalDate today = LocalDate.now(IST);
            if (anyOpenLegExpiredBy(today)) settleExpiredLegs(today);
        }
    }

    private synchronized void checkLegSlTriggers() {
        boolean changed = false;
        for (Leg leg : openLegs()) {
            if (!isShort(leg) || leg.entryPremium <= 0) continue;
            double l = ltp(leg.symbol);
            if (l <= 0) continue;
            double trigger = effectiveLegTrigger(leg);
            if (trigger <= 0 || l < trigger) continue;
            String thr = leg.slManual > 0 ? "MANUAL" : leg.slMovedToCost ? "COST" : String.format("%.0f%%", leg.slPct);
            info(String.format(java.util.Locale.US, "%s SL hit — entry %.2f, live %.2f (+%.2f pts, trigger %.2f %s). Closing this leg only.",
                legLabel(leg), leg.entryPremium, l, l - leg.entryPremium, trigger, thr));
            closeLegInternal(leg, leg.side + "_SL_HIT");
            changed = true;
        }
        if (changed) afterLegsChanged();
    }

    @Override
    public synchronized void sampleCombinedPremium() {
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;
        if (!hasOpenLegs()) return;
        if (LocalTime.now(IST).isAfter(MARKET_CLOSE_TIME.plusMinutes(5))) return;
        boolean any = false;
        for (Leg leg : openLegs()) {
            double l = ltp(leg.symbol);
            if (l <= 0) continue;
            if (leg.samples == null) leg.samples = new ArrayList<>();
            leg.samples.add(sample(l));
            while (leg.samples.size() > MAX_SAMPLES_PER_LEG) leg.samples.remove(0);
            any = true;
        }
        if (any) persist();
    }

    private static Map<String, Object> sample(double v) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("t", LocalTime.now(IST).format(java.time.format.DateTimeFormatter.ofPattern("HH:mm")));
        s.put("v", Math.round(v * 100.0) / 100.0);
        return s;
    }

    @Override
    public Map<String, Object> legChart(String legId) {
        Leg leg = findLeg(legId);
        Map<String, Object> out = new LinkedHashMap<>();
        if (leg == null) return out;
        out.put("legId", leg.id);
        out.put("side", leg.side);
        out.put("kind", leg.kind);
        out.put("symbol", leg.symbol);
        out.put("strike", parseStrikeFromSymbol(leg.symbol, spot()));
        out.put("entry", round2(leg.entryPremium));
        out.put("open", isOpen(leg));
        out.put("ltp", round2(isOpen(leg) ? ltp(leg.symbol) : leg.closePremium));
        out.put("slTrigger", isShort(leg) && isOpen(leg) ? round2(effectiveLegTrigger(leg)) : 0.0);
        out.put("samples", leg.samples == null ? List.of() : new ArrayList<>(leg.samples));
        return out;
    }

    // ── SL maths ──────────────────────────────────────────────────────────────
    /** Manual override wins, then entry × (1 + slPct/100), collapsed to entry when COST is on. */
    private double effectiveLegTrigger(Leg leg) {
        double entry = leg.entryPremium;
        if (entry <= 0) return 0;
        if (leg.slManual > 0) return leg.slManual;
        double base = leg.slPct > 0 ? entry * (1.0 + leg.slPct / 100.0) : 0;
        if (leg.slMovedToCost && (base <= 0 || base > entry)) base = entry;
        return base;
    }

    // ── Settings — only the underlying survives, driven from the page header ──
    @Override public List<Map<String, Object>> getSettingsSchema() { return List.of(); }

    /** Instance defaults for the open dialogs (editable on the POSITIONS settings tab). */
    public int defaultLots() { return Math.max(1, riskSettings.getStrategyInt(instanceId, "defaultLots", 1)); }
    public String defaultProduct() { return normaliseProduct(riskSettings.getStrategyString(instanceId, "defaultProduct", "INTRADAY")); }
    public double defaultSlPct() { double v = riskSettings.getStrategyDouble(instanceId, "defaultSlPct", 50); return v >= 0 ? v : 50; }

    @Override
    public Map<String, Object> getSettingsValues() {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("underlying", underlying().name());
        v.put("defaultLots", defaultLots());
        v.put("defaultProduct", defaultProduct());
        v.put("defaultSlPct", defaultSlPct());
        return v;
    }

    @Override
    public synchronized void saveSettings(Map<String, Object> values) {
        if (values == null) return;
        boolean dirty = false;
        if (values.containsKey("defaultLots")) {
            int n = 1;
            try { n = Integer.parseInt(String.valueOf(values.get("defaultLots")).trim()); } catch (NumberFormatException ignored) {}
            riskSettings.setStrategySetting(instanceId, "defaultLots", Math.max(1, Math.min(100, n)));
            dirty = true;
        }
        if (values.containsKey("defaultProduct")) {
            riskSettings.setStrategySetting(instanceId, "defaultProduct", normaliseProduct(String.valueOf(values.get("defaultProduct"))));
            dirty = true;
        }
        if (values.containsKey("defaultSlPct")) {
            double n = 50;
            try { n = Double.parseDouble(String.valueOf(values.get("defaultSlPct")).trim()); } catch (NumberFormatException ignored) {}
            riskSettings.setStrategySetting(instanceId, "defaultSlPct", Math.max(0, Math.min(500, n)));
            dirty = true;
        }
        if (dirty) riskSettings.saveFor("live");
        if (!values.containsKey("underlying")) return;
        String u = String.valueOf(values.get("underlying")).trim().toUpperCase();
        if (!"NIFTY".equals(u) && !"SENSEX".equals(u)) return;
        if (u.equals(underlying().name())) return;
        if (hasOpenLegs()) {
            warn("Underlying change to " + u + " refused — legs are open");
            return;
        }
        riskSettings.setStrategySetting(instanceId, "underlying", u);
        riskSettings.saveFor("live");
        this.currentWeeklyExpiry = "";
        info("Underlying set to " + u);
        persist();
    }

    /** Intraday squareoff time from the global PORTFOLIO RISK settings (HH:mm), default 15:20. */
    private LocalTime intradaySquareoff() {
        try {
            String v = riskSettings.getIntradaySquareOffTime();
            if (v != null && !v.isBlank()) return LocalTime.parse(v.trim());
        } catch (Exception ignored) {}
        return INTRADAY_SQUAREOFF_DEFAULT;
    }

    protected Underlying underlying() {
        return Underlying.fromName(riskSettings.getStrategyString(instanceId, "underlying", "NIFTY"));
    }
    private double exchangeRateFracForUnderlying() {
        double configured = underlying() == Underlying.SENSEX ? riskSettings.getBseExchangeRate() : riskSettings.getExchangeRate();
        return configured > 0 ? configured / 100.0 : EXCH_TXN_PCT_FALLBACK;
    }

    // ── Boot resume + legacy migration ────────────────────────────────────────
    private void init() {
        ShortStrangleStateStore.State p = stateStore.get(instanceId);
        if (p != null && p.state != null) {
            this.dayKey = p.dayKey != null ? p.dayKey : "";
            this.lastEntryNifty = p.lastEntryNifty;
            this.realisedPnlToday = p.realisedPnlToday;
            this.sellPremiumTurnoverToday = p.sellPremiumTurnoverToday;
            this.buyPremiumTurnoverToday  = p.buyPremiumTurnoverToday;
            this.orderCountToday = p.orderCountToday;
            this.slHitsToday     = p.slHitsToday;
            this.cycleStartRealisedPnl  = p.cycleStartRealisedPnl;
            this.cycleStartSellTurnover = p.cycleStartSellTurnover;
            this.cycleStartBuyTurnover  = p.cycleStartBuyTurnover;
            this.cycleStartOrderCount   = p.cycleStartOrderCount;
            this.cycleStartSlHits       = p.cycleStartSlHits;
            this.consumedRiskToday      = p.consumedRiskToday;
            this.currentWeeklyExpiry = p.currentWeeklyExpiry != null ? p.currentWeeklyExpiry : "";
            legs.clear();
            if (p.legs != null) {
                for (Leg l : p.legs) { if (l.samples == null) l.samples = new ArrayList<>(); legs.add(l); }
                this.legSeq = Math.max(p.legSeq, legs.size());
            } else {
                migrateLegacyLegs(p);
            }
            backfillLegDefaults();
            if (p.recentEvents != null) {
                this.recentEvents.clear();
                for (Map<String, Object> r : p.recentEvents) {
                    this.recentEvents.addLast(new CycleEvent(
                        String.valueOf(r.getOrDefault("time", "")),
                        String.valueOf(r.getOrDefault("event", "")),
                        ((Number) r.getOrDefault("nifty", 0)).doubleValue(),
                        String.valueOf(r.getOrDefault("ce", "")),
                        String.valueOf(r.getOrDefault("pe", "")),
                        ((Number) r.getOrDefault("pnl", 0)).doubleValue()));
                }
            }
            if (hasOpenLegs()) this.state = LifecycleState.OPEN;
            else if (!legs.isEmpty() || p.state.startsWith("OPEN") || "DONE_FOR_DAY".equals(p.state)) this.state = LifecycleState.DONE_FOR_DAY;
            else this.state = LifecycleState.ARMED;
            log.info("[{}] Resumed state={} dayKey={} legs={} open={} realised={}",
                tag(), state, dayKey, legs.size(), openLegs().size(), realisedPnlToday);
        }
        rolloverIfNewDay();
        List<String> resub = new ArrayList<>();
        for (Leg l : openLegs()) resub.add(l.symbol);
        if (!resub.isEmpty()) {
            try {
                marketDataService.subscribeAdditional(resub);
                log.info("[{}] Re-subscribed open legs to data WS after restart: {}", tag(), resub);
                for (String s : resub) seedLegQuote(s);
            } catch (Exception e) {
                log.warn("[{}] Re-subscribe failed: {}", tag(), e.getMessage());
            }
        }
    }

    /** Legs written before product / slPct existed: product from the old orderType setting,
     *  SL % from the old DTE row for today (default 50). */
    private void backfillLegDefaults() {
        String legacyProduct = normaliseProduct(riskSettings.getStrategyString(instanceId, "orderType", "INTRADAY"));
        boolean changed = false;
        for (Leg l : legs) {
            if (l.product != null && !l.product.isEmpty()) continue;   // written by this version — SL 0 means no SL
            l.product = legacyProduct; changed = true;
            if (isShort(l) && l.slPct <= 0) l.slPct = legacySlPct();
        }
        if (changed) log.info("[{}] Backfilled product / SL % on {} leg(s) from legacy settings", tag(), legs.size());
    }

    private double legacySlPct() {
        int dte = tradingDaysToExpiry(currentWeeklyExpiry);
        if (dte < 0) return 50;
        if (dte > 4) dte = 4;
        double pct = riskSettings.getStrategyDouble(instanceId, "dte." + dte + ".legSlPct", 50);
        return pct > 0 ? pct : 50;
    }

    private void migrateLegacyLegs(ShortStrangleStateStore.State p) {
        int defQty = Math.max(1, riskSettings.getStrategyInt(instanceId, "lotsPerLeg", 1)) * underlying().lotSize();
        long fallbackClosed = p.updatedAtMillis > 0 ? p.updatedAtMillis : System.currentTimeMillis();
        legacyLeg("CE", "SHORT", p.ceSymbol, p.ceQty, p.ceOrderId, p.ceEntryPremium, p.ceClosedAtMillis, p.ceClosePremium,
            p.ceLegPnl, p.ceSlManual, p.ceSlMovedToCost, defQty, fallbackClosed, p.combinedPremiumSamples, "ce");
        legacyLeg("PE", "SHORT", p.peSymbol, p.peQty, p.peOrderId, p.peEntryPremium, p.peClosedAtMillis, p.peClosePremium,
            p.peLegPnl, p.peSlManual, p.peSlMovedToCost, defQty, fallbackClosed, p.combinedPremiumSamples, "pe");
        legacyLeg("CE", "HEDGE", p.hedgeCeSymbol, p.hedgeCeQty, "", p.hedgeCeEntryPremium, 0, p.hedgeCeClosePremium,
            p.hedgeCeLegPnl, 0, false, defQty, fallbackClosed, null, null);
        legacyLeg("PE", "HEDGE", p.hedgePeSymbol, p.hedgePeQty, "", p.hedgePeEntryPremium, 0, p.hedgePeClosePremium,
            p.hedgePeLegPnl, 0, false, defQty, fallbackClosed, null, null);
        this.legSeq = legs.size();
        if (!legs.isEmpty()) {
            log.info("[{}] Migrated legacy two-leg state into {} leg(s)", tag(), legs.size());
            eventService.log("[INFO] [" + instanceId + "] Migrated legacy strangle state into the leg book (" + legs.size() + " legs)");
        }
    }

    private void legacyLeg(String side, String kind, String symbol, int qty, String orderId, double entry, long closedAt,
                           double closePrem, double pnl, double slManual, boolean moved, int defQty,
                           long fallbackClosed, List<Map<String, Object>> samples, String sampleKey) {
        if (symbol == null || symbol.isEmpty()) return;
        Leg l = new Leg();
        l.id = "L" + (legs.size() + 1);
        l.side = side; l.kind = kind; l.symbol = symbol;
        boolean open = qty > 0;
        l.qty = open ? qty : defQty;
        l.orderId = orderId != null ? orderId : "";
        l.entryPremium = entry;
        l.openedAtMillis = 0;
        l.closedAtMillis = open ? 0 : (closedAt > 0 ? closedAt : fallbackClosed);
        l.closePremium = closePrem;
        l.pnl = pnl;
        l.closeReason = open ? null : "MIGRATED";
        l.slManual = slManual;
        l.slMovedToCost = moved;
        l.samples = new ArrayList<>();
        if (samples != null && sampleKey != null) {
            for (Map<String, Object> s : samples) {
                Object v = s.get(sampleKey);
                if (v instanceof Number n && n.doubleValue() > 0) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("t", s.get("t")); m.put("v", n.doubleValue());
                    l.samples.add(m);
                }
            }
        }
        legs.add(l);
    }

    // ── Charges ───────────────────────────────────────────────────────────────
    private Map<String, Double> computeChargesBreakdown() {
        int projectedOrders = orderCountToday;
        double projectedBuyT  = buyPremiumTurnoverToday;
        double projectedSellT = sellPremiumTurnoverToday;
        for (Leg l : openLegs()) {
            double lp = ltp(l.symbol);
            if (lp > 0) { if (isShort(l)) projectedBuyT += lp * l.qty; else projectedSellT += lp * l.qty; }
            projectedOrders++;
        }
        double brokerage = projectedOrders * riskSettings.getBrokeragePerOrder();
        double sellT = projectedSellT;
        double buyT  = projectedBuyT;
        double totalT = sellT + buyT;
        double exchRateFrac  = exchangeRateFracForUnderlying();
        double sttRateFrac   = riskSettings.getSttRate()       > 0 ? riskSettings.getSttRate()       / 100.0 : STT_SELL_PCT_FALLBACK;
        double stampRateFrac = riskSettings.getStampDutyRate() > 0 ? riskSettings.getStampDutyRate() / 100.0 : STAMP_BUY_PCT_FALLBACK;
        double stt      = sellT * sttRateFrac;
        double exchange = totalT * exchRateFrac;
        double sebi     = (totalT / 10_000_000.0) * SEBI_PER_CRORE;
        double stamp    = buyT * stampRateFrac;
        double gst      = (brokerage + exchange + sebi) * GST_PCT;
        double total    = brokerage + stt + exchange + sebi + stamp + gst;
        Map<String, Double> b = new LinkedHashMap<>();
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

    private double computeCycleCharges(double sellPrem, double buyPrem, int orders) {
        double brokerage = orders * riskSettings.getBrokeragePerOrder();
        double totalPrem = sellPrem + buyPrem;
        double exchRateFrac  = exchangeRateFracForUnderlying();
        double sttRateFrac   = riskSettings.getSttRate()       > 0 ? riskSettings.getSttRate()       / 100.0 : STT_SELL_PCT_FALLBACK;
        double stampRateFrac = riskSettings.getStampDutyRate() > 0 ? riskSettings.getStampDutyRate() / 100.0 : STAMP_BUY_PCT_FALLBACK;
        double stt      = sellPrem * sttRateFrac;
        double exchange = totalPrem * exchRateFrac;
        double sebi     = (totalPrem / 10_000_000.0) * SEBI_PER_CRORE;
        double stamp    = buyPrem * stampRateFrac;
        double gst      = (brokerage + exchange + sebi) * GST_PCT;
        return round2(brokerage + stt + exchange + sebi + stamp + gst);
    }

    private double openMtm() {
        double m = 0;
        for (Leg l : openLegs()) {
            double lp = ltp(l.symbol);
            if (lp <= 0 || l.entryPremium <= 0) continue;
            m += isShort(l) ? (l.entryPremium - lp) * l.qty : (lp - l.entryPremium) * l.qty;
        }
        return m;
    }

    @Override
    public double liveNetPnlToday() {
        if (marketDataService == null) return realisedPnlToday;
        return realisedPnlToday + openMtm() - computeChargesBreakdown().getOrDefault("total", 0.0);
    }

    @Override
    public double liveChargesToday() {
        return computeChargesBreakdown().getOrDefault("total", 0.0);
    }

    // ── Dashboard / status ────────────────────────────────────────────────────
    @Override
    public Map<String, Object> getDashboard() {
        rolloverIfNewDay();
        if (currentWeeklyExpiry != null && !currentWeeklyExpiry.isEmpty()) {
            try {
                java.time.DayOfWeek expected = underlying().expiryDayOfWeek();
                LocalDate cur = LocalDate.parse(currentWeeklyExpiry);
                boolean ok = marketHolidayService != null
                    ? marketHolidayService.isWeeklyExpiryDate(cur, expected)
                    : cur.getDayOfWeek() == expected;
                if (!ok) this.currentWeeklyExpiry = "";
            } catch (Exception ignored) {}
        }
        if (currentWeeklyExpiry == null || currentWeeklyExpiry.isEmpty()) tryResolveWeeklyExpiry();

        Map<String, Object> m = getStatus();
        m.put("dashboardShape", "short-strangle");
        String displayExpiry = (currentWeeklyExpiry != null && !currentWeeklyExpiry.isEmpty())
            ? currentWeeklyExpiry : nextExpectedWeeklyExpiry();
        m.put("weeklyExpiry", displayExpiry);
        m.put("daysToExpiry", tradingDaysToExpiry(displayExpiry));
        double sp = spot();
        m.put("niftyLtp", sp);
        if (marketDataService != null) {
            m.put("niftyDisplayLtp", round2(marketDataService.getDisplayLtp(underlying().indexSymbol())));
            m.put("niftyChange",     round2(marketDataService.getDisplayChange(underlying().indexSymbol())));
            m.put("niftyChangePct",  round2(marketDataService.getDisplayChangePct(underlying().indexSymbol())));
            String vix = "NSE:INDIAVIX-INDEX";
            m.put("vixDisplayLtp",   round2(marketDataService.getDisplayLtp(vix)));
            m.put("vixChange",       round2(marketDataService.getDisplayChange(vix)));
            m.put("vixChangePct",    round2(marketDataService.getDisplayChangePct(vix)));
        }

        List<Map<String, Object>> legRows = new ArrayList<>();
        double mtmTotal = 0, maxLoss = 0;
        int uncapped = 0;
        int openShortCount = 0, openHedgeCount = 0;
        for (Leg l : legs) {
            boolean open = isOpen(l);
            boolean shortLeg = isShort(l);
            double lp = open ? ltp(l.symbol) : l.closePremium;
            double mtm;
            if (open) {
                mtm = (lp > 0 && l.entryPremium > 0) ? (shortLeg ? (l.entryPremium - lp) * l.qty : (lp - l.entryPremium) * l.qty) : 0;
                mtmTotal += mtm;
                if (shortLeg) openShortCount++; else openHedgeCount++;
            } else {
                mtm = l.pnl;
            }
            double trigger = (open && shortLeg) ? effectiveLegTrigger(l) : 0;
            if (open && shortLeg && l.entryPremium > 0 && trigger > l.entryPremium) maxLoss += (trigger - l.entryPremium) * l.qty;
            if (open && shortLeg && trigger <= 0) uncapped++;
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("id", l.id);
            r.put("side", l.side);
            r.put("kind", l.kind);
            r.put("symbol", l.symbol);
            r.put("expiry", parseExpiryFromSymbol(l.symbol, underlying()));
            r.put("product", normaliseProduct(l.product));
            r.put("strike", parseStrikeFromSymbol(l.symbol, sp));
            r.put("moneyness", moneynessLabel(l.symbol, isCe(l)));
            r.put("qty", l.qty);
            r.put("entryPremium", round2(l.entryPremium));
            r.put("ltp", round2(lp));
            r.put("mtm", round2(mtm));
            r.put("open", open);
            r.put("openedAtMillis", l.openedAtMillis);
            r.put("closedAtMillis", l.closedAtMillis);
            r.put("closePremium", round2(l.closePremium));
            r.put("closeReason", l.closeReason);
            r.put("slPct", l.slPct);
            r.put("slTrigger", round2(trigger));
            r.put("slManual", l.slManual > 0);
            r.put("slMovedToCost", l.slMovedToCost);
            double consumed = 0;
            if (open && shortLeg && trigger > 0 && lp > 0 && Math.abs(trigger - l.entryPremium) > 0.0001) {
                consumed = ((lp - l.entryPremium) / (trigger - l.entryPremium)) * 100.0;
            }
            r.put("slConsumedPct", round2(consumed));
            Map<String, Object> g = open ? computeLegGreeks(l.symbol, sp, lp, isCe(l)) : emptyGreeks();
            r.put("delta", g.get("delta")); r.put("theta", g.get("theta"));
            r.put("vega", g.get("vega"));   r.put("iv", g.get("iv"));
            legRows.add(r);
        }
        m.put("legs", legRows);
        m.put("openShortCount", openShortCount);
        m.put("openHedgeCount", openHedgeCount);
        m.put("combinedMtm", round2(mtmTotal));
        m.put("realisedPnlToday", round2(realisedPnlToday));
        m.put("totalPnlToday", round2(realisedPnlToday + mtmTotal));
        m.put("maxLossPerStrangle", round2(maxLoss));
        m.put("uncappedLegs", uncapped);
        m.put("payoff", openPayoffStats(sp));
        m.put("spanMargin", round2(currentSpanMargin(sp)));
        m.put("consumedRiskToday", round2(consumedRiskToday));
        Map<String, Double> charges = computeChargesBreakdown();
        m.put("charges", charges);
        m.put("netPnlToday", round2(realisedPnlToday - charges.get("total")));

        List<Map<String, Object>> events = new ArrayList<>();
        for (CycleEvent e : recentEvents) {
            Map<String, Object> rm = new LinkedHashMap<>();
            rm.put("time",  e.time());
            rm.put("event", e.event());
            rm.put("nifty", e.nifty());
            rm.put("ce",    e.ce());
            rm.put("pe",    e.pe());
            rm.put("pnl",   round2(e.pnl()));
            events.add(rm);
        }
        m.put("recentRolls", events);
        return m;
    }

    @Override
    public Map<String, Object> getStatus() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("state", state.name());
        boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();
        String displayState = (state == LifecycleState.ARMED && !tradingDay) ? "IDLE" : state.name();
        m.put("displayState", displayState);
        if ("IDLE".equals(displayState)) {
            java.time.DayOfWeek dow = LocalDate.now(IST).getDayOfWeek();
            m.put("displayStateReason", (dow == java.time.DayOfWeek.SATURDAY || dow == java.time.DayOfWeek.SUNDAY)
                ? "Market closed — weekend." : "Market closed — NSE holiday.");
        }
        m.put("dayKey", dayKey);
        m.put("strategyType", strategyType());
        m.put("lastEntryNifty", lastEntryNifty);
        m.put("underlying", underlying().name());
        m.put("lotSize", underlying().lotSize());
        m.put("intradaySquareoff", intradaySquareoff().toString());
        m.put("defaultLots", defaultLots());
        m.put("defaultProduct", defaultProduct());
        m.put("defaultSlPct", defaultSlPct());
        m.put("hasOpenIntraday", hasOpenIntraday());
        m.put("hasOpenOvernight", hasOpenOvernight());
        m.put("expiryDayToday", isExpiryDayToday());
        m.put("enabled", isEnabled());
        int open = 0; for (Leg l : legs) if (isOpen(l)) open++;
        m.put("openLegCount", open);
        m.put("legCount", legs.size());
        return m;
    }

    // ── Manual controls ───────────────────────────────────────────────────────
    public synchronized boolean forceCloseAll(String reason) {
        if (!hasOpenLegs()) {
            log.info("[{}] forceClose ignored — no open legs", tag());
            return false;
        }
        info("Manual squareoff (" + reason + ") — flattening " + openLegs().size() + " open leg(s)");
        closeAllLegs(reason);
        return true;
    }

    @Override
    public synchronized void parkDoneForDay(String reason) {
        if (hasOpenLegs()) {
            info("Portfolio kill (" + reason + ") — flattening every open leg");
            closeAllLegs(reason);
        } else {
            log.info("[{}] parkDoneForDay({}) — nothing open", tag(), reason);
        }
    }

    @Override
    public synchronized boolean resetToIdle(String reason) {
        if (hasOpenLegs()) {
            warn("RESET refused — legs are still open. Squareoff first.");
            return false;
        }
        info("state reset to ARMED (" + reason + ")");
        transitionTo(LifecycleState.ARMED);
        return true;
    }

    @Override
    public synchronized boolean resetTodayCounters() {
        if (hasOpenLegs()) return false;
        resetDayState();
        persist();
        log.warn("[{}] [{}] today's counters reset by operator (clear trade data)", tag(), instanceId);
        eventService.log("[WARNING] [" + instanceId + "] today's counters reset by operator (clear trade data)");
        return true;
    }

    // ── Day rollover + session persistence ────────────────────────────────────
    private synchronized void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(dayKey)) return;
        LocalDate yesterday = LocalDate.now(IST).minusDays(1);   // expiry strictly before today
        if (hasOpenLegs() && anyOpenLegExpiredBy(yesterday)) {
            warn("open legs from an expired series (last seen " + dayKey + ") — settling as expired");
            settleExpiredLegs(yesterday);
        }
        if (hasOpenIntraday()) {
            warn("stale INTRADAY legs from " + dayKey + " detected at startup — flattening");
            for (Leg l : openLegs()) if (isIntraday(l)) closeLegInternal(l, "STALE_DAY_RESET");
            afterLegsChanged();
        }
        if (hasOpenLegs()) {
            String prevDay = dayKey;
            this.dayKey = today;
            resetDayCounters();
            resubscribeOpenLegs();
            List<String> syms = new ArrayList<>();
            for (Leg l : openLegs()) syms.add(l.symbol);
            info("position carried overnight " + prevDay + " → " + today + " (NRML) — " + String.join(", ", syms) + "; SL continues");
            persist();
            return;
        }
        if (dayKey != null && !dayKey.isEmpty() && realisedPnlToday != 0) {
            try { persistSessionFor(dayKey); }
            catch (Exception e) { log.warn("[{}] Failed to persist session row for {}: {}", tag(), dayKey, e.getMessage()); }
        }
        this.dayKey = today;
        resetDayState();
        transitionTo(LifecycleState.ARMED);
    }

    private void resetDayCounters() {
        this.realisedPnlToday = 0;
        this.slHitsToday = 0;
        this.cycleStartRealisedPnl = 0;
        this.cycleStartSellTurnover = 0;
        this.cycleStartBuyTurnover = 0;
        this.cycleStartOrderCount = 0;
        this.cycleStartSlHits = 0;
        this.consumedRiskToday = 0;
        this.pendingFills.clear();
        this.recentEvents.clear();
        for (Leg l : legs) if (l.samples != null) l.samples.clear();
        legs.removeIf(l -> !isOpen(l));
    }

    private void resubscribeOpenLegs() {
        List<String> syms = new ArrayList<>();
        for (Leg l : openLegs()) syms.add(l.symbol);
        if (syms.isEmpty()) return;
        try { marketDataService.subscribeAdditional(syms); } catch (Exception ignored) {}
    }

    private void resetDayState() {
        this.lastEntryNifty = 0;
        this.legs.clear();
        this.legSeq = 0;
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
    }

    private void persistSessionFor(String date) {
        if (sessionRepo == null) return;
        Map<String, Double> chargesBreakdown = computeChargesBreakdown();
        double charges = chargesBreakdown.get("total");
        double gross   = realisedPnlToday;
        double net     = gross - charges;
        com.rydytrader.autotrader.entity.StrategySessionEntity row =
            sessionRepo.findByStrategyIdAndSessionDate(instanceId, date)
                       .orElseGet(com.rydytrader.autotrader.entity.StrategySessionEntity::new);
        row.setStrategyId(instanceId);
        row.setSessionDate(date);
        row.setEntries(!legs.isEmpty() || realisedPnlToday != 0 ? 1 : 0);
        row.setRolls(0);
        row.setFinalState(state.name());
        row.setPremiumCollected(round2(sellPremiumTurnoverToday));
        row.setPremiumPaidBack(round2(buyPremiumTurnoverToday));
        row.setGrossPnl(gross);
        row.setCharges(charges);
        row.setNetPnl(net);
        if (row.getCreatedAt() == 0) row.setCreatedAt(System.currentTimeMillis());
        sessionRepo.save(row);
        log.info("[{}] Persisted session row for {}: gross={} net={}", tag(), date, gross, net);
    }

    // ── Expiry helpers ────────────────────────────────────────────────────────
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
                log.warn("[{}] async expiry refresh failed: {}", tag(), e.getMessage());
            } finally {
                expiryRefreshInFlight.set(false);
            }
        });
    }

    private String nextExpectedWeeklyExpiry() {
        try {
            java.time.DayOfWeek expiryDow = underlying().expiryDayOfWeek();
            LocalDate today = LocalDate.now(IST);
            if (marketHolidayService != null) return marketHolidayService.weeklyExpiryOnOrAfter(today, expiryDow).toString();
            LocalDate cursor = today;
            while (cursor.getDayOfWeek() != expiryDow) cursor = cursor.plusDays(1);
            return cursor.toString();
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

    private boolean isExpiryDayToday() {
        return currentWeeklyExpiry != null && !currentWeeklyExpiry.isEmpty()
            && tradingDaysToExpiry(currentWeeklyExpiry) == 0;
    }

    // ── Symbols, moneyness, greeks ────────────────────────────────────────────
    private static final java.util.regex.Pattern STRIKE_PATTERN =
        java.util.regex.Pattern.compile("(\\d+)(?:CE|PE)$", java.util.regex.Pattern.CASE_INSENSITIVE);

    private String moneynessLabel(String sym, boolean ce) {
        double sp = spot();
        long strike = parseStrikeFromSymbol(sym, sp);
        if (strike <= 0 || sp <= 0) return null;
        long step = underlying().strikeStep();
        long atm  = Math.round(sp / step) * step;
        long n    = (strike - atm) / step;
        if (n == 0) return "ATM";
        boolean otm = ce ? n > 0 : n < 0;
        return (otm ? "OTM-" : "ITM-") + Math.abs(n);
    }

    private static long parseStrikeFromSymbol(String sym, double spotHint) {
        if (sym == null) return 0;
        java.util.regex.Matcher m = STRIKE_PATTERN.matcher(sym);
        if (!m.find()) return 0;
        String digits = m.group(1);
        int[] order = spotHint > 0 ? new int[]{4, 5, 6} : new int[]{5, 4, 6};
        long best = 0; double bestDist = Double.MAX_VALUE;
        for (int len : order) {
            if (digits.length() < len) continue;
            long v = Long.parseLong(digits.substring(digits.length() - len));
            if (spotHint <= 0) return v;
            double dist = Math.abs(v - spotHint);
            if (dist < bestDist) { bestDist = dist; best = v; }
        }
        return best;
    }

    private double yearsToExpiryPrecise(String symbol) {
        String exp = symbol == null ? "" : parseExpiryFromSymbol(symbol, underlying());
        if (exp.isEmpty()) exp = currentWeeklyExpiry == null ? "" : currentWeeklyExpiry;
        if (exp.isEmpty()) return 0;
        try {
            LocalDate expiry = LocalDate.parse(exp);
            java.time.ZonedDateTime expiryClose = expiry.atTime(15, 40).atZone(IST);
            long seconds = java.time.temporal.ChronoUnit.SECONDS.between(java.time.ZonedDateTime.now(IST), expiryClose);
            if (seconds <= 0) return 1.0 / (365.0 * 24.0);
            return seconds / (365.0 * 24.0 * 3600.0);
        } catch (Exception e) { return 0; }
    }

    private Map<String, Object> computeLegGreeks(String symbol, double sp, double optionLtp, boolean isCall) {
        Map<String, Object> g = emptyGreeks();
        if (sp <= 0 || optionLtp <= 0 || symbol == null || symbol.isEmpty()) return g;
        long strike = parseStrikeFromSymbol(symbol, sp);
        if (strike <= 0) return g;
        double T = yearsToExpiryPrecise(symbol);
        if (T <= 0) return g;
        double r = com.rydytrader.autotrader.util.BlackScholes.DEFAULT_RISK_FREE_RATE;
        double iv = com.rydytrader.autotrader.util.BlackScholes.impliedVol(sp, strike, T, r, optionLtp, isCall);
        if (iv <= 0) return g;
        g.put("delta", round4(com.rydytrader.autotrader.util.BlackScholes.delta(sp, strike, T, r, iv, isCall)));
        g.put("theta", round2(com.rydytrader.autotrader.util.BlackScholes.theta(sp, strike, T, r, iv, isCall)));
        g.put("vega",  round2(com.rydytrader.autotrader.util.BlackScholes.vega (sp, strike, T, r, iv)));
        g.put("gamma", round4(com.rydytrader.autotrader.util.BlackScholes.gamma(sp, strike, T, r, iv)));
        g.put("iv",    round2(iv * 100.0));
        return g;
    }

    private static Map<String, Object> emptyGreeks() {
        Map<String, Object> g = new LinkedHashMap<>();
        g.put("delta", null); g.put("theta", null); g.put("vega", null);
        g.put("gamma", null); g.put("iv", null);
        return g;
    }

    public static String parseExpiryFromSymbol(String fyersSymbol) { return parseExpiryFromSymbol(fyersSymbol, null); }

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
                else expiryDow = "SENSEX".equals(matched) ? java.time.DayOfWeek.THURSDAY : java.time.DayOfWeek.TUESDAY;
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

    // ── Quotes + recovery ─────────────────────────────────────────────────────
    private double readEntryPremium(String symbol) {
        try {
            double l = marketDataService.getLtp(symbol);
            if (l > 0) return l;
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
                    return lp;
                }
            }
        } catch (Exception e) {
            log.warn("[{}] REST quote fallback failed for {}: {}", tag(), symbol, e.getMessage());
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
            log.warn("[{}] Seed leg quote failed for {}: {}", tag(), symbol, e.getMessage());
        }
    }

    private synchronized void tryRecoverEntryPremiumFromTradebook() {
        if (tokenStore.getAccessToken() == null || tokenStore.getAccessToken().isEmpty()) return;
        boolean changed = false;
        for (Leg l : openLegs()) {
            if (l.entryPremium > 0 || l.orderId == null || l.orderId.isEmpty()) continue;
            try {
                double fill = orderService.getFilledPriceByOrderId(l.orderId);
                if (fill > 0) {
                    l.entryPremium = fill;
                    if (isShort(l)) sellPremiumTurnoverToday += fill * l.qty; else buyPremiumTurnoverToday += fill * l.qty;
                    log.info("[{}] Recovered entry premium from tradebook for {}: {}", tag(), legLabel(l), fill);
                    changed = true;
                }
            } catch (Exception ignored) {}
        }
        if (changed) persist();
    }

    // ── Events, transitions, persistence ──────────────────────────────────────
    private void pushEvent(String evt, double nifty, String ce, String pe, double pnl) {
        String ts = LocalTime.now(IST).format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        synchronized (recentEvents) {
            recentEvents.addFirst(new CycleEvent(ts, evt, nifty, ce, pe, pnl));
            while (recentEvents.size() > 30) recentEvents.removeLast();
        }
    }

    private void transitionTo(LifecycleState next) {
        LifecycleState prev = this.state;
        this.state = next;
        if (next == LifecycleState.DONE_FOR_DAY && prev != LifecycleState.DONE_FOR_DAY) persistStraddleTrade();
        persist();
    }

    private void persistStraddleTrade() {
        if (tradeRepo == null) return;
        double cycleGross = realisedPnlToday         - cycleStartRealisedPnl;
        double cycleSellT = sellPremiumTurnoverToday - cycleStartSellTurnover;
        double cycleBuyT  = buyPremiumTurnoverToday  - cycleStartBuyTurnover;
        int    cycleOrders= orderCountToday          - cycleStartOrderCount;
        int    cycleSls   = slHitsToday              - cycleStartSlHits;
        if (Math.abs(cycleGross) < 0.01 && cycleSellT < 0.01) return;
        try {
            double charges = computeCycleCharges(cycleSellT, cycleBuyT, cycleOrders);
            com.rydytrader.autotrader.entity.StrategyTradeEntity t =
                new com.rydytrader.autotrader.entity.StrategyTradeEntity();
            t.setStrategyId(instanceId);
            t.setSessionDate(dayKey != null && !dayKey.isEmpty() ? dayKey : LocalDate.now(IST).toString());
            t.setClosedAtMillis(System.currentTimeMillis());
            int qty = 0;
            for (Leg l : legs) if (isShort(l)) qty = Math.max(qty, l.qty);
            if (qty == 0) qty = underlying().lotSize();
            t.setQty(qty);
            t.setGrossPnl(round2(cycleGross));
            t.setCharges(round2(charges));
            t.setNetPnl(round2(cycleGross - charges));
            t.setCloseReason("DONE_FOR_DAY");
            t.setSlHitCount(cycleSls);
            tradeRepo.save(t);
            this.cycleStartRealisedPnl  = realisedPnlToday;
            this.cycleStartSellTurnover = sellPremiumTurnoverToday;
            this.cycleStartBuyTurnover  = buyPremiumTurnoverToday;
            this.cycleStartOrderCount   = orderCountToday;
            this.cycleStartSlHits       = slHitsToday;
        } catch (Exception e) {
            log.warn("[{}] Failed to persist strategy_trades row: {}", tag(), e.getMessage());
        }
    }

    private synchronized void persist() {
        ShortStrangleStateStore.State s = new ShortStrangleStateStore.State();
        s.dayKey = this.dayKey;
        s.state = this.state.name();
        List<Leg> copy = new ArrayList<>();
        for (Leg l : legs) {
            Leg c = new Leg();
            c.id = l.id; c.side = l.side; c.kind = l.kind; c.symbol = l.symbol; c.qty = l.qty; c.orderId = l.orderId;
            c.product = l.product; c.slPct = l.slPct;
            c.entryPremium = l.entryPremium; c.openedAtMillis = l.openedAtMillis; c.closedAtMillis = l.closedAtMillis;
            c.closePremium = l.closePremium; c.pnl = l.pnl; c.closeReason = l.closeReason; c.slManual = l.slManual;
            c.slMovedToCost = l.slMovedToCost; c.trailingStepsHit = l.trailingStepsHit;
            c.samples = l.samples == null ? new ArrayList<>() : new ArrayList<>(l.samples);
            copy.add(c);
        }
        s.legs = copy;
        s.legSeq = this.legSeq;
        s.lastEntryNifty = this.lastEntryNifty;
        s.realisedPnlToday = this.realisedPnlToday;
        s.sellPremiumTurnoverToday = this.sellPremiumTurnoverToday;
        s.buyPremiumTurnoverToday  = this.buyPremiumTurnoverToday;
        s.orderCountToday = this.orderCountToday;
        s.slHitsToday     = this.slHitsToday;
        s.cycleStartRealisedPnl  = this.cycleStartRealisedPnl;
        s.cycleStartSellTurnover = this.cycleStartSellTurnover;
        s.cycleStartBuyTurnover  = this.cycleStartBuyTurnover;
        s.cycleStartOrderCount   = this.cycleStartOrderCount;
        s.cycleStartSlHits       = this.cycleStartSlHits;
        s.consumedRiskToday      = this.consumedRiskToday;
        s.currentWeeklyExpiry = this.currentWeeklyExpiry;
        List<Map<String, Object>> events = new ArrayList<>();
        synchronized (recentEvents) {
            for (CycleEvent e : recentEvents) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("time", e.time()); m.put("event", e.event()); m.put("nifty", e.nifty());
                m.put("ce", e.ce()); m.put("pe", e.pe()); m.put("pnl", e.pnl());
                events.add(m);
            }
        }
        s.recentEvents = events;
        stateStore.update(instanceId, s);
    }

    // ── Small helpers ─────────────────────────────────────────────────────────
    private static boolean afterOrAt(LocalTime a, LocalTime b) { return !a.isBefore(b); }
    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
    private static double round4(double v) { return Math.round(v * 10000.0) / 10000.0; }
}
