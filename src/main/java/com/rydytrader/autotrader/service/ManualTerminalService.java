package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.manual.ManualClosedTrade;
import com.rydytrader.autotrader.store.manual.ManualPosition;
import com.rydytrader.autotrader.store.manual.ManualTerminalStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Manual NIFTY options terminal — places market orders, tracks open positions / closed trades
 * in {@link ManualTerminalStore}, listens to {@link OrderEventService}'s WS fill events filtered
 * to its own orderIds, and exposes a dashboard payload for the modal poll.
 *
 * <p>Strictly isolated from {@code ShortStraddle}: the only shared infrastructure is the
 * order WS fill listener (which uses orderId membership in the store as the filter) and
 * {@link MarketDataService}'s subscription bus.
 */
@Service
public class ManualTerminalService {

    private static final Logger log = LoggerFactory.getLogger(ManualTerminalService.class);

    private static final int NIFTY_LOT_SIZE = 65;
    // Same rates ShortStraddle uses — kept in sync (manual trades go through the same broker
    // pipeline so the charge math is identical). GST base = brokerage + exchange + sebi.
    private static final double STT_SELL_PCT   = 0.000625;
    private static final double EXCH_TXN_PCT   = 0.0003503;
    private static final double GST_PCT        = 0.18;
    private static final double SEBI_PER_CRORE = 10.0;
    private static final double STAMP_BUY_PCT  = 0.00003;

    private final OrderService        orderService;
    private final MarketDataService   marketDataService;
    private final FyersClientRouter   fyersClient;
    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final OrderEventService   orderEventService;
    private final EventService        eventService;
    private final ManualTerminalStore store;
    private final com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;

    public ManualTerminalService(OrderService orderService,
                                 @Lazy MarketDataService marketDataService,
                                 FyersClientRouter fyersClient,
                                 TokenStore tokenStore,
                                 FyersProperties fyersProperties,
                                 @Lazy OrderEventService orderEventService,
                                 EventService eventService,
                                 ManualTerminalStore store,
                                 com.rydytrader.autotrader.store.RiskSettingsStore riskSettings) {
        this.orderService       = orderService;
        this.marketDataService  = marketDataService;
        this.fyersClient        = fyersClient;
        this.tokenStore         = tokenStore;
        this.fyersProperties    = fyersProperties;
        this.orderEventService  = orderEventService;
        this.eventService       = eventService;
        this.store              = store;
        this.riskSettings       = riskSettings;
    }

    @PostConstruct
    private void init() {
        // Re-subscribe LTPs for every open position so the dashboard's P&L computation works
        // right after a restart.
        for (ManualPosition p : store.openSnapshot()) {
            try { marketDataService.subscribeAdditional(List.of(p.symbol)); }
            catch (Exception ignored) {}
        }
        orderEventService.addFillListener(this::onFill);
        log.info("[ManualTerminal] Initialised with {} open position(s).", store.openSnapshot().size());
    }

    // ── Place / close ─────────────────────────────────────────────────────────

    /** Small result struct so the controller can surface the broker's exact rejection
     *  reason to the frontend instead of a generic "check event log" message. */
    public record PlaceResult(boolean success, String orderId, String message) {}

    /** Backward-compat overload (defaults SL to 50 pts). */
    public synchronized PlaceResult placeOrder(String symbol, int lots, String side, String product) {
        return placeOrder(symbol, lots, side, product, 50);
    }

    /** Place a market Buy or Sell with a stop-loss expressed in PREMIUM POINTS from entry.
     *  Pass 0 to disable SL. The scheduled monitor computes the trigger price as
     *  {@code avgPrice ± stopLossPts} per side and auto-closes on breach. */
    public synchronized PlaceResult placeOrder(String symbol, int lots, String side, String product, double stopLossPts) {
        if (symbol == null || symbol.isBlank()) return new PlaceResult(false, null, "Symbol is required");
        boolean buy  = "BUY".equalsIgnoreCase(side);
        boolean sell = "SELL".equalsIgnoreCase(side);
        if (!buy && !sell) return new PlaceResult(false, null, "Side must be BUY or SELL");
        int qty = Math.max(1, lots) * NIFTY_LOT_SIZE;
        int sideInt = buy ? 1 : -1;
        String prod = (product == null || product.isBlank()) ? "INTRADAY" : product.trim().toUpperCase();

        OrderDTO resp;
        try {
            resp = orderService.placeOrder(symbol, qty, sideInt, 0, prod);
        } catch (Exception e) {
            log.error("[ManualTerminal] place {} {} qty={} {} threw: {}", side, symbol, qty, prod, e.getMessage());
            eventService.log("[ERROR] [manual] place " + side + " " + symbol + " — " + e.getMessage());
            return new PlaceResult(false, null, "Order failed: " + e.getMessage());
        }
        if (resp == null || resp.getId() == null || resp.getId().isEmpty()
            || !"ok".equalsIgnoreCase(resp.getStatus())) {
            String msg = resp == null ? "null response" : (resp.getMessage() == null ? "rejected" : resp.getMessage());
            log.warn("[ManualTerminal] {} order rejected: {}", side, msg);
            eventService.log("[ERROR] [manual] " + side + " " + symbol + " rejected — " + msg);
            return new PlaceResult(false, null, side + " order rejected: " + msg);
        }

        ManualPosition p = new ManualPosition();
        p.orderId    = resp.getId();
        p.symbol     = symbol;
        p.side       = buy ? "BUY" : "SELL";
        p.qty        = qty;
        p.avgPrice   = 0;
        p.openMillis = System.currentTimeMillis();
        p.product    = prod;
        p.filled     = false;
        p.stopLossPts = Math.max(0, stopLossPts);
        store.putOpen(p);

        try { marketDataService.subscribeAdditional(List.of(symbol)); }
        catch (Exception ignored) {}

        eventService.log("[INFO] [manual] " + side + " " + symbol + " qty=" + qty + " (orderId=" + resp.getId() + ")");
        return new PlaceResult(true, resp.getId(), side + " submitted for " + symbol + " — orderId " + resp.getId());
    }

    /** Close a specific manual position by entry orderId — places an opposite-side market
     *  order. Returns the close orderId on success, null otherwise. */
    public synchronized String closePosition(String entryOrderId) {
        Optional<ManualPosition> opt = store.findByOrderId(entryOrderId);
        if (opt.isEmpty()) return null;
        ManualPosition p = opt.get();
        // Allow close even if not yet filled — Fyers may have accepted but the WS push lagged.
        // The fill listener will reconcile.
        String oppositeSide = "BUY".equalsIgnoreCase(p.side) ? "SELL" : "BUY";
        int sideInt = "BUY".equalsIgnoreCase(oppositeSide) ? 1 : -1;
        OrderDTO resp;
        try {
            resp = orderService.placeOrder(p.symbol, p.qty, sideInt, 0, p.product);
        } catch (Exception e) {
            log.error("[ManualTerminal] close {} threw: {}", p.symbol, e.getMessage());
            eventService.log("[ERROR] [manual] close " + p.symbol + " — " + e.getMessage());
            return null;
        }
        if (resp == null || resp.getId() == null || resp.getId().isEmpty()
            || !"ok".equalsIgnoreCase(resp.getStatus())) {
            String msg = resp == null ? "null response" : (resp.getMessage() == null ? "rejected" : resp.getMessage());
            log.warn("[ManualTerminal] close rejected for {}: {}", p.symbol, msg);
            eventService.log("[ERROR] [manual] close " + p.symbol + " rejected — " + msg);
            return null;
        }
        store.mapCloseToEntry(resp.getId(), p.orderId);
        eventService.log("[INFO] [manual] CLOSE " + p.symbol + " qty=" + p.qty + " (closeId=" + resp.getId() + ")");
        return resp.getId();
    }

    public synchronized int closeAll() {
        int n = 0;
        for (ManualPosition p : store.openSnapshot()) {
            String closeId = closePosition(p.orderId);
            if (closeId != null) n++;
        }
        return n;
    }

    /** Cancel every working manual order at Fyers — anything in our {@code closeOrderIdToEntry}
     *  map that's still in Transit/Pending in the orderbook gets cancelled. Returns count. */
    public synchronized int cancelAll() {
        int n = 0;
        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
        try {
            JsonNode root = fyersClient.getOrders(auth);
            if (root == null) return 0;
            JsonNode book = root.has("orderBook") ? root.get("orderBook") : root.get("data");
            if (book == null || !book.isArray()) return 0;
            // Build the set of orderIds we own — both entry orderIds and active close orderIds.
            java.util.Set<String> ours = new java.util.HashSet<>();
            for (ManualPosition p : store.openSnapshot()) ours.add(p.orderId);
            // Walk live orderbook, cancel anything we own that's still working.
            for (JsonNode row : book) {
                String id = row.has("id") ? row.get("id").asText() : "";
                int status = row.has("status") ? row.get("status").asInt() : 0;
                if (id.isEmpty() || !ours.contains(id)) continue;
                if (status != 4 && status != 6) continue;          // 4=Transit, 6=Pending
                try { fyersClient.cancelOrder(id, auth); n++; }
                catch (Exception ignored) {}
            }
        } catch (Exception e) {
            log.warn("[ManualTerminal] cancelAll failed: {}", e.getMessage());
        }
        return n;
    }

    // ── WS fill callback ──────────────────────────────────────────────────────

    private void onFill(String orderId, double price) {
        // First, see if this is an entry fill we placed.
        Optional<ManualPosition> entry = store.findByOrderId(orderId);
        if (entry.isPresent()) {
            ManualPosition p = entry.get();
            p.avgPrice = price;
            p.filled   = true;
            store.persistAfterFill();
            log.info("[ManualTerminal] entry fill {} @ {}", orderId, price);
            return;
        }
        // Otherwise check whether it's a close fill we mapped.
        Optional<String> entryId = store.findEntryForClose(orderId);
        if (entryId.isPresent()) {
            Optional<ManualPosition> parent = store.findByOrderId(entryId.get());
            if (parent.isEmpty()) return;
            ManualPosition p = parent.get();
            store.completeClose(p, orderId, price, System.currentTimeMillis());
            unsubscribeIfUnused(p.symbol);
            log.info("[ManualTerminal] close fill {} @ {} parent={}", orderId, price, entryId.get());
            return;
        }
        // Not ours — ignore (bot fills land here too because there's one global listener).
    }

    private void unsubscribeIfUnused(String symbol) {
        for (ManualPosition p : store.openSnapshot()) {
            if (symbol.equals(p.symbol)) return; // still in use
        }
        try { marketDataService.unsubscribeAdditional(List.of(symbol)); }
        catch (Exception ignored) {}
    }

    // ── Boot reconcile + reject sweep ─────────────────────────────────────────

    /** Every 2 s: for each filled open position with an SL set, compare live LTP against the
     *  trigger and auto-close on breach. SELL → close when LTP ≥ SL (loss as premium rises);
     *  BUY → close when LTP ≤ SL (loss as premium falls). The {@code slTriggered} flag
     *  prevents double-firing while the close fill is in flight. */
    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void sweepStopLosses() {
        if (!tokenStore.isTokenAvailable()) return;
        for (ManualPosition p : store.openSnapshot()) {
            if (!p.filled || p.stopLossPts <= 0 || p.slTriggered || p.avgPrice <= 0) continue;
            double ltp;
            try { ltp = marketDataService.getLtp(p.symbol); }
            catch (Exception e) { continue; }
            if (ltp <= 0) continue;
            // SELL: loss when premium rises past entry + pts. BUY: loss when premium drops
            // past entry − pts.
            double trigger = "SELL".equalsIgnoreCase(p.side) ? (p.avgPrice + p.stopLossPts)
                                                              : (p.avgPrice - p.stopLossPts);
            boolean breach = "SELL".equalsIgnoreCase(p.side) ? (ltp >= trigger) : (ltp <= trigger);
            if (!breach) continue;
            log.warn("[ManualTerminal] SL breach {} ({}): ltp={} trigger={} side={} — auto-closing",
                p.orderId, p.symbol, ltp, trigger, p.side);
            eventService.log("[WARNING] [manual] SL breach " + p.symbol + " — ltp=" + ltp + " trigger=" + trigger);
            p.slTriggered = true;
            store.persistAfterFill();
            try { closePosition(p.orderId); }
            catch (Exception e) {
                log.warn("[ManualTerminal] SL auto-close failed for {}: {}", p.orderId, e.getMessage());
                p.slTriggered = false;
                store.persistAfterFill();
            }
        }
    }

    /** Every 30s: any open position with {@code filled=false} older than 30s gets checked
     *  against the live orderbook. If the broker shows it as rejected (status=5) or cancelled
     *  (status=1), archive it. Also catches the "Fyers accepted then rejected" edge case. */
    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void sweepPendingRejects() {
        if (!tokenStore.isTokenAvailable()) return;
        long now = System.currentTimeMillis();
        List<ManualPosition> pending = new ArrayList<>();
        for (ManualPosition p : store.openSnapshot()) {
            if (!p.filled && (now - p.openMillis) >= 30_000) pending.add(p);
        }
        if (pending.isEmpty()) return;
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getOrders(auth);
            if (root == null) return;
            JsonNode book = root.has("orderBook") ? root.get("orderBook") : root.get("data");
            if (book == null || !book.isArray()) return;
            for (ManualPosition p : pending) {
                int status = 0;
                for (JsonNode row : book) {
                    String id = row.has("id") ? row.get("id").asText() : "";
                    if (id.equals(p.orderId)) {
                        status = row.has("status") ? row.get("status").asInt() : 0;
                        break;
                    }
                }
                // 1=Cancelled, 5=Rejected
                if (status == 1 || status == 5) {
                    log.warn("[ManualTerminal] orphan position {} ({}) status={} — archiving",
                        p.orderId, p.symbol, status);
                    eventService.log("[WARNING] [manual] " + p.symbol + " never filled (status=" + status + ") — archived");
                    store.archive(p, "broker-status=" + status);
                    unsubscribeIfUnused(p.symbol);
                }
            }
        } catch (Exception e) {
            log.warn("[ManualTerminal] sweep failed: {}", e.getMessage());
        }
    }

    // ── Dashboard payload ─────────────────────────────────────────────────────

    public Map<String, Object> dashboard() {
        return dashboard(null, null);
    }

    /** Dashboard payload variant — when the operator has a CE / PE strike selected in the
     *  terminal dropdown, the frontend supplies the symbols on each poll so we can return
     *  live LTP + change + change% even for symbols that don't have an open position yet.
     *  Symbols are subscribed lazily on first sight. */
    public Map<String, Object> dashboard(String ceSymbol, String peSymbol) {
        // Lazy subscribe so the selected dropdown strikes get WS ticks. MarketDataService
        // de-duplicates already-subscribed symbols, so the overhead is just a Set add.
        List<String> toSub = new ArrayList<>();
        if (ceSymbol != null && !ceSymbol.isBlank()) toSub.add(ceSymbol);
        if (peSymbol != null && !peSymbol.isBlank()) toSub.add(peSymbol);
        if (!toSub.isEmpty()) {
            try { marketDataService.subscribeAdditional(toSub); }
            catch (Exception ignored) {}
        }

        double niftyLtp = 0, niftyChange = 0, niftyChangePct = 0;
        try {
            niftyLtp       = marketDataService.getDisplayLtp("NSE:NIFTY50-INDEX");
            niftyChange    = marketDataService.getDisplayChange("NSE:NIFTY50-INDEX");
            niftyChangePct = marketDataService.getDisplayChangePct("NSE:NIFTY50-INDEX");
            if (niftyLtp <= 0) niftyLtp = marketDataService.getLtp("NSE:NIFTY50-INDEX");
        } catch (Exception ignored) {}

        List<Map<String, Object>> open = new ArrayList<>();
        double totalMtm = 0;
        for (ManualPosition p : store.openSnapshot()) {
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); }
            catch (Exception ignored) {}
            double pnl = 0;
            if (p.filled && p.avgPrice > 0 && ltp > 0) {
                pnl = "BUY".equalsIgnoreCase(p.side)
                    ? (ltp - p.avgPrice) * p.qty
                    : (p.avgPrice - ltp) * p.qty;
            }
            totalMtm += pnl;
            double slTrigger = 0;
            if (p.stopLossPts > 0 && p.avgPrice > 0) {
                slTrigger = "SELL".equalsIgnoreCase(p.side) ? (p.avgPrice + p.stopLossPts)
                                                            : (p.avgPrice - p.stopLossPts);
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("orderId",     p.orderId);
            row.put("symbol",      p.symbol);
            row.put("side",        p.side);
            row.put("qty",         p.qty);
            row.put("avgPrice",    round2(p.avgPrice));
            row.put("ltp",         round2(ltp));
            row.put("pnl",         round2(pnl));
            row.put("filled",      p.filled);
            row.put("stopLossPts", round2(p.stopLossPts));
            row.put("slTrigger",   round2(slTrigger));
            open.add(row);
        }

        // Options Terminal is a per-trading-day view — recent trades, realised P&L and
        // running charges only count rows whose close happened today (IST). Any earlier
        // rows live in the store for the analytics layer but don't pollute the terminal
        // modal after a day rollover.
        long todayStartMs = startOfTodayMillisIst();
        List<Map<String, Object>> recent = new ArrayList<>();
        for (ManualClosedTrade t : store.recentSnapshot()) {
            if (t.closeMillis < todayStartMs) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("orderId",    t.orderId);
            row.put("symbol",     t.symbol);
            row.put("side",       t.side);
            row.put("qty",        t.qty);
            row.put("openPrice",  round2(t.openPrice));
            row.put("closePrice", round2(t.closePrice));
            row.put("pnl",        round2(t.pnl));
            row.put("openMillis", t.openMillis);
            row.put("closeMillis",t.closeMillis);
            row.put("note",       t.note == null ? "" : t.note);
            recent.add(row);
        }

        // Net P&L surfaced in the modal header and the minimized pill — live MTM on open
        // positions + TODAY'S realised P&L − today's running charges. Older days are
        // excluded so the terminal reads as a fresh slate each session.
        double realisedPnl = 0;
        for (ManualClosedTrade t : store.recentSnapshot()) {
            if (t.closeMillis >= todayStartMs) realisedPnl += t.pnl;
        }
        double charges = computeRunningCharges(todayStartMs);
        double netPnl  = totalMtm + realisedPnl - charges;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("niftyLtp",       round2(niftyLtp));
        out.put("niftyChange",    round2(niftyChange));
        out.put("niftyChangePct", round2(niftyChangePct));
        out.put("selectedCe",     quoteFor(ceSymbol));
        out.put("selectedPe",     quoteFor(peSymbol));
        out.put("openPositions",  open);
        out.put("totalMtm",       round2(totalMtm));
        out.put("realisedPnl",    round2(realisedPnl));
        out.put("charges",        round2(charges));
        out.put("netPnl",         round2(netPnl));
        out.put("recentTrades",   recent);
        return out;
    }

    /** Estimated all-in charges across every manual order placed in the current session —
     *  open positions count as 1 order each (entry only), closed trades count as 2 (entry +
     *  exit). Brokerage flat per order from {@link RiskSettingsStore}, statutory rates
     *  match {@code ShortStraddle.computeChargesBreakdown}. Used by the modal header /
     *  pill to show running Net P&L net of charges. */
    private double computeRunningCharges(long todayStartMs) {
        double brokerPer = riskSettings.getBrokeragePerOrder();
        double sellT = 0, buyT = 0;
        int    orders = 0;
        for (ManualPosition p : store.openSnapshot()) {
            if (!p.filled || p.avgPrice <= 0) continue;
            double t = p.avgPrice * p.qty;
            if ("SELL".equalsIgnoreCase(p.side)) sellT += t; else buyT += t;
            orders++;
        }
        for (ManualClosedTrade t : store.recentSnapshot()) {
            // Only today's closed trades belong in the running-charges sum so the modal
            // header doesn't carry yesterday's charges forward.
            if (t.closeMillis < todayStartMs) continue;
            double openT  = t.openPrice  * t.qty;
            double closeT = t.closePrice * t.qty;
            if ("SELL".equalsIgnoreCase(t.side)) { sellT += openT;  buyT  += closeT; }
            else                                  { buyT  += openT;  sellT += closeT; }
            orders += 2;
        }
        double totalT    = sellT + buyT;
        double brokerage = orders * brokerPer;
        double stt       = sellT * STT_SELL_PCT;
        double exchange  = totalT * EXCH_TXN_PCT;
        double sebi      = (totalT / 10_000_000.0) * SEBI_PER_CRORE;
        double stamp     = buyT  * STAMP_BUY_PCT;
        double gst       = (brokerage + exchange + sebi) * GST_PCT;
        return brokerage + stt + exchange + sebi + stamp + gst;
    }

    /** Live LTP + change + change% for a single symbol — used by the manual terminal's
     *  CE / PE LTP row. Returns an empty payload when the symbol isn't set. */
    private Map<String, Object> quoteFor(String symbol) {
        Map<String, Object> q = new LinkedHashMap<>();
        if (symbol == null || symbol.isBlank()) {
            q.put("symbol", "");
            q.put("ltp", 0.0); q.put("change", 0.0); q.put("changePct", 0.0);
            return q;
        }
        double ltp = 0, change = 0, changePct = 0;
        try {
            ltp       = marketDataService.getDisplayLtp(symbol);
            change    = marketDataService.getDisplayChange(symbol);
            changePct = marketDataService.getDisplayChangePct(symbol);
            if (ltp <= 0) ltp = marketDataService.getLtp(symbol);
        } catch (Exception ignored) {}
        q.put("symbol",    symbol);
        q.put("ltp",       round2(ltp));
        q.put("change",    round2(change));
        q.put("changePct", round2(changePct));
        return q;
    }

    /** Public accessor for the analytics layer — returns the same recent-trades snapshot the
     *  store exposes. Analytics filters by date range when summing for the "Adjustments"
     *  card and equity-curve points. */
    public Collection<ManualClosedTrade> recentTrades() {
        return store.recentSnapshot();
    }

    /** Live mark-to-market across every open manual position — sums (LTP − avgPrice) × qty
     *  for BUY legs and (avgPrice − LTP) × qty for SELL legs. Used by the analytics layer's
     *  "Today" view so the Include Adjustments checkbox folds in not just closed manual
     *  trades but also the running MTM of anything still open. Returns 0 when no positions
     *  are filled or when LTPs aren't available yet. */
    public double openPositionsLiveMtm() {
        double total = 0;
        for (ManualPosition p : store.openSnapshot()) {
            if (!p.filled || p.avgPrice <= 0) continue;
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); } catch (Exception ignored) {}
            if (ltp <= 0) continue;
            total += "BUY".equalsIgnoreCase(p.side)
                ? (ltp - p.avgPrice) * p.qty
                : (p.avgPrice - ltp) * p.qty;
        }
        return total;
    }

    /** Epoch millis at midnight IST today. Used by the terminal dashboard to scope its
     *  recent trades / realised P&L / running charges to the current trading day only. */
    private static long startOfTodayMillisIst() {
        ZoneId ist = ZoneId.of("Asia/Kolkata");
        return ZonedDateTime.of(LocalDate.now(ist).atStartOfDay(), ist).toInstant().toEpochMilli();
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
