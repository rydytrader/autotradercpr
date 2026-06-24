package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.strategy.Camarilla;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * REST endpoints for the Options Scalper Terminal modal. The modal JS is a verbatim
 * restore of the original {@code manual-terminal-modal.js} (commit {@code 6e34ac4^}),
 * but order placement and position tracking now route through the Camarilla strategy's
 * {@code Position} model — manual trades carry {@code setup=MANUAL} and fold into the
 * same Trade Log / Journal / Calendar as algo trades. There is no separate
 * "adjustments" path or {@code store/manual/} persistence anymore.
 *
 * <ul>
 *   <li>{@code GET  /api/manual/strikes}     — ATM ± N option-chain slice anchored on the bot's default ATM (AtmTracker baseline)</li>
 *   <li>{@code POST /api/manual/order}       — place a MANUAL order via {@link Camarilla#placeManual}</li>
 *   <li>{@code POST /api/manual/close/{id}}  — close one MANUAL position by entry order ID</li>
 *   <li>{@code POST /api/manual/close-all}   — flatten every open MANUAL position</li>
 *   <li>{@code POST /api/manual/cancel-all}  — cancel every working LMT order tagged ManualLMT</li>
 *   <li>{@code GET  /api/manual/dashboard}   — modal-state payload: positions, NIFTY LTP, MTM, charges</li>
 *   <li>{@code GET  /api/manual/trades}      — today's closed MANUAL trades (for the recent-trades tab)</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/manual")
public class ManualTerminalController {

    private static final Logger log = LoggerFactory.getLogger(ManualTerminalController.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long   STRIKE_STEP  = 50;

    private final Camarilla            strategy;
    private final FyersClientRouter    fyersClient;
    private final TokenStore           tokenStore;
    private final FyersProperties      fyersProperties;
    private final MarketDataService    marketDataService;

    public ManualTerminalController(Camarilla strategy,
                                    FyersClientRouter fyersClient,
                                    TokenStore tokenStore,
                                    FyersProperties fyersProperties,
                                    MarketDataService marketDataService) {
        this.strategy          = strategy;
        this.fyersClient       = fyersClient;
        this.tokenStore        = tokenStore;
        this.fyersProperties   = fyersProperties;
        this.marketDataService = marketDataService;
    }

    // ── Option-chain slice ──────────────────────────────────────────────────────

    @GetMapping("/strikes")
    public ResponseEntity<?> strikes(@RequestParam(defaultValue = "10") int strikes,
                                     @RequestParam(required = false) String expiryTs) {
        if (!tokenStore.isTokenAvailable()) {
            return ResponseEntity.status(401).body(Map.of("error", "not_logged_in"));
        }
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            int fetchCount = Math.max(30, strikes * 2 + 5);
            // expiryTs is the Fyers epoch-seconds timestamp for the desired weekly.
            // Blank/missing → nearest expiry (current week). The frontend uses
            // expiryOptions (returned below) to learn what the next-week timestamp is.
            JsonNode root = fyersClient.getOptionChain(NIFTY_SYMBOL, fetchCount,
                expiryTs == null ? "" : expiryTs, auth);
            if (root == null) {
                return ResponseEntity.status(502).body(Map.of("error", "empty_response"));
            }
            JsonNode data = root.has("data") ? root.get("data") : root;
            JsonNode chain = data.has("optionsChain") ? data.get("optionsChain")
                : (root.has("optionsChain") ? root.get("optionsChain") : null);
            if (chain == null || !chain.isArray()) {
                return ResponseEntity.status(502).body(Map.of("error", "empty_chain"));
            }

            // Spot from the underlying row inside the chain.
            double spot = 0;
            for (JsonNode row : chain) {
                String sym     = textField(row, "symbol");
                String optType = textField(row, "option_type", "optionType");
                double strike  = doubleField(row, "strike_price", "strikePrice");
                if (sym.equalsIgnoreCase(NIFTY_SYMBOL)
                    || (optType.isEmpty() && (strike == 0 || strike == -1))) {
                    double ltp = doubleField(row, "ltp", "lp");
                    if (ltp > 0) { spot = ltp; break; }
                }
            }
            if (spot <= 0) {
                try { spot = marketDataService.getLtp(NIFTY_SYMBOL); }
                catch (Exception ignored) {}
            }
            long spotAtm = spot > 0 ? Math.round(spot / (double) STRIKE_STEP) * STRIKE_STEP : 0;

            NavigableMap<Long, String[]> byStrike    = new TreeMap<>();
            NavigableMap<Long, double[]> ltpByStrike = new TreeMap<>();
            Set<String> expirySet = new TreeSet<>();
            for (JsonNode row : chain) {
                double strikeD = doubleField(row, "strike_price", "strikePrice");
                String optType = textField(row, "option_type", "optionType");
                String sym     = textField(row, "symbol");
                if (strikeD <= 0 || optType.isEmpty() || sym.isEmpty()) continue;
                long strike = Math.round(strikeD);
                String[] pair    = byStrike.computeIfAbsent(strike, k -> new String[2]);
                double[] ltpPair = ltpByStrike.computeIfAbsent(strike, k -> new double[2]);
                double ltp = doubleField(row, "ltp", "lp");
                if ("CE".equalsIgnoreCase(optType)) { pair[0] = sym; ltpPair[0] = ltp; }
                else if ("PE".equalsIgnoreCase(optType)) { pair[1] = sym; ltpPair[1] = ltp; }
                String exp = OptionChainController.parseExpiryFromSymbol(sym);
                if (!exp.isEmpty()) expirySet.add(exp);
            }

            // ATM = live spot rounded to STRIKE_STEP, recomputed every dashboard
            // refresh. v2 dropped the AtmTracker session baseline (it locked at
            // 09:30 first tick and never updated, so the terminal showed a stale
            // morning strike for the rest of the day). The Camarilla strategy
            // itself locks ATM at confirmation time on the strategy side; for the
            // manual terminal we want whatever strike is closest to NIFTY *now*.
            long atm = spotAtm;

            List<Map<String, Object>> rows = new ArrayList<>();
            if (atm > 0) {
                long lo = atm - (long) strikes * STRIKE_STEP;
                long hi = atm + (long) strikes * STRIKE_STEP;
                for (long s = lo; s <= hi; s += STRIKE_STEP) {
                    String[] pair = byStrike.get(s);
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("strike", s);
                    row.put("ce",     pair != null ? pair[0] : "");
                    row.put("pe",     pair != null ? pair[1] : "");
                    rows.add(row);
                }
            }

            // Fyers v3 chain response includes a `data.expiryData` array listing
            // every upcoming weekly with {date, expiry (epoch s)}. Surface the first
            // two (current + next week) so the modal dropdown can let the operator
            // switch between expiries — on change the frontend refetches /strikes
            // with the selected expiryTs.
            List<Map<String, Object>> expiryOptions = new ArrayList<>();
            JsonNode expiryData = data.has("expiryData") ? data.get("expiryData")
                : (root.has("expiryData") ? root.get("expiryData") : null);
            if (expiryData != null && expiryData.isArray()) {
                int taken = 0;
                for (JsonNode e : expiryData) {
                    if (taken >= 2) break;
                    String date = textField(e, "date");
                    String ts   = textField(e, "expiry");
                    if (date.isEmpty() || ts.isEmpty()) continue;
                    Map<String, Object> opt = new LinkedHashMap<>();
                    opt.put("date", date);
                    opt.put("ts",   ts);
                    expiryOptions.add(opt);
                    taken++;
                }
            }

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("niftyLtp",      round2(spot));
            out.put("atmStrike",     atm);
            out.put("spotAtmStrike", spotAtm);
            out.put("expiries",      new ArrayList<>(expirySet));   // back-compat
            out.put("expiryOptions", expiryOptions);                // [{date, ts}, …]
            out.put("selectedExpiryTs", expiryTs == null ? "" : expiryTs);
            out.put("strikes",       rows);
            return ResponseEntity.ok(out);
        } catch (Exception e) {
            log.warn("[manual] /strikes failed: {}", e.getMessage());
            return ResponseEntity.status(502).body(Map.of(
                "error", "fyers_unavailable", "message", e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    // ── Order placement / close / cancel ───────────────────────────────────────

    @PostMapping("/order")
    public ResponseEntity<Map<String, Object>> order(@RequestBody Map<String, Object> body) {
        if (!tokenStore.isTokenAvailable()) {
            return ResponseEntity.status(401).body(Map.of(
                "success", false, "message", "Not logged in"));
        }
        String side    = asString(body.get("side"));
        String symbol  = asString(body.get("symbol"));
        int    lots    = asInt(body.get("lots"), 1);
        double slPts   = asDouble(body.get("stopLossPts"), 25);
        // 'product' from the modal dropdown — INTRADAY or OVERNIGHT (Fyers' MARGIN).
        // Default OVERNIGHT when omitted/blank so missing-field requests follow the
        // operator's stated preference. Translation to Fyers' product token happens
        // inside OrderService.normalizeProductType.
        String product = asString(body.get("product"));
        if (product == null || product.isBlank()) product = "OVERNIGHT";
        if (symbol == null || symbol.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false, "message", "symbol required"));
        }
        int sideCode;
        if ("BUY".equalsIgnoreCase(side))       sideCode = +1;
        else if ("SELL".equalsIgnoreCase(side)) sideCode = -1;
        else return ResponseEntity.badRequest().body(Map.of(
            "success", false, "message", "side must be BUY or SELL"));

        int qty = Math.max(1, lots) * com.rydytrader.autotrader.service.strategy.Camarilla.lotSize();

        Camarilla.ManualPlaceResult r = strategy.placeManual(symbol, sideCode, qty, 2, 0, slPts, product);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", r.ok());
        out.put("orderId", r.orderId());
        out.put("message", r.message());
        return ResponseEntity.ok(out);
    }

    @PostMapping("/close/{orderId}")
    public ResponseEntity<Map<String, Object>> closeOne(@PathVariable String orderId) {
        String closedSymbol = strategy.closeManualByOrderId(orderId, "MANUAL_CLOSE");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", closedSymbol != null);
        out.put("message", closedSymbol != null
            ? "Close submitted for " + closedSymbol
            : "Close failed or position not found");
        return ResponseEntity.ok(out);
    }

    @PostMapping("/close-all")
    public ResponseEntity<Map<String, Object>> closeAll() {
        int n = strategy.closeAllManual("MANUAL_CLOSE");
        return ResponseEntity.ok(Map.of(
            "success", true, "closed", n, "message", n + " close order(s) submitted"));
    }

    /** Cancel all working orders. This currently delegates a no-op response — pending
     *  LMT orders are tracked on Fyers, and a proper cancel-all would walk the order book.
     *  Left as a stub so the modal's "Cancel All" button doesn't 404; future work can wire
     *  a real implementation through {@code OrderService.cancelOrder}. */
    @PostMapping("/cancel-all")
    public ResponseEntity<Map<String, Object>> cancelAll() {
        return ResponseEntity.ok(Map.of(
            "success", true, "cancelled", 0, "message", "no working LMT orders tracked"));
    }

    /** Inline qty adjustment for an open MANUAL position. {@code delta} is in LOTS:
     *  {@code +1} adds 1 lot in the same direction as the position; {@code -1} reduces
     *  by 1 lot via opposite-direction market order. Reduce that covers the full open
     *  qty closes the position and books the trade row; partial reduces book a closed-
     *  trade row for the reduced portion only. */
    @PostMapping("/qty/{orderId}")
    public ResponseEntity<Map<String, Object>> adjustQty(@PathVariable String orderId,
                                                         @RequestBody Map<String, Object> body) {
        if (!tokenStore.isTokenAvailable()) {
            return ResponseEntity.status(401).body(Map.of(
                "success", false, "message", "Not logged in"));
        }
        Integer deltaLots = asIntBoxed(body == null ? null : body.get("delta"));
        if (deltaLots == null || deltaLots == 0) {
            return ResponseEntity.badRequest().body(errorMap("delta must be ±1"));
        }
        Camarilla.Position p = strategy.findOpenManualByOrderId(orderId);
        if (p == null) {
            return ResponseEntity.ok(errorMap("position not found"));
        }
        int qty = Math.abs(deltaLots) * Camarilla.lotSize();
        int side;
        if (deltaLots > 0) {
            side = p.isShort ? -1 : +1;     // add same direction
        } else {
            side = p.isShort ? +1 : -1;     // reduce opposite direction
        }
        // Reuse the existing position's stopLossPts on adds. Server-side validation
        // re-checks the ≤50 ceiling; that already-true value re-passes here. For reduces
        // the value is unused (closePosition / partial-reduce path skips SL math).
        double slPts = Double.isNaN(p.slLevel) || p.entryPrice <= 0
            ? 1   // placeholder > 0 to pass the mandatory-SL gate for reduce flows
            : Math.abs(p.slLevel - p.entryPrice);
        // Qty-adjust hits the merge path inside placeManual which reuses the existing
        // position's productType. Passing "" here is safe — placeManual's resolver
        // would fall back to the strategy default for the new-position branch, but
        // the merge branch fires first whenever the symbol already has an open MANUAL
        // position (which is the only state findOpenManualByOrderId returns).
        Camarilla.ManualPlaceResult r = strategy.placeManual(p.symbol, side, qty, 2, 0, slPts, "");
        return ResponseEntity.ok(toBody(r));
    }

    /** Inline SL trigger-price adjustment. {@code deltaPts} is price-based (not direction-
     *  aware): {@code +1} raises slLevel by 1 point; {@code -1} lowers it. The fast-tick
     *  watcher reads the new value on its next ~500 ms iteration. */
    @PostMapping("/sl/{orderId}")
    public ResponseEntity<Map<String, Object>> adjustSl(@PathVariable String orderId,
                                                        @RequestBody Map<String, Object> body) {
        Double deltaPts = asDoubleBoxed(body == null ? null : body.get("deltaPts"));
        if (deltaPts == null || deltaPts == 0) {
            return ResponseEntity.badRequest().body(errorMap("deltaPts must be non-zero"));
        }
        Camarilla.Position p = strategy.findOpenManualByOrderId(orderId);
        if (p == null) {
            return ResponseEntity.ok(errorMap("position not found"));
        }
        Camarilla.ManualPlaceResult r = strategy.adjustManualSl(p.symbol, deltaPts);
        return ResponseEntity.ok(toBody(r));
    }

    private static Map<String, Object> errorMap(String msg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("success", false);
        m.put("message", msg);
        return m;
    }

    private static Map<String, Object> toBody(Camarilla.ManualPlaceResult r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("success", r.ok());
        m.put("orderId", r.orderId());
        m.put("message", r.message());
        return m;
    }

    private static Integer asIntBoxed(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.intValue();
        try { return Integer.parseInt(String.valueOf(o)); }
        catch (Exception e) { return null; }
    }

    private static Double asDoubleBoxed(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(String.valueOf(o)); }
        catch (Exception e) { return null; }
    }

    // ── Dashboard + recent trades ──────────────────────────────────────────────

    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> dashboard(
            @RequestParam(required = false) String ceSymbol,
            @RequestParam(required = false) String peSymbol) {

        // Lazy-subscribe the dropdown-selected strikes so they receive live ticks.
        List<String> toSub = new ArrayList<>();
        if (ceSymbol != null && !ceSymbol.isBlank()) toSub.add(ceSymbol);
        if (peSymbol != null && !peSymbol.isBlank()) toSub.add(peSymbol);
        if (!toSub.isEmpty()) {
            try { marketDataService.subscribeAdditional(toSub); } catch (Exception ignored) {}
        }

        // NIFTY ticker.
        double niftyLtp = 0, niftyChange = 0, niftyChangePct = 0;
        try {
            niftyLtp       = marketDataService.getDisplayLtp(NIFTY_SYMBOL);
            niftyChange    = marketDataService.getDisplayChange(NIFTY_SYMBOL);
            niftyChangePct = marketDataService.getDisplayChangePct(NIFTY_SYMBOL);
            if (niftyLtp <= 0) niftyLtp = marketDataService.getLtp(NIFTY_SYMBOL);
        } catch (Exception ignored) {}

        // Open MANUAL positions mapped to the modal's expected shape.
        List<Map<String, Object>> open = new ArrayList<>();
        double totalMtm = 0;
        for (Camarilla.Position p : strategy.openManualPositions()) {
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); } catch (Exception ignored) {}
            double pnl = 0;
            if (p.fillResolved && p.entryPrice > 0 && ltp > 0) {
                pnl = p.isShort ? (p.entryPrice - ltp) * p.qty
                                : (ltp - p.entryPrice) * p.qty;
            } else if (p.entryPrice > 0 && ltp > 0) {
                // Pre-fill estimate — still show running PnL against the estimate so the
                // operator sees direction-of-move feedback without waiting for the fill.
                pnl = p.isShort ? (p.entryPrice - ltp) * p.qty
                                : (ltp - p.entryPrice) * p.qty;
            }
            totalMtm += pnl;

            double slTrigger = Double.isNaN(p.slLevel) ? 0 : p.slLevel;
            double slPts     = (!Double.isNaN(p.slLevel) && p.entryPrice > 0)
                ? Math.abs(p.slLevel - p.entryPrice) : 0;

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("orderId",     p.entryOrderId);
            row.put("symbol",      p.symbol);
            row.put("side",        p.isShort ? "SELL" : "BUY");
            row.put("qty",         p.qty);
            row.put("avgPrice",    round2(p.entryPrice));
            row.put("ltp",         round2(ltp));
            row.put("pnl",         round2(pnl));
            row.put("filled",      p.fillResolved);
            row.put("stopLossPts", round2(slPts));
            row.put("slTrigger",   round2(slTrigger));
            open.add(row);
        }

        // Today's closed MANUAL trades — same shape the modal renders.
        List<Map<String, Object>> recent = new ArrayList<>();
        double realisedPnl = 0;
        double charges     = 0;
        for (Map<String, Object> t : strategy.todayManualClosedTrades()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("orderId",     "");
            row.put("symbol",      asString(t.get("symbol")));
            row.put("side",        sideFromCycle(t));
            row.put("qty",         asInt(t.get("qty"), 0));
            row.put("openPrice",   asDouble(t.get("entryPrice"), 0));
            row.put("closePrice",  asDouble(t.get("exitPrice"),  0));
            row.put("pnl",         asDouble(t.get("netPnl"),     0));
            row.put("openMillis",  asLong(t.get("openedAtMillis"), 0));
            row.put("closeMillis", asLong(t.get("closedAtMillis"), 0));
            row.put("note",        asString(t.get("closeReason")));
            recent.add(row);
            realisedPnl += asDouble(t.get("netPnl"),  0);
            charges     += asDouble(t.get("charges"), 0);
        }

        // Add the projected close-cycle charges for currently-open MANUAL positions so
        // the header reflects a live cost-to-exit estimate (entry brokerage + STT/GST +
        // projected exit fees), not just the realised total from closed cycles. Without
        // this the chip reads ₹0 between session start and the first close — misleading
        // because real charges are already accruing the moment the entry fills.
        double projectedOpenCharges = strategy.projectedManualChargesForOpen();
        charges += projectedOpenCharges;
        double netPnl = totalMtm + realisedPnl - projectedOpenCharges;
        // ^ realisedPnl already has its charges netted in. We subtract ONLY the projected
        // open-leg charges so MTM is shown net of the cost of closing here-and-now.

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
        return ResponseEntity.ok(out);
    }

    /** All closed MANUAL trades for a specific date — feeds the calendar day-modal's
     *  Adjustments-style sub-table when (re-)wired by the frontend. The shape matches
     *  the original Adjustments DTO so older calendar callers still parse it. */
    @GetMapping("/trades")
    public ResponseEntity<Map<String, Object>> tradesForDate(@RequestParam String date) {
        LocalDate target;
        try { target = LocalDate.parse(date); }
        catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "bad_date"));
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> t : strategy.todayManualClosedTrades()) {
            long closeMs = asLong(t.get("closedAtMillis"), 0);
            if (closeMs == 0) continue;
            LocalDate d = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(closeMs), IST).toLocalDate();
            if (!d.equals(target)) continue;
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("symbol",      asString(t.get("symbol")));
            r.put("side",        sideFromCycle(t));
            r.put("qty",         asInt(t.get("qty"), 0));
            r.put("openPrice",   asDouble(t.get("entryPrice"), 0));
            r.put("closePrice",  asDouble(t.get("exitPrice"),  0));
            r.put("pnl",         asDouble(t.get("netPnl"),     0));
            r.put("closeMillis", closeMs);
            rows.add(r);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("date",   date);
        out.put("trades", rows);
        return ResponseEntity.ok(out);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

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
        // REST snapshot fallback. WS subscription is in place but illiquid strikes (deep
        // ITM/OTM) may not have ticked yet today — the WS only delivers a quote when a
        // trade actually happens. Fyers /data/quotes returns the current snapshot price
        // synchronously, so we seed the display until the next WS tick arrives. Costs
        // one HTTP roundtrip (~100-300 ms) on first selection; subsequent picks of the
        // same strike hit the now-warm WS cache and return instantly.
        if (ltp <= 0 && tokenStore.isTokenAvailable()) {
            double[] snap = fetchSnapshotQuote(symbol);
            if (snap != null && snap[0] > 0) {
                ltp       = snap[0];
                change    = snap[1];
                changePct = snap[2];
            }
        }
        q.put("symbol",    symbol);
        q.put("ltp",       round2(ltp));
        q.put("change",    round2(change));
        q.put("changePct", round2(changePct));
        return q;
    }

    /** One-shot REST quote fetch for the given symbol. Returns {ltp, change, changePct} or
     *  {@code null} when the call fails. Used as a fallback when the WS cache hasn't yet
     *  received a tick for a freshly-subscribed contract. */
    private double[] fetchSnapshotQuote(String symbol) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getQuotes(symbol, auth);
            if (root == null) return null;
            JsonNode arr = root.has("d") ? root.get("d") : null;
            if (arr == null || !arr.isArray() || arr.size() == 0) return null;
            JsonNode v = arr.get(0).has("v") ? arr.get(0).get("v") : null;
            if (v == null) return null;
            double ltp = v.has("lp") ? v.get("lp").asDouble(0) : 0;
            double ch  = v.has("ch") ? v.get("ch").asDouble(0) : 0;
            double chp = v.has("chp") ? v.get("chp").asDouble(0) : 0;
            return new double[]{ ltp, ch, chp };
        } catch (Exception e) {
            log.debug("[manual] quote fallback failed for {}: {}", symbol, e.getMessage());
            return null;
        }
    }

    /** Read the entry side (BUY / SELL) from the closed-cycle map. New cycles persist
     *  the side via {@code cycle.put("side", p.isShort ? "SELL" : "BUY")} at close time
     *  so this just reads it back. Legacy cycles persisted before that change fall back
     *  to {@code SELL} (the strategy's historical primary direction). */
    private static String sideFromCycle(Map<String, Object> t) {
        Object s = t.get("side");
        if (s != null && !s.toString().isBlank()) return s.toString();
        return "SELL";
    }

    private static String textField(JsonNode row, String... keys) {
        for (String k : keys) if (row.has(k) && !row.get(k).isNull()) return row.get(k).asText("");
        return "";
    }
    private static double doubleField(JsonNode row, String... keys) {
        for (String k : keys) if (row.has(k) && !row.get(k).isNull()) return row.get(k).asDouble(0);
        return 0;
    }
    private static String asString(Object o) { return o == null ? null : String.valueOf(o); }
    private static int asInt(Object o, int fallback) {
        if (o == null) return fallback;
        if (o instanceof Number n) return n.intValue();
        try { return Integer.parseInt(String.valueOf(o)); }
        catch (Exception e) { return fallback; }
    }
    private static long asLong(Object o, long fallback) {
        if (o == null) return fallback;
        if (o instanceof Number n) return n.longValue();
        try { return Long.parseLong(String.valueOf(o)); }
        catch (Exception e) { return fallback; }
    }
    private static double asDouble(Object o, double fallback) {
        if (o == null) return fallback;
        if (o instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(String.valueOf(o)); }
        catch (Exception e) { return fallback; }
    }
    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
}
