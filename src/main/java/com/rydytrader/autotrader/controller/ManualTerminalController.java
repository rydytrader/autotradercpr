package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.service.ManualTerminalService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.strategy.ShortStraddle;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * REST endpoints for the manual options terminal. Strike resolution reuses the existing
 * Fyers option-chain plumbing; orders go through {@link ManualTerminalService}.
 */
@RestController
@RequestMapping("/api/manual")
public class ManualTerminalController {

    private static final Logger log = LoggerFactory.getLogger(ManualTerminalController.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_STEP  = 50;

    private final ManualTerminalService service;
    private final FyersClientRouter     fyersClient;
    private final TokenStore            tokenStore;
    private final FyersProperties       fyersProperties;
    private final MarketDataService     marketDataService;

    public ManualTerminalController(ManualTerminalService service,
                                    FyersClientRouter fyersClient,
                                    TokenStore tokenStore,
                                    FyersProperties fyersProperties,
                                    MarketDataService marketDataService) {
        this.service           = service;
        this.fyersClient       = fyersClient;
        this.tokenStore        = tokenStore;
        this.fyersProperties   = fyersProperties;
        this.marketDataService = marketDataService;
    }

    @GetMapping("/strikes")
    public ResponseEntity<?> strikes(@RequestParam(defaultValue = "10") int strikes) {
        if (!tokenStore.isTokenAvailable()) {
            return ResponseEntity.status(401).body(Map.of("error", "not_logged_in"));
        }
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            int fetchCount = Math.max(30, strikes * 2 + 5);
            JsonNode root = fyersClient.getOptionChain(NIFTY_SYMBOL, fetchCount, auth);
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
            long atm = spot > 0 ? Math.round(spot / (double) STRIKE_STEP) * STRIKE_STEP : 0;

            // Group rows by strike for the slice, dedupe expiries, capture the symbol for
            // each strike's CE and PE leg.
            NavigableMap<Long, String[]> byStrike = new TreeMap<>(); // strike -> [ceSym, peSym]
            Set<String> expirySet = new TreeSet<>();
            for (JsonNode row : chain) {
                double strikeD = doubleField(row, "strike_price", "strikePrice");
                String optType = textField(row, "option_type", "optionType");
                String sym     = textField(row, "symbol");
                if (strikeD <= 0 || optType.isEmpty() || sym.isEmpty()) continue;
                long strike = Math.round(strikeD);
                String[] pair = byStrike.computeIfAbsent(strike, k -> new String[2]);
                if ("CE".equalsIgnoreCase(optType)) pair[0] = sym;
                else if ("PE".equalsIgnoreCase(optType)) pair[1] = sym;
                String exp = ShortStraddle.parseExpiryFromSymbol(sym);
                if (!exp.isEmpty()) expirySet.add(exp);
            }

            // ATM ± N slice
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

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("niftyLtp",  round2(spot));
            out.put("atmStrike", atm);
            out.put("expiries",  new ArrayList<>(expirySet));
            out.put("strikes",   rows);
            return ResponseEntity.ok(out);
        } catch (Exception e) {
            log.warn("[manual] /strikes failed: {}", e.getMessage());
            return ResponseEntity.status(502).body(Map.of(
                "error", "fyers_unavailable", "message", e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    @PostMapping("/order")
    public ResponseEntity<Map<String, Object>> order(@RequestBody Map<String, Object> body) {
        if (!tokenStore.isTokenAvailable()) {
            return ResponseEntity.status(401).body(Map.of(
                "success", false, "message", "Not logged in"));
        }
        String side    = asString(body.get("side"));
        String symbol  = asString(body.get("symbol"));
        int    lots    = asInt(body.get("lots"), 1);
        String product = asString(body.get("product"));
        double slPts   = asDouble(body.get("stopLossPts"), 50);
        if (product == null || product.isBlank()) product = "INTRADAY";
        if (symbol == null || symbol.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false, "message", "symbol required"));
        }
        if (!"BUY".equalsIgnoreCase(side) && !"SELL".equalsIgnoreCase(side)) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false, "message", "side must be BUY or SELL"));
        }
        ManualTerminalService.PlaceResult result = service.placeOrder(symbol, lots, side, product, slPts);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", result.success());
        out.put("orderId", result.orderId());
        out.put("message", result.message());
        return ResponseEntity.ok(out);
    }

    @PostMapping("/close/{orderId}")
    public ResponseEntity<Map<String, Object>> closeOne(@PathVariable String orderId) {
        String closeId = service.closePosition(orderId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", closeId != null);
        out.put("message", closeId != null
            ? "Close submitted (closeId=" + closeId + ")"
            : "Close failed or position not found");
        return ResponseEntity.ok(out);
    }

    @PostMapping("/close-all")
    public ResponseEntity<Map<String, Object>> closeAll() {
        int n = service.closeAll();
        return ResponseEntity.ok(Map.of(
            "success", true, "closed", n, "message", n + " close order(s) submitted"));
    }

    @PostMapping("/cancel-all")
    public ResponseEntity<Map<String, Object>> cancelAll() {
        int n = service.cancelAll();
        return ResponseEntity.ok(Map.of(
            "success", true, "cancelled", n, "message", n + " working order(s) cancelled"));
    }

    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> dashboard(
            @RequestParam(required = false) String ceSymbol,
            @RequestParam(required = false) String peSymbol) {
        return ResponseEntity.ok(service.dashboard(ceSymbol, peSymbol));
    }

    /** All closed manual trades for a specific date — feeds the calendar day-modal's
     *  Adjustments sub-table. Returns symbol / side / qty / openPrice / closePrice / pnl. */
    @GetMapping("/trades")
    public ResponseEntity<Map<String, Object>> tradesForDate(@RequestParam String date) {
        java.time.LocalDate target;
        try { target = java.time.LocalDate.parse(date); }
        catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "bad_date"));
        }
        java.time.ZoneId IST = java.time.ZoneId.of("Asia/Kolkata");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (com.rydytrader.autotrader.store.manual.ManualClosedTrade t : service.recentTrades()) {
            java.time.LocalDate d = java.time.Instant.ofEpochMilli(t.closeMillis).atZone(IST).toLocalDate();
            if (!d.equals(target)) continue;
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("symbol",     t.symbol);
            r.put("side",       t.side);
            r.put("qty",        t.qty);
            r.put("openPrice",  t.openPrice);
            r.put("closePrice", t.closePrice);
            r.put("pnl",        t.pnl);
            r.put("closeMillis", t.closeMillis);
            rows.add(r);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("date",  date);
        out.put("trades", rows);
        return ResponseEntity.ok(out);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

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
        try { return Integer.parseInt(String.valueOf(o)); }
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
