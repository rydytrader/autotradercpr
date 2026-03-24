package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final FyersClientRouter   fyersClient;
    private final SymbolMasterService symbolMaster;

    public OrderService(TokenStore tokenStore,
                        FyersProperties fyersProperties,
                        FyersClientRouter fyersClient,
                        SymbolMasterService symbolMaster) {
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
        this.fyersClient     = fyersClient;
        this.symbolMaster    = symbolMaster;
    }

    // ── ENTRY ORDER (Market) ──────────────────────────────────────────────────
    public OrderDTO placeOrder(String symbol, int qty, int side, double stoploss) {
        try {
            String json = buildOrderJson(symbol, qty, side, 2, 0, 0, "AutoTrader");
            System.out.println("Entry Order: " + json);
            return postOrder(json);
        } catch (Exception e) { e.printStackTrace(); return null; }
    }

    // ── STOP LOSS ORDER (Stop-Market) ─────────────────────────────────────────
    public OrderDTO placeStopLoss(String symbol, int qty, int side, double slPrice) {
        try {
            if (Double.isNaN(slPrice) || slPrice <= 0) {
                System.err.println("[OrderService] Invalid SL price " + slPrice + " for " + symbol + " — skipping order");
                return null;
            }
            double rounded = roundToTick(slPrice, symbol);
            String json = buildOrderJson(symbol, qty, side, 3, 0, rounded, "AutoSL");
            System.out.println("SL Order: " + json);
            return postOrder(json);
        } catch (Exception e) { e.printStackTrace(); return null; }
    }

    // ── TARGET ORDER (Limit) ──────────────────────────────────────────────────
    public OrderDTO placeTarget(String symbol, int qty, int side, double targetPrice) {
        try {
            if (Double.isNaN(targetPrice) || targetPrice <= 0) {
                System.err.println("[OrderService] Invalid target price " + targetPrice + " for " + symbol + " — skipping order");
                return null;
            }
            double rounded = roundToTick(targetPrice, symbol);
            String json = buildOrderJson(symbol, qty, side, 1, rounded, 0, "AutoTarget");
            System.out.println("Target Order: " + json);
            return postOrder(json);
        } catch (Exception e) { e.printStackTrace(); return null; }
    }

    // ── EXIT MARKET ORDER (Square-off) ────────────────────────────────────────
    public OrderDTO placeExitOrder(String symbol, int qty, int side) {
        try {
            String json = buildOrderJson(symbol, qty, side, 2, 0, 0, "SquareOff");
            System.out.println("Exit Order: " + json);
            return postOrder(json);
        } catch (Exception e) { e.printStackTrace(); return null; }
    }

    // ── CANCEL ORDER ──────────────────────────────────────────────────────────
    // Cancels all pending/transit orders by fetching current order book
    // Fyers statuses: 1=Cancelled, 2=Filled, 4=Transit, 5=Rejected, 6=Pending
    public void cancelAllPendingOrders(String symbol) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getOrder(null, auth); // null = fetch all orders
            if (node != null && node.has("orderBook")) {
                int cancelled = 0;
                for (JsonNode o : node.get("orderBook")) {
                    int status = o.has("status") ? o.get("status").asInt() : 0;
                    String orderSymbol = o.has("symbol") ? o.get("symbol").asText() : "";
                    if (orderSymbol.equals(symbol) && (status == 6 || status == 4)) {
                        String orderId = o.get("id").asText();
                        int orderType = o.has("type") ? o.get("type").asInt() : 0;
                        System.out.println("[CancelAll] Cancelling order " + orderId
                            + " | symbol=" + orderSymbol + " | status=" + status + " | type=" + orderType);
                        cancelOrder(orderId);
                        cancelled++;
                    }
                }
                if (cancelled == 0) {
                    System.out.println("[CancelAll] No pending/transit orders found for " + symbol
                        + " — dumping order book for symbol:");
                    for (JsonNode o : node.get("orderBook")) {
                        String orderSymbol = o.has("symbol") ? o.get("symbol").asText() : "";
                        if (orderSymbol.equals(symbol)) {
                            int status = o.has("status") ? o.get("status").asInt() : 0;
                            System.out.println("  order=" + o.get("id").asText() + " status=" + status
                                + " type=" + (o.has("type") ? o.get("type").asInt() : "?"));
                        }
                    }
                } else {
                    System.out.println("[CancelAll] Cancelled " + cancelled + " order(s) for " + symbol);
                }
            } else if (node != null && node.has("code") && node.get("code").asInt() == -429) {
                System.out.println("[CancelAll] Fyers rate limit hit for " + symbol + " — will retry on next sync");
            } else {
                System.out.println("[CancelAll] No orderBook in response for " + symbol);
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    private volatile long lastCancelTime = 0;
    private static final long CANCEL_THROTTLE_MS = 1000; // 1 second between cancel calls

    public boolean cancelOrder(String orderId) {
        try {
            long now = System.currentTimeMillis();
            long wait = CANCEL_THROTTLE_MS - (now - lastCancelTime);
            if (wait > 0) {
                try { Thread.sleep(wait); } catch (InterruptedException ignored) {}
            }
            lastCancelTime = System.currentTimeMillis();
            JsonNode node = fyersClient.cancelOrder(orderId, authHeader());
            boolean ok = node != null && node.get("s") != null && "ok".equals(node.get("s").asText());
            int code = node != null && node.has("code") ? node.get("code").asInt() : 0;
            if (ok || code == -52) {
                System.out.println("Cancel " + orderId + " → " + (ok ? "OK" : "already cancelled/filled"));
                return true;
            } else {
                System.out.println("Cancel " + orderId + " → FAILED | response: " + node);
            }
            return ok;
        } catch (Exception e) { e.printStackTrace(); return false; }
    }

    // ── TRADEBOOK CACHE ───────────────────────────────────────────────────────
    private volatile JsonNode cachedTradebook = null;
    private volatile long     tradebookFetchTime = 0;
    private static final long TRADEBOOK_CACHE_MS = 5000; // 5 seconds

    private JsonNode getCachedTradebook() {
        long now = System.currentTimeMillis();
        if (cachedTradebook != null && (now - tradebookFetchTime) < TRADEBOOK_CACHE_MS) {
            return cachedTradebook;
        }
        try {
            JsonNode root = getCachedTradebook();
            if (root != null && "ok".equals(root.get("s").asText())) {
                cachedTradebook = root;
                tradebookFetchTime = now;
                return root;
            }
            // Rate limit — back off and return stale cache
            if (root != null && root.has("code") && root.get("code").asInt() == -429) {
                tradebookFetchTime = now + 30000;
                return cachedTradebook;
            }
        } catch (Exception e) { e.printStackTrace(); }
        return cachedTradebook; // return stale cache on any error
    }

    // ── NET DAY P&L (from tradebook) ──────────────────────────────────────────
    public double getNetDayPnl() {
        try {
            JsonNode root = getCachedTradebook();
            if (!"ok".equals(root.get("s").asText())) return 0;
            double netPnl = 0;
            for (JsonNode trade : root.get("tradeBook")) {
                double val = trade.get("tradePrice").asDouble() * trade.get("tradedQty").asInt();
                netPnl += trade.get("side").asInt() == 1 ? -val : val;
            }
            return netPnl;
        } catch (Exception e) { e.printStackTrace(); return 0; }
    }

    // ── TRADEBOOK FILL PRICE ──────────────────────────────────────────────────
    public double getFilledPriceFromTradebook(String symbol, int side) {
        try {
            JsonNode root = getCachedTradebook();
            if (!"ok".equals(root.get("s").asText())) return 0;
            JsonNode tradeBook = root.get("tradeBook");
            // Walk in reverse (newest first), skip ManualSquareOff trades for entry lookup
            for (int i = tradeBook.size() - 1; i >= 0; i--) {
                JsonNode t = tradeBook.get(i);
                if (t.get("symbol").asText().equals(symbol)
                        && t.get("side").asInt() == side
                        && !"ManualSquareOff".equals(t.has("orderTag") ? t.get("orderTag").asText() : "")) {
                    return t.get("tradePrice").asDouble();
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
        return 0;
    }

    /** Look up fill price by exact order ID — unambiguous even with multiple trades on same symbol. */
    public double getFilledPriceByOrderId(String orderId) {
        try {
            JsonNode root = getCachedTradebook();
            if (!"ok".equals(root.get("s").asText())) return 0;
            for (JsonNode t : root.get("tradeBook")) {
                String tid = t.has("orderNumber") ? t.get("orderNumber").asText()
                           : t.has("orderId") ? t.get("orderId").asText() : "";
                if (orderId.equals(tid)) {
                    return t.get("tradePrice").asDouble();
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
        return 0;
    }

    public double getExitPriceFromTradebook(String symbol) {
        return getExitPriceFromTradebook(symbol, null);
    }

    /**
     * Looks up exit price from tradebook.
     * positionSide = "LONG" or "SHORT" — used to target the opposite-side exit trade.
     */
    public double getExitPriceFromTradebook(String symbol, String positionSide) {
        try {
            JsonNode root = getCachedTradebook();
            if (!"ok".equals(root.get("s").asText())) return 0;
            JsonNode tradeBook = root.get("tradeBook");

            // Prefer ManualSquareOff trade as exit price from UI
            for (int i = tradeBook.size() - 1; i >= 0; i--) {
                JsonNode t = tradeBook.get(i);
                if (t.get("symbol").asText().equals(symbol)
                        && "ManualSquareOff".equals(t.has("orderTag") ? t.get("orderTag").asText() : "")) {

                    System.out.println("Exit price calculated from ManualSquareOff Tag " + t.get("tradePrice").asDouble());
                    return t.get("tradePrice").asDouble();
                }
            }
            // Use known position side to find the exit on the opposite side
            if (positionSide != null) {
                int exitSide = "LONG".equals(positionSide) ? -1 : 1;  // LONG closed by SELL(-1), SHORT by BUY(1)
                double price = getFilledPriceFromTradebook(symbol, exitSide);
                System.out.println("Exit price calculated from SIDE LOGIC  ( LONG closed by SELL(-1), SHORT by BUY(1) )  logic " + price);
                if (price > 0) return price;
            }
            // Final fallback: try both sides
            double price = getFilledPriceFromTradebook(symbol, -1);
            if (price <= 0) price = getFilledPriceFromTradebook(symbol, 1);
            System.out.println("Exit price calculated from Final fallback: try both sides " + price);
            return price;
        } catch (Exception e) { e.printStackTrace(); }
        return 0;
    }

    // ── HELPERS ───────────────────────────────────────────────────────────────
    private OrderDTO postOrder(String json) throws Exception {
        JsonNode node = fyersClient.placeOrder(json, authHeader());
        String s   = node.has("s")       ? node.get("s").asText()       : "unknown";
        String id  = node.has("id")      ? node.get("id").asText()       : "";
        String msg = node.has("message") ? node.get("message").asText()  : "";
        System.out.println("[ORDER] status=" + s + " | id=" + id + " | message=" + msg);
        if (!"ok".equals(s)) {
            System.err.println("[ORDER ERROR] Fyers rejected order: " + msg + " | Full response: " + node);
        }
        return new OrderDTO(s, id, msg);
    }

    private String authHeader() {
        return fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
    }

    private String buildOrderJson(String symbol, int qty, int side, int type,
                                   double limitPrice, double stopPrice, String tag) {
        return "{"
            + "\"symbol\":\"" + symbol + "\","
            + "\"qty\":" + qty + ","
            + "\"type\":" + type + ","
            + "\"side\":" + side + ","
            + "\"productType\":\"INTRADAY\","
            + "\"limitPrice\":" + limitPrice + ","
            + "\"stopPrice\":" + stopPrice + ","
            + "\"validity\":\"DAY\","
            + "\"disclosedQty\":0,"
            + "\"offlineOrder\":false,"
            + "\"stopLoss\":0,"
            + "\"takeProfit\":0,"
            + "\"orderTag\":\"" + tag + "\","
            + "\"isSliceOrder\":false"
            + "}";
    }

    public double roundToTick(double price, String symbol) {
        double tick = symbolMaster.getTickSize(symbol);
        if (Double.isNaN(tick) || tick <= 0) {
            System.err.println("[OrderService] Invalid tick size " + tick + " for " + symbol + " — using default 0.05");
            tick = 0.05;
        }
        // Round price to nearest tick: e.g. tick=0.05 → nearest 0.05, tick=5.0 → nearest 5
        return Math.round(price / tick) * tick;
    }
}