package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

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
            log.info("Entry Order: {}", json);
            return postOrder(json);
        } catch (Exception e) { log.error("Error placing entry order", e); return null; }
    }

    // ── STOP LOSS ORDER (Stop-Market) ─────────────────────────────────────────
    public OrderDTO placeStopLoss(String symbol, int qty, int side, double slPrice) {
        try {
            if (Double.isNaN(slPrice) || slPrice <= 0) {
                log.error("[OrderService] Invalid SL price {} for {} — skipping order", slPrice, symbol);
                return null;
            }
            double rounded = roundToTick(slPrice, symbol);
            String json = buildOrderJson(symbol, qty, side, 3, 0, rounded, "AutoSL");
            log.info("SL Order: {}", json);
            return postOrder(json);
        } catch (Exception e) { log.error("Error placing SL order", e); return null; }
    }

    // ── MODIFY SL ORDER (change stop price) ────────────────────────────────────
    public boolean modifySlOrder(String orderId, double newSlPrice, String symbol) {
        try {
            double rounded = roundToTick(newSlPrice, symbol);
            // Format price to avoid floating point noise (e.g. 465.70000000000005 → 465.70)
            String priceStr = String.format("%.2f", rounded);
            String json = "{\"id\":\"" + orderId + "\",\"type\":3,\"stopPrice\":" + priceStr + ",\"limitPrice\":0}";
            log.info("[OrderService] Modify SL {} → {} body={}", orderId, priceStr, json);
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode resp = fyersClient.modifyOrder(json, auth);
            boolean ok = resp != null && resp.has("s") && "ok".equals(resp.get("s").asText());
            if (ok) {
                log.info("[OrderService] SL modified successfully: {} → {}", orderId, rounded);
            } else {
                log.error("[OrderService] SL modify failed | Response: {} | OrderId: {} | Body: {}", resp, orderId, json);
            }
            return ok;
        } catch (Exception e) {
            log.error("[OrderService] Error modifying SL order {}", orderId, e);
            return false;
        }
    }

    // ── TARGET ORDER (Limit) ──────────────────────────────────────────────────
    public OrderDTO placeTarget(String symbol, int qty, int side, double targetPrice) {
        try {
            if (Double.isNaN(targetPrice) || targetPrice <= 0) {
                log.error("[OrderService] Invalid target price {} for {} — skipping order", targetPrice, symbol);
                return null;
            }
            double rounded = roundToTick(targetPrice, symbol);
            String json = buildOrderJson(symbol, qty, side, 1, rounded, 0, "AutoTarget");
            log.info("Target Order: {}", json);
            return postOrder(json);
        } catch (Exception e) { log.error("Error placing target order", e); return null; }
    }

    // ── EXIT MARKET ORDER (Square-off) ────────────────────────────────────────
    public OrderDTO placeExitOrder(String symbol, int qty, int side) {
        try {
            String json = buildOrderJson(symbol, qty, side, 2, 0, 0, "SquareOff");
            log.info("Exit Order: {}", json);
            return postOrder(json);
        } catch (Exception e) { log.error("Error placing exit order", e); return null; }
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
                        log.info("[CancelAll] Cancelling order {} | symbol={} | status={} | type={}", orderId, orderSymbol, status, orderType);
                        cancelOrder(orderId);
                        cancelled++;
                    }
                }
                if (cancelled == 0) {
                    log.info("[CancelAll] No pending/transit orders found for {} — dumping order book for symbol:", symbol);
                    for (JsonNode o : node.get("orderBook")) {
                        String orderSymbol = o.has("symbol") ? o.get("symbol").asText() : "";
                        if (orderSymbol.equals(symbol)) {
                            int status = o.has("status") ? o.get("status").asInt() : 0;
                            log.info("  order={} status={} type={}", o.get("id").asText(), status, o.has("type") ? o.get("type").asInt() : "?");
                        }
                    }
                } else {
                    log.info("[CancelAll] Cancelled {} order(s) for {}", cancelled, symbol);
                }
            } else if (node != null && node.has("code") && node.get("code").asInt() == -429) {
                log.info("[CancelAll] Fyers rate limit hit for {} — will retry on next sync", symbol);
            } else {
                log.info("[CancelAll] No orderBook in response for {}", symbol);
            }
        } catch (Exception e) { log.error("Error cancelling pending orders for " + symbol, e); }
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
                log.info("Cancel {} → {}", orderId, ok ? "OK" : "already cancelled/filled");
                return true;
            } else {
                log.info("Cancel {} → FAILED | response: {}", orderId, node);
            }
            return ok;
        } catch (Exception e) { log.error("Error cancelling order " + orderId, e); return false; }
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
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getTradebook(auth);
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
        } catch (Exception e) { log.error("Error fetching tradebook", e); }
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
        } catch (Exception e) { log.error("Error calculating net day P&L", e); return 0; }
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
        } catch (Exception e) { log.error("Error getting filled price from tradebook", e); }
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
        } catch (Exception e) { log.error("Error getting filled price by order ID", e); }
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

                    log.info("Exit price calculated from ManualSquareOff Tag {}", t.get("tradePrice").asDouble());
                    return t.get("tradePrice").asDouble();
                }
            }
            // Use known position side to find the exit on the opposite side
            if (positionSide != null) {
                int exitSide = "LONG".equals(positionSide) ? -1 : 1;  // LONG closed by SELL(-1), SHORT by BUY(1)
                double price = getFilledPriceFromTradebook(symbol, exitSide);
                log.info("Exit price calculated from SIDE LOGIC  ( LONG closed by SELL(-1), SHORT by BUY(1) )  logic {}", price);
                if (price > 0) return price;
            }
            // Final fallback: try both sides
            double price = getFilledPriceFromTradebook(symbol, -1);
            if (price <= 0) price = getFilledPriceFromTradebook(symbol, 1);
            log.info("Exit price calculated from Final fallback: try both sides {}", price);
            return price;
        } catch (Exception e) { log.error("Error getting exit price from tradebook", e); }
        return 0;
    }

    // ── HELPERS ───────────────────────────────────────────────────────────────
    private OrderDTO postOrder(String json) throws Exception {
        JsonNode node = fyersClient.placeOrder(json, authHeader());
        String s   = node.has("s")       ? node.get("s").asText()       : "unknown";
        String id  = node.has("id")      ? node.get("id").asText()       : "";
        String msg = node.has("message") ? node.get("message").asText()  : "";
        log.info("[ORDER] status={} | id={} | message={}", s, id, msg);
        if (!"ok".equals(s)) {
            log.error("[ORDER ERROR] Fyers rejected order: {} | Full response: {}", msg, node);
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
            log.error("[OrderService] Invalid tick size {} for {} — using default 0.05", tick, symbol);
            tick = 0.05;
        }
        // Round price to nearest tick: e.g. tick=0.05 → nearest 0.05, tick=5.0 → nearest 5
        return Math.round(price / tick) * tick;
    }
}