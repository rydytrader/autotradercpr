package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;
    private final FyersClientRouter fyersClient;

    public OrderService(TokenStore tokenStore,
                        FyersProperties fyersProperties,
                        FyersClientRouter fyersClient) {
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
        this.fyersClient     = fyersClient;
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
            double rounded = roundToTick(slPrice);
            String json = buildOrderJson(symbol, qty, side, 3, 0, rounded, "AutoSL");
            System.out.println("SL Order: " + json);
            return postOrder(json);
        } catch (Exception e) { e.printStackTrace(); return null; }
    }

    // ── TARGET ORDER (Limit) ──────────────────────────────────────────────────
    public OrderDTO placeTarget(String symbol, int qty, int side, double targetPrice) {
        try {
            double rounded = roundToTick(targetPrice);
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
    // Cancels all pending orders by fetching current order book and cancelling PENDING ones
    public void cancelAllPendingOrders(String symbol) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getOrder(null, auth); // null = fetch all orders
            if (node != null && node.has("orderBook")) {
                for (JsonNode o : node.get("orderBook")) {
                    int status = o.has("status") ? o.get("status").asInt() : 0;
                    if (status == 1 && o.get("symbol").asText().equals(symbol)) { // PENDING for this symbol
                        cancelOrder(o.get("id").asText());
                    }
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    public boolean cancelOrder(String orderId) {
        try {
            JsonNode node = fyersClient.cancelOrder(orderId, authHeader());
            boolean ok = "ok".equals(node.get("s").asText());
            System.out.println("Cancel " + orderId + " → " + (ok ? "OK" : "FAILED"));
            return ok;
        } catch (Exception e) { e.printStackTrace(); return false; }
    }

    // ── NET DAY P&L (from tradebook) ──────────────────────────────────────────
    public double getNetDayPnl() {
        try {
            JsonNode root = fyersClient.getTradebook(authHeader());
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
            JsonNode root = fyersClient.getTradebook(authHeader());
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

    public double getExitPriceFromTradebook(String symbol) {
        try {
            JsonNode root = fyersClient.getTradebook(authHeader());
            if (!"ok".equals(root.get("s").asText())) return 0;
            JsonNode tradeBook = root.get("tradeBook");
            // Prefer ManualSquareOff trade as exit price
            for (int i = tradeBook.size() - 1; i >= 0; i--) {
                JsonNode t = tradeBook.get(i);
                if (t.get("symbol").asText().equals(symbol)
                        && "ManualSquareOff".equals(t.has("orderTag") ? t.get("orderTag").asText() : "")) {
                    return t.get("tradePrice").asDouble();
                }
            }
            // Fallback: most recent trade on either side
            double price = getFilledPriceFromTradebook(symbol, -1);
            if (price <= 0) price = getFilledPriceFromTradebook(symbol, 1);
            return price;
        } catch (Exception e) { e.printStackTrace(); }
        return 0;
    }

    // ── HELPERS ───────────────────────────────────────────────────────────────
    private OrderDTO postOrder(String json) throws Exception {
        JsonNode node = fyersClient.placeOrder(json, authHeader());
        return new OrderDTO(
            node.get("s").asText(),
            node.has("id") ? node.get("id").asText() : "",
            node.has("message") ? node.get("message").asText() : ""
        );
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

    private double roundToTick(double price) {
        return Math.round(price / 0.05) * 0.05;
    }
}