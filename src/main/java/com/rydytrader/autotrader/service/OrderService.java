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
            double rounded = roundToTick(slPrice, symbol);
            String json = buildOrderJson(symbol, qty, side, 3, 0, rounded, "AutoSL");
            System.out.println("SL Order: " + json);
            return postOrder(json);
        } catch (Exception e) { e.printStackTrace(); return null; }
    }

    // ── TARGET ORDER (Limit) ──────────────────────────────────────────────────
    public OrderDTO placeTarget(String symbol, int qty, int side, double targetPrice) {
        try {
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
    // Cancels all pending orders by fetching current order book and cancelling PENDING ones
    public void cancelAllPendingOrders(String symbol) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode node = fyersClient.getOrder(null, auth); // null = fetch all orders
            if (node != null && node.has("orderBook")) {
                for (JsonNode o : node.get("orderBook")) {
                    int status = o.has("status") ? o.get("status").asInt() : 0;
                    if (status == 6 && o.get("symbol").asText().equals(symbol)) { // PENDING for this symbol
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
        return getExitPriceFromTradebook(symbol, null);
    }

    /**
     * Looks up exit price from tradebook.
     * positionSide = "LONG" or "SHORT" — used to target the opposite-side exit trade.
     */
    public double getExitPriceFromTradebook(String symbol, String positionSide) {
        try {
            JsonNode root = fyersClient.getTradebook(authHeader());
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

    private double roundToTick(double price, String symbol) {
        double tick   = symbolMaster.getTickSize(symbol);
        long   factor = Math.round(1.0 / tick);  // 0.05→20, 0.10→10, 0.25→4, 0.50→2
        return Math.round(price * factor) / (double) factor;
    }
}