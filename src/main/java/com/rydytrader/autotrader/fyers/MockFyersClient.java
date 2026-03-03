package com.rydytrader.autotrader.fyers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.mock.MockState;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class MockFyersClient implements FyersClient {

    private final MockState state;
    private final ObjectMapper mapper = new ObjectMapper();

    public MockFyersClient(MockState state) {
        this.state = state;
    }

    @Override
    public JsonNode placeOrder(String orderJson, String authHeader) throws Exception {
        Map<String, Object> body = mapper.readValue(orderJson, Map.class);
        String orderId = state.nextOrderId();

        Map<String, Object> order = new LinkedHashMap<>();
        order.put("id",         orderId);
        order.put("symbol",     body.getOrDefault("symbol", state.getActiveSymbol()));
        order.put("qty",        toInt(body.get("qty")));
        order.put("type",       toInt(body.get("type")));
        order.put("side",       toInt(body.get("side")));
        order.put("productType",body.getOrDefault("productType", "INTRADAY"));
        order.put("limitPrice", toDouble(body.get("limitPrice")));
        order.put("stopPrice",  toDouble(body.get("stopPrice")));
        order.put("orderTag",   body.getOrDefault("orderTag", ""));
        order.put("status",     MockState.STATUS_PENDING);
        order.put("tradedPrice",0.0);

        state.addOrder(order);

        // Auto-fill market orders (type=2)
        if (state.isAutoFill() && toInt(body.get("type")) == 2) {
            int delay = state.getFillDelayMs();
            new Thread(() -> {
                try { Thread.sleep(delay); } catch (InterruptedException ignored) {}
                state.fillOrder(orderId, state.getCurrentPrice());
            }).start();
        }

        return ok().put("id", orderId).put("message", "Order placed");
    }

    @Override
    public JsonNode cancelOrder(String orderId, String authHeader) throws Exception {
        boolean ok = state.cancelOrder(orderId);
        if (ok) return ok().put("id", orderId).put("message", "Cancelled");
        return error("Order not found: " + orderId);
    }

    @Override
    public JsonNode getOrder(String orderId, String authHeader) throws Exception {
        var root = mapper.createObjectNode();
        var arr  = mapper.createArrayNode();

        // null orderId = fetch all orders
        java.util.Collection<java.util.Map<String, Object>> orderList =
            (orderId == null) ? state.getAllOrders()
                              : (state.getOrder(orderId) != null
                                    ? java.util.List.of(state.getOrder(orderId))
                                    : java.util.List.of());

        for (var order : orderList) {
            String id = (String) order.get("id");
            var oNode = mapper.createObjectNode();
            oNode.put("id",          id);
            oNode.put("status",      toInt(order.get("status")));
            oNode.put("tradedPrice", toDouble(order.get("tradedPrice")));
            oNode.put("filledQty",   toInt(order.get("status")) == MockState.STATUS_TRADED
                                        ? toInt(order.get("qty")) : 0);
            arr.add(oNode);
        }
        root.put("s", "ok");
        root.set("orderBook", arr);
        return root;
    }

    @Override
    public JsonNode getPositions(String authHeader) throws Exception {
        var root = mapper.createObjectNode();
        root.put("s", "ok");
        root.put("code", 200);
        var arr = mapper.createArrayNode();

        for (Map<String, Object> pos : state.getAllPositions()) {
            var p = mapper.createObjectNode();
            String sym = (String) pos.get("symbol");
            p.put("symbol",      sym);
            p.put("productType", (String) pos.get("productType"));
            p.put("netQty",      toInt(pos.get("netQty")));
            p.put("netAvgPrice", toDouble(pos.get("netAvgPrice")));
            p.put("side",        toInt(pos.get("side")));
            double ltp     = state.getCurrentPrice(sym);
            p.put("ltp",         ltp);
            int    netQty   = toInt(pos.get("netQty"));
            double avgPrice = toDouble(pos.get("netAvgPrice"));
            double unreal   = (ltp - avgPrice) * netQty;
            p.put("unrealizedProfit", Math.round(unreal * 100.0) / 100.0);
            p.put("buyAvg",  toDouble(pos.get("buyAvg")));
            p.put("sellAvg", toDouble(pos.get("sellAvg")));
            p.put("buyQty",  toInt(pos.get("buyQty")));
            p.put("sellQty", toInt(pos.get("sellQty")));
            arr.add(p);
        }

        root.set("netPositions", arr);
        root.put("overall", mapper.createObjectNode().put("count_total", arr.size()));
        return root;
    }

    @Override
    public JsonNode getTradebook(String authHeader) throws Exception {
        var root = mapper.createObjectNode();
        root.put("s", "ok");
        root.put("code", 200);
        var arr = mapper.createArrayNode();
        for (Map<String, Object> t : state.getTradebook()) {
            var tn = mapper.createObjectNode();
            tn.put("id",         t.get("id").toString());
            tn.put("symbol",     t.get("symbol").toString());
            tn.put("side",       toInt(t.get("side")));
            tn.put("tradedQty",  toInt(t.get("tradedQty")));
            tn.put("tradePrice", toDouble(t.get("tradePrice")));
            tn.put("orderTag",   t.getOrDefault("orderTag", "").toString());
            arr.add(tn);
        }
        root.set("tradeBook", arr);
        return root;
    }

    @Override
    public JsonNode validateAuthCode(String requestBody) throws Exception {
        return ok().put("access_token", "SIM_TOKEN_" + System.currentTimeMillis())
                   .put("message", "Simulator login successful");
    }

    // ── HELPERS ───────────────────────────────────────────────────────────────
    private com.fasterxml.jackson.databind.node.ObjectNode ok() {
        var n = mapper.createObjectNode();
        n.put("s", "ok");
        n.put("code", 200);
        return n;
    }

    private JsonNode error(String msg) {
        var n = mapper.createObjectNode();
        n.put("s", "error");
        n.put("message", msg);
        return n;
    }

    private int    toInt(Object v)    { return v == null ? 0 : Integer.parseInt(v.toString()); }
    private double toDouble(Object v) { return v == null ? 0.0 : Double.parseDouble(v.toString()); }
}