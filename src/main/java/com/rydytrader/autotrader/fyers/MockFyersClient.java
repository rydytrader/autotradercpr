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
            String sym = (String) order.get("symbol");
            new Thread(() -> {
                try { Thread.sleep(delay); } catch (InterruptedException ignored) {}
                state.fillOrder(orderId, state.getCurrentPrice(sym));
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
    public JsonNode getOrders(String authHeader) throws Exception {
        return getOrder(null, authHeader);
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
            oNode.put("symbol",      String.valueOf(order.getOrDefault("symbol", "")));
            oNode.put("status",      toInt(order.get("status")));
            oNode.put("tradedPrice", toDouble(order.get("tradedPrice")));
            oNode.put("stopPrice",   toDouble(order.get("stopPrice")));
            oNode.put("limitPrice",  toDouble(order.get("limitPrice")));
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
            tn.put("id",          t.get("id").toString());
            tn.put("orderNumber", t.getOrDefault("orderId", "").toString());
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

    @Override
    public JsonNode getOptionChain(String symbol, int strikeCount, String authHeader) throws Exception {
        // Return mock option chain with realistic per-strike OI data
        var root = mapper.createObjectNode();
        root.put("s", "ok");
        root.put("code", 200);
        var data = mapper.createObjectNode();
        var chain = mapper.createArrayNode();

        // ATM ~24200, generate strikes around it
        int atm = 24200;
        int step = 100;
        int count = Math.max(strikeCount, 5);
        long totalCallOi = 0, totalPutOi = 0;

        // OI profiles: higher OI near ATM, tapering off; puts heavier below ATM, calls heavier above
        for (int i = -count; i <= count; i++) {
            int strike = atm + i * step;
            // CE OI: higher for strikes above ATM (OTM calls get more OI from writers)
            long ceOi = (long)(300_000 + Math.random() * 200_000);
            if (i >= 1) ceOi += (long)(i * 150_000 * (1 + Math.random() * 0.3));
            // PE OI: higher for strikes below ATM (OTM puts get more OI from writers)
            long peOi = (long)(300_000 + Math.random() * 200_000);
            if (i <= -1) peOi += (long)((-i) * 150_000 * (1 + Math.random() * 0.3));

            var ce = mapper.createObjectNode();
            ce.put("strike_price", strike);
            ce.put("option_type", "CE");
            ce.put("oi", ceOi);
            ce.put("ltp", Math.max(0, (atm - strike) + 100 + Math.random() * 50));
            chain.add(ce);

            var pe = mapper.createObjectNode();
            pe.put("strike_price", strike);
            pe.put("option_type", "PE");
            pe.put("oi", peOi);
            pe.put("ltp", Math.max(0, (strike - atm) + 100 + Math.random() * 50));
            chain.add(pe);

            totalCallOi += ceOi;
            totalPutOi += peOi;
        }

        data.put("callOi", totalCallOi);
        data.put("putOi", totalPutOi);
        data.set("optionsChain", chain);
        root.set("data", data);
        return root;
    }

    @Override
    public JsonNode getQuotes(String symbols, String authHeader) throws Exception {
        var root = mapper.createObjectNode();
        root.put("s", "ok");
        root.put("code", 200);
        var arr = mapper.createArrayNode();
        // Mock data for key indices
        String[][] mockData = {
            {"NSE:NIFTY50-INDEX", "NIFTY 50", "24250.30", "185.40", "0.77"},
            {"NSE:NIFTYBANK-INDEX", "BANK NIFTY", "51820.55", "-142.30", "-0.27"},
            {"NSE:NIFTYIT-INDEX", "NIFTY IT", "35420.10", "312.60", "0.89"},
            {"NSE:RELIANCE-EQ", "RELIANCE", "2485.60", "28.35", "1.15"},
            {"NSE:TCS-EQ", "TCS", "3892.45", "-45.20", "-1.15"},
            {"NSE:HDFCBANK-EQ", "HDFC BANK", "1678.30", "12.80", "0.77"},
            {"NSE:INFY-EQ", "INFOSYS", "1542.75", "18.90", "1.24"},
            {"NSE:ICICIBANK-EQ", "ICICI BANK", "1265.40", "-8.55", "-0.67"}
        };
        for (String[] d : mockData) {
            var item = mapper.createObjectNode();
            var v = mapper.createObjectNode();
            v.put("symbol", d[0]);
            v.put("short_name", d[1]);
            v.put("lp", Double.parseDouble(d[2]));
            v.put("chp", Double.parseDouble(d[4]));
            v.put("ch", Double.parseDouble(d[3]));
            item.set("v", v);
            arr.add(item);
        }
        root.set("d", arr);
        return root;
    }

    // ── HELPERS ───────────────────────────────────────────────────────────────
    private com.fasterxml.jackson.databind.node.ObjectNode ok() {
        var n = mapper.createObjectNode();
        n.put("s", "ok");
        n.put("code", 200);
        return n;
    }

    @Override
    public JsonNode getProfile(String authHeader) throws Exception {
        var root = mapper.createObjectNode();
        root.put("s", "ok");
        var data = mapper.createObjectNode();
        data.put("name", "Simulator User");
        data.put("fy_id", "SIM001");
        data.put("email_id", "sim@traderedge.com");
        root.set("data", data);
        return root;
    }

    @Override
    public JsonNode modifyOrder(String orderJson, String authHeader) throws Exception {
        var root = mapper.createObjectNode();
        root.put("s", "ok");
        root.put("code", 200);
        root.put("message", "Order modified (mock)");
        return root;
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