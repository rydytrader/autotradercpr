package com.rydytrader.autotrader.fyers;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

/**
 * Single injection point for all services. Delegates to LiveFyersClient.
 */
@Component
public class FyersClientRouter implements FyersClient {

    private final LiveFyersClient live;

    public FyersClientRouter(LiveFyersClient live) {
        this.live = live;
    }

    @Override public JsonNode placeOrder(String json, String auth) throws Exception    { return live.placeOrder(json, auth); }
    @Override public JsonNode cancelOrder(String id, String auth) throws Exception     { return live.cancelOrder(id, auth); }
    @Override public JsonNode getOrder(String id, String auth) throws Exception        { return live.getOrder(id, auth); }
    @Override public JsonNode getOrders(String auth) throws Exception                 { return live.getOrders(auth); }
    @Override public JsonNode getPositions(String auth) throws Exception               { return live.getPositions(auth); }
    @Override public JsonNode getHoldings(String auth) throws Exception               { return live.getHoldings(auth); }
    @Override public JsonNode getTradebook(String auth) throws Exception               { return live.getTradebook(auth); }
    @Override public JsonNode validateAuthCode(String body) throws Exception           { return live.validateAuthCode(body); }
    @Override public JsonNode getOptionChain(String sym, int strikes, String auth) throws Exception { return live.getOptionChain(sym, strikes, auth); }
    @Override public JsonNode getQuotes(String symbols, String auth) throws Exception { return live.getQuotes(symbols, auth); }
    @Override public JsonNode getProfile(String auth) throws Exception { return live.getProfile(auth); }
    @Override public JsonNode modifyOrder(String orderJson, String auth) throws Exception { return live.modifyOrder(orderJson, auth); }
}
