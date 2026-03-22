package com.rydytrader.autotrader.fyers;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.store.ModeStore;
import org.springframework.stereotype.Component;

/**
 * Single injection point for all services.
 * Delegates to LiveFyersClient or MockFyersClient based on current ModeStore.
 */
@Component
public class FyersClientRouter implements FyersClient {

    private final ModeStore        modeStore;
    private final LiveFyersClient  live;
    private final MockFyersClient  mock;

    public FyersClientRouter(ModeStore modeStore,
                              LiveFyersClient live,
                              MockFyersClient mock) {
        this.modeStore = modeStore;
        this.live      = live;
        this.mock      = mock;
    }

    private FyersClient client() {
        return modeStore.isLive() ? live : mock;
    }

    @Override public JsonNode placeOrder(String json, String auth) throws Exception    { return client().placeOrder(json, auth); }
    @Override public JsonNode cancelOrder(String id, String auth) throws Exception     { return client().cancelOrder(id, auth); }
    @Override public JsonNode getOrder(String id, String auth) throws Exception        { return client().getOrder(id, auth); }
    @Override public JsonNode getOrders(String auth) throws Exception                 { return client().getOrders(auth); }
    @Override public JsonNode getPositions(String auth) throws Exception               { return client().getPositions(auth); }
    @Override public JsonNode getTradebook(String auth) throws Exception               { return client().getTradebook(auth); }
    @Override public JsonNode validateAuthCode(String body) throws Exception           { return client().validateAuthCode(body); }
    @Override public JsonNode getOptionChain(String sym, int strikes, String auth) throws Exception { return client().getOptionChain(sym, strikes, auth); }
    @Override public JsonNode getQuotes(String symbols, String auth) throws Exception { return client().getQuotes(symbols, auth); }
}