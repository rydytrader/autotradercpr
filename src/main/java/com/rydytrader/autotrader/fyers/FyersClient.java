package com.rydytrader.autotrader.fyers;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Abstraction over all Fyers API calls.
 * Implementation: LiveFyersClient (real Fyers API).
 */
public interface FyersClient {

    /** POST /api/v3/orders/sync — place any order, returns full JSON response */
    JsonNode placeOrder(String orderJson, String authHeader) throws Exception;

    /** DELETE /api/v3/orders — cancel an order by id */
    JsonNode cancelOrder(String orderId, String authHeader) throws Exception;

    /** GET /api/v3/orders?id=X — get single order status */
    JsonNode getOrder(String orderId, String authHeader) throws Exception;

    /** GET /api/v3/orders — get all orders (full order book) */
    JsonNode getOrders(String authHeader) throws Exception;

    /** GET /api/v3/positions — get open positions (intraday) */
    JsonNode getPositions(String authHeader) throws Exception;

    /** GET /api/v3/holdings — get CNC holdings (multi-day positions) */
    JsonNode getHoldings(String authHeader) throws Exception;

    /** GET /api/v3/tradebook — get tradebook */
    JsonNode getTradebook(String authHeader) throws Exception;

    /** POST /api/v3/validate-authcode — exchange auth code for token */
    JsonNode validateAuthCode(String requestBody) throws Exception;

    /** GET /api/v3/optionChain — get option chain with OI data */
    JsonNode getOptionChain(String symbol, int strikeCount, String authHeader) throws Exception;

    /** GET /data/quotes — get quotes for a comma-separated list of symbols */
    JsonNode getQuotes(String symbols, String authHeader) throws Exception;

    /** GET /api/v3/profile — get user profile (name, email, etc.) */
    JsonNode getProfile(String authHeader) throws Exception;

    /** PUT /api/v3/orders/sync — modify an existing order */
    JsonNode modifyOrder(String orderJson, String authHeader) throws Exception;
}