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

    /** GET /api/v3/positions — get open positions */
    JsonNode getPositions(String authHeader) throws Exception;

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

    /** GET /data/history — historical OHLC candles for a symbol.
     *  @param symbol     Fyers symbol e.g. "NSE:NIFTY50-INDEX"
     *  @param resolution candle resolution — "1D" for daily, "15" for 15-min, etc.
     *  @param fromDate   ISO yyyy-MM-dd
     *  @param toDate     ISO yyyy-MM-dd
     *  Returns the raw JsonNode with {@code candles} array of {@code [ts, o, h, l, c, v]}. */
    JsonNode getHistoricalCandles(String symbol, String resolution, String fromDate, String toDate, String authHeader) throws Exception;
}