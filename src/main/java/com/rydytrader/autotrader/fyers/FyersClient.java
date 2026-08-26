package com.rydytrader.autotrader.fyers;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Abstraction over Fyers order-side, login-side, and market-data API calls.
 *
 * <p>Implementation: LiveFyersClient (real Fyers API).
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

    /** GET /api/v3/profile — get user profile (name, email, etc.) */
    JsonNode getProfile(String authHeader) throws Exception;

    /** PUT /api/v3/orders/sync — modify an existing order */
    JsonNode modifyOrder(String orderJson, String authHeader) throws Exception;

    /** GET /data/history — historical OHLC bars for {@code symbol}.
     *  {@code resolution} is Fyers-shape: "1", "3", "5", "15", "30", "60", "D".
     *  {@code rangeFromIso} / {@code rangeToIso} are {@code YYYY-MM-DD} strings.
     *  Response JSON contains {@code candles: [[epoch_sec, o, h, l, c, v], ...]}. */
    JsonNode getHistory(String symbol, String resolution,
                        String rangeFromIso, String rangeToIso,
                        String authHeader) throws Exception;
}
