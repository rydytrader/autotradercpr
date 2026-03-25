package com.rydytrader.autotrader.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;

/**
 * WebSocket client for Fyers Order Update feed.
 * Connects to wss://socket.fyers.in/trade/v3, receives JSON push events
 * for order status changes, trade fills, and position updates.
 */
public class FyersOrderWebSocket extends WebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(FyersOrderWebSocket.class);

    public interface OrderCallback {
        void onOrderEvent(JsonNode order);
        void onTradeEvent(JsonNode trade);
        void onPositionEvent(JsonNode position);
        void onWsConnected();
        void onWsDisconnected(String reason);
    }

    private final OrderCallback callback;
    private final ObjectMapper mapper = new ObjectMapper();

    public FyersOrderWebSocket(String accessToken, OrderCallback callback) {
        super(URI.create("wss://socket.fyers.in/trade/v3"),
              new Draft_6455(),
              Map.of("authorization", accessToken),
              10000);
        this.callback = callback;
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        log.info("[OrderWS] Connected to Fyers Order WebSocket");
        // Subscribe to orders, trades, and positions
        String subMsg = "{\"T\":\"SUB_ORD\",\"SLIST\":[\"orders\",\"trades\",\"positions\"],\"SUB_T\":1}";
        send(subMsg);
        log.info("[OrderWS] Subscribed to orders, trades, positions");
        callback.onWsConnected();
    }

    @Override
    public void onMessage(String message) {
        if ("pong".equals(message)) return; // Ping response

        try {
            JsonNode json = mapper.readTree(message);

            if (json.has("orders")) {
                JsonNode order = json.get("orders");
                log.info("[OrderWS] Order event: {} status={} tag={}", order.get("symbol").asText(), order.get("org_ord_status").asText(), order.has("ordertag") ? order.get("ordertag").asText() : "");
                callback.onOrderEvent(order);
            } else if (json.has("trades")) {
                JsonNode trade = json.get("trades");
                log.info("[OrderWS] Trade event: {} price={}", trade.get("symbol").asText(), trade.get("price_traded").asText());
                callback.onTradeEvent(trade);
            } else if (json.has("positions")) {
                JsonNode position = json.get("positions");
                log.info("[OrderWS] Position event: {} netQty={}", position.get("symbol").asText(), position.get("net_qty").asText());
                callback.onPositionEvent(position);
            } else {
                // General event (login, alerts, etc.)
                log.info("[OrderWS] General event: {}", message);
            }
        } catch (Exception e) {
            log.error("[OrderWS] Parse error: {}", e.getMessage());
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        log.info("[OrderWS] Disconnected: code={} reason={}", code, reason);
        callback.onWsDisconnected(reason);
    }

    @Override
    public void onError(Exception ex) {
        log.error("[OrderWS] Error", ex);
    }

    /** Send ping to keep connection alive. Called every 10s by scheduler. */
    public void sendPing() {
        if (isOpen()) {
            send("ping");
        }
    }
}
