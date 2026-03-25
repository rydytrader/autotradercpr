package com.rydytrader.autotrader.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.Map;

/**
 * WebSocket client for Fyers Order Update feed.
 * Connects to wss://socket.fyers.in/trade/v3, receives JSON push events
 * for order status changes, trade fills, and position updates.
 */
public class FyersOrderWebSocket extends WebSocketClient {

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
        System.out.println("[OrderWS] Connected to Fyers Order WebSocket");
        // Subscribe to orders, trades, and positions
        String subMsg = "{\"T\":\"SUB_ORD\",\"SLIST\":[\"orders\",\"trades\",\"positions\"],\"SUB_T\":1}";
        send(subMsg);
        System.out.println("[OrderWS] Subscribed to orders, trades, positions");
        callback.onWsConnected();
    }

    @Override
    public void onMessage(String message) {
        if ("pong".equals(message)) return; // Ping response

        try {
            JsonNode json = mapper.readTree(message);

            if (json.has("orders")) {
                JsonNode order = json.get("orders");
                System.out.println("[OrderWS] Order event: " + order.get("symbol").asText()
                    + " status=" + order.get("org_ord_status").asText()
                    + " tag=" + (order.has("ordertag") ? order.get("ordertag").asText() : ""));
                callback.onOrderEvent(order);
            } else if (json.has("trades")) {
                JsonNode trade = json.get("trades");
                System.out.println("[OrderWS] Trade event: " + trade.get("symbol").asText()
                    + " price=" + trade.get("price_traded").asText());
                callback.onTradeEvent(trade);
            } else if (json.has("positions")) {
                JsonNode position = json.get("positions");
                System.out.println("[OrderWS] Position event: " + position.get("symbol").asText()
                    + " netQty=" + position.get("net_qty").asText());
                callback.onPositionEvent(position);
            } else {
                // General event (login, alerts, etc.)
                System.out.println("[OrderWS] General event: " + message);
            }
        } catch (Exception e) {
            System.out.println("[OrderWS] Parse error: " + e.getMessage());
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("[OrderWS] Disconnected: code=" + code + " reason=" + reason);
        callback.onWsDisconnected(reason);
    }

    @Override
    public void onError(Exception ex) {
        System.out.println("[OrderWS] Error: " + ex.getMessage());
        ex.printStackTrace();
    }

    /** Send ping to keep connection alive. Called every 10s by scheduler. */
    public void sendPing() {
        if (isOpen()) {
            send("ping");
        }
    }
}
