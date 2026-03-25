package com.rydytrader.autotrader.websocket;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket client for Fyers HSM (High Speed Market data) binary feed.
 * Connects to wss://socket.fyers.in/hsm/v1-5/prod, authenticates,
 * subscribes to symbols, and forwards parsed ticks via a callback.
 */
public class FyersDataWebSocket extends WebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(FyersDataWebSocket.class);

    public interface TickCallback {
        void onTick(HsmBinaryParser.RawTick tick);
        void onConnected();
        void onDisconnected(String reason);
        void onAuthResult(boolean success, int ackCount);
    }

    private final String hsmKey;
    private final List<String> hsmTokens;
    private final int channelNum;
    private final TickCallback callback;
    private final boolean liteMode;

    // Per-topic metadata (multiplier, precision) populated from snapshots
    private final Map<Integer, HsmBinaryParser.SymbolMeta> symbolMeta = new ConcurrentHashMap<>();
    // HSM token → Fyers symbol mapping (e.g. "sf|nse_cm|2885" → "NSE:RELIANCE-EQ")
    private final Map<String, String> hsmToFyersSymbol;

    private volatile int ackCount = 0;
    private volatile int updateCount = 0;

    public FyersDataWebSocket(String hsmKey,
                               List<String> hsmTokens,
                               Map<String, String> hsmToFyersSymbol,
                               boolean liteMode,
                               int channelNum,
                               TickCallback callback) {
        super(URI.create("wss://socket.fyers.in/hsm/v1-5/prod"));
        this.hsmKey = hsmKey;
        this.hsmTokens = hsmTokens;
        this.hsmToFyersSymbol = hsmToFyersSymbol;
        this.liteMode = liteMode;
        this.channelNum = channelNum;
        this.callback = callback;
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        log.info("[FyersWS] Connected to HSM WebSocket");
        // Step 1: Send auth message
        byte[] authMsg = HsmBinaryParser.buildAuthMessage(hsmKey);
        send(authMsg);
    }

    @Override
    public void onMessage(String message) {
        // Text messages not expected from HSM feed
        log.info("[FyersWS] Unexpected text message: {}", message);
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        byte[] data = new byte[bytes.remaining()];
        bytes.get(data);

        int respType = HsmBinaryParser.parseResponseType(data);

        switch (respType) {
            case 1: // Auth response
                int ack = HsmBinaryParser.parseAuthResponse(data);
                if (ack >= 0) {
                    this.ackCount = ack;
                    log.info("[FyersWS] Auth success, ack_count={}", ack);
                    callback.onAuthResult(true, ack);

                    // Step 2: Subscribe to symbols
                    if (!hsmTokens.isEmpty()) {
                        byte[] subMsg = HsmBinaryParser.buildSubscribeMessage(hsmTokens, channelNum);
                        send(subMsg);
                        log.info("[FyersWS] Subscribed to {} symbols", hsmTokens.size());
                    }

                    // Step 3: Switch to lite mode if requested
                    if (liteMode) {
                        byte[] liteMsg = HsmBinaryParser.buildLiteModeMessage(channelNum);
                        send(liteMsg);
                        log.info("[FyersWS] Lite mode enabled");
                    }

                    callback.onConnected();
                } else {
                    log.error("[FyersWS] Auth FAILED");
                    callback.onAuthResult(false, 0);
                    close();
                }
                break;

            case 4: // Subscribe response
                log.info("[FyersWS] Subscribe response received");
                break;

            case 5: // Unsubscribe response
                log.info("[FyersWS] Unsubscribe response received");
                break;

            case 6: // Data feed
                handleDataFeed(data);
                break;

            case 12: // Mode change response
                log.info("[FyersWS] Mode change response received");
                break;

            default:
                // Ignore unknown response types
                break;
        }
    }

    private void handleDataFeed(byte[] data) {
        // ACK handling
        if (ackCount > 0) {
            updateCount++;
            if (updateCount >= ackCount) {
                int msgNum = HsmBinaryParser.extractMessageNumber(data);
                byte[] ackMsg = HsmBinaryParser.buildAckMessage(msgNum);
                send(ackMsg);
                updateCount = 0;
            }
        }

        // Parse ticks
        List<HsmBinaryParser.RawTick> ticks = HsmBinaryParser.parseDataFeed(
            data, symbolMeta, hsmToFyersSymbol);
        for (HsmBinaryParser.RawTick tick : ticks) {
            callback.onTick(tick);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        log.info("[FyersWS] Disconnected: code={} reason={}", code, reason);
        callback.onDisconnected(reason);
    }

    @Override
    public void onError(Exception ex) {
        log.error("[FyersWS] Error: {}", ex.getMessage());
    }

    /** Send subscribe for additional symbols (delta subscription). */
    public void subscribeSymbols(List<String> tokens) {
        if (isOpen() && !tokens.isEmpty()) {
            byte[] msg = HsmBinaryParser.buildSubscribeMessage(tokens, channelNum);
            send(msg);
        }
    }

    /** Send unsubscribe for removed symbols. */
    public void unsubscribeSymbols(List<String> tokens) {
        if (isOpen() && !tokens.isEmpty()) {
            byte[] msg = HsmBinaryParser.buildUnsubscribeMessage(tokens, channelNum);
            send(msg);
        }
    }

    /** Send ping to keep connection alive. Called by MarketDataService scheduler. */
    public void sendPing() {
        if (isOpen()) {
            send(HsmBinaryParser.buildPingMessage());
        }
    }

    /** Clear stored metadata (for reconnection). */
    public void clearMeta() {
        symbolMeta.clear();
        updateCount = 0;
    }
}
