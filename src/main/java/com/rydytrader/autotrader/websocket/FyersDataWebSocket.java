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

    // ── Diagnostic: pre-snapshot drop tracking ─────────────────────────────────
    // Count of full-update (dataType=85) frames dropped because the topicId had no
    // SymbolMeta yet (i.e. snapshot hadn't arrived). Populated by parseDataFeed.
    private final Map<Integer, Integer> preSnapshotDropCounts = new ConcurrentHashMap<>();
    // topicId → epochMillis of first snapshot, logged exactly once per topic.
    private final Map<Integer, Long> firstSnapshotTimes = new ConcurrentHashMap<>();
    private volatile long subscribeTime = 0;
    // Event-log sink so we can write to the user-visible event log in addition to SLF4J.
    private volatile java.util.function.Consumer<String> eventLogSink;

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
                        subscribeTime = System.currentTimeMillis();
                        String subAt = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
                            .toLocalTime()
                            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
                        log.info("[FyersWS] Subscribed to {} symbols at {} — monitoring pre-snapshot drops",
                            hsmTokens.size(), subAt);
                        if (eventLogSink != null) {
                            eventLogSink.accept("[INFO] [HsmParser] Subscribed to " + hsmTokens.size()
                                + " symbols at " + subAt + " — monitoring pre-snapshot update drops");
                        }
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

        // Parse ticks (diagnostic: pass the pre-snapshot drop counter so the parser
        // records any dataType=85 frames that hit the meta==null branch).
        List<HsmBinaryParser.RawTick> ticks = HsmBinaryParser.parseDataFeed(
            data, symbolMeta, hsmToFyersSymbol, preSnapshotDropCounts);
        for (HsmBinaryParser.RawTick tick : ticks) {
            callback.onTick(tick);
        }

        // Diagnostic: any topic whose SymbolMeta just got populated by this frame and
        // that we haven't logged yet is a "first snapshot arrival". Log it once with
        // the accumulated drop count so we can see which symbols had pre-snapshot data loss.
        for (Map.Entry<Integer, HsmBinaryParser.SymbolMeta> e : symbolMeta.entrySet()) {
            Integer topicId = e.getKey();
            if (firstSnapshotTimes.putIfAbsent(topicId, System.currentTimeMillis()) == null) {
                String topicName = e.getValue().topicName;
                String fyersSymbol = hsmToFyersSymbol.getOrDefault(topicName, topicName);
                int dropped = preSnapshotDropCounts.getOrDefault(topicId, 0);
                String arrivedAt = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
                    .toLocalTime()
                    .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
                long delayMs = subscribeTime > 0 ? System.currentTimeMillis() - subscribeTime : 0;
                String msg = "First snapshot for " + fyersSymbol + " (" + topicName
                    + ") at " + arrivedAt + " — " + dropped + " pre-snapshot updates dropped"
                    + (delayMs > 0 ? " (" + (delayMs / 1000) + "s after subscribe)" : "");
                log.info("[HsmParser] {}", msg);
                if (eventLogSink != null && dropped > 0) {
                    // Only surface to event log when there's an actual drop — otherwise
                    // this would spam 34+ "0 dropped" lines at startup.
                    eventLogSink.accept("[WARNING] [HsmParser] " + msg);
                }
            }
        }
    }

    /** Set a consumer that receives diagnostic messages for the user-visible event log. */
    public void setEventLogSink(java.util.function.Consumer<String> sink) {
        this.eventLogSink = sink;
    }

    /**
     * Emit a one-line summary of pre-snapshot drops since the last reset (or since start).
     * Called from MarketDataService at the end of the 9:25 opening refresh.
     */
    public String buildPreSnapshotDropSummary() {
        int total = preSnapshotDropCounts.values().stream().mapToInt(Integer::intValue).sum();
        int affectedSymbols = (int) preSnapshotDropCounts.values().stream().filter(v -> v > 0).count();
        int totalSnapshots = firstSnapshotTimes.size();
        if (totalSnapshots == 0) {
            return "Pre-snapshot drop summary — no snapshots observed yet";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Pre-snapshot drop summary — ").append(totalSnapshots).append(" symbols snapshotted, ")
          .append(affectedSymbols).append(" with drops, ").append(total).append(" total updates dropped");
        if (affectedSymbols > 0) {
            sb.append(" [");
            boolean first = true;
            for (Map.Entry<Integer, Integer> e : preSnapshotDropCounts.entrySet()) {
                if (e.getValue() <= 0) continue;
                HsmBinaryParser.SymbolMeta meta = symbolMeta.get(e.getKey());
                String name = meta != null ? hsmToFyersSymbol.getOrDefault(meta.topicName, meta.topicName) : ("topic=" + e.getKey());
                if (!first) sb.append(", ");
                sb.append(name).append("=").append(e.getValue());
                first = false;
            }
            sb.append("]");
        }
        return sb.toString();
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
        // Reset diagnostic state so reconnect scenarios are measured independently.
        preSnapshotDropCounts.clear();
        firstSnapshotTimes.clear();
        subscribeTime = 0;
    }
}
