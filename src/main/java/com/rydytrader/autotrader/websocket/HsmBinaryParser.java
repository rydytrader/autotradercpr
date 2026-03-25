package com.rydytrader.autotrader.websocket;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Binary protocol builder/parser for Fyers HSM Data WebSocket.
 * All methods are static. Wire format is big-endian.
 */
public class HsmBinaryParser {

    private static final int SENTINEL = -2147483648; // Integer.MIN_VALUE
    private static final String SOURCE = "JavaSDK-1.0";

    // Field names for scrip updates (sf) — 21 fields
    public static final String[] SCRIP_FIELDS = {
        "ltp", "vol_traded_today", "last_traded_time", "exch_feed_time",
        "bid_size", "ask_size", "bid_price", "ask_price",
        "last_traded_qty", "tot_buy_qty", "tot_sell_qty", "avg_trade_price",
        "OI", "low_price", "high_price", "Yhigh", "Ylow",
        "lower_ckt", "upper_ckt", "open_price", "prev_close_price"
    };

    // Field names for index updates (if) — 6 fields
    public static final String[] INDEX_FIELDS = {
        "ltp", "prev_close_price", "exch_feed_time",
        "high_price", "low_price", "open_price"
    };

    // Fields where price formula applies
    private static final java.util.Set<String> PRICE_FIELDS = java.util.Set.of(
        "ltp", "bid_price", "ask_price", "avg_trade_price",
        "low_price", "high_price", "open_price", "prev_close_price"
    );

    // ────────────────────────────────────────────────────────────────────────────
    // Outgoing message builders
    // ────────────────────────────────────────────────────────────────────────────

    /** Build auth message (req_type=1). */
    public static byte[] buildAuthMessage(String hsmKey) {
        byte[] keyBytes = hsmKey.getBytes(StandardCharsets.UTF_8);
        byte[] srcBytes = SOURCE.getBytes(StandardCharsets.UTF_8);
        int bufferSize = 18 + keyBytes.length + srcBytes.length;

        ByteBuffer buf = ByteBuffer.allocate(bufferSize).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) (bufferSize - 2)); // total size minus 2
        buf.put((byte) 1);  // req_type = auth
        buf.put((byte) 4);  // field_count

        // Field 1: hsm_key
        buf.put((byte) 1);
        buf.putShort((short) keyBytes.length);
        buf.put(keyBytes);

        // Field 2: mode = "P"
        buf.put((byte) 2);
        buf.putShort((short) 1);
        buf.put((byte) 'P');

        // Field 3: byte 1
        buf.put((byte) 3);
        buf.putShort((short) 1);
        buf.put((byte) 1);

        // Field 4: source
        buf.put((byte) 4);
        buf.putShort((short) srcBytes.length);
        buf.put(srcBytes);

        return buf.array();
    }

    /** Build subscribe message (req_type=4). */
    public static byte[] buildSubscribeMessage(List<String> hsmTokens, int channelNum) {
        byte[] scripsData = buildScripsData(hsmTokens);
        byte[] srcBytes = SOURCE.getBytes(StandardCharsets.UTF_8);
        // Python SDK: data_len = 18 + len(scrips_data) + len(access_token) + len(source)
        // But we just need the actual payload size
        int totalSize = 4 + 1 + 2 + scripsData.length + 1 + 2 + 1;

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) totalSize); // total size
        buf.put((byte) 4);  // req_type = subscribe
        buf.put((byte) 2);  // field_count

        // Field 1: scrips data
        buf.put((byte) 1);
        buf.putShort((short) scripsData.length);
        buf.put(scripsData);

        // Field 2: channel number
        buf.put((byte) 2);
        buf.putShort((short) 1);
        buf.put((byte) channelNum);

        return buf.array();
    }

    /** Build unsubscribe message (req_type=5). */
    public static byte[] buildUnsubscribeMessage(List<String> hsmTokens, int channelNum) {
        byte[] scripsData = buildScripsData(hsmTokens);
        int totalSize = 4 + 1 + 2 + scripsData.length + 1 + 2 + 1;

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) totalSize);
        buf.put((byte) 5);  // req_type = unsubscribe
        buf.put((byte) 2);

        buf.put((byte) 1);
        buf.putShort((short) scripsData.length);
        buf.put(scripsData);

        buf.put((byte) 2);
        buf.putShort((short) 1);
        buf.put((byte) channelNum);

        return buf.array();
    }

    /** Build lite mode switch message (req_type=12). */
    public static byte[] buildLiteModeMessage(int channelNum) {
        ByteBuffer buf = ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) 0);   // placeholder size
        buf.put((byte) 12);       // req_type = mode change
        buf.put((byte) 2);        // field_count

        // Field 1: channel bits (uint64)
        long channelBits = 1L << channelNum;
        buf.put((byte) 1);
        buf.putShort((short) 8);
        buf.putLong(channelBits);

        // Field 2: mode = 76 (lite)
        buf.put((byte) 2);
        buf.putShort((short) 1);
        buf.put((byte) 76);

        return buf.array();
    }

    /** Ping message: 3 bytes. */
    public static byte[] buildPingMessage() {
        return new byte[]{0, 1, 11};
    }

    /** Build ACK message (req_type=3). */
    public static byte[] buildAckMessage(int messageNumber) {
        ByteBuffer buf = ByteBuffer.allocate(11).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) 9);   // total_size - 2 = 9
        buf.put((byte) 3);        // req_type = ack
        buf.put((byte) 1);        // field_count
        buf.put((byte) 1);        // field_id
        buf.putShort((short) 4);  // field_size
        buf.putInt(messageNumber);
        return buf.array();
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Response parsers
    // ────────────────────────────────────────────────────────────────────────────

    /** Parse the response type from the first 3 bytes. Returns -1 on error. */
    public static int parseResponseType(byte[] data) {
        if (data.length < 3) return -1;
        return data[2] & 0xFF;
    }

    /** Parse auth response. Returns ack_count, or -1 on auth failure. */
    public static int parseAuthResponse(byte[] data) {
        try {
            int offset = 4;
            // Field 1: status string
            offset++; // field_id
            int fieldLen = readUint16(data, offset);
            offset += 2;
            String status = new String(data, offset, fieldLen, StandardCharsets.UTF_8);
            offset += fieldLen;

            if (!"K".equals(status)) return -1; // auth failed

            // Field 2: ack_count
            offset++; // field_id
            int ackFieldLen = readUint16(data, offset);
            offset += 2;
            return readInt32(data, offset);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Parse a data feed response (resp_type=6).
     * Updates symbolMeta map with multiplier/precision from snapshots.
     * Returns list of parsed ticks.
     */
    public static List<RawTick> parseDataFeed(byte[] data, Map<Integer, SymbolMeta> symbolMeta,
                                               Map<String, String> hsmToFyersSymbol) {
        List<RawTick> ticks = new ArrayList<>();
        try {
            int messageNumber = readInt32(data, 3);
            int scripCount = readUint16(data, 7);
            int offset = 9;

            for (int s = 0; s < scripCount; s++) {
                int dataType = data[offset] & 0xFF;

                if (dataType == 83) {
                    // ── Snapshot ──
                    offset++;
                    int topicId = readUint16LE(data, offset);
                    offset += 2;
                    int nameLen = data[offset] & 0xFF;
                    offset++;
                    String topicName = new String(data, offset, nameLen, StandardCharsets.UTF_8);
                    offset += nameLen;

                    String type = topicName.substring(0, 2); // "sf", "if", "dp"
                    String[] fields = "if".equals(type) ? INDEX_FIELDS : SCRIP_FIELDS;

                    int fieldCount = data[offset] & 0xFF;
                    offset++;

                    Map<String, Integer> rawValues = new java.util.LinkedHashMap<>();
                    for (int i = 0; i < fieldCount && i < fields.length; i++) {
                        int val = readInt32(data, offset);
                        offset += 4;
                        if (val != SENTINEL) {
                            rawValues.put(fields[i], val);
                        }
                    }

                    // Skip 2 unknown bytes
                    offset += 2;

                    // Multiplier and precision
                    int multiplier = readUint16(data, offset);
                    offset += 2;
                    int precision = data[offset] & 0xFF;
                    offset++;

                    // 3 strings: exchange, exchange_token, symbol
                    String exchange = "", exchangeToken = "", symbolName = "";
                    for (int i = 0; i < 3; i++) {
                        int sLen = data[offset] & 0xFF;
                        offset++;
                        String sVal = new String(data, offset, sLen, StandardCharsets.UTF_8);
                        offset += sLen;
                        if (i == 0) exchange = sVal;
                        else if (i == 1) exchangeToken = sVal;
                        else symbolName = sVal;
                    }

                    // Store meta for future incremental updates
                    SymbolMeta meta = new SymbolMeta(topicName, type, multiplier, precision);
                    meta.rawValues.putAll(rawValues);
                    symbolMeta.put(topicId, meta);

                    // Build tick
                    RawTick tick = buildTick(meta, hsmToFyersSymbol);
                    if (tick != null) {
                        tick.messageNumber = messageNumber;
                        ticks.add(tick);
                    }

                } else if (dataType == 85) {
                    // ── Full update ──
                    offset++;
                    int topicId = readUint16LE(data, offset);
                    offset += 2;
                    int fieldCount = data[offset] & 0xFF;
                    offset++;

                    SymbolMeta meta = symbolMeta.get(topicId);
                    String[] fields = (meta != null && "if".equals(meta.type)) ? INDEX_FIELDS : SCRIP_FIELDS;
                    boolean changed = false;

                    for (int i = 0; i < fieldCount && i < fields.length; i++) {
                        int val = readInt32(data, offset);
                        offset += 4;
                        if (val != SENTINEL && meta != null) {
                            Integer prev = meta.rawValues.get(fields[i]);
                            if (prev == null || prev != val) {
                                meta.rawValues.put(fields[i], val);
                                changed = true;
                            }
                        }
                    }

                    if (changed && meta != null) {
                        RawTick tick = buildTick(meta, hsmToFyersSymbol);
                        if (tick != null) {
                            tick.messageNumber = messageNumber;
                            ticks.add(tick);
                        }
                    }

                } else if (dataType == 76) {
                    // ── Lite update (LTP only) ──
                    offset++;
                    int topicId = readUint16LE(data, offset);
                    offset += 2;
                    int ltpRaw = readInt32(data, offset);
                    offset += 4;

                    SymbolMeta meta = symbolMeta.get(topicId);
                    if (meta != null && ltpRaw != SENTINEL) {
                        Integer prevLtp = meta.rawValues.get("ltp");
                        if (prevLtp == null || prevLtp != ltpRaw) {
                            meta.rawValues.put("ltp", ltpRaw);
                            RawTick tick = buildTick(meta, hsmToFyersSymbol);
                            if (tick != null) {
                                tick.messageNumber = messageNumber;
                                ticks.add(tick);
                            }
                        }
                    }

                } else {
                    // Unknown data type — skip cautiously
                    break;
                }
            }
        } catch (Exception e) {
            // Log but don't crash — partial parse is better than none
            System.out.println("[HsmParser] Error parsing data feed: " + e.getMessage());
        }
        return ticks;
    }

    /** Extract message_number from data feed for ACK purposes. */
    public static int extractMessageNumber(byte[] data) {
        if (data.length < 7) return 0;
        return readInt32(data, 3);
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ────────────────────────────────────────────────────────────────────────────

    private static RawTick buildTick(SymbolMeta meta, Map<String, String> hsmToFyersSymbol) {
        double divisor = Math.pow(10, meta.precision) * meta.multiplier;
        if (divisor == 0) divisor = 1;

        Integer ltpRaw = meta.rawValues.get("ltp");
        if (ltpRaw == null) return null;

        RawTick tick = new RawTick();
        tick.hsmToken = meta.topicName;
        tick.fyersSymbol = hsmToFyersSymbol.getOrDefault(meta.topicName, "");
        tick.ltp = ltpRaw / divisor;

        String prevCloseField = "if".equals(meta.type) ? "prev_close_price" : "prev_close_price";
        Integer pcRaw = meta.rawValues.get(prevCloseField);
        if (pcRaw != null) {
            tick.prevClose = pcRaw / divisor;
            tick.change = tick.ltp - tick.prevClose;
            tick.changePercent = tick.prevClose > 0 ? (tick.change / tick.prevClose) * 100 : 0;
        }

        Integer openRaw = meta.rawValues.get("open_price");
        if (openRaw != null) tick.open = openRaw / divisor;

        Integer highRaw = meta.rawValues.get("high_price");
        if (highRaw != null) tick.high = highRaw / divisor;

        Integer lowRaw = meta.rawValues.get("low_price");
        if (lowRaw != null) tick.low = lowRaw / divisor;

        return tick;
    }

    private static byte[] buildScripsData(List<String> hsmTokens) {
        // uint16 count | for each: byte name_len | ascii name
        int size = 2;
        for (String t : hsmTokens) size += 1 + t.length();
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.put((byte) ((hsmTokens.size() >> 8) & 0xFF));
        buf.put((byte) (hsmTokens.size() & 0xFF));
        for (String t : hsmTokens) {
            byte[] bytes = t.getBytes(StandardCharsets.US_ASCII);
            buf.put((byte) bytes.length);
            buf.put(bytes);
        }
        return buf.array();
    }

    private static int readUint16(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    /** Read uint16 in little-endian (topic_id uses LE in Python SDK: struct.unpack("H", ...)) */
    private static int readUint16LE(byte[] data, int offset) {
        return (data[offset] & 0xFF) | ((data[offset + 1] & 0xFF) << 8);
    }

    private static int readInt32(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24) |
               ((data[offset + 1] & 0xFF) << 16) |
               ((data[offset + 2] & 0xFF) << 8) |
               (data[offset + 3] & 0xFF);
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Inner classes
    // ────────────────────────────────────────────────────────────────────────────

    /** Metadata stored per topic_id from snapshot, used for incremental updates. */
    public static class SymbolMeta {
        public final String topicName;
        public final String type; // "sf", "if", "dp"
        public final int multiplier;
        public final int precision;
        public final Map<String, Integer> rawValues = new ConcurrentHashMap<>();

        public SymbolMeta(String topicName, String type, int multiplier, int precision) {
            this.topicName = topicName;
            this.type = type;
            this.multiplier = multiplier;
            this.precision = precision;
        }
    }

    /** Parsed tick from a data feed message. */
    public static class RawTick {
        public String hsmToken;
        public String fyersSymbol;
        public double ltp;
        public double prevClose;
        public double change;
        public double changePercent;
        public double open;
        public double high;
        public double low;
        public int messageNumber;
    }
}
