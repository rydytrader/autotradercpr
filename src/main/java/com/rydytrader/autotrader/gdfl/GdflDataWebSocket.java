package com.rydytrader.autotrader.gdfl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.function.Consumer;

/**
 * Global Data Feeds (GDFL) WebSocket client for the NFO real-time push feed.
 *
 * <p>Wire protocol (per the GDFL WebSocket API docs at globaldatafeeds.in):
 * <ol>
 *   <li>Client opens a WS connection to {@code wss://<host>:<port>/}.</li>
 *   <li>Client sends {@code {"MessageType":"Authenticate","Password":"<API_KEY>"}} as
 *       the first frame. The API key goes in the {@code Password} field — spelled that
 *       way in the docs.</li>
 *   <li>Server responds with {@code {"Complete":true,"Message":"Welcome!",
 *       "MessageType":"AuthenticateResult"}} on success.</li>
 *   <li>Client sends a {@code SubscribeRealtime} message per symbol:
 *       {@code {"MessageType":"SubscribeRealtime","Exchange":"NFO",
 *       "InstrumentIdentifier":"NIFTY28JUL2624200CE"}}. Only three fields —
 *       {@code Unsubscribe} is NOT part of {@code SubscribeRealtime} per the official
 *       Java sample; it appears on {@code SubscribeSnapshot} but not this one.</li>
 *   <li>Server streams tick frames whose {@code MessageType} is {@code RealtimeResult}
 *       carrying {@code LastTradePrice, LastTradeTime, ServerTime, AverageTradedPrice,
 *       OpenInterest, ...}.</li>
 * </ol>
 *
 * <p>All parsing happens in {@link #onMessage(String)}; the caller-supplied
 * {@link #tickListener} is invoked with the raw {@link JsonNode} so field extraction can
 * live in {@link GdflService} where the symbol mapping + {@code MarketDataService}
 * hand-off happens.
 */
public class GdflDataWebSocket extends WebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(GdflDataWebSocket.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private final String apiKey;
    private final String exchange;
    private final List<String> gdflSymbols;
    private final Consumer<JsonNode> tickListener;
    /** Fired for each server-side aggregated OHLC bar (SnapshotResult push) and
     *  each element of a GetHistory response. Payload is the raw JsonNode carrying
     *  Exchange / InstrumentIdentifier / LastTradeTime / Open / High / Low / Close.
     *  Null when no OHLC subscribers are registered. */
    private final Consumer<JsonNode> ohlcBarListener;
    private final Runnable onDisconnect;

    private volatile boolean authenticated = false;

    /** {@code gdflSymbols} may be empty at construction time — {@link GdflService}
     *  keeps it empty and calls {@link #subscribeSymbol} dynamically once OptionScalping
     *  resolves the day's ATM. Any entries passed in at construct time are sent as
     *  soon as {@code AuthenticateResult} arrives.
     *
     *  <p>{@code ohlcBarListener} receives every {@code SnapshotResult} push and every
     *  {@code HistoryResult} row so {@link GdflService} can hand server-side canonical
     *  OHLC bars to {@link com.rydytrader.autotrader.service.CandleAggregator} for the
     *  trading pair. May be {@code null} when the caller doesn't want OHLC.
     *
     *  <p>{@code onDisconnect} fires from {@link #onClose} on any close (remote drop,
     *  network hiccup, auth reject). {@link GdflService} uses it to schedule a
     *  reconnect. May be {@code null} for callers that don't care. */
    public GdflDataWebSocket(URI endpoint,
                             String apiKey,
                             String exchange,
                             List<String> gdflSymbols,
                             Consumer<JsonNode> tickListener,
                             Consumer<JsonNode> ohlcBarListener,
                             Runnable onDisconnect) {
        super(endpoint);
        this.apiKey          = apiKey;
        this.exchange        = exchange;
        this.gdflSymbols     = gdflSymbols;
        this.tickListener    = tickListener;
        this.ohlcBarListener = ohlcBarListener;
        this.onDisconnect    = onDisconnect;
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        log.info("[GdflWS] connected to {} — sending Authenticate", getURI());
        try {
            ObjectNode auth = mapper.createObjectNode();
            auth.put("MessageType", "Authenticate");
            auth.put("Password",    apiKey);  // GDFL puts the API key in "Password", not "ApiKey"
            send(mapper.writeValueAsString(auth));
        } catch (Exception e) {
            log.warn("[GdflWS] failed to send Authenticate: {}", e.getMessage());
            close();
        }
    }

    @Override
    public void onMessage(String message) {
        JsonNode root;
        try { root = mapper.readTree(message); }
        catch (Exception e) {
            log.warn("[GdflWS] failed to parse frame: {}", e.getMessage());
            return;
        }
        String type = root.path("MessageType").asText("");

        if ("AuthenticateResult".equalsIgnoreCase(type)) {
            boolean ok = root.path("Complete").asBoolean(false);
            log.info("[GdflWS] AuthenticateResult ok={} message={}", ok, root.path("Message").asText(""));
            if (ok) {
                authenticated = true;
                subscribeAll();
            } else {
                log.warn("[GdflWS] authentication rejected — {}", message);
                close();
            }
            return;
        }

        // Every subscribed-symbol tick arrives as RealtimeResult. We hand the raw JsonNode
        // to GdflService which extracts fields under the same schema documented at
        // globaldatafeeds.in/.../function-subscriberealtime.
        if ("RealtimeResult".equalsIgnoreCase(type)) {
            try { tickListener.accept(root); }
            catch (Exception e) { log.warn("[GdflWS] tick listener threw: {}", e.getMessage()); }
            return;
        }

        // Server-side aggregated OHLC bar push — one per subscribed symbol per bar close.
        // Fields per GDFL docs: Exchange, InstrumentIdentifier, LastTradeTime (bar close
        // epoch sec), TradedQty, OpenInterest, Open, High, Low, Close. Same schema used
        // for GetHistoryResult rows below.
        if ("SnapshotResult".equalsIgnoreCase(type)) {
            if (ohlcBarListener != null) {
                try { ohlcBarListener.accept(root); }
                catch (Exception e) { log.warn("[GdflWS] snapshot listener threw: {}", e.getMessage()); }
            }
            return;
        }

        // GetHistory response — a batch of OHLC bars. GDFL nests them under a "Result"
        // array of objects with the same field schema as SnapshotResult. We fan each
        // row through the same OHLC listener to reuse the CandleAggregator overwrite
        // path. Response envelope carries the Exchange + InstrumentIdentifier at the
        // top level, so we stamp each row with those before delivering.
        if ("HistoryResult".equalsIgnoreCase(type) || "GetHistoryResult".equalsIgnoreCase(type)) {
            if (ohlcBarListener == null) return;
            String topExchange = root.path("Exchange").asText(exchange);
            String topSymbol   = root.path("InstrumentIdentifier").asText("");
            JsonNode rows = root.has("Result") ? root.path("Result")
                          : root.has("Data")   ? root.path("Data")
                          : root;
            if (rows != null && rows.isArray()) {
                for (JsonNode row : rows) {
                    if (row == null || !row.isObject()) continue;
                    ObjectNode stamped = (ObjectNode) row;
                    if (!stamped.has("Exchange") && !topExchange.isEmpty()) stamped.put("Exchange", topExchange);
                    if (!stamped.has("InstrumentIdentifier") && !topSymbol.isEmpty()) stamped.put("InstrumentIdentifier", topSymbol);
                    try { ohlcBarListener.accept(stamped); }
                    catch (Exception e) { log.warn("[GdflWS] history row listener threw: {}", e.getMessage()); }
                }
            }
            return;
        }

        // Subscribe / unsubscribe ACKs and any error frames — log for now, don't crash.
        log.debug("[GdflWS] frame type={} body={}", type, message);
    }

    private void subscribeAll() {
        for (String gdflSym : gdflSymbols) subscribeSymbol(gdflSym);
    }

    /** Sends {@code SubscribeRealtime} for one GDFL identifier. Public so
     *  {@link GdflService} can send subscribes AFTER connect + auth, once OptionScalping has
     *  resolved today's ATM. Returns {@code true} when the send completed, {@code false}
     *  on any error (caller may retry). Per the official Java sample the payload has
     *  exactly three fields — no {@code Unsubscribe}, which lives on
     *  {@code SubscribeSnapshot} instead. */
    public boolean subscribeSymbol(String gdflSym) {
        if (gdflSym == null || gdflSym.isBlank()) return false;
        if (!authenticated) {
            log.warn("[GdflWS] subscribe requested for {} but not authenticated yet — skipping",
                gdflSym);
            return false;
        }
        try {
            ObjectNode sub = mapper.createObjectNode();
            sub.put("MessageType",         "SubscribeRealtime");
            sub.put("Exchange",            exchange);
            sub.put("InstrumentIdentifier", gdflSym);
            send(mapper.writeValueAsString(sub));
            log.info("[GdflWS] subscribed {}", gdflSym);
            return true;
        } catch (Exception e) {
            log.warn("[GdflWS] subscribe failed for {}: {}", gdflSym, e.getMessage());
            return false;
        }
    }

    /** Sends {@code SubscribeSnapshot} for one symbol. GDFL will push a
     *  {@code SnapshotResult} frame on every bar close for the requested
     *  {@code (Periodicity, Period)}. Server-side aggregated (sees every trade)
     *  so the OHLC matches TradingView.
     *
     *  <p>Common call: {@code subscribeSnapshot("NIFTY23500CE", "MINUTE", 1)}.
     *  Costs one slot of the account's 50-symbol WS cap ON TOP of any
     *  {@link #subscribeSymbol} slot for the same symbol. */
    public boolean subscribeSnapshot(String gdflSym, String periodicity, int period) {
        if (gdflSym == null || gdflSym.isBlank()) return false;
        if (!authenticated) {
            log.warn("[GdflWS] snapshot subscribe requested for {} but not authenticated yet",
                gdflSym);
            return false;
        }
        try {
            ObjectNode sub = mapper.createObjectNode();
            sub.put("MessageType",         "SubscribeSnapshot");
            sub.put("Exchange",            exchange);
            sub.put("InstrumentIdentifier", gdflSym);
            sub.put("Periodicity",         periodicity);
            sub.put("Period",              period);
            sub.put("Unsubscribe",         false);
            send(mapper.writeValueAsString(sub));
            log.info("[GdflWS] snapshot-subscribed {} ({}x{})", gdflSym, periodicity, period);
            return true;
        } catch (Exception e) {
            log.warn("[GdflWS] snapshot subscribe failed for {}: {}", gdflSym, e.getMessage());
            return false;
        }
    }

    /** One-shot request for the most recent {@code maxRecords} OHLC bars for
     *  one symbol. Used to backfill the very first bar of the session (which
     *  the SubscribeSnapshot push misses since the subscription is placed AFTER
     *  the 09:16 ATM lock — the 09:15 → 09:16 bar has already closed).
     *
     *  <p>Response arrives as a {@code HistoryResult} frame; {@link #onMessage}
     *  fans each row through {@code ohlcBarListener}. */
    public boolean getHistory(String gdflSym, String periodicity, int period, int maxRecords) {
        if (gdflSym == null || gdflSym.isBlank()) return false;
        if (!authenticated) {
            log.warn("[GdflWS] getHistory requested for {} but not authenticated yet", gdflSym);
            return false;
        }
        try {
            ObjectNode req = mapper.createObjectNode();
            req.put("MessageType",         "GetHistory");
            req.put("Exchange",            exchange);
            req.put("InstrumentIdentifier", gdflSym);
            req.put("Periodicity",         periodicity);
            req.put("Period",              period);
            req.put("MaxNoOfRecords",      Math.max(1, maxRecords));
            send(mapper.writeValueAsString(req));
            log.info("[GdflWS] getHistory {} ({}x{}, last {})", gdflSym, periodicity, period, maxRecords);
            return true;
        } catch (Exception e) {
            log.warn("[GdflWS] getHistory failed for {}: {}", gdflSym, e.getMessage());
            return false;
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        authenticated = false;
        log.info("[GdflWS] closed code={} reason={} remote={}", code, reason, remote);
        if (onDisconnect != null) {
            try { onDisconnect.run(); }
            catch (Exception e) { log.warn("[GdflWS] onDisconnect callback threw: {}", e.getMessage()); }
        }
    }

    @Override
    public void onError(Exception ex) {
        log.warn("[GdflWS] error: {}", ex.getMessage());
    }

    public boolean isAuthenticated() { return authenticated; }
}
