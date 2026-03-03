package com.rydytrader.autotrader.mock;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class MockState {

    public static final int STATUS_PENDING   = 1;
    public static final int STATUS_TRADED    = 2;
    public static final int STATUS_CANCELLED = 5;

    private final AtomicInteger orderCounter = new AtomicInteger(100001);
    private final Map<String, Map<String, Object>> orders    = new ConcurrentHashMap<>();
    private final List<Map<String, Object>>        tradebook = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentHashMap<String, Map<String, Object>> positions = new ConcurrentHashMap<>();

    // Control panel settings
    private volatile String  activeSymbol = "NSE:NIFTY25FEBFUT";
    private volatile double  currentPrice = 22400.00;
    private volatile boolean autoFill     = true;
    private volatile int     fillDelayMs  = 100;
    private final ConcurrentHashMap<String, Double> priceBySymbol = new ConcurrentHashMap<>();

    private final List<String> eventLog = Collections.synchronizedList(new ArrayList<>());

    // ── ORDER OPS ─────────────────────────────────────────────────────────────
    public String nextOrderId() { return "SIM" + orderCounter.getAndIncrement(); }

    public void addOrder(Map<String, Object> order) {
        order.put("timestamp", now());
        orders.put((String) order.get("id"), order);
        log("ORDER: " + order.get("id") + " | " + order.get("symbol")
                + " | type=" + order.get("type") + " | side=" + order.get("side")
                + " | qty=" + order.get("qty"));
    }

    public Map<String, Object> getOrder(String id) { return orders.get(id); }

    public boolean cancelOrder(String id) {
        Map<String, Object> order = orders.get(id);
        if (order == null) return false;
        order.put("status", STATUS_CANCELLED);
        log("CANCEL: " + id + " [" + order.get("symbol") + "]");
        return true;
    }

    public void fillOrder(String id, double fillPrice) {
        Map<String, Object> order = orders.get(id);
        if (order == null || (int) order.get("status") != STATUS_PENDING) return;
        order.put("status",      STATUS_TRADED);
        order.put("tradedPrice", fillPrice);

        Map<String, Object> trade = new LinkedHashMap<>();
        trade.put("id",         "T" + id);
        trade.put("orderId",    id);
        trade.put("symbol",     order.get("symbol"));
        trade.put("side",       order.get("side"));
        trade.put("tradedQty",  order.get("qty"));
        trade.put("tradePrice", fillPrice);
        trade.put("orderTag",   order.get("orderTag"));
        trade.put("timestamp",  now());
        tradebook.add(trade);

        updatePosition(order, fillPrice);
        log("FILLED: " + id + " @ " + fillPrice + " [" + order.get("symbol") + "] | tag=" + order.get("orderTag"));
    }

    private void updatePosition(Map<String, Object> order, double fillPrice) {
        int    side   = toInt(order.get("side"));
        int    qty    = toInt(order.get("qty"));
        String symbol = (String) order.get("symbol");

        Map<String, Object> pos = positions.get(symbol);
        if (pos == null) {
            pos = new LinkedHashMap<>();
            pos.put("symbol",           symbol);
            pos.put("productType",      "INTRADAY");
            pos.put("side",             side == 1 ? 1 : -1);
            pos.put("netQty",           side == 1 ? qty : -qty);
            pos.put("netAvgPrice",      fillPrice);
            pos.put("buyAvg",           side == 1 ? fillPrice : 0.0);
            pos.put("sellAvg",          side == -1 ? fillPrice : 0.0);
            pos.put("buyQty",           side == 1 ? qty : 0);
            pos.put("sellQty",          side == -1 ? qty : 0);
            pos.put("unrealizedProfit", 0.0);
            positions.put(symbol, pos);
            activeSymbol = symbol;  // track most recently opened position's symbol
        } else {
            int currentNet = toInt(pos.get("netQty"));
            int newNet     = currentNet + (side == 1 ? qty : -qty);
            if (newNet == 0) {
                positions.remove(symbol);
                log("POSITION CLOSED [" + symbol + "]");
            } else {
                pos.put("netQty", newNet);
                pos.put("side", newNet > 0 ? 1 : -1);
            }
        }
    }

    // ── SCENARIO TRIGGERS ─────────────────────────────────────────────────────
    public void triggerSlHit(String symbol) {
        orders.values().stream()
            .filter(o -> symbol.equals(o.get("symbol"))
                      && "AutoSL".equals(o.get("orderTag"))
                      && toInt(o.get("status")) == STATUS_PENDING)
            .findFirst().ifPresent(o -> {
                double slPrice = toDouble(o.get("stopPrice"));
                log("SCENARIO: SL HIT @ " + slPrice + " [" + symbol + "]");
                fillOrder((String) o.get("id"), slPrice);
                cancelBySymbolAndTag(symbol, "AutoTarget");
            });
    }

    public void triggerTargetHit(String symbol) {
        orders.values().stream()
            .filter(o -> symbol.equals(o.get("symbol"))
                      && "AutoTarget".equals(o.get("orderTag"))
                      && toInt(o.get("status")) == STATUS_PENDING)
            .findFirst().ifPresent(o -> {
                double tp = toDouble(o.get("limitPrice"));
                log("SCENARIO: TARGET HIT @ " + tp + " [" + symbol + "]");
                fillOrder((String) o.get("id"), tp);
                cancelBySymbolAndTag(symbol, "AutoSL");
            });
    }

    public void triggerManualSquareOff(String symbol) {
        Map<String, Object> pos = positions.get(symbol);
        if (pos == null) { log("SCENARIO: No open position to square off [" + symbol + "]"); return; }
        log("SCENARIO: Manual square-off — position closed at " + currentPrice + " [" + symbol + "]");
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("id",         "T_SQF_" + System.currentTimeMillis());
        t.put("symbol",     symbol);
        t.put("side",       toInt(pos.get("netQty")) > 0 ? -1 : 1);
        t.put("tradedQty",  Math.abs(toInt(pos.get("netQty"))));
        t.put("tradePrice", currentPrice);
        t.put("orderTag",   "ManualSquareOff");
        t.put("timestamp",  now());
        tradebook.add(t);
        // Clear position before cancelling so OCO monitor skips "still active" warnings
        positions.remove(symbol);
        // Cancel all pending orders for this symbol silently
        orders.values().stream()
            .filter(o -> symbol.equals(o.get("symbol")) && toInt(o.get("status")) == STATUS_PENDING)
            .forEach(o -> o.put("status", STATUS_CANCELLED));
    }

    public void resetAll() {
        orders.clear();
        tradebook.clear();
        positions.clear();
        priceBySymbol.clear();
        eventLog.clear();
        log("STATE RESET");
    }

    private void cancelBySymbolAndTag(String symbol, String tag) {
        orders.values().stream()
            .filter(o -> symbol.equals(o.get("symbol"))
                      && tag.equals(o.get("orderTag"))
                      && toInt(o.get("status")) == STATUS_PENDING)
            .forEach(o -> cancelOrder((String) o.get("id")));
    }

    // ── GETTERS / SETTERS ─────────────────────────────────────────────────────
    public Collection<Map<String, Object>> getAllOrders() {
        return orders.values().stream()
            .sorted(Comparator.comparing((Map<String, Object> o) -> {
                String id = o.getOrDefault("id", "SIM0").toString().replaceAll("[^0-9]", "");
                return id.isEmpty() ? 0 : Integer.parseInt(id);
            }).reversed())
            .collect(java.util.stream.Collectors.toList());
    }

    public List<Map<String, Object>>             getTradebook()             { return tradebook; }
    public Map<String, Object>                   getPosition(String symbol) { return positions.get(symbol); }
    public Collection<Map<String, Object>>       getAllPositions()           { return positions.values(); }
    /** Returns first open position or null — used for single-symbol simulator panel display. */
    public Map<String, Object>                   getPosition()              { return positions.isEmpty() ? null : positions.values().iterator().next(); }
    public String  getActiveSymbol() { return activeSymbol; }
    public double  getCurrentPrice() { return currentPrice; }
    public boolean isAutoFill()      { return autoFill; }
    public int     getFillDelayMs()  { return fillDelayMs; }
    public List<String> getEventLog(){ return eventLog; }

    public void restorePosition(String symbol, String side, int qty, double avgPrice) {
        Map<String, Object> pos = new LinkedHashMap<>();
        pos.put("symbol",           symbol);
        pos.put("productType",      "INTRADAY");
        pos.put("side",             "LONG".equals(side) ? 1 : -1);
        pos.put("netQty",           "LONG".equals(side) ? qty : -qty);
        pos.put("netAvgPrice",      avgPrice);
        pos.put("buyAvg",           "LONG".equals(side) ? avgPrice : 0.0);
        pos.put("sellAvg",          "SHORT".equals(side) ? avgPrice : 0.0);
        pos.put("buyQty",           "LONG".equals(side) ? qty : 0);
        pos.put("sellQty",          "SHORT".equals(side) ? qty : 0);
        pos.put("unrealizedProfit", 0.0);
        positions.put(symbol, pos);
        activeSymbol = symbol;
        log("RESTORED position: " + side + " " + qty + " @ " + avgPrice + " [" + symbol + "]");
    }

    public void setActiveSymbol(String s) { activeSymbol = s; log("Symbol → " + s); }
    public void setCurrentPrice(double p) { currentPrice = p; }
    public void setCurrentPrice(String symbol, double price) { priceBySymbol.put(symbol, price); }
    public double getCurrentPrice(String symbol) { return priceBySymbol.getOrDefault(symbol, currentPrice); }
    public Map<String, Double> getPriceBySymbol() { return priceBySymbol; }
    public void setAutoFill(boolean b)    { autoFill = b;  log("AutoFill → " + b); }
    public void setFillDelayMs(int ms)    { fillDelayMs = ms; }

    // ── LOG ───────────────────────────────────────────────────────────────────
    public void log(String msg) {
        String e = "[" + now() + "] " + msg;
        System.out.println("SIM | " + e);
        eventLog.add(0, e);
        if (eventLog.size() > 200) eventLog.remove(eventLog.size() - 1);
    }

    private String now() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
    private int    toInt(Object v)    { return v == null ? 0 : Integer.parseInt(v.toString()); }
    private double toDouble(Object v) { return v == null ? 0.0 : Double.parseDouble(v.toString()); }
}
