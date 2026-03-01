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
    private final Map<String, Map<String, Object>> orders = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> tradebook = Collections.synchronizedList(new ArrayList<>());
    private volatile Map<String, Object> position = null;

    // Control panel settings
    private volatile String  activeSymbol = "NSE:NIFTY25FEBFUT";
    private volatile double  currentPrice = 22400.00;
    private volatile boolean autoFill     = true;
    private volatile int     fillDelayMs  = 500;

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
        log("CANCEL: " + id);
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
        log("FILLED: " + id + " @ " + fillPrice + " | tag=" + order.get("orderTag"));
    }

    private void updatePosition(Map<String, Object> order, double fillPrice) {
        int side = toInt(order.get("side"));
        int qty  = toInt(order.get("qty"));
        String symbol = (String) order.get("symbol");

        if (position == null) {
            position = new LinkedHashMap<>();
            position.put("symbol",       symbol);
            position.put("productType",  "INTRADAY");
            position.put("side",         side == 1 ? 1 : -1);
            position.put("netQty",       side == 1 ? qty : -qty);
            position.put("netAvgPrice",  fillPrice);
            position.put("buyAvg",       side == 1 ? fillPrice : 0.0);
            position.put("sellAvg",      side == -1 ? fillPrice : 0.0);
            position.put("buyQty",       side == 1 ? qty : 0);
            position.put("sellQty",      side == -1 ? qty : 0);
            position.put("unrealizedProfit", 0.0);
        } else {
            int currentNet = toInt(position.get("netQty"));
            int newNet     = currentNet + (side == 1 ? qty : -qty);
            if (newNet == 0) { position = null; log("POSITION CLOSED"); }
            else { position.put("netQty", newNet); position.put("side", newNet > 0 ? 1 : -1); }
        }
    }

    // ── SCENARIO TRIGGERS ─────────────────────────────────────────────────────
    public void triggerSlHit() {
        orders.values().stream()
            .filter(o -> "AutoSL".equals(o.get("orderTag")) && toInt(o.get("status")) == STATUS_PENDING)
            .findFirst().ifPresent(o -> {
                double slPrice = toDouble(o.get("stopPrice"));
                log("SCENARIO: SL HIT @ " + slPrice);
                fillOrder((String) o.get("id"), slPrice);
                cancelByTag("AutoTarget");
            });
    }

    public void triggerTargetHit() {
        orders.values().stream()
            .filter(o -> "AutoTarget".equals(o.get("orderTag")) && toInt(o.get("status")) == STATUS_PENDING)
            .findFirst().ifPresent(o -> {
                double tp = toDouble(o.get("limitPrice"));
                log("SCENARIO: TARGET HIT @ " + tp);
                fillOrder((String) o.get("id"), tp);
                cancelByTag("AutoSL");
            });
    }

    public void triggerManualSquareOff() {
        if (position == null) { log("SCENARIO: No open position to square off"); return; }
        log("SCENARIO: Manual square-off — position closed at " + currentPrice);
        // Add closing trade at current market price
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("id", "T_SQF_" + System.currentTimeMillis());
        t.put("symbol", activeSymbol);
        t.put("side", toInt(position.get("netQty")) > 0 ? -1 : 1);
        t.put("tradedQty", Math.abs(toInt(position.get("netQty"))));
        t.put("tradePrice", currentPrice);
        t.put("orderTag", "ManualSquareOff");
        t.put("timestamp", now());
        tradebook.add(t);
        // Clear position BEFORE cancelling orders so OCO monitor skips "still active" warnings
        position = null;
        // Cancel all pending orders silently (logs handled by SimulatorController)
        orders.values().stream()
            .filter(o -> toInt(o.get("status")) == STATUS_PENDING)
            .forEach(o -> o.put("status", STATUS_CANCELLED));
    }

    public void resetAll() {
        orders.clear();
        tradebook.clear();
        position = null;
        eventLog.clear();
        log("STATE RESET");
    }

    private void cancelByTag(String tag) {
        orders.values().stream()
            .filter(o -> tag.equals(o.get("orderTag")) && toInt(o.get("status")) == STATUS_PENDING)
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
    public List<Map<String, Object>>       getTradebook()  { return tradebook; }
    public Map<String, Object>             getPosition()   { return position; }
    public String  getActiveSymbol() { return activeSymbol; }
    public double  getCurrentPrice() { return currentPrice; }
    public boolean isAutoFill()      { return autoFill; }
    public int     getFillDelayMs()  { return fillDelayMs; }
    public List<String> getEventLog(){ return eventLog; }

    public void setActiveSymbol(String s) { activeSymbol = s; log("Symbol → " + s); }
    public void setCurrentPrice(double p) { currentPrice = p; }
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