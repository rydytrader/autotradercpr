package com.rydytrader.autotrader.manager;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PositionManager {

    // Thread-safe per-symbol position holder
    private static final ConcurrentHashMap<String, String> positions = new ConcurrentHashMap<>();

    /** Returns "LONG", "SHORT", or "NONE" for the given symbol. */
    public static String getPosition(String symbol) {
        return positions.getOrDefault(symbol, "NONE");
    }

    /** Sets or clears the position for the given symbol. */
    public static void setPosition(String symbol, String side) {
        if ("NONE".equals(side)) {
            positions.remove(symbol);
        } else {
            positions.put(symbol, side);
        }
        System.out.println("Position Updated [" + symbol + "]: " + side);
    }

    /** Returns true if any symbol has an open position. */
    public static boolean hasAnyPosition() {
        return !positions.isEmpty();
    }

    /** Returns the set of symbols with an open position. */
    public static Set<String> getAllSymbols() {
        return positions.keySet();
    }

    /** Clears all tracked positions (used on reset). */
    public static void resetAll() {
        positions.clear();
        System.out.println("PositionManager: all positions cleared");
    }
}
