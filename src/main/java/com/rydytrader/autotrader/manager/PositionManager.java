package com.rydytrader.autotrader.manager;

import java.util.concurrent.atomic.AtomicReference;

public class PositionManager {

    // Thread-safe position holder
    private static final AtomicReference<String> currentPosition =
            new AtomicReference<>("NONE");

    public static String getPosition() {
        return currentPosition.get();
    }

    public static void setPosition(String position) {
        currentPosition.set(position);
        System.out.println("Position Updated: " + position);
    }
}
