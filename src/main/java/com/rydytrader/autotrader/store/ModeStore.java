package com.rydytrader.autotrader.store;

import org.springframework.stereotype.Component;

@Component
public class ModeStore {

    public enum Mode { LIVE, SIMULATOR }

    private volatile Mode mode = Mode.LIVE; // default to live on startup

    public Mode getMode() { return mode; }

    public void setMode(Mode mode) {
        this.mode = mode;
        System.out.println("▶ Trading mode switched to: " + mode);
    }

    public boolean isLive()      { return mode == Mode.LIVE; }
    public boolean isSimulator() { return mode == Mode.SIMULATOR; }
}