package com.rydytrader.autotrader.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Persists risk management settings to disk so they survive server restarts.
 * Live settings:      store/live/risk-settings.json
 * Simulator settings: store/simulator/risk-settings.json
 */
@Component
public class RiskSettingsStore {

    private static final String LIVE_FILE = "../store/live/risk-settings.json";
    private static final String SIM_FILE  = "../store/simulator/risk-settings.json";
    private static final String LEGACY_FILE = "../store/risk-settings.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    private ModeStore modeStore;

    // ── per-mode settings containers ─────────────────────────────────────────
    static class Cfg {
        volatile String tradingStartTime  = "09:15";
        volatile String tradingEndTime    = "15:25";
        volatile double maxDailyLoss      = 0;  // 0 = disabled
        volatile int    maxTradesPerDay   = 0;  // 0 = disabled
        volatile String autoSquareOffTime = "";  // empty = disabled, e.g. "15:15"
        volatile double atrMultiplier     = 1.5; // SL = close ± (ATR × this)
        volatile boolean enableR4S4       = false; // allow BUY_ABOVE_R4 / SELL_BELOW_S4
        volatile double sessionMoveLimit = 2.0;   // qty halved if session move exceeds this % (0 = disabled)
    }

    private final Cfg live = new Cfg();
    private final Cfg sim  = new Cfg();

    public RiskSettingsStore() {
        new File("../store/live").mkdirs();
        new File("../store/simulator").mkdirs();
        migrate();
        load("live");
        load("simulator");
    }

    @Autowired
    public void setModeStore(ModeStore modeStore) {
        this.modeStore = modeStore;
    }

    // ── route to active mode ──────────────────────────────────────────────────
    private Cfg cfg() {
        return (modeStore == null || modeStore.isLive()) ? live : sim;
    }

    public Cfg cfgFor(String mode) {
        return "live".equalsIgnoreCase(mode) ? live : sim;
    }

    private String fileFor(String mode) {
        return "live".equalsIgnoreCase(mode) ? LIVE_FILE : SIM_FILE;
    }

    // ── parameterless getters/setters (used by TradingController etc.) ────────
    public String getTradingStartTime()  { return cfg().tradingStartTime; }
    public String getTradingEndTime()    { return cfg().tradingEndTime; }
    public double getMaxDailyLoss()      { return cfg().maxDailyLoss; }
    public int    getMaxTradesPerDay()   { return cfg().maxTradesPerDay; }
    public String getAutoSquareOffTime() { return cfg().autoSquareOffTime; }
    public double getAtrMultiplier()     { return cfg().atrMultiplier; }
    public boolean isEnableR4S4()        { return cfg().enableR4S4; }
    public double getSessionMoveLimit() { return cfg().sessionMoveLimit; }

    public void setTradingStartTime(String v)  { cfg().tradingStartTime = v; }
    public void setTradingEndTime(String v)    { cfg().tradingEndTime = v; }
    public void setMaxDailyLoss(double v)      { cfg().maxDailyLoss = v; }
    public void setMaxTradesPerDay(int v)      { cfg().maxTradesPerDay = v; }
    public void setAutoSquareOffTime(String v) { cfg().autoSquareOffTime = v; }
    public void setAtrMultiplier(double v)     { cfg().atrMultiplier = v; }
    public void setEnableR4S4(boolean v)       { cfg().enableR4S4 = v; }
    public void setSessionMoveLimit(double v) { cfg().sessionMoveLimit = v; }

    // ── mode-specific getters/setters (used by SettingsController) ────────────
    public String getTradingStartTime(String mode)  { return cfgFor(mode).tradingStartTime; }
    public String getTradingEndTime(String mode)    { return cfgFor(mode).tradingEndTime; }
    public double getMaxDailyLoss(String mode)      { return cfgFor(mode).maxDailyLoss; }
    public int    getMaxTradesPerDay(String mode)    { return cfgFor(mode).maxTradesPerDay; }
    public String getAutoSquareOffTime(String mode) { return cfgFor(mode).autoSquareOffTime; }
    public double getAtrMultiplier(String mode)     { return cfgFor(mode).atrMultiplier; }
    public boolean isEnableR4S4(String mode)       { return cfgFor(mode).enableR4S4; }
    public double getSessionMoveLimit(String mode) { return cfgFor(mode).sessionMoveLimit; }

    public void setTradingStartTime(String mode, String v)  { cfgFor(mode).tradingStartTime = v; }
    public void setTradingEndTime(String mode, String v)    { cfgFor(mode).tradingEndTime = v; }
    public void setMaxDailyLoss(String mode, double v)      { cfgFor(mode).maxDailyLoss = v; }
    public void setMaxTradesPerDay(String mode, int v)      { cfgFor(mode).maxTradesPerDay = v; }
    public void setAutoSquareOffTime(String mode, String v) { cfgFor(mode).autoSquareOffTime = v; }
    public void setAtrMultiplier(String mode, double v)     { cfgFor(mode).atrMultiplier = v; }
    public void setEnableR4S4(String mode, boolean v)       { cfgFor(mode).enableR4S4 = v; }
    public void setSessionMoveLimit(String mode, double v) { cfgFor(mode).sessionMoveLimit = v; }

    // ── save ──────────────────────────────────────────────────────────────────
    /** Saves the currently active mode's settings. */
    public void save() {
        String activeMode = (modeStore == null || modeStore.isLive()) ? "live" : "simulator";
        saveFor(activeMode);
    }

    /** Saves the specified mode's settings. */
    public void saveFor(String mode) {
        Cfg c = cfgFor(mode);
        try {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("tradingStartTime",  c.tradingStartTime);
            state.put("tradingEndTime",    c.tradingEndTime);
            state.put("maxDailyLoss",      c.maxDailyLoss);
            state.put("maxTradesPerDay",   c.maxTradesPerDay);
            state.put("autoSquareOffTime", c.autoSquareOffTime);
            state.put("atrMultiplier",    c.atrMultiplier);
            state.put("enableR4S4",      c.enableR4S4);
            state.put("sessionMoveLimit", c.sessionMoveLimit);
            Files.writeString(Paths.get(fileFor(mode)), mapper.writeValueAsString(state));
        } catch (IOException e) {
            System.err.println("[RiskSettingsStore] Failed to save " + mode + ": " + e.getMessage());
        }
    }

    // ── load ──────────────────────────────────────────────────────────────────
    @SuppressWarnings("unchecked")
    private void load(String mode) {
        try {
            Path path = Paths.get(fileFor(mode));
            if (!Files.exists(path)) return;
            Map<String, Object> state = mapper.readValue(Files.readString(path), Map.class);
            Cfg c = cfgFor(mode);
            if (state.containsKey("tradingStartTime"))  c.tradingStartTime  = state.get("tradingStartTime").toString();
            if (state.containsKey("tradingEndTime"))    c.tradingEndTime    = state.get("tradingEndTime").toString();
            if (state.containsKey("maxDailyLoss"))      c.maxDailyLoss      = Double.parseDouble(state.get("maxDailyLoss").toString());
            if (state.containsKey("maxTradesPerDay"))   c.maxTradesPerDay   = Integer.parseInt(state.get("maxTradesPerDay").toString());
            if (state.containsKey("autoSquareOffTime")) c.autoSquareOffTime = state.get("autoSquareOffTime").toString();
            if (state.containsKey("atrMultiplier"))    c.atrMultiplier    = Double.parseDouble(state.get("atrMultiplier").toString());
            if (state.containsKey("enableR4S4"))       c.enableR4S4       = Boolean.parseBoolean(state.get("enableR4S4").toString());
            if (state.containsKey("sessionMoveLimit")) c.sessionMoveLimit = Double.parseDouble(state.get("sessionMoveLimit").toString());
            System.out.println("[RiskSettingsStore] Loaded " + mode + ": start=" + c.tradingStartTime
                + " end=" + c.tradingEndTime + " maxLoss=" + c.maxDailyLoss + " maxTrades=" + c.maxTradesPerDay
                + " autoSquareOff=" + c.autoSquareOffTime + " atrMult=" + c.atrMultiplier
                + " enableR4S4=" + c.enableR4S4 + " sessionMove=" + c.sessionMoveLimit + "%");
        } catch (IOException e) {
            System.err.println("[RiskSettingsStore] Failed to load " + mode + ": " + e.getMessage());
        }
    }

    /** Migrates legacy logs/risk-settings.json to both mode-specific files on first run. */
    private void migrate() {
        Path legacy = Paths.get(LEGACY_FILE);
        Path livePath = Paths.get(LIVE_FILE);
        Path simPath  = Paths.get(SIM_FILE);
        if (!Files.exists(legacy)) return;
        if (Files.exists(livePath) && Files.exists(simPath)) return; // already migrated
        try {
            String json = Files.readString(legacy);
            if (!Files.exists(livePath)) Files.writeString(livePath, json);
            if (!Files.exists(simPath))  Files.writeString(simPath,  json);
            System.out.println("[RiskSettingsStore] Migrated legacy risk-settings.json to live/ and simulator/");
        } catch (IOException e) {
            System.err.println("[RiskSettingsStore] Migration failed: " + e.getMessage());
        }
    }
}
