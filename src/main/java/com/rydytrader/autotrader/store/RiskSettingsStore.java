package com.rydytrader.autotrader.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(RiskSettingsStore.class);

    private static final String LIVE_FILE = "../store/live/risk-settings.json";
    private static final String SIM_FILE  = "../store/simulator/risk-settings.json";
    private static final String LEGACY_FILE = "../store/risk-settings.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    private ModeStore modeStore;

    // ── per-mode settings containers ─────────────────────────────────────────
    static class Cfg {
        volatile String tradingStartTime  = "09:15";
        volatile String tradingEndTime    = "15:25";
        volatile double totalCapital      = 0;     // total trading capital in ₹
        volatile double maxRiskPerDayPct  = 1.0;  // max risk per day as % of totalCapital
        volatile double riskPerTrade      = 1000;  // max ₹ loss per trade if SL hits
        volatile String autoSquareOffTime = "";  // empty = disabled, e.g. "15:15"
        volatile double atrMultiplier     = 1.5; // SL = close ± (ATR × this)
        volatile boolean enableR4S4       = false; // allow BUY_ABOVE_R4 / SELL_BELOW_S4
        volatile double sessionMoveLimit = 2.0;   // qty halved if session move exceeds this % (0 = disabled)
        volatile double brokeragePerOrder = 20.0;  // flat brokerage per order in ₹ (Fyers default)
        volatile int    fixedQuantity    = 2;     // -1 = use capital-based calculation
        volatile double capitalPerTrade  = 0;     // ₹ per trade (used when fixedQuantity == -1)
        volatile int    telegramAlertFrequency = 60; // seconds between Telegram portfolio updates (0 = disabled)
        volatile boolean enableLargeCandleFilter = true; // reject trade if candle > largeCandleAtrThreshold ATR from breakout level
        volatile double largeCandleAtrThreshold = 1.0; // ATR multiplier for large candle filter
        volatile boolean enableTargetShift = true; // shift target to next level if default target < 1 ATR. If false, skip the entry.
        volatile boolean enableSessionTargetCap = true; // cap target at session high/low if it's between close and target
        volatile boolean enableSmallCandleFilter = false; // reject if candle move from breakout level < smallCandleAtrThreshold ATR
        volatile double smallCandleAtrThreshold = 0.5; // ATR multiplier for small candle filter
        volatile double trailTriggerPct = 75;  // % of range from entry to target that triggers trailing SL
        volatile double trailSlPct      = 50;  // % of range to lock as profit when trailing SL triggers
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
    public double getTotalCapital()       { return cfg().totalCapital; }
    public double getMaxRiskPerDayPct()  { return cfg().maxRiskPerDayPct; }
    public double getRiskPerTrade()      { return cfg().riskPerTrade; }
    public double getMaxDailyLoss()      { return cfg().totalCapital * cfg().maxRiskPerDayPct / 100.0; }
    public String getAutoSquareOffTime() { return cfg().autoSquareOffTime; }
    public double getAtrMultiplier()     { return cfg().atrMultiplier; }
    public boolean isEnableR4S4()        { return cfg().enableR4S4; }
    public double getSessionMoveLimit() { return cfg().sessionMoveLimit; }
    public double getBrokeragePerOrder() { return cfg().brokeragePerOrder; }
    public int    getFixedQuantity()   { return cfg().fixedQuantity; }
    public double getCapitalPerTrade() { return cfg().capitalPerTrade; }
    public int    getTelegramAlertFrequency() { return cfg().telegramAlertFrequency; }
    public boolean isEnableLargeCandleFilter() { return cfg().enableLargeCandleFilter; }
    public double getLargeCandleAtrThreshold() { return cfg().largeCandleAtrThreshold; }
    public boolean isEnableTargetShift() { return cfg().enableTargetShift; }
    public boolean isEnableSessionTargetCap() { return cfg().enableSessionTargetCap; }
    public boolean isEnableSmallCandleFilter() { return cfg().enableSmallCandleFilter; }
    public double getTrailTriggerPct() { return cfg().trailTriggerPct; }
    public double getTrailSlPct()      { return cfg().trailSlPct; }
    public double getSmallCandleAtrThreshold() { return cfg().smallCandleAtrThreshold; }

    public void setTradingStartTime(String v)  { cfg().tradingStartTime = v; }
    public void setTradingEndTime(String v)    { cfg().tradingEndTime = v; }
    public void setTotalCapital(double v)       { cfg().totalCapital = v; }
    public void setMaxRiskPerDayPct(double v)  { cfg().maxRiskPerDayPct = v; }
    public void setRiskPerTrade(double v)      { cfg().riskPerTrade = v; }
    public void setAutoSquareOffTime(String v) { cfg().autoSquareOffTime = v; }
    public void setAtrMultiplier(double v)     { cfg().atrMultiplier = v; }
    public void setEnableR4S4(boolean v)       { cfg().enableR4S4 = v; }
    public void setSessionMoveLimit(double v) { cfg().sessionMoveLimit = v; }
    public void setBrokeragePerOrder(double v) { cfg().brokeragePerOrder = v; }
    public void setFixedQuantity(int v)      { cfg().fixedQuantity = v; }
    public void setCapitalPerTrade(double v) { cfg().capitalPerTrade = v; }
    public void setTelegramAlertFrequency(int v) { cfg().telegramAlertFrequency = v; }
    public void setEnableLargeCandleFilter(boolean v) { cfg().enableLargeCandleFilter = v; }
    public void setLargeCandleAtrThreshold(double v) { cfg().largeCandleAtrThreshold = v; }
    public void setEnableTargetShift(boolean v) { cfg().enableTargetShift = v; }
    public void setEnableSessionTargetCap(boolean v) { cfg().enableSessionTargetCap = v; }
    public void setEnableSmallCandleFilter(boolean v) { cfg().enableSmallCandleFilter = v; }
    public void setTrailTriggerPct(double v) { cfg().trailTriggerPct = v; }
    public void setTrailSlPct(double v)      { cfg().trailSlPct = v; }
    public void setSmallCandleAtrThreshold(double v) { cfg().smallCandleAtrThreshold = v; }

    // ── mode-specific getters/setters (used by SettingsController) ────────────
    public String getTradingStartTime(String mode)  { return cfgFor(mode).tradingStartTime; }
    public String getTradingEndTime(String mode)    { return cfgFor(mode).tradingEndTime; }
    public double getTotalCapital(String mode)       { return cfgFor(mode).totalCapital; }
    public double getMaxRiskPerDayPct(String mode)  { return cfgFor(mode).maxRiskPerDayPct; }
    public double getRiskPerTrade(String mode)      { return cfgFor(mode).riskPerTrade; }
    public double getMaxDailyLoss(String mode)      { return cfgFor(mode).totalCapital * cfgFor(mode).maxRiskPerDayPct / 100.0; }
    public String getAutoSquareOffTime(String mode) { return cfgFor(mode).autoSquareOffTime; }
    public double getAtrMultiplier(String mode)     { return cfgFor(mode).atrMultiplier; }
    public boolean isEnableR4S4(String mode)       { return cfgFor(mode).enableR4S4; }
    public double getSessionMoveLimit(String mode) { return cfgFor(mode).sessionMoveLimit; }
    public double getBrokeragePerOrder(String mode) { return cfgFor(mode).brokeragePerOrder; }
    public int    getFixedQuantity(String mode)   { return cfgFor(mode).fixedQuantity; }
    public double getCapitalPerTrade(String mode) { return cfgFor(mode).capitalPerTrade; }
    public int    getTelegramAlertFrequency(String mode) { return cfgFor(mode).telegramAlertFrequency; }
    public boolean isEnableLargeCandleFilter(String mode) { return cfgFor(mode).enableLargeCandleFilter; }
    public double getLargeCandleAtrThreshold(String mode) { return cfgFor(mode).largeCandleAtrThreshold; }
    public boolean isEnableTargetShift(String mode) { return cfgFor(mode).enableTargetShift; }
    public boolean isEnableSessionTargetCap(String mode) { return cfgFor(mode).enableSessionTargetCap; }
    public boolean isEnableSmallCandleFilter(String mode) { return cfgFor(mode).enableSmallCandleFilter; }
    public double getTrailTriggerPct(String mode) { return cfgFor(mode).trailTriggerPct; }
    public double getTrailSlPct(String mode)      { return cfgFor(mode).trailSlPct; }
    public double getSmallCandleAtrThreshold(String mode) { return cfgFor(mode).smallCandleAtrThreshold; }

    public void setTradingStartTime(String mode, String v)  { cfgFor(mode).tradingStartTime = v; }
    public void setTradingEndTime(String mode, String v)    { cfgFor(mode).tradingEndTime = v; }
    public void setTotalCapital(String mode, double v)       { cfgFor(mode).totalCapital = v; }
    public void setMaxRiskPerDayPct(String mode, double v)  { cfgFor(mode).maxRiskPerDayPct = v; }
    public void setRiskPerTrade(String mode, double v)      { cfgFor(mode).riskPerTrade = v; }
    public void setAutoSquareOffTime(String mode, String v) { cfgFor(mode).autoSquareOffTime = v; }
    public void setAtrMultiplier(String mode, double v)     { cfgFor(mode).atrMultiplier = v; }
    public void setEnableR4S4(String mode, boolean v)       { cfgFor(mode).enableR4S4 = v; }
    public void setSessionMoveLimit(String mode, double v) { cfgFor(mode).sessionMoveLimit = v; }
    public void setBrokeragePerOrder(String mode, double v) { cfgFor(mode).brokeragePerOrder = v; }
    public void setFixedQuantity(String mode, int v)      { cfgFor(mode).fixedQuantity = v; }
    public void setCapitalPerTrade(String mode, double v) { cfgFor(mode).capitalPerTrade = v; }
    public void setTelegramAlertFrequency(String mode, int v) { cfgFor(mode).telegramAlertFrequency = v; }
    public void setEnableLargeCandleFilter(String mode, boolean v) { cfgFor(mode).enableLargeCandleFilter = v; }
    public void setLargeCandleAtrThreshold(String mode, double v) { cfgFor(mode).largeCandleAtrThreshold = v; }
    public void setEnableTargetShift(String mode, boolean v) { cfgFor(mode).enableTargetShift = v; }
    public void setEnableSessionTargetCap(String mode, boolean v) { cfgFor(mode).enableSessionTargetCap = v; }
    public void setEnableSmallCandleFilter(String mode, boolean v) { cfgFor(mode).enableSmallCandleFilter = v; }
    public void setTrailTriggerPct(String mode, double v) { cfgFor(mode).trailTriggerPct = v; }
    public void setTrailSlPct(String mode, double v)      { cfgFor(mode).trailSlPct = v; }
    public void setSmallCandleAtrThreshold(String mode, double v) { cfgFor(mode).smallCandleAtrThreshold = v; }

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
            state.put("totalCapital",      c.totalCapital);
            state.put("maxRiskPerDayPct", c.maxRiskPerDayPct);
            state.put("riskPerTrade",     c.riskPerTrade);
            state.put("autoSquareOffTime", c.autoSquareOffTime);
            state.put("atrMultiplier",    c.atrMultiplier);
            state.put("enableR4S4",      c.enableR4S4);
            state.put("sessionMoveLimit", c.sessionMoveLimit);
            state.put("brokeragePerOrder", c.brokeragePerOrder);
            state.put("fixedQuantity",   c.fixedQuantity);
            state.put("capitalPerTrade", c.capitalPerTrade);
            state.put("telegramAlertFrequency", c.telegramAlertFrequency);
            state.put("enableLargeCandleFilter", c.enableLargeCandleFilter);
            state.put("largeCandleAtrThreshold", c.largeCandleAtrThreshold);
            state.put("enableTargetShift", c.enableTargetShift);
            state.put("enableSessionTargetCap", c.enableSessionTargetCap);
            state.put("enableSmallCandleFilter", c.enableSmallCandleFilter);
            state.put("smallCandleAtrThreshold", c.smallCandleAtrThreshold);
            state.put("trailTriggerPct", c.trailTriggerPct);
            state.put("trailSlPct", c.trailSlPct);
            Files.writeString(Paths.get(fileFor(mode)), mapper.writeValueAsString(state));
        } catch (IOException e) {
            log.error("[RiskSettingsStore] Failed to save {}: {}", mode, e.getMessage());
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
            if (state.containsKey("totalCapital"))      c.totalCapital      = Double.parseDouble(state.get("totalCapital").toString());
            if (state.containsKey("maxRiskPerDayPct")) c.maxRiskPerDayPct = Double.parseDouble(state.get("maxRiskPerDayPct").toString());
            if (state.containsKey("riskPerTrade"))     c.riskPerTrade     = Double.parseDouble(state.get("riskPerTrade").toString());
            if (state.containsKey("autoSquareOffTime")) c.autoSquareOffTime = state.get("autoSquareOffTime").toString();
            if (state.containsKey("atrMultiplier"))    c.atrMultiplier    = Double.parseDouble(state.get("atrMultiplier").toString());
            if (state.containsKey("enableR4S4"))       c.enableR4S4       = Boolean.parseBoolean(state.get("enableR4S4").toString());
            if (state.containsKey("sessionMoveLimit")) c.sessionMoveLimit = Double.parseDouble(state.get("sessionMoveLimit").toString());
            if (state.containsKey("brokeragePerOrder")) c.brokeragePerOrder = Double.parseDouble(state.get("brokeragePerOrder").toString());
            if (state.containsKey("fixedQuantity"))   c.fixedQuantity   = Integer.parseInt(state.get("fixedQuantity").toString());
            if (state.containsKey("capitalPerTrade")) c.capitalPerTrade = Double.parseDouble(state.get("capitalPerTrade").toString());
            if (state.containsKey("telegramAlertFrequency")) c.telegramAlertFrequency = Integer.parseInt(state.get("telegramAlertFrequency").toString());
            if (state.containsKey("enableLargeCandleFilter")) c.enableLargeCandleFilter = Boolean.parseBoolean(state.get("enableLargeCandleFilter").toString());
            if (state.containsKey("largeCandleAtrThreshold")) c.largeCandleAtrThreshold = Double.parseDouble(state.get("largeCandleAtrThreshold").toString());
            if (state.containsKey("enableTargetShift")) c.enableTargetShift = Boolean.parseBoolean(state.get("enableTargetShift").toString());
            if (state.containsKey("enableSessionTargetCap")) c.enableSessionTargetCap = Boolean.parseBoolean(state.get("enableSessionTargetCap").toString());
            if (state.containsKey("enableSmallCandleFilter")) c.enableSmallCandleFilter = Boolean.parseBoolean(state.get("enableSmallCandleFilter").toString());
            if (state.containsKey("smallCandleAtrThreshold")) c.smallCandleAtrThreshold = Double.parseDouble(state.get("smallCandleAtrThreshold").toString());
            if (state.containsKey("trailTriggerPct")) c.trailTriggerPct = Double.parseDouble(state.get("trailTriggerPct").toString());
            if (state.containsKey("trailSlPct")) c.trailSlPct = Double.parseDouble(state.get("trailSlPct").toString());
            log.info("[RiskSettingsStore] Loaded {}: start={} end={} totalCapital={} maxRiskPerDayPct={}% riskPerTrade={} autoSquareOff={} atrMult={} enableR4S4={} sessionMove={}% brokerage={} fixedQty={} capitalPerTrade={} trail={}%/{}%", mode, c.tradingStartTime, c.tradingEndTime, c.totalCapital, c.maxRiskPerDayPct, c.riskPerTrade, c.autoSquareOffTime, c.atrMultiplier, c.enableR4S4, c.sessionMoveLimit, c.brokeragePerOrder, c.fixedQuantity, c.capitalPerTrade, c.trailTriggerPct, c.trailSlPct);
        } catch (IOException e) {
            log.error("[RiskSettingsStore] Failed to load {}: {}", mode, e.getMessage());
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
            log.info("[RiskSettingsStore] Migrated legacy risk-settings.json to live/ and simulator/");
        } catch (IOException e) {
            log.error("[RiskSettingsStore] Migration failed: {}", e.getMessage());
        }
    }
}
