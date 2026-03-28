package com.rydytrader.autotrader.store;

import com.rydytrader.autotrader.entity.SettingEntity;
import com.rydytrader.autotrader.repository.SettingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Persists risk management settings to SQLite so they survive server restarts.
 */
@Component
public class RiskSettingsStore {

    private static final Logger log = LoggerFactory.getLogger(RiskSettingsStore.class);

    @Autowired
    private SettingRepository settingRepo;

    // ── settings container ───────────────────────────────────────────────────
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
        volatile boolean enableSmallCandleFilter = false; // reject if candle move from breakout level < smallCandleAtrThreshold ATR
        volatile double smallCandleAtrThreshold = 0.5; // ATR multiplier for small candle filter
        volatile double trailTriggerPct = 75;  // % of range from entry to target that triggers trailing SL
        volatile double trailSlPct      = 50;  // % of range to lock as profit when trailing SL triggers
        // Scanner settings
        volatile String signalSource    = "TRADINGVIEW"; // TRADINGVIEW or INTERNAL
        volatile int    scannerTimeframe = 15;  // candle timeframe in minutes
        volatile boolean enableVwapCheck = true; // require VWAP confirmation for scanner signals
        volatile boolean enableHpt      = true;  // High Probable Trade signals
        volatile boolean enableMpt      = false; // Medium Probable Trade signals
        volatile boolean enableLpt      = false; // Low Probable Trade signals
    }

    private final Cfg live = new Cfg();

    @jakarta.annotation.PostConstruct
    public void init() {
        load("live");
    }

    // ── always returns live config ───────────────────────────────────────────
    private Cfg cfg() {
        return live;
    }

    public Cfg cfgFor(String mode) {
        return live;
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
    public boolean isEnableSmallCandleFilter() { return cfg().enableSmallCandleFilter; }
    public double getTrailTriggerPct() { return cfg().trailTriggerPct; }
    public double getTrailSlPct()      { return cfg().trailSlPct; }
    public double getSmallCandleAtrThreshold() { return cfg().smallCandleAtrThreshold; }

    public String  getSignalSource()      { return cfg().signalSource; }
    public int     getScannerTimeframe()  { return cfg().scannerTimeframe; }
    public boolean isEnableVwapCheck()    { return cfg().enableVwapCheck; }
    public boolean isEnableHpt()          { return cfg().enableHpt; }
    public boolean isEnableMpt()          { return cfg().enableMpt; }
    public boolean isEnableLpt()          { return cfg().enableLpt; }

    public void setSignalSource(String v)      { cfg().signalSource = v; }
    public void setScannerTimeframe(int v)     { cfg().scannerTimeframe = v; }
    public void setEnableVwapCheck(boolean v)  { cfg().enableVwapCheck = v; }
    public void setEnableHpt(boolean v)        { cfg().enableHpt = v; }
    public void setEnableMpt(boolean v)        { cfg().enableMpt = v; }
    public void setEnableLpt(boolean v)        { cfg().enableLpt = v; }

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
    public void setEnableSmallCandleFilter(String mode, boolean v) { cfgFor(mode).enableSmallCandleFilter = v; }
    public void setTrailTriggerPct(String mode, double v) { cfgFor(mode).trailTriggerPct = v; }
    public void setTrailSlPct(String mode, double v)      { cfgFor(mode).trailSlPct = v; }
    public void setSmallCandleAtrThreshold(String mode, double v) { cfgFor(mode).smallCandleAtrThreshold = v; }

    // ── save ──────────────────────────────────────────────────────────────────
    /** Saves the current settings. */
    public void save() {
        saveFor("live");
    }

    /** Saves the specified mode's settings. */
    public void saveFor(String mode) {
        Cfg c = cfgFor(mode);
        try {
            upsert("tradingStartTime", c.tradingStartTime);
            upsert("tradingEndTime", c.tradingEndTime);
            upsert("totalCapital", String.valueOf(c.totalCapital));
            upsert("maxRiskPerDayPct", String.valueOf(c.maxRiskPerDayPct));
            upsert("riskPerTrade", String.valueOf(c.riskPerTrade));
            upsert("autoSquareOffTime", c.autoSquareOffTime);
            upsert("atrMultiplier", String.valueOf(c.atrMultiplier));
            upsert("enableR4S4", String.valueOf(c.enableR4S4));
            upsert("sessionMoveLimit", String.valueOf(c.sessionMoveLimit));
            upsert("brokeragePerOrder", String.valueOf(c.brokeragePerOrder));
            upsert("fixedQuantity", String.valueOf(c.fixedQuantity));
            upsert("capitalPerTrade", String.valueOf(c.capitalPerTrade));
            upsert("telegramAlertFrequency", String.valueOf(c.telegramAlertFrequency));
            upsert("enableLargeCandleFilter", String.valueOf(c.enableLargeCandleFilter));
            upsert("largeCandleAtrThreshold", String.valueOf(c.largeCandleAtrThreshold));
            upsert("enableTargetShift", String.valueOf(c.enableTargetShift));
            upsert("enableSmallCandleFilter", String.valueOf(c.enableSmallCandleFilter));
            upsert("smallCandleAtrThreshold", String.valueOf(c.smallCandleAtrThreshold));
            upsert("trailTriggerPct", String.valueOf(c.trailTriggerPct));
            upsert("trailSlPct", String.valueOf(c.trailSlPct));
            upsert("signalSource", c.signalSource);
            upsert("scannerTimeframe", String.valueOf(c.scannerTimeframe));
            upsert("enableVwapCheck", String.valueOf(c.enableVwapCheck));
            upsert("enableHpt", String.valueOf(c.enableHpt));
            upsert("enableMpt", String.valueOf(c.enableMpt));
            upsert("enableLpt", String.valueOf(c.enableLpt));
        } catch (Exception e) {
            log.error("[RiskSettingsStore] Failed to save {}: {}", mode, e.getMessage());
        }
    }

    private void upsert(String key, String value) {
        SettingEntity entity = settingRepo.findBySettingKey(key).orElse(new SettingEntity(key, value));
        entity.setSettingValue(value);
        settingRepo.save(entity);
    }

    // ── load ──────────────────────────────────────────────────────────────────
    private void load(String mode) {
        try {
            List<SettingEntity> all = settingRepo.findAll();
            if (all.isEmpty()) return;
            Cfg c = cfgFor(mode);
            for (SettingEntity s : all) {
                String k = s.getSettingKey();
                String v = s.getSettingValue();
                if (v == null) continue;
                switch (k) {
                    case "tradingStartTime"  -> c.tradingStartTime = v;
                    case "tradingEndTime"    -> c.tradingEndTime = v;
                    case "totalCapital"      -> c.totalCapital = Double.parseDouble(v);
                    case "maxRiskPerDayPct"  -> c.maxRiskPerDayPct = Double.parseDouble(v);
                    case "riskPerTrade"      -> c.riskPerTrade = Double.parseDouble(v);
                    case "autoSquareOffTime" -> c.autoSquareOffTime = v;
                    case "atrMultiplier"     -> c.atrMultiplier = Double.parseDouble(v);
                    case "enableR4S4"        -> c.enableR4S4 = Boolean.parseBoolean(v);
                    case "sessionMoveLimit"  -> c.sessionMoveLimit = Double.parseDouble(v);
                    case "brokeragePerOrder" -> c.brokeragePerOrder = Double.parseDouble(v);
                    case "fixedQuantity"     -> c.fixedQuantity = Integer.parseInt(v);
                    case "capitalPerTrade"   -> c.capitalPerTrade = Double.parseDouble(v);
                    case "telegramAlertFrequency" -> c.telegramAlertFrequency = Integer.parseInt(v);
                    case "enableLargeCandleFilter" -> c.enableLargeCandleFilter = Boolean.parseBoolean(v);
                    case "largeCandleAtrThreshold" -> c.largeCandleAtrThreshold = Double.parseDouble(v);
                    case "enableTargetShift" -> c.enableTargetShift = Boolean.parseBoolean(v);
                    case "enableSmallCandleFilter" -> c.enableSmallCandleFilter = Boolean.parseBoolean(v);
                    case "smallCandleAtrThreshold" -> c.smallCandleAtrThreshold = Double.parseDouble(v);
                    case "trailTriggerPct"   -> c.trailTriggerPct = Double.parseDouble(v);
                    case "trailSlPct"        -> c.trailSlPct = Double.parseDouble(v);
                    case "signalSource"      -> c.signalSource = v;
                    case "scannerTimeframe"  -> c.scannerTimeframe = Integer.parseInt(v);
                    case "enableVwapCheck"   -> c.enableVwapCheck = Boolean.parseBoolean(v);
                    case "enableHpt"         -> c.enableHpt = Boolean.parseBoolean(v);
                    case "enableMpt"         -> c.enableMpt = Boolean.parseBoolean(v);
                    case "enableLpt"         -> c.enableLpt = Boolean.parseBoolean(v);
                }
            }
            log.info("[RiskSettingsStore] Loaded {}: start={} end={} totalCapital={} maxRiskPerDayPct={}% riskPerTrade={} autoSquareOff={} atrMult={} enableR4S4={} sessionMove={}% brokerage={} fixedQty={} capitalPerTrade={} trail={}%/{}%", mode, c.tradingStartTime, c.tradingEndTime, c.totalCapital, c.maxRiskPerDayPct, c.riskPerTrade, c.autoSquareOffTime, c.atrMultiplier, c.enableR4S4, c.sessionMoveLimit, c.brokeragePerOrder, c.fixedQuantity, c.capitalPerTrade, c.trailTriggerPct, c.trailSlPct);
        } catch (Exception e) {
            log.error("[RiskSettingsStore] Failed to load {}: {}", mode, e.getMessage());
        }
    }
}
