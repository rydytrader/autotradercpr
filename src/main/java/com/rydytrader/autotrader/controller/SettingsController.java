package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Settings GET/POST for the options-selling bot. Slim — only fields that survive
 * the strip-to-options refactor are exposed here. The Money / Risk / Hours / Charges /
 * Users tabs are the only consumers.
 */
@RestController
public class SettingsController {

    private final RiskSettingsStore riskSettings;

    public SettingsController(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
    }

    // ── GET SETTINGS ──────────────────────────────────────────────────────────
    @GetMapping("/api/settings/risk")
    public Map<String, Object> getSettings(@RequestParam(defaultValue = "") String mode) {
        String effectiveMode = resolveMode(mode);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode",                effectiveMode);
        result.put("activeMode",          "live");
        // Hours
        result.put("tradingStartTime",    riskSettings.getTradingStartTime(effectiveMode));
        result.put("tradingEndTime",      riskSettings.getTradingEndTime(effectiveMode));
        result.put("autoSquareOffTime",   riskSettings.getAutoSquareOffTime(effectiveMode));
        // ATM VWAP
        result.put("optionScalpingEnabled",            riskSettings.isOptionScalpingEnabled(effectiveMode));
        result.put("optionScalpingLotsPerLeg",         riskSettings.getOptionScalpingLotsPerLeg(effectiveMode));
        result.put("optionScalpingOrderType",          riskSettings.getOptionScalpingOrderType(effectiveMode));
        result.put("optionScalpingTradingStartTime",   riskSettings.getOptionScalpingTradingStartTime(effectiveMode));
        result.put("optionScalpingTradingEndTime",     riskSettings.getOptionScalpingTradingEndTime(effectiveMode));
        result.put("optionScalpingSquareOffTime",      riskSettings.getOptionScalpingSquareOffTime(effectiveMode));
        result.put("optionScalpingMaxConcurrentPositions", riskSettings.getOptionScalpingMaxConcurrentPositions(effectiveMode));
        result.put("optionScalpingMaxCeTradesPerDay",  riskSettings.getOptionScalpingMaxCeTradesPerDay(effectiveMode));
        result.put("optionScalpingMaxPeTradesPerDay",  riskSettings.getOptionScalpingMaxPeTradesPerDay(effectiveMode));
        // OPTION BUYING (Ganesan 4-indicator framework — coexists with OPTION SCALPING)
        result.put("optionBuyingEnabled",           riskSettings.isOptionBuyingEnabled());
        result.put("optionBuyingLotsPerLeg",        riskSettings.getOptionBuyingLotsPerLeg());
        result.put("optionBuyingOrderType",         riskSettings.getOptionBuyingOrderType());
        result.put("optionBuyingTradingStartTime",  riskSettings.getOptionBuyingTradingStartTime());
        result.put("optionBuyingTradingEndTime",    riskSettings.getOptionBuyingTradingEndTime());
        result.put("optionBuyingSquareOffTime",     riskSettings.getOptionBuyingSquareOffTime());
        result.put("optionBuyingMaxTradesPerDay",   riskSettings.getOptionBuyingMaxTradesPerDay());
        // Money / Risk
        result.put("totalCapital",        riskSettings.getTotalCapital(effectiveMode));
        result.put("maxRiskPerDayPct",    riskSettings.getMaxRiskPerDayPct(effectiveMode));
        result.put("riskPerTrade",        riskSettings.getRiskPerTrade(effectiveMode));
        result.put("maxDailyLoss",        riskSettings.getMaxDailyLoss(effectiveMode));
        result.put("capitalPerTrade",     riskSettings.getCapitalPerTrade(effectiveMode));
        result.put("fixedQuantity",       riskSettings.getFixedQuantity(effectiveMode));
        // Portfolio Risk (global)
        result.put("startingCapital",       riskSettings.getStartingCapital(effectiveMode));
        result.put("portfolioMaxRiskPct",   riskSettings.getPortfolioMaxRiskPct(effectiveMode));
        result.put("portfolioMaxDailyLoss", riskSettings.getPortfolioMaxDailyLoss()); // derived ₹ for display
        // Charges
        result.put("brokeragePerOrder",   riskSettings.getBrokeragePerOrder(effectiveMode));
        result.put("sttRate",             riskSettings.getSttRate(effectiveMode));
        result.put("exchangeRate",        riskSettings.getExchangeRate(effectiveMode));
        result.put("gstRate",             riskSettings.getGstRate(effectiveMode));
        result.put("sebiRate",            riskSettings.getSebiRate(effectiveMode));
        result.put("stampDutyRate",       riskSettings.getStampDutyRate(effectiveMode));
        result.put("brokeragePct",        riskSettings.getBrokeragePct(effectiveMode));
        // Notifications
        result.put("telegramAlertFrequency", riskSettings.getTelegramAlertFrequency(effectiveMode));
        return result;
    }

    // ── SAVE SETTINGS ─────────────────────────────────────────────────────────
    @PostMapping("/api/settings/risk")
    public ResponseEntity<Map<String, Object>> saveSettings(
            @RequestParam(defaultValue = "") String mode,
            @RequestBody Map<String, Object> body) {
        try {
            String effectiveMode = resolveMode(mode);
            // Hours
            if (body.containsKey("tradingStartTime"))  riskSettings.setTradingStartTime(effectiveMode, body.get("tradingStartTime").toString());
            if (body.containsKey("tradingEndTime"))    riskSettings.setTradingEndTime(effectiveMode, body.get("tradingEndTime").toString());
            if (body.containsKey("autoSquareOffTime")) riskSettings.setAutoSquareOffTime(effectiveMode, body.get("autoSquareOffTime").toString());
            // ATM VWAP
            if (body.containsKey("optionScalpingEnabled"))           riskSettings.setOptionScalpingEnabled(effectiveMode, Boolean.parseBoolean(body.get("optionScalpingEnabled").toString()));
            if (body.containsKey("optionScalpingLotsPerLeg"))        riskSettings.setOptionScalpingLotsPerLeg(effectiveMode, Integer.parseInt(body.get("optionScalpingLotsPerLeg").toString()));
            if (body.containsKey("optionScalpingOrderType"))         riskSettings.setOptionScalpingOrderType(effectiveMode, body.get("optionScalpingOrderType").toString());
            if (body.containsKey("optionScalpingTradingStartTime"))  riskSettings.setOptionScalpingTradingStartTime(effectiveMode, body.get("optionScalpingTradingStartTime").toString());
            if (body.containsKey("optionScalpingTradingEndTime"))    riskSettings.setOptionScalpingTradingEndTime(effectiveMode, body.get("optionScalpingTradingEndTime").toString());
            if (body.containsKey("optionScalpingSquareOffTime"))     riskSettings.setOptionScalpingSquareOffTime(effectiveMode, body.get("optionScalpingSquareOffTime").toString());
            if (body.containsKey("optionScalpingMaxConcurrentPositions")) riskSettings.setOptionScalpingMaxConcurrentPositions(effectiveMode, Integer.parseInt(body.get("optionScalpingMaxConcurrentPositions").toString()));
            if (body.containsKey("optionScalpingMaxCeTradesPerDay"))      riskSettings.setOptionScalpingMaxCeTradesPerDay(effectiveMode, Integer.parseInt(body.get("optionScalpingMaxCeTradesPerDay").toString()));
            if (body.containsKey("optionScalpingMaxPeTradesPerDay"))      riskSettings.setOptionScalpingMaxPeTradesPerDay(effectiveMode, Integer.parseInt(body.get("optionScalpingMaxPeTradesPerDay").toString()));
            // SuperTrend params (Atr / Mult / Spot Atr / Spot Mult) intentionally not
            // exposed to POST — hardcoded to (10, 3) in OptionScalping.java.
            // TrailingExitEnabled + RequireGapOpenAboveVwap both hardcoded ON in OptionScalping — no POST.
            // OPTION BUYING — HardSlPct retired; Enabled is per-strategy kill switch.
            if (body.containsKey("optionBuyingEnabled"))          riskSettings.setOptionBuyingEnabled(Boolean.parseBoolean(body.get("optionBuyingEnabled").toString()));
            if (body.containsKey("optionBuyingLotsPerLeg"))       riskSettings.setOptionBuyingLotsPerLeg(Integer.parseInt(body.get("optionBuyingLotsPerLeg").toString()));
            if (body.containsKey("optionBuyingOrderType"))        riskSettings.setOptionBuyingOrderType(body.get("optionBuyingOrderType").toString());
            if (body.containsKey("optionBuyingTradingStartTime")) riskSettings.setOptionBuyingTradingStartTime(body.get("optionBuyingTradingStartTime").toString());
            if (body.containsKey("optionBuyingTradingEndTime"))   riskSettings.setOptionBuyingTradingEndTime(body.get("optionBuyingTradingEndTime").toString());
            if (body.containsKey("optionBuyingSquareOffTime"))    riskSettings.setOptionBuyingSquareOffTime(body.get("optionBuyingSquareOffTime").toString());
            if (body.containsKey("optionBuyingMaxTradesPerDay"))  riskSettings.setOptionBuyingMaxTradesPerDay(Integer.parseInt(body.get("optionBuyingMaxTradesPerDay").toString()));
            // Money / Risk
            if (body.containsKey("totalCapital"))      riskSettings.setTotalCapital(effectiveMode, Double.parseDouble(body.get("totalCapital").toString()));
            if (body.containsKey("maxRiskPerDayPct"))  riskSettings.setMaxRiskPerDayPct(effectiveMode, Double.parseDouble(body.get("maxRiskPerDayPct").toString()));
            if (body.containsKey("riskPerTrade"))      riskSettings.setRiskPerTrade(effectiveMode, Double.parseDouble(body.get("riskPerTrade").toString()));
            if (body.containsKey("capitalPerTrade"))   riskSettings.setCapitalPerTrade(effectiveMode, Double.parseDouble(body.get("capitalPerTrade").toString()));
            if (body.containsKey("fixedQuantity"))     riskSettings.setFixedQuantity(effectiveMode, Integer.parseInt(body.get("fixedQuantity").toString()));
            // Portfolio Risk (global)
            if (body.containsKey("startingCapital"))     riskSettings.setStartingCapital(effectiveMode, Double.parseDouble(body.get("startingCapital").toString()));
            if (body.containsKey("portfolioMaxRiskPct")) riskSettings.setPortfolioMaxRiskPct(effectiveMode, Double.parseDouble(body.get("portfolioMaxRiskPct").toString()));
            // Charges
            if (body.containsKey("brokeragePerOrder")) riskSettings.setBrokeragePerOrder(effectiveMode, Double.parseDouble(body.get("brokeragePerOrder").toString()));
            if (body.containsKey("sttRate"))           riskSettings.setSttRate(effectiveMode, Double.parseDouble(body.get("sttRate").toString()));
            if (body.containsKey("exchangeRate"))      riskSettings.setExchangeRate(effectiveMode, Double.parseDouble(body.get("exchangeRate").toString()));
            if (body.containsKey("gstRate"))           riskSettings.setGstRate(effectiveMode, Double.parseDouble(body.get("gstRate").toString()));
            if (body.containsKey("sebiRate"))          riskSettings.setSebiRate(effectiveMode, Double.parseDouble(body.get("sebiRate").toString()));
            if (body.containsKey("stampDutyRate"))     riskSettings.setStampDutyRate(effectiveMode, Double.parseDouble(body.get("stampDutyRate").toString()));
            if (body.containsKey("brokeragePct"))      riskSettings.setBrokeragePct(effectiveMode, Double.parseDouble(body.get("brokeragePct").toString()));
            // Notifications
            if (body.containsKey("telegramAlertFrequency")) riskSettings.setTelegramAlertFrequency(effectiveMode, Integer.parseInt(body.get("telegramAlertFrequency").toString()));
            riskSettings.saveFor(effectiveMode);
            return ResponseEntity.ok(Map.of("ok", true, "message", "Settings saved"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("ok", false, "message", e.getMessage()));
        }
    }

    private String resolveMode(String mode) {
        return "live";
    }
}
