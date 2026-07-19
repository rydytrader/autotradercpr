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
        result.put("atmVwapEnabled",            riskSettings.isAtmVwapEnabled(effectiveMode));
        result.put("atmVwapLotsPerLeg",         riskSettings.getAtmVwapLotsPerLeg(effectiveMode));
        result.put("atmVwapOrderType",          riskSettings.getAtmVwapOrderType(effectiveMode));
        result.put("atmVwapTradingStartTime",   riskSettings.getAtmVwapTradingStartTime(effectiveMode));
        result.put("atmVwapTradingEndTime",     riskSettings.getAtmVwapTradingEndTime(effectiveMode));
        result.put("atmVwapSquareOffTime",      riskSettings.getAtmVwapSquareOffTime(effectiveMode));
        result.put("atmVwapMaxConcurrentPositions", riskSettings.getAtmVwapMaxConcurrentPositions(effectiveMode));
        result.put("atmVwapMinSlPoints",        riskSettings.getAtmVwapMinSlPoints(effectiveMode));
        result.put("atmVwapMaxSlPoints",        riskSettings.getAtmVwapMaxSlPoints(effectiveMode));
        result.put("atmVwapMaxCeTradesPerDay",  riskSettings.getAtmVwapMaxCeTradesPerDay(effectiveMode));
        result.put("atmVwapMaxPeTradesPerDay",  riskSettings.getAtmVwapMaxPeTradesPerDay(effectiveMode));
        result.put("atmVwapOiBiasThresholdPct", riskSettings.getAtmVwapOiBiasThresholdPct(effectiveMode));
        result.put("atmVwapOiBiasFilterEnabled", riskSettings.isAtmVwapOiBiasFilterEnabled(effectiveMode));
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
            if (body.containsKey("atmVwapEnabled"))           riskSettings.setAtmVwapEnabled(effectiveMode, Boolean.parseBoolean(body.get("atmVwapEnabled").toString()));
            if (body.containsKey("atmVwapLotsPerLeg"))        riskSettings.setAtmVwapLotsPerLeg(effectiveMode, Integer.parseInt(body.get("atmVwapLotsPerLeg").toString()));
            if (body.containsKey("atmVwapOrderType"))         riskSettings.setAtmVwapOrderType(effectiveMode, body.get("atmVwapOrderType").toString());
            if (body.containsKey("atmVwapTradingStartTime"))  riskSettings.setAtmVwapTradingStartTime(effectiveMode, body.get("atmVwapTradingStartTime").toString());
            if (body.containsKey("atmVwapTradingEndTime"))    riskSettings.setAtmVwapTradingEndTime(effectiveMode, body.get("atmVwapTradingEndTime").toString());
            if (body.containsKey("atmVwapSquareOffTime"))     riskSettings.setAtmVwapSquareOffTime(effectiveMode, body.get("atmVwapSquareOffTime").toString());
            if (body.containsKey("atmVwapMaxConcurrentPositions")) riskSettings.setAtmVwapMaxConcurrentPositions(effectiveMode, Integer.parseInt(body.get("atmVwapMaxConcurrentPositions").toString()));
            if (body.containsKey("atmVwapMinSlPoints"))            riskSettings.setAtmVwapMinSlPoints(effectiveMode, Double.parseDouble(body.get("atmVwapMinSlPoints").toString()));
            if (body.containsKey("atmVwapMaxSlPoints"))            riskSettings.setAtmVwapMaxSlPoints(effectiveMode, Double.parseDouble(body.get("atmVwapMaxSlPoints").toString()));
            if (body.containsKey("atmVwapMaxCeTradesPerDay"))      riskSettings.setAtmVwapMaxCeTradesPerDay(effectiveMode, Integer.parseInt(body.get("atmVwapMaxCeTradesPerDay").toString()));
            if (body.containsKey("atmVwapMaxPeTradesPerDay"))      riskSettings.setAtmVwapMaxPeTradesPerDay(effectiveMode, Integer.parseInt(body.get("atmVwapMaxPeTradesPerDay").toString()));
            if (body.containsKey("atmVwapOiBiasThresholdPct"))     riskSettings.setAtmVwapOiBiasThresholdPct(effectiveMode, Double.parseDouble(body.get("atmVwapOiBiasThresholdPct").toString()));
            if (body.containsKey("atmVwapOiBiasFilterEnabled"))    riskSettings.setAtmVwapOiBiasFilterEnabled(effectiveMode, Boolean.parseBoolean(body.get("atmVwapOiBiasFilterEnabled").toString()));
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
