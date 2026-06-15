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
        result.put("manualAutoSquareOffTime", riskSettings.getManualAutoSquareOffTime(effectiveMode));
        // Camarilla
        result.put("camarillaEnabled",            riskSettings.isCamarillaEnabled(effectiveMode));
        result.put("camarillaLotsPerLeg",         riskSettings.getCamarillaLotsPerLeg(effectiveMode));
        result.put("camarillaOrderType",          riskSettings.getCamarillaOrderType(effectiveMode));
        result.put("camarillaSquareOffTime",      riskSettings.getCamarillaSquareOffTime(effectiveMode));
        result.put("camarillaH3RevEnabled",       riskSettings.isCamarillaH3RevEnabled(effectiveMode));
        result.put("camarillaL3RevEnabled",       riskSettings.isCamarillaL3RevEnabled(effectiveMode));
        result.put("camarillaH4BoEnabled",        riskSettings.isCamarillaH4BoEnabled(effectiveMode));
        result.put("camarillaL4BdEnabled",        riskSettings.isCamarillaL4BdEnabled(effectiveMode));
        result.put("camarillaMaxTradesPerDay",    riskSettings.getCamarillaMaxTradesPerDay(effectiveMode));
        result.put("camarillaPauseAfterNLosses",  riskSettings.getCamarillaPauseAfterNLosses(effectiveMode));
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
            if (body.containsKey("manualAutoSquareOffTime")) riskSettings.setManualAutoSquareOffTime(effectiveMode, body.get("manualAutoSquareOffTime") == null ? "" : body.get("manualAutoSquareOffTime").toString());
            // Camarilla
            if (body.containsKey("camarillaEnabled"))           riskSettings.setCamarillaEnabled(effectiveMode, Boolean.parseBoolean(body.get("camarillaEnabled").toString()));
            if (body.containsKey("camarillaLotsPerLeg"))        riskSettings.setCamarillaLotsPerLeg(effectiveMode, Integer.parseInt(body.get("camarillaLotsPerLeg").toString()));
            if (body.containsKey("camarillaOrderType"))         riskSettings.setCamarillaOrderType(effectiveMode, body.get("camarillaOrderType").toString());
            if (body.containsKey("camarillaSquareOffTime"))     riskSettings.setCamarillaSquareOffTime(effectiveMode, body.get("camarillaSquareOffTime").toString());
            if (body.containsKey("camarillaH3RevEnabled"))      riskSettings.setCamarillaH3RevEnabled(effectiveMode, Boolean.parseBoolean(body.get("camarillaH3RevEnabled").toString()));
            if (body.containsKey("camarillaL3RevEnabled"))      riskSettings.setCamarillaL3RevEnabled(effectiveMode, Boolean.parseBoolean(body.get("camarillaL3RevEnabled").toString()));
            if (body.containsKey("camarillaH4BoEnabled"))       riskSettings.setCamarillaH4BoEnabled(effectiveMode, Boolean.parseBoolean(body.get("camarillaH4BoEnabled").toString()));
            if (body.containsKey("camarillaL4BdEnabled"))       riskSettings.setCamarillaL4BdEnabled(effectiveMode, Boolean.parseBoolean(body.get("camarillaL4BdEnabled").toString()));
            if (body.containsKey("camarillaMaxTradesPerDay"))   riskSettings.setCamarillaMaxTradesPerDay(effectiveMode, Integer.parseInt(body.get("camarillaMaxTradesPerDay").toString()));
            if (body.containsKey("camarillaPauseAfterNLosses")) riskSettings.setCamarillaPauseAfterNLosses(effectiveMode, Integer.parseInt(body.get("camarillaPauseAfterNLosses").toString()));
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
