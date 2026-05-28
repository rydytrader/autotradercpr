package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Settings GET/POST for the options-selling bot. Slim — only fields that survive
 * the strip-to-options refactor are exposed here. The Money / Risk / Hours / Charges /
 * Users / Rolling Straddle tabs are the only consumers.
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
        // Rolling Straddle
        result.put("enableRollingStraddle",  riskSettings.isEnableRollingStraddle());
        result.put("straddleEntryTime",      riskSettings.getStraddleEntryTime());
        result.put("straddleSquareOffTime",  riskSettings.getStraddleSquareOffTime());
        result.put("straddleMaxRolls",       riskSettings.getStraddleMaxRolls());
        result.put("straddleLotsPerLeg",     riskSettings.getStraddleLotsPerLeg());
        result.put("straddleCombinedSlPct",  riskSettings.getStraddleCombinedSlPct());
        result.put("straddleCombinedTargetPct", riskSettings.getStraddleCombinedTargetPct());
        result.put("straddleRollWaitMin",    riskSettings.getStraddleRollWaitMin());
        result.put("straddleRollCutoffTime", riskSettings.getStraddleRollCutoffTime());
        result.put("straddleMaxDailyLoss",   riskSettings.getStraddleMaxDailyLoss());
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
            // Rolling Straddle
            if (body.containsKey("enableRollingStraddle"))  riskSettings.setEnableRollingStraddle(Boolean.parseBoolean(body.get("enableRollingStraddle").toString()));
            if (body.containsKey("straddleEntryTime"))      riskSettings.setStraddleEntryTime(body.get("straddleEntryTime").toString());
            if (body.containsKey("straddleSquareOffTime"))  riskSettings.setStraddleSquareOffTime(body.get("straddleSquareOffTime").toString());
            if (body.containsKey("straddleMaxRolls"))       riskSettings.setStraddleMaxRolls(Integer.parseInt(body.get("straddleMaxRolls").toString()));
            if (body.containsKey("straddleLotsPerLeg"))     riskSettings.setStraddleLotsPerLeg(Integer.parseInt(body.get("straddleLotsPerLeg").toString()));
            if (body.containsKey("straddleCombinedSlPct"))  riskSettings.setStraddleCombinedSlPct(Double.parseDouble(body.get("straddleCombinedSlPct").toString()));
            if (body.containsKey("straddleCombinedTargetPct")) riskSettings.setStraddleCombinedTargetPct(Double.parseDouble(body.get("straddleCombinedTargetPct").toString()));
            if (body.containsKey("straddleRollWaitMin"))    riskSettings.setStraddleRollWaitMin(Integer.parseInt(body.get("straddleRollWaitMin").toString()));
            if (body.containsKey("straddleRollCutoffTime")) riskSettings.setStraddleRollCutoffTime(body.get("straddleRollCutoffTime").toString());
            if (body.containsKey("straddleMaxDailyLoss"))   riskSettings.setStraddleMaxDailyLoss(Double.parseDouble(body.get("straddleMaxDailyLoss").toString()));
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
