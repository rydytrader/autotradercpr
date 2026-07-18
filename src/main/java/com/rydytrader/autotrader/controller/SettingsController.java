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
        // Strangle
        result.put("strangleEnabled",               riskSettings.isStrangleEnabled(effectiveMode));
        result.put("strangleLotsPerLeg",            riskSettings.getStrangleLotsPerLeg(effectiveMode));
        result.put("strangleOrderType",             riskSettings.getStrangleOrderType(effectiveMode));
        result.put("strangleEntryTime",             riskSettings.getStrangleEntryTime(effectiveMode));
        result.put("strangleSquareOffTime",         riskSettings.getStrangleSquareOffTime(effectiveMode));
        result.put("strangleNiftyTargetPremium",    riskSettings.getStrangleNiftyTargetPremium(effectiveMode));
        result.put("strangleSensexTargetPremium",   riskSettings.getStrangleSensexTargetPremium(effectiveMode));
        result.put("strangleSlMultiplier",          riskSettings.getStrangleSlMultiplier(effectiveMode));
        result.put("strangleHedgeStrikesAway",      riskSettings.getStrangleHedgeStrikesAway(effectiveMode));
        result.put("strangleHedgeQtyMultiplier",    riskSettings.getStrangleHedgeQtyMultiplier(effectiveMode));
        result.put("strangleMondayInstrument",      riskSettings.getStrangleMondayInstrument(effectiveMode));
        result.put("strangleTuesdayInstrument",     riskSettings.getStrangleTuesdayInstrument(effectiveMode));
        result.put("strangleWednesdayInstrument",   riskSettings.getStrangleWednesdayInstrument(effectiveMode));
        result.put("strangleThursdayInstrument",    riskSettings.getStrangleThursdayInstrument(effectiveMode));
        result.put("strangleFridayInstrument",      riskSettings.getStrangleFridayInstrument(effectiveMode));
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
            // Strangle
            if (body.containsKey("strangleEnabled"))              riskSettings.setStrangleEnabled(effectiveMode, Boolean.parseBoolean(body.get("strangleEnabled").toString()));
            if (body.containsKey("strangleLotsPerLeg"))           riskSettings.setStrangleLotsPerLeg(effectiveMode, Integer.parseInt(body.get("strangleLotsPerLeg").toString()));
            if (body.containsKey("strangleOrderType"))            riskSettings.setStrangleOrderType(effectiveMode, body.get("strangleOrderType").toString());
            if (body.containsKey("strangleEntryTime"))            riskSettings.setStrangleEntryTime(effectiveMode, body.get("strangleEntryTime").toString());
            if (body.containsKey("strangleSquareOffTime"))        riskSettings.setStrangleSquareOffTime(effectiveMode, body.get("strangleSquareOffTime").toString());
            if (body.containsKey("strangleNiftyTargetPremium"))   riskSettings.setStrangleNiftyTargetPremium(effectiveMode, Double.parseDouble(body.get("strangleNiftyTargetPremium").toString()));
            if (body.containsKey("strangleSensexTargetPremium"))  riskSettings.setStrangleSensexTargetPremium(effectiveMode, Double.parseDouble(body.get("strangleSensexTargetPremium").toString()));
            if (body.containsKey("strangleSlMultiplier"))         riskSettings.setStrangleSlMultiplier(effectiveMode, Double.parseDouble(body.get("strangleSlMultiplier").toString()));
            if (body.containsKey("strangleHedgeStrikesAway"))     riskSettings.setStrangleHedgeStrikesAway(effectiveMode, Integer.parseInt(body.get("strangleHedgeStrikesAway").toString()));
            if (body.containsKey("strangleHedgeQtyMultiplier"))   riskSettings.setStrangleHedgeQtyMultiplier(effectiveMode, Double.parseDouble(body.get("strangleHedgeQtyMultiplier").toString()));
            if (body.containsKey("strangleMondayInstrument"))     riskSettings.setStrangleMondayInstrument(effectiveMode, body.get("strangleMondayInstrument").toString());
            if (body.containsKey("strangleTuesdayInstrument"))    riskSettings.setStrangleTuesdayInstrument(effectiveMode, body.get("strangleTuesdayInstrument").toString());
            if (body.containsKey("strangleWednesdayInstrument"))  riskSettings.setStrangleWednesdayInstrument(effectiveMode, body.get("strangleWednesdayInstrument").toString());
            if (body.containsKey("strangleThursdayInstrument"))   riskSettings.setStrangleThursdayInstrument(effectiveMode, body.get("strangleThursdayInstrument").toString());
            if (body.containsKey("strangleFridayInstrument"))     riskSettings.setStrangleFridayInstrument(effectiveMode, body.get("strangleFridayInstrument").toString());
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
