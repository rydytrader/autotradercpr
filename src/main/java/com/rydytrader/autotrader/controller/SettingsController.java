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
        // Strangle + Adjustments
        result.put("strangleAdjustEnabled",              riskSettings.isStrangleAdjustEnabled(effectiveMode));
        result.put("strangleAdjustLotsPerLeg",           riskSettings.getStrangleAdjustLotsPerLeg(effectiveMode));
        result.put("strangleAdjustOrderType",            riskSettings.getStrangleAdjustOrderType(effectiveMode));
        result.put("strangleAdjustEntryTime",            riskSettings.getStrangleAdjustEntryTime(effectiveMode));
        result.put("strangleAdjustSquareOffTime",        riskSettings.getStrangleAdjustSquareOffTime(effectiveMode));
        result.put("strangleAdjustNiftyTargetPremium",   riskSettings.getStrangleAdjustNiftyTargetPremium(effectiveMode));
        result.put("strangleAdjustSensexTargetPremium",  riskSettings.getStrangleAdjustSensexTargetPremium(effectiveMode));
        result.put("strangleAdjustSlMultiplier",         riskSettings.getStrangleAdjustSlMultiplier(effectiveMode));
        result.put("strangleAdjustHedgeStrikesAway",     riskSettings.getStrangleAdjustHedgeStrikesAway(effectiveMode));
        result.put("strangleAdjustHedgeQtyMultiplier",   riskSettings.getStrangleAdjustHedgeQtyMultiplier(effectiveMode));
        result.put("strangleAdjustMondayInstrument",     riskSettings.getStrangleAdjustMondayInstrument(effectiveMode));
        result.put("strangleAdjustTuesdayInstrument",    riskSettings.getStrangleAdjustTuesdayInstrument(effectiveMode));
        result.put("strangleAdjustWednesdayInstrument",  riskSettings.getStrangleAdjustWednesdayInstrument(effectiveMode));
        result.put("strangleAdjustThursdayInstrument",   riskSettings.getStrangleAdjustThursdayInstrument(effectiveMode));
        result.put("strangleAdjustFridayInstrument",     riskSettings.getStrangleAdjustFridayInstrument(effectiveMode));
        result.put("strangleAdjustInitialCapital",       riskSettings.getStrangleAdjustInitialCapital(effectiveMode));
        // Strangle (simple)
        result.put("strangleEnabled",         riskSettings.isStrangleEnabled(effectiveMode));
        result.put("strangleLotsPerLeg",      riskSettings.getStrangleLotsPerLeg(effectiveMode));
        result.put("strangleOrderType",       riskSettings.getStrangleOrderType(effectiveMode));
        result.put("strangleEntryTime",       riskSettings.getStrangleEntryTime(effectiveMode));
        result.put("strangleSquareOffTime",   riskSettings.getStrangleSquareOffTime(effectiveMode));
        result.put("strangleShortPremium",    riskSettings.getStrangleShortPremium(effectiveMode));
        result.put("strangleHedgePremium",    riskSettings.getStrangleHedgePremium(effectiveMode));
        result.put("strangleSlMultiplier",    riskSettings.getStrangleSlMultiplier(effectiveMode));
        result.put("strangleInitialCapital",  riskSettings.getStrangleInitialCapital(effectiveMode));
        // Money / Risk
        result.put("totalCapital",        riskSettings.getTotalCapital(effectiveMode));
        result.put("maxRiskPerDayPct",    riskSettings.getMaxRiskPerDayPct(effectiveMode));
        result.put("riskPerTrade",        riskSettings.getRiskPerTrade(effectiveMode));
        result.put("maxDailyLoss",        riskSettings.getMaxDailyLoss(effectiveMode));
        result.put("capitalPerTrade",     riskSettings.getCapitalPerTrade(effectiveMode));
        result.put("fixedQuantity",       riskSettings.getFixedQuantity(effectiveMode));
        // Portfolio Risk retired — per-strategy Initial Capital replaces the global.
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
            // Strangle + Adjustments
            if (body.containsKey("strangleAdjustEnabled"))              riskSettings.setStrangleAdjustEnabled(effectiveMode, Boolean.parseBoolean(body.get("strangleAdjustEnabled").toString()));
            if (body.containsKey("strangleAdjustLotsPerLeg"))           riskSettings.setStrangleAdjustLotsPerLeg(effectiveMode, Integer.parseInt(body.get("strangleAdjustLotsPerLeg").toString()));
            if (body.containsKey("strangleAdjustOrderType"))            riskSettings.setStrangleAdjustOrderType(effectiveMode, body.get("strangleAdjustOrderType").toString());
            if (body.containsKey("strangleAdjustEntryTime"))            riskSettings.setStrangleAdjustEntryTime(effectiveMode, body.get("strangleAdjustEntryTime").toString());
            if (body.containsKey("strangleAdjustSquareOffTime"))        riskSettings.setStrangleAdjustSquareOffTime(effectiveMode, body.get("strangleAdjustSquareOffTime").toString());
            if (body.containsKey("strangleAdjustNiftyTargetPremium"))   riskSettings.setStrangleAdjustNiftyTargetPremium(effectiveMode, Double.parseDouble(body.get("strangleAdjustNiftyTargetPremium").toString()));
            if (body.containsKey("strangleAdjustSensexTargetPremium"))  riskSettings.setStrangleAdjustSensexTargetPremium(effectiveMode, Double.parseDouble(body.get("strangleAdjustSensexTargetPremium").toString()));
            if (body.containsKey("strangleAdjustSlMultiplier"))         riskSettings.setStrangleAdjustSlMultiplier(effectiveMode, Double.parseDouble(body.get("strangleAdjustSlMultiplier").toString()));
            if (body.containsKey("strangleAdjustHedgeStrikesAway"))     riskSettings.setStrangleAdjustHedgeStrikesAway(effectiveMode, Integer.parseInt(body.get("strangleAdjustHedgeStrikesAway").toString()));
            if (body.containsKey("strangleAdjustHedgeQtyMultiplier"))   riskSettings.setStrangleAdjustHedgeQtyMultiplier(effectiveMode, Double.parseDouble(body.get("strangleAdjustHedgeQtyMultiplier").toString()));
            if (body.containsKey("strangleAdjustMondayInstrument"))     riskSettings.setStrangleAdjustMondayInstrument(effectiveMode, body.get("strangleAdjustMondayInstrument").toString());
            if (body.containsKey("strangleAdjustTuesdayInstrument"))    riskSettings.setStrangleAdjustTuesdayInstrument(effectiveMode, body.get("strangleAdjustTuesdayInstrument").toString());
            if (body.containsKey("strangleAdjustWednesdayInstrument"))  riskSettings.setStrangleAdjustWednesdayInstrument(effectiveMode, body.get("strangleAdjustWednesdayInstrument").toString());
            if (body.containsKey("strangleAdjustThursdayInstrument"))   riskSettings.setStrangleAdjustThursdayInstrument(effectiveMode, body.get("strangleAdjustThursdayInstrument").toString());
            if (body.containsKey("strangleAdjustFridayInstrument"))     riskSettings.setStrangleAdjustFridayInstrument(effectiveMode, body.get("strangleAdjustFridayInstrument").toString());
            if (body.containsKey("strangleAdjustInitialCapital"))       riskSettings.setStrangleAdjustInitialCapital(effectiveMode, Double.parseDouble(body.get("strangleAdjustInitialCapital").toString()));
            // Strangle (simple)
            if (body.containsKey("strangleEnabled"))        riskSettings.setStrangleEnabled(effectiveMode, Boolean.parseBoolean(body.get("strangleEnabled").toString()));
            if (body.containsKey("strangleLotsPerLeg"))     riskSettings.setStrangleLotsPerLeg(effectiveMode, Integer.parseInt(body.get("strangleLotsPerLeg").toString()));
            if (body.containsKey("strangleOrderType"))      riskSettings.setStrangleOrderType(effectiveMode, body.get("strangleOrderType").toString());
            if (body.containsKey("strangleEntryTime"))      riskSettings.setStrangleEntryTime(effectiveMode, body.get("strangleEntryTime").toString());
            if (body.containsKey("strangleSquareOffTime"))  riskSettings.setStrangleSquareOffTime(effectiveMode, body.get("strangleSquareOffTime").toString());
            if (body.containsKey("strangleShortPremium"))   riskSettings.setStrangleShortPremium(effectiveMode, Double.parseDouble(body.get("strangleShortPremium").toString()));
            if (body.containsKey("strangleHedgePremium"))   riskSettings.setStrangleHedgePremium(effectiveMode, Double.parseDouble(body.get("strangleHedgePremium").toString()));
            if (body.containsKey("strangleSlMultiplier"))   riskSettings.setStrangleSlMultiplier(effectiveMode, Double.parseDouble(body.get("strangleSlMultiplier").toString()));
            if (body.containsKey("strangleInitialCapital")) riskSettings.setStrangleInitialCapital(effectiveMode, Double.parseDouble(body.get("strangleInitialCapital").toString()));
            // Money / Risk
            if (body.containsKey("totalCapital"))      riskSettings.setTotalCapital(effectiveMode, Double.parseDouble(body.get("totalCapital").toString()));
            if (body.containsKey("maxRiskPerDayPct"))  riskSettings.setMaxRiskPerDayPct(effectiveMode, Double.parseDouble(body.get("maxRiskPerDayPct").toString()));
            if (body.containsKey("riskPerTrade"))      riskSettings.setRiskPerTrade(effectiveMode, Double.parseDouble(body.get("riskPerTrade").toString()));
            if (body.containsKey("capitalPerTrade"))   riskSettings.setCapitalPerTrade(effectiveMode, Double.parseDouble(body.get("capitalPerTrade").toString()));
            if (body.containsKey("fixedQuantity"))     riskSettings.setFixedQuantity(effectiveMode, Integer.parseInt(body.get("fixedQuantity").toString()));
            // Portfolio Risk retired.
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
