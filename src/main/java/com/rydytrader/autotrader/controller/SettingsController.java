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
        // OPTION BUYING — first 5-min NIFTY futures bar vs VWAP; buy 1 OTM CE/PE.
        result.put("optionBuyingEnabled",           riskSettings.isOptionBuyingEnabled());
        result.put("optionBuyingLotsPerLeg",        riskSettings.getOptionBuyingLotsPerLeg());
        result.put("optionBuyingOrderType",         riskSettings.getOptionBuyingOrderType());
        result.put("optionBuyingSquareOffTime",     riskSettings.getOptionBuyingSquareOffTime());
        result.put("optionBuyingTargetPoints",      riskSettings.getOptionBuyingTargetPoints());
        // VWAP + SUPERTREND — chosen CE/PE nearest to target premium, VWAP-bounce
        // entry + Supertrend trail.
        result.put("vwapStEnabled",           riskSettings.isVwapStEnabled());
        result.put("vwapStLotsPerLeg",        riskSettings.getVwapStLotsPerLeg());
        result.put("vwapStStartTime",         riskSettings.getVwapStStartTime());
        result.put("vwapStTradingEndTime",    riskSettings.getVwapStTradingEndTime());
        result.put("vwapStSquareOffTime",     riskSettings.getVwapStSquareOffTime());
        result.put("vwapStTargetPremium",     riskSettings.getVwapStTargetPremium());
        result.put("vwapStStrikesRange",      riskSettings.getVwapStStrikesRange());
        result.put("vwapStCandleMinutes",     riskSettings.getVwapStCandleMinutes());
        result.put("vwapStAtrPeriod",         riskSettings.getVwapStAtrPeriod());
        result.put("vwapStMultiplier",        riskSettings.getVwapStMultiplier());
        result.put("vwapStSlBufferPoints",    riskSettings.getVwapStSlBufferPoints());
        result.put("vwapStSlBufferMode",      riskSettings.getVwapStSlBufferMode());
        result.put("vwapStSlAtrMultiplier",   riskSettings.getVwapStSlAtrMultiplier());
        result.put("vwapStMaxSlPoints",       riskSettings.getVwapStMaxSlPoints());
        result.put("vwapStSupertrendTargetMode", riskSettings.getVwapStSupertrendTargetMode());
        result.put("vwapStRewardRiskRatio",   riskSettings.getVwapStRewardRiskRatio());
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
            // OPTION BUYING — Enabled is the per-strategy kill switch; sizing / target / squareoff.
            if (body.containsKey("optionBuyingEnabled"))          riskSettings.setOptionBuyingEnabled(Boolean.parseBoolean(body.get("optionBuyingEnabled").toString()));
            if (body.containsKey("optionBuyingLotsPerLeg"))       riskSettings.setOptionBuyingLotsPerLeg(Integer.parseInt(body.get("optionBuyingLotsPerLeg").toString()));
            if (body.containsKey("optionBuyingOrderType"))        riskSettings.setOptionBuyingOrderType(body.get("optionBuyingOrderType").toString());
            if (body.containsKey("optionBuyingSquareOffTime"))    riskSettings.setOptionBuyingSquareOffTime(body.get("optionBuyingSquareOffTime").toString());
            if (body.containsKey("optionBuyingTargetPoints"))     riskSettings.setOptionBuyingTargetPoints(Double.parseDouble(body.get("optionBuyingTargetPoints").toString()));
            // VWAP + SUPERTREND
            if (body.containsKey("vwapStEnabled"))          riskSettings.setVwapStEnabled(Boolean.parseBoolean(body.get("vwapStEnabled").toString()));
            if (body.containsKey("vwapStLotsPerLeg"))       riskSettings.setVwapStLotsPerLeg(Integer.parseInt(body.get("vwapStLotsPerLeg").toString()));
            if (body.containsKey("vwapStStartTime"))        riskSettings.setVwapStStartTime(body.get("vwapStStartTime").toString());
            if (body.containsKey("vwapStTradingEndTime"))   riskSettings.setVwapStTradingEndTime(body.get("vwapStTradingEndTime").toString());
            if (body.containsKey("vwapStSquareOffTime"))    riskSettings.setVwapStSquareOffTime(body.get("vwapStSquareOffTime").toString());
            if (body.containsKey("vwapStTargetPremium"))    riskSettings.setVwapStTargetPremium(Double.parseDouble(body.get("vwapStTargetPremium").toString()));
            if (body.containsKey("vwapStStrikesRange"))     riskSettings.setVwapStStrikesRange(Integer.parseInt(body.get("vwapStStrikesRange").toString()));
            if (body.containsKey("vwapStCandleMinutes"))    riskSettings.setVwapStCandleMinutes(Integer.parseInt(body.get("vwapStCandleMinutes").toString()));
            if (body.containsKey("vwapStAtrPeriod"))        riskSettings.setVwapStAtrPeriod(Integer.parseInt(body.get("vwapStAtrPeriod").toString()));
            if (body.containsKey("vwapStMultiplier"))       riskSettings.setVwapStMultiplier(Double.parseDouble(body.get("vwapStMultiplier").toString()));
            if (body.containsKey("vwapStSlBufferPoints"))   riskSettings.setVwapStSlBufferPoints(Double.parseDouble(body.get("vwapStSlBufferPoints").toString()));
            if (body.containsKey("vwapStSlBufferMode"))     riskSettings.setVwapStSlBufferMode(body.get("vwapStSlBufferMode").toString());
            if (body.containsKey("vwapStSlAtrMultiplier"))  riskSettings.setVwapStSlAtrMultiplier(Double.parseDouble(body.get("vwapStSlAtrMultiplier").toString()));
            if (body.containsKey("vwapStMaxSlPoints"))      riskSettings.setVwapStMaxSlPoints(Double.parseDouble(body.get("vwapStMaxSlPoints").toString()));
            if (body.containsKey("vwapStSupertrendTargetMode")) riskSettings.setVwapStSupertrendTargetMode(body.get("vwapStSupertrendTargetMode").toString());
            if (body.containsKey("vwapStRewardRiskRatio"))  riskSettings.setVwapStRewardRiskRatio(Double.parseDouble(body.get("vwapStRewardRiskRatio").toString()));
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
