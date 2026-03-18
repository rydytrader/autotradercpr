package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.TradeHistoryService;
import com.rydytrader.autotrader.store.ModeStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class SettingsController {

    private final RiskSettingsStore   riskSettings;
    private final TradeHistoryService tradeHistoryService;
    private final ModeStore           modeStore;

    public SettingsController(RiskSettingsStore riskSettings,
                               TradeHistoryService tradeHistoryService,
                               ModeStore modeStore) {
        this.riskSettings        = riskSettings;
        this.tradeHistoryService = tradeHistoryService;
        this.modeStore           = modeStore;
    }

    // ── GET SETTINGS + TODAY'S STATUS ─────────────────────────────────────────
    @GetMapping("/api/settings/risk")
    public Map<String, Object> getSettings(
            @RequestParam(defaultValue = "") String mode) {

        String effectiveMode = resolveMode(mode);
        double todayPnl    = tradeHistoryService.getTrades().stream().mapToDouble(t -> t.getPnl()).sum();
        int    todayTrades = tradeHistoryService.getTrades().size();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode",             effectiveMode);
        result.put("activeMode",       modeStore.isLive() ? "live" : "simulator");
        result.put("tradingStartTime",  riskSettings.getTradingStartTime(effectiveMode));
        result.put("tradingEndTime",   riskSettings.getTradingEndTime(effectiveMode));
        result.put("maxDailyLoss",     riskSettings.getMaxDailyLoss(effectiveMode));
        result.put("maxTradesPerDay",  riskSettings.getMaxTradesPerDay(effectiveMode));
        result.put("autoSquareOffTime", riskSettings.getAutoSquareOffTime(effectiveMode));
        result.put("atrMultiplier",    riskSettings.getAtrMultiplier(effectiveMode));
        result.put("enableR4S4",      riskSettings.isEnableR4S4(effectiveMode));
        result.put("sessionMoveLimit", riskSettings.getSessionMoveLimit(effectiveMode));
        result.put("todayPnl",         Math.round(todayPnl * 100.0) / 100.0);
        result.put("todayTrades",      todayTrades);
        return result;
    }

    // ── SAVE SETTINGS ─────────────────────────────────────────────────────────
    @PostMapping("/api/settings/risk")
    public ResponseEntity<Map<String, Object>> saveSettings(
            @RequestParam(defaultValue = "") String mode,
            @RequestBody Map<String, Object> body) {
        try {
            String effectiveMode = resolveMode(mode);
            if (body.containsKey("tradingStartTime"))  riskSettings.setTradingStartTime(effectiveMode, body.get("tradingStartTime").toString());
            if (body.containsKey("tradingEndTime"))    riskSettings.setTradingEndTime(effectiveMode, body.get("tradingEndTime").toString());
            if (body.containsKey("maxDailyLoss"))      riskSettings.setMaxDailyLoss(effectiveMode, Double.parseDouble(body.get("maxDailyLoss").toString()));
            if (body.containsKey("maxTradesPerDay"))   riskSettings.setMaxTradesPerDay(effectiveMode, Integer.parseInt(body.get("maxTradesPerDay").toString()));
            if (body.containsKey("autoSquareOffTime")) riskSettings.setAutoSquareOffTime(effectiveMode, body.get("autoSquareOffTime").toString());
            if (body.containsKey("atrMultiplier"))    riskSettings.setAtrMultiplier(effectiveMode, Double.parseDouble(body.get("atrMultiplier").toString()));
            if (body.containsKey("enableR4S4"))       riskSettings.setEnableR4S4(effectiveMode, Boolean.parseBoolean(body.get("enableR4S4").toString()));
            if (body.containsKey("sessionMoveLimit")) riskSettings.setSessionMoveLimit(effectiveMode, Double.parseDouble(body.get("sessionMoveLimit").toString()));
            riskSettings.saveFor(effectiveMode);
            return ResponseEntity.ok(Map.of("ok", true, "message", "Settings saved"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("ok", false, "message", e.getMessage()));
        }
    }

    private String resolveMode(String mode) {
        if (mode == null || mode.isEmpty()) {
            return modeStore.isLive() ? "live" : "simulator";
        }
        return "live".equalsIgnoreCase(mode) ? "live" : "simulator";
    }
}
