package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.service.BhavcopyService;
import com.rydytrader.autotrader.service.TradeHistoryService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class SettingsController {

    private final RiskSettingsStore   riskSettings;
    private final TradeHistoryService tradeHistoryService;
    private final BhavcopyService     bhavcopyService;

    public SettingsController(RiskSettingsStore riskSettings,
                               TradeHistoryService tradeHistoryService,
                               BhavcopyService bhavcopyService) {
        this.riskSettings        = riskSettings;
        this.tradeHistoryService = tradeHistoryService;
        this.bhavcopyService     = bhavcopyService;
    }

    // ── GET SETTINGS + TODAY'S STATUS ─────────────────────────────────────────
    @GetMapping("/api/settings/risk")
    public Map<String, Object> getSettings(
            @RequestParam(defaultValue = "") String mode) {

        String effectiveMode = resolveMode(mode);
        double todayPnl    = tradeHistoryService.getTrades().stream().mapToDouble(t -> t.getNetPnl()).sum();
        int    todayTrades = tradeHistoryService.getTrades().size();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode",             effectiveMode);
        result.put("activeMode",       "live");
        result.put("tradingStartTime",  riskSettings.getTradingStartTime(effectiveMode));
        result.put("tradingEndTime",   riskSettings.getTradingEndTime(effectiveMode));
        result.put("totalCapital",     riskSettings.getTotalCapital(effectiveMode));
        result.put("maxRiskPerDayPct", riskSettings.getMaxRiskPerDayPct(effectiveMode));
        result.put("riskPerTrade",     riskSettings.getRiskPerTrade(effectiveMode));
        result.put("maxDailyLoss",     riskSettings.getMaxDailyLoss(effectiveMode));
        result.put("autoSquareOffTime", riskSettings.getAutoSquareOffTime(effectiveMode));
        result.put("atrMultiplier",    riskSettings.getAtrMultiplier(effectiveMode));
        result.put("enableR4S4",      riskSettings.isEnableR4S4(effectiveMode));
        result.put("sessionMoveLimit", riskSettings.getSessionMoveLimit(effectiveMode));
        result.put("brokeragePerOrder", riskSettings.getBrokeragePerOrder(effectiveMode));
        result.put("sttRate",         riskSettings.getSttRate(effectiveMode));
        result.put("exchangeRate",    riskSettings.getExchangeRate(effectiveMode));
        result.put("gstRate",         riskSettings.getGstRate(effectiveMode));
        result.put("sebiRate",        riskSettings.getSebiRate(effectiveMode));
        result.put("stampDutyRate",   riskSettings.getStampDutyRate(effectiveMode));
        result.put("brokeragePct",    riskSettings.getBrokeragePct(effectiveMode));
        result.put("fixedQuantity",   riskSettings.getFixedQuantity(effectiveMode));
        result.put("capitalPerTrade", riskSettings.getCapitalPerTrade(effectiveMode));
        result.put("telegramAlertFrequency", riskSettings.getTelegramAlertFrequency(effectiveMode));
        result.put("enableLargeCandleFilter", riskSettings.isEnableLargeCandleFilter(effectiveMode));
        result.put("largeCandleAtrThreshold", riskSettings.getLargeCandleAtrThreshold(effectiveMode));
        result.put("enableTargetShift", riskSettings.isEnableTargetShift(effectiveMode));
        result.put("targetShiftAtrThreshold", riskSettings.getTargetShiftAtrThreshold(effectiveMode));
        result.put("enableSmallCandleFilter", riskSettings.isEnableSmallCandleFilter(effectiveMode));
        result.put("smallCandleAtrThreshold", riskSettings.getSmallCandleAtrThreshold(effectiveMode));
        result.put("wickRejectionRatio", riskSettings.getWickRejectionRatio(effectiveMode));
        result.put("oppositeWickRatio", riskSettings.getOppositeWickRatio(effectiveMode));
        result.put("enableVolumeFilter", riskSettings.isEnableVolumeFilter(effectiveMode));
        result.put("volumeMultiple", riskSettings.getVolumeMultiple(effectiveMode));
        result.put("volumeLookback", riskSettings.getVolumeLookback(effectiveMode));
        result.put("enableTrailingSl", riskSettings.isEnableTrailingSl(effectiveMode));
        result.put("trailingSlNoTarget", riskSettings.isTrailingSlNoTarget());
        result.put("r3s3QtyFactor", riskSettings.getR3s3QtyFactor());
        result.put("r4s4QtyFactor", riskSettings.getR4s4QtyFactor());
        result.put("atrPeriod", riskSettings.getAtrPeriod());
        result.put("trailingSlActivationAtr", riskSettings.getTrailingSlActivationAtr());
        result.put("chandelierPeriod", riskSettings.getChandelierPeriod(effectiveMode));
        result.put("chandelierMultiplier", riskSettings.getChandelierMultiplier(effectiveMode));
        result.put("signalSource", riskSettings.getSignalSource());
        result.put("scannerTimeframe", riskSettings.getScannerTimeframe());
        result.put("enableAtpCheck", riskSettings.isEnableAtpCheck());
        result.put("scanIncludeNS", riskSettings.isScanIncludeNS());
        result.put("scanIncludeNL", riskSettings.isScanIncludeNL());
        result.put("scanIncludeIS", riskSettings.isScanIncludeIS());
        result.put("scanIncludeIL", riskSettings.isScanIncludeIL());
        result.put("enableHpt", riskSettings.isEnableHpt());
        result.put("enableMpt", riskSettings.isEnableMpt());
        result.put("enableLpt", riskSettings.isEnableLpt());
        result.put("mptQtyFactor", riskSettings.getMptQtyFactor());
        result.put("lptQtyFactor", riskSettings.getLptQtyFactor());
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
            if (body.containsKey("totalCapital"))      riskSettings.setTotalCapital(effectiveMode, Double.parseDouble(body.get("totalCapital").toString()));
            if (body.containsKey("maxRiskPerDayPct")) riskSettings.setMaxRiskPerDayPct(effectiveMode, Double.parseDouble(body.get("maxRiskPerDayPct").toString()));
            if (body.containsKey("riskPerTrade"))     riskSettings.setRiskPerTrade(effectiveMode, Double.parseDouble(body.get("riskPerTrade").toString()));
            if (body.containsKey("autoSquareOffTime")) riskSettings.setAutoSquareOffTime(effectiveMode, body.get("autoSquareOffTime").toString());
            if (body.containsKey("atrMultiplier"))    riskSettings.setAtrMultiplier(effectiveMode, Double.parseDouble(body.get("atrMultiplier").toString()));
            if (body.containsKey("enableR4S4"))       riskSettings.setEnableR4S4(effectiveMode, Boolean.parseBoolean(body.get("enableR4S4").toString()));
            if (body.containsKey("sessionMoveLimit")) riskSettings.setSessionMoveLimit(effectiveMode, Double.parseDouble(body.get("sessionMoveLimit").toString()));
            if (body.containsKey("brokeragePerOrder")) riskSettings.setBrokeragePerOrder(effectiveMode, Double.parseDouble(body.get("brokeragePerOrder").toString()));
            if (body.containsKey("sttRate"))         riskSettings.setSttRate(effectiveMode, Double.parseDouble(body.get("sttRate").toString()));
            if (body.containsKey("exchangeRate"))    riskSettings.setExchangeRate(effectiveMode, Double.parseDouble(body.get("exchangeRate").toString()));
            if (body.containsKey("gstRate"))         riskSettings.setGstRate(effectiveMode, Double.parseDouble(body.get("gstRate").toString()));
            if (body.containsKey("sebiRate"))        riskSettings.setSebiRate(effectiveMode, Double.parseDouble(body.get("sebiRate").toString()));
            if (body.containsKey("stampDutyRate"))   riskSettings.setStampDutyRate(effectiveMode, Double.parseDouble(body.get("stampDutyRate").toString()));
            if (body.containsKey("brokeragePct"))    riskSettings.setBrokeragePct(effectiveMode, Double.parseDouble(body.get("brokeragePct").toString()));
            if (body.containsKey("fixedQuantity"))   riskSettings.setFixedQuantity(effectiveMode, Integer.parseInt(body.get("fixedQuantity").toString()));
            if (body.containsKey("capitalPerTrade")) riskSettings.setCapitalPerTrade(effectiveMode, Double.parseDouble(body.get("capitalPerTrade").toString()));
            if (body.containsKey("telegramAlertFrequency")) riskSettings.setTelegramAlertFrequency(effectiveMode, Integer.parseInt(body.get("telegramAlertFrequency").toString()));
            if (body.containsKey("enableLargeCandleFilter")) riskSettings.setEnableLargeCandleFilter(effectiveMode, Boolean.parseBoolean(body.get("enableLargeCandleFilter").toString()));
            if (body.containsKey("largeCandleAtrThreshold")) riskSettings.setLargeCandleAtrThreshold(effectiveMode, Double.parseDouble(body.get("largeCandleAtrThreshold").toString()));
            if (body.containsKey("enableTargetShift")) riskSettings.setEnableTargetShift(effectiveMode, Boolean.parseBoolean(body.get("enableTargetShift").toString()));
            if (body.containsKey("targetShiftAtrThreshold")) riskSettings.setTargetShiftAtrThreshold(effectiveMode, Double.parseDouble(body.get("targetShiftAtrThreshold").toString()));
            if (body.containsKey("enableSmallCandleFilter")) riskSettings.setEnableSmallCandleFilter(effectiveMode, Boolean.parseBoolean(body.get("enableSmallCandleFilter").toString()));
            if (body.containsKey("smallCandleAtrThreshold")) riskSettings.setSmallCandleAtrThreshold(effectiveMode, Double.parseDouble(body.get("smallCandleAtrThreshold").toString()));
            if (body.containsKey("wickRejectionRatio")) riskSettings.setWickRejectionRatio(effectiveMode, Double.parseDouble(body.get("wickRejectionRatio").toString()));
            if (body.containsKey("oppositeWickRatio")) riskSettings.setOppositeWickRatio(effectiveMode, Double.parseDouble(body.get("oppositeWickRatio").toString()));
            if (body.containsKey("enableVolumeFilter")) riskSettings.setEnableVolumeFilter(effectiveMode, Boolean.parseBoolean(body.get("enableVolumeFilter").toString()));
            if (body.containsKey("volumeMultiple")) riskSettings.setVolumeMultiple(effectiveMode, Double.parseDouble(body.get("volumeMultiple").toString()));
            if (body.containsKey("volumeLookback")) riskSettings.setVolumeLookback(effectiveMode, Integer.parseInt(body.get("volumeLookback").toString()));
            if (body.containsKey("enableTrailingSl")) riskSettings.setEnableTrailingSl(effectiveMode, Boolean.parseBoolean(body.get("enableTrailingSl").toString()));
            if (body.containsKey("trailingSlNoTarget")) riskSettings.setTrailingSlNoTarget(Boolean.parseBoolean(body.get("trailingSlNoTarget").toString()));
            if (body.containsKey("r3s3QtyFactor")) riskSettings.setR3s3QtyFactor(Double.parseDouble(body.get("r3s3QtyFactor").toString()));
            if (body.containsKey("r4s4QtyFactor")) riskSettings.setR4s4QtyFactor(Double.parseDouble(body.get("r4s4QtyFactor").toString()));
            if (body.containsKey("atrPeriod")) riskSettings.setAtrPeriod(Integer.parseInt(body.get("atrPeriod").toString()));
            if (body.containsKey("trailingSlActivationAtr")) riskSettings.setTrailingSlActivationAtr(Double.parseDouble(body.get("trailingSlActivationAtr").toString()));
            if (body.containsKey("chandelierPeriod")) riskSettings.setChandelierPeriod(effectiveMode, Integer.parseInt(body.get("chandelierPeriod").toString()));
            if (body.containsKey("chandelierMultiplier")) riskSettings.setChandelierMultiplier(effectiveMode, Double.parseDouble(body.get("chandelierMultiplier").toString()));
            if (body.containsKey("signalSource")) riskSettings.setSignalSource(body.get("signalSource").toString());
            if (body.containsKey("scannerTimeframe")) riskSettings.setScannerTimeframe(Integer.parseInt(body.get("scannerTimeframe").toString()));
            if (body.containsKey("enableAtpCheck")) riskSettings.setEnableAtpCheck(Boolean.parseBoolean(body.get("enableAtpCheck").toString()));
            if (body.containsKey("scanIncludeNS")) riskSettings.setScanIncludeNS(Boolean.parseBoolean(body.get("scanIncludeNS").toString()));
            if (body.containsKey("scanIncludeNL")) riskSettings.setScanIncludeNL(Boolean.parseBoolean(body.get("scanIncludeNL").toString()));
            if (body.containsKey("scanIncludeIS")) riskSettings.setScanIncludeIS(Boolean.parseBoolean(body.get("scanIncludeIS").toString()));
            if (body.containsKey("scanIncludeIL")) riskSettings.setScanIncludeIL(Boolean.parseBoolean(body.get("scanIncludeIL").toString()));
            if (body.containsKey("enableHpt")) riskSettings.setEnableHpt(Boolean.parseBoolean(body.get("enableHpt").toString()));
            if (body.containsKey("enableMpt")) riskSettings.setEnableMpt(Boolean.parseBoolean(body.get("enableMpt").toString()));
            if (body.containsKey("enableLpt")) riskSettings.setEnableLpt(Boolean.parseBoolean(body.get("enableLpt").toString()));
            if (body.containsKey("mptQtyFactor")) riskSettings.setMptQtyFactor(Double.parseDouble(body.get("mptQtyFactor").toString()));
            if (body.containsKey("lptQtyFactor")) riskSettings.setLptQtyFactor(Double.parseDouble(body.get("lptQtyFactor").toString()));
            riskSettings.saveFor(effectiveMode);
            return ResponseEntity.ok(Map.of("ok", true, "message", "Settings saved"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("ok", false, "message", e.getMessage()));
        }
    }

    // ── NARROW CPR STOCKS ─────────────────────────────────────────────────────
    @GetMapping("/api/narrow-cpr")
    public Map<String, Object> getNarrowCprStocks() {
        List<CprLevels> narrow = bhavcopyService.getNarrowCprStocks();
        List<Map<String, Object>> list = narrow.stream().map(c -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("symbol", c.getSymbol());
            m.put("close", Math.round(c.getClose() * 100.0) / 100.0);
            m.put("pivot", Math.round(c.getPivot() * 100.0) / 100.0);
            m.put("tc", Math.round(c.getTc() * 100.0) / 100.0);
            m.put("bc", Math.round(c.getBc() * 100.0) / 100.0);
            m.put("cprWidthPct", Math.round(c.getCprWidthPct() * 1000.0) / 1000.0);
            m.put("r1", Math.round(c.getR1() * 100.0) / 100.0);
            m.put("s1", Math.round(c.getS1() * 100.0) / 100.0);
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("date", bhavcopyService.getCachedDate());
        result.put("totalNfoStocks", bhavcopyService.getLoadedCount());
        result.put("narrowCount", narrow.size());
        result.put("stocks", list);
        return result;
    }

    // ── INSIDE CPR STOCKS ─────────────────────────────────────────────────────
    @GetMapping("/api/inside-cpr")
    public Map<String, Object> getInsideCprStocks() {
        List<CprLevels> inside = bhavcopyService.getInsideCprStocks();
        List<Map<String, Object>> list = inside.stream().map(c -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("symbol", c.getSymbol());
            m.put("close", Math.round(c.getClose() * 100.0) / 100.0);
            m.put("pivot", Math.round(c.getPivot() * 100.0) / 100.0);
            m.put("tc", Math.round(c.getTc() * 100.0) / 100.0);
            m.put("bc", Math.round(c.getBc() * 100.0) / 100.0);
            m.put("cprWidthPct", Math.round(c.getCprWidthPct() * 1000.0) / 1000.0);
            m.put("r1", Math.round(c.getR1() * 100.0) / 100.0);
            m.put("s1", Math.round(c.getS1() * 100.0) / 100.0);
            // Previous day CPR for verification
            CprLevels prev = bhavcopyService.getPreviousCpr(c.getSymbol());
            if (prev != null) {
                m.put("prevTc", Math.round(prev.getTc() * 100.0) / 100.0);
                m.put("prevBc", Math.round(prev.getBc() * 100.0) / 100.0);
                m.put("prevPivot", Math.round(prev.getPivot() * 100.0) / 100.0);
            }
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("date", bhavcopyService.getCachedDate());
        result.put("previousDate", bhavcopyService.getPreviousDate());
        result.put("totalNfoStocks", bhavcopyService.getLoadedCount());
        result.put("insideCount", inside.size());
        result.put("stocks", list);
        return result;
    }

    private String resolveMode(String mode) {
        return "live";
    }
}
