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
        result.put("enableSessionMoveLimit", riskSettings.isEnableSessionMoveLimit());
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
        result.put("enableGapCheck", riskSettings.isEnableGapCheck());
        result.put("enableDayHighLowTargetShift", riskSettings.isEnableDayHighLowTargetShift());
        result.put("enableDailySma200TargetShift", riskSettings.isEnableDailySma200TargetShift());
        result.put("dayHighLowShiftMinDistAtr", riskSettings.getDayHighLowShiftMinDistAtr());
        result.put("enableWeeklyLevelTargetShift", riskSettings.isEnableWeeklyLevelTargetShift());
        result.put("enableWeeklySmaTargetShift", riskSettings.isEnableWeeklySmaTargetShift());
        result.put("enableHtfHurdleFilter", riskSettings.isEnableHtfHurdleFilter());
        result.put("enableNiftyHtfHurdleFilter", riskSettings.isEnableNiftyHtfHurdleFilter());
        result.put("enableHtfSmaAlignment", riskSettings.isEnableHtfSmaAlignment());
        result.put("enableHtfSmaAlignmentCheck", riskSettings.isEnableHtfSmaAlignmentCheck());
        result.put("enableStructuralSl",    riskSettings.isEnableStructuralSl());
        result.put("structuralSlBufferAtr", riskSettings.getStructuralSlBufferAtr());
        result.put("singleLevelSlBufferAtr", riskSettings.getSingleLevelSlBufferAtr());
        result.put("dayHighLowMinAtr", riskSettings.getDayHighLowMinAtr());
        result.put("enableRiskRewardFilter", riskSettings.isEnableRiskRewardFilter());
        result.put("minRiskRewardRatio", riskSettings.getMinRiskRewardRatio());
        result.put("enableSmaTrendCheck", riskSettings.isEnableSmaTrendCheck());
        result.put("enableSmaTrendCheckLenient", riskSettings.isEnableSmaTrendCheckLenient());
        result.put("enableSmaAlignmentCheck", riskSettings.isEnableSmaAlignmentCheck());
        result.put("enableSmaAlignmentCheckLenient", riskSettings.isEnableSmaAlignmentCheckLenient());
        result.put("enableSmaVsAtpCheck", riskSettings.isEnableSmaVsAtpCheck());
        result.put("requireRtpPattern", riskSettings.isRequireRtpPattern());
        result.put("skipTradesInZigZag", riskSettings.isSkipTradesInZigZag());
        result.put("smaCloseDistanceAtr", riskSettings.getSmaCloseDistanceAtr());
        result.put("enableSmaLevelCountFilter", riskSettings.isEnableSmaLevelCountFilter());
        result.put("smaLevelMinRangePct", riskSettings.getSmaLevelMinRangePct());
        result.put("smaLevelFilterMorningSkip", riskSettings.isSmaLevelFilterMorningSkip());
        result.put("smaLevelFilterMorningSkipUntil", riskSettings.getSmaLevelFilterMorningSkipUntil());
        result.put("enableTargetShift", riskSettings.isEnableTargetShift(effectiveMode));
        result.put("enableSplitTarget", riskSettings.isEnableSplitTarget());
        result.put("t1DistancePct", riskSettings.getT1DistancePct());
        result.put("splitMinDistanceAtr", riskSettings.getSplitMinDistanceAtr());
        result.put("enableTargetTolerance", riskSettings.isEnableTargetTolerance());
        result.put("targetToleranceAtr", riskSettings.getTargetToleranceAtr());
        result.put("enableIndexAlignment", riskSettings.isEnableIndexAlignment());
        result.put("enableSmallCandleFilter", riskSettings.isEnableSmallCandleFilter(effectiveMode));
        result.put("enableLargeCandleBodyFilter", riskSettings.isEnableLargeCandleBodyFilter());
        result.put("largeCandleBodyAtrThreshold", riskSettings.getLargeCandleBodyAtrThreshold());
        result.put("smallCandleAtrThreshold", riskSettings.getSmallCandleAtrThreshold(effectiveMode));
        result.put("smallCandleBodyAtrThreshold", riskSettings.getSmallCandleBodyAtrThreshold(effectiveMode));
        result.put("smallCandleMoveAtrThreshold", riskSettings.getSmallCandleMoveAtrThreshold(effectiveMode));
        result.put("wickRejectionRatio", riskSettings.getWickRejectionRatio(effectiveMode));
        result.put("oppositeWickRatio", riskSettings.getOppositeWickRatio(effectiveMode));
        result.put("enableVolumeFilter", riskSettings.isEnableVolumeFilter(effectiveMode));
        result.put("volumeMultiple", riskSettings.getVolumeMultiple(effectiveMode));
        result.put("volumeLookback", riskSettings.getVolumeLookback(effectiveMode));
        result.put("enableTrailingSl", riskSettings.isEnableTrailingSl(effectiveMode));
        result.put("enableSmaCrossExit", riskSettings.isEnableSmaCrossExit());
        result.put("enablePriceSmaExit", riskSettings.isEnablePriceSmaExit());
        result.put("perSymbolDailyTradeLimit", riskSettings.getPerSymbolDailyTradeLimit());
        result.put("fibStage1TriggerPct", riskSettings.getFibStage1TriggerPct());
        result.put("fibStage1SlAtrMult",  riskSettings.getFibStage1SlAtrMult());
        result.put("fibStage2TriggerPct", riskSettings.getFibStage2TriggerPct());
        result.put("fibStage2SlPct",      riskSettings.getFibStage2SlPct());
        result.put("skipR3S3IvOvDays", riskSettings.isSkipR3S3IvOvDays());
        result.put("skipR3S3EvDays",   riskSettings.isSkipR3S3EvDays());
        result.put("skipR4S4IvOvDays", riskSettings.isSkipR4S4IvOvDays());
        result.put("skipR4S4EvDays",   riskSettings.isSkipR4S4EvDays());
        result.put("skipHtfR3S3NormalDays", riskSettings.isSkipHtfR3S3NormalDays());
        result.put("skipHtfR4S4NormalDays", riskSettings.isSkipHtfR4S4NormalDays());
        result.put("atrPeriod", riskSettings.getAtrPeriod());
        result.put("signalSource", riskSettings.getSignalSource());
        result.put("scannerTimeframe", riskSettings.getScannerTimeframe());
        result.put("higherTimeframe", riskSettings.getHigherTimeframe());
        result.put("enableAtpCheck", riskSettings.isEnableAtpCheck());
        result.put("narrowCprMaxWidth", riskSettings.getNarrowCprMaxWidth());
        result.put("narrowCprMinWidth", riskSettings.getNarrowCprMinWidth());
        result.put("insideCprMaxWidth", riskSettings.getInsideCprMaxWidth());
        result.put("scanUniverse", riskSettings.getScanUniverse());
        result.put("scanMinPrice", riskSettings.getScanMinPrice());
        result.put("scanMaxPrice", riskSettings.getScanMaxPrice());
        result.put("scanMinTurnover", riskSettings.getScanMinTurnover());
        result.put("scanMinVolume", riskSettings.getScanMinVolume());
        result.put("scanMinBeta", riskSettings.getScanMinBeta());
        result.put("scanMaxBeta", riskSettings.getScanMaxBeta());
        result.put("openingRangeMinutes", riskSettings.getOpeningRangeMinutes());
        result.put("enableOpeningRefresh", riskSettings.isEnableOpeningRefresh());
        result.put("openingRefreshTime", riskSettings.getOpeningRefreshTime());
        result.put("scanIncludeNS", riskSettings.isScanIncludeNS());
        result.put("scanIncludeNL", riskSettings.isScanIncludeNL());
        result.put("scanIncludeIS", riskSettings.isScanIncludeIS());
        result.put("scanIncludeIL", riskSettings.isScanIncludeIL());
        result.put("enableHpt", riskSettings.isEnableHpt());
        result.put("enableLpt", riskSettings.isEnableLpt());
        result.put("lptQtyFactor", riskSettings.getLptQtyFactor());
        result.put("enableMpt", riskSettings.isEnableMpt());
        result.put("mptQtyFactor", riskSettings.getMptQtyFactor());
        result.put("smallRangeAdrPct", riskSettings.getSmallRangeAdrPct());
        result.put("minAbsoluteProfit", riskSettings.getMinAbsoluteProfit());
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
            if (body.containsKey("enableSessionMoveLimit")) riskSettings.setEnableSessionMoveLimit(Boolean.parseBoolean(body.get("enableSessionMoveLimit").toString()));
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
            if (body.containsKey("enableGapCheck")) riskSettings.setEnableGapCheck(Boolean.parseBoolean(body.get("enableGapCheck").toString()));
            if (body.containsKey("enableDayHighLowTargetShift")) riskSettings.setEnableDayHighLowTargetShift(Boolean.parseBoolean(body.get("enableDayHighLowTargetShift").toString()));
            if (body.containsKey("enableDailySma200TargetShift")) riskSettings.setEnableDailySma200TargetShift(Boolean.parseBoolean(body.get("enableDailySma200TargetShift").toString()));
            if (body.containsKey("dayHighLowShiftMinDistAtr")) riskSettings.setDayHighLowShiftMinDistAtr(Double.parseDouble(body.get("dayHighLowShiftMinDistAtr").toString()));
            if (body.containsKey("enableWeeklyLevelTargetShift")) riskSettings.setEnableWeeklyLevelTargetShift(Boolean.parseBoolean(body.get("enableWeeklyLevelTargetShift").toString()));
            if (body.containsKey("enableWeeklySmaTargetShift")) riskSettings.setEnableWeeklySmaTargetShift(Boolean.parseBoolean(body.get("enableWeeklySmaTargetShift").toString()));
            if (body.containsKey("enableHtfHurdleFilter")) riskSettings.setEnableHtfHurdleFilter(Boolean.parseBoolean(body.get("enableHtfHurdleFilter").toString()));
            if (body.containsKey("enableNiftyHtfHurdleFilter")) riskSettings.setEnableNiftyHtfHurdleFilter(Boolean.parseBoolean(body.get("enableNiftyHtfHurdleFilter").toString()));
            if (body.containsKey("enableHtfSmaAlignment")) riskSettings.setEnableHtfSmaAlignment(Boolean.parseBoolean(body.get("enableHtfSmaAlignment").toString()));
            if (body.containsKey("enableHtfSmaAlignmentCheck")) riskSettings.setEnableHtfSmaAlignmentCheck(Boolean.parseBoolean(body.get("enableHtfSmaAlignmentCheck").toString()));
            if (body.containsKey("enableStructuralSl")) riskSettings.setEnableStructuralSl(Boolean.parseBoolean(body.get("enableStructuralSl").toString()));
            if (body.containsKey("structuralSlBufferAtr")) riskSettings.setStructuralSlBufferAtr(Double.parseDouble(body.get("structuralSlBufferAtr").toString()));
            if (body.containsKey("singleLevelSlBufferAtr")) riskSettings.setSingleLevelSlBufferAtr(Double.parseDouble(body.get("singleLevelSlBufferAtr").toString()));
            if (body.containsKey("dayHighLowMinAtr")) riskSettings.setDayHighLowMinAtr(Double.parseDouble(body.get("dayHighLowMinAtr").toString()));
            if (body.containsKey("enableRiskRewardFilter")) riskSettings.setEnableRiskRewardFilter(Boolean.parseBoolean(body.get("enableRiskRewardFilter").toString()));
            if (body.containsKey("minRiskRewardRatio")) riskSettings.setMinRiskRewardRatio(Double.parseDouble(body.get("minRiskRewardRatio").toString()));
            if (body.containsKey("enableSmaTrendCheck")) riskSettings.setEnableSmaTrendCheck(Boolean.parseBoolean(body.get("enableSmaTrendCheck").toString()));
            if (body.containsKey("enableSmaTrendCheckLenient")) riskSettings.setEnableSmaTrendCheckLenient(Boolean.parseBoolean(body.get("enableSmaTrendCheckLenient").toString()));
            if (body.containsKey("enableSmaAlignmentCheck")) riskSettings.setEnableSmaAlignmentCheck(Boolean.parseBoolean(body.get("enableSmaAlignmentCheck").toString()));
            if (body.containsKey("enableSmaAlignmentCheckLenient")) riskSettings.setEnableSmaAlignmentCheckLenient(Boolean.parseBoolean(body.get("enableSmaAlignmentCheckLenient").toString()));
            if (body.containsKey("enableSmaVsAtpCheck")) riskSettings.setEnableSmaVsAtpCheck(Boolean.parseBoolean(body.get("enableSmaVsAtpCheck").toString()));
            if (body.containsKey("requireRtpPattern")) riskSettings.setRequireRtpPattern(Boolean.parseBoolean(body.get("requireRtpPattern").toString()));
            if (body.containsKey("skipTradesInZigZag")) riskSettings.setSkipTradesInZigZag(Boolean.parseBoolean(body.get("skipTradesInZigZag").toString()));
            if (body.containsKey("smaCloseDistanceAtr")) riskSettings.setSmaCloseDistanceAtr(Double.parseDouble(body.get("smaCloseDistanceAtr").toString()));
            if (body.containsKey("enableSmaLevelCountFilter")) riskSettings.setEnableSmaLevelCountFilter(Boolean.parseBoolean(body.get("enableSmaLevelCountFilter").toString()));
            if (body.containsKey("smaLevelMinRangePct")) riskSettings.setSmaLevelMinRangePct(Integer.parseInt(body.get("smaLevelMinRangePct").toString()));
            if (body.containsKey("smaLevelFilterMorningSkip")) riskSettings.setSmaLevelFilterMorningSkip(Boolean.parseBoolean(body.get("smaLevelFilterMorningSkip").toString()));
            if (body.containsKey("smaLevelFilterMorningSkipUntil")) riskSettings.setSmaLevelFilterMorningSkipUntil(body.get("smaLevelFilterMorningSkipUntil").toString());
            if (body.containsKey("enableTargetShift")) riskSettings.setEnableTargetShift(effectiveMode, Boolean.parseBoolean(body.get("enableTargetShift").toString()));
            if (body.containsKey("enableSplitTarget")) riskSettings.setEnableSplitTarget(Boolean.parseBoolean(body.get("enableSplitTarget").toString()));
            if (body.containsKey("t1DistancePct")) riskSettings.setT1DistancePct(Integer.parseInt(body.get("t1DistancePct").toString()));
            if (body.containsKey("splitMinDistanceAtr")) riskSettings.setSplitMinDistanceAtr(Double.parseDouble(body.get("splitMinDistanceAtr").toString()));
            if (body.containsKey("enableTargetTolerance")) riskSettings.setEnableTargetTolerance(Boolean.parseBoolean(body.get("enableTargetTolerance").toString()));
            if (body.containsKey("targetToleranceAtr")) riskSettings.setTargetToleranceAtr(Double.parseDouble(body.get("targetToleranceAtr").toString()));
            if (body.containsKey("enableIndexAlignment")) riskSettings.setEnableIndexAlignment(Boolean.parseBoolean(body.get("enableIndexAlignment").toString()));
            if (body.containsKey("enableSmallCandleFilter")) riskSettings.setEnableSmallCandleFilter(effectiveMode, Boolean.parseBoolean(body.get("enableSmallCandleFilter").toString()));
            if (body.containsKey("enableLargeCandleBodyFilter")) riskSettings.setEnableLargeCandleBodyFilter(Boolean.parseBoolean(body.get("enableLargeCandleBodyFilter").toString()));
            if (body.containsKey("largeCandleBodyAtrThreshold")) riskSettings.setLargeCandleBodyAtrThreshold(Double.parseDouble(body.get("largeCandleBodyAtrThreshold").toString()));
            if (body.containsKey("smallCandleAtrThreshold")) riskSettings.setSmallCandleAtrThreshold(effectiveMode, Double.parseDouble(body.get("smallCandleAtrThreshold").toString()));
            if (body.containsKey("smallCandleBodyAtrThreshold")) riskSettings.setSmallCandleBodyAtrThreshold(effectiveMode, Double.parseDouble(body.get("smallCandleBodyAtrThreshold").toString()));
            if (body.containsKey("smallCandleMoveAtrThreshold")) riskSettings.setSmallCandleMoveAtrThreshold(effectiveMode, Double.parseDouble(body.get("smallCandleMoveAtrThreshold").toString()));
            if (body.containsKey("wickRejectionRatio")) riskSettings.setWickRejectionRatio(effectiveMode, Double.parseDouble(body.get("wickRejectionRatio").toString()));
            if (body.containsKey("oppositeWickRatio")) riskSettings.setOppositeWickRatio(effectiveMode, Double.parseDouble(body.get("oppositeWickRatio").toString()));
            if (body.containsKey("enableVolumeFilter")) riskSettings.setEnableVolumeFilter(effectiveMode, Boolean.parseBoolean(body.get("enableVolumeFilter").toString()));
            if (body.containsKey("volumeMultiple")) riskSettings.setVolumeMultiple(effectiveMode, Double.parseDouble(body.get("volumeMultiple").toString()));
            if (body.containsKey("volumeLookback")) riskSettings.setVolumeLookback(effectiveMode, Integer.parseInt(body.get("volumeLookback").toString()));
            if (body.containsKey("enableTrailingSl")) riskSettings.setEnableTrailingSl(effectiveMode, Boolean.parseBoolean(body.get("enableTrailingSl").toString()));
            if (body.containsKey("enableSmaCrossExit")) riskSettings.setEnableSmaCrossExit(Boolean.parseBoolean(body.get("enableSmaCrossExit").toString()));
            if (body.containsKey("enablePriceSmaExit")) riskSettings.setEnablePriceSmaExit(Boolean.parseBoolean(body.get("enablePriceSmaExit").toString()));
            if (body.containsKey("perSymbolDailyTradeLimit")) {
                try {
                    riskSettings.setPerSymbolDailyTradeLimit(Integer.parseInt(body.get("perSymbolDailyTradeLimit").toString()));
                } catch (NumberFormatException ignored) { /* leave at current value */ }
            }
            if (body.containsKey("fibStage1TriggerPct")) riskSettings.setFibStage1TriggerPct(Double.parseDouble(body.get("fibStage1TriggerPct").toString()));
            if (body.containsKey("fibStage1SlAtrMult"))  riskSettings.setFibStage1SlAtrMult(Double.parseDouble(body.get("fibStage1SlAtrMult").toString()));
            if (body.containsKey("fibStage2TriggerPct")) riskSettings.setFibStage2TriggerPct(Double.parseDouble(body.get("fibStage2TriggerPct").toString()));
            if (body.containsKey("fibStage2SlPct"))      riskSettings.setFibStage2SlPct(Double.parseDouble(body.get("fibStage2SlPct").toString()));
            if (body.containsKey("skipR3S3IvOvDays")) riskSettings.setSkipR3S3IvOvDays(Boolean.parseBoolean(body.get("skipR3S3IvOvDays").toString()));
            if (body.containsKey("skipR3S3EvDays"))   riskSettings.setSkipR3S3EvDays(Boolean.parseBoolean(body.get("skipR3S3EvDays").toString()));
            if (body.containsKey("skipR4S4IvOvDays")) riskSettings.setSkipR4S4IvOvDays(Boolean.parseBoolean(body.get("skipR4S4IvOvDays").toString()));
            if (body.containsKey("skipR4S4EvDays"))   riskSettings.setSkipR4S4EvDays(Boolean.parseBoolean(body.get("skipR4S4EvDays").toString()));
            if (body.containsKey("skipHtfR3S3NormalDays")) riskSettings.setSkipHtfR3S3NormalDays(Boolean.parseBoolean(body.get("skipHtfR3S3NormalDays").toString()));
            if (body.containsKey("skipHtfR4S4NormalDays")) riskSettings.setSkipHtfR4S4NormalDays(Boolean.parseBoolean(body.get("skipHtfR4S4NormalDays").toString()));
            if (body.containsKey("atrPeriod")) riskSettings.setAtrPeriod(Integer.parseInt(body.get("atrPeriod").toString()));
            if (body.containsKey("signalSource")) riskSettings.setSignalSource(body.get("signalSource").toString());
            if (body.containsKey("scannerTimeframe")) riskSettings.setScannerTimeframe(Integer.parseInt(body.get("scannerTimeframe").toString()));
            if (body.containsKey("higherTimeframe")) riskSettings.setHigherTimeframe(Integer.parseInt(body.get("higherTimeframe").toString()));
            if (body.containsKey("enableAtpCheck")) riskSettings.setEnableAtpCheck(Boolean.parseBoolean(body.get("enableAtpCheck").toString()));
            if (body.containsKey("narrowCprMaxWidth")) riskSettings.setNarrowCprMaxWidth(Double.parseDouble(body.get("narrowCprMaxWidth").toString()));
            if (body.containsKey("narrowCprMinWidth")) riskSettings.setNarrowCprMinWidth(Double.parseDouble(body.get("narrowCprMinWidth").toString()));
            if (body.containsKey("insideCprMaxWidth")) riskSettings.setInsideCprMaxWidth(Double.parseDouble(body.get("insideCprMaxWidth").toString()));
            if (body.containsKey("scanUniverse")) riskSettings.setScanUniverse(body.get("scanUniverse").toString());
            if (body.containsKey("scanMinPrice")) riskSettings.setScanMinPrice(Double.parseDouble(body.get("scanMinPrice").toString()));
            if (body.containsKey("scanMaxPrice")) riskSettings.setScanMaxPrice(Double.parseDouble(body.get("scanMaxPrice").toString()));
            if (body.containsKey("scanMinTurnover")) riskSettings.setScanMinTurnover(Double.parseDouble(body.get("scanMinTurnover").toString()));
            if (body.containsKey("scanMinVolume")) riskSettings.setScanMinVolume(Long.parseLong(body.get("scanMinVolume").toString()));
            if (body.containsKey("scanMinBeta")) riskSettings.setScanMinBeta(Double.parseDouble(body.get("scanMinBeta").toString()));
            if (body.containsKey("scanMaxBeta")) riskSettings.setScanMaxBeta(Double.parseDouble(body.get("scanMaxBeta").toString()));
            if (body.containsKey("openingRangeMinutes")) riskSettings.setOpeningRangeMinutes(Integer.parseInt(body.get("openingRangeMinutes").toString()));
            if (body.containsKey("enableOpeningRefresh")) riskSettings.setEnableOpeningRefresh(Boolean.parseBoolean(body.get("enableOpeningRefresh").toString()));
            if (body.containsKey("openingRefreshTime")) riskSettings.setOpeningRefreshTime(body.get("openingRefreshTime").toString());
            if (body.containsKey("scanIncludeNS")) riskSettings.setScanIncludeNS(Boolean.parseBoolean(body.get("scanIncludeNS").toString()));
            if (body.containsKey("scanIncludeNL")) riskSettings.setScanIncludeNL(Boolean.parseBoolean(body.get("scanIncludeNL").toString()));
            if (body.containsKey("scanIncludeIS")) riskSettings.setScanIncludeIS(Boolean.parseBoolean(body.get("scanIncludeIS").toString()));
            if (body.containsKey("scanIncludeIL")) riskSettings.setScanIncludeIL(Boolean.parseBoolean(body.get("scanIncludeIL").toString()));
            if (body.containsKey("enableHpt")) riskSettings.setEnableHpt(Boolean.parseBoolean(body.get("enableHpt").toString()));
            if (body.containsKey("enableLpt")) riskSettings.setEnableLpt(Boolean.parseBoolean(body.get("enableLpt").toString()));
            if (body.containsKey("lptQtyFactor")) riskSettings.setLptQtyFactor(Double.parseDouble(body.get("lptQtyFactor").toString()));
            if (body.containsKey("enableMpt")) riskSettings.setEnableMpt(Boolean.parseBoolean(body.get("enableMpt").toString()));
            if (body.containsKey("mptQtyFactor")) riskSettings.setMptQtyFactor(Double.parseDouble(body.get("mptQtyFactor").toString()));
            if (body.containsKey("smallRangeAdrPct")) {
                riskSettings.setSmallRangeAdrPct(Double.parseDouble(body.get("smallRangeAdrPct").toString()));
                bhavcopyService.reclassifyNarrowRangeTypes();
            }
            if (body.containsKey("minAbsoluteProfit")) riskSettings.setMinAbsoluteProfit(Double.parseDouble(body.get("minAbsoluteProfit").toString()));
            riskSettings.saveFor(effectiveMode);
            return ResponseEntity.ok(Map.of("ok", true, "message", "Settings saved"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("ok", false, "message", e.getMessage()));
        }
    }

    // ── NARROW CPR STOCKS ─────────────────────────────────────────────────────
    @GetMapping("/api/narrow-cpr")
    public Map<String, Object> getNarrowCprStocks() {
        double maxWidth = riskSettings.getNarrowCprMaxWidth();
        double minWidth = riskSettings.getNarrowCprMinWidth();
        List<CprLevels> narrow = bhavcopyService.getAllCprLevels().values().stream()
            .filter(c -> !bhavcopyService.isIndex(c.getSymbol()))
            .filter(c -> c.getCprWidthPct() >= minWidth && c.getCprWidthPct() < maxWidth)
            .sorted(java.util.Comparator.comparing(CprLevels::getSymbol))
            .collect(Collectors.toList());
        List<Map<String, Object>> list = narrow.stream().map(c -> buildStockRow(c)).collect(Collectors.toList());

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
        double insideMaxWidth = riskSettings.getInsideCprMaxWidth();
        List<CprLevels> inside = bhavcopyService.getInsideCprStocks().stream()
            .filter(c -> !bhavcopyService.isIndex(c.getSymbol()))
            .filter(c -> insideMaxWidth <= 0 || c.getCprWidthPct() < insideMaxWidth)
            .sorted(java.util.Comparator.comparing(CprLevels::getSymbol))
            .collect(Collectors.toList());
        List<Map<String, Object>> list = inside.stream().map(c -> buildStockRow(c)).collect(Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("date", bhavcopyService.getCachedDate());
        result.put("previousDate", bhavcopyService.getPreviousDate());
        result.put("totalNfoStocks", bhavcopyService.getLoadedCount());
        result.put("insideCount", inside.size());
        result.put("stocks", list);
        return result;
    }

    private Map<String, Object> buildStockRow(CprLevels c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("symbol", c.getSymbol());
        m.put("close", Math.round(c.getClose() * 100.0) / 100.0);
        m.put("cprWidthPct", Math.round(c.getCprWidthPct() * 1000.0) / 1000.0);
        m.put("volume", c.getVolume());
        m.put("avgVolume20", c.getAvgVolume20());
        m.put("volumeMultiple", c.getVolumeMultiple());
        m.put("turnover", Math.round(c.getTurnover() / 100000.0) / 100.0); // ₹ Cr
        m.put("avgTurnover20", Math.round(c.getAvgTurnover20() / 100000.0) / 100.0); // ₹ Cr
        m.put("turnoverMultiple", c.getTurnoverMultiple());
        m.put("beta", c.getBeta());
        m.put("capCategory", c.getCapCategory() != null ? c.getCapCategory() : "SMALL");
        return m;
    }

    private String resolveMode(String mode) {
        return "live";
    }
}
