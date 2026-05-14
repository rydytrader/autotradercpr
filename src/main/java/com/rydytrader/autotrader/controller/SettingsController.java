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
        result.put("enableWeeklyLevelTargetShift", riskSettings.isEnableWeeklyLevelTargetShift());
        result.put("enableHtfHurdleFilter", riskSettings.isEnableHtfHurdleFilter());
        result.put("enableNiftyHtfHurdleFilter", riskSettings.isEnableNiftyHtfHurdleFilter());
        result.put("niftyHurdleMinHeadroomAtr", riskSettings.getNiftyHurdleMinHeadroomAtr());
        result.put("enableNifty5mHurdleFilter", riskSettings.isEnableNifty5mHurdleFilter());
        result.put("nifty5mHurdleMinHeadroomAtr", riskSettings.getNifty5mHurdleMinHeadroomAtr());
        result.put("enableHtfCandleFilter", riskSettings.isEnableHtfCandleFilter());
        result.put("enableStructuralSl",    riskSettings.isEnableStructuralSl());
        result.put("structuralSlBufferAtr", riskSettings.getStructuralSlBufferAtr());
        result.put("singleLevelSlBufferAtr", riskSettings.getSingleLevelSlBufferAtr());
        result.put("dayHighLowMinAtr", riskSettings.getDayHighLowMinAtr());
        result.put("enableRiskRewardFilter", riskSettings.isEnableRiskRewardFilter());
        result.put("minRiskRewardRatio", riskSettings.getMinRiskRewardRatio());
        result.put("enableSmaTrendCheck", riskSettings.isEnableSmaTrendCheck());
        result.put("enableSmaVsAtpCheck", riskSettings.isEnableSmaVsAtpCheck());
        result.put("enableSmaLevelCountFilter", riskSettings.isEnableSmaLevelCountFilter());
        result.put("smaLevelMinRangePct", riskSettings.getSmaLevelMinRangePct());
        result.put("smaLevelFilterMorningSkip", riskSettings.isSmaLevelFilterMorningSkip());
        result.put("smaLevelFilterMorningSkipUntil", riskSettings.getSmaLevelFilterMorningSkipUntil());
        result.put("enableTargetShift", riskSettings.isEnableTargetShift(effectiveMode));
        result.put("enableTargetTolerance", riskSettings.isEnableTargetTolerance());
        result.put("targetToleranceAtr", riskSettings.getTargetToleranceAtr());
        result.put("enableIndexAlignment", riskSettings.isEnableIndexAlignment());
        result.put("enableNiftySma20Factor", riskSettings.isEnableNiftySma20Factor());
        result.put("marubozuBodyAtrMult",          riskSettings.getMarubozuBodyAtrMult());
        result.put("marubozuMaxBodyAtrMult",       riskSettings.getMarubozuMaxBodyAtrMult());
        result.put("marubozuMaxWicksPctOfBody",    riskSettings.getMarubozuMaxWicksPctOfBody());
        result.put("goodSizeCandleBodyAtrMult",          riskSettings.getGoodSizeCandleBodyAtrMult());
        result.put("goodSizeCandleMaxBodyAtrMult",       riskSettings.getGoodSizeCandleMaxBodyAtrMult());
        result.put("goodSizeCandleMaxOppositeWickRatio", riskSettings.getGoodSizeCandleMaxOppositeWickRatio());
        result.put("pinBarRejectionWickBodyMult",  riskSettings.getPinBarRejectionWickBodyMult());
        result.put("pinBarOppositeWickBodyMult",   riskSettings.getPinBarOppositeWickBodyMult());
        result.put("pinBarSmallBodyMaxRangeRatio",    riskSettings.getPinBarSmallBodyMaxRangeRatio());
        result.put("pinBarDominantWickMinRangeRatio", riskSettings.getPinBarDominantWickMinRangeRatio());
        result.put("pinBarOppositeWickMaxRangeRatio", riskSettings.getPinBarOppositeWickMaxRangeRatio());
        result.put("engulfingMinBodyMultiple",     riskSettings.getEngulfingMinBodyMultiple());
        result.put("engulfingMinBodyAtrMult",      riskSettings.getEngulfingMinBodyAtrMult());
        result.put("engulfingMaxBodyAtrMult",      riskSettings.getEngulfingMaxBodyAtrMult());
        result.put("piercingPrevBodyAtrMult",      riskSettings.getPiercingPrevBodyAtrMult());
        result.put("piercingPenetrationPct",       riskSettings.getPiercingPenetrationPct());
        result.put("tweezerPrevBodyAtrMult",       riskSettings.getTweezerPrevBodyAtrMult());
        result.put("tweezerLowHighMatchAtr",       riskSettings.getTweezerLowHighMatchAtr());
        result.put("haramiBodyAtrMult",            riskSettings.getHaramiBodyAtrMult());
        result.put("haramiInnerBodyMaxRatio",      riskSettings.getHaramiInnerBodyMaxRatio());
        result.put("dojiBodyMaxRangeRatio",        riskSettings.getDojiBodyMaxRangeRatio());
        result.put("dojiConfirmBodyAtrMult",       riskSettings.getDojiConfirmBodyAtrMult());
        result.put("dojiConfirmMaxBodyAtrMult",    riskSettings.getDojiConfirmMaxBodyAtrMult());
        result.put("starOuterBodyAtrMult",         riskSettings.getStarOuterBodyAtrMult());
        result.put("starOuterMaxBodyAtrMult",      riskSettings.getStarOuterMaxBodyAtrMult());
        result.put("starMiddleBodyMaxMultOfOuter", riskSettings.getStarMiddleBodyMaxMultOfOuter());
        result.put("levelTouchToleranceAtr",       riskSettings.getLevelTouchToleranceAtr());
        result.put("enableTrailingSl", riskSettings.isEnableTrailingSl(effectiveMode));
        result.put("enablePriceSmaExit", riskSettings.isEnablePriceSmaExit());
        result.put("virginCprExpiryDays", riskSettings.getVirginCprExpiryDays());
        result.put("enableVirginCprHurdleFilter", riskSettings.isEnableVirginCprHurdleFilter());
        result.put("virginCprHurdleHeadroomAtr", riskSettings.getVirginCprHurdleHeadroomAtr());
        result.put("breakevenTriggerPct", riskSettings.getBreakevenTriggerPct());
        result.put("breakevenSlAtrMult",  riskSettings.getBreakevenSlAtrMult());
        result.put("skipR3S3IvOvDays", riskSettings.isSkipR3S3IvOvDays());
        result.put("skipR3S3EvDays",   riskSettings.isSkipR3S3EvDays());
        result.put("skipR4S4IvOvDays", riskSettings.isSkipR4S4IvOvDays());
        result.put("skipR4S4EvDays",   riskSettings.isSkipR4S4EvDays());
        result.put("skipHtfR3S3NormalDays", riskSettings.isSkipHtfR3S3NormalDays());
        result.put("skipHtfR4S4NormalDays", riskSettings.isSkipHtfR4S4NormalDays());
        result.put("enableMeanReversionTrades", riskSettings.isEnableMeanReversionTrades());
        result.put("enableMagnetTrades",        riskSettings.isEnableMagnetTrades());
        result.put("magnetTradesQtyFactor",     riskSettings.getMagnetTradesQtyFactor());
        result.put("meanReversionQtyFactor",    riskSettings.getMeanReversionQtyFactor());
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
        result.put("enableOpeningRefresh", riskSettings.isEnableOpeningRefresh());
        result.put("openingRefreshTime", riskSettings.getOpeningRefreshTime());
        result.put("scanOnlyNifty50", riskSettings.isScanOnlyNifty50());
        result.put("enableHpt", riskSettings.isEnableHpt());
        result.put("enableMpt", riskSettings.isEnableMpt());
        result.put("mptQtyFactor", riskSettings.getMptQtyFactor());
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
            if (body.containsKey("enableWeeklyLevelTargetShift")) riskSettings.setEnableWeeklyLevelTargetShift(Boolean.parseBoolean(body.get("enableWeeklyLevelTargetShift").toString()));
            if (body.containsKey("enableHtfHurdleFilter")) riskSettings.setEnableHtfHurdleFilter(Boolean.parseBoolean(body.get("enableHtfHurdleFilter").toString()));
            if (body.containsKey("enableNiftyHtfHurdleFilter")) riskSettings.setEnableNiftyHtfHurdleFilter(Boolean.parseBoolean(body.get("enableNiftyHtfHurdleFilter").toString()));
            if (body.containsKey("niftyHurdleMinHeadroomAtr")) riskSettings.setNiftyHurdleMinHeadroomAtr(Double.parseDouble(body.get("niftyHurdleMinHeadroomAtr").toString()));
            if (body.containsKey("enableNifty5mHurdleFilter")) riskSettings.setEnableNifty5mHurdleFilter(Boolean.parseBoolean(body.get("enableNifty5mHurdleFilter").toString()));
            if (body.containsKey("nifty5mHurdleMinHeadroomAtr")) riskSettings.setNifty5mHurdleMinHeadroomAtr(Double.parseDouble(body.get("nifty5mHurdleMinHeadroomAtr").toString()));
            if (body.containsKey("enableHtfCandleFilter")) riskSettings.setEnableHtfCandleFilter(Boolean.parseBoolean(body.get("enableHtfCandleFilter").toString()));
            if (body.containsKey("enableStructuralSl")) riskSettings.setEnableStructuralSl(Boolean.parseBoolean(body.get("enableStructuralSl").toString()));
            if (body.containsKey("structuralSlBufferAtr")) riskSettings.setStructuralSlBufferAtr(Double.parseDouble(body.get("structuralSlBufferAtr").toString()));
            if (body.containsKey("singleLevelSlBufferAtr")) riskSettings.setSingleLevelSlBufferAtr(Double.parseDouble(body.get("singleLevelSlBufferAtr").toString()));
            if (body.containsKey("dayHighLowMinAtr")) riskSettings.setDayHighLowMinAtr(Double.parseDouble(body.get("dayHighLowMinAtr").toString()));
            if (body.containsKey("enableRiskRewardFilter")) riskSettings.setEnableRiskRewardFilter(Boolean.parseBoolean(body.get("enableRiskRewardFilter").toString()));
            if (body.containsKey("minRiskRewardRatio")) riskSettings.setMinRiskRewardRatio(Double.parseDouble(body.get("minRiskRewardRatio").toString()));
            if (body.containsKey("enableSmaTrendCheck")) riskSettings.setEnableSmaTrendCheck(Boolean.parseBoolean(body.get("enableSmaTrendCheck").toString()));
            if (body.containsKey("enableSmaVsAtpCheck")) riskSettings.setEnableSmaVsAtpCheck(Boolean.parseBoolean(body.get("enableSmaVsAtpCheck").toString()));
            if (body.containsKey("enableSmaLevelCountFilter")) riskSettings.setEnableSmaLevelCountFilter(Boolean.parseBoolean(body.get("enableSmaLevelCountFilter").toString()));
            if (body.containsKey("smaLevelMinRangePct")) riskSettings.setSmaLevelMinRangePct(Integer.parseInt(body.get("smaLevelMinRangePct").toString()));
            if (body.containsKey("smaLevelFilterMorningSkip")) riskSettings.setSmaLevelFilterMorningSkip(Boolean.parseBoolean(body.get("smaLevelFilterMorningSkip").toString()));
            if (body.containsKey("smaLevelFilterMorningSkipUntil")) riskSettings.setSmaLevelFilterMorningSkipUntil(body.get("smaLevelFilterMorningSkipUntil").toString());
            if (body.containsKey("enableTargetShift")) riskSettings.setEnableTargetShift(effectiveMode, Boolean.parseBoolean(body.get("enableTargetShift").toString()));
            if (body.containsKey("enableTargetTolerance")) riskSettings.setEnableTargetTolerance(Boolean.parseBoolean(body.get("enableTargetTolerance").toString()));
            if (body.containsKey("targetToleranceAtr")) riskSettings.setTargetToleranceAtr(Double.parseDouble(body.get("targetToleranceAtr").toString()));
            if (body.containsKey("enableIndexAlignment"))   riskSettings.setEnableIndexAlignment(Boolean.parseBoolean(body.get("enableIndexAlignment").toString()));
            if (body.containsKey("enableNiftySma20Factor")) riskSettings.setEnableNiftySma20Factor(Boolean.parseBoolean(body.get("enableNiftySma20Factor").toString()));
            if (body.containsKey("marubozuBodyAtrMult"))         riskSettings.setMarubozuBodyAtrMult(Double.parseDouble(body.get("marubozuBodyAtrMult").toString()));
            if (body.containsKey("marubozuMaxBodyAtrMult"))      riskSettings.setMarubozuMaxBodyAtrMult(Double.parseDouble(body.get("marubozuMaxBodyAtrMult").toString()));
            if (body.containsKey("marubozuMaxWicksPctOfBody"))   riskSettings.setMarubozuMaxWicksPctOfBody(Double.parseDouble(body.get("marubozuMaxWicksPctOfBody").toString()));
            if (body.containsKey("goodSizeCandleBodyAtrMult"))   riskSettings.setGoodSizeCandleBodyAtrMult(Double.parseDouble(body.get("goodSizeCandleBodyAtrMult").toString()));
            if (body.containsKey("goodSizeCandleMaxBodyAtrMult")) riskSettings.setGoodSizeCandleMaxBodyAtrMult(Double.parseDouble(body.get("goodSizeCandleMaxBodyAtrMult").toString()));
            if (body.containsKey("goodSizeCandleMaxOppositeWickRatio")) riskSettings.setGoodSizeCandleMaxOppositeWickRatio(Double.parseDouble(body.get("goodSizeCandleMaxOppositeWickRatio").toString()));
            if (body.containsKey("pinBarRejectionWickBodyMult")) riskSettings.setPinBarRejectionWickBodyMult(Double.parseDouble(body.get("pinBarRejectionWickBodyMult").toString()));
            if (body.containsKey("pinBarOppositeWickBodyMult"))  riskSettings.setPinBarOppositeWickBodyMult(Double.parseDouble(body.get("pinBarOppositeWickBodyMult").toString()));
            if (body.containsKey("pinBarSmallBodyMaxRangeRatio"))    riskSettings.setPinBarSmallBodyMaxRangeRatio(Double.parseDouble(body.get("pinBarSmallBodyMaxRangeRatio").toString()));
            if (body.containsKey("pinBarDominantWickMinRangeRatio")) riskSettings.setPinBarDominantWickMinRangeRatio(Double.parseDouble(body.get("pinBarDominantWickMinRangeRatio").toString()));
            if (body.containsKey("pinBarOppositeWickMaxRangeRatio")) riskSettings.setPinBarOppositeWickMaxRangeRatio(Double.parseDouble(body.get("pinBarOppositeWickMaxRangeRatio").toString()));
            if (body.containsKey("engulfingMinBodyMultiple"))    riskSettings.setEngulfingMinBodyMultiple(Double.parseDouble(body.get("engulfingMinBodyMultiple").toString()));
            if (body.containsKey("engulfingMinBodyAtrMult"))     riskSettings.setEngulfingMinBodyAtrMult(Double.parseDouble(body.get("engulfingMinBodyAtrMult").toString()));
            if (body.containsKey("engulfingMaxBodyAtrMult"))     riskSettings.setEngulfingMaxBodyAtrMult(Double.parseDouble(body.get("engulfingMaxBodyAtrMult").toString()));
            if (body.containsKey("piercingPrevBodyAtrMult"))     riskSettings.setPiercingPrevBodyAtrMult(Double.parseDouble(body.get("piercingPrevBodyAtrMult").toString()));
            if (body.containsKey("piercingPenetrationPct"))      riskSettings.setPiercingPenetrationPct(Double.parseDouble(body.get("piercingPenetrationPct").toString()));
            if (body.containsKey("tweezerPrevBodyAtrMult"))      riskSettings.setTweezerPrevBodyAtrMult(Double.parseDouble(body.get("tweezerPrevBodyAtrMult").toString()));
            if (body.containsKey("tweezerLowHighMatchAtr"))      riskSettings.setTweezerLowHighMatchAtr(Double.parseDouble(body.get("tweezerLowHighMatchAtr").toString()));
            if (body.containsKey("haramiBodyAtrMult"))           riskSettings.setHaramiBodyAtrMult(Double.parseDouble(body.get("haramiBodyAtrMult").toString()));
            if (body.containsKey("haramiInnerBodyMaxRatio"))     riskSettings.setHaramiInnerBodyMaxRatio(Double.parseDouble(body.get("haramiInnerBodyMaxRatio").toString()));
            if (body.containsKey("dojiBodyMaxRangeRatio"))       riskSettings.setDojiBodyMaxRangeRatio(Double.parseDouble(body.get("dojiBodyMaxRangeRatio").toString()));
            if (body.containsKey("dojiConfirmBodyAtrMult"))      riskSettings.setDojiConfirmBodyAtrMult(Double.parseDouble(body.get("dojiConfirmBodyAtrMult").toString()));
            else if (body.containsKey("dojiPrevBodyAtrMult"))    riskSettings.setDojiConfirmBodyAtrMult(Double.parseDouble(body.get("dojiPrevBodyAtrMult").toString())); // legacy key
            if (body.containsKey("dojiConfirmMaxBodyAtrMult"))   riskSettings.setDojiConfirmMaxBodyAtrMult(Double.parseDouble(body.get("dojiConfirmMaxBodyAtrMult").toString()));
            if (body.containsKey("starOuterBodyAtrMult"))        riskSettings.setStarOuterBodyAtrMult(Double.parseDouble(body.get("starOuterBodyAtrMult").toString()));
            if (body.containsKey("starOuterMaxBodyAtrMult"))     riskSettings.setStarOuterMaxBodyAtrMult(Double.parseDouble(body.get("starOuterMaxBodyAtrMult").toString()));
            if (body.containsKey("starMiddleBodyMaxMultOfOuter")) riskSettings.setStarMiddleBodyMaxMultOfOuter(Double.parseDouble(body.get("starMiddleBodyMaxMultOfOuter").toString()));
            if (body.containsKey("levelTouchToleranceAtr"))      riskSettings.setLevelTouchToleranceAtr(Double.parseDouble(body.get("levelTouchToleranceAtr").toString()));
            if (body.containsKey("enableTrailingSl")) riskSettings.setEnableTrailingSl(effectiveMode, Boolean.parseBoolean(body.get("enableTrailingSl").toString()));
            if (body.containsKey("enablePriceSmaExit")) riskSettings.setEnablePriceSmaExit(Boolean.parseBoolean(body.get("enablePriceSmaExit").toString()));
            if (body.containsKey("virginCprExpiryDays")) {
                try {
                    riskSettings.setVirginCprExpiryDays(Integer.parseInt(body.get("virginCprExpiryDays").toString()));
                } catch (NumberFormatException ignored) { /* leave at current value */ }
            }
            if (body.containsKey("enableVirginCprHurdleFilter")) riskSettings.setEnableVirginCprHurdleFilter(Boolean.parseBoolean(body.get("enableVirginCprHurdleFilter").toString()));
            if (body.containsKey("virginCprHurdleHeadroomAtr")) riskSettings.setVirginCprHurdleHeadroomAtr(Double.parseDouble(body.get("virginCprHurdleHeadroomAtr").toString()));
            if (body.containsKey("breakevenTriggerPct")) riskSettings.setBreakevenTriggerPct(Double.parseDouble(body.get("breakevenTriggerPct").toString()));
            if (body.containsKey("breakevenSlAtrMult"))  riskSettings.setBreakevenSlAtrMult(Double.parseDouble(body.get("breakevenSlAtrMult").toString()));
            if (body.containsKey("skipR3S3IvOvDays")) riskSettings.setSkipR3S3IvOvDays(Boolean.parseBoolean(body.get("skipR3S3IvOvDays").toString()));
            if (body.containsKey("skipR3S3EvDays"))   riskSettings.setSkipR3S3EvDays(Boolean.parseBoolean(body.get("skipR3S3EvDays").toString()));
            if (body.containsKey("skipR4S4IvOvDays")) riskSettings.setSkipR4S4IvOvDays(Boolean.parseBoolean(body.get("skipR4S4IvOvDays").toString()));
            if (body.containsKey("skipR4S4EvDays"))   riskSettings.setSkipR4S4EvDays(Boolean.parseBoolean(body.get("skipR4S4EvDays").toString()));
            if (body.containsKey("skipHtfR3S3NormalDays")) riskSettings.setSkipHtfR3S3NormalDays(Boolean.parseBoolean(body.get("skipHtfR3S3NormalDays").toString()));
            if (body.containsKey("skipHtfR4S4NormalDays")) riskSettings.setSkipHtfR4S4NormalDays(Boolean.parseBoolean(body.get("skipHtfR4S4NormalDays").toString()));
            if (body.containsKey("enableMeanReversionTrades")) riskSettings.setEnableMeanReversionTrades(Boolean.parseBoolean(body.get("enableMeanReversionTrades").toString()));
            if (body.containsKey("enableMagnetTrades"))        riskSettings.setEnableMagnetTrades(Boolean.parseBoolean(body.get("enableMagnetTrades").toString()));
            if (body.containsKey("magnetTradesQtyFactor"))     riskSettings.setMagnetTradesQtyFactor(Double.parseDouble(body.get("magnetTradesQtyFactor").toString()));
            if (body.containsKey("meanReversionQtyFactor"))    riskSettings.setMeanReversionQtyFactor(Double.parseDouble(body.get("meanReversionQtyFactor").toString()));
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
            if (body.containsKey("enableOpeningRefresh")) riskSettings.setEnableOpeningRefresh(Boolean.parseBoolean(body.get("enableOpeningRefresh").toString()));
            if (body.containsKey("openingRefreshTime")) riskSettings.setOpeningRefreshTime(body.get("openingRefreshTime").toString());
            if (body.containsKey("scanOnlyNifty50")) riskSettings.setScanOnlyNifty50(Boolean.parseBoolean(body.get("scanOnlyNifty50").toString()));
            if (body.containsKey("enableHpt")) riskSettings.setEnableHpt(Boolean.parseBoolean(body.get("enableHpt").toString()));
            if (body.containsKey("enableMpt")) riskSettings.setEnableMpt(Boolean.parseBoolean(body.get("enableMpt").toString()));
            if (body.containsKey("mptQtyFactor")) riskSettings.setMptQtyFactor(Double.parseDouble(body.get("mptQtyFactor").toString()));
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
