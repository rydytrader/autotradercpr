package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.service.BhavcopyService;
import com.rydytrader.autotrader.service.IndexTrendService;
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
    private final IndexTrendService   indexTrendService;

    public SettingsController(RiskSettingsStore riskSettings,
                               TradeHistoryService tradeHistoryService,
                               BhavcopyService bhavcopyService,
                               IndexTrendService indexTrendService) {
        this.riskSettings        = riskSettings;
        this.tradeHistoryService = tradeHistoryService;
        this.bhavcopyService     = bhavcopyService;
        this.indexTrendService   = indexTrendService;
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
        result.put("enableEmaTrendCheck", riskSettings.isEnableEmaTrendCheck());
        result.put("enableEmaVsAtpCheck", riskSettings.isEnableEmaVsAtpCheck());
        result.put("enableEmaLevelCountFilter", riskSettings.isEnableEmaLevelCountFilter());
        result.put("emaLevelMinRangePct", riskSettings.getEmaLevelMinRangePct());
        result.put("emaLevelFilterMorningSkip", riskSettings.isEmaLevelFilterMorningSkip());
        result.put("emaLevelFilterMorningSkipUntil", riskSettings.getEmaLevelFilterMorningSkipUntil());
        result.put("enableTargetShift", riskSettings.isEnableTargetShift(effectiveMode));
        result.put("enableTargetTolerance", riskSettings.isEnableTargetTolerance());
        result.put("targetToleranceAtr", riskSettings.getTargetToleranceAtr());
        result.put("enableIndexAlignment", riskSettings.isEnableIndexAlignment());
        result.put("enableNiftyEma20Factor", riskSettings.isEnableNiftyEma20Factor());
        result.put("enableNiftyFutVwapFactor", riskSettings.isEnableNiftyFutVwapFactor());
        result.put("goodSizeCandleBodyAtrMult",          riskSettings.getGoodSizeCandleBodyAtrMult());
        result.put("goodSizeCandleMaxBodyAtrMult",       riskSettings.getGoodSizeCandleMaxBodyAtrMult());
        result.put("goodSizeCandleMaxOppositeWickRatio", riskSettings.getGoodSizeCandleMaxOppositeWickRatio());
        result.put("pinBarRejectionWickBodyMult",  riskSettings.getPinBarRejectionWickBodyMult());
        result.put("pinBarOppositeWickBodyMult",   riskSettings.getPinBarOppositeWickBodyMult());
        result.put("pinBarSmallBodyMaxRangeRatio",    riskSettings.getPinBarSmallBodyMaxRangeRatio());
        result.put("pinBarDominantWickMinRangeRatio", riskSettings.getPinBarDominantWickMinRangeRatio());
        result.put("pinBarOppositeWickMaxRangeRatio", riskSettings.getPinBarOppositeWickMaxRangeRatio());
        result.put("outsideReversalMinBodyAtrMult", riskSettings.getOutsideReversalMinBodyAtrMult());
        result.put("outsideReversalMaxBodyAtrMult", riskSettings.getOutsideReversalMaxBodyAtrMult());
        result.put("outsideReversalPenetrationPct", riskSettings.getOutsideReversalPenetrationPct());
        result.put("haramiBodyAtrMult",            riskSettings.getHaramiBodyAtrMult());
        result.put("haramiBodyMaxAtrMult",         riskSettings.getHaramiBodyMaxAtrMult());
        result.put("haramiBar3PenetrationPct",     riskSettings.getHaramiBar3PenetrationPct());
        result.put("dojiBodyMaxRangeRatio",        riskSettings.getDojiBodyMaxRangeRatio());
        result.put("dojiConfirmBodyAtrMult",       riskSettings.getDojiConfirmBodyAtrMult());
        result.put("dojiConfirmMaxBodyAtrMult",    riskSettings.getDojiConfirmMaxBodyAtrMult());
        result.put("starOuterBodyAtrMult",         riskSettings.getStarOuterBodyAtrMult());
        result.put("starOuterMaxBodyAtrMult",      riskSettings.getStarOuterMaxBodyAtrMult());
        result.put("starMiddleBodyMaxMultOfOuter", riskSettings.getStarMiddleBodyMaxMultOfOuter());
        result.put("starBar3PenetrationPct",       riskSettings.getStarBar3PenetrationPct());
        result.put("levelTouchToleranceAtr",       riskSettings.getLevelTouchToleranceAtr());
        result.put("enableTrailingSl", riskSettings.isEnableTrailingSl(effectiveMode));
        result.put("enablePriceEmaExit", riskSettings.isEnablePriceEmaExit());
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
        result.put("narrowCprZoneCollapseWidthPct", riskSettings.getNarrowCprZoneCollapseWidthPct());
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
            if (body.containsKey("enableEmaTrendCheck")) riskSettings.setEnableEmaTrendCheck(Boolean.parseBoolean(body.get("enableEmaTrendCheck").toString()));
            if (body.containsKey("enableEmaVsAtpCheck")) riskSettings.setEnableEmaVsAtpCheck(Boolean.parseBoolean(body.get("enableEmaVsAtpCheck").toString()));
            if (body.containsKey("enableEmaLevelCountFilter")) riskSettings.setEnableEmaLevelCountFilter(Boolean.parseBoolean(body.get("enableEmaLevelCountFilter").toString()));
            if (body.containsKey("emaLevelMinRangePct")) riskSettings.setEmaLevelMinRangePct(Integer.parseInt(body.get("emaLevelMinRangePct").toString()));
            if (body.containsKey("emaLevelFilterMorningSkip")) riskSettings.setEmaLevelFilterMorningSkip(Boolean.parseBoolean(body.get("emaLevelFilterMorningSkip").toString()));
            if (body.containsKey("emaLevelFilterMorningSkipUntil")) riskSettings.setEmaLevelFilterMorningSkipUntil(body.get("emaLevelFilterMorningSkipUntil").toString());
            if (body.containsKey("enableTargetShift")) riskSettings.setEnableTargetShift(effectiveMode, Boolean.parseBoolean(body.get("enableTargetShift").toString()));
            if (body.containsKey("enableTargetTolerance")) riskSettings.setEnableTargetTolerance(Boolean.parseBoolean(body.get("enableTargetTolerance").toString()));
            if (body.containsKey("targetToleranceAtr")) riskSettings.setTargetToleranceAtr(Double.parseDouble(body.get("targetToleranceAtr").toString()));
            if (body.containsKey("enableIndexAlignment"))   riskSettings.setEnableIndexAlignment(Boolean.parseBoolean(body.get("enableIndexAlignment").toString()));
            if (body.containsKey("enableNiftyEma20Factor")) riskSettings.setEnableNiftyEma20Factor(Boolean.parseBoolean(body.get("enableNiftyEma20Factor").toString()));
            if (body.containsKey("enableNiftyFutVwapFactor")) riskSettings.setEnableNiftyFutVwapFactor(Boolean.parseBoolean(body.get("enableNiftyFutVwapFactor").toString()));
            if (body.containsKey("goodSizeCandleBodyAtrMult"))   riskSettings.setGoodSizeCandleBodyAtrMult(Double.parseDouble(body.get("goodSizeCandleBodyAtrMult").toString()));
            if (body.containsKey("goodSizeCandleMaxBodyAtrMult")) riskSettings.setGoodSizeCandleMaxBodyAtrMult(Double.parseDouble(body.get("goodSizeCandleMaxBodyAtrMult").toString()));
            if (body.containsKey("goodSizeCandleMaxOppositeWickRatio")) riskSettings.setGoodSizeCandleMaxOppositeWickRatio(Double.parseDouble(body.get("goodSizeCandleMaxOppositeWickRatio").toString()));
            if (body.containsKey("pinBarRejectionWickBodyMult")) riskSettings.setPinBarRejectionWickBodyMult(Double.parseDouble(body.get("pinBarRejectionWickBodyMult").toString()));
            if (body.containsKey("pinBarOppositeWickBodyMult"))  riskSettings.setPinBarOppositeWickBodyMult(Double.parseDouble(body.get("pinBarOppositeWickBodyMult").toString()));
            if (body.containsKey("pinBarSmallBodyMaxRangeRatio"))    riskSettings.setPinBarSmallBodyMaxRangeRatio(Double.parseDouble(body.get("pinBarSmallBodyMaxRangeRatio").toString()));
            if (body.containsKey("pinBarDominantWickMinRangeRatio")) riskSettings.setPinBarDominantWickMinRangeRatio(Double.parseDouble(body.get("pinBarDominantWickMinRangeRatio").toString()));
            if (body.containsKey("pinBarOppositeWickMaxRangeRatio")) riskSettings.setPinBarOppositeWickMaxRangeRatio(Double.parseDouble(body.get("pinBarOppositeWickMaxRangeRatio").toString()));
            if (body.containsKey("outsideReversalMinBodyAtrMult")) riskSettings.setOutsideReversalMinBodyAtrMult(Double.parseDouble(body.get("outsideReversalMinBodyAtrMult").toString()));
            if (body.containsKey("outsideReversalMaxBodyAtrMult")) riskSettings.setOutsideReversalMaxBodyAtrMult(Double.parseDouble(body.get("outsideReversalMaxBodyAtrMult").toString()));
            if (body.containsKey("outsideReversalPenetrationPct")) riskSettings.setOutsideReversalPenetrationPct(Double.parseDouble(body.get("outsideReversalPenetrationPct").toString()));
            if (body.containsKey("haramiBodyAtrMult"))           riskSettings.setHaramiBodyAtrMult(Double.parseDouble(body.get("haramiBodyAtrMult").toString()));
            if (body.containsKey("haramiBodyMaxAtrMult"))        riskSettings.setHaramiBodyMaxAtrMult(Double.parseDouble(body.get("haramiBodyMaxAtrMult").toString()));
            if (body.containsKey("haramiBar3PenetrationPct"))    riskSettings.setHaramiBar3PenetrationPct(Double.parseDouble(body.get("haramiBar3PenetrationPct").toString()));
            if (body.containsKey("dojiBodyMaxRangeRatio"))       riskSettings.setDojiBodyMaxRangeRatio(Double.parseDouble(body.get("dojiBodyMaxRangeRatio").toString()));
            if (body.containsKey("dojiConfirmBodyAtrMult"))      riskSettings.setDojiConfirmBodyAtrMult(Double.parseDouble(body.get("dojiConfirmBodyAtrMult").toString()));
            else if (body.containsKey("dojiPrevBodyAtrMult"))    riskSettings.setDojiConfirmBodyAtrMult(Double.parseDouble(body.get("dojiPrevBodyAtrMult").toString())); // legacy key
            if (body.containsKey("dojiConfirmMaxBodyAtrMult"))   riskSettings.setDojiConfirmMaxBodyAtrMult(Double.parseDouble(body.get("dojiConfirmMaxBodyAtrMult").toString()));
            if (body.containsKey("starOuterBodyAtrMult"))        riskSettings.setStarOuterBodyAtrMult(Double.parseDouble(body.get("starOuterBodyAtrMult").toString()));
            if (body.containsKey("starOuterMaxBodyAtrMult"))     riskSettings.setStarOuterMaxBodyAtrMult(Double.parseDouble(body.get("starOuterMaxBodyAtrMult").toString()));
            if (body.containsKey("starMiddleBodyMaxMultOfOuter")) riskSettings.setStarMiddleBodyMaxMultOfOuter(Double.parseDouble(body.get("starMiddleBodyMaxMultOfOuter").toString()));
            if (body.containsKey("starBar3PenetrationPct"))       riskSettings.setStarBar3PenetrationPct(Double.parseDouble(body.get("starBar3PenetrationPct").toString()));
            if (body.containsKey("levelTouchToleranceAtr"))      riskSettings.setLevelTouchToleranceAtr(Double.parseDouble(body.get("levelTouchToleranceAtr").toString()));
            if (body.containsKey("enableTrailingSl")) riskSettings.setEnableTrailingSl(effectiveMode, Boolean.parseBoolean(body.get("enableTrailingSl").toString()));
            if (body.containsKey("enablePriceEmaExit")) riskSettings.setEnablePriceEmaExit(Boolean.parseBoolean(body.get("enablePriceEmaExit").toString()));
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
            if (body.containsKey("narrowCprZoneCollapseWidthPct")) riskSettings.setNarrowCprZoneCollapseWidthPct(Double.parseDouble(body.get("narrowCprZoneCollapseWidthPct").toString()));
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
            // If a trend-factor toggle changed, refresh the sticky NIFTY trend state so the
            // UI reflects the new setting immediately instead of waiting for the next NIFTY
            // 5-min candle close.
            if (body.containsKey("enableNiftyEma20Factor") || body.containsKey("enableNiftyFutVwapFactor")) {
                try { indexTrendService.recomputeStates(); } catch (Exception ignored) {}
            }
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
