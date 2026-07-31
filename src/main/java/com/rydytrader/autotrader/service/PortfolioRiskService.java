package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Portfolio-wide kill switch — fires when the active strategy's live net day P&L drops
 * below the configured {@code portfolioMaxRiskPct} of {@code startingCapital}.
 *
 * <p>Since OptionSelling is the only strategy, the "portfolio" here is just the singleton's
 * P&L. Kept as a separate service so the threshold can fire even on strategies we may add
 * later, and so the manual-terminal book stays independent.
 *
 * <p>Disabled when {@code portfolioMaxRiskPct} is 0. Fires once per day; the fire-once
 * flag rolls over on a new IST date.
 */
@Service
public class PortfolioRiskService {

    private static final Logger log = LoggerFactory.getLogger(PortfolioRiskService.class);

    private final ObjectProvider<Strategy> strategyProvider;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;

    @Autowired @Lazy private MarketHolidayService marketHolidayService;
    @Autowired @Lazy private TelegramService telegramService;

    private volatile String lastFiredDayKey = "";

    public PortfolioRiskService(ObjectProvider<Strategy> strategyProvider,
                                 RiskSettingsStore riskSettings,
                                 EventService eventService) {
        this.strategyProvider = strategyProvider;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;

        Strategy strategy = strategyProvider == null ? null : strategyProvider.getIfAvailable();
        if (strategy == null || !strategy.isEnabled()) return;

        double maxLoss = riskSettings.getPortfolioMaxDailyLoss();
        if (maxLoss <= 0) return;

        String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString();
        if (!today.equals(lastFiredDayKey)) lastFiredDayKey = "";
        if (today.equals(lastFiredDayKey)) return;

        double aggregate;
        try { aggregate = strategy.liveNetPnlToday(); }
        catch (Exception e) {
            log.warn("[PortfolioRisk] liveNetPnlToday failed: {}", e.getMessage());
            return;
        }
        if (aggregate >= -maxLoss) return;

        double riskPct = riskSettings.getPortfolioMaxRiskPct();
        String msg = String.format(
            "PORTFOLIO MAX LOSS HIT — net %.2f < -%.2f (%.2f%% of ₹%.0f starting capital). Flattening.",
            aggregate, maxLoss, riskPct, riskSettings.getStartingCapital());
        log.warn("[PortfolioRisk] {}", msg);
        eventService.log("[WARNING] [portfolio-risk] " + msg);
        try { if (telegramService != null) telegramService.sendMessage("[PortfolioRisk] " + msg); }
        catch (Exception ignored) {}

        try {
            strategy.forceClose("PORTFOLIO_MAX_LOSS_HIT");
            log.info("[PortfolioRisk] flattened {}", strategy.id());
        } catch (Exception e) {
            log.error("[PortfolioRisk] forceClose({}) failed: {}", strategy.id(), e.getMessage());
        }
        lastFiredDayKey = today;
    }
}
