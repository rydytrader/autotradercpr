package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Portfolio-wide kill switch — fires when the combined live net day P&L across
 * all enabled strategies drops below the configured {@code portfolioMaxDailyLoss}
 * threshold.
 *
 * <p>Multi-strategy aware: iterates every {@link Strategy} bean via Spring's
 * bean-id-keyed {@code Map<String, Strategy>} injection. Only enabled strategies
 * contribute to the aggregate; when the aggregate breaches, every enabled
 * strategy is force-closed independently so no side keeps running past the
 * portfolio cap.
 *
 * <p>Disabled when {@code portfolioMaxDailyLoss} is 0. Fires once per day; the
 * fire-once flag rolls over on a new IST date.
 */
@Service
public class PortfolioRiskService {

    private static final Logger log = LoggerFactory.getLogger(PortfolioRiskService.class);

    /** Bean-id → Strategy. Spring auto-injects every {@code @Service implements
     *  Strategy} bean into this map when the field is declared as
     *  {@code Map<String, Strategy>} — the key is the Spring bean id (typically
     *  the lower-camelCase class name). */
    private final Map<String, Strategy> strategies;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;

    @Autowired @Lazy private MarketHolidayService marketHolidayService;
    @Autowired @Lazy private TelegramService telegramService;

    private volatile String lastFiredDayKey = "";

    public PortfolioRiskService(Map<String, Strategy> strategies,
                                 RiskSettingsStore riskSettings,
                                 EventService eventService) {
        this.strategies = strategies == null ? Map.of() : strategies;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        if (strategies.isEmpty()) return;

        double maxLoss = riskSettings.getPortfolioMaxDailyLoss();
        if (maxLoss <= 0) return;

        String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString();
        if (!today.equals(lastFiredDayKey)) lastFiredDayKey = "";
        if (today.equals(lastFiredDayKey)) return;

        double aggregate = 0;
        boolean anyEnabled = false;
        for (Strategy s : strategies.values()) {
            if (s == null || !s.isEnabled()) continue;
            anyEnabled = true;
            try { aggregate += s.liveNetPnlToday(); }
            catch (Exception e) {
                log.warn("[PortfolioRisk] liveNetPnlToday({}) failed: {}", s.id(), e.getMessage());
            }
        }
        if (!anyEnabled) return;
        if (aggregate >= -maxLoss) return;

        double riskPct = riskSettings.getPortfolioMaxRiskPct();
        String msg = String.format(
            "PORTFOLIO MAX LOSS HIT — net %.2f < -%.2f (%.2f%% of ₹%.0f starting capital). Flattening every enabled strategy.",
            aggregate, maxLoss, riskPct, riskSettings.getStartingCapital());
        log.warn("[PortfolioRisk] {}", msg);
        eventService.log("[WARNING] [portfolio-risk] " + msg);
        try { if (telegramService != null) telegramService.sendMessage("[PortfolioRisk] " + msg); }
        catch (Exception ignored) {}

        for (Strategy s : strategies.values()) {
            if (s == null || !s.isEnabled()) continue;
            try {
                s.forceClose("PORTFOLIO_MAX_LOSS_HIT");
                log.info("[PortfolioRisk] flattened {}", s.id());
            } catch (Exception e) {
                log.error("[PortfolioRisk] forceClose({}) failed: {}", s.id(), e.getMessage());
            }
        }
        lastFiredDayKey = today;
    }
}
