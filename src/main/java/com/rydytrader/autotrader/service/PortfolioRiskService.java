package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.service.strategy.StrategyRegistry;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Portfolio-wide kill switch — fires when the aggregate live net day P&L across all registered
 * strategies drops below the configured {@code portfolioMaxRiskPct} of {@code startingCapital}.
 *
 * <p>Independent of each strategy's own max-loss kill switch. The per-strategy switch fires when
 * a single strategy loses too much; this one fires when the COMBINED loss across the book is
 * too much. When triggered, every strategy with an open position is force-closed and parked
 * DONE_FOR_DAY for the rest of the session.
 *
 * <p>Disabled when {@code portfolioMaxRiskPct} is 0. Triggers once per day — after firing, the
 * fire-once flag stays set until day rollover (resolved by re-checking the day key).
 */
@Service
public class PortfolioRiskService {

    private static final Logger log = LoggerFactory.getLogger(PortfolioRiskService.class);

    private final StrategyRegistry strategyRegistry;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;

    @Autowired @Lazy private MarketHolidayService marketHolidayService;
    @Autowired @Lazy private TelegramService telegramService;

    /** ISO date string of the last day we fired the kill switch — prevents repeated trigger
     *  spam if the operator manually opens a new straddle after a portfolio-kill. Reset by
     *  comparing against today's date each tick. */
    private volatile String lastFiredDayKey = "";

    public PortfolioRiskService(StrategyRegistry strategyRegistry,
                                 RiskSettingsStore riskSettings,
                                 EventService eventService) {
        this.strategyRegistry = strategyRegistry;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
    }

    @Scheduled(fixedDelay = 5000)
    public void tick() {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        if (strategyRegistry == null || strategyRegistry.isEmpty()) return;

        double maxLoss = riskSettings.getPortfolioMaxDailyLoss(); // computed from pct × startingCapital
        if (maxLoss <= 0) return; // disabled

        // Reset the fire-once flag on a new day.
        String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString();
        if (!today.equals(lastFiredDayKey)) lastFiredDayKey = "";

        if (today.equals(lastFiredDayKey)) return; // already fired today

        double aggregate = 0;
        for (Strategy s : strategyRegistry.all()) {
            try { aggregate += s.liveNetPnlToday(); }
            catch (Exception e) {
                log.warn("[PortfolioRisk] {} liveNetPnlToday failed: {}", s.id(), e.getMessage());
            }
        }

        if (aggregate >= -maxLoss) return; // still within budget

        double riskPct = riskSettings.getPortfolioMaxRiskPct();
        String msg = String.format(
            "PORTFOLIO MAX LOSS HIT — aggregate %.2f < -%.2f (%.2f%% of ₹%.0f starting capital). " +
            "Flattening every strategy and parking DONE_FOR_DAY.",
            aggregate, maxLoss, riskPct, riskSettings.getStartingCapital());
        log.warn("[PortfolioRisk] {}", msg);
        eventService.log("[ERROR] " + msg);
        try { if (telegramService != null) telegramService.sendMessage("[PortfolioRisk] " + msg); }
        catch (Exception ignored) {}

        for (Strategy s : strategyRegistry.all()) {
            try {
                boolean closed = s.forceClose("PORTFOLIO_MAX_LOSS_HIT");
                log.info("[PortfolioRisk] forceClose({}): {}", s.id(), closed ? "flattened" : "nothing to close");
            } catch (Exception e) {
                log.error("[PortfolioRisk] forceClose({}) failed: {}", s.id(), e.getMessage());
            }
        }
        lastFiredDayKey = today;
    }
}
