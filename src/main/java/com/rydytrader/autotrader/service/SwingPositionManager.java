package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Manages swing (CNC) position lifecycle:
 * - Morning reconcile at 9:15 AM: gap check, re-place SL/target
 * - Friday squareoff at 3:25 PM
 * - Trailing SL on 75-min candle close (Chandelier Exit)
 */
@Service
public class SwingPositionManager implements SwingCandleAggregator.SwingCandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(SwingPositionManager.class);

    private final PositionStateStore positionStateStore;
    private final OrderService orderService;
    private final MarketDataService marketDataService;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;
    private final TradeHistoryService tradeHistoryService;

    public SwingPositionManager(PositionStateStore positionStateStore,
                                 OrderService orderService,
                                 MarketDataService marketDataService,
                                 RiskSettingsStore riskSettings,
                                 EventService eventService,
                                 TradeHistoryService tradeHistoryService) {
        this.positionStateStore = positionStateStore;
        this.orderService = orderService;
        this.marketDataService = marketDataService;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.tradeHistoryService = tradeHistoryService;
    }

    // ── Morning reconcile at 9:15 AM ─────────────────────────────────────────

    @Scheduled(cron = "0 16 9 * * MON-FRI")
    public void morningReconcile() {
        Map<String, Map<String, Object>> cncPositions = positionStateStore.loadByProductType(PositionStateStore.CNC);
        if (cncPositions.isEmpty()) {
            log.info("[SwingMgr] No CNC positions to reconcile");
            return;
        }

        log.info("[SwingMgr] Morning reconcile: {} CNC positions", cncPositions.size());
        eventService.log("[INFO] Swing morning reconcile: " + cncPositions.size() + " CNC positions");

        for (var entry : cncPositions.entrySet()) {
            String symbol = entry.getKey();
            Map<String, Object> state = entry.getValue();
            try {
                reconcilePosition(symbol, state);
            } catch (Exception e) {
                log.error("[SwingMgr] Reconcile failed for {}: {}", symbol, e.getMessage());
                eventService.log("[ERROR] Swing reconcile failed for " + symbol + ": " + e.getMessage());
            }
        }
    }

    private void reconcilePosition(String symbol, Map<String, Object> state) {
        String side = state.getOrDefault("side", "").toString();
        double savedSl = Double.parseDouble(state.getOrDefault("slPrice", "0").toString());
        double savedTarget = Double.parseDouble(state.getOrDefault("targetPrice", "0").toString());
        double avgPrice = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString());
        int qty = Integer.parseInt(state.getOrDefault("qty", "0").toString());

        double ltp = marketDataService.getLtp(symbol);
        if (ltp <= 0) {
            log.warn("[SwingMgr] No LTP for {} — skipping reconcile", symbol);
            return;
        }

        boolean isLong = "LONG".equals(side);

        // Gap check: if open breaches target → close as profit
        if (savedTarget > 0) {
            if ((isLong && ltp >= savedTarget) || (!isLong && ltp <= savedTarget)) {
                log.info("[SwingMgr] {} gap hit target: LTP={} target={}", symbol, ltp, savedTarget);
                eventService.log("[SWING] " + symbol + " — gap hit target @ " + String.format("%.2f", ltp));
                exitSwingPosition(symbol, qty, side, avgPrice, ltp, "GAP_TARGET");
                return;
            }
        }

        // Gap check: if open breaches SL → close as loss
        if (savedSl > 0) {
            if ((isLong && ltp <= savedSl) || (!isLong && ltp >= savedSl)) {
                log.info("[SwingMgr] {} gap hit SL: LTP={} SL={}", symbol, ltp, savedSl);
                eventService.log("[SWING] " + symbol + " — gap hit SL @ " + String.format("%.2f", ltp));
                exitSwingPosition(symbol, qty, side, avgPrice, ltp, "GAP_SL");
                return;
            }
        }

        // Re-place SL and target as fresh CNC orders
        int exitSide = isLong ? -1 : 1; // opposite side for exit
        boolean skipTarget = riskSettings.isEnableTrailingSl() && riskSettings.isTrailingSlNoTarget();

        if (savedSl > 0) {
            OrderDTO slOrder = orderService.placeStopLoss(symbol, qty, exitSide, savedSl, PositionStateStore.CNC);
            if (slOrder != null && slOrder.getId() != null && !slOrder.getId().isEmpty()) {
                String tgtId = "";
                if (!skipTarget && savedTarget > 0) {
                    OrderDTO tgtOrder = orderService.placeTarget(symbol, qty, exitSide, savedTarget, PositionStateStore.CNC);
                    if (tgtOrder != null && tgtOrder.getId() != null) tgtId = tgtOrder.getId();
                }
                positionStateStore.saveOcoState(symbol, slOrder.getId(), tgtId, savedSl, savedTarget, PositionStateStore.CNC);
                eventService.log("[SWING] Re-placed SL=" + String.format("%.2f", savedSl)
                    + (skipTarget ? " (no target, trailing)" : " Target=" + String.format("%.2f", savedTarget))
                    + " for " + symbol);
            } else {
                eventService.log("[ERROR] Failed to re-place SL for swing position " + symbol);
            }
        }
    }

    private void exitSwingPosition(String symbol, int qty, String side, double avgPrice, double exitPrice, String reason) {
        int exitSide = "LONG".equals(side) ? -1 : 1;
        orderService.placeExitOrder(symbol, qty, exitSide, PositionStateStore.CNC);
        PositionManager.setPosition(symbol, "NONE");
        positionStateStore.clear(symbol, PositionStateStore.CNC);
        tradeHistoryService.record(symbol, side, qty, avgPrice, exitPrice, reason, "SWING_" + reason);
        eventService.log("[SWING] Exited " + symbol + " @ " + String.format("%.2f", exitPrice) + " — " + reason);
    }

    // ── Friday squareoff at 3:25 PM ──────────────────────────────────────────

    @Scheduled(cron = "0 25 15 * * FRI")
    public void fridaySquareoff() {
        Map<String, Map<String, Object>> cncPositions = positionStateStore.loadByProductType(PositionStateStore.CNC);
        if (cncPositions.isEmpty()) {
            log.info("[SwingMgr] No CNC positions to squareoff on Friday");
            return;
        }

        log.info("[SwingMgr] Friday squareoff: {} CNC positions", cncPositions.size());
        eventService.log("[INFO] Friday swing squareoff: " + cncPositions.size() + " CNC positions");

        for (var entry : cncPositions.entrySet()) {
            String symbol = entry.getKey();
            Map<String, Object> state = entry.getValue();
            try {
                String side = state.getOrDefault("side", "").toString();
                double avgPrice = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString());
                int qty = Integer.parseInt(state.getOrDefault("qty", "0").toString());

                // Cancel existing SL/target orders
                String slOrderId = state.getOrDefault("slOrderId", "").toString();
                String tgtOrderId = state.getOrDefault("targetOrderId", "").toString();
                if (!slOrderId.isEmpty()) orderService.cancelOrder(slOrderId);
                if (!tgtOrderId.isEmpty()) orderService.cancelOrder(tgtOrderId);

                double ltp = marketDataService.getLtp(symbol);
                if (ltp <= 0) ltp = avgPrice; // fallback

                exitSwingPosition(symbol, qty, side, avgPrice, ltp, "FRIDAY_SQUAREOFF");
            } catch (Exception e) {
                log.error("[SwingMgr] Friday squareoff failed for {}: {}", symbol, e.getMessage());
            }
        }
    }

    // ── Swing trailing SL on 75-min candle close ─────────────────────────────

    @Override
    public void onSwingCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (!riskSettings.isEnableTrailingSl()) return;

        // Only trail CNC positions
        Map<String, Object> state = positionStateStore.load(fyersSymbol, PositionStateStore.CNC);
        if (state == null) return;

        String slOrderId = state.getOrDefault("slOrderId", "").toString();
        if (slOrderId.isEmpty()) return;

        String side = state.getOrDefault("side", "").toString();
        double currentSlPrice = 0;
        try { currentSlPrice = Double.parseDouble(state.getOrDefault("slPrice", "0").toString()); }
        catch (NumberFormatException ignored) {}

        double newSl = marketDataService.calculateSwingChandelierSl(fyersSymbol, side);
        if (newSl <= 0) return;

        newSl = orderService.roundToTick(newSl, fyersSymbol);

        // Only move SL in favorable direction (tighter)
        if ("LONG".equals(side) && newSl <= currentSlPrice) return;
        if ("SHORT".equals(side) && newSl >= currentSlPrice) return;

        log.info("[SwingTrail] {} {} — new SL={} current={}", fyersSymbol, side,
            String.format("%.2f", newSl), String.format("%.2f", currentSlPrice));

        boolean ok = orderService.modifySlOrder(slOrderId, newSl, fyersSymbol);
        if (ok) {
            String targetOrderId = state.getOrDefault("targetOrderId", "").toString();
            double currentTarget = Double.parseDouble(state.getOrDefault("targetPrice", "0").toString());
            positionStateStore.saveOcoState(fyersSymbol, slOrderId, targetOrderId, newSl, currentTarget, PositionStateStore.CNC);
            positionStateStore.appendDescription(fyersSymbol,
                "[SWING_TRAIL] SL → " + String.format("%.2f", newSl), PositionStateStore.CNC);
            eventService.log("[SWING] Chandelier SL for " + fyersSymbol + ": " + String.format("%.2f", currentSlPrice) + " → " + String.format("%.2f", newSl));
        }
    }
}
