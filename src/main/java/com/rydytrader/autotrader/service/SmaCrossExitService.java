package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Defensive SMA-cross exit. At every 5-min candle close, if the just-closed bar's SMA 20
 * has stacked against the open trade direction (LONG: SMA 20 < SMA 50; SHORT: SMA 20 > SMA 50),
 * the bot squareoffs the position before SL hits.
 *
 * Stateless evaluation — no "did a cross just happen this bar" tracking. At every boundary
 * it simply asks "is the SMA stack currently against the trade?". If yes -> exit. Robust to
 * restarts, LPT entries where SMAs were already crossed at entry, and crosses that happened
 * multiple bars ago without the bot acting.
 *
 * SMA values are read from {@link CandleAggregator.CandleBar#sma20} / {@code sma50} which are
 * the post-close completed-only snapshots populated by SmaService.onCandleClose before this
 * listener fires. No live blending.
 *
 * Gated by {@link RiskSettingsStore#isEnableSmaCrossExit()} (default off).
 */
@Service
public class SmaCrossExitService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(SmaCrossExitService.class);

    private final RiskSettingsStore riskSettings;
    private final PositionStateStore positionStateStore;
    private final EventService eventService;
    private final PollingService pollingService;

    public SmaCrossExitService(RiskSettingsStore riskSettings,
                               PositionStateStore positionStateStore,
                               EventService eventService,
                               @Lazy @Autowired PollingService pollingService) {
        this.riskSettings = riskSettings;
        this.positionStateStore = positionStateStore;
        this.eventService = eventService;
        this.pollingService = pollingService;
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar candle) {
        if (!riskSettings.isEnableSmaCrossExit()) return;
        if (candle == null) return;

        String position = PositionManager.getPosition(fyersSymbol);
        if (!"LONG".equals(position) && !"SHORT".equals(position)) return;

        double sma20 = candle.sma20;
        double sma50 = candle.sma50;
        if (sma20 <= 0 || sma50 <= 0) return; // not seeded yet — fail-open

        boolean exitLong  = "LONG".equals(position)  && sma20 < sma50;
        boolean exitShort = "SHORT".equals(position) && sma20 > sma50;
        if (!exitLong && !exitShort) return;

        Map<String, Object> state = positionStateStore.load(fyersSymbol);
        if (state == null || state.get("qty") == null) return;
        int qty;
        try {
            qty = Integer.parseInt(state.get("qty").toString());
        } catch (NumberFormatException e) {
            log.warn("[SmaCrossExit] {} qty parse failed: {}", fyersSymbol, state.get("qty"));
            return;
        }
        if (qty <= 0) return;

        eventService.log("[INFO] " + fyersSymbol + " SMA cross exit triggered — "
            + position + " position, 5m SMA 20=" + String.format("%.2f", sma20)
            + " " + (exitLong ? "<" : ">") + " SMA 50=" + String.format("%.2f", sma50));
        pollingService.squareOff(fyersSymbol, qty, "SMA_CROSS_EXIT");
    }
}
