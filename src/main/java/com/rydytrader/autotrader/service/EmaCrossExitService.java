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
 * Defensive exit — single check. Runs on every 5-min candle close after EmaService has
 * populated {@code candle.ema20}.
 *
 * <p><b>Stock price-vs-EMA exit</b> ({@link RiskSettingsStore#isEnablePriceEmaExit()}, default off):
 * Stock's just-closed candle on the wrong side of its 5-min EMA 20 → exit.
 * LONG: close &lt; EMA 20. SHORT: close &gt; EMA 20.
 *
 * <p>Stateless evaluation — no "did a cross just happen this bar" tracking. At every boundary
 * the check simply asks "is the trade currently structurally wrong?". If yes -> exit.
 *
 * <p>Stock-side EMA values come from {@link CandleAggregator.CandleBar#ema20} (post-close
 * completed-only snapshots stamped by EmaService.onCandleClose before this listener fires).
 */
@Service
public class EmaCrossExitService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(EmaCrossExitService.class);

    private final RiskSettingsStore riskSettings;
    private final PositionStateStore positionStateStore;
    private final EventService eventService;
    private final PollingService pollingService;
    @Autowired @Lazy private MarketDataService marketDataService;
    @Autowired @Lazy private BhavcopyService bhavcopyService;
    @Autowired @Lazy private IndexTrendService indexTrendService;
    @Autowired @Lazy private VirginCprService virginCprService;
    @Autowired @Lazy private CandleAggregator candleAggregator;

    public EmaCrossExitService(RiskSettingsStore riskSettings,
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
        if (candle == null) return;
        // Skip the NIFTY index itself — NIFTY isn't tradable, no position can exist on it.
        if (IndexTrendService.NIFTY_SYMBOL.equals(fyersSymbol)) return;

        String position = PositionManager.getPosition(fyersSymbol);
        if (!"LONG".equals(position) && !"SHORT".equals(position)) return;

        double ema20 = candle.ema20;
        double close = candle.close;
        if (ema20 <= 0) return; // not seeded yet — fail-open

        // ── 1. Stock price-vs-EMA exit: close on the wrong side of the 5-min EMA 20 ──
        if (riskSettings.isEnablePriceEmaExit() && close > 0) {
            boolean exitLong  = "LONG".equals(position)  && close < ema20;
            boolean exitShort = "SHORT".equals(position) && close > ema20;
            if (exitLong || exitShort) {
                int qty = readQty(fyersSymbol);
                if (qty <= 0) return;
                eventService.log("[INFO] " + fyersSymbol + " Price-EMA exit triggered — "
                    + position + " position, close=" + String.format("%.2f", close)
                    + " " + (exitLong ? "<" : ">") + " 5m EMA 20=" + String.format("%.2f", ema20));
                pollingService.squareOff(fyersSymbol, qty, "PRICE_EMA_EXIT");
                return;
            }
        }

    }

    private int readQty(String fyersSymbol) {
        Map<String, Object> state = positionStateStore.load(fyersSymbol);
        if (state == null || state.get("qty") == null) return 0;
        try {
            return Integer.parseInt(state.get("qty").toString());
        } catch (NumberFormatException e) {
            log.warn("[EmaCrossExit] {} qty parse failed: {}", fyersSymbol, state.get("qty"));
            return 0;
        }
    }
}
