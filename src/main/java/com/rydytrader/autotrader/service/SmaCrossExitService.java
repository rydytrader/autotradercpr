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
 * Defensive SMA-based exits — four independent checks, each toggled separately. All run on
 * every 5-min candle close, after SmaService has populated {@code candle.sma20} / {@code sma50}.
 *
 * <ol>
 *   <li><b>Stock SMA-cross exit</b> ({@link RiskSettingsStore#isEnableSmaCrossExit()}, default off):
 *       Stock's 5-min SMA stack against trade direction → exit.
 *       LONG: SMA 20 &lt; SMA 50. SHORT: SMA 20 &gt; SMA 50.</li>
 *   <li><b>Stock price-vs-SMA exit</b> ({@link RiskSettingsStore#isEnablePriceSmaExit()}, default off):
 *       Stock's just-closed candle on the wrong side of its 5-min SMA 50 → exit.
 *       LONG: close &lt; SMA 50. SHORT: close &gt; SMA 50.</li>
 *   <li><b>NIFTY SMA-cross exit</b> ({@link RiskSettingsStore#isEnableNiftySmaCrossExit()}, default off):
 *       NIFTY's 5-min SMA stack against trade direction → exit. Macro safety net.</li>
 *   <li><b>NIFTY price-vs-SMA exit</b> ({@link RiskSettingsStore#isEnableNiftyPriceSmaExit()}, default off):
 *       NIFTY's live LTP on the wrong side of NIFTY's 5-min SMA 50 → exit.</li>
 * </ol>
 *
 * <p>Stateless evaluation — no "did a cross just happen this bar" tracking. At every boundary
 * each enabled check simply asks "is the trade currently structurally wrong?". If yes -> exit.
 *
 * <p>Stock-side SMA values come from {@link CandleAggregator.CandleBar#sma20} / {@code sma50}
 * (post-close completed-only snapshots stamped by SmaService.onCandleClose before this listener
 * fires). NIFTY-side values come from {@code SmaService.getSma(NIFTY_SYMBOL)} (live-blended)
 * and {@code MarketDataService.getLtp(NIFTY_SYMBOL)}; both reflect NIFTY's current state at
 * the moment the stock's 5-min candle closes.
 *
 * <p>Checks run in order — first match wins. If multiple fire on the same bar, only the
 * first-evaluated reason is recorded.
 */
@Service
public class SmaCrossExitService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(SmaCrossExitService.class);

    private final RiskSettingsStore riskSettings;
    private final PositionStateStore positionStateStore;
    private final EventService eventService;
    private final PollingService pollingService;
    @Autowired @Lazy private SmaService smaService;
    @Autowired @Lazy private MarketDataService marketDataService;

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
        if (candle == null) return;
        // Skip the NIFTY index itself — NIFTY isn't tradable, no position can exist on it.
        if (IndexTrendService.NIFTY_SYMBOL.equals(fyersSymbol)) return;

        String position = PositionManager.getPosition(fyersSymbol);
        if (!"LONG".equals(position) && !"SHORT".equals(position)) return;

        double sma20 = candle.sma20;
        double sma50 = candle.sma50;
        double close = candle.close;
        if (sma20 <= 0) return; // not seeded yet — fail-open

        // ── 1. Stock SMA-cross exit: SMA 20 vs SMA 50 stack against the trade ──
        if (riskSettings.isEnableSmaCrossExit() && sma50 > 0) {
            boolean exitLong  = "LONG".equals(position)  && sma20 < sma50;
            boolean exitShort = "SHORT".equals(position) && sma20 > sma50;
            if (exitLong || exitShort) {
                int qty = readQty(fyersSymbol);
                if (qty <= 0) return;
                eventService.log("[INFO] " + fyersSymbol + " SMA cross exit triggered — "
                    + position + " position, 5m SMA 20=" + String.format("%.2f", sma20)
                    + " " + (exitLong ? "<" : ">") + " SMA 50=" + String.format("%.2f", sma50));
                pollingService.squareOff(fyersSymbol, qty, "SMA_CROSS_EXIT");
                return;
            }
        }

        // ── 2. Stock price-vs-SMA exit: close on the wrong side of the 5-min SMA 50 ──
        if (riskSettings.isEnablePriceSmaExit() && close > 0 && sma50 > 0) {
            boolean exitLong  = "LONG".equals(position)  && close < sma50;
            boolean exitShort = "SHORT".equals(position) && close > sma50;
            if (exitLong || exitShort) {
                int qty = readQty(fyersSymbol);
                if (qty <= 0) return;
                eventService.log("[INFO] " + fyersSymbol + " Price-SMA exit triggered — "
                    + position + " position, close=" + String.format("%.2f", close)
                    + " " + (exitLong ? "<" : ">") + " 5m SMA 50=" + String.format("%.2f", sma50));
                pollingService.squareOff(fyersSymbol, qty, "PRICE_SMA_EXIT");
                return;
            }
        }

        // ── 3. NIFTY SMA-cross exit: macro safety net using NIFTY's 5-min SMA stack ──
        if (riskSettings.isEnableNiftySmaCrossExit() && smaService != null) {
            double nSma20 = smaService.getSma(IndexTrendService.NIFTY_SYMBOL);
            double nSma50 = smaService.getSma50(IndexTrendService.NIFTY_SYMBOL);
            if (nSma20 > 0 && nSma50 > 0) {
                boolean exitLong  = "LONG".equals(position)  && nSma20 < nSma50;
                boolean exitShort = "SHORT".equals(position) && nSma20 > nSma50;
                if (exitLong || exitShort) {
                    int qty = readQty(fyersSymbol);
                    if (qty <= 0) return;
                    eventService.log("[INFO] " + fyersSymbol + " NIFTY SMA cross exit triggered — "
                        + position + " position, NIFTY 5m SMA 20=" + String.format("%.2f", nSma20)
                        + " " + (exitLong ? "<" : ">") + " SMA 50=" + String.format("%.2f", nSma50));
                    pollingService.squareOff(fyersSymbol, qty, "NIFTY_SMA_CROSS_EXIT");
                    return;
                }
            }
        }

        // ── 4. NIFTY price-vs-SMA exit: NIFTY LTP on the wrong side of NIFTY's 5-min SMA 50 ──
        if (riskSettings.isEnableNiftyPriceSmaExit() && smaService != null && marketDataService != null) {
            double nSma50 = smaService.getSma50(IndexTrendService.NIFTY_SYMBOL);
            double nLtp = marketDataService.getLtp(IndexTrendService.NIFTY_SYMBOL);
            if (nSma50 > 0 && nLtp > 0) {
                boolean exitLong  = "LONG".equals(position)  && nLtp < nSma50;
                boolean exitShort = "SHORT".equals(position) && nLtp > nSma50;
                if (exitLong || exitShort) {
                    int qty = readQty(fyersSymbol);
                    if (qty <= 0) return;
                    eventService.log("[INFO] " + fyersSymbol + " NIFTY Price-SMA exit triggered — "
                        + position + " position, NIFTY=" + String.format("%.2f", nLtp)
                        + " " + (exitLong ? "<" : ">") + " NIFTY 5m SMA 50=" + String.format("%.2f", nSma50));
                    pollingService.squareOff(fyersSymbol, qty, "NIFTY_PRICE_SMA_EXIT");
                }
            }
        }
    }

    private int readQty(String fyersSymbol) {
        Map<String, Object> state = positionStateStore.load(fyersSymbol);
        if (state == null || state.get("qty") == null) return 0;
        try {
            return Integer.parseInt(state.get("qty").toString());
        } catch (NumberFormatException e) {
            log.warn("[SmaCrossExit] {} qty parse failed: {}", fyersSymbol, state.get("qty"));
            return 0;
        }
    }
}
