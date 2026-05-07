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
 * Defensive exits — three independent checks, each toggled separately. All run on every 5-min
 * candle close after SmaService has populated {@code candle.sma20} / {@code sma50}.
 *
 * <ol>
 *   <li><b>Stock SMA-cross exit</b> ({@link RiskSettingsStore#isEnableSmaCrossExit()}, default off):
 *       Stock's 5-min SMA stack against trade direction → exit.
 *       LONG: SMA 20 &lt; SMA 50. SHORT: SMA 20 &gt; SMA 50.</li>
 *   <li><b>Stock price-vs-SMA exit</b> ({@link RiskSettingsStore#isEnablePriceSmaExit()}, default off):
 *       Stock's just-closed candle on the wrong side of its 5-min SMA 50 → exit.
 *       LONG: close &lt; SMA 50. SHORT: close &gt; SMA 50.</li>
 *   <li><b>NIFTY reversal CPR-touch exit</b>
 *       ({@link RiskSettingsStore#isEnableNiftyReversalCprExit()}, default off):
 *       When NIFTY is in BULLISH_REVERSAL (NIFTY below CPR + bullish SMAs), the bullish move
 *       has played out once NIFTY climbs back to/past the CPR bottom — exit all open LONG
 *       positions before NIFTY consolidates inside CPR. Mirror for BEARISH_REVERSAL: NIFTY
 *       above CPR drops to/below CPR top → exit all SHORT positions.</li>
 * </ol>
 *
 * <p>Stateless evaluation — no "did a cross just happen this bar" tracking. At every boundary
 * each enabled check simply asks "is the trade currently structurally wrong?". If yes -> exit.
 *
 * <p>Stock-side SMA values come from {@link CandleAggregator.CandleBar#sma20} / {@code sma50}
 * (post-close completed-only snapshots stamped by SmaService.onCandleClose before this listener
 * fires). NIFTY state + CPR levels come from {@link IndexTrendService#getStickyState()} and
 * {@link BhavcopyService#getCprLevels(String) bhavcopyService.getCprLevels("NIFTY50")}.
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
    @Autowired @Lazy private MarketDataService marketDataService;
    @Autowired @Lazy private BhavcopyService bhavcopyService;
    @Autowired @Lazy private IndexTrendService indexTrendService;

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

        // ── 3. NIFTY reversal CPR-touch exit ──
        // BULLISH_REVERSAL: NIFTY currently below CPR (cprBullish=false) but SMAs bullish.
        // We took LONG positions expecting NIFTY to keep rallying. If NIFTY climbs back to /
        // past the CPR bottom, the bullish move has played out — exit before NIFTY consolidates.
        // Mirror for BEARISH_REVERSAL.
        if (riskSettings.isEnableNiftyReversalCprExit()
            && indexTrendService != null && bhavcopyService != null && marketDataService != null) {
            String niftyState = indexTrendService.getStickyState();
            boolean inBullishReversal = "BULLISH_REVERSAL".equals(niftyState);
            boolean inBearishReversal = "BEARISH_REVERSAL".equals(niftyState);
            if (inBullishReversal || inBearishReversal) {
                var cpr = bhavcopyService.getCprLevels("NIFTY50");
                double niftyLtp = marketDataService.getLtp(IndexTrendService.NIFTY_SYMBOL);
                if (cpr != null && niftyLtp > 0 && cpr.getTc() > 0 && cpr.getBc() > 0) {
                    double cprTop = Math.max(cpr.getTc(), cpr.getBc());
                    double cprBot = Math.min(cpr.getTc(), cpr.getBc());
                    boolean exitLong  = inBullishReversal && "LONG".equals(position)  && niftyLtp >= cprBot;
                    boolean exitShort = inBearishReversal && "SHORT".equals(position) && niftyLtp <= cprTop;
                    if (exitLong || exitShort) {
                        int qty = readQty(fyersSymbol);
                        if (qty <= 0) return;
                        double touchedLevel = exitLong ? cprBot : cprTop;
                        String levelName = exitLong ? "CPR bottom" : "CPR top";
                        eventService.log("[INFO] " + fyersSymbol + " NIFTY reversal-CPR exit triggered — "
                            + position + " position, NIFTY " + niftyState + " touched " + levelName
                            + " (NIFTY=" + String.format("%.2f", niftyLtp)
                            + ", " + levelName + "=" + String.format("%.2f", touchedLevel) + ")");
                        pollingService.squareOff(fyersSymbol, qty, "NIFTY_REVERSAL_CPR_EXIT");
                    }
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
