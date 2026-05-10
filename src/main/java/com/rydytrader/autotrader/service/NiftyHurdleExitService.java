package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.Map;

/**
 * Defensive exit triggered when NIFTY's prior 15-min HTF Hurdle confirmation is broken.
 *
 * <p>When a stock trade fires under the NIFTY HTF Hurdle filter (i.e., NIFTY's prior
 * 15-min close had cleared its nearest weekly hurdle in trade direction), the gating
 * 15-min bar's LOW (for buys) or HIGH (for sells) is captured onto the position record
 * via {@code PositionStateStore.saveNiftyHurdleGuard} at fill time. This service listens
 * to NIFTY 5-min candle closes and squares off any open position whose guard has been
 * breached:
 *
 * <ul>
 *   <li>LONG: NIFTY 5-min close &lt; {@code niftyHurdleGuardLow} → exit, reason
 *       {@code NIFTY_HURDLE_FAIL}</li>
 *   <li>SHORT: NIFTY 5-min close &gt; {@code niftyHurdleGuardHigh} → exit</li>
 * </ul>
 *
 * <p>Gated by {@link RiskSettingsStore#isEnableNiftyHtfHurdleExit()} (default off — opt-in).
 * Positions without a guard (entered when the filter was off or no hurdle existed in trade
 * direction at entry) are skipped naturally. Strict comparison ({@code <} / {@code >}) — a
 * close exactly equal to the guard counts as "still defended".
 */
@Service
public class NiftyHurdleExitService implements CandleAggregator.CandleCloseListener {

    private final RiskSettingsStore riskSettings;
    private final PositionStateStore positionStateStore;
    private final EventService eventService;
    @Autowired @Lazy private PollingService pollingService;
    @Autowired @Lazy private CandleAggregator candleAggregator;

    public NiftyHurdleExitService(RiskSettingsStore riskSettings,
                                  PositionStateStore positionStateStore,
                                  EventService eventService) {
        this.riskSettings = riskSettings;
        this.positionStateStore = positionStateStore;
        this.eventService = eventService;
    }

    @PostConstruct
    public void registerCandleListener() {
        if (candleAggregator != null) {
            candleAggregator.addListener(this);
        }
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar candle) {
        if (candle == null) return;
        // Only react to NIFTY index 5-min closes — that's our trigger source.
        if (!IndexTrendService.NIFTY_SYMBOL.equals(fyersSymbol)) return;
        if (!riskSettings.isEnableNiftyHtfHurdleExit()) return;
        if (candle.close <= 0) return;

        Map<String, Map<String, Object>> openPositions = positionStateStore.loadAll();
        if (openPositions == null || openPositions.isEmpty()) return;

        double niftyClose = candle.close;
        for (Map.Entry<String, Map<String, Object>> entry : openPositions.entrySet()) {
            String symbol = entry.getKey();
            Map<String, Object> state = entry.getValue();
            if (state == null) continue;

            Object guardLowObj  = state.get("niftyHurdleGuardLow");
            Object guardHighObj = state.get("niftyHurdleGuardHigh");
            if (guardLowObj == null && guardHighObj == null) continue;

            double guardLow  = parseDouble(guardLowObj);
            double guardHigh = parseDouble(guardHighObj);
            if (guardLow <= 0 && guardHigh <= 0) continue;

            String side = state.get("side") != null ? state.get("side").toString() : "";
            boolean breached = false;
            String reason = null;
            if ("LONG".equals(side) && guardLow > 0 && niftyClose < guardLow) {
                breached = true;
                reason = "NIFTY 5m close " + String.format("%.2f", niftyClose)
                       + " < hurdle-guard low " + String.format("%.2f", guardLow);
            } else if ("SHORT".equals(side) && guardHigh > 0 && niftyClose > guardHigh) {
                breached = true;
                reason = "NIFTY 5m close " + String.format("%.2f", niftyClose)
                       + " > hurdle-guard high " + String.format("%.2f", guardHigh);
            }
            if (!breached) continue;

            int qty = readQty(state);
            if (qty <= 0) continue;

            eventService.log("[INFO] " + symbol + " NIFTY HTF Hurdle break exit triggered — "
                + side + " position, " + reason);
            if (pollingService != null) {
                pollingService.squareOff(symbol, qty, "NIFTY_HURDLE_FAIL");
            }
        }
    }

    private int readQty(Map<String, Object> state) {
        Object q = state.get("qty");
        if (q == null) return 0;
        try { return Integer.parseInt(q.toString()); }
        catch (NumberFormatException e) { return 0; }
    }

    private double parseDouble(Object v) {
        if (v == null) return 0;
        try { return Double.parseDouble(v.toString()); }
        catch (NumberFormatException e) { return 0; }
    }
}
