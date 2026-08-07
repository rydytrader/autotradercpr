package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.strategy.OptionBuying;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST endpoints for the OPTION BUYING strategy.
 * <ul>
 *   <li>{@code GET  /api/option-buying/state}     — live snapshot (open positions,
 *       today's cycles, event log, counters).</li>
 *   <li>{@code GET  /api/option-buying/indicators} — NIFTY SuperTrend / RSI / BB / pivots.</li>
 *   <li>{@code POST /api/option-buying/squareoff} — flatten every open position.</li>
 *   <li>{@code POST /api/option-buying/reset}     — recovery: drop in-memory positions
 *       without exit orders (in case broker is out of sync).</li>
 *   <li>{@code POST /api/option-buying/enable}    — kill-switch toggle. Existing
 *       open positions keep running; only new entries are blocked when disabled.</li>
 * </ul>
 */
@RestController
public class OptionBuyingController {

    private final OptionBuying      strategy;
    private final RiskSettingsStore riskSettings;

    public OptionBuyingController(OptionBuying strategy,
                                   RiskSettingsStore riskSettings) {
        this.strategy     = strategy;
        this.riskSettings = riskSettings;
    }

    @GetMapping("/api/option-buying/state")
    public Map<String, Object> getState() {
        return strategy.stateSnapshot();
    }

    /** NIFTY spot indicator snapshot for the header strip on /positions:
     *  SuperTrend line + direction, Bollinger upper/mid/lower, RSI, and R1/S1
     *  from yesterday's daily pivots. Cheap to compute (single indicator pass
     *  per call), safe to poll every few seconds. */
    @GetMapping("/api/option-buying/indicators")
    public Map<String, Object> getIndicators() {
        return strategy.indicatorsSnapshot();
    }

    @PostMapping("/api/option-buying/squareoff")
    public Map<String, Object> squareoff() {
        boolean closed = strategy.forceClose("MANUAL");
        return Map.of("ok", true, "closedSomething", closed);
    }

    @PostMapping("/api/option-buying/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }

    @PostMapping("/api/option-buying/enable")
    public Map<String, Object> setEnabled(@RequestBody Map<String, Object> body) {
        Object v = body == null ? null : body.get("enabled");
        boolean enabled = (v instanceof Boolean) ? (Boolean) v
            : v != null && Boolean.parseBoolean(v.toString());
        riskSettings.setOptionBuyingEnabled(enabled);
        riskSettings.save();
        return Map.of("ok", true, "enabled", enabled);
    }
}
