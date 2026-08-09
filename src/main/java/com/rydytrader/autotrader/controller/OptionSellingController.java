package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.strategy.OptionSelling;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST endpoints for the OPTIONS SELLING strategy.
 * <ul>
 *   <li>{@code GET  /api/option-selling/state}     — live multi-position state.
 *       Shape matches OptionScalping so {@code mergeOptionBuyingInto} on the
 *       Trade page can fold this payload into the primary state object.</li>
 *   <li>{@code POST /api/option-selling/squareoff} — flatten one symbol or every
 *       open position.</li>
 *   <li>{@code POST /api/option-selling/reset}     — recovery: drop in-memory
 *       positions without exits.</li>
 *   <li>{@code POST /api/option-selling/enable}    — kill-switch toggle.</li>
 * </ul>
 *
 * <p>SSE stream broker deferred per plan — 2 s polling is fine for MVP.
 */
@RestController
public class OptionSellingController {

    private final OptionSelling      strategy;
    private final RiskSettingsStore  riskSettings;

    public OptionSellingController(OptionSelling strategy,
                                   RiskSettingsStore riskSettings) {
        this.strategy     = strategy;
        this.riskSettings = riskSettings;
    }

    @GetMapping("/api/option-selling/state")
    public Map<String, Object> getState() {
        return strategy.dashboardState();
    }

    @PostMapping("/api/option-selling/squareoff")
    public Map<String, Object> squareoff(@RequestBody(required = false) Map<String, Object> body) {
        Object symObj = body == null ? null : body.get("symbol");
        String symbol = symObj == null ? "" : symObj.toString().trim();
        boolean closed;
        if (!symbol.isEmpty()) {
            closed = strategy.forceCloseSymbol(symbol, "MANUAL");
        } else {
            closed = strategy.forceClose("MANUAL");
        }
        return Map.of("ok", true, "closedSomething", closed);
    }

    @PostMapping("/api/option-selling/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }

    @PostMapping("/api/option-selling/enable")
    public Map<String, Object> setEnabled(@RequestBody Map<String, Object> body) {
        Object v = body == null ? null : body.get("enabled");
        boolean enabled = (v instanceof Boolean) ? (Boolean) v
            : v != null && Boolean.parseBoolean(v.toString());
        boolean previous = riskSettings.isOptionSellingEnabled();
        riskSettings.setOptionSellingEnabled(enabled);
        riskSettings.save();
        if (enabled != previous) {
            strategy.postEvent(
                enabled ? "[SUCCESS]" : "[WARNING]",
                "System",
                enabled
                    ? "Trading resumed — new entries enabled"
                    : "Trading stopped — no new entries will fire (existing positions still managed)");
        }
        return Map.of("ok", true, "enabled", enabled);
    }
}
