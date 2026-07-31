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
 *   <li>{@code POST /api/option-buying/squareoff} — flatten every open position.</li>
 *   <li>{@code POST /api/option-buying/reset}     — recovery: drop in-memory positions
 *       without exit orders (in case broker is out of sync).</li>
 *   <li>{@code POST /api/option-buying/enable}    — kill-switch toggle. Existing open
 *       positions keep running; only new entries are blocked when disabled.</li>
 * </ul>
 *
 * <p>No SSE endpoint yet — the /state poll is sufficient for the shared /positions
 * dashboard. Can be added later if we need sub-second UI updates.
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
        boolean previous = riskSettings.isOptionBuyingEnabled();
        riskSettings.setOptionBuyingEnabled(enabled);
        riskSettings.save();
        if (enabled != previous) {
            // No postEvent hook on OptionBuying yet — just log; the state
            // snapshot's `enabled` flag reflects the new value on next poll.
        }
        return Map.of("ok", true, "enabled", enabled);
    }
}
