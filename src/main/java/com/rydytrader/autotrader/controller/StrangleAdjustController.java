package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.StrangleAdjustStreamBroker;
import com.rydytrader.autotrader.service.strategy.StrangleAdjust;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * REST endpoints for the StrangleAdjust strategy:
 * <ul>
 *   <li>{@code GET  /api/strangle-adjust/state}     — dashboard payload (open positions, risk, events)</li>
 *   <li>{@code GET  /api/strangle-adjust/stream}    — SSE stream of the same</li>
 *   <li>{@code POST /api/strangle-adjust/squareoff} — flatten one symbol or every open position</li>
 *   <li>{@code POST /api/strangle-adjust/reset}     — recovery: drop in-memory positions without exits</li>
 *   <li>{@code POST /api/strangle-adjust/enable}    — kill-switch toggle</li>
 * </ul>
 */
@RestController
public class StrangleAdjustController {

    private final StrangleAdjust             strategy;
    private final RiskSettingsStore    riskSettings;
    private final StrangleAdjustStreamBroker streamBroker;

    public StrangleAdjustController(StrangleAdjust strategy,
                              RiskSettingsStore riskSettings,
                              StrangleAdjustStreamBroker streamBroker) {
        this.strategy     = strategy;
        this.riskSettings = riskSettings;
        this.streamBroker = streamBroker;
    }

    @GetMapping(value = "/api/strangle-adjust/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);
        streamBroker.addEmitter(emitter);
        return emitter;
    }

    @GetMapping("/api/strangle-adjust/state")
    public Map<String, Object> getState() {
        return strategy.dashboardState();
    }

    @PostMapping("/api/strangle-adjust/squareoff")
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

    @PostMapping("/api/strangle-adjust/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }

    @PostMapping("/api/strangle-adjust/enable")
    public Map<String, Object> setEnabled(@RequestBody Map<String, Object> body) {
        Object v = body == null ? null : body.get("enabled");
        boolean enabled = (v instanceof Boolean) ? (Boolean) v
            : v != null && Boolean.parseBoolean(v.toString());
        boolean previous = riskSettings.isStrangleAdjustEnabled();
        riskSettings.setStrangleAdjustEnabled(enabled);
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
