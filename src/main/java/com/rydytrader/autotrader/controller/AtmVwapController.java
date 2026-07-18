package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.AtmVwapStreamBroker;
import com.rydytrader.autotrader.service.strategy.AtmVwap;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * REST endpoints for the ATM VWAP strategy.
 * <ul>
 *   <li>{@code GET  /api/atmvwap/state}     — live multi-position state (open positions,
 *       risk block, today's closes, events, spot)</li>
 *   <li>{@code GET  /api/atmvwap/stream}    — SSE stream of the same payload</li>
 *   <li>{@code POST /api/atmvwap/squareoff} — flatten one symbol or every open position</li>
 *   <li>{@code POST /api/atmvwap/reset}     — recovery: drop in-memory positions without exits</li>
 *   <li>{@code POST /api/atmvwap/enable}    — kill-switch toggle (Trade page)</li>
 * </ul>
 */
@RestController
public class AtmVwapController {

    private final AtmVwap             strategy;
    private final RiskSettingsStore   riskSettings;
    private final AtmVwapStreamBroker streamBroker;

    public AtmVwapController(AtmVwap strategy,
                             RiskSettingsStore riskSettings,
                             AtmVwapStreamBroker streamBroker) {
        this.strategy     = strategy;
        this.riskSettings = riskSettings;
        this.streamBroker = streamBroker;
    }

    @GetMapping(value = "/api/atmvwap/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);
        streamBroker.addEmitter(emitter);
        return emitter;
    }

    @GetMapping("/api/atmvwap/state")
    public Map<String, Object> getState() {
        return strategy.dashboardState();
    }

    @PostMapping("/api/atmvwap/squareoff")
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

    @PostMapping("/api/atmvwap/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }

    @PostMapping("/api/atmvwap/enable")
    public Map<String, Object> setEnabled(@RequestBody Map<String, Object> body) {
        Object v = body == null ? null : body.get("enabled");
        boolean enabled = (v instanceof Boolean) ? (Boolean) v
            : v != null && Boolean.parseBoolean(v.toString());
        boolean previous = riskSettings.isAtmVwapEnabled();
        riskSettings.setAtmVwapEnabled(enabled);
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
