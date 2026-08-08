package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.OptionScalpingStreamBroker;
import com.rydytrader.autotrader.service.strategy.OptionScalping;
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
 *   <li>{@code GET  /api/option-scalping/state}     — live multi-position state (open positions,
 *       risk block, today's closes, events, spot)</li>
 *   <li>{@code GET  /api/option-scalping/stream}    — SSE stream of the same payload</li>
 *   <li>{@code POST /api/option-scalping/squareoff} — flatten one symbol or every open position</li>
 *   <li>{@code POST /api/option-scalping/reset}     — recovery: drop in-memory positions without exits</li>
 *   <li>{@code POST /api/option-scalping/enable}    — kill-switch toggle (Trade page)</li>
 * </ul>
 */
@RestController
public class OptionScalpingController {

    private final OptionScalping             strategy;
    private final RiskSettingsStore   riskSettings;
    private final OptionScalpingStreamBroker streamBroker;

    public OptionScalpingController(OptionScalping strategy,
                             RiskSettingsStore riskSettings,
                             OptionScalpingStreamBroker streamBroker) {
        this.strategy     = strategy;
        this.riskSettings = riskSettings;
        this.streamBroker = streamBroker;
    }

    @GetMapping(value = "/api/option-scalping/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);
        streamBroker.addEmitter(emitter);
        return emitter;
    }

    @GetMapping("/api/option-scalping/state")
    public Map<String, Object> getState() {
        return strategy.dashboardState();
    }

    /** Premium SuperTrend snapshot for both ATM CE and PE legs — consumed by the
     *  /positions NIFTY Technicals row so the operator sees the same premium-ST
     *  values the entry gate + trailing SL evaluate. */
    @GetMapping("/api/option-scalping/indicators")
    public Map<String, Object> getIndicators() {
        return strategy.optionIndicatorsSnapshot();
    }

    @PostMapping("/api/option-scalping/squareoff")
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

    @PostMapping("/api/option-scalping/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }

    @PostMapping("/api/option-scalping/enable")
    public Map<String, Object> setEnabled(@RequestBody Map<String, Object> body) {
        Object v = body == null ? null : body.get("enabled");
        boolean enabled = (v instanceof Boolean) ? (Boolean) v
            : v != null && Boolean.parseBoolean(v.toString());
        boolean previous = riskSettings.isOptionBuyingEnabled();
        riskSettings.setOptionBuyingEnabled(enabled);
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
