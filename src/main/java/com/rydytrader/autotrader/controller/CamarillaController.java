package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CamarillaLevels;
import com.rydytrader.autotrader.service.CamarillaService;
import com.rydytrader.autotrader.service.CamarillaStreamBroker;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.strategy.Camarilla;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * REST endpoints for the Camarilla strategy.
 * <ul>
 *   <li>{@code GET  /api/camarilla/levels}    — per-symbol cached Camarilla pivots</li>
 *   <li>{@code GET  /api/camarilla/state}     — live multi-position state (open positions,
 *       per-symbol levels, risk block, today's closes, events, spot)</li>
 *   <li>{@code POST /api/camarilla/squareoff} — flatten one symbol or every open position</li>
 *   <li>{@code POST /api/camarilla/reset}     — recovery: drop in-memory positions without exits</li>
 *   <li>{@code POST /api/camarilla/enable}    — kill-switch toggle (Trade page)</li>
 * </ul>
 */
@RestController
public class CamarillaController {

    private final CamarillaService     levels;
    private final Camarilla            strategy;
    private final RiskSettingsStore    riskSettings;
    private final CamarillaStreamBroker streamBroker;
    private final EventService         eventService;

    public CamarillaController(CamarillaService levels,
                               Camarilla strategy,
                               RiskSettingsStore riskSettings,
                               CamarillaStreamBroker streamBroker,
                               EventService eventService) {
        this.levels       = levels;
        this.strategy     = strategy;
        this.riskSettings = riskSettings;
        this.streamBroker = streamBroker;
        this.eventService = eventService;
    }

    @GetMapping(value = "/api/camarilla/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L); // no timeout
        streamBroker.addEmitter(emitter);
        return emitter;
    }

    @GetMapping("/api/camarilla/levels")
    public ResponseEntity<Map<String, CamarillaLevels>> getLevels() {
        return ResponseEntity.ok(levels.snapshot());
    }

    @GetMapping("/api/camarilla/state")
    public Map<String, Object> getState() {
        return strategy.dashboardState();
    }

    @PostMapping("/api/camarilla/squareoff")
    public Map<String, Object> squareoff(@RequestBody(required = false) Map<String, Object> body) {
        // Per-row close — caller passes {"symbol":"NSE:NIFTY...CE"}. Closes only that symbol;
        // other open positions stay running. Empty/missing symbol flattens everything (the
        // header "Square Off" button continues to work that way).
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

    @PostMapping("/api/camarilla/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }

    @PostMapping("/api/camarilla/enable")
    public Map<String, Object> setEnabled(@RequestBody Map<String, Object> body) {
        Object v = body == null ? null : body.get("enabled");
        boolean enabled = (v instanceof Boolean) ? (Boolean) v
            : v != null && Boolean.parseBoolean(v.toString());
        boolean previous = riskSettings.isCamarillaEnabled();
        riskSettings.setCamarillaEnabled(enabled);
        riskSettings.save();
        // Only log on an actual state transition — a redundant toggle
        // (checkbox re-clicked without a change) shouldn't clutter the log.
        if (enabled != previous && eventService != null) {
            eventService.log(enabled
                ? "[SUCCESS] Trading resumed — new entries enabled"
                : "[WARNING] Trading stopped — no new entries will fire (existing positions still managed)");
        }
        return Map.of("ok", true, "enabled", enabled);
    }
}
