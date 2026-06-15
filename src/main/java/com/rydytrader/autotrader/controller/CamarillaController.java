package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CamarillaLevels;
import com.rydytrader.autotrader.service.CamarillaService;
import com.rydytrader.autotrader.service.strategy.Camarilla;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST endpoints for the Camarilla singleton strategy + its pivot levels.
 * <ul>
 *   <li>{@code GET  /api/camarilla/levels}    — today's H1–H6, L1–L6, PP for NIFTY</li>
 *   <li>{@code GET  /api/camarilla/state}     — live strategy state (active trade, today's
 *       cycles, recent events, spot, MTM)</li>
 *   <li>{@code POST /api/camarilla/squareoff} — manual flatten</li>
 *   <li>{@code POST /api/camarilla/reset}     — recovery: flip in-memory state to IDLE</li>
 * </ul>
 */
@RestController
public class CamarillaController {

    private final CamarillaService levels;
    private final Camarilla strategy;

    public CamarillaController(CamarillaService levels, Camarilla strategy) {
        this.levels   = levels;
        this.strategy = strategy;
    }

    @GetMapping("/api/camarilla/levels")
    public ResponseEntity<?> getLevels() {
        CamarillaLevels lv = levels.getNiftyLevels();
        if (lv == null) return ResponseEntity.status(503).body(Map.of("error", "warming"));
        return ResponseEntity.ok(lv);
    }

    @GetMapping("/api/camarilla/state")
    public Map<String, Object> getState() {
        return strategy.dashboardState();
    }

    @PostMapping("/api/camarilla/squareoff")
    public Map<String, Object> squareoff() {
        boolean closed = strategy.forceClose("MANUAL");
        return Map.of("ok", true, "closedSomething", closed);
    }

    @PostMapping("/api/camarilla/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }
}
