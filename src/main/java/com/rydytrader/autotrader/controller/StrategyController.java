package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.service.strategy.StrategyRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic per-strategy REST endpoints. The dashboard JS for {@code /strategies/{id}} polls
 * these instead of any strategy-specific routes — the same JS file works for every strategy.
 *
 * <p>Each endpoint resolves the {@link Strategy} via {@link StrategyRegistry#get(String)} and
 * delegates to the interface methods. Unknown strategy IDs return 404.
 */
@RestController
public class StrategyController {

    private final StrategyRegistry registry;

    public StrategyController(StrategyRegistry registry) {
        this.registry = registry;
    }

    /** List of all registered strategies — used by the left sidebar to render nav icons. */
    @GetMapping("/api/strategies")
    public List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Strategy s : registry.all()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", s.id());
            m.put("displayName", s.displayName());
            m.put("currentState", s.currentState());
            m.put("navIcon", s.navIcon());
            out.add(m);
        }
        return out;
    }

    @GetMapping("/api/strategies/{id}/status")
    public ResponseEntity<Map<String, Object>> status(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.getStatus());
    }

    @GetMapping("/api/strategies/{id}/dashboard")
    public ResponseEntity<Map<String, Object>> dashboard(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.getDashboard());
    }

    @GetMapping("/api/strategies/{id}/settings/schema")
    public ResponseEntity<List<Map<String, Object>>> settingsSchema(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.getSettingsSchema());
    }

    @PostMapping("/api/strategies/{id}/reset")
    public ResponseEntity<Map<String, Object>> reset(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        s.resetToIdle("manual");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", true);
        out.put("message", "Strategy " + id + " state reset to IDLE. Next scheduler tick will evaluate entry time.");
        return ResponseEntity.ok(out);
    }

    @PostMapping("/api/strategies/{id}/close")
    public ResponseEntity<Map<String, Object>> close(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        boolean ok = s.forceClose("MANUAL_DASHBOARD");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok);
        out.put("message", ok
            ? "Strategy " + id + " — legs flattened. State → DONE_FOR_DAY."
            : "Nothing to square off — no active position for " + id + ".");
        return ResponseEntity.ok(out);
    }
}
