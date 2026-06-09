package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.entity.StrategyInstanceEntity;
import com.rydytrader.autotrader.service.StrangleInstanceManager;
import com.rydytrader.autotrader.service.strategy.ShortStrangle;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CRUD REST endpoints for short-straddle instances. Backs the Straddles tab in the global
 * Settings modal.
 *
 * <ul>
 *   <li>{@code GET    /api/strangles}                   — list active instances</li>
 *   <li>{@code POST   /api/strangles}                   — create new instance</li>
 *   <li>{@code PUT    /api/strangles/{id}}              — rename / re-describe</li>
 *   <li>{@code DELETE /api/strangles/{id}}              — soft-delete</li>
 *   <li>{@code POST   /api/strangles/{id}/enable}       — flip enabled flag → true</li>
 *   <li>{@code POST   /api/strangles/{id}/disable}      — flip enabled flag → false (409 if open)</li>
 * </ul>
 *
 * <p>The {@code id} path component is the full strategy id ({@code "inst-<entityId>"}). 409
 * responses surface the lifecycle state so the UI can show "legs open, squareoff first".
 */
@RestController
@RequestMapping("/api/strangles")
public class StrangleInstanceController {

    private final StrangleInstanceManager manager;

    public StrangleInstanceController(StrangleInstanceManager manager) {
        this.manager = manager;
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (ShortStrangle s : manager.all()) {
            out.add(toDto(s));
        }
        return out;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            String name        = asString(body.get("name"));
            String description = asString(body.get("description"));
            String shortCode   = asString(body.get("shortCode"));
            ShortStrangle s = manager.create(name, description, shortCode);
            return ResponseEntity.status(HttpStatus.CREATED).body(toDto(s));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(error(e.getMessage()));
        }
    }

    @PutMapping("/{id}")
    public ResponseEntity<Map<String, Object>> rename(@PathVariable String id,
                                                      @RequestBody Map<String, Object> body) {
        try {
            String name        = asString(body.get("name"));
            String description = asString(body.get("description"));
            String shortCode   = asString(body.get("shortCode"));
            ShortStrangle s = manager.rename(id, name, description, shortCode);
            if (s == null) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(toDto(s));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(error(e.getMessage()));
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> delete(@PathVariable String id) {
        try {
            manager.softDelete(id);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("success", true);
            return ResponseEntity.ok(out);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error(e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(error(e.getMessage()));
        }
    }

    @PostMapping("/{id}/enable")
    public ResponseEntity<Map<String, Object>> enable(@PathVariable String id) {
        return toggleEnabled(id, true);
    }

    @PostMapping("/{id}/disable")
    public ResponseEntity<Map<String, Object>> disable(@PathVariable String id) {
        return toggleEnabled(id, false);
    }

    private ResponseEntity<Map<String, Object>> toggleEnabled(String id, boolean enabled) {
        try {
            manager.setEnabled(id, enabled);
            ShortStrangle s = manager.get(id);
            return ResponseEntity.ok(s != null ? toDto(s) : okOnly());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error(e.getMessage()));
        } catch (IllegalStateException e) {
            ShortStrangle s = manager.get(id);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("success", false);
            body.put("error", e.getMessage());
            if (s != null) body.put("currentState", s.currentState());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
        }
    }

    private Map<String, Object> toDto(ShortStrangle s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id",           s.id());
        m.put("name",         s.displayName());
        m.put("description",  s.description());
        m.put("shortCode",    s.shortCode());
        m.put("navIcon",      s.navIcon());
        m.put("currentState", s.currentState());
        // enabled = riskSettings.getStrategyBool — read via the entity to avoid races; instance
        // settings are saved through the strategy interface POST, which is the same data flow.
        StrategyInstanceEntity row = manager.findEntity(s.id()).orElse(null);
        m.put("enabled", row != null && manager.allEnabled().contains(s));
        return m;
    }

    private Map<String, Object> error(String msg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("success", false);
        m.put("error", msg);
        return m;
    }

    private Map<String, Object> okOnly() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("success", true);
        return m;
    }

    private static String asString(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}
