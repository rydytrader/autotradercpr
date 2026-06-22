package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.OptionOiTracker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the {@link OptionOiTracker} state to the UI:
 *
 * <ul>
 *   <li>{@code GET /api/option-oi/state}   — current snapshot for the Live Positions
 *       header tiles (ΔCE, ΔPE, DIFF, TREND).</li>
 *   <li>{@code GET /api/option-oi/history} — per-sample series since 09:15 IST for
 *       the trend modal's Chart.js line chart.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/option-oi")
public class OptionOiController {

    private final OptionOiTracker tracker;

    public OptionOiController(OptionOiTracker tracker) {
        this.tracker = tracker;
    }

    @GetMapping("/state")
    public ResponseEntity<OptionOiTracker.Snapshot> state() {
        return ResponseEntity.ok(tracker.snapshot());
    }

    @GetMapping("/history")
    public ResponseEntity<OptionOiTracker.History> history() {
        return ResponseEntity.ok(tracker.history());
    }
}
