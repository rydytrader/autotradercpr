package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.OptionOiTracker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes {@link OptionOiTracker} state to the trade page.
 *
 * <ul>
 *   <li>{@code GET /api/atmvwap/oi/state}   — LAST 1-min snapshot of cumulative
 *       CE/PE change + bias. Frozen between minute boundaries so a browser
 *       refresh doesn't produce a different % / bias on each poll. Internal
 *       callers (AtmVwap entry filter) still read the live values via
 *       {@link OptionOiTracker#snapshot()}.</li>
 *   <li>{@code GET /api/atmvwap/oi/history} — per-sample series since baseline for
 *       the inline Chart.js line chart.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/atmvwap/oi")
public class OptionOiController {

    private final OptionOiTracker tracker;

    public OptionOiController(OptionOiTracker tracker) {
        this.tracker = tracker;
    }

    @GetMapping("/state")
    public ResponseEntity<OptionOiTracker.Snapshot> state() {
        return ResponseEntity.ok(tracker.snapshotForUi());
    }

    @GetMapping("/history")
    public ResponseEntity<OptionOiTracker.History> history() {
        return ResponseEntity.ok(tracker.history());
    }
}
