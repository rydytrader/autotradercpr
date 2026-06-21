package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.OptionOiBuildupService;
import com.rydytrader.autotrader.service.OptionOiTracker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Exposes the {@link OptionOiTracker} state to the UI:
 *
 * <ul>
 *   <li>{@code GET /api/option-oi/state}        — current snapshot for the Live Positions
 *       header tiles (ΔCE, ΔPE, DIFF, TREND).</li>
 *   <li>{@code GET /api/option-oi/history}      — per-sample series since 09:15 IST for
 *       the trend modal's Chart.js line chart.</li>
 *   <li>{@code GET /api/option-oi/max-buildup}  — strike with highest CE OI and strike
 *       with highest PE OI, each with their buildup classification. Mostly a fallback
 *       now — primary delivery is via the Camarilla SSE state event which embeds the
 *       same payload under the {@code maxOiBuildup} key.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/option-oi")
public class OptionOiController {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final OptionOiTracker          tracker;
    private final OptionOiBuildupService   buildupService;

    public OptionOiController(OptionOiTracker tracker, OptionOiBuildupService buildupService) {
        this.tracker        = tracker;
        this.buildupService = buildupService;
    }

    @GetMapping("/state")
    public ResponseEntity<OptionOiTracker.Snapshot> state() {
        return ResponseEntity.ok(tracker.snapshot());
    }

    @GetMapping("/history")
    public ResponseEntity<OptionOiTracker.History> history() {
        return ResponseEntity.ok(tracker.history());
    }

    @GetMapping("/max-buildup")
    public Map<String, Object> maxBuildup() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("asOf", LocalTime.now(IST).format(HHMMSS));
        out.putAll(buildupService.currentEnriched());
        return out;
    }
}
