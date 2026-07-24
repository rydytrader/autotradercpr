package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OptionOiTracker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Exposes {@link OptionOiTracker} state to the trade page.
 *
 * <ul>
 *   <li>{@code GET /api/atmvwap/oi/state}   — current cumulative snapshot + bias.</li>
 *   <li>{@code GET /api/atmvwap/oi/history} — per-sample series since baseline for
 *       the inline Chart.js line chart.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/atmvwap/oi")
public class OptionOiController {

    private final OptionOiTracker    tracker;
    private final MarketDataService  marketDataService;

    public OptionOiController(OptionOiTracker tracker, MarketDataService marketDataService) {
        this.tracker           = tracker;
        this.marketDataService = marketDataService;
    }

    @GetMapping("/state")
    public ResponseEntity<Map<String, Object>> state() {
        // Wrap the Snapshot record + exchange-clock reference so the UI can align its
        // minute-boundary polls to EXCHANGE time (not browser wall clock). exchangeNowMs
        // = max exchFeedTime across all subscribed symbols in epoch millis; 0 pre-tick.
        Map<String, Object> body = new LinkedHashMap<>();
        OptionOiTracker.Snapshot snap = tracker.snapshot();
        // Flatten the record into the map so existing UI keys keep working verbatim.
        body.put("baselineTakenAt",      snap.baselineTakenAt());
        body.put("lastSampleAt",         snap.lastSampleAt());
        body.put("samplesTaken",         snap.samplesTaken());
        body.put("cumulativeCeChange",   snap.cumulativeCeChange());
        body.put("cumulativePeChange",   snap.cumulativePeChange());
        body.put("diff",                 snap.diff());
        body.put("ceToPeRatio",          snap.ceToPeRatio());
        body.put("bias",                 snap.bias());
        body.put("atmStrike",            snap.atmStrike());
        body.put("activeStrikeCount",    snap.activeStrikeCount());
        body.put("biasThresholdPct",     snap.biasThresholdPct());
        body.put("biasActualPct",        snap.biasActualPct());
        long latestExchSec = marketDataService.getLatestExchFeedTimeSec();
        body.put("exchangeNowMs",        latestExchSec > 0 ? latestExchSec * 1000L : 0L);
        return ResponseEntity.ok(body);
    }

    @GetMapping("/history")
    public ResponseEntity<OptionOiTracker.History> history() {
        return ResponseEntity.ok(tracker.history());
    }
}
