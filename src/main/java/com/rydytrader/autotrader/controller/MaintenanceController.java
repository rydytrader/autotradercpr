package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.strategy.OptionBuying;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maintenance endpoints — destructive recovery actions that wipe state without
 * touching live trading. Currently exposes one action: clearing today's recorded
 * trades + events. Open positions are deliberately preserved.
 */
@RestController
public class MaintenanceController {

    private final OptionBuying strategy;

    public MaintenanceController(OptionBuying strategy) {
        this.strategy = strategy;
    }

    /** Clear all of today's closed-trade records, today's event-log entries, and
     *  today's DB rows (both ALGO and MANUAL). Open positions stay running.
     *  Returns the count of records cleared per bucket. */
    @PostMapping("/api/maintenance/clear-today")
    public Map<String, Object> clearToday() {
        Map<String, Object> result = strategy.clearTodayRecords();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.putAll(result);
        return out;
    }

    /** Wipe EVERY closed trade in the database + every event in the in-memory
     *  ring + all in-flight pending confirmations. Open positions stay running.
     *  Hard reset for a clean post-test baseline. Irreversible. */
    @PostMapping("/api/maintenance/clear-all")
    public Map<String, Object> clearAll() {
        Map<String, Object> result = strategy.clearAllRecords();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.putAll(result);
        return out;
    }
}
