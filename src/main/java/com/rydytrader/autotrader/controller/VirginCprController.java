package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.VirginCprService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Read-only status endpoint for the active NIFTY Virgin CPR (or null if none). */
@RestController
public class VirginCprController {

    private final VirginCprService virginCprService;

    public VirginCprController(VirginCprService virginCprService) {
        this.virginCprService = virginCprService;
    }

    @GetMapping("/api/virgin-cpr/status")
    public Map<String, Object> status() {
        Map<String, Object> r = virginCprService.getActiveStatus();
        // Always return a Map so the JS layer can branch on `active` flag without
        // dealing with null bodies.
        if (r == null) return Map.of("active", false);
        return Map.of(
            "active", true,
            "date", r.getOrDefault("date", ""),
            "tc",   r.getOrDefault("tc", 0),
            "pivot",r.getOrDefault("pivot", 0),
            "bc",   r.getOrDefault("bc", 0),
            "tradingDaysSince", r.getOrDefault("tradingDaysSince", 0),
            "daysRemaining",    r.getOrDefault("daysRemaining", 0)
        );
    }

    /**
     * One-time backfill — scans NIFTY's last N trading days for any virgin CPRs and
     * caches the most recent one. Accepts both GET (browser-friendly) and POST so an
     * admin can hit it via the address bar:
     * {@code http://localhost:8080/api/virgin-cpr/backfill?days=10}.
     */
    @RequestMapping(value = "/api/virgin-cpr/backfill", method = { RequestMethod.GET, RequestMethod.POST })
    public Map<String, Object> backfill(@RequestParam(defaultValue = "10") int days) {
        return virginCprService.backfill(days);
    }
}
