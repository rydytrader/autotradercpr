package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.VirginCprService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Read-only status endpoint for active Virgin CPR snapshots. The no-{@code ticker} variants
 * default to NIFTY50 for backwards compatibility with the NIFTY card UI; pass
 * {@code ?ticker=NIFTYBANK} (or any other index ticker in the Stock Universe) to operate
 * on a sector index instead.
 */
@RestController
public class VirginCprController {

    private final VirginCprService virginCprService;

    public VirginCprController(VirginCprService virginCprService) {
        this.virginCprService = virginCprService;
    }

    @GetMapping("/api/virgin-cpr/status")
    public Map<String, Object> status(@RequestParam(defaultValue = "NIFTY50") String ticker) {
        Map<String, Object> r = virginCprService.getActiveStatus(ticker);
        // Always return a Map so the JS layer can branch on `active` flag without
        // dealing with null bodies.
        if (r == null) return Map.of("active", false, "ticker", ticker);
        return Map.of(
            "active", true,
            "ticker", r.getOrDefault("ticker", ticker),
            "date", r.getOrDefault("date", ""),
            "tc",   r.getOrDefault("tc", 0),
            "pivot",r.getOrDefault("pivot", 0),
            "bc",   r.getOrDefault("bc", 0),
            "tradingDaysSince", r.getOrDefault("tradingDaysSince", 0),
            "daysRemaining",    r.getOrDefault("daysRemaining", 0)
        );
    }

    /**
     * One-time backfill — scans the index's last N trading days for any virgin CPRs and
     * caches the most recent one. Accepts both GET (browser-friendly) and POST so an admin
     * can hit it via the address bar:
     * {@code http://localhost:8080/api/virgin-cpr/backfill?days=10&ticker=NIFTYBANK}.
     */
    @RequestMapping(value = "/api/virgin-cpr/backfill", method = { RequestMethod.GET, RequestMethod.POST })
    public Map<String, Object> backfill(@RequestParam(defaultValue = "10") int days,
                                         @RequestParam(defaultValue = "NIFTY50") String ticker) {
        return virginCprService.backfill(days, ticker);
    }

    /**
     * Manually clear the active virgin CPR for a specific index ticker. Accepts GET and POST.
     */
    @RequestMapping(value = "/api/virgin-cpr/clear", method = { RequestMethod.GET, RequestMethod.POST })
    public Map<String, Object> clear(@RequestParam(defaultValue = "NIFTY50") String ticker) {
        boolean wasActive = virginCprService.getActiveVirginCpr(ticker) != null;
        virginCprService.clearSnapshot(ticker);
        return Map.of("cleared", wasActive, "ticker", ticker);
    }
}
