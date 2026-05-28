package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.AnalyticsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Powers the Analytics Home page. One endpoint returns the hero card payload + equity curve
 * for a given period × strategy scope.
 *
 * <p>Period values: {@code all}, {@code today}, {@code expiry}, {@code mtd}, {@code ytd}.
 * Default {@code all}. Strategy scope: any registered strategy id (e.g. {@code combined-sl-roll},
 * {@code leg-sl}) or blank / {@code all} for aggregated.
 *
 * <p>Explicit date range params {@code from} and {@code to} (ISO yyyy-MM-dd) override the period
 * preset when present. Either may be omitted. Used by the Home page month/year picker.
 */
@RestController
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    public AnalyticsController(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @GetMapping("/api/analytics/summary")
    public Map<String, Object> summary(
            @RequestParam(defaultValue = "all") String period,
            @RequestParam(defaultValue = "all") String strategyId,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to) {
        return analyticsService.summary(period, strategyId, from, to);
    }
}
