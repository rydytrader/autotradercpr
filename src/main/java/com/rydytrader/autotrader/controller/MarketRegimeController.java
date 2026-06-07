package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.MarketRegimeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Single-endpoint exposure of {@link MarketRegimeService}'s cached snapshot. Polled
 * every 30 s by the analytics page's Overview-header pill. Returns:
 * <ul>
 *   <li>{@code marketRegime} — primary playbook recommendation: MEAN-REV / TREND / UNKNOWN.</li>
 *   <li>{@code hurstAxis} / {@code atrAxis} — underlying axis labels (for the tooltip).</li>
 *   <li>{@code hurstExponent} / {@code atrShort} / {@code atrLong} / {@code atrRatio} — raw
 *       numerics (for the tooltip).</li>
 *   <li>{@code asOfMillis} — when the cached snapshot was last computed.</li>
 * </ul>
 */
@RestController
public class MarketRegimeController {

    private final MarketRegimeService service;

    public MarketRegimeController(MarketRegimeService service) {
        this.service = service;
    }

    @GetMapping("/api/market-regime")
    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("marketRegime",  service.marketRegime());
        m.put("hurstAxis",     service.hurstAxis());
        m.put("atrAxis",       service.atrAxis());
        m.put("hurstExponent", service.hurstValue());
        m.put("atrShort",      service.atrShortValue());
        m.put("atrLong",       service.atrLongValue());
        m.put("atrRatio",      service.atrRatioValue());
        m.put("asOfMillis",    service.asOfMillis());
        return m;
    }
}
