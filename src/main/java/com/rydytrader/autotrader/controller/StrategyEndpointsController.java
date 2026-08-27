package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.service.strategy.VwapSupertrendStrategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * URL-compatible replacement for the retired OptionBuyingController. The
 * {@code /trade} positions page + {@code ticker.js} were hard-wired to
 * {@code /api/option-buying/*} — keeping the paths lets the frontend work
 * unchanged while the backend now delegates to {@link VwapSupertrendStrategy}
 * and {@link RiskSettingsStore#isVwapStEnabled()}.
 */
@RestController
public class StrategyEndpointsController {

    private static final Logger log = LoggerFactory.getLogger(StrategyEndpointsController.class);

    private final RiskSettingsStore riskSettings;
    private final EventService      eventService;
    private final PollingService    pollingService;
    private final ObjectProvider<VwapSupertrendStrategy> strategyProvider;

    public StrategyEndpointsController(RiskSettingsStore riskSettings,
                                        EventService eventService,
                                        PollingService pollingService,
                                        ObjectProvider<VwapSupertrendStrategy> strategyProvider) {
        this.riskSettings     = riskSettings;
        this.eventService     = eventService;
        this.pollingService   = pollingService;
        this.strategyProvider = strategyProvider;
    }

    /** Snapshot used by the positions page + navbar ticker. */
    @GetMapping("/api/option-buying/state")
    public Map<String, Object> state() {
        VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled",       riskSettings.isVwapStEnabled());
        out.put("lifecycle",     s == null ? "IDLE" : s.currentState());
        out.put("currentState",  s == null ? "IDLE" : s.currentState());
        out.put("openPositions", buildOpenPositions());
        out.put("todayClosedTrades", s == null ? Collections.emptyList() : s.todayClosedTrades());
        out.put("recentEvents",  buildRecentEvents(200));
        out.put("risk",          buildRisk(s));
        out.put("liveNetPnl",    s == null ? 0.0 : s.liveNetPnlToday());
        out.put("liveCharges",   s == null ? 0.0 : s.liveChargesToday());
        return out;
    }

    /** Master kill switch — toggles {@code vwapStEnabled}. */
    @PostMapping("/api/option-buying/enable")
    public Map<String, Object> setEnabled(@RequestBody Map<String, Object> body) {
        boolean enabled = body != null
            && body.get("enabled") != null
            && Boolean.parseBoolean(body.get("enabled").toString());
        riskSettings.setVwapStEnabled(enabled);
        try { riskSettings.saveFor("live"); } catch (Exception e) {
            log.warn("[Strategy] saveFor failed: {}", e.getMessage());
        }
        log.info("[Strategy] kill switch → {}", enabled ? "ON" : "OFF");
        return Map.of("ok", true, "enabled", enabled);
    }

    /** Force-close every open leg via the strategy. */
    @PostMapping("/api/option-buying/squareoff")
    public Map<String, Object> squareoff() {
        VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
        boolean acted = s != null && s.forceClose("MANUAL_SQUAREOFF");
        return Map.of("ok", true, "acted", acted);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private List<Map<String, Object>> buildOpenPositions() {
        try {
            VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
            List<Map<String, Object>> out = new ArrayList<>();
            pollingService.fetchPositions().forEach(p -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("symbol",   p.getSymbol());
                m.put("qty",      p.getQty());
                m.put("side",     p.getSide());
                m.put("avgPrice", p.getAvgPrice());
                m.put("ltp",      p.getLtp());
                double pnl = "LONG".equals(p.getSide())
                    ? (p.getLtp() - p.getAvgPrice()) * p.getQty()
                    : (p.getAvgPrice() - p.getLtp()) * p.getQty();
                m.put("pnl", pnl);
                m.put("mtm", pnl);   // trade.html reads either field
                // Merge strategy-computed levels: entryPrice (fill), slPrice,
                // targetPrice, and a leg-side setup label (CE / PE). Only
                // present when this symbol matches the strategy's chosen pair.
                if (s != null) {
                    Map<String, Object> leg = s.getLegSnapshot(p.getSymbol());
                    if (!leg.isEmpty()) {
                        Object entry = leg.get("entryPrice");
                        Object sl    = leg.get("slPrice");
                        Object tgt   = leg.get("targetPrice");
                        Object side  = leg.get("side");
                        if (entry instanceof Number && ((Number) entry).doubleValue() > 0) m.put("entryPrice", entry);
                        if (sl    instanceof Number && ((Number) sl).doubleValue()    > 0) m.put("slPrice",     sl);
                        if (tgt   instanceof Number && ((Number) tgt).doubleValue()   > 0) m.put("targetLevel", tgt);
                        if (side  != null) m.put("setup", side);
                    }
                }
                out.add(m);
            });
            return out;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    // Event line shape: "HH:mm:ss - [SEVERITY] [SOURCE] message body" (SEVERITY
    // and SOURCE both optional). Parse into the fields the trade page expects
    // ({ts, severity, source, message}) — otherwise the frontend renders empty
    // rows tagged "[Strategy]" (its default when source is undefined).
    private static final java.util.regex.Pattern EVENT_LINE = java.util.regex.Pattern.compile(
        "^(\\d{2}:\\d{2}:\\d{2})\\s*-\\s*"
      + "(?:\\[([A-Z_-]+)\\]\\s*)?"                          // optional severity
      + "(?:\\[([A-Za-z0-9 _.:-]+)\\]\\s*)?"                 // optional source
      + "(.*)$");

    private List<Map<String, Object>> buildRecentEvents(int limit) {
        try {
            List<String> all = eventService.getTradeLogs();
            int from = Math.max(0, all.size() - Math.max(1, limit));
            List<String> tail = new ArrayList<>(all.subList(from, all.size()));
            Collections.reverse(tail);
            java.time.LocalDate today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata"));
            List<Map<String, Object>> out = new ArrayList<>(tail.size());
            for (String line : tail) {
                Map<String, Object> m = new LinkedHashMap<>();
                java.util.regex.Matcher mt = EVENT_LINE.matcher(line);
                if (mt.matches()) {
                    // Build a ts (epoch ms in the browser's local TZ = IST) from the
                    // HH:mm:ss + today's date so frontend fmtTime renders correctly.
                    try {
                        java.time.LocalTime lt = java.time.LocalTime.parse(mt.group(1));
                        long ts = today.atTime(lt)
                            .atZone(java.time.ZoneId.of("Asia/Kolkata"))
                            .toInstant().toEpochMilli();
                        m.put("ts", ts);
                    } catch (Exception ignored) {}
                    String sev = mt.group(2);
                    if (sev == null || sev.isBlank()) sev = "INFO";
                    m.put("severity", sev);
                    m.put("source",   mt.group(3) != null ? mt.group(3) : "Strategy");
                    m.put("message",  mt.group(4) != null ? mt.group(4).trim() : "");
                } else {
                    // Line didn't match — pass the whole thing as message.
                    m.put("severity", "INFO");
                    m.put("source",   "Strategy");
                    m.put("message",  line);
                }
                out.add(m);
            }
            return out;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private Map<String, Object> buildRisk(VwapSupertrendStrategy s) {
        Map<String, Object> r = new LinkedHashMap<>();
        double net = s == null ? 0.0 : s.liveNetPnlToday();
        double consumed = Math.min(0, net) * -1.0;
        // Uses portfolioMaxRiskPct × startingCapital — same source of truth the
        // Risk tab in the settings modal shows. Was previously getMaxDailyLoss()
        // (totalCapital × maxRiskPerDayPct), a legacy pair the UI never edits,
        // which caused the trade page to show stale defaults.
        double budget = riskSettings.getPortfolioMaxDailyLoss();
        r.put("consumedRisk",    consumed);
        r.put("dailyRiskBudget", budget);
        r.put("realisedPnl",     net);
        return r;
    }
}
