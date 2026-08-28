package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.service.strategy.VwapSupertrendStrategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

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
    private final MarketDataService marketDataService;
    private final ObjectProvider<VwapSupertrendStrategy> strategyProvider;

    private final CopyOnWriteArrayList<SseEmitter> stateEmitters = new CopyOnWriteArrayList<>();
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final AtomicLong lastPushMs = new AtomicLong(0);
    /** Push cadence guard for tick-driven state emissions — limits max fanout
     *  to ~10 Hz even during heavy tick storms so /trade doesn't get
     *  flooded and browsers can keep up rendering. */
    private static final long MIN_PUSH_INTERVAL_MS = 100;

    public StrategyEndpointsController(RiskSettingsStore riskSettings,
                                        EventService eventService,
                                        PollingService pollingService,
                                        MarketDataService marketDataService,
                                        ObjectProvider<VwapSupertrendStrategy> strategyProvider) {
        this.riskSettings      = riskSettings;
        this.eventService      = eventService;
        this.pollingService    = pollingService;
        this.marketDataService = marketDataService;
        this.strategyProvider  = strategyProvider;
    }

    @PostConstruct
    public void wireTickListener() {
        // Every LTP tick that touches a symbol the strategy tracks fires a
        // fresh state push to every connected /api/option-buying/stream
        // subscriber. Throttled to MIN_PUSH_INTERVAL_MS so heavy tick storms
        // don't flood the frontend.
        marketDataService.addLtpListener(t -> {
            if (t == null) return;
            String sym = t.fyersSymbol();
            if (sym == null) return;
            // Only care about ticks that could change the state — the chosen
            // legs' own symbols, and any tracked position via PositionManager.
            VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
            boolean interesting =
                (s != null && (sym.equals(s.getChosenCeSymbol()) || sym.equals(s.getChosenPeSymbol())))
                || PositionManager.getAllSymbols().contains(sym);
            if (!interesting) return;
            long now = System.currentTimeMillis();
            long prev = lastPushMs.get();
            if (now - prev < MIN_PUSH_INTERVAL_MS) return;
            if (!lastPushMs.compareAndSet(prev, now)) return;
            pushStateToSubscribers();
        });
    }

    private void pushStateToSubscribers() {
        if (stateEmitters.isEmpty()) return;
        Map<String, Object> payload;
        String json;
        try {
            payload = state();
            json = jsonMapper.writeValueAsString(payload);
        } catch (Exception e) {
            return;
        }
        for (SseEmitter em : stateEmitters) {
            try {
                em.send(SseEmitter.event().name("state").data(json, MediaType.APPLICATION_JSON));
            } catch (Exception ex) {
                stateEmitters.remove(em);
                try { em.complete(); } catch (Exception ignored) {}
            }
        }
    }

    /** SSE stream — pushes the same JSON /api/option-buying/state returns on
     *  every relevant LTP tick (throttled to 10 Hz) so the positions page
     *  updates tick-by-tick without polling. Initial snapshot sent
     *  immediately on connect. */
    @GetMapping(value = "/api/option-buying/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);   // no timeout
        stateEmitters.add(emitter);
        emitter.onCompletion(() -> stateEmitters.remove(emitter));
        emitter.onTimeout(()    -> { stateEmitters.remove(emitter); emitter.complete(); });
        emitter.onError(err     -> stateEmitters.remove(emitter));
        // Immediate snapshot so the UI populates before the first tick.
        try {
            String json = jsonMapper.writeValueAsString(state());
            emitter.send(SseEmitter.event().name("state").data(json, MediaType.APPLICATION_JSON));
        } catch (Exception ignored) {}
        return emitter;
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

    /** Reset the strategy FSM back to ARMED after a portfolio-risk force-close.
     *  Clears per-leg state and moves fsm out of DONE_FOR_DAY so bar-close
     *  evaluations resume against the (typically updated) portfolio limit.
     *  Chosen strikes are preserved. */
    @PostMapping("/api/option-buying/reset-strategy")
    public Map<String, Object> resetStrategy(@RequestBody(required = false) Map<String, Object> body) {
        VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
        if (s == null) return Map.of("ok", false, "reason", "strategy unavailable");
        String reason = body != null && body.get("reason") != null
            ? body.get("reason").toString() : "MANUAL_RESET";
        s.resetToIdle(reason);
        return Map.of("ok", true, "state", s.currentState());
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
                // Compute MTM using the STRATEGY'S current-cycle fillPrice when
                // available (from getLegSnapshot). Fyers's netAvg blends across
                // all buys of the day for the symbol, so on a re-entry after a
                // full close the netAvg smears both cycles together and MTM
                // becomes offset from the strategy's own view. Using the
                // per-leg fillPrice keeps MTM aligned with liveNetPnlToday()
                // (the header P&L) and with the current-cycle economics.
                double costBasis = p.getAvgPrice();
                if (s != null) {
                    Map<String, Object> leg0 = s.getLegSnapshot(p.getSymbol());
                    Object legEntry = leg0.get("entryPrice");
                    if (legEntry instanceof Number && ((Number) legEntry).doubleValue() > 0) {
                        costBasis = ((Number) legEntry).doubleValue();
                    }
                }
                double pnl = "LONG".equals(p.getSide())
                    ? (p.getLtp() - costBasis) * p.getQty()
                    : (costBasis - p.getLtp()) * p.getQty();
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
                        Object setup = leg.get("setup");
                        Object side  = leg.get("side");
                        if (entry instanceof Number && ((Number) entry).doubleValue() > 0) m.put("entryPrice", entry);
                        if (sl    instanceof Number && ((Number) sl).doubleValue()    > 0) m.put("slPrice",     sl);
                        if (tgt   instanceof Number && ((Number) tgt).doubleValue()   > 0) m.put("targetLevel", tgt);
                        // Prefer the full pathway+side setup label (e.g. 'VWAP_BREAKOUT CE');
                        // fall back to just the side if pathway isn't populated yet.
                        if (setup != null)      m.put("setup", setup);
                        else if (side != null)  m.put("setup", side);
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
