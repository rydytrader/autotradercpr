package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.entity.StrategySessionEntity;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategySessionRepository;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.service.strategy.StrategyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Global trade-history maintenance — backs the DATA tab in the Settings modal.
 *
 * <ul>
 *   <li>{@code GET  /api/trade-data/stats} — row counts + date range, for the pane's summary.</li>
 *   <li>{@code POST /api/trade-data/clear} — wipes {@code strategy_trades} + {@code strategy_sessions}
 *       for EVERY instance. DB rows only: today's in-memory counters are untouched and roll over
 *       at the normal day boundary. Refused while any instance has an open leg, and requires the
 *       literal body {@code {"confirm":"DELETE"}} so a stray click can't do it.</li>
 * </ul>
 */
@RestController
public class TradeDataController {

    private static final Logger log = LoggerFactory.getLogger(TradeDataController.class);
    private static final Set<String> OPEN_STATES = Set.of("OPEN_BOTH", "OPEN_CE_ONLY", "OPEN_PE_ONLY");

    private final StrategyTradeRepository   tradeRepo;
    private final StrategySessionRepository sessionRepo;
    private final StrategyRegistry          registry;
    private final EventService              eventService;

    public TradeDataController(StrategyTradeRepository tradeRepo,
                               StrategySessionRepository sessionRepo,
                               StrategyRegistry registry,
                               EventService eventService) {
        this.tradeRepo    = tradeRepo;
        this.sessionRepo  = sessionRepo;
        this.registry     = registry;
        this.eventService = eventService;
    }

    @GetMapping("/api/trade-data/stats")
    public Map<String, Object> stats() {
        List<StrategyTradeEntity>   trades   = tradeRepo.findAllByOrderByClosedAtMillisAsc();
        List<StrategySessionEntity> sessions = sessionRepo.findAllByOrderBySessionDateDesc();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("tradeRows",   trades.size());
        out.put("sessionRows", sessions.size());
        out.put("firstDate",   trades.isEmpty()   ? null : trades.get(0).getSessionDate());
        out.put("lastDate",    sessions.isEmpty() ? null : sessions.get(0).getSessionDate());
        out.put("openLegs",    openLegInstances());
        return out;
    }

    @PostMapping("/api/trade-data/clear")
    @Transactional
    public ResponseEntity<Map<String, Object>> clear(@RequestBody(required = false) Map<String, Object> body) {
        Map<String, Object> out = new LinkedHashMap<>();
        String confirm = body == null ? "" : String.valueOf(body.getOrDefault("confirm", ""));
        if (!"DELETE".equals(confirm)) {
            out.put("success", false);
            out.put("message", "Confirmation missing — body must be {\"confirm\":\"DELETE\"}.");
            return ResponseEntity.badRequest().body(out);
        }
        List<String> open = openLegInstances();
        if (!open.isEmpty()) {
            out.put("success", false);
            out.put("message", "Refused — open legs on " + String.join(", ", open) + ". Square off first.");
            out.put("openLegs", open);
            return ResponseEntity.status(409).body(out);
        }
        long trades   = tradeRepo.count();
        long sessions = sessionRepo.count();
        tradeRepo.deleteAllInBatch();
        sessionRepo.deleteAllInBatch();
        // Also zero today's in-memory counters on every instance — otherwise the analytics
        // live overlay keeps re-synthesising today from realisedPnlToday / the events ring,
        // and persistSessionFor() quietly re-creates today's session row.
        List<String> reset = new ArrayList<>();
        for (Strategy s : registry.all()) {
            try { if (s.resetTodayCounters()) reset.add(s.id()); }
            catch (Exception e) { log.warn("[trade-data] resetTodayCounters({}) failed: {}", s.id(), e.getMessage()); }
        }
        String msg = "Trade history CLEARED — " + trades + " trade rows + " + sessions
            + " session rows deleted (all instances); today's counters reset on " + reset.size() + " instance(s).";
        log.warn("[trade-data] {}", msg);
        eventService.log("[WARNING] [trade-data] " + msg);
        out.put("success", true);
        out.put("message", msg);
        out.put("tradeRowsDeleted",   trades);
        out.put("sessionRowsDeleted", sessions);
        return ResponseEntity.ok(out);
    }

    private List<String> openLegInstances() {
        List<String> open = new ArrayList<>();
        for (Strategy s : registry.all()) {
            String st = s.currentState();
            if (st != null && OPEN_STATES.contains(st)) open.add(s.id());
        }
        return open;
    }
}
