package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.entity.StrategySessionEntity;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategySessionRepository;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-strategy history endpoints. The {@code strategyId} path variable now resolves to a
 * fixed value ({@code "camarilla"}) since Camarilla is the only strategy; the
 * {@code /api/strategies/{id}/*} URL pattern is kept so the calendar's day-detail modal
 * can stay strategy-agnostic.
 */
@RestController
public class StrategyHistoryController {

    private final StrategySessionRepository repo;
    private final StrategyTradeRepository tradeRepo;

    public StrategyHistoryController(StrategySessionRepository repo,
                                     StrategyTradeRepository tradeRepo) {
        this.repo = repo;
        this.tradeRepo = tradeRepo;
    }

    @GetMapping("/api/strategies/{id}/history")
    public ResponseEntity<Map<String, Object>> list(@PathVariable String id) {
        List<StrategySessionEntity> rows = repo.findByStrategyIdOrderBySessionDateDesc(id);
        List<Map<String, Object>> sessions = new ArrayList<>();
        double totalGross = 0, totalCharges = 0, totalNet = 0;
        int totalRolls = 0;
        for (StrategySessionEntity s : rows) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("date",            s.getSessionDate());
            m.put("entries",         s.getEntries());
            m.put("rolls",           s.getRolls());
            m.put("finalState",      s.getFinalState());
            m.put("niftyOpen",       s.getNiftyOpen());
            m.put("niftyHigh",       s.getNiftyHigh());
            m.put("niftyLow",        s.getNiftyLow());
            m.put("niftyClose",      s.getNiftyClose());
            m.put("premiumCollected", round(s.getPremiumCollected()));
            m.put("premiumPaidBack",  round(s.getPremiumPaidBack()));
            m.put("grossPnl",         round(s.getGrossPnl()));
            m.put("charges",          round(s.getCharges()));
            m.put("netPnl",           round(s.getNetPnl()));
            sessions.add(m);
            totalGross   += s.getGrossPnl();
            totalCharges += s.getCharges();
            totalNet     += s.getNetPnl();
            totalRolls   += s.getRolls();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("strategyId",   id);
        out.put("sessions",     sessions);
        out.put("totalGross",   round(totalGross));
        out.put("totalCharges", round(totalCharges));
        out.put("totalNet",     round(totalNet));
        out.put("totalRolls",   totalRolls);
        out.put("sessionCount", sessions.size());
        return ResponseEntity.ok(out);
    }

    /** Per-trade rows for a strategy on a specific date — drives the calendar's day-detail
     *  modal, which lists every closed cycle on that day. */
    @GetMapping("/api/strategies/{id}/trades")
    public ResponseEntity<Map<String, Object>> tradesForDate(@PathVariable String id,
                                                             @RequestParam String date) {
        List<StrategyTradeEntity> rows =
            tradeRepo.findByStrategyIdAndSessionDateOrderByClosedAtMillisAsc(id, date);
        List<Map<String, Object>> trades = new ArrayList<>();
        for (StrategyTradeEntity t : rows) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id",             t.getId());
            m.put("strategyId",     t.getStrategyId());
            m.put("sessionDate",    t.getSessionDate());
            m.put("closedAtMillis", t.getClosedAtMillis());
            m.put("qty",            t.getQty());
            m.put("grossPnl",       round(t.getGrossPnl()));
            m.put("charges",        round(t.getCharges()));
            m.put("netPnl",         round(t.getNetPnl()));
            m.put("closeReason",    t.getCloseReason());
            m.put("slHitCount",     t.getSlHitCount() == null ? 0 : t.getSlHitCount());
            trades.add(m);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("strategyId", id);
        out.put("date",       date);
        out.put("trades",     trades);
        return ResponseEntity.ok(out);
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
