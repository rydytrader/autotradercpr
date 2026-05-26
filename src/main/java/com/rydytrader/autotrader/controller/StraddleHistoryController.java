package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.entity.StraddleSessionEntity;
import com.rydytrader.autotrader.repository.StraddleSessionRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * Read-only listing of past Rolling Straddle daily sessions. Backs the Straddle History page.
 * Rows are written by {@code RollingStraddleService} on day rollover.
 */
@RestController
public class StraddleHistoryController {

    private final StraddleSessionRepository repo;

    public StraddleHistoryController(StraddleSessionRepository repo) {
        this.repo = repo;
    }

    @GetMapping("/api/straddle/history")
    public Map<String, Object> list() {
        List<StraddleSessionEntity> rows = repo.findAllByOrderBySessionDateDesc();
        List<Map<String, Object>> sessions = new ArrayList<>();
        double totalGross = 0, totalCharges = 0, totalNet = 0;
        int totalRolls = 0;
        for (StraddleSessionEntity s : rows) {
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
        out.put("sessions",     sessions);
        out.put("totalGross",   round(totalGross));
        out.put("totalCharges", round(totalCharges));
        out.put("totalNet",     round(totalNet));
        out.put("totalRolls",   totalRolls);
        out.put("sessionCount", sessions.size());
        return out;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
