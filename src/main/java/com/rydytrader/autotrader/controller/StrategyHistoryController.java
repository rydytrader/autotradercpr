package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.entity.StrategySessionEntity;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategySessionRepository;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.strategy.Strategy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.ZoneId;
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

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final StrategySessionRepository repo;
    private final StrategyTradeRepository tradeRepo;
    private final ObjectProvider<Strategy> strategyProvider;

    public StrategyHistoryController(StrategySessionRepository repo,
                                     StrategyTradeRepository tradeRepo,
                                     ObjectProvider<Strategy> strategyProvider) {
        this.repo = repo;
        this.tradeRepo = tradeRepo;
        this.strategyProvider = strategyProvider;
    }

    @GetMapping("/api/strategies/{id}/history")
    public ResponseEntity<Map<String, Object>> list(@PathVariable String id) {
        List<StrategySessionEntity> rows = repo.findByStrategyIdOrderBySessionDateDesc(id);
        Map<String, Map<String, Object>> byDate = new LinkedHashMap<>();
        double totalGross = 0, totalCharges = 0, totalNet = 0;
        int totalRolls = 0;
        // 1. Legacy session rows take precedence per date (older straddle/strangle data).
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
            m.put("wins",             0);
            m.put("losses",           0);
            byDate.put(s.getSessionDate(), m);
            totalGross   += s.getGrossPnl();
            totalCharges += s.getCharges();
            totalNet     += s.getNetPnl();
            totalRolls   += s.getRolls();
        }
        // 2. Camarilla (and any other cycle-based strategies) persist per-cycle trade rows
        // instead of session rows. Aggregate those by sessionDate into session-shaped maps
        // so the calendar's yearly view + day modal see real data. ONLY emit a row for
        // dates that don't already have a legacy session row, so we don't double-count.
        List<StrategyTradeEntity> tradeRows = tradeRepo.findByStrategyIdOrderByClosedAtMillisAsc(id);
        Map<String, double[]> tradeAggByDate = new LinkedHashMap<>();   // date → [gross, charges, net, wins, losses, count]
        for (StrategyTradeEntity t : tradeRows) {
            String d = t.getSessionDate();
            if (d == null || d.isBlank()) continue;
            if (byDate.containsKey(d)) continue;                       // legacy session wins
            double[] agg = tradeAggByDate.computeIfAbsent(d, k -> new double[6]);
            agg[0] += t.getGrossPnl();
            agg[1] += t.getCharges();
            agg[2] += t.getNetPnl();
            if (t.getNetPnl() > 0)      agg[3]++;
            else if (t.getNetPnl() < 0) agg[4]++;
            agg[5]++;
        }
        for (Map.Entry<String, double[]> e : tradeAggByDate.entrySet()) {
            double[] a = e.getValue();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("date",            e.getKey());
            m.put("entries",         (int) a[5]);                       // count of trades that day
            m.put("rolls",           0);
            m.put("finalState",      "");
            m.put("niftyOpen",       0.0);
            m.put("niftyHigh",       0.0);
            m.put("niftyLow",        0.0);
            m.put("niftyClose",      0.0);
            m.put("premiumCollected", 0.0);
            m.put("premiumPaidBack",  0.0);
            m.put("grossPnl",         round(a[0]));
            m.put("charges",          round(a[1]));
            m.put("netPnl",           round(a[2]));
            m.put("wins",             (int) a[3]);
            m.put("losses",           (int) a[4]);
            byDate.put(e.getKey(), m);
            totalGross   += a[0];
            totalCharges += a[1];
            totalNet     += a[2];
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("strategyId",   id);
        out.put("sessions",     new ArrayList<>(byDate.values()));
        out.put("totalGross",   round(totalGross));
        out.put("totalCharges", round(totalCharges));
        out.put("totalNet",     round(totalNet));
        out.put("totalRolls",   totalRolls);
        out.put("sessionCount", byDate.size());
        return ResponseEntity.ok(out);
    }

    /** Per-trade rows for a strategy on a specific date — drives the calendar's day-detail
     *  modal, which lists every closed cycle on that day. For today's rows persisted before
     *  the symbol column was added, the value is backfilled from the strategy's in-memory
     *  ring buffer (matched by {@code closedAtMillis} within a 5-second tolerance window). */
    @GetMapping("/api/strategies/{id}/trades")
    public ResponseEntity<Map<String, Object>> tradesForDate(@PathVariable String id,
                                                             @RequestParam String date) {
        List<StrategyTradeEntity> rows =
            tradeRepo.findByStrategyIdAndSessionDateOrderByClosedAtMillisAsc(id, date);
        LiveBackfill backfill = liveBackfillForDate(id, date);
        List<Map<String, Object>> trades = new ArrayList<>();
        for (StrategyTradeEntity t : rows) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id",             t.getId());
            m.put("strategyId",     t.getStrategyId());
            m.put("symbol",         backfill.lookup(backfill.symbols, t.getSymbol(), t.getClosedAtMillis()));
            m.put("setup",          backfill.lookup(backfill.setups, t.getSetup(),  t.getClosedAtMillis()));
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

    /** Collects symbol and setup maps (keyed by closedAtMillis) from the strategy's in-memory
     *  ring buffer for today, so legacy DB rows persisted before those columns existed can
     *  be backfilled when surfaced to the calendar's day modal. */
    private LiveBackfill liveBackfillForDate(String strategyId, String date) {
        LiveBackfill out = new LiveBackfill();
        if (date == null || !date.equals(LocalDate.now(IST).toString())) return out;
        Strategy strat = strategyProvider.getIfAvailable();
        if (strat == null || !strategyId.equals(strat.id())) return out;
        for (Map<String, Object> m : strat.todayClosedTrades()) {
            Object ts = m.get("closedAtMillis");
            if (!(ts instanceof Number)) continue;
            long ms = ((Number) ts).longValue();
            Object sym = m.get("symbol");
            Object setup = m.get("setup");
            if (sym   != null) out.symbols.put(ms, String.valueOf(sym));
            if (setup != null) out.setups.put(ms,  String.valueOf(setup));
        }
        return out;
    }

    private static final class LiveBackfill {
        final Map<Long, String> symbols = new LinkedHashMap<>();
        final Map<Long, String> setups  = new LinkedHashMap<>();

        /** Returns {@code dbValue} when present; otherwise the closest in-memory entry within
         *  5 seconds of {@code closedAt}; otherwise {@code null}. */
        String lookup(Map<Long, String> ring, String dbValue, long closedAt) {
            if (dbValue != null && !dbValue.isBlank()) return dbValue;
            if (ring.isEmpty()) return null;
            final long WINDOW_MS = 5_000L;
            for (Map.Entry<Long, String> e : ring.entrySet()) {
                if (Math.abs(e.getKey() - closedAt) <= WINDOW_MS) return e.getValue();
            }
            return null;
        }
    }
}
