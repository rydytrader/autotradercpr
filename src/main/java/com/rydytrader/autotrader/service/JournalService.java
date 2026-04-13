package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.TradeRecord;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class JournalService {

    private static final String LOG_DIR     = "logs";
    private static final String FILE_PREFIX = "trades-history-";
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // ── LOAD TRADES FOR DATE RANGE ────────────────────────────────────────────
    public List<TradeRecord> loadTrades(LocalDate from, LocalDate to) {
        List<TradeRecord> all = new ArrayList<>();
        LocalDate cursor = from;
        while (!cursor.isAfter(to)) {
            all.addAll(loadDayTrades(cursor));
            cursor = cursor.plusDays(1);
        }
        Collections.reverse(all); // newest first
        return all;
    }

    private List<TradeRecord> loadDayTrades(LocalDate date) {
        List<TradeRecord> trades = new ArrayList<>();
        Path path = Paths.get(LOG_DIR, FILE_PREFIX + date.format(DATE_FMT) + ".csv");
        if (!Files.exists(path)) return trades;
        String dayPrefix = date.format(DATE_FMT) + " ";
        try {
            List<String> lines = Files.readAllLines(path);
            for (int i = 1; i < lines.size(); i++) {
                String[] p = lines.get(i).split(",");
                if (p.length < 7) continue;
                try {
                    // CSV stores time-only timestamps (HH:mm:ss). Prefix with the file's
                    // date so downstream code (computeMetrics dailyPnl/equity) can extract
                    // a yyyy-MM-dd day key via substring(0, 10).
                    String ts = p[0] != null && p[0].length() >= 10 ? p[0] : dayPrefix + p[0];
                    trades.add(new TradeRecord(ts, p[1], p[2],
                            Integer.parseInt(p[3]),
                            Double.parseDouble(p[4]),
                            Double.parseDouble(p[5]),
                            p[6]));
                } catch (Exception ignored) {}
            }
        } catch (IOException e) { e.printStackTrace(); }
        return trades;
    }

    // ── METRICS ───────────────────────────────────────────────────────────────
    public Map<String, Object> computeMetrics(List<TradeRecord> trades) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (trades.isEmpty()) { m.put("totalTrades", 0); return m; }

        // All metrics use NET P&L (after charges). Classification matches TradeRecord.result:
        // PROFIT (netPnl > 0), LOSS (netPnl < 0), BREAKEVEN (== 0). Breakeven trades excluded
        // from win/loss counts and from profit factor (industry standard).
        List<TradeRecord> wins   = trades.stream().filter(t -> "PROFIT".equals(t.getResult())).collect(Collectors.toList());
        List<TradeRecord> losses = trades.stream().filter(t -> "LOSS".equals(t.getResult())).collect(Collectors.toList());
        long breakeven = trades.stream().filter(t -> "BREAKEVEN".equals(t.getResult())).count();

        double grossProfit  = wins.stream().mapToDouble(TradeRecord::getNetPnl).sum();
        double grossLoss    = Math.abs(losses.stream().mapToDouble(TradeRecord::getNetPnl).sum());
        double netPnl       = trades.stream().mapToDouble(TradeRecord::getNetPnl).sum();
        double avgWin       = wins.isEmpty()   ? 0 : grossProfit / wins.size();
        double avgLoss      = losses.isEmpty() ? 0 : grossLoss   / losses.size();
        double winRate      = (double) wins.size() / trades.size() * 100;
        double profitFactor = grossLoss == 0 ? grossProfit : grossProfit / grossLoss;
        double expectancy   = (winRate / 100 * avgWin) - ((1 - winRate / 100) * avgLoss);
        double rr           = avgLoss == 0 ? 0 : avgWin / avgLoss;

        // Consecutive wins/losses — BREAKEVEN trades break both streaks
        int maxCW = 0, maxCL = 0, cw = 0, cl = 0;
        List<TradeRecord> chrono = new ArrayList<>(trades);
        Collections.reverse(chrono);
        for (TradeRecord t : chrono) {
            if      ("PROFIT".equals(t.getResult())) { cw++; cl = 0; maxCW = Math.max(maxCW, cw); }
            else if ("LOSS".equals(t.getResult()))   { cl++; cw = 0; maxCL = Math.max(maxCL, cl); }
            else                                      { cw = 0; cl = 0; }
        }

        m.put("totalTrades",     trades.size());
        m.put("wins",            wins.size());
        m.put("losses",          losses.size());
        m.put("breakeven",       breakeven);
        m.put("winRate",         round(winRate));
        m.put("netPnl",          round(netPnl));
        m.put("grossProfit",     round(grossProfit));
        m.put("grossLoss",       round(grossLoss));
        m.put("avgWin",          round(avgWin));
        m.put("avgLoss",         round(avgLoss));
        m.put("profitFactor",    round(profitFactor));
        m.put("expectancy",      round(expectancy));
        m.put("riskReward",      round(rr));
        m.put("maxConsecWins",   maxCW);
        m.put("maxConsecLosses", maxCL);
        m.put("bestTrade",       trades.stream().mapToDouble(TradeRecord::getNetPnl).max().orElse(0));
        m.put("worstTrade",      trades.stream().mapToDouble(TradeRecord::getNetPnl).min().orElse(0));

        // By exit reason
        Map<String, Map<String, Object>> byReason = new LinkedHashMap<>();
        for (String reason : List.of("SL", "TARGET", "MANUAL")) {
            List<TradeRecord> g = trades.stream().filter(t -> reason.equals(t.getExitReason())).collect(Collectors.toList());
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("count", g.size());
            r.put("pnl",   round(g.stream().mapToDouble(TradeRecord::getNetPnl).sum()));
            r.put("wins",  g.stream().filter(t -> t.getNetPnl() > 0).count());
            byReason.put(reason, r);
        }
        m.put("byReason", byReason);

        // By side
        Map<String, Map<String, Object>> bySide = new LinkedHashMap<>();
        for (String side : List.of("LONG", "SHORT")) {
            List<TradeRecord> g = trades.stream().filter(t -> side.equals(t.getSide())).collect(Collectors.toList());
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("count", g.size());
            r.put("pnl",   round(g.stream().mapToDouble(TradeRecord::getNetPnl).sum()));
            r.put("wins",  g.stream().filter(t -> t.getNetPnl() > 0).count());
            bySide.put(side, r);
        }
        m.put("bySide", bySide);

        // Daily P&L (bar chart) — handles both yyyy-MM-dd HH:mm:ss (full) and HH:mm:ss (live cache)
        Map<String, Double> dailyPnl = new LinkedHashMap<>();
        for (TradeRecord t : chrono) {
            String ts = t.getTimestamp();
            String day = ts != null && ts.length() >= 10 ? ts.substring(0, 10) : "today";
            dailyPnl.merge(day, t.getNetPnl(), Double::sum);
        }
        Map<String, Double> dailyRounded = new LinkedHashMap<>();
        dailyPnl.forEach((k, v) -> dailyRounded.put(k, round(v)));
        m.put("dailyPnl", dailyRounded);

        // Equity curve
        List<Map<String, Object>> equity = new ArrayList<>();
        double cum = 0;
        for (TradeRecord t : chrono) {
            cum += t.getNetPnl();
            String ts = t.getTimestamp();
            Map<String, Object> pt = new LinkedHashMap<>();
            pt.put("label", ts != null && ts.length() >= 10 ? ts.substring(0, 10) : "");
            pt.put("value", round(cum));
            equity.add(pt);
        }
        m.put("equityCurve", equity);

        return m;
    }

    // ── AVAILABLE DATE RANGE ──────────────────────────────────────────────────
    public Map<String, String> getAvailableDateRange() {
        Map<String, String> range = new LinkedHashMap<>();
        try {
            Path dir = Paths.get(LOG_DIR);
            if (!Files.exists(dir)) return range;
            List<Path> files = Files.list(dir)
                    .filter(p -> p.getFileName().toString().startsWith(FILE_PREFIX))
                    .sorted().collect(Collectors.toList());
            if (!files.isEmpty()) {
                range.put("earliest", files.get(0).getFileName().toString().replace(FILE_PREFIX, "").replace(".csv", ""));
                range.put("latest",   files.get(files.size()-1).getFileName().toString().replace(FILE_PREFIX, "").replace(".csv", ""));
            }
        } catch (IOException e) { e.printStackTrace(); }
        return range;
    }

    private double round(double v) { return Math.round(v * 100.0) / 100.0; }
}