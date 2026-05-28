package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.entity.StraddleSessionEntity;
import com.rydytrader.autotrader.repository.StraddleSessionRepository;
import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.service.strategy.StrategyRegistry;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes the Analytics Home page metrics from the {@code straddle_sessions} table. One row
 * per (strategy, trading day), produced by each strategy on day rollover. All time-bucketing +
 * filtering is done in-memory — the operator generally has a year or two of daily rows, so the
 * full dataset is trivially small to load and process.
 *
 * <p>Period filters mirror the UI:
 * <ul>
 *   <li>{@code today}  — today's session(s) only</li>
 *   <li>{@code expiry} — sessions in the current weekly-options cycle (day after last expiry → today)</li>
 *   <li>{@code mtd}    — first day of current calendar month onwards</li>
 *   <li>{@code ytd}    — Apr 1 of current Indian financial year onwards (FY starts Apr 1)</li>
 *   <li>{@code all}    — every row</li>
 * </ul>
 *
 * <p>Strategy scope filters by {@code strategyId}; null/blank/"all" → all strategies aggregated.
 *
 * <p>Explicit date range: when {@code from} and/or {@code to} are passed, they override
 * the period preset. Use them to scope to a specific calendar month, a specific year, or any
 * custom window. Either bound may be omitted (open-ended in that direction).
 */
@Service
public class AnalyticsService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final StraddleSessionRepository sessionRepo;
    private final RiskSettingsStore riskSettings;
    private final StrategyRegistry strategyRegistry;

    public AnalyticsService(StraddleSessionRepository sessionRepo,
                            RiskSettingsStore riskSettings,
                            StrategyRegistry strategyRegistry) {
        this.sessionRepo = sessionRepo;
        this.riskSettings = riskSettings;
        this.strategyRegistry = strategyRegistry;
    }

    /** Composite payload for the Analytics Home page. Combines the four hero panes and the
     *  equity curve into a single response so the client only makes one round-trip per selector
     *  change. */
    public Map<String, Object> summary(String period, String strategyId, String from, String to) {
        List<StraddleSessionEntity> rows = filterRows(period, strategyId, from, to);
        double startingCapital = riskSettings.getStartingCapital();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("period",        period);
        out.put("strategyId",    strategyId);
        out.put("from",          from);
        out.put("to",            to);
        out.put("sessionCount",  rows.size());

        out.put("capital",     capital(rows, startingCapital));
        out.put("performance", performance(rows));
        out.put("extremes",    extremes(rows));
        out.put("streaks",     streaks(rows));
        out.put("edge",        edge(rows, startingCapital));
        out.put("equityCurve", equityCurve(rows, startingCapital));
        return out;
    }

    /** Apply the period + strategy filters. Returns rows in chronological order (oldest first)
     *  so cumulative + streak calculations don't have to re-sort.
     *
     *  <p>If {@code from} or {@code to} are non-blank ISO dates, they override the period preset.
     *  Either bound may be missing — that bound is treated as open-ended. */
    private List<StraddleSessionEntity> filterRows(String period, String strategyId,
                                                    String from, String to) {
        List<StraddleSessionEntity> all = sessionRepo.findAll();
        LocalDate today = LocalDate.now(IST);
        LocalDate rangeFrom = parseIso(from);
        LocalDate rangeTo   = parseIso(to);
        boolean explicitRange = rangeFrom != null || rangeTo != null;
        // Period preset only applies when no explicit range was passed.
        LocalDate cutoff = explicitRange ? null : switch (period == null ? "all" : period.toLowerCase()) {
            case "today"  -> today;
            case "expiry" -> currentExpiryStart(today);
            case "ytd"    -> indianFinancialYearStart(today);
            case "mtd"    -> LocalDate.of(today.getYear(), today.getMonthValue(), 1);
            default       -> null; // all-time
        };
        boolean allStrategies = strategyId == null || strategyId.isBlank() || "all".equalsIgnoreCase(strategyId);
        List<StraddleSessionEntity> filtered = new ArrayList<>();
        for (StraddleSessionEntity s : all) {
            if (!allStrategies && !strategyId.equals(s.getStrategyId())) continue;
            LocalDate d;
            try { d = LocalDate.parse(s.getSessionDate()); }
            catch (Exception ignored) { continue; }
            if (cutoff != null && d.isBefore(cutoff)) continue;
            if (rangeFrom != null && d.isBefore(rangeFrom)) continue;
            if (rangeTo   != null && d.isAfter(rangeTo))    continue;
            filtered.add(s);
        }
        filtered.sort(Comparator.comparing(StraddleSessionEntity::getSessionDate));
        return filtered;
    }

    private static LocalDate parseIso(String s) {
        if (s == null || s.isBlank()) return null;
        try { return LocalDate.parse(s.trim()); } catch (Exception e) { return null; }
    }

    /** Start of the current Indian financial year — April 1. If today is Jan/Feb/Mar, the FY
     *  started on Apr 1 of the previous calendar year; otherwise Apr 1 of the current year. */
    private static LocalDate indianFinancialYearStart(LocalDate today) {
        int fyYear = today.getMonthValue() >= 4 ? today.getYear() : today.getYear() - 1;
        return LocalDate.of(fyYear, 4, 1);
    }

    /** Resolve the start date of the current weekly-options cycle: the day AFTER the most recent
     *  expiry. Reads each strategy's cached {@code currentWeeklyExpiry} (next upcoming expiry,
     *  ISO yyyy-MM-dd); the previous expiry is exactly 7 days before that. Falls back to
     *  {@code today.minusDays(7)} when no strategy has resolved an expiry yet. */
    private LocalDate currentExpiryStart(LocalDate today) {
        if (strategyRegistry != null) {
            for (Strategy s : strategyRegistry.all()) {
                String exp = s.currentWeeklyExpiry();
                if (exp == null || exp.isEmpty()) continue;
                try {
                    LocalDate expiry = LocalDate.parse(exp);
                    return expiry.minusDays(7).plusDays(1); // first day of current cycle
                } catch (Exception ignored) {}
            }
        }
        return today.minusDays(7);
    }

    // ── Pane 1: CAPITAL ──────────────────────────────────────────────────────
    private Map<String, Object> capital(List<StraddleSessionEntity> rows, double starting) {
        double netSum = rows.stream().mapToDouble(StraddleSessionEntity::getNetPnl).sum();
        double current = starting + netSum;
        double returnPct = starting > 0 ? (netSum / starting) * 100.0 : 0;

        // Average monthly return % — group by YYYY-MM, sum netPnl, divide by starting, average.
        Map<String, Double> byMonth = new LinkedHashMap<>();
        for (StraddleSessionEntity s : rows) {
            String ym = s.getSessionDate() != null && s.getSessionDate().length() >= 7
                ? s.getSessionDate().substring(0, 7) : "";
            if (ym.isEmpty()) continue;
            byMonth.merge(ym, s.getNetPnl(), Double::sum);
        }
        double avgMonthlyPct = 0;
        if (!byMonth.isEmpty() && starting > 0) {
            double sumMonthlyPct = 0;
            for (Double m : byMonth.values()) sumMonthlyPct += (m / starting) * 100.0;
            avgMonthlyPct = sumMonthlyPct / byMonth.size();
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("startingCapital", round2(starting));
        m.put("currentCapital",  round2(current));
        m.put("totalReturn",     round2(netSum));
        m.put("totalReturnPct",  round2(returnPct));
        m.put("avgMonthlyPct",   round2(avgMonthlyPct));
        return m;
    }

    // ── Pane 2: PERFORMANCE ──────────────────────────────────────────────────
    private Map<String, Object> performance(List<StraddleSessionEntity> rows) {
        int wins = 0, losses = 0;
        double grossProfit = 0, grossLoss = 0;
        double sumWin = 0, sumLoss = 0;
        for (StraddleSessionEntity s : rows) {
            double pnl = s.getNetPnl();
            if (pnl > 0)      { wins++;   grossProfit += pnl; sumWin  += pnl; }
            else if (pnl < 0) { losses++; grossLoss   += pnl; sumLoss += pnl; }
        }
        int total = rows.size();
        double winRate      = total > 0 ? (wins / (double) total) * 100.0 : 0;
        double profitFactor = grossLoss < 0 ? grossProfit / Math.abs(grossLoss) : 0;
        double avgWin       = wins > 0 ? sumWin / wins : 0;
        double avgLoss      = losses > 0 ? sumLoss / losses : 0;
        double riskReward   = avgLoss < 0 ? avgWin / Math.abs(avgLoss) : 0;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sessions",     total);
        m.put("wins",         wins);
        m.put("losses",       losses);
        m.put("winRate",      round2(winRate));
        m.put("profitFactor", round2(profitFactor));
        m.put("riskReward",   round2(riskReward));
        return m;
    }

    // ── Pane 3: P&L EXTREMES ─────────────────────────────────────────────────
    private Map<String, Object> extremes(List<StraddleSessionEntity> rows) {
        double maxProfit = 0, maxLoss = 0;
        double sumWin = 0, sumLoss = 0;
        int wins = 0, losses = 0;
        for (StraddleSessionEntity s : rows) {
            double pnl = s.getNetPnl();
            if (pnl > maxProfit) maxProfit = pnl;
            if (pnl < maxLoss)   maxLoss   = pnl;
            if (pnl > 0)      { wins++;   sumWin  += pnl; }
            else if (pnl < 0) { losses++; sumLoss += pnl; }
        }
        // Max drawdown — largest peak-to-trough decline in the running cumulative.
        double peak = 0, cum = 0, maxDd = 0;
        for (StraddleSessionEntity s : rows) {
            cum += s.getNetPnl();
            if (cum > peak) peak = cum;
            double dd = cum - peak; // <= 0
            if (dd < maxDd) maxDd = dd;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("maxProfit", round2(maxProfit));
        m.put("maxLoss",   round2(maxLoss));
        m.put("avgProfit", round2(wins   > 0 ? sumWin  / wins   : 0));
        m.put("avgLoss",   round2(losses > 0 ? sumLoss / losses : 0));
        m.put("maxDrawdown", round2(maxDd));
        return m;
    }

    // ── Pane 4: STREAKS ──────────────────────────────────────────────────────
    private Map<String, Object> streaks(List<StraddleSessionEntity> rows) {
        int curWin = 0, curLoss = 0;
        int longestWin = 0, longestLoss = 0;
        double totalCharges = 0;
        for (StraddleSessionEntity s : rows) {
            double pnl = s.getNetPnl();
            totalCharges += s.getCharges();
            if (pnl > 0) {
                curWin++;
                curLoss = 0;
                if (curWin > longestWin) longestWin = curWin;
            } else if (pnl < 0) {
                curLoss++;
                curWin = 0;
                if (curLoss > longestLoss) longestLoss = curLoss;
            } else {
                // Zero P&L → break both streaks but don't extend either.
                curWin = 0; curLoss = 0;
            }
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("currentWinStreak",  curWin);
        m.put("longestWinStreak",  longestWin);
        m.put("currentLossStreak", curLoss);
        m.put("longestLossStreak", longestLoss);
        m.put("totalCharges",      round2(totalCharges));
        return m;
    }

    // ── Edge metrics ─────────────────────────────────────────────────────────
    /** Risk-adjusted performance numbers — used to judge whether the bot has a real,
     *  persistent edge vs random P&L noise.
     *  <ul>
     *    <li><b>Expectancy</b> per session — mean net P&L. {@code totalNetPnl / N}.</li>
     *    <li><b>Recovery Factor</b> — {@code totalNetPnl / |maxDrawdown|}. >1 means the
     *        strategy has recovered any drawdown it incurred; higher = more cushion.</li>
     *    <li><b>Sharpe (annualised)</b> — {@code mean(dailyReturn) / stddev(dailyReturn) ×
     *        sqrt(252)}, where dailyReturn = sessionNetPnl / startingCapital. >1 is good,
     *        >2 is excellent for an options-selling strategy.</li>
     *  </ul>
     */
    private Map<String, Object> edge(List<StraddleSessionEntity> rows, double starting) {
        int n = rows.size();
        Map<String, Object> m = new LinkedHashMap<>();
        if (n == 0) {
            m.put("expectancy",     0.0);
            m.put("recoveryFactor", 0.0);
            m.put("sharpe",         0.0);
            return m;
        }
        double netSum = 0;
        for (StraddleSessionEntity s : rows) netSum += s.getNetPnl();
        double expectancy = netSum / n;

        // Recovery factor needs max drawdown — same calc as extremes(), recomputed here so the
        // method is self-contained.
        double peak = 0, cum = 0, maxDd = 0;
        for (StraddleSessionEntity s : rows) {
            cum += s.getNetPnl();
            if (cum > peak) peak = cum;
            double dd = cum - peak;
            if (dd < maxDd) maxDd = dd;
        }
        double recoveryFactor = maxDd < 0 ? netSum / Math.abs(maxDd) : 0;

        // Annualised Sharpe — treat each session as one day, dailyReturn = netPnl / startingCapital.
        double sharpe = 0;
        if (n >= 2 && starting > 0) {
            double[] returns = new double[n];
            double mean = 0;
            for (int i = 0; i < n; i++) {
                returns[i] = rows.get(i).getNetPnl() / starting;
                mean += returns[i];
            }
            mean /= n;
            double sq = 0;
            for (double r : returns) sq += (r - mean) * (r - mean);
            double stddev = Math.sqrt(sq / n);
            if (stddev > 0) {
                sharpe = (mean / stddev) * Math.sqrt(252);
            }
        }

        m.put("expectancy",     round2(expectancy));
        m.put("recoveryFactor", round2(recoveryFactor));
        m.put("sharpe",         round2(sharpe));
        return m;
    }

    // ── Equity curve ─────────────────────────────────────────────────────────
    private Map<String, Object> equityCurve(List<StraddleSessionEntity> rows, double starting) {
        List<String> labels = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        double cum = starting;
        for (StraddleSessionEntity s : rows) {
            cum += s.getNetPnl();
            labels.add(s.getSessionDate());
            values.add(round2(cum));
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("labels", labels);
        m.put("values", values);
        return m;
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
}
