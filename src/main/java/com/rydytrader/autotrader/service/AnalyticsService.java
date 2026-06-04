package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.entity.StraddleTradeEntity;
import com.rydytrader.autotrader.repository.StraddleTradeRepository;
import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.service.strategy.StrategyRegistry;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Powers the Analytics Home page. All metrics are computed per-straddle from the
 * {@code straddle_trades} table — one row per individual short-straddle cycle. The
 * {@code leg-sl} strategy writes one row per day (when the straddle reaches DONE_FOR_DAY).
 *
 * <p>Period filters (UI):
 * <ul>
 *   <li>{@code today}  — today's straddles only</li>
 *   <li>{@code expiry} — straddles closed in the current weekly-options cycle (day after last
 *       expiry through today)</li>
 *   <li>{@code mtd}    — first day of current calendar month onwards</li>
 *   <li>{@code ytd}    — Apr 1 of current Indian FY onwards</li>
 *   <li>{@code all}    — every trade</li>
 * </ul>
 *
 * <p>Strategy scope filters by {@code strategyId}; null/blank/"all" → all strategies aggregated.
 *
 * <p>Explicit date range: {@code from} and/or {@code to} (ISO yyyy-MM-dd) override the period
 * preset. Used by the Year + Month picker.
 *
 * <p>Live overlay: when the filter window includes today, today's in-progress closes are pulled
 * from each strategy's in-memory recent-events buffer via {@link Strategy#todayClosedTrades()}
 * so today's metrics reflect what's been closed so far instead of waiting for the day-end row.
 */
@Service
public class AnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final StraddleTradeRepository tradeRepo;
    private final RiskSettingsStore riskSettings;
    private final StrategyRegistry strategyRegistry;
    private final org.springframework.beans.factory.ObjectProvider<com.rydytrader.autotrader.service.ManualTerminalService> manualTerminalProvider;

    public AnalyticsService(StraddleTradeRepository tradeRepo,
                            RiskSettingsStore riskSettings,
                            StrategyRegistry strategyRegistry,
                            org.springframework.beans.factory.ObjectProvider<com.rydytrader.autotrader.service.ManualTerminalService> manualTerminalProvider) {
        this.tradeRepo = tradeRepo;
        this.riskSettings = riskSettings;
        this.strategyRegistry = strategyRegistry;
        this.manualTerminalProvider = manualTerminalProvider;
    }

    /** Composite payload — one round-trip serves all four hero tiles, the four detail cards,
     *  and the equity curve. */
    public Map<String, Object> summary(String period, String strategyId, String from, String to) {
        return summary(period, strategyId, from, to, true);
    }

    /** Same as {@link #summary(String, String, String, String)} but with explicit
     *  {@code includeAdjustments} control for the home analytics' "Include adjustments
     *  in P&L" checkbox. When true, manual-terminal closed trades sum into the money
     *  tiles (Total Return, Current Capital, Avg Monthly %, Total Charges) and the
     *  equity curve; strategy-stats tiles stay strategy-pure regardless. */
    public Map<String, Object> summary(String period, String strategyId, String from, String to,
                                       boolean includeAdjustments) {
        List<Trade> trades = loadTrades(period, strategyId, from, to);
        List<Trade> closed = new ArrayList<>();
        for (Trade t : trades) if (isClosedStraddle(t)) closed.add(t);
        double startingCapital = riskSettings.getStartingCapital();

        // Filter manual-terminal closed trades to the same date window as the straddle filter.
        List<com.rydytrader.autotrader.store.manual.ManualClosedTrade> adjustments =
            loadAdjustments(period, from, to);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("period",        period);
        out.put("strategyId",    strategyId);
        out.put("from",          from);
        out.put("to",            to);
        out.put("straddleCount", closed.size());
        out.put("sessionCount",  distinctDates(closed));
        out.put("includeAdjustments", includeAdjustments);

        out.put("capital",     capital(trades, startingCapital,
                                       includeAdjustments ? adjustments : java.util.List.of()));
        out.put("performance", performance(closed));
        out.put("extremes",    extremes(closed));
        out.put("streaks",     streaks(closed, includeAdjustments ? adjustments : java.util.List.of()));
        out.put("edge",        edge(closed, startingCapital));
        out.put("equityCurve", equityCurve(trades, startingCapital,
                                           includeAdjustments ? adjustments : java.util.List.of()));
        out.put("adjustments", adjustmentSummary(adjustments));
        out.put("byMonth",     byMonth(closed));
        return out;
    }

    /** Per-month aggregation of straddle outcomes — keyed by {@code yyyy-MM} so the calendar's
     *  year-grid month cards (and any other per-month consumer) can read true per-straddle
     *  wins / losses / winRate instead of bucketing daily session rows. NetPnl is also
     *  reported for symmetry, though daily summing already gets that right. */
    private Map<String, Object> byMonth(List<Trade> closed) {
        java.util.NavigableMap<String, int[]>    winLossByKey = new java.util.TreeMap<>();
        java.util.NavigableMap<String, double[]> netByKey     = new java.util.TreeMap<>();
        for (Trade t : closed) {
            String date = t.sessionDate();
            if (date == null || date.length() < 7) continue;
            String key  = date.substring(0, 7);
            double pnl  = t.netPnl();
            int[]    wl = winLossByKey.computeIfAbsent(key, k -> new int[2]);
            double[] ns = netByKey.computeIfAbsent(key, k -> new double[1]);
            if      (pnl > 0) wl[0]++;
            else if (pnl < 0) wl[1]++;
            ns[0] += pnl;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (String key : winLossByKey.keySet()) {
            int[] wl = winLossByKey.get(key);
            int total = wl[0] + wl[1];
            double winRate = total > 0 ? (wl[0] * 100.0 / total) : 0;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("straddles", total);
            m.put("wins",      wl[0]);
            m.put("losses",    wl[1]);
            m.put("winRate",   round2(winRate));
            m.put("netPnl",    round2(netByKey.get(key)[0]));
            out.put(key, m);
        }
        return out;
    }

    /** Pull the manual terminal's closed trades that fall within the requested filter window.
     *  Defensive: if {@link com.rydytrader.autotrader.service.ManualTerminalService} isn't
     *  on the classpath yet / bean creation failed, return an empty list. */
    private List<com.rydytrader.autotrader.store.manual.ManualClosedTrade> loadAdjustments(
            String period, String from, String to) {
        com.rydytrader.autotrader.service.ManualTerminalService svc =
            manualTerminalProvider == null ? null : manualTerminalProvider.getIfAvailable();
        if (svc == null) return java.util.List.of();
        LocalDate today = LocalDate.now(IST);
        LocalDate rangeFrom = parseIso(from);
        LocalDate rangeTo   = parseIso(to);
        boolean explicitRange = rangeFrom != null || rangeTo != null;
        LocalDate cutoff = explicitRange ? null : switch (period == null ? "all" : period.toLowerCase()) {
            case "today"  -> today;
            case "expiry" -> currentExpiryStart(today);
            case "ytd"    -> indianFinancialYearStart(today);
            case "mtd"    -> LocalDate.of(today.getYear(), today.getMonthValue(), 1);
            default       -> null;
        };
        List<com.rydytrader.autotrader.store.manual.ManualClosedTrade> out = new ArrayList<>();
        for (com.rydytrader.autotrader.store.manual.ManualClosedTrade t : svc.recentTrades()) {
            LocalDate d = java.time.Instant.ofEpochMilli(t.closeMillis).atZone(IST).toLocalDate();
            if (cutoff    != null && d.isBefore(cutoff))   continue;
            if (rangeFrom != null && d.isBefore(rangeFrom)) continue;
            if (rangeTo   != null && d.isAfter(rangeTo))    continue;
            out.add(t);
        }
        return out;
    }

    /** Compact mini-card payload shown after the Costs card on home — always visible
     *  regardless of the include-adjustments checkbox. */
    private Map<String, Object> adjustmentSummary(List<com.rydytrader.autotrader.store.manual.ManualClosedTrade> adj) {
        double netPnl = 0;
        for (com.rydytrader.autotrader.store.manual.ManualClosedTrade t : adj) netPnl += t.pnl;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count",   adj.size());
        m.put("netPnl",  round2(netPnl));
        m.put("charges", 0.0); // Per-trade brokerage estimate could be added later — kept 0 for v1.
        return m;
    }

    // ── Trade loading ───────────────────────────────────────────────────────
    /** Internal lightweight trade record used by all metric calcs. Decoupled from the JPA
     *  entity so the synthesized live-today rows (which have no DB id) plug in cleanly. */
    private record Trade(String strategyId, String sessionDate, long closedAtMillis,
                         double grossPnl, double charges, double netPnl, String closeReason,
                         int slHitCount) {}

    private List<Trade> loadTrades(String period, String strategyId, String from, String to) {
        LocalDate today = LocalDate.now(IST);
        LocalDate rangeFrom = parseIso(from);
        LocalDate rangeTo   = parseIso(to);
        boolean explicitRange = rangeFrom != null || rangeTo != null;
        LocalDate cutoff = explicitRange ? null : switch (period == null ? "all" : period.toLowerCase()) {
            case "today"  -> today;
            case "expiry" -> currentExpiryStart(today);
            case "ytd"    -> indianFinancialYearStart(today);
            case "mtd"    -> LocalDate.of(today.getYear(), today.getMonthValue(), 1);
            default       -> null; // all-time
        };
        boolean allStrategies = strategyId == null || strategyId.isBlank() || "all".equalsIgnoreCase(strategyId);

        List<Trade> out = new ArrayList<>();
        // Persisted rows
        for (StraddleTradeEntity e : tradeRepo.findAllByOrderByClosedAtMillisAsc()) {
            if (!allStrategies && !strategyId.equals(e.getStrategyId())) continue;
            LocalDate d;
            try { d = LocalDate.parse(e.getSessionDate()); }
            catch (Exception ignored) { continue; }
            if (cutoff    != null && d.isBefore(cutoff))   continue;
            if (rangeFrom != null && d.isBefore(rangeFrom)) continue;
            if (rangeTo   != null && d.isAfter(rangeTo))    continue;
            int sl = e.getSlHitCount() == null ? 0 : e.getSlHitCount();
            out.add(new Trade(e.getStrategyId(), e.getSessionDate(), e.getClosedAtMillis(),
                              e.getGrossPnl(), e.getCharges(), e.getNetPnl(), e.getCloseReason(), sl));
        }
        // Live overlay: today's in-progress closes from in-memory state.
        if (windowIncludesToday(cutoff, rangeFrom, rangeTo, today)) {
            appendLiveTodayTrades(out, strategyId, today);
        }
        out.sort(Comparator.comparingLong(Trade::closedAtMillis));
        return out;
    }

    /** Marker reason on a synthetic row representing the strategy's currently-open position
     *  (its live MTM). Capital / equity-curve sums include this; per-trade counters
     *  (wins/losses/streaks/extremes) filter it out so it doesn't pollute closed-trade stats. */
    private static final String OPEN_POSITION_MTM_REASON = "OPEN_POSITION_MTM";

    /** Pull today's already-closed straddles from each strategy's in-memory ring buffer AND a
     *  synthetic row for any still-open position, so Today's Total Return reflects realised +
     *  open MTM − charges (matching each strategy's dashboard Net Day P&L). Skips strategies
     *  whose day-end row has already been persisted. */
    private void appendLiveTodayTrades(List<Trade> out, String strategyId, LocalDate today) {
        if (strategyRegistry == null || strategyRegistry.isEmpty()) return;
        String iso = today.toString();
        boolean allStrategies = strategyId == null || strategyId.isBlank() || "all".equalsIgnoreCase(strategyId);
        Set<String> persistedTodayStrategies = new java.util.HashSet<>();
        for (Trade t : out) {
            if (iso.equals(t.sessionDate())) persistedTodayStrategies.add(t.strategyId());
        }
        for (Strategy strat : strategyRegistry.all()) {
            if (!allStrategies && !strat.id().equals(strategyId)) continue;
            if (persistedTodayStrategies.contains(strat.id())) continue;
            try {
                // 1. Today's already-closed events from the strategy's recent-events ring.
                double addedToday = 0;
                List<Map<String, Object>> live = strat.todayClosedTrades();
                if (live != null) {
                    for (Map<String, Object> m : live) {
                        double gross = asDouble(m.get("grossPnl"));
                        double ch    = asDouble(m.get("charges"));
                        double net   = ch != 0 ? gross - ch : gross;
                        long ts = asLong(m.get("closedAtMillis"));
                        if (ts == 0) ts = System.currentTimeMillis();
                        String reason = String.valueOf(m.getOrDefault("closeReason", "OPEN"));
                        out.add(new Trade(strat.id(), iso, ts, gross, ch, net, reason, 0));
                        addedToday += net;
                    }
                }
                // 2. Synthetic OPEN_POSITION_MTM row for whatever the strategy's net day P&L
                //    is NOT yet accounted for above. Equals open MTM − projected charges if
                //    nothing has closed yet; equals MTM-of-remaining-leg if leg-sl has closed
                //    one leg already; equals 0 once everything is closed. Sum across all rows
                //    today equals strategy.liveNetPnlToday() so the Capital pane reads right.
                double liveNet = strat.liveNetPnlToday();
                double openMtmRemainder = liveNet - addedToday;
                if (Math.abs(openMtmRemainder) > 0.01) {
                    out.add(new Trade(strat.id(), iso, System.currentTimeMillis(),
                        openMtmRemainder, 0, openMtmRemainder, OPEN_POSITION_MTM_REASON, 0));
                }
            } catch (Exception e) {
                log.warn("[Analytics] Live today overlay failed for {}: {}", strat.id(), e.getMessage());
            }
        }
    }

    /** True for rows that count as completed straddles. False for the synthetic
     *  {@link #OPEN_POSITION_MTM_REASON} row. */
    private static boolean isClosedStraddle(Trade t) {
        return !OPEN_POSITION_MTM_REASON.equals(t.closeReason());
    }

    private static int distinctDates(List<Trade> trades) {
        Set<String> dates = new java.util.HashSet<>();
        for (Trade t : trades) dates.add(t.sessionDate());
        return dates.size();
    }

    // ── Period helpers ──────────────────────────────────────────────────────
    private static LocalDate indianFinancialYearStart(LocalDate today) {
        int fyYear = today.getMonthValue() >= 4 ? today.getYear() : today.getYear() - 1;
        return LocalDate.of(fyYear, 4, 1);
    }

    private LocalDate currentExpiryStart(LocalDate today) {
        if (strategyRegistry != null) {
            for (Strategy s : strategyRegistry.all()) {
                String exp = s.currentWeeklyExpiry();
                if (exp == null || exp.isEmpty()) continue;
                try {
                    LocalDate expiry = LocalDate.parse(exp);
                    return expiry.minusDays(7).plusDays(1);
                } catch (Exception ignored) {}
            }
        }
        return today.minusDays(7);
    }

    private static boolean windowIncludesToday(LocalDate cutoff, LocalDate from, LocalDate to, LocalDate today) {
        if (cutoff != null && today.isBefore(cutoff)) return false;
        if (from   != null && today.isBefore(from))   return false;
        if (to     != null && today.isAfter(to))      return false;
        return true;
    }

    // ── CAPITAL ─────────────────────────────────────────────────────────────
    private Map<String, Object> capital(List<Trade> trades, double starting,
                                        List<com.rydytrader.autotrader.store.manual.ManualClosedTrade> adjustments) {
        double netSum = trades.stream().mapToDouble(Trade::netPnl).sum();
        // Fold adjustments into the money totals when the caller passes them in (i.e. the
        // operator has the "Include adjustments in P&L" checkbox ticked).
        for (com.rydytrader.autotrader.store.manual.ManualClosedTrade t : adjustments) netSum += t.pnl;
        double current = starting + netSum;
        double returnPct = starting > 0 ? (netSum / starting) * 100.0 : 0;
        // Avg monthly % — group by yyyy-MM, sum net, divide by starting, average.
        Map<String, Double> byMonth = new LinkedHashMap<>();
        for (Trade t : trades) {
            String ym = t.sessionDate() != null && t.sessionDate().length() >= 7
                ? t.sessionDate().substring(0, 7) : "";
            if (ym.isEmpty()) continue;
            byMonth.merge(ym, t.netPnl(), Double::sum);
        }
        for (com.rydytrader.autotrader.store.manual.ManualClosedTrade t : adjustments) {
            String ym = java.time.Instant.ofEpochMilli(t.closeMillis).atZone(IST).toLocalDate().toString().substring(0, 7);
            byMonth.merge(ym, t.pnl, Double::sum);
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

    // ── PERFORMANCE (per straddle) ──────────────────────────────────────────
    private Map<String, Object> performance(List<Trade> trades) {
        int wins = 0, losses = 0;
        double grossProfit = 0, grossLoss = 0;
        double sumWin = 0, sumLoss = 0;
        for (Trade t : trades) {
            double pnl = t.netPnl();
            if (pnl > 0)      { wins++;   grossProfit += pnl; sumWin  += pnl; }
            else if (pnl < 0) { losses++; grossLoss   += pnl; sumLoss += pnl; }
        }
        int total = trades.size();
        double winRate      = total > 0 ? (wins / (double) total) * 100.0 : 0;
        double profitFactor = grossLoss < 0 ? grossProfit / Math.abs(grossLoss) : 0;
        double avgWin       = wins > 0 ? sumWin / wins : 0;
        double avgLoss      = losses > 0 ? sumLoss / losses : 0;
        double riskReward   = avgLoss < 0 ? avgWin / Math.abs(avgLoss) : 0;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("straddles",    total);
        m.put("wins",         wins);
        m.put("losses",       losses);
        m.put("winRate",      round2(winRate));
        m.put("profitFactor", round2(profitFactor));
        m.put("riskReward",   round2(riskReward));
        return m;
    }

    // ── P&L EXTREMES (per straddle) ─────────────────────────────────────────
    private Map<String, Object> extremes(List<Trade> trades) {
        double maxProfit = 0, maxLoss = 0;
        double sumWin = 0, sumLoss = 0;
        int wins = 0, losses = 0;
        for (Trade t : trades) {
            double pnl = t.netPnl();
            if (pnl > maxProfit) maxProfit = pnl;
            if (pnl < maxLoss)   maxLoss   = pnl;
            if (pnl > 0)      { wins++;   sumWin  += pnl; }
            else if (pnl < 0) { losses++; sumLoss += pnl; }
        }
        // Drawdown over the cumulative per-trade equity curve. Also tracks the index of the
        // peak that preceded the deepest drawdown and the trough index so we can report
        // "Max Drawdown Days" = number of trades between them (inclusive of the trough).
        double peak = 0, cum = 0, maxDd = 0;
        int peakIdx = 0, troughIdx = 0, curPeakIdx = 0;
        for (int i = 0; i < trades.size(); i++) {
            cum += trades.get(i).netPnl();
            if (cum > peak) { peak = cum; curPeakIdx = i; }
            double dd = cum - peak;
            if (dd < maxDd) { maxDd = dd; peakIdx = curPeakIdx; troughIdx = i; }
        }
        int maxDrawdownDays = (maxDd < 0) ? (troughIdx - peakIdx) : 0;

        // Per-day SL hit histogram. Per the operator's request, exposed in the hero so the
        // mix between "both legs survived to squareoff" (0 SL), "one leg got stopped" (1 SL)
        // and "both legs stopped out" (2 SL) is visible at a glance.
        int zeroSl = 0, oneSl = 0, twoSl = 0;
        for (Trade t : trades) {
            int n = t.slHitCount();
            if (n <= 0)      zeroSl++;
            else if (n == 1) oneSl++;
            else             twoSl++;
        }

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("maxProfit",      round2(maxProfit));
        m.put("maxLoss",        round2(maxLoss));
        m.put("avgProfit",      round2(wins   > 0 ? sumWin  / wins   : 0));
        m.put("avgLoss",        round2(losses > 0 ? sumLoss / losses : 0));
        m.put("maxDrawdown",    round2(maxDd));
        m.put("maxDrawdownDays", maxDrawdownDays);
        m.put("zeroSlDays",     zeroSl);
        m.put("oneSlDays",      oneSl);
        m.put("twoSlDays",      twoSl);
        return m;
    }

    // ── STREAKS (per straddle, with total charges) ──────────────────────────
    private Map<String, Object> streaks(List<Trade> trades,
                                        List<com.rydytrader.autotrader.store.manual.ManualClosedTrade> adjustments) {
        int curWin = 0, curLoss = 0, longestWin = 0, longestLoss = 0;
        double totalCharges = 0;
        for (Trade t : trades) {
            double pnl = t.netPnl();
            totalCharges += t.charges();
            // Streaks are per-straddle only — adjustments contribute to charges (a money
            // metric) but NOT to win/loss streak counters which are strategy diagnostics.
            if (pnl > 0) {
                curWin++; curLoss = 0;
                if (curWin > longestWin) longestWin = curWin;
            } else if (pnl < 0) {
                curLoss++; curWin = 0;
                if (curLoss > longestLoss) longestLoss = curLoss;
            } else {
                curWin = 0; curLoss = 0;
            }
        }
        // Adjustments' brokerage estimate would add here once we record per-trade charges;
        // for v1 we report 0 in adjustmentSummary so totalCharges stays unchanged either way.
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("currentWinStreak",  curWin);
        m.put("longestWinStreak",  longestWin);
        m.put("currentLossStreak", curLoss);
        m.put("longestLossStreak", longestLoss);
        m.put("totalCharges",      round2(totalCharges));
        return m;
    }

    // ── EDGE (per straddle) ─────────────────────────────────────────────────
    private Map<String, Object> edge(List<Trade> trades, double starting) {
        int n = trades.size();
        Map<String, Object> m = new LinkedHashMap<>();
        if (n == 0) { m.put("expectancy", 0.0); m.put("recoveryFactor", 0.0); m.put("sharpe", 0.0); return m; }
        double netSum = 0;
        for (Trade t : trades) netSum += t.netPnl();
        double expectancy = netSum / n;
        // Recovery factor: total net / |max drawdown over per-trade equity curve|.
        double peak = 0, cum = 0, maxDd = 0;
        for (Trade t : trades) {
            cum += t.netPnl();
            if (cum > peak) peak = cum;
            double dd = cum - peak;
            if (dd < maxDd) maxDd = dd;
        }
        double recoveryFactor = maxDd < 0 ? netSum / Math.abs(maxDd) : 0;
        // Sharpe: treat each trade as one return-bearing event. r_i = trade.netPnl / startingCapital.
        // Annualise assuming an active trader does ~250 straddles a year (variable by strategy
        // mix; a reasonable default). Adjust upward if you do multiple straddles per day on average.
        double sharpe = 0;
        if (n >= 2 && starting > 0) {
            double[] returns = new double[n];
            double mean = 0;
            for (int i = 0; i < n; i++) {
                returns[i] = trades.get(i).netPnl() / starting;
                mean += returns[i];
            }
            mean /= n;
            double sq = 0;
            for (double r : returns) sq += (r - mean) * (r - mean);
            double stddev = Math.sqrt(sq / n);
            if (stddev > 0) sharpe = (mean / stddev) * Math.sqrt(250);
        }
        m.put("expectancy",     round2(expectancy));
        m.put("recoveryFactor", round2(recoveryFactor));
        m.put("sharpe",         round2(sharpe));
        return m;
    }

    // ── EQUITY CURVE ────────────────────────────────────────────────────────
    /** Cumulative equity over trades, one point per trade. X-axis label = session date so the
     *  chart still reads day-by-day; multiple trades on the same day produce multiple points
     *  with the same label (intentional — shows intraday rolls).
     *
     *  <p>Always prepends a "Start" baseline at the starting-capital value so a single-trade
     *  history still produces 2 points (enough for Chart.js to draw a connecting line — the
     *  hero chart uses {@code pointRadius: 0}, so a single isolated point would be invisible). */
    private Map<String, Object> equityCurve(List<Trade> trades, double starting,
                                            List<com.rydytrader.autotrader.store.manual.ManualClosedTrade> adjustments) {
        // Merge straddle closes + adjustment closes into one time-sorted sequence so the cum
        // equity curve threads through both in chronological order. Each emitted point carries
        // a "kind" flag (STRADDLE / ADJUSTMENT) so the home page's renderer can mark
        // adjustment points distinctly (amber triangles).
        record EquityEvent(long millis, String label, double net, String kind) {}
        List<EquityEvent> events = new ArrayList<>();
        for (Trade t : trades) {
            events.add(new EquityEvent(t.closedAtMillis(),
                t.sessionDate() == null ? "" : t.sessionDate(), t.netPnl(), "STRADDLE"));
        }
        for (com.rydytrader.autotrader.store.manual.ManualClosedTrade t : adjustments) {
            String label = java.time.Instant.ofEpochMilli(t.closeMillis).atZone(IST).toLocalDate().toString();
            events.add(new EquityEvent(t.closeMillis, label, t.pnl, "ADJUSTMENT"));
        }
        events.sort(Comparator.comparingLong(EquityEvent::millis));

        List<String>  labels = new ArrayList<>();
        List<Double>  values = new ArrayList<>();
        List<String>  kinds  = new ArrayList<>();
        labels.add("Start"); values.add(round2(starting)); kinds.add("START");
        double cum = starting;
        for (EquityEvent e : events) {
            cum += e.net();
            labels.add(e.label());
            values.add(round2(cum));
            kinds.add(e.kind());
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("labels", labels);
        m.put("values", values);
        m.put("kinds",  kinds);
        return m;
    }

    // ── Utility ─────────────────────────────────────────────────────────────
    private static LocalDate parseIso(String s) {
        if (s == null || s.isBlank()) return null;
        try { return LocalDate.parse(s.trim()); } catch (Exception e) { return null; }
    }
    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
    private static double asDouble(Object o) {
        if (o instanceof Number n) return n.doubleValue();
        if (o == null) return 0.0;
        try { return Double.parseDouble(String.valueOf(o)); } catch (Exception e) { return 0.0; }
    }
    private static long asLong(Object o) {
        if (o instanceof Number n) return n.longValue();
        if (o == null) return 0;
        try { return Long.parseLong(String.valueOf(o)); } catch (Exception e) { return 0; }
    }
}
