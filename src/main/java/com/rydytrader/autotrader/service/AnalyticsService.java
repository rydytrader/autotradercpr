package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.strategy.Strategy;
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

    private final StrategyTradeRepository tradeRepo;
    private final RiskSettingsStore riskSettings;
    private final org.springframework.beans.factory.ObjectProvider<Strategy> strategyProvider;

    public AnalyticsService(StrategyTradeRepository tradeRepo,
                            RiskSettingsStore riskSettings,
                            org.springframework.beans.factory.ObjectProvider<Strategy> strategyProvider) {
        this.tradeRepo = tradeRepo;
        this.riskSettings = riskSettings;
        this.strategyProvider = strategyProvider;
    }

    /** Returns the single Strategy bean if one is registered, else null. */
    private Strategy strategy() {
        return strategyProvider == null ? null : strategyProvider.getIfAvailable();
    }

    /** Composite payload — one round-trip serves all four hero tiles, the four detail cards,
     *  and the equity curve. */
    public Map<String, Object> summary(String period, String strategyId, String from, String to) {
        return summary(period, strategyId, from, to, true);
    }

    /** {@code includeAdjustments} is kept as a parameter for backward-compat with the home
     *  page query string but is now a no-op — the manual-terminal "adjustments" feature
     *  was removed when Options Terminal was retired. */
    public Map<String, Object> summary(String period, String strategyId, String from, String to,
                                       boolean includeAdjustments) {
        List<Trade> trades = loadTrades(period, strategyId, from, to);
        List<Trade> closed = new ArrayList<>();
        for (Trade t : trades) if (isClosedStraddle(t)) closed.add(t);
        double startingCapital = riskSettings.getStartingCapital();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("period",        period);
        out.put("strategyId",    strategyId);
        out.put("from",          from);
        out.put("to",            to);
        out.put("straddleCount", closed.size());
        out.put("sessionCount",  distinctDates(closed));
        out.put("includeAdjustments", false);

        out.put("capital",     capital(trades, startingCapital));
        out.put("performance", performance(closed));
        out.put("extremes",    extremes(closed));
        out.put("streaks",     streaks(closed));
        out.put("edge",        edge(closed, startingCapital));
        out.put("equityCurve", equityCurve(trades, startingCapital));
        out.put("byMonth",     byMonth(trades, closed));
        out.put("byDate",      byDate(trades, closed));
        return out;
    }

    /** Per-day aggregation of straddle outcomes — keyed by {@code yyyy-MM-dd} so the
     *  calendar's day cells can show the true net P&L matching the home page hero.
     *
     *  <p>NetPnl/charges are summed across {@code trades} (including the synthetic
     *  {@code OPEN_POSITION_MTM} row) so today's open MTM is folded in — same source the
     *  hero's {@code totalReturn} uses. Straddles/wins/losses counters only consider
     *  {@code closed} trades; the open-position row isn't a completed outcome. */
    private Map<String, Object> byDate(List<Trade> trades, List<Trade> closed) {
        java.util.NavigableMap<String, double[]> sumByKey = new java.util.TreeMap<>();
        java.util.NavigableMap<String, int[]>    cntByKey = new java.util.TreeMap<>();
        for (Trade t : trades) {
            String key = t.sessionDate();
            if (key == null || key.isEmpty()) continue;
            double[] s = sumByKey.computeIfAbsent(key, k -> new double[2]);  // [net, charges]
            s[0] += t.netPnl();
            s[1] += t.charges();
        }
        for (Trade t : closed) {
            String key = t.sessionDate();
            if (key == null || key.isEmpty()) continue;
            double pnl = t.netPnl();
            int[]  c = cntByKey.computeIfAbsent(key, k -> new int[3]);       // [straddles, wins, losses]
            c[0]++;
            if (pnl > 0) c[1]++;
            else if (pnl < 0) c[2]++;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        java.util.Set<String> allKeys = new java.util.TreeSet<>();
        allKeys.addAll(sumByKey.keySet());
        allKeys.addAll(cntByKey.keySet());
        for (String key : allKeys) {
            double[] s = sumByKey.getOrDefault(key, new double[2]);
            int[]    c = cntByKey.getOrDefault(key, new int[3]);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("netPnl",    round2(s[0]));
            m.put("charges",   round2(s[1]));
            m.put("straddles", c[0]);
            m.put("wins",      c[1]);
            m.put("losses",    c[2]);
            out.put(key, m);
        }
        return out;
    }

    /** Per-month aggregation of straddle outcomes — keyed by {@code yyyy-MM} so the calendar's
     *  year-grid month cards (and any other per-month consumer) can read true per-straddle
     *  wins / losses / winRate instead of bucketing daily session rows. NetPnl is also
     *  reported for symmetry, though daily summing already gets that right. */
    private Map<String, Object> byMonth(List<Trade> trades, List<Trade> closed) {
        java.util.NavigableMap<String, int[]>    winLossByKey = new java.util.TreeMap<>();
        java.util.NavigableMap<String, double[]> netByKey     = new java.util.TreeMap<>();
        // Win/loss counters — only closed straddles count as outcomes.
        for (Trade t : closed) {
            String date = t.sessionDate();
            if (date == null || date.length() < 7) continue;
            String key  = date.substring(0, 7);
            double pnl  = t.netPnl();
            int[]    wl = winLossByKey.computeIfAbsent(key, k -> new int[2]);
            if      (pnl > 0) wl[0]++;
            else if (pnl < 0) wl[1]++;
        }
        // NetPnl — sum across ALL trades (including OPEN_POSITION_MTM) so the month's
        // net P&L includes today's open MTM and matches the home page hero exactly.
        for (Trade t : trades) {
            String date = t.sessionDate();
            if (date == null || date.length() < 7) continue;
            String key  = date.substring(0, 7);
            double[] ns = netByKey.computeIfAbsent(key, k -> new double[1]);
            ns[0] += t.netPnl();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        java.util.Set<String> allKeys = new java.util.TreeSet<>();
        allKeys.addAll(winLossByKey.keySet());
        allKeys.addAll(netByKey.keySet());
        for (String key : allKeys) {
            int[] wl = winLossByKey.getOrDefault(key, new int[2]);
            double[] ns = netByKey.getOrDefault(key, new double[1]);
            int total = wl[0] + wl[1];
            double winRate = total > 0 ? (wl[0] * 100.0 / total) : 0;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("straddles", total);
            m.put("wins",      wl[0]);
            m.put("losses",    wl[1]);
            m.put("winRate",   round2(winRate));
            m.put("netPnl",    round2(ns[0]));
            out.put(key, m);
        }
        return out;
    }

    // (Manual-terminal "adjustments" feature removed alongside Options Terminal.)

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
            case "qtd"    -> indianFinancialQuarterStart(today);
            case "ytd"    -> indianFinancialYearStart(today);
            case "mtd"    -> LocalDate.of(today.getYear(), today.getMonthValue(), 1);
            default       -> null; // all-time
        };
        boolean allStrategies = strategyId == null || strategyId.isBlank() || "all".equalsIgnoreCase(strategyId);

        // Singleton strategy now: if the bean exists and is disabled, hide its trades. If the
        // bean isn't present yet (Commit A intermediate state, before Camarilla lands), keep
        // every persisted row visible — there's no other source.
        Strategy strat = strategy();
        boolean strategyEnabled = strat == null || strat.isEnabled();

        List<Trade> out = new ArrayList<>();
        // Persisted rows
        for (StrategyTradeEntity e : tradeRepo.findAllByOrderByClosedAtMillisAsc()) {
            if (!allStrategies && !strategyId.equals(e.getStrategyId())) continue;
            if (!strategyEnabled) continue;
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
     *  synthetic row representing the still-open portion of the day's P&L (live MTM minus
     *  whatever's already accounted for by today's persisted straddle_trades rows + in-memory
     *  closed-live rows). The OPEN_POSITION_MTM row carries the leftover NET and CHARGES so
     *  that summing across all of today's trade rows reproduces
     *  {@code strategy.liveNetPnlToday()} and {@code strategy.liveChargesToday()} exactly.
     *
     *  <p>Previously this method skipped strategies entirely when today already had a
     *  persisted straddle_trades row — but that left the still-running cycle's MTM out of
     *  today's per-day analytics on multi-cycle days (cycle 1 persists at close, cycle 2's
     *  live MTM was then lost). The skip is gone: every strategy is processed and the
     *  OPEN_POSITION_MTM remainder is attributed regardless of persisted history. */
    private void appendLiveTodayTrades(List<Trade> out, String strategyId, LocalDate today) {
        Strategy strat = strategy();
        if (strat == null || !strat.isEnabled()) return;
        boolean allStrategies = strategyId == null || strategyId.isBlank() || "all".equalsIgnoreCase(strategyId);
        if (!allStrategies && !strat.id().equals(strategyId)) return;

        String iso = today.toString();
        // Pre-compute today's persisted net + charges so OPEN_POSITION_MTM only carries the
        // leftover, never double-counts what's already in strategy_trades. Also collect the
        // closedAtMillis of every today-row already in `out` so we can dedup the in-memory
        // ring entries that map to the same cycle — without this, every closed trade today
        // would be counted twice (once from DB, once from todayClosedTrades).
        double persistedNet = 0, persistedCh = 0;
        List<Long> persistedMillis = new ArrayList<>();
        for (Trade t : out) {
            if (!iso.equals(t.sessionDate())) continue;
            if (!strat.id().equals(t.strategyId())) continue;
            persistedNet += t.netPnl();
            persistedCh  += t.charges();
            persistedMillis.add(t.closedAtMillis());
        }
        // Dedup window: legacy DB rows were persisted with a separate System.currentTimeMillis()
        // call from the in-memory ring entry, so timestamps can drift by a few ms. Match any
        // ring entry whose closedAtMillis falls within this window of an already-persisted
        // row's millis. New cycles (post-fix) stamp both writes with the SAME value, so 0-ms
        // drift — but legacy today-rows from before the fix need the tolerance to dedup.
        final long DEDUP_WINDOW_MS = 5_000L;
        try {
            // 1. Today's already-closed events from the strategy's recent-events ring.
            //    Skip entries whose closedAtMillis is within the dedup window of a row already
            //    in `out` (those came from the DB on the same cycle). The remaining in-memory
            //    entries are cycles that haven't been persisted yet — keep them so today's
            //    analytics stay current.
            double addedTodayNet     = 0;
            double addedTodayCharges = 0;
            List<Map<String, Object>> live = strat.todayClosedTrades();
            if (live != null) {
                for (Map<String, Object> m : live) {
                    long ts = asLong(m.get("closedAtMillis"));
                    if (ts == 0) ts = System.currentTimeMillis();
                    boolean dup = false;
                    for (Long pm : persistedMillis) {
                        if (Math.abs(pm - ts) <= DEDUP_WINDOW_MS) { dup = true; break; }
                    }
                    if (dup) continue;
                    double gross = asDouble(m.get("grossPnl"));
                    double ch    = asDouble(m.get("charges"));
                    double net   = ch != 0 ? gross - ch : gross;
                    String reason = String.valueOf(m.getOrDefault("closeReason", "OPEN"));
                    out.add(new Trade(strat.id(), iso, ts, gross, ch, net, reason, 0));
                    addedTodayNet     += net;
                    addedTodayCharges += ch;
                }
            }
            // 2. Synthetic OPEN_POSITION_MTM row for the leftover live MTM.
            double liveNet        = strat.liveNetPnlToday();
            double liveCharges    = strat.liveChargesToday();
            double openNet        = liveNet     - persistedNet - addedTodayNet;
            double openChargesRem = liveCharges - persistedCh  - addedTodayCharges;
            double openGross      = openNet + openChargesRem;
            if (Math.abs(openNet) > 0.01 || Math.abs(openChargesRem) > 0.01) {
                out.add(new Trade(strat.id(), iso, System.currentTimeMillis(),
                    openGross, openChargesRem, openNet, OPEN_POSITION_MTM_REASON, 0));
            }
        } catch (Exception e) {
            log.warn("[Analytics] Live today overlay failed: {}", e.getMessage());
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

    /** Quarter-to-date start aligned to the Indian financial year. Q1=Apr-Jun, Q2=Jul-Sep,
     *  Q3=Oct-Dec, Q4=Jan-Mar — returns the first day of the quarter that {@code today}
     *  falls in. Jan/Feb/Mar return Jan 1 of the current calendar year (Q4 of last FY). */
    private static LocalDate indianFinancialQuarterStart(LocalDate today) {
        int m = today.getMonthValue();
        int quarterStartMonth = ((m - 4 + 12) % 12 / 3) * 3 + 4;       // 4, 7, 10, or 13 (→ Jan)
        int year = quarterStartMonth > 12 ? today.getYear() : today.getYear();
        if (quarterStartMonth > 12) { quarterStartMonth -= 12; }
        return LocalDate.of(year, quarterStartMonth, 1);
    }

    private LocalDate currentExpiryStart(LocalDate today) {
        // Camarilla doesn't pin to a specific weekly expiry — it trades whatever this week's
        // weekly is. The "current expiry" period therefore just rolls back 7 days from today.
        return today.minusDays(7);
    }

    private static boolean windowIncludesToday(LocalDate cutoff, LocalDate from, LocalDate to, LocalDate today) {
        if (cutoff != null && today.isBefore(cutoff)) return false;
        if (from   != null && today.isBefore(from))   return false;
        if (to     != null && today.isAfter(to))      return false;
        return true;
    }

    // ── CAPITAL ─────────────────────────────────────────────────────────────
    private Map<String, Object> capital(List<Trade> trades, double starting) {
        double netSum = trades.stream().mapToDouble(Trade::netPnl).sum();
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
        int total = 0;
        for (Trade t : trades) {
            if (!isClosedStraddle(t)) continue;
            double pnl = t.netPnl();
            total++;
            if (pnl > 0)      { wins++;   grossProfit += pnl; sumWin  += pnl; }
            else if (pnl < 0) { losses++; grossLoss   += pnl; sumLoss += pnl; }
        }
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
        // Filter out the synthetic OPEN_POSITION_MTM row so per-trade stats (max/min,
        // averages, drawdown, SL histogram) reflect closed cycles only — otherwise an
        // unrealized open MTM gets counted as a "trade" and pollutes wins/losses/extremes.
        List<Trade> closed = new ArrayList<>();
        for (Trade t : trades) {
            if (isClosedStraddle(t)) closed.add(t);
        }
        double maxProfit = 0, maxLoss = 0;
        double sumWin = 0, sumLoss = 0;
        int wins = 0, losses = 0;
        for (Trade t : closed) {
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
        for (int i = 0; i < closed.size(); i++) {
            cum += closed.get(i).netPnl();
            if (cum > peak) { peak = cum; curPeakIdx = i; }
            double dd = cum - peak;
            if (dd < maxDd) { maxDd = dd; peakIdx = curPeakIdx; troughIdx = i; }
        }
        int maxDrawdownDays = (maxDd < 0) ? (troughIdx - peakIdx) : 0;

        // Per-day SL hit histogram. Per the operator's request, exposed in the hero so the
        // mix between "both legs survived to squareoff" (0 SL), "one leg got stopped" (1 SL)
        // and "both legs stopped out" (2 SL) is visible at a glance.
        int zeroSl = 0, oneSl = 0, twoSl = 0;
        for (Trade t : closed) {
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

    // ── STREAKS (per trade, with total charges) ─────────────────────────────
    private Map<String, Object> streaks(List<Trade> trades) {
        int curWin = 0, curLoss = 0, longestWin = 0, longestLoss = 0;
        double totalCharges = 0;
        for (Trade t : trades) {
            if (!isClosedStraddle(t)) continue;
            double pnl = t.netPnl();
            totalCharges += t.charges();
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
    private Map<String, Object> equityCurve(List<Trade> trades, double starting) {
        record EquityEvent(long millis, String label, double net) {}
        List<EquityEvent> events = new ArrayList<>();
        for (Trade t : trades) {
            events.add(new EquityEvent(t.closedAtMillis(),
                t.sessionDate() == null ? "" : t.sessionDate(), t.netPnl()));
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
            kinds.add("TRADE");
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
