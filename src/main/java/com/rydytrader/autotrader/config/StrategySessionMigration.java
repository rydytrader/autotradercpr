package com.rydytrader.autotrader.config;

import com.rydytrader.autotrader.entity.SettingEntity;
import com.rydytrader.autotrader.repository.SettingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

/**
 * One-time database migrations for the multi-strategy refactor.
 *
 * <p>Currently handles:
 * <ul>
 *   <li>Backfilling the {@code strategy_id} column on the existing {@code strategy_sessions}
 *       table with {@code combined-sl-roll} for rows written before the column existed.</li>
 *   <li>Dropping the obsolete unique constraint on {@code session_date} (replaced by the
 *       composite unique on {@code (strategy_id, session_date)} declared in the entity).
 *       Without this, leg-sl can't persist a session on a day combined-sl-roll already wrote.</li>
 * </ul>
 * Idempotent — running multiple times is safe.
 */
@Component
public class StrategySessionMigration {

    private static final Logger log = LoggerFactory.getLogger(StrategySessionMigration.class);

    @PersistenceContext
    private EntityManager em;

    private final SettingRepository settingRepo;

    public StrategySessionMigration(SettingRepository settingRepo) {
        this.settingRepo = settingRepo;
    }

    /** Each migration step is its own @EventListener / @Transactional method so a SQL failure
     *  inside one can't mark the surrounding transaction rollback-only and trigger
     *  UnexpectedRollbackException on commit. Listener order matters only for steps that
     *  depend on each other; here they're independent. */
    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txBackfillStrategyId() { backfillStrategyId(); }

    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txDropOldSessionDateUniqueConstraint() { dropOldSessionDateUniqueConstraint(); }

    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txOneShotResetTradeHistory() { oneShotResetTradeHistory(); }

    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txOneShotWipeForMultiInstance() { oneShotWipeForMultiInstance(); }

    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txCleanupSoftDeletedInstances() { cleanupSoftDeletedInstances(); }

    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txOneShotFixInst1_20260602() { oneShotFixInst1_20260602(); }

    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txOneShotFixInst1_20260603() { oneShotFixInst1_20260603(); }

    @EventListener(ContextRefreshedEvent.class) @Transactional
    public void txPurgeLegacyInstancesAndSettings() { purgeLegacyInstancesAndSettings(); }

    /** One-shot purge of every {@code strategy_instances} row + its per-instance settings
     *  rows. Runs once on first boot after the strategy cutover (gated by the
     *  {@code strategy.cutover.done} flag). The branch deletes Straddle + Strangle code; this
     *  removes their persisted instance rows so {@code AnalyticsService} doesn't carry them
     *  forward as orphans. Trade rows + session rows are kept — they're already empty in this
     *  user's DB (wiped earlier), and even if some history existed it stays in the
     *  {@code strategy_trades} table for analytics history. */
    private void purgeLegacyInstancesAndSettings() {
        String flagKey = "strategy.cutover.done";
        String legacyFlagKey = "camarilla.cutover.done";
        try {
            if (settingRepo.findBySettingKey(flagKey).isPresent()
                || settingRepo.findBySettingKey(legacyFlagKey).isPresent()) return;

            int instances = safeUpdate("DELETE FROM strategy_instances");
            int settings  = safeUpdate(
                "DELETE FROM settings WHERE setting_key LIKE 'strategies.inst-%'");

            settingRepo.save(new SettingEntity(flagKey, String.valueOf(System.currentTimeMillis())));

            log.warn("[StrategyMigration] strategy cutover — purged {} legacy strategy_instances " +
                "row(s) + {} per-instance setting(s). OptionBuying is now the only strategy.",
                instances, settings);
        } catch (Exception e) {
            log.warn("[StrategyMigration] strategy cutover purge skipped: {}", e.getMessage());
        }
    }

    /** Hard-deletes every {@code strategy_instances} row marked {@code active = false} along
     *  with its {@code strategies.inst-<id>.*} settings rows and its on-disk state file.
     *  Runs on every boot — idempotent (no soft-deleted rows = no-op). Since the Straddles
     *  tab no longer offers a Delete button, soft-delete is effectively dormant; this keeps
     *  the table clean if any rows ever land in that state via direct API. */
    private void cleanupSoftDeletedInstances() {
        try {
            @SuppressWarnings("unchecked")
            java.util.List<Number> ids = em.createNativeQuery(
                "SELECT id FROM strategy_instances WHERE active = false").getResultList();
            if (ids == null || ids.isEmpty()) return;
            for (Number n : ids) {
                String strategyId = "inst-" + n.longValue();
                safeUpdate("DELETE FROM settings WHERE setting_key LIKE 'strategies." + strategyId + ".%'");
                // Both strategy types have their own state-file prefix — try each.
                for (String prefix : new String[]{"short-straddle-", "short-strangle-"}) {
                    try {
                        java.io.File f = new java.io.File("../store/data/strategies/" + prefix + strategyId + "-state.json");
                        if (f.exists() && f.delete()) {
                            log.info("[StrategyMigration] Deleted state file for soft-deleted {}", strategyId);
                        }
                    } catch (Exception ignored) {}
                }
            }
            int rows = safeUpdate("DELETE FROM strategy_instances WHERE active = false");
            log.info("[StrategyMigration] Hard-deleted {} soft-deleted strategy instance(s) + their settings/state files", rows);
        } catch (Exception e) {
            log.warn("[StrategyMigration] Soft-delete cleanup skipped: {}", e.getMessage());
        }
    }

    /** Separate listener — runs in its own transaction. */
    @EventListener(ContextRefreshedEvent.class)
    @Transactional
    public void runRename() {
        renameLegSlToShortStraddle();
    }

    /** Renames every {@code leg-sl} row / SETTINGS key to {@code short-straddle} so the
     *  strategy keeps reading its own state after the id rename. Idempotent — a second pass
     *  finds no {@code leg-sl} rows and no-ops.
     *
     *  <p>Settings keys can already exist under BOTH the legacy and new prefix when the
     *  operator saved settings under the new id before this migration ran. In that case the
     *  new key is authoritative — we delete the legacy duplicate before renaming the rest,
     *  so the UPDATE never collides with the unique index on {@code setting_key}. */
    private void renameLegSlToShortStraddle() {
        try {
            int sessions = safeUpdate(
                "UPDATE strategy_sessions SET strategy_id = 'short-straddle' WHERE strategy_id = 'leg-sl'");
            int trades   = safeUpdate(
                "UPDATE strategy_trades   SET strategy_id = 'short-straddle' WHERE strategy_id = 'leg-sl'");
            // Pre-clean: drop any legacy leg-sl key that already has a short-straddle counterpart.
            int deleted = safeUpdate(
                "DELETE FROM settings WHERE setting_key LIKE 'strategies.leg-sl.%' " +
                "AND EXISTS (SELECT 1 FROM settings s2 WHERE s2.setting_key = " +
                "REPLACE(settings.setting_key, 'strategies.leg-sl.', 'strategies.short-straddle.'))");
            // Rename the remaining leg-sl keys — by now no collisions remain.
            int renamed = safeUpdate(
                "UPDATE settings SET setting_key = REPLACE(setting_key, 'strategies.leg-sl.', 'strategies.short-straddle.') " +
                "WHERE setting_key LIKE 'strategies.leg-sl.%'");
            if (sessions + trades + deleted + renamed > 0) {
                log.info("[StrategyMigration] Renamed leg-sl → short-straddle: {} session(s), {} trade(s), {} setting(s) renamed, {} duplicate setting(s) discarded",
                    sessions, trades, renamed, deleted);
            }
        } catch (Exception e) {
            log.warn("[StrategyMigration] leg-sl → short-straddle rename skipped: {}", e.getMessage());
        }
    }

    private int safeUpdate(String sql) {
        try { return em.createNativeQuery(sql).executeUpdate(); }
        catch (Exception e) {
            log.warn("[StrategyMigration] {} failed: {}", sql, e.getMessage());
            return 0;
        }
    }

    /** v2 wipe — multi-instance refactor. Removes every legacy single-instance row so the
     *  operator starts with zero instances. Sessions / trades rows are dropped (analytics
     *  would otherwise attribute them to an instance that no longer exists). SETTINGS rows
     *  keyed by the old singleton id ({@code strategies.short-straddle.*} or
     *  {@code strategies.leg-sl.*}) are dropped. Every {@code *-state.json} under the strategy
     *  state directory is removed. Gated by the {@code trades.reset.v2.done} flag so it runs
     *  exactly once. */
    private void oneShotWipeForMultiInstance() {
        String flagKey = "trades.reset.v2.done";
        try {
            if (settingRepo.findBySettingKey(flagKey).isPresent()) return;

            int trades   = safeDelete("DELETE FROM strategy_trades");
            int sessions = safeDelete("DELETE FROM strategy_sessions");
            int settings = safeDelete(
                "DELETE FROM settings WHERE setting_key LIKE 'strategies.short-straddle.%' " +
                "OR setting_key LIKE 'strategies.leg-sl.%'");
            int stateFiles = wipeStrategyStateFiles();

            settingRepo.save(new SettingEntity(flagKey, String.valueOf(System.currentTimeMillis())));

            log.warn("[StrategyMigration] v2 wipe (multi-instance) — cleared {} trade row(s), " +
                "{} session row(s), {} legacy setting(s), {} state file(s). Operator must " +
                "create instances via Settings → Straddles.",
                trades, sessions, settings, stateFiles);
        } catch (Exception e) {
            log.warn("[StrategyMigration] v2 wipe skipped: {}", e.getMessage());
        }
    }

    /** v1 wipe — pre-multi-instance. Kept so a fresh install on a v1-flagged DB doesn't re-run.
     *  v2 supersedes it; both flags are checked on boot. */
    private void oneShotResetTradeHistory() {
        String flagKey = "trades.reset.v1.done";
        try {
            if (settingRepo.findBySettingKey(flagKey).isPresent()) return;

            int trades     = safeDelete("DELETE FROM strategy_trades");
            int sessions   = safeDelete("DELETE FROM strategy_sessions");
            int stateFiles = wipeStrategyStateFiles();

            settingRepo.save(new SettingEntity(flagKey, String.valueOf(System.currentTimeMillis())));

            log.warn("[StrategyMigration] ONE-SHOT TRADE HISTORY RESET — cleared {} trade rows, " +
                "{} session rows, {} strategy state files. SETTINGS + APP_USER preserved.",
                trades, sessions, stateFiles);
        } catch (Exception e) {
            log.warn("[StrategyMigration] Trade history reset skipped: {}", e.getMessage());
        }
    }

    private int safeDelete(String sql) {
        try { return em.createNativeQuery(sql).executeUpdate(); }
        catch (Exception e) {
            log.warn("[StrategyMigration] {} failed: {}", sql, e.getMessage());
            return 0;
        }
    }

    /** Delete every {@code *-state.json} file under {@code ../store/data/strategies/}. Strategies
     *  re-init from empty state on next boot (or in this same boot if they haven't loaded yet —
     *  PostConstruct ordering is unpredictable, so the operator may need a second restart for the
     *  state files to fully disappear from the in-memory cache). */
    private int wipeStrategyStateFiles() {
        java.io.File dir = new java.io.File("../store/data/strategies");
        if (!dir.isDirectory()) return 0;
        java.io.File[] files = dir.listFiles((d, name) -> name.endsWith("-state.json") || name.endsWith("-state.json.tmp"));
        if (files == null || files.length == 0) return 0;
        int deleted = 0;
        for (java.io.File f : files) {
            if (f.delete()) deleted++;
        }
        return deleted;
    }

    private void backfillStrategyId() {
        try {
            int rows = em.createNativeQuery(
                "UPDATE strategy_sessions SET strategy_id = 'combined-sl-roll' " +
                "WHERE strategy_id IS NULL OR strategy_id = ''"
            ).executeUpdate();
            if (rows > 0) {
                log.info("[StrategyMigration] Backfilled strategy_id='combined-sl-roll' on {} legacy row(s)", rows);
            } else {
                log.info("[StrategyMigration] strategy_id backfill check ran — 0 rows needed updating (H2 column DEFAULT already populated)");
            }
        } catch (Exception e) {
            log.warn("[StrategyMigration] Backfill skipped: {}", e.getMessage());
        }
    }

    /** One-shot fix for inst-1 / 2026-06-02 — the bot was offline at squareoff time, so the
     *  row got persisted with the 3:18 LTPs instead of the real fills (CE leg hit SL at
     *  104.60, PE leg closed at 2.35; both legs were 130 qty, entries 54.60 / 67.05).
     *  Recomputes gross / charges / net from the corrected fills and updates both the
     *  {@code strategy_trades} and {@code strategy_sessions} rows. Gated by the
     *  {@code trade.fix.inst-1.2026-06-02.done} flag so it runs exactly once. */
    private void oneShotFixInst1_20260602() {
        String flagKey = "trade.fix.inst-1.2026-06-02.done";
        try {
            if (settingRepo.findBySettingKey(flagKey).isPresent()) return;

            final String strategyId = "inst-1";
            final String sessionDate = "2026-06-02";
            final double ceEntry = 54.60, ceExit = 104.60;
            final double peEntry = 67.05, peExit = 2.35;
            final int qty = 130;

            double sellT  = (ceEntry + peEntry) * qty;
            double buyT   = (ceExit  + peExit)  * qty;
            double totalT = sellT + buyT;

            // Read brokeragePerOrder from SETTINGS (default 20 if absent or unparseable).
            double brokeragePerOrder = readDouble("brokeragePerOrder", 20.0);
            int orders = 4; // CE sell + PE sell + CE buy + PE buy.
            double brokerage = orders * brokeragePerOrder;

            // Same rates as ShortStraddle.computeChargesBreakdown.
            double stt      = sellT  * 0.000625;
            double exchange = totalT * 0.0003503;
            double sebi     = (totalT / 10_000_000.0) * 10.0;
            double stamp    = buyT   * 0.00003;
            double gst      = (brokerage + exchange) * 0.18;
            double charges  = round2(brokerage + stt + exchange + sebi + stamp + gst);

            double gross = round2((ceEntry - ceExit) * qty + (peEntry - peExit) * qty);
            double net   = round2(gross - charges);

            int tradeRows = safeUpdate(String.format(java.util.Locale.US,
                "UPDATE strategy_trades " +
                "SET gross_pnl = %s, charges = %s, net_pnl = %s, " +
                "    close_reason = 'CE_SL_HIT', sl_hit_count = 1, qty = %d " +
                "WHERE strategy_id = '%s' AND session_date = '%s'",
                gross, charges, net, qty, strategyId, sessionDate));

            int sessionRows = safeUpdate(String.format(java.util.Locale.US,
                "UPDATE strategy_sessions " +
                "SET premium_collected = %s, premium_paid_back = %s, " +
                "    gross_pnl = %s, charges = %s, net_pnl = %s " +
                "WHERE strategy_id = '%s' AND session_date = '%s'",
                round2(sellT), round2(buyT), gross, charges, net, strategyId, sessionDate));

            settingRepo.save(new SettingEntity(flagKey, String.valueOf(System.currentTimeMillis())));

            log.warn("[StrategyMigration] inst-1 / 2026-06-02 fix applied — " +
                "trades rows updated: {}, sessions rows updated: {}, gross={} charges={} net={}",
                tradeRows, sessionRows, gross, charges, net);
        } catch (Exception e) {
            log.warn("[StrategyMigration] inst-1 / 2026-06-02 fix skipped: {}", e.getMessage());
        }
    }

    /** One-shot fix for inst-1 / 2026-06-03 — portfolio-SL day. PE leg hit its per-leg SL at
     *  237.90; the portfolio kill switch then force-closed the CE leg at 240.00. With the
     *  pre-fix {@code persistStraddleTrade}, the row was written before the WS confirmed
     *  the CE close fill, so it captured only the PE leg's loss and {@code slHitCount = 1}.
     *  Recomputes gross / charges / net using both legs' realised fills and bumps the SL
     *  count to 2 (per-leg SL + portfolio-induced close both count under the new rule).
     *  Charges use the corrected GST base ({@code brokerage + exchange + sebi}). */
    private void oneShotFixInst1_20260603() {
        String flagKey = "trade.fix.inst-1.2026-06-03.done";
        try {
            if (settingRepo.findBySettingKey(flagKey).isPresent()) return;

            final String strategyId = "inst-1";
            final String sessionDate = "2026-06-03";
            // ceExit = 240.10 (not the rounded 240 the operator quoted) so the row matches
            // the broker-confirmed realised P&L of -10,049 stored in the live state JSON.
            final double ceEntry = 216.10, ceExit = 240.10;
            final double peEntry = 184.60, peExit = 237.90;
            final int qty = 130;

            double sellT  = (ceEntry + peEntry) * qty;
            double buyT   = (ceExit  + peExit)  * qty;
            double totalT = sellT + buyT;

            double brokeragePerOrder = readDouble("brokeragePerOrder", 20.0);
            int orders = 4;
            double brokerage = orders * brokeragePerOrder;

            double stt      = sellT  * 0.000625;
            double exchange = totalT * 0.0003503;
            double sebi     = (totalT / 10_000_000.0) * 10.0;
            double stamp    = buyT   * 0.00003;
            double gst      = (brokerage + exchange + sebi) * 0.18;
            double charges  = round2(brokerage + stt + exchange + sebi + stamp + gst);

            double gross = round2((ceEntry - ceExit) * qty + (peEntry - peExit) * qty);
            double net   = round2(gross - charges);

            int tradeRows = safeUpdate(String.format(java.util.Locale.US,
                "UPDATE strategy_trades " +
                "SET gross_pnl = %s, charges = %s, net_pnl = %s, " +
                "    close_reason = 'PORTFOLIO_MAX_LOSS_HIT', sl_hit_count = 2, qty = %d " +
                "WHERE strategy_id = '%s' AND session_date = '%s'",
                gross, charges, net, qty, strategyId, sessionDate));

            int sessionRows = safeUpdate(String.format(java.util.Locale.US,
                "UPDATE strategy_sessions " +
                "SET premium_collected = %s, premium_paid_back = %s, " +
                "    gross_pnl = %s, charges = %s, net_pnl = %s " +
                "WHERE strategy_id = '%s' AND session_date = '%s'",
                round2(sellT), round2(buyT), gross, charges, net, strategyId, sessionDate));

            settingRepo.save(new SettingEntity(flagKey, String.valueOf(System.currentTimeMillis())));

            log.warn("[StrategyMigration] inst-1 / 2026-06-03 fix applied — " +
                "trades rows updated: {}, sessions rows updated: {}, gross={} charges={} net={}",
                tradeRows, sessionRows, gross, charges, net);
        } catch (Exception e) {
            log.warn("[StrategyMigration] inst-1 / 2026-06-03 fix skipped: {}", e.getMessage());
        }
    }

    private double readDouble(String key, double fallback) {
        return settingRepo.findBySettingKey(key)
            .map(s -> {
                try { return Double.parseDouble(s.getSettingValue()); }
                catch (Exception e) { return fallback; }
            })
            .orElse(fallback);
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Find and drop any UNIQUE constraint on {@code strategy_sessions(session_date)} that covers
     *  ONLY that column. The composite constraint added by the entity ({@code strategy_id +
     *  session_date}) is left alone since it spans 2 columns and won't match this query. */
    @SuppressWarnings("unchecked")
    private void dropOldSessionDateUniqueConstraint() {
        try {
            // H2 v2 INFORMATION_SCHEMA: find UNIQUE constraints on the table whose only column is SESSION_DATE.
            java.util.List<Object[]> rows = em.createNativeQuery(
                "SELECT tc.CONSTRAINT_NAME, COUNT(kcu.COLUMN_NAME) AS col_count " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " +
                "  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME " +
                " AND tc.TABLE_NAME = kcu.TABLE_NAME " +
                "WHERE UPPER(tc.TABLE_NAME) = 'STRATEGY_SESSIONS' " +
                "  AND tc.CONSTRAINT_TYPE = 'UNIQUE' " +
                "  AND UPPER(kcu.COLUMN_NAME) = 'SESSION_DATE' " +
                "GROUP BY tc.CONSTRAINT_NAME"
            ).getResultList();
            for (Object[] row : rows) {
                String name = String.valueOf(row[0]);
                long colCount = ((Number) row[1]).longValue();
                // Only drop SINGLE-COLUMN unique constraints (the legacy one). The composite
                // (strategy_id + session_date) constraint will have 2 columns — leave it alone.
                if (colCount == 1) {
                    em.createNativeQuery("ALTER TABLE strategy_sessions DROP CONSTRAINT IF EXISTS " + name).executeUpdate();
                    log.info("[StrategyMigration] Dropped obsolete single-column UNIQUE constraint {} on strategy_sessions(session_date)", name);
                }
            }
        } catch (Exception e) {
            log.warn("[StrategyMigration] Could not enumerate/drop old session_date unique constraint: {}", e.getMessage());
        }
    }
}
