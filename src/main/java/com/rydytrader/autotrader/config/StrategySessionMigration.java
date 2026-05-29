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
 *   <li>Backfilling the {@code strategy_id} column on the existing {@code straddle_sessions}
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

    /** Runs after the application context is fully refreshed (JPA + Hibernate schema update done).
     *  The leg-sl → short-straddle rename is split into its own listener method below so a SQL
     *  failure there (e.g. duplicate-key collision) can't poison this transaction. */
    @EventListener(ContextRefreshedEvent.class)
    @Transactional
    public void runMigrations() {
        backfillStrategyId();
        dropOldSessionDateUniqueConstraint();
        oneShotResetTradeHistory();
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
                "UPDATE straddle_sessions SET strategy_id = 'short-straddle' WHERE strategy_id = 'leg-sl'");
            int trades   = safeUpdate(
                "UPDATE straddle_trades   SET strategy_id = 'short-straddle' WHERE strategy_id = 'leg-sl'");
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

    /** One-time wipe of analytics data — gated by a flag in the SETTINGS table so it runs only
     *  on the first boot after this commit. Operator asked for a clean slate going into the new
     *  per-straddle analytics. Preserves SETTINGS + APP_USER + every other table; clears only
     *  the two analytics tables and the per-strategy state JSON files. */
    private void oneShotResetTradeHistory() {
        String flagKey = "trades.reset.v1.done";
        try {
            if (settingRepo.findBySettingKey(flagKey).isPresent()) return;

            int trades     = safeDelete("DELETE FROM straddle_trades");
            int sessions   = safeDelete("DELETE FROM straddle_sessions");
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
                "UPDATE straddle_sessions SET strategy_id = 'combined-sl-roll' " +
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

    /** Find and drop any UNIQUE constraint on {@code straddle_sessions(session_date)} that covers
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
                "WHERE UPPER(tc.TABLE_NAME) = 'STRADDLE_SESSIONS' " +
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
                    em.createNativeQuery("ALTER TABLE straddle_sessions DROP CONSTRAINT IF EXISTS " + name).executeUpdate();
                    log.info("[StrategyMigration] Dropped obsolete single-column UNIQUE constraint {} on straddle_sessions(session_date)", name);
                }
            }
        } catch (Exception e) {
            log.warn("[StrategyMigration] Could not enumerate/drop old session_date unique constraint: {}", e.getMessage());
        }
    }
}
