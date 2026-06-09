package com.rydytrader.autotrader.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Schema-rename migration: takes the legacy {@code straddle_*} tables and renames them to
 * {@code strategy_*} so they semantically hold rows for any options strategy (straddle today,
 * strangle next). Runs once on first boot after deploy; subsequent boots are a no-op.
 *
 * <p>{@code @Order(0)} guarantees this runs before {@code StraddleInstanceManager}'s
 * {@code @PostConstruct boot()} fires JPA queries. By the time the manager calls
 * {@code findAllByActiveTrueAndTypeOrderByIdAsc("STRADDLE")}, Hibernate has already mapped
 * the (renamed) tables to the entities and we've backfilled the new {@code strategy_type}
 * discriminator column with {@code 'STRADDLE'} for legacy rows.
 *
 * <p>Rollback if anything goes wrong on prod: stop the app, run
 * {@code ALTER TABLE strategy_instances RENAME TO straddle_instances} (and the other two),
 * drop the {@code strategy_type} column, redeploy the previous version.
 */
@Component
@Order(0)
public class SchemaMigration implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SchemaMigration.class);

    private final JdbcTemplate jdbc;

    public SchemaMigration(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void run(ApplicationArguments args) {
        boolean legacyExists = tableExists("straddle_instances");
        boolean newExists    = tableExists("strategy_instances");

        // CASE A: legacy tables present, no new tables yet — clean rename.
        if (legacyExists && !newExists) {
            log.info("[SchemaMigration] renaming legacy straddle_* tables → strategy_*");
            jdbc.execute("ALTER TABLE straddle_instances RENAME TO strategy_instances");
            if (tableExists("straddle_sessions"))
                jdbc.execute("ALTER TABLE straddle_sessions RENAME TO strategy_sessions");
            if (tableExists("straddle_trades"))
                jdbc.execute("ALTER TABLE straddle_trades RENAME TO strategy_trades");
            // Hibernate already finished DDL before this runner — add the new column ourselves.
            jdbc.execute("ALTER TABLE strategy_instances ADD COLUMN IF NOT EXISTS strategy_type VARCHAR(20)");
        }
        // CASE B: BOTH sets of tables exist. Hibernate auto-DDL created empty strategy_* during
        // context refresh while the legacy straddle_* still holds the operator's data. We need
        // to swap them — drop the empty Hibernate-created ones and rename the legacy ones in.
        else if (legacyExists && newExists) {
            Integer newCount = countRows("strategy_instances");
            Integer legacyCount = countRows("straddle_instances");
            if (newCount != null && newCount == 0 && legacyCount != null && legacyCount > 0) {
                log.info("[SchemaMigration] swapping {} legacy rows from straddle_* into the "
                    + "Hibernate-created (empty) strategy_* shell", legacyCount);
                jdbc.execute("DROP TABLE IF EXISTS strategy_trades");
                jdbc.execute("DROP TABLE IF EXISTS strategy_sessions");
                jdbc.execute("DROP TABLE strategy_instances");
                jdbc.execute("ALTER TABLE straddle_instances RENAME TO strategy_instances");
                if (tableExists("straddle_sessions"))
                    jdbc.execute("ALTER TABLE straddle_sessions RENAME TO strategy_sessions");
                if (tableExists("straddle_trades"))
                    jdbc.execute("ALTER TABLE straddle_trades RENAME TO strategy_trades");
                // Re-add the new column (the freshly renamed table doesn't have it yet).
                jdbc.execute("ALTER TABLE strategy_instances ADD COLUMN IF NOT EXISTS strategy_type VARCHAR(20)");
            }
        }

        // Drop ANY legacy unique constraint on short_code, regardless of its name. Two names
        // have been used historically:
        //   - uk_straddle_instance_short_code (created when the entity was StraddleInstanceEntity)
        //   - uk_strategy_instance_short_code (after the rename)
        // Operators may want to reuse the same short code across multiple instances (e.g. two
        // 9:20 straddles for A/B testing different settings). Enumerate INFORMATION_SCHEMA and
        // drop every single-column UNIQUE constraint that covers SHORT_CODE.
        try {
            java.util.List<java.util.Map<String, Object>> rows = jdbc.queryForList(
                "SELECT tc.CONSTRAINT_NAME AS NAME, COUNT(kcu.COLUMN_NAME) AS COL_COUNT " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " +
                "  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME " +
                " AND tc.TABLE_NAME = kcu.TABLE_NAME " +
                "WHERE UPPER(tc.TABLE_NAME) = 'STRATEGY_INSTANCES' " +
                "  AND tc.CONSTRAINT_TYPE = 'UNIQUE' " +
                "  AND UPPER(kcu.COLUMN_NAME) = 'SHORT_CODE' " +
                "GROUP BY tc.CONSTRAINT_NAME");
            for (java.util.Map<String, Object> r : rows) {
                long colCount = ((Number) r.get("COL_COUNT")).longValue();
                if (colCount != 1) continue;   // composite constraints aren't ours to drop
                String name = String.valueOf(r.get("NAME"));
                jdbc.execute("ALTER TABLE strategy_instances DROP CONSTRAINT IF EXISTS " + name);
                log.info("[SchemaMigration] dropped legacy unique constraint {} on strategy_instances(short_code)", name);
            }
        } catch (Exception e) {
            log.debug("[SchemaMigration] drop unique constraint skipped — {}", e.getMessage());
        }

        // Backfill the discriminator on any row Hibernate just inserted a NULL into when it
        // auto-added the strategy_type column to the renamed strategy_instances table.
        try {
            int updated = jdbc.update(
                "UPDATE strategy_instances SET strategy_type = 'STRADDLE' WHERE strategy_type IS NULL");
            if (updated > 0) {
                log.info("[SchemaMigration] backfilled strategy_type='STRADDLE' on {} legacy rows", updated);
            }
        } catch (Exception e) {
            // The table or column may not exist yet on a brand-new install (no prior straddle
            // ever created). That's fine — Hibernate auto-DDL will create the new schema with
            // the column already in place.
            log.debug("[SchemaMigration] skip backfill — {}", e.getMessage());
        }
    }

    private Integer countRows(String table) {
        try {
            return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
        } catch (Exception e) { return null; }
    }

    private boolean tableExists(String name) {
        try {
            Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?)",
                Integer.class, name);
            return n != null && n > 0;
        } catch (Exception e) {
            return false;
        }
    }
}
