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
        if (tableExists("straddle_instances") && !tableExists("strategy_instances")) {
            log.info("[SchemaMigration] renaming legacy straddle_* tables → strategy_*");
            jdbc.execute("ALTER TABLE straddle_instances RENAME TO strategy_instances");
            jdbc.execute("ALTER TABLE straddle_sessions  RENAME TO strategy_sessions");
            jdbc.execute("ALTER TABLE straddle_trades    RENAME TO strategy_trades");
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
