package com.rydytrader.autotrader.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * One-off data migration: renames {@code strategy_id} column values from the legacy
 * {@code "option-scalping"} to {@code "option-buying"} in every table that carries a
 * per-strategy row.
 *
 * <p>Background: the OPTIONS BUYING strategy's Java class was named {@code OptionScalping}
 * from before the Phase 4 rewrite. Its bean id + persisted {@code strategy_id} both
 * inherited the legacy name. When the class + bean + REST prefix were renamed in commit
 * {@code 8e15244}, new rows started using {@code "option-buying"} — but every historical
 * row still carried {@code "option-scalping"}. Analytics + history queries papered over
 * this with runtime aliases; this migration collapses everything to one canonical id so
 * the aliases can go away.
 *
 * <p>Runs once on boot via {@code ApplicationRunner}. {@code @Order(1)} guarantees it
 * fires AFTER {@link SchemaMigration} (which renames the legacy {@code straddle_*} tables
 * to {@code strategy_*}) so the target tables exist by the time we UPDATE.
 *
 * <p>Idempotent: a second run finds zero matching rows and no-ops. Safe to leave
 * permanently registered.
 *
 * <p>Destructive if run against the wrong DB — take a copy of {@code store/data/db/*}
 * before deploying this migration for the first time if you want a safety net.
 */
@Component
@Order(1)
public class StrategyIdMigration implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StrategyIdMigration.class);

    private static final String LEGACY_ID    = "option-scalping";
    private static final String CANONICAL_ID = "option-buying";

    private final JdbcTemplate jdbc;

    public StrategyIdMigration(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void run(ApplicationArguments args) {
        int trades   = safeUpdate("strategy_trades");
        int sessions = safeUpdate("strategy_sessions");
        if (trades > 0 || sessions > 0) {
            log.info("[StrategyIdMigration] renamed strategy_id '{}' -> '{}' — trades={} sessions={}",
                LEGACY_ID, CANONICAL_ID, trades, sessions);
        } else {
            log.debug("[StrategyIdMigration] no legacy strategy_id rows to migrate — no-op");
        }
    }

    private int safeUpdate(String table) {
        try {
            return jdbc.update(
                "UPDATE " + table + " SET strategy_id = ? WHERE strategy_id = ?",
                CANONICAL_ID, LEGACY_ID);
        } catch (Exception e) {
            // Table might not exist yet on a fresh DB (SchemaMigration runs before us
            // but a truly empty deploy has no tables until Hibernate creates them).
            log.warn("[StrategyIdMigration] UPDATE {} failed: {}", table, e.getMessage());
            return 0;
        }
    }
}
