package com.rydytrader.autotrader.config;

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
 * <p>Currently handles: backfilling the {@code strategy_id} column on the existing
 * {@code straddle_sessions} table with the literal {@code combined-sl-roll} for rows that
 * were written before the column existed. Idempotent — running multiple times is safe.
 */
@Component
public class StrategySessionMigration {

    private static final Logger log = LoggerFactory.getLogger(StrategySessionMigration.class);

    @PersistenceContext
    private EntityManager em;

    /** Runs after the application context is fully refreshed (JPA + Hibernate schema update done). */
    @EventListener(ContextRefreshedEvent.class)
    @Transactional
    public void backfillStrategyId() {
        try {
            int rows = em.createNativeQuery(
                "UPDATE straddle_sessions SET strategy_id = 'combined-sl-roll' " +
                "WHERE strategy_id IS NULL OR strategy_id = ''"
            ).executeUpdate();
            if (rows > 0) {
                log.info("[StrategyMigration] Backfilled strategy_id='combined-sl-roll' on {} legacy row(s)", rows);
            } else {
                log.debug("[StrategyMigration] No legacy rows needed strategy_id backfill");
            }
        } catch (Exception e) {
            // Could fail if column doesn't exist yet (Hibernate ddl-auto=update should have added it).
            // Log and move on — next boot will retry.
            log.warn("[StrategyMigration] Backfill skipped: {}", e.getMessage());
        }
    }
}
