package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface StrategyTradeRepository extends JpaRepository<StrategyTradeEntity, Long> {
    List<StrategyTradeEntity> findAllByOrderByClosedAtMillisAsc();
    List<StrategyTradeEntity> findByStrategyIdOrderByClosedAtMillisAsc(String strategyId);
    /** All cycles for a strategy on one date, oldest first — drives the calendar day-detail modal. */
    List<StrategyTradeEntity> findByStrategyIdAndSessionDateOrderByClosedAtMillisAsc(String strategyId, String sessionDate);

    /** Delete every row whose {@code sessionDate} equals the given ISO date (yyyy-MM-dd).
     *  Used by the Maintenance "Clear today's records" action to wipe both ALGO and MANUAL
     *  rows in a single call. Spring Data derives the implementation from the method name;
     *  {@code @Transactional} is required for derived delete queries. Returns the row count. */
    @Transactional
    long deleteBySessionDate(String sessionDate);

    /** Wipe every row in the strategy_trades table — used by the Maintenance "Clear ALL
     *  Records" action. Explicit {@code @Modifying} + {@code @Transactional} + JPQL DELETE
     *  (instead of {@code deleteAllInBatch()}) is the most reliable shape: it commits
     *  inside a freshly-opened transaction when called from a non-{@code @Transactional}
     *  service method, doesn't depend on AOP proxy quirks, and uses the same template
     *  Spring Data applies for {@code deleteBySessionDate} above. Returns the row count. */
    @Modifying
    @Transactional
    @Query("DELETE FROM StrategyTradeEntity")
    int deleteAllRows();
}
