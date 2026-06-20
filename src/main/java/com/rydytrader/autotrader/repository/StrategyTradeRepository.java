package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
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
}
