package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface StrategyTradeRepository extends JpaRepository<StrategyTradeEntity, Long> {
    List<StrategyTradeEntity> findAllByOrderByClosedAtMillisAsc();
    List<StrategyTradeEntity> findByStrategyIdOrderByClosedAtMillisAsc(String strategyId);
    /** All cycles for a strategy on one date, oldest first — drives the calendar day-detail modal. */
    List<StrategyTradeEntity> findByStrategyIdAndSessionDateOrderByClosedAtMillisAsc(String strategyId, String sessionDate);
}
