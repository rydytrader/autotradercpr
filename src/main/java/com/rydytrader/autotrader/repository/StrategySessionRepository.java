package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StrategySessionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface StrategySessionRepository extends JpaRepository<StrategySessionEntity, Long> {
    Optional<StrategySessionEntity> findBySessionDate(String sessionDate);
    List<StrategySessionEntity> findAllByOrderBySessionDateDesc();
    // Multi-strategy variants — filter by strategyId for per-strategy history pages
    Optional<StrategySessionEntity> findByStrategyIdAndSessionDate(String strategyId, String sessionDate);
    List<StrategySessionEntity> findByStrategyIdOrderBySessionDateDesc(String strategyId);
}
