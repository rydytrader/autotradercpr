package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StraddleSessionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface StraddleSessionRepository extends JpaRepository<StraddleSessionEntity, Long> {
    Optional<StraddleSessionEntity> findBySessionDate(String sessionDate);
    List<StraddleSessionEntity> findAllByOrderBySessionDateDesc();
    // Multi-strategy variants — filter by strategyId for per-strategy history pages
    Optional<StraddleSessionEntity> findByStrategyIdAndSessionDate(String strategyId, String sessionDate);
    List<StraddleSessionEntity> findByStrategyIdOrderBySessionDateDesc(String strategyId);
}
