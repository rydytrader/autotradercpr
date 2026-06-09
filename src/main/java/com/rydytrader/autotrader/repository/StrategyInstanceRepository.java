package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StrategyInstanceEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface StrategyInstanceRepository extends JpaRepository<StrategyInstanceEntity, Long> {
    /** Every instance the operator hasn't soft-deleted, oldest first (matches sidebar order). */
    List<StrategyInstanceEntity> findAllByActiveTrueOrderByIdAsc();
    /** Filtered by strategy type — used by both {@code StraddleInstanceManager} (type='STRADDLE')
     *  and {@code StrangleInstanceManager} (type='STRANGLE') so each manager only materialises
     *  its own kind of strategy. */
    List<StrategyInstanceEntity> findAllByActiveTrueAndTypeOrderByIdAsc(String type);
    Optional<StrategyInstanceEntity> findByShortCode(String shortCode);
}
