package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.PositionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PositionRepository extends JpaRepository<PositionEntity, Long> {
    Optional<PositionEntity> findBySymbol(String symbol);
    void deleteBySymbol(String symbol);
}
