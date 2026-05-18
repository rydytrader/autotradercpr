package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StockEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface StockRepository extends JpaRepository<StockEntity, Long> {
    Optional<StockEntity> findByTicker(String ticker);
    List<StockEntity> findByEnabledTrue();
    boolean existsByTicker(String ticker);
}
