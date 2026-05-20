package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.IndexEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface IndexRepository extends JpaRepository<IndexEntity, Long> {
    Optional<IndexEntity> findByName(String name);
    Optional<IndexEntity> findByTicker(String ticker);
    boolean existsByTicker(String ticker);
}
