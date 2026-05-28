package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StraddleTradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface StraddleTradeRepository extends JpaRepository<StraddleTradeEntity, Long> {
    List<StraddleTradeEntity> findAllByOrderByClosedAtMillisAsc();
    List<StraddleTradeEntity> findByStrategyIdOrderByClosedAtMillisAsc(String strategyId);
}
