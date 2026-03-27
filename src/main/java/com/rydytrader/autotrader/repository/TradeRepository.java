package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.TradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.List;

public interface TradeRepository extends JpaRepository<TradeEntity, Long> {
    List<TradeEntity> findByTradeDate(LocalDate date);
    List<TradeEntity> findByTradeDateBetween(LocalDate from, LocalDate to);
    void deleteByTradeDate(LocalDate date);
}
