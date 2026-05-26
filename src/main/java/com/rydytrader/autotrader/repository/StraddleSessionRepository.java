package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StraddleSessionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface StraddleSessionRepository extends JpaRepository<StraddleSessionEntity, Long> {
    Optional<StraddleSessionEntity> findBySessionDate(String sessionDate);
    List<StraddleSessionEntity> findAllByOrderBySessionDateDesc();
}
