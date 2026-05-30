package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.StraddleInstanceEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface StraddleInstanceRepository extends JpaRepository<StraddleInstanceEntity, Long> {
    /** Every instance the operator hasn't soft-deleted, oldest first (matches sidebar order). */
    List<StraddleInstanceEntity> findAllByActiveTrueOrderByIdAsc();
    Optional<StraddleInstanceEntity> findByShortCode(String shortCode);
}
