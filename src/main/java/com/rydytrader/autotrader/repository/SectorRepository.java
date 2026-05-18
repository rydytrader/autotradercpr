package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.SectorEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SectorRepository extends JpaRepository<SectorEntity, Long> {
    Optional<SectorEntity> findByName(String name);
}
