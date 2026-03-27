package com.rydytrader.autotrader.repository;

import com.rydytrader.autotrader.entity.SettingEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SettingRepository extends JpaRepository<SettingEntity, Long> {
    Optional<SettingEntity> findBySettingKey(String key);
}
