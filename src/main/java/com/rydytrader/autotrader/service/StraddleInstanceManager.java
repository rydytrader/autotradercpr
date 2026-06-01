package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.entity.StraddleInstanceEntity;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.repository.StraddleInstanceRepository;
import com.rydytrader.autotrader.repository.StraddleSessionRepository;
import com.rydytrader.autotrader.repository.StraddleTradeRepository;
import com.rydytrader.autotrader.service.strategy.ShortStraddle;
import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.store.strategy.ShortStraddleStateStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Owns the runtime collection of {@link ShortStraddle} instances. Each row of
 * {@link StraddleInstanceEntity} (where {@code active = true}) materialises into one
 * {@link ShortStraddle} stored under the entity's {@code strategyId()} (e.g.
 * {@code "inst-7"}) in an in-memory {@link LinkedHashMap}.
 *
 * <p>{@code StraddleScheduler} fans out {@code tick} / {@code fastSlCheck} /
 * {@code sampleCombinedPremium} across {@link #allEnabled()}. The CRUD endpoints in
 * {@code StraddleInstanceController} delegate here.
 *
 * <p>Soft delete: setting {@code active = false} removes the instance from the in-memory map.
 * Sessions, trades, settings rows, state JSON file are preserved.
 */
@Service
public class StraddleInstanceManager {

    private static final Logger log = LoggerFactory.getLogger(StraddleInstanceManager.class);

    private final StraddleInstanceRepository repo;
    private final RiskSettingsStore riskSettings;
    private final ShortStraddleStateStore stateStore;
    private final EventService eventService;
    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final FyersClientRouter fyersClient;
    private final ObjectProvider<MarketDataService> marketDataServiceProvider;
    private final ObjectProvider<OrderService> orderServiceProvider;
    private final ObjectProvider<MarketHolidayService> marketHolidayServiceProvider;
    private final ObjectProvider<TelegramService> telegramServiceProvider;
    private final ObjectProvider<StraddleSessionRepository> sessionRepoProvider;
    private final ObjectProvider<StraddleTradeRepository> tradeRepoProvider;
    private final ObjectProvider<OrderEventService> orderEventServiceProvider;

    private final Map<String, ShortStraddle> instances = Collections.synchronizedMap(new LinkedHashMap<>());

    public StraddleInstanceManager(StraddleInstanceRepository repo,
                                    RiskSettingsStore riskSettings,
                                    ShortStraddleStateStore stateStore,
                                    EventService eventService,
                                    TokenStore tokenStore,
                                    FyersProperties fyersProperties,
                                    FyersClientRouter fyersClient,
                                    @Lazy ObjectProvider<MarketDataService> marketDataServiceProvider,
                                    @Lazy ObjectProvider<OrderService> orderServiceProvider,
                                    @Lazy ObjectProvider<MarketHolidayService> marketHolidayServiceProvider,
                                    @Lazy ObjectProvider<TelegramService> telegramServiceProvider,
                                    @Lazy ObjectProvider<StraddleSessionRepository> sessionRepoProvider,
                                    @Lazy ObjectProvider<StraddleTradeRepository> tradeRepoProvider,
                                    @Lazy ObjectProvider<OrderEventService> orderEventServiceProvider) {
        this.repo = repo;
        this.riskSettings = riskSettings;
        this.stateStore = stateStore;
        this.eventService = eventService;
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.fyersClient = fyersClient;
        this.marketDataServiceProvider = marketDataServiceProvider;
        this.orderServiceProvider = orderServiceProvider;
        this.marketHolidayServiceProvider = marketHolidayServiceProvider;
        this.telegramServiceProvider = telegramServiceProvider;
        this.sessionRepoProvider = sessionRepoProvider;
        this.tradeRepoProvider = tradeRepoProvider;
        this.orderEventServiceProvider = orderEventServiceProvider;
    }

    @PostConstruct
    public void boot() {
        List<StraddleInstanceEntity> rows = repo.findAllByActiveTrueOrderByIdAsc();
        for (StraddleInstanceEntity row : rows) {
            try {
                ShortStraddle s = construct(row);
                s.bootstrap();
                instances.put(s.id(), s);
            } catch (Exception e) {
                log.error("[InstanceManager] Failed to materialise instance {}: {}", row.strategyId(), e.getMessage(), e);
            }
        }
        log.info("[InstanceManager] Loaded {} short-straddle instance(s)", instances.size());
    }

    private ShortStraddle construct(StraddleInstanceEntity row) {
        return new ShortStraddle(
            row,
            riskSettings,
            stateStore,
            eventService,
            tokenStore,
            fyersProperties,
            fyersClient,
            marketDataServiceProvider.getObject(),
            orderServiceProvider.getObject(),
            marketHolidayServiceProvider.getObject(),
            telegramServiceProvider.getObject(),
            sessionRepoProvider.getObject(),
            tradeRepoProvider.getObject(),
            orderEventServiceProvider.getObject()
        );
    }

    /** Every active instance. Used by the sidebar nav and registry facade. */
    public Collection<ShortStraddle> all() {
        synchronized (instances) {
            return new ArrayList<>(instances.values());
        }
    }

    /** Every active instance whose {@code enabled} setting is true. Used by the scheduler. */
    public Collection<ShortStraddle> allEnabled() {
        List<ShortStraddle> out = new ArrayList<>();
        for (ShortStraddle s : all()) {
            if (riskSettings.getStrategyBool(s.id(), "enabled", false)) out.add(s);
        }
        return out;
    }

    public ShortStraddle get(String instanceId) {
        return instanceId == null ? null : instances.get(instanceId);
    }

    public Optional<StraddleInstanceEntity> findEntity(String instanceId) {
        if (instanceId == null || !instanceId.startsWith("inst-")) return Optional.empty();
        try { return repo.findById(Long.parseLong(instanceId.substring(5))); }
        catch (NumberFormatException e) { return Optional.empty(); }
    }

    /** Create a new instance row, materialise it, and register it under its id. Initial
     *  settings are the schema defaults. */
    public synchronized ShortStraddle create(String name, String description, String shortCode) {
        validateName(name);
        validateShortCode(shortCode);
        if (repo.findByShortCode(shortCode.trim()).isPresent()) {
            throw new IllegalArgumentException("Short code already in use: " + shortCode);
        }
        StraddleInstanceEntity row = new StraddleInstanceEntity();
        row.setName(name.trim());
        row.setDescription(description == null ? "" : description.trim());
        row.setShortCode(shortCode.trim());
        row.setEnabled(true);
        row.setActive(true);
        long now = System.currentTimeMillis();
        row.setCreatedAtMillis(now);
        row.setUpdatedAtMillis(now);
        row = repo.save(row);
        // The runtime gate for the scheduler is the SETTINGS row, not the entity column.
        // Write enabled=true now so the new instance is picked up immediately by the next tick
        // without requiring the operator to open ⚙ Settings and flip a toggle.
        riskSettings.setStrategySetting(row.strategyId(), "enabled", true);
        riskSettings.saveFor("live");
        ShortStraddle s = construct(row);
        s.bootstrap();
        instances.put(s.id(), s);
        log.info("[InstanceManager] Created instance {} (shortCode={}, name={}) — enabled by default", s.id(), shortCode, name);
        return s;
    }

    /** Rename / re-describe an existing instance. Idempotent on the same values. */
    public synchronized ShortStraddle rename(String instanceId, String newName, String newDescription, String newShortCode) {
        StraddleInstanceEntity row = findEntity(instanceId)
            .orElseThrow(() -> new IllegalArgumentException("Unknown instance: " + instanceId));
        if (newName != null && !newName.isBlank()) row.setName(newName.trim());
        if (newDescription != null)               row.setDescription(newDescription.trim());
        if (newShortCode != null && !newShortCode.isBlank()) {
            String trimmed = newShortCode.trim();
            if (!trimmed.equals(row.getShortCode())) {
                Optional<StraddleInstanceEntity> clash = repo.findByShortCode(trimmed);
                if (clash.isPresent() && !clash.get().getId().equals(row.getId())) {
                    throw new IllegalArgumentException("Short code already in use: " + newShortCode);
                }
                row.setShortCode(trimmed);
            }
        }
        row.setUpdatedAtMillis(System.currentTimeMillis());
        row = repo.save(row);
        ShortStraddle s = instances.get(instanceId);
        if (s != null) s.syncFromEntity(row);
        return s;
    }

    /** Flip the {@code enabled} setting. When disabling and the instance has any open leg,
     *  throws {@link IllegalStateException} (translated to HTTP 409 by the controller). */
    public synchronized void setEnabled(String instanceId, boolean enabled) {
        ShortStraddle s = get(instanceId);
        if (s == null) throw new IllegalArgumentException("Unknown instance: " + instanceId);
        if (!enabled) {
            String state = s.currentState();
            if (!"IDLE".equals(state) && !"DONE_FOR_DAY".equals(state)) {
                throw new IllegalStateException(
                    "Cannot disable while legs are open (state=" + state + "). Squareoff first.");
            }
        }
        riskSettings.setStrategySetting(instanceId, "enabled", enabled);
        riskSettings.saveFor("live");
    }

    /** Soft-delete: mark the entity {@code active = false}, save, evict from the in-memory map.
     *  Sessions / trades / settings rows / state JSON are all preserved. */
    public synchronized void softDelete(String instanceId) {
        StraddleInstanceEntity row = findEntity(instanceId)
            .orElseThrow(() -> new IllegalArgumentException("Unknown instance: " + instanceId));
        ShortStraddle s = instances.get(instanceId);
        if (s != null) {
            String state = s.currentState();
            if (!"IDLE".equals(state) && !"DONE_FOR_DAY".equals(state)) {
                throw new IllegalStateException(
                    "Cannot delete while legs are open (state=" + state + "). Squareoff first.");
            }
        }
        row.setActive(false);
        row.setUpdatedAtMillis(System.currentTimeMillis());
        repo.save(row);
        instances.remove(instanceId);
        stateStore.evict(instanceId);
        log.info("[InstanceManager] Soft-deleted instance {}", instanceId);
    }

    private static void validateName(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name is required");
        }
        if (name.length() > 80) {
            throw new IllegalArgumentException("Name must be 80 chars or fewer");
        }
    }

    private static void validateShortCode(String shortCode) {
        if (shortCode == null || shortCode.trim().isEmpty()) {
            throw new IllegalArgumentException("Short code is required");
        }
        if (shortCode.length() > 24) {
            throw new IllegalArgumentException("Short code must be 24 chars or fewer");
        }
    }

    // Convenience for controllers — expose instances as the Strategy interface.
    public Collection<Strategy> allAsStrategy() {
        synchronized (instances) {
            return new ArrayList<>(instances.values());
        }
    }
}
