package com.rydytrader.autotrader.store;

import com.rydytrader.autotrader.entity.PositionEntity;
import com.rydytrader.autotrader.repository.PositionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Persists open position state to SQLite so polling resumes correctly after server restarts.
 * Supports both INTRADAY (day trading) and CNC (swing trading) product types.
 */
@Component
public class PositionStateStore {

    private static final Logger log = LoggerFactory.getLogger(PositionStateStore.class);
    public static final String INTRADAY = "INTRADAY";
    public static final String CNC = "CNC";

    @Autowired
    private PositionRepository positionRepo;

    // ── Save (backward compat defaults to INTRADAY) ──────────────────────────

    public void save(String symbol, String side, int qty, double avgPrice,
                     String setup, String entryTime, double slPrice, double targetPrice) {
        save(symbol, side, qty, avgPrice, setup, entryTime, slPrice, targetPrice, INTRADAY);
    }

    public void save(String symbol, String side, int qty, double avgPrice,
                     String setup, String entryTime, double slPrice, double targetPrice, String productType) {
        try {
            PositionEntity entity = positionRepo.findBySymbolAndProductType(symbol, productType)
                .orElse(new PositionEntity());
            entity.setSymbol(symbol);
            entity.setProductType(productType);
            entity.setSide(side);
            entity.setQty(qty);
            entity.setAvgPrice(avgPrice);
            entity.setSetup(setup != null ? setup : "");
            entity.setEntryTime(entryTime != null ? entryTime : "");
            entity.setSlPrice(slPrice);
            entity.setTargetPrice(targetPrice);
            positionRepo.save(entity);
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to save {}/{}: {}", symbol, productType, e.getMessage());
        }
    }

    // ── Append description ───────────────────────────────────────────────────

    public void appendDescription(String symbol, String text) {
        appendDescription(symbol, text, INTRADAY);
    }

    public void appendDescription(String symbol, String text, String productType) {
        try {
            positionRepo.findBySymbolAndProductType(symbol, productType).ifPresent(entity -> {
                String current = entity.getDescription() != null ? entity.getDescription() : "";
                entity.setDescription(current.isEmpty() ? text : current + "\n" + text);
                positionRepo.save(entity);
            });
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to appendDescription for {}/{}: {}", symbol, productType, e.getMessage());
        }
    }

    // ── Get description ──────────────────────────────────────────────────────

    public String getDescription(String symbol) {
        return getDescription(symbol, INTRADAY);
    }

    public String getDescription(String symbol, String productType) {
        try {
            return positionRepo.findBySymbolAndProductType(symbol, productType)
                .map(PositionEntity::getDescription)
                .orElse(null);
        } catch (Exception e) {
            return null;
        }
    }

    // ── Save OCO state ───────────────────────────────────────────────────────

    public void saveOcoState(String symbol, String slOrderId, String targetOrderId,
                             double slPrice, double targetPrice) {
        saveOcoState(symbol, slOrderId, targetOrderId, slPrice, targetPrice, INTRADAY);
    }

    public void saveOcoState(String symbol, String slOrderId, String targetOrderId,
                             double slPrice, double targetPrice, String productType) {
        try {
            Optional<PositionEntity> opt = positionRepo.findBySymbolAndProductType(symbol, productType);
            if (opt.isEmpty()) return;
            PositionEntity entity = opt.get();
            entity.setSlOrderId(slOrderId);
            entity.setTargetOrderId(targetOrderId);
            entity.setSlPrice(slPrice);
            entity.setTargetPrice(targetPrice);
            positionRepo.save(entity);
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to save OCO state for {}/{}: {}", symbol, productType, e.getMessage());
        }
    }

    // ── Clear ────────────────────────────────────────────────────────────────

    @Transactional
    public void clear(String symbol) {
        clear(symbol, INTRADAY);
    }

    @Transactional
    public void clear(String symbol, String productType) {
        try {
            positionRepo.deleteBySymbolAndProductType(symbol, productType);
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to clear {}/{}: {}", symbol, productType, e.getMessage());
        }
    }

    // ── Load ─────────────────────────────────────────────────────────────────

    public Map<String, Object> load(String symbol) {
        return load(symbol, INTRADAY);
    }

    public Map<String, Object> load(String symbol, String productType) {
        try {
            Optional<PositionEntity> opt = positionRepo.findBySymbolAndProductType(symbol, productType);
            if (opt.isEmpty()) {
                // Backward compat: try without productType filter for legacy data
                opt = positionRepo.findBySymbol(symbol);
            }
            if (opt.isEmpty()) return null;
            return entityToMap(opt.get());
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to load {}/{}: {}", symbol, productType, e.getMessage());
            return null;
        }
    }

    // ── Clear all ────────────────────────────────────────────────────────────

    @Transactional
    public void clearAll() {
        try {
            positionRepo.deleteAll();
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to clearAll: {}", e.getMessage());
        }
    }

    // ── Load all ─────────────────────────────────────────────────────────────

    public Map<String, Map<String, Object>> loadAll() {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        try {
            List<PositionEntity> all = positionRepo.findAll();
            for (PositionEntity entity : all) {
                // Key by symbol for backward compat (day trading callers expect symbol key)
                result.put(entity.getSymbol(), entityToMap(entity));
            }
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to loadAll: {}", e.getMessage());
        }
        return result;
    }

    /** Load all positions of a specific product type. */
    public Map<String, Map<String, Object>> loadByProductType(String productType) {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        try {
            List<PositionEntity> all = positionRepo.findByProductType(productType);
            for (PositionEntity entity : all) {
                result.put(entity.getSymbol(), entityToMap(entity));
            }
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to loadByProductType {}: {}", productType, e.getMessage());
        }
        return result;
    }

    // ── Probability ──────────────────────────────────────────────────────────

    public void saveProbability(String symbol, String probability) {
        saveProbability(symbol, probability, INTRADAY);
    }

    public void saveProbability(String symbol, String probability, String productType) {
        try {
            positionRepo.findBySymbolAndProductType(symbol, productType).ifPresent(entity -> {
                entity.setProbability(probability != null ? probability : "");
                positionRepo.save(entity);
            });
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to saveProbability for {}/{}: {}", symbol, productType, e.getMessage());
        }
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private Map<String, Object> entityToMap(PositionEntity e) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("symbol",        e.getSymbol());
        map.put("productType",   e.getProductType());
        map.put("side",          e.getSide());
        map.put("qty",           e.getQty());
        map.put("avgPrice",      e.getAvgPrice());
        map.put("setup",         e.getSetup());
        map.put("entryTime",     e.getEntryTime());
        map.put("slPrice",       e.getSlPrice());
        map.put("targetPrice",   e.getTargetPrice());
        map.put("slOrderId",     e.getSlOrderId());
        map.put("targetOrderId", e.getTargetOrderId());
        map.put("description",   e.getDescription());
        map.put("probability",   e.getProbability());
        return map;
    }
}
