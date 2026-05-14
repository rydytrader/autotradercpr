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
 */
@Component
public class PositionStateStore {

    private static final Logger log = LoggerFactory.getLogger(PositionStateStore.class);

    @Autowired
    private PositionRepository positionRepo;

    public void save(String symbol, String side, int qty, double avgPrice,
                     String setup, String entryTime, double slPrice, double targetPrice) {
        try {
            PositionEntity entity = positionRepo.findBySymbol(symbol).orElse(new PositionEntity());
            entity.setSymbol(symbol);
            entity.setSide(side);
            entity.setQty(qty);
            entity.setAvgPrice(avgPrice);
            entity.setSetup(setup != null ? setup : "");
            entity.setEntryTime(entryTime != null ? entryTime : "");
            entity.setSlPrice(slPrice);
            entity.setTargetPrice(targetPrice);
            positionRepo.save(entity);
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to save {}: {}", symbol, e.getMessage());
        }
    }

    /** Appends a line to the description field of a persisted position. */
    public void appendDescription(String symbol, String text) {
        try {
            positionRepo.findBySymbol(symbol).ifPresent(entity -> {
                String current = entity.getDescription() != null ? entity.getDescription() : "";
                entity.setDescription(current.isEmpty() ? text : current + "\n" + text);
                positionRepo.save(entity);
            });
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to appendDescription for {}: {}", symbol, e.getMessage());
        }
    }

    /** Returns the current description for a symbol, or null. */
    public String getDescription(String symbol) {
        try {
            return positionRepo.findBySymbol(symbol)
                .map(PositionEntity::getDescription)
                .orElse(null);
        } catch (Exception e) {
            return null;
        }
    }

    /** Updates persisted state with OCO order IDs and prices (single target). */
    public void saveOcoState(String symbol, String slOrderId, String targetOrderId,
                             double slPrice, double targetPrice) {
        try {
            Optional<PositionEntity> opt = positionRepo.findBySymbol(symbol);
            if (opt.isEmpty()) return;
            PositionEntity entity = opt.get();
            entity.setSlOrderId(slOrderId);
            entity.setTargetOrderId(targetOrderId);
            entity.setSlPrice(slPrice);
            entity.setTargetPrice(targetPrice);
            positionRepo.save(entity);
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to save OCO state for {}: {}", symbol, e.getMessage());
        }
    }

    @Transactional
    public void clear(String symbol) {
        try {
            positionRepo.deleteBySymbol(symbol);
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to clear {}: {}", symbol, e.getMessage());
        }
    }

    /** Returns null if no persisted state exists for this symbol. */
    public Map<String, Object> load(String symbol) {
        try {
            Optional<PositionEntity> opt = positionRepo.findBySymbol(symbol);
            if (opt.isEmpty()) return null;
            return entityToMap(opt.get());
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to load {}: {}", symbol, e.getMessage());
            return null;
        }
    }

    /** Deletes all persisted positions. */
    @Transactional
    public void clearAll() {
        try {
            positionRepo.deleteAll();
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to clearAll: {}", e.getMessage());
        }
    }

    /** Loads all persisted positions. */
    public Map<String, Map<String, Object>> loadAll() {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        try {
            List<PositionEntity> all = positionRepo.findAll();
            for (PositionEntity entity : all) {
                result.put(entity.getSymbol(), entityToMap(entity));
            }
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to loadAll: {}", e.getMessage());
        }
        return result;
    }

    private Map<String, Object> entityToMap(PositionEntity e) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("symbol",          e.getSymbol());
        map.put("side",            e.getSide());
        map.put("qty",             e.getQty());
        map.put("avgPrice",        e.getAvgPrice());
        map.put("setup",           e.getSetup());
        map.put("entryTime",       e.getEntryTime());
        map.put("slPrice",         e.getSlPrice());
        map.put("targetPrice",     e.getTargetPrice());
        map.put("slOrderId",       e.getSlOrderId());
        map.put("targetOrderId",   e.getTargetOrderId());
        map.put("description",     e.getDescription());
        map.put("probability",     e.getProbability());
        map.put("niftyHurdleGuardLow",  e.getNiftyHurdleGuardLow());
        map.put("niftyHurdleGuardHigh", e.getNiftyHurdleGuardHigh());
        map.put("niftyTrendAtEntry",    e.getNiftyTrendAtEntry());
        return map;
    }

    /** Persist the NIFTY sticky-trend state at entry-fill time. Reads the value back into
     *  the positions API so the UI can flag a red stripe when current trend diverges. */
    public void saveNiftyTrendAtEntry(String symbol, String trend) {
        try {
            positionRepo.findBySymbol(symbol).ifPresent(entity -> {
                entity.setNiftyTrendAtEntry(trend != null ? trend : "");
                positionRepo.save(entity);
            });
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to saveNiftyTrendAtEntry for {}: {}", symbol, e.getMessage());
        }
    }

    /**
     * Persist the NIFTY HTF Hurdle break-guard levels onto an existing position record.
     * Captured at entry when the trade was gated by an active NIFTY 15-min hurdle
     * confirmation. {@code low} is defended for LONG positions, {@code high} for SHORT.
     * Pass null/0 to clear.
     */
    public void saveNiftyHurdleGuard(String symbol, Double low, Double high) {
        try {
            positionRepo.findBySymbol(symbol).ifPresent(entity -> {
                entity.setNiftyHurdleGuardLow(low);
                entity.setNiftyHurdleGuardHigh(high);
                positionRepo.save(entity);
            });
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to saveNiftyHurdleGuard for {}: {}", symbol, e.getMessage());
        }
    }

    /** Updates the probability field for a position. */
    public void saveProbability(String symbol, String probability) {
        try {
            positionRepo.findBySymbol(symbol).ifPresent(entity -> {
                entity.setProbability(probability != null ? probability : "");
                positionRepo.save(entity);
            });
        } catch (Exception e) {
            log.error("[PositionStateStore] Failed to saveProbability for {}: {}", symbol, e.getMessage());
        }
    }
}
