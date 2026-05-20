package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.entity.IndexEntity;
import com.rydytrader.autotrader.entity.StockEntity;
import com.rydytrader.autotrader.repository.IndexRepository;
import com.rydytrader.autotrader.repository.StockRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * CRUD endpoints for the DB-backed stock + index universe used by the scanner.
 * Settings UI consumes these to render an editable table of stocks with primary-index
 * dropdown, enabled toggle, and add/edit/delete actions. Replaces the old sector-based
 * model — each stock now maps directly to one primary index (NIFTY 50 or a sector
 * index), driving the bot's alignment / HTF hurdle / 5m hurdle filters.
 */
@RestController
@RequestMapping("/api")
public class StockUniverseController {

    private final IndexRepository indexRepo;
    private final StockRepository stockRepo;

    public StockUniverseController(IndexRepository indexRepo, StockRepository stockRepo) {
        this.indexRepo = indexRepo;
        this.stockRepo = stockRepo;
    }

    /** List all indices — feeds the primary-index dropdown in the Settings UI. */
    @GetMapping("/indices")
    public List<Map<String, Object>> listIndices() {
        List<IndexEntity> indices = indexRepo.findAll();
        indices.sort(Comparator.comparing(IndexEntity::getName, String.CASE_INSENSITIVE_ORDER));
        List<Map<String, Object>> out = new ArrayList<>(indices.size());
        for (IndexEntity i : indices) out.add(toIndexDto(i));
        return out;
    }

    /** Create an index. Body: { name, ticker }. */
    @PostMapping("/indices")
    @Transactional
    public ResponseEntity<?> createIndex(@RequestBody Map<String, Object> body) {
        String name = stringOrNull(body.get("name"));
        String ticker = stringOrNull(body.get("ticker"));
        if (name == null) return ResponseEntity.badRequest().body(Map.of("error", "name required"));
        if (ticker == null) return ResponseEntity.badRequest().body(Map.of("error", "ticker required"));
        if (indexRepo.findByName(name).isPresent()) {
            return ResponseEntity.badRequest().body(Map.of("error", "index with this name already exists"));
        }
        if (indexRepo.existsByTicker(ticker)) {
            return ResponseEntity.badRequest().body(Map.of("error", "index with this ticker already exists"));
        }
        IndexEntity created = indexRepo.save(new IndexEntity(name, ticker));
        return ResponseEntity.ok(toIndexDto(created));
    }

    /** Delete an index by id. Refuses if any stock still references it (FK guard). */
    @DeleteMapping("/indices/{id}")
    @Transactional
    public ResponseEntity<?> deleteIndex(@PathVariable Long id) {
        IndexEntity i = indexRepo.findById(id).orElse(null);
        if (i == null) return ResponseEntity.notFound().build();
        long stocksUsing = stockRepo.findAll().stream()
            .filter(st -> st.getPrimaryIndex() != null && id.equals(st.getPrimaryIndex().getId()))
            .count();
        if (stocksUsing > 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Cannot delete — " + stocksUsing + " stock(s) still mapped to this index"
            ));
        }
        indexRepo.deleteById(id);
        return ResponseEntity.ok(Map.of("deleted", id));
    }

    /** Update an index. Body fields are optional: { name, ticker }. */
    @PutMapping("/indices/{id}")
    @Transactional
    public ResponseEntity<?> updateIndex(@PathVariable Long id, @RequestBody Map<String, Object> body) {
        IndexEntity i = indexRepo.findById(id).orElse(null);
        if (i == null) return ResponseEntity.notFound().build();

        if (body.containsKey("name")) {
            String name = stringOrNull(body.get("name"));
            if (name == null) return ResponseEntity.badRequest().body(Map.of("error", "name cannot be blank"));
            var collision = indexRepo.findByName(name);
            if (collision.isPresent() && !collision.get().getId().equals(i.getId())) {
                return ResponseEntity.badRequest().body(Map.of("error", "another index already uses this name"));
            }
            i.setName(name);
        }
        if (body.containsKey("ticker")) {
            String ticker = stringOrNull(body.get("ticker"));
            if (ticker == null) return ResponseEntity.badRequest().body(Map.of("error", "ticker cannot be blank"));
            var collision = indexRepo.findByTicker(ticker);
            if (collision.isPresent() && !collision.get().getId().equals(i.getId())) {
                return ResponseEntity.badRequest().body(Map.of("error", "another index already uses this ticker"));
            }
            i.setTicker(ticker);
        }

        return ResponseEntity.ok(toIndexDto(indexRepo.save(i)));
    }

    /** List all stocks. Includes disabled stocks so the Settings UI can toggle them on. */
    @GetMapping("/stocks")
    public List<Map<String, Object>> listStocks() {
        List<StockEntity> stocks = stockRepo.findAll();
        stocks.sort(Comparator.comparing(StockEntity::getTicker, String.CASE_INSENSITIVE_ORDER));
        List<Map<String, Object>> out = new ArrayList<>(stocks.size());
        for (StockEntity s : stocks) out.add(toStockDto(s));
        return out;
    }

    /** Create a new stock. Body: { ticker, name, primaryIndexId, enabled }. */
    @PostMapping("/stocks")
    @Transactional
    public ResponseEntity<?> createStock(@RequestBody Map<String, Object> body) {
        String ticker = stringOrNull(body.get("ticker"));
        String name = stringOrNull(body.get("name"));
        Long primaryIndexId = longOrNull(body.get("primaryIndexId"));
        boolean enabled = body.get("enabled") == null || Boolean.parseBoolean(body.get("enabled").toString());

        if (ticker == null || ticker.isBlank()) return ResponseEntity.badRequest().body(Map.of("error", "ticker required"));
        ticker = ticker.trim().toUpperCase();
        if (stockRepo.existsByTicker(ticker)) return ResponseEntity.badRequest().body(Map.of("error", "ticker already exists"));

        IndexEntity primaryIndex = null;
        if (primaryIndexId != null) primaryIndex = indexRepo.findById(primaryIndexId).orElse(null);

        StockEntity created = stockRepo.save(new StockEntity(ticker, name, primaryIndex, enabled));
        return ResponseEntity.ok(toStockDto(created));
    }

    /** Update a stock. Body fields are optional: { name, primaryIndexId, enabled }. */
    @PutMapping("/stocks/{id}")
    @Transactional
    public ResponseEntity<?> updateStock(@PathVariable Long id, @RequestBody Map<String, Object> body) {
        StockEntity s = stockRepo.findById(id).orElse(null);
        if (s == null) return ResponseEntity.notFound().build();

        if (body.containsKey("name")) s.setName(stringOrNull(body.get("name")));
        if (body.containsKey("primaryIndexId")) {
            Long primaryIndexId = longOrNull(body.get("primaryIndexId"));
            s.setPrimaryIndex(primaryIndexId == null ? null : indexRepo.findById(primaryIndexId).orElse(null));
        }
        if (body.containsKey("enabled")) s.setEnabled(Boolean.parseBoolean(body.get("enabled").toString()));

        return ResponseEntity.ok(toStockDto(stockRepo.save(s)));
    }

    /** Delete a stock by id. */
    @DeleteMapping("/stocks/{id}")
    @Transactional
    public ResponseEntity<?> deleteStock(@PathVariable Long id) {
        if (!stockRepo.existsById(id)) return ResponseEntity.notFound().build();
        stockRepo.deleteById(id);
        return ResponseEntity.ok(Map.of("deleted", id));
    }

    private static Map<String, Object> toStockDto(StockEntity s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", s.getId());
        m.put("ticker", s.getTicker());
        m.put("name", s.getName());
        m.put("enabled", s.isEnabled());
        if (s.getPrimaryIndex() != null) {
            m.put("primaryIndexId", s.getPrimaryIndex().getId());
            m.put("primaryIndexName", s.getPrimaryIndex().getName());
            m.put("primaryIndexTicker", s.getPrimaryIndex().getTicker());
        } else {
            m.put("primaryIndexId", null);
            m.put("primaryIndexName", null);
            m.put("primaryIndexTicker", null);
        }
        return m;
    }

    private static Map<String, Object> toIndexDto(IndexEntity i) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", i.getId());
        m.put("name", i.getName());
        m.put("ticker", i.getTicker());
        return m;
    }

    private static String stringOrNull(Object o) {
        if (o == null) return null;
        String s = o.toString();
        return s.isBlank() ? null : s;
    }

    private static Long longOrNull(Object o) {
        if (o == null) return null;
        try {
            if (o instanceof Number n) return n.longValue();
            return Long.parseLong(o.toString());
        } catch (Exception e) {
            return null;
        }
    }
}
