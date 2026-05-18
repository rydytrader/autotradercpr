package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.entity.SectorEntity;
import com.rydytrader.autotrader.entity.StockEntity;
import com.rydytrader.autotrader.repository.SectorRepository;
import com.rydytrader.autotrader.repository.StockRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * CRUD endpoints for the DB-backed stock + sector universe used by the scanner.
 * Settings UI consumes these to render an editable table of stocks with sector
 * dropdown, enabled toggle, and add/edit/delete actions.
 */
@RestController
@RequestMapping("/api")
public class StockUniverseController {

    private final SectorRepository sectorRepo;
    private final StockRepository stockRepo;

    public StockUniverseController(SectorRepository sectorRepo, StockRepository stockRepo) {
        this.sectorRepo = sectorRepo;
        this.stockRepo = stockRepo;
    }

    /** List all sectors — feeds the sector dropdown in the Settings UI. */
    @GetMapping("/sectors")
    public List<Map<String, Object>> listSectors() {
        List<SectorEntity> sectors = sectorRepo.findAll();
        sectors.sort(Comparator.comparing(SectorEntity::getName, String.CASE_INSENSITIVE_ORDER));
        List<Map<String, Object>> out = new ArrayList<>(sectors.size());
        for (SectorEntity s : sectors) out.add(toSectorDto(s));
        return out;
    }

    /** Create a sector. Body: { name, indexTicker }. */
    @PostMapping("/sectors")
    @Transactional
    public ResponseEntity<?> createSector(@RequestBody Map<String, Object> body) {
        String name = stringOrNull(body.get("name"));
        if (name == null) return ResponseEntity.badRequest().body(Map.of("error", "name required"));
        if (sectorRepo.findByName(name).isPresent()) {
            return ResponseEntity.badRequest().body(Map.of("error", "sector with this name already exists"));
        }
        String indexTicker = stringOrNull(body.get("indexTicker"));
        SectorEntity created = sectorRepo.save(new SectorEntity(name, indexTicker));
        return ResponseEntity.ok(toSectorDto(created));
    }

    /** Delete a sector by id. Refuses if any stock still references it (FK guard). */
    @DeleteMapping("/sectors/{id}")
    @Transactional
    public ResponseEntity<?> deleteSector(@PathVariable Long id) {
        SectorEntity s = sectorRepo.findById(id).orElse(null);
        if (s == null) return ResponseEntity.notFound().build();
        long stocksInSector = stockRepo.findAll().stream()
            .filter(st -> st.getSector() != null && id.equals(st.getSector().getId()))
            .count();
        if (stocksInSector > 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Cannot delete — " + stocksInSector + " stock(s) still mapped to this sector"
            ));
        }
        sectorRepo.deleteById(id);
        return ResponseEntity.ok(Map.of("deleted", id));
    }

    /** Update a sector. Body fields are optional: { name, indexTicker }. */
    @PutMapping("/sectors/{id}")
    @Transactional
    public ResponseEntity<?> updateSector(@PathVariable Long id, @RequestBody Map<String, Object> body) {
        SectorEntity s = sectorRepo.findById(id).orElse(null);
        if (s == null) return ResponseEntity.notFound().build();

        if (body.containsKey("name")) {
            String name = stringOrNull(body.get("name"));
            if (name == null) return ResponseEntity.badRequest().body(Map.of("error", "name cannot be blank"));
            // Reject rename if it collides with another row (the unique constraint would
            // throw, but a clean 400 is friendlier for the UI).
            var collision = sectorRepo.findByName(name);
            if (collision.isPresent() && !collision.get().getId().equals(s.getId())) {
                return ResponseEntity.badRequest().body(Map.of("error", "another sector already uses this name"));
            }
            s.setName(name);
        }
        if (body.containsKey("indexTicker")) s.setIndexTicker(stringOrNull(body.get("indexTicker")));

        return ResponseEntity.ok(toSectorDto(sectorRepo.save(s)));
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

    /** Create a new stock. Body: { ticker, name, sectorId, enabled }. */
    @PostMapping("/stocks")
    @Transactional
    public ResponseEntity<?> createStock(@RequestBody Map<String, Object> body) {
        String ticker = stringOrNull(body.get("ticker"));
        String name = stringOrNull(body.get("name"));
        Long sectorId = longOrNull(body.get("sectorId"));
        boolean enabled = body.get("enabled") == null || Boolean.parseBoolean(body.get("enabled").toString());

        if (ticker == null || ticker.isBlank()) return ResponseEntity.badRequest().body(Map.of("error", "ticker required"));
        ticker = ticker.trim().toUpperCase();
        if (stockRepo.existsByTicker(ticker)) return ResponseEntity.badRequest().body(Map.of("error", "ticker already exists"));

        SectorEntity sector = null;
        if (sectorId != null) sector = sectorRepo.findById(sectorId).orElse(null);

        StockEntity created = stockRepo.save(new StockEntity(ticker, name, sector, enabled));
        return ResponseEntity.ok(toStockDto(created));
    }

    /** Update a stock. Body fields are optional: { name, sectorId, enabled }. */
    @PutMapping("/stocks/{id}")
    @Transactional
    public ResponseEntity<?> updateStock(@PathVariable Long id, @RequestBody Map<String, Object> body) {
        StockEntity s = stockRepo.findById(id).orElse(null);
        if (s == null) return ResponseEntity.notFound().build();

        if (body.containsKey("name")) s.setName(stringOrNull(body.get("name")));
        if (body.containsKey("sectorId")) {
            Long sectorId = longOrNull(body.get("sectorId"));
            s.setSector(sectorId == null ? null : sectorRepo.findById(sectorId).orElse(null));
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
        if (s.getSector() != null) {
            m.put("sectorId", s.getSector().getId());
            m.put("sectorName", s.getSector().getName());
            m.put("sectorIndexTicker", s.getSector().getIndexTicker());
        } else {
            m.put("sectorId", null);
            m.put("sectorName", null);
            m.put("sectorIndexTicker", null);
        }
        return m;
    }

    private static Map<String, Object> toSectorDto(SectorEntity s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", s.getId());
        m.put("name", s.getName());
        m.put("indexTicker", s.getIndexTicker());
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
