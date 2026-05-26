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

    /** Create a new stock. Body: { ticker, name, primaryIndexId, enabled, membership }. */
    @PostMapping("/stocks")
    @Transactional
    public ResponseEntity<?> createStock(@RequestBody Map<String, Object> body) {
        String ticker = stringOrNull(body.get("ticker"));
        String name = stringOrNull(body.get("name"));
        Long primaryIndexId = longOrNull(body.get("primaryIndexId"));
        boolean enabled = body.get("enabled") == null || Boolean.parseBoolean(body.get("enabled").toString());
        StockEntity.Membership membership = parseMembership(body.get("membership"));

        if (ticker == null || ticker.isBlank()) return ResponseEntity.badRequest().body(Map.of("error", "ticker required"));
        ticker = ticker.trim().toUpperCase();
        if (stockRepo.existsByTicker(ticker)) return ResponseEntity.badRequest().body(Map.of("error", "ticker already exists"));

        IndexEntity primaryIndex = null;
        if (primaryIndexId != null) primaryIndex = indexRepo.findById(primaryIndexId).orElse(null);

        StockEntity created = stockRepo.save(new StockEntity(ticker, name, primaryIndex, enabled, membership));
        return ResponseEntity.ok(toStockDto(created));
    }

    /** Update a stock. Body fields are optional: { name, primaryIndexId, enabled, membership }. */
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
        if (body.containsKey("membership")) s.setMembership(parseMembership(body.get("membership")));

        return ResponseEntity.ok(toStockDto(stockRepo.save(s)));
    }

    private static StockEntity.Membership parseMembership(Object o) {
        if (o == null) return StockEntity.Membership.OTHERS;
        String v = o.toString().trim().toUpperCase().replace(' ', '_').replace('-', '_');
        try {
            return StockEntity.Membership.valueOf(v);
        } catch (IllegalArgumentException e) {
            return StockEntity.Membership.OTHERS;
        }
    }

    /** Delete a stock by id. */
    @DeleteMapping("/stocks/{id}")
    @Transactional
    public ResponseEntity<?> deleteStock(@PathVariable Long id) {
        if (!stockRepo.existsById(id)) return ResponseEntity.notFound().build();
        stockRepo.deleteById(id);
        return ResponseEntity.ok(Map.of("deleted", id));
    }

    /** Bulk add stocks by ticker. Body: { tickers: ["BANDHANBNK","LICI",...] }. Used by the
     *  F&O Audit modal to seed new F&O stocks NSE has added. Creates each missing ticker with
     *  name=ticker (placeholder), primaryIndex=NIFTY50 (broad-market default — user re-points
     *  via the per-row edit), enabled=true, membership=OTHERS. Skips tickers that already
     *  exist. Returns counts. */
    @PostMapping("/stocks/bulk-add")
    @Transactional
    public Map<String, Object> bulkAdd(@RequestBody Map<String, Object> body) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> tickers = new ArrayList<>();
        if (body != null && body.get("tickers") instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof String s && !s.isBlank()) tickers.add(s.trim().toUpperCase());
            }
        }
        // Default primary index for new bulk-adds = NIFTY50 (broad market). User can re-point
        // via the per-row Edit action once the rows are inserted.
        IndexEntity defaultIndex = indexRepo.findByTicker("NIFTY50").orElse(null);
        List<String> added = new ArrayList<>();
        List<String> alreadyExists = new ArrayList<>();
        for (String t : tickers) {
            if (stockRepo.existsByTicker(t)) { alreadyExists.add(t); continue; }
            StockEntity s = new StockEntity(t, t, defaultIndex, true, StockEntity.Membership.OTHERS);
            stockRepo.save(s);
            added.add(t);
        }
        result.put("requested",      tickers.size());
        result.put("added",          added.size());
        result.put("alreadyExists",  alreadyExists.size());
        result.put("addedTickers",   added);
        result.put("defaultIndex",   defaultIndex != null ? defaultIndex.getTicker() : null);
        return result;
    }

    /** Bulk disable stocks by ticker. Body: { tickers: ["SUNTV","ZEEL",...] }. Used by the
     *  F&O Audit modal to disable stocks NSE has dropped from F&O. Returns counts of
     *  matched/disabled/skipped (already-disabled or not-found) tickers. */
    @PostMapping("/stocks/bulk-disable")
    @Transactional
    public Map<String, Object> bulkDisable(@RequestBody Map<String, Object> body) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> tickers = new ArrayList<>();
        if (body != null && body.get("tickers") instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof String s && !s.isBlank()) tickers.add(s.trim().toUpperCase());
            }
        }
        List<String> disabled = new ArrayList<>();
        List<String> alreadyDisabled = new ArrayList<>();
        List<String> notFound = new ArrayList<>();
        for (String t : tickers) {
            var opt = stockRepo.findByTicker(t);
            if (opt.isEmpty()) { notFound.add(t); continue; }
            StockEntity s = opt.get();
            if (!s.isEnabled()) { alreadyDisabled.add(t); continue; }
            s.setEnabled(false);
            stockRepo.save(s);
            disabled.add(t);
        }
        result.put("requested",        tickers.size());
        result.put("disabled",         disabled.size());
        result.put("alreadyDisabled",  alreadyDisabled.size());
        result.put("notFound",         notFound.size());
        result.put("disabledTickers",  disabled);
        result.put("notFoundTickers",  notFound);
        return result;
    }

    private static Map<String, Object> toStockDto(StockEntity s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", s.getId());
        m.put("ticker", s.getTicker());
        m.put("name", s.getName());
        m.put("enabled", s.isEnabled());
        m.put("membership", s.getMembership() != null ? s.getMembership().name() : StockEntity.Membership.OTHERS.name());
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
