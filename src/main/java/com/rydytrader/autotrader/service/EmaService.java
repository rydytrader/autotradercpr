package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Exponential Moving Average (EMA) service for the 20-period EMA on each candle close.
 * EMA reacts faster to recent price than SMA (weight = 2/(period+1) ≈ 9.5% for period 20).
 * Recurrence: ema = alpha * close + (1 - alpha) * prev_ema. First bar seeded with close;
 * a symbol is treated as "loaded" once it has accumulated >= 20 bars.
 * Used by BreakoutScanner for directional trade confirmation.
 */
@Service
public class EmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(EmaService.class);
    private static final int    EMA_PERIOD = 20;
    private static final double ALPHA      = 2.0 / (EMA_PERIOD + 1); // 2/21 ≈ 0.0952

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE        = "../store/cache/ema-5min.json";
    private static final String LEGACY_SMA_CACHE  = "../store/cache/sma-5min.json";

    private final CandleAggregator candleAggregator;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService marketHolidayService;
    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private AtrService atrService;
    private final ObjectMapper mapper = new ObjectMapper();

    // Per-symbol current EMA value (only valid once a candle close has seeded it).
    private final ConcurrentHashMap<String, Double>  emaBySymbol      = new ConcurrentHashMap<>();
    // Per-symbol bar-count fed into the EMA — used for the >=20 "loaded" gate.
    private final ConcurrentHashMap<String, Integer> barCountBySymbol = new ConcurrentHashMap<>();
    // Per-symbol last applied candle epoch — used by AtrService catch-up to skip bars already baked in.
    private final ConcurrentHashMap<String, Long>    lastCandleEpochBySymbol = new ConcurrentHashMap<>();
    private volatile boolean seededFromCache = false;

    public EmaService(CandleAggregator candleAggregator) {
        this.candleAggregator = candleAggregator;
    }

    /** Restore EMA state + ATR from disk cache before scanner init tries to seed from Fyers. */
    @PostConstruct
    public void init() {
        loadCache();
    }

    public boolean isSeededFromCache()             { return seededFromCache; }
    public boolean hasCachedSymbol(String symbol)  { return emaBySymbol.containsKey(symbol); }
    public long    getLastCandleEpoch(String sym)  { return lastCandleEpochBySymbol.getOrDefault(sym, 0L); }
    public void    flushCache()                    { saveCache(); }

    /**
     * Get current EMA(20) for a symbol. Returns 0 if not enough data (< period bars).
     * During market hours blends the live LTP as the in-progress bar's partial close:
     * {@code ema_blended = alpha * ltp + (1 - alpha) * ema_prev}. Outside market hours
     * returns the completed-bar EMA so the value stays in sync with TV's static post-market
     * display rather than wandering with closing-auction LTPs.
     */
    public double getEma(String symbol) {
        Integer bars = barCountBySymbol.get(symbol);
        if (bars == null || bars < EMA_PERIOD) return 0;
        Double prev = emaBySymbol.get(symbol);
        if (prev == null) return 0;
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) {
            return prev;
        }
        double ltp = candleAggregator.getLtp(symbol);
        if (ltp <= 0) return prev;
        return ALPHA * ltp + (1 - ALPHA) * prev;
    }

    /** Completed-bars-only EMA — value as of the last candle close. */
    private double getEmaCompleted(String symbol) {
        Integer bars = barCountBySymbol.get(symbol);
        if (bars == null || bars < EMA_PERIOD) return 0;
        Double prev = emaBySymbol.get(symbol);
        return prev != null ? prev : 0;
    }

    /**
     * Seed EMA from an explicit list of candles. Walks the closes through the EMA
     * recurrence, seeding the first bar with its close. Replaces any existing EMA state
     * for the symbol.
     */
    public void seedFromCandles(String symbol, List<CandleAggregator.CandleBar> candles) {
        if (candles == null || candles.size() < EMA_PERIOD) return;

        Double ema = null;
        int bars = 0;
        for (CandleAggregator.CandleBar c : candles) {
            double close = c.close;
            if (close <= 0) continue;
            if (ema == null) ema = close;
            else             ema = ALPHA * close + (1 - ALPHA) * ema;
            bars++;
        }
        if (ema == null) return;
        emaBySymbol.put(symbol, ema);
        barCountBySymbol.put(symbol, bars);

        CandleAggregator.CandleBar last = candles.get(candles.size() - 1);
        if (last != null && last.epochSec > 0) {
            lastCandleEpochBySymbol.put(symbol, last.epochSec);
        }
    }

    /** Seed from CandleAggregator.getCompletedCandles — convenience wrapper. */
    public void seedFromHistory(String symbol) {
        List<CandleAggregator.CandleBar> candles = candleAggregator.getCompletedCandles(symbol);
        seedFromCandles(symbol, candles);
    }

    public int getLoadedCount()           { return countWithAtLeast(EMA_PERIOD); }

    private int countWithAtLeast(int period) {
        int n = 0;
        for (Integer bars : barCountBySymbol.values()) {
            if (bars != null && bars >= period) n++;
        }
        return n;
    }

    /** Watchlist-scoped loaded counts for dashboard stats. */
    public int getLoadedCountFor(java.util.Collection<String> symbols) {
        return countForWithAtLeast(symbols, EMA_PERIOD);
    }

    private int countForWithAtLeast(java.util.Collection<String> symbols, int period) {
        int n = 0;
        for (String s : symbols) {
            Integer bars = barCountBySymbol.get(s);
            if (bars != null && bars >= period) n++;
        }
        return n;
    }

    public Map<String, Double> getAllEma() {
        java.util.Map<String, Double> out = new java.util.HashMap<>();
        for (String s : emaBySymbol.keySet()) out.put(s, getEma(s));
        return out;
    }

    /**
     * Drop any EMA entries whose symbol isn't in the current watchlist.
     */
    public synchronized int pruneTo(java.util.Collection<String> watchlist) {
        java.util.Set<String> keep = new java.util.HashSet<>(watchlist);
        int before = emaBySymbol.size();
        emaBySymbol.keySet().retainAll(keep);
        barCountBySymbol.keySet().retainAll(keep);
        lastCandleEpochBySymbol.keySet().retainAll(keep);
        int removed = before - emaBySymbol.size();
        if (removed > 0) {
            log.info("[EmaService] Pruned {} stale EMA entries not in watchlist ({} remaining)", removed, emaBySymbol.size());
            saveCache();
        }
        return removed;
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (completedCandle == null || completedCandle.close <= 0) return;

        double close = completedCandle.close;
        Double prev  = emaBySymbol.get(fyersSymbol);
        double ema   = prev == null ? close : ALPHA * close + (1 - ALPHA) * prev;
        emaBySymbol.put(fyersSymbol, ema);
        barCountBySymbol.merge(fyersSymbol, 1, Integer::sum);

        double ema20 = getEmaCompleted(fyersSymbol);
        if (ema20 > 0) {
            completedCandle.ema20 = ema20;
        }

        lastCandleEpochBySymbol.put(fyersSymbol, completedCandle.epochSec);
        saveCache();
    }

    // ── Disk cache ─────────────────────────────────────────────────────────────

    private synchronized void saveCache() {
        try {
            Path path = Paths.get(CACHE_FILE);
            Files.createDirectories(path.getParent());
            ObjectNode root = mapper.createObjectNode();
            root.put("savedAt", Instant.now().atZone(IST).toString());
            ObjectNode bySymbol = root.putObject("bySymbol");
            for (Map.Entry<String, Double> e : emaBySymbol.entrySet()) {
                String sym = e.getKey();
                ObjectNode entry = bySymbol.putObject(sym);
                entry.put("ema", e.getValue());
                Integer bars = barCountBySymbol.get(sym);
                if (bars != null) entry.put("barCount", bars);
                double atr = atrService != null ? atrService.getAtr(sym) : 0;
                if (atr > 0) entry.put("atr", atr);
                Long lastEpoch = lastCandleEpochBySymbol.get(sym);
                if (lastEpoch != null) entry.put("lastCandleEpoch", lastEpoch);
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(root));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, path);
        } catch (Exception e) {
            log.error("[CACHE] EmaService saveCache failed: {}", e.getMessage());
        }
    }

    private synchronized void loadCache() {
        try {
            Path path = Paths.get(CACHE_FILE);
            if (!Files.exists(path)) {
                migrateFromLegacySmaCache();
                return;
            }
            JsonNode root = mapper.readTree(Files.readString(path));
            String savedAtStr = root.has("savedAt") ? root.get("savedAt").asText("") : "";
            if (savedAtStr.isEmpty()) return;
            LocalDate cacheDate;
            try { cacheDate = LocalDate.parse(savedAtStr.substring(0, 10)); }
            catch (Exception e) { return; }
            LocalDate today = LocalDate.now(IST);
            LocalDate lastTradingDay = marketHolidayService != null
                ? marketHolidayService.getLastTradingDay() : today;
            if (!cacheDate.equals(today) && !cacheDate.equals(lastTradingDay)) {
                log.info("[CACHE] EMA cache stale (cacheDate={}, today={}, lastTradingDay={}) — will full-fetch",
                    cacheDate, today, lastTradingDay);
                return;
            }
            JsonNode bySymbol = root.get("bySymbol");
            if (bySymbol == null || !bySymbol.isObject()) return;
            int count = 0;
            Iterator<Map.Entry<String, JsonNode>> it = bySymbol.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String sym = e.getKey();
                JsonNode entry = e.getValue();
                double ema = entry.path("ema").asDouble(0);
                int bars   = entry.path("barCount").asInt(0);
                if (ema <= 0 || bars <= 0) continue;
                emaBySymbol.put(sym, ema);
                barCountBySymbol.put(sym, bars);
                long lastEpoch = entry.path("lastCandleEpoch").asLong(0);
                if (lastEpoch > 0) lastCandleEpochBySymbol.put(sym, lastEpoch);
                double atr = entry.path("atr").asDouble(0);
                if (atr > 0 && atrService != null) atrService.primeFromCache(sym, atr);
                count++;
            }
            seededFromCache = count > 0;
            if (seededFromCache) {
                log.info("[CACHE] EMA seed restored from cache (savedAt={}, {} symbols)", savedAtStr, count);
            }
        } catch (Exception e) {
            log.error("[CACHE] EmaService loadCache failed: {}", e.getMessage());
        }
    }

    /**
     * One-time migration: if no EMA cache exists but the legacy SMA cache does, replay each
     * symbol's stored closes through the EMA recurrence to derive an initial EMA value. This
     * is a rough seed — the true EMA depends on all prior closes — but with just 20 closes it
     * converges quickly once live bars start flowing. Deletes the legacy file afterwards so
     * the migration only runs once.
     */
    private void migrateFromLegacySmaCache() {
        try {
            Path legacy = Paths.get(LEGACY_SMA_CACHE);
            if (!Files.exists(legacy)) return;
            JsonNode root = mapper.readTree(Files.readString(legacy));
            String savedAtStr = root.has("savedAt") ? root.get("savedAt").asText("") : "";
            LocalDate cacheDate = null;
            try { if (!savedAtStr.isEmpty()) cacheDate = LocalDate.parse(savedAtStr.substring(0, 10)); }
            catch (Exception ignored) {}
            LocalDate today = LocalDate.now(IST);
            LocalDate lastTradingDay = marketHolidayService != null
                ? marketHolidayService.getLastTradingDay() : today;
            if (cacheDate == null || (!cacheDate.equals(today) && !cacheDate.equals(lastTradingDay))) {
                // Stale legacy cache — delete and let live seed take over.
                Files.deleteIfExists(legacy);
                log.info("[CACHE] Deleted stale legacy SMA cache (savedAt={})", savedAtStr);
                return;
            }
            JsonNode bySymbol = root.get("bySymbol");
            if (bySymbol == null || !bySymbol.isObject()) {
                Files.deleteIfExists(legacy);
                return;
            }
            int count = 0;
            Iterator<Map.Entry<String, JsonNode>> it = bySymbol.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String sym = e.getKey();
                JsonNode entry = e.getValue();
                JsonNode closesNode = entry.get("closes");
                if (closesNode == null || !closesNode.isArray() || closesNode.size() == 0) continue;
                Double ema = null;
                int bars = 0;
                for (JsonNode v : closesNode) {
                    double close = v.asDouble(0);
                    if (close <= 0) continue;
                    if (ema == null) ema = close;
                    else             ema = ALPHA * close + (1 - ALPHA) * ema;
                    bars++;
                }
                if (ema == null) continue;
                emaBySymbol.put(sym, ema);
                barCountBySymbol.put(sym, bars);
                long lastEpoch = entry.path("lastCandleEpoch").asLong(0);
                if (lastEpoch > 0) lastCandleEpochBySymbol.put(sym, lastEpoch);
                double atr = entry.path("atr").asDouble(0);
                if (atr > 0 && atrService != null) atrService.primeFromCache(sym, atr);
                count++;
            }
            seededFromCache = count > 0;
            if (seededFromCache) {
                log.info("[CACHE] Migrated {} symbols from legacy SMA cache", count);
            }
            Files.deleteIfExists(legacy);
            // Persist the migrated values to the new cache file straight away so a crash mid-session
            // doesn't lose what we just derived.
            saveCache();
        } catch (Exception e) {
            log.warn("[CACHE] Legacy SMA cache migration failed: {}", e.getMessage());
        }
    }
}
