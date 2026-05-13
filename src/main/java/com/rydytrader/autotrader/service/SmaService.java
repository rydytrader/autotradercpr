package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple Moving Average (SMA) service for the 20-period SMA on each candle close.
 * SMA is exact from a rolling close window and converges to TradingView values
 * without depending on seed depth. Used by BreakoutScanner for directional trade
 * confirmation.
 */
@Service
public class SmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(SmaService.class);
    private static final int SMA_PERIOD = 20;

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE = "../store/cache/sma-5min.json";
    private static final String LEGACY_CACHE_FILE = "../store/cache/ema-5min.json";

    private final CandleAggregator candleAggregator;
    private final com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService marketHolidayService;
    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private AtrService atrService;
    private final ObjectMapper mapper = new ObjectMapper();

    // Rolling window of last SMA_PERIOD closes per symbol — used to derive SMA(20) on demand.
    private final ConcurrentHashMap<String, Deque<Double>> closesBySymbol = new ConcurrentHashMap<>();
    // Per-symbol last applied candle epoch — used by AtrService catch-up to skip bars already baked in.
    private final ConcurrentHashMap<String, Long> lastCandleEpochBySymbol = new ConcurrentHashMap<>();
    private volatile boolean seededFromCache = false;

    public SmaService(CandleAggregator candleAggregator,
                      com.rydytrader.autotrader.store.RiskSettingsStore riskSettings) {
        this.candleAggregator = candleAggregator;
        this.riskSettings = riskSettings;
    }

    /** Restore SMA state + ATR from disk cache before scanner init tries to seed from Fyers. */
    @PostConstruct
    public void init() {
        deleteLegacyCache();
        loadCache();
    }

    /** One-time migration: remove the pre-SMA ema-5min.json file if still sitting on disk. */
    private void deleteLegacyCache() {
        try {
            Path legacy = Paths.get(LEGACY_CACHE_FILE);
            if (Files.exists(legacy)) {
                Files.delete(legacy);
                log.info("[CACHE] Deleted legacy EMA cache at {}", legacy);
            }
        } catch (Exception e) {
            log.warn("[CACHE] Could not delete legacy EMA cache: {}", e.getMessage());
        }
    }

    public boolean isSeededFromCache()             { return seededFromCache; }
    public boolean hasCachedSymbol(String symbol)  { return closesBySymbol.containsKey(symbol); }
    public long    getLastCandleEpoch(String sym)  { return lastCandleEpochBySymbol.getOrDefault(sym, 0L); }
    public void    flushCache()                    { saveCache(); }

    /** Get current SMA(20) for a symbol. Returns 0 if not enough data. */
    public double getSma(String symbol) {
        return computeSmaBlended(symbol, SMA_PERIOD);
    }

    /**
     * Blend current LTP as the in-progress 5-min bar's partial close. Matches TradingView's
     * intra-bar SMA behaviour: SMA = (last period-1 completed closes + current LTP) / period.
     * Falls back to completed-only when LTP isn't available, or when the market is closed —
     * post-market the LTP keeps moving (closing-auction prints, settlement) but TV's chart
     * freezes at the last bar's value, so we return completed-SMA after 15:30 to stay in sync
     * with TV's static post-market display.
     */
    private double computeSmaBlended(String symbol, int period) {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) {
            return computeSmaCompleted(symbol, period);
        }
        double ltp = candleAggregator.getLtp(symbol);
        if (ltp <= 0) return computeSmaCompleted(symbol, period);
        Deque<Double> closes = closesBySymbol.get(symbol);
        if (closes == null) return 0;
        synchronized (closes) {
            if (closes.size() < period - 1) return 0;
            double sum = ltp;
            int skip = closes.size() - (period - 1);
            int i = 0, count = 1;
            for (Double v : closes) {
                if (i++ < skip) continue;
                sum += v;
                count++;
            }
            return count == period ? sum / period : 0;
        }
    }

    /**
     * Completed-bars-only SMA. Used internally by onCandleClose / seedFromCandles so that
     * bar-close downstream listeners see the canonical post-close SMA.
     */
    private double computeSmaCompleted(String symbol, int period) {
        Deque<Double> closes = closesBySymbol.get(symbol);
        if (closes == null) return 0;
        synchronized (closes) {
            if (closes.size() < period) return 0;
            double sum = 0;
            int count = 0;
            int skip = closes.size() - period;
            int i = 0;
            for (Double v : closes) {
                if (i++ < skip) continue;
                sum += v;
                count++;
            }
            return count == period ? sum / period : 0;
        }
    }

    /**
     * Seed SMA close window from an explicit list of candles. We only keep the last
     * SMA_PERIOD closes for state.
     */
    public void seedFromCandles(String symbol, List<CandleAggregator.CandleBar> candles) {
        if (candles == null || candles.size() < SMA_PERIOD) return;

        Deque<Double> closes = closesBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (closes) {
            closes.clear();
        }

        for (int i = 0; i < candles.size(); i++) {
            double close = candles.get(i).close;
            if (close <= 0) continue;
            synchronized (closes) {
                closes.addLast(close);
                while (closes.size() > SMA_PERIOD) closes.removeFirst();
            }
        }

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

    public int getLoadedCount()           { return countWithAtLeast(SMA_PERIOD); }

    private int countWithAtLeast(int period) {
        int n = 0;
        for (Deque<Double> d : closesBySymbol.values()) {
            synchronized (d) { if (d.size() >= period) n++; }
        }
        return n;
    }

    /** Watchlist-scoped loaded counts for dashboard stats. */
    public int getLoadedCountFor(java.util.Collection<String> symbols) {
        return countForWithAtLeast(symbols, SMA_PERIOD);
    }

    private int countForWithAtLeast(java.util.Collection<String> symbols, int period) {
        int n = 0;
        for (String s : symbols) {
            Deque<Double> d = closesBySymbol.get(s);
            if (d == null) continue;
            synchronized (d) { if (d.size() >= period) n++; }
        }
        return n;
    }

    public Map<String, Double> getAllSma() {
        java.util.Map<String, Double> out = new java.util.HashMap<>();
        for (String s : closesBySymbol.keySet()) out.put(s, getSma(s));
        return out;
    }

    /**
     * Drop any SMA entries whose symbol isn't in the current watchlist.
     */
    public synchronized int pruneTo(java.util.Collection<String> watchlist) {
        java.util.Set<String> keep = new java.util.HashSet<>(watchlist);
        int before = closesBySymbol.size();
        closesBySymbol.keySet().retainAll(keep);
        lastCandleEpochBySymbol.keySet().retainAll(keep);
        int removed = before - closesBySymbol.size();
        if (removed > 0) {
            log.info("[SmaService] Pruned {} stale SMA entries not in watchlist ({} remaining)", removed, closesBySymbol.size());
            saveCache();
        }
        return removed;
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (completedCandle == null || completedCandle.close <= 0) return;

        Deque<Double> closes = closesBySymbol.computeIfAbsent(fyersSymbol, s -> new ArrayDeque<>());
        synchronized (closes) {
            closes.addLast(completedCandle.close);
            while (closes.size() > SMA_PERIOD) closes.removeFirst();
        }

        double sma20 = computeSmaCompleted(fyersSymbol, SMA_PERIOD);
        if (sma20 > 0) {
            completedCandle.sma20 = sma20;
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
            for (Map.Entry<String, Deque<Double>> e : closesBySymbol.entrySet()) {
                String sym = e.getKey();
                ObjectNode entry = bySymbol.putObject(sym);
                entry.set("closes", closesArr(e.getValue()));
                double atr = atrService != null ? atrService.getAtr(sym) : 0;
                if (atr > 0) entry.put("atr", atr);
                Long lastEpoch = lastCandleEpochBySymbol.get(sym);
                if (lastEpoch != null) entry.put("lastCandleEpoch", lastEpoch);
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(root));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, path);
        } catch (Exception e) {
            log.error("[CACHE] SmaService saveCache failed: {}", e.getMessage());
        }
    }

    private ArrayNode closesArr(Deque<Double> deque) {
        ArrayNode arr = mapper.createArrayNode();
        if (deque == null) return arr;
        synchronized (deque) { for (Double v : deque) arr.add(v); }
        return arr;
    }

    private synchronized void loadCache() {
        try {
            Path path = Paths.get(CACHE_FILE);
            if (!Files.exists(path)) return;
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
                log.info("[CACHE] SMA cache stale (cacheDate={}, today={}, lastTradingDay={}) — will full-fetch",
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
                Deque<Double> closes = new ArrayDeque<>();
                JsonNode closesNode = entry.get("closes");
                if (closesNode != null && closesNode.isArray()) {
                    for (JsonNode v : closesNode) closes.addLast(v.asDouble());
                }
                if (closes.isEmpty()) continue;
                while (closes.size() > SMA_PERIOD) closes.removeFirst();
                closesBySymbol.put(sym, closes);
                long lastEpoch = entry.path("lastCandleEpoch").asLong(0);
                if (lastEpoch > 0) lastCandleEpochBySymbol.put(sym, lastEpoch);
                double atr = entry.path("atr").asDouble(0);
                if (atr > 0 && atrService != null) atrService.primeFromCache(sym, atr);
                count++;
            }
            seededFromCache = count > 0;
            if (seededFromCache) {
                log.info("[CACHE] SMA seed restored from cache (savedAt={}, {} symbols)", savedAtStr, count);
            }
        } catch (Exception e) {
            log.error("[CACHE] SmaService loadCache failed: {}", e.getMessage());
        }
    }
}
