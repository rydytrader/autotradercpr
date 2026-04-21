package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Calculates 20-period and 200-period EMA on each candle close.
 * 20 EMA: short-term momentum filter + slope tracking.
 * 200 EMA: long-term trend filter (value only, no slope).
 * Used by BreakoutScanner for directional trade confirmation.
 */
@Service
public class EmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(EmaService.class);
    private static final int EMA_PERIOD = 20;
    private static final int EMA_MID_PERIOD = 50;
    private static final int EMA_LONG_PERIOD = 200;

    private static final int HISTORY_SIZE = 20;  // ring buffer of recent EMA(20) values for slope calc
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE = "../store/cache/ema-5min.json";

    private final CandleAggregator candleAggregator;
    private final com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService marketHolidayService;
    // ATR lookup lazily injected (avoids circular dep at construction)
    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private AtrService atrService;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, Double> emaBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<Double>> emaHistoryBySymbol = new ConcurrentHashMap<>();
    // 50 EMA: value + ring buffer of recent values for braided/railway pattern detection
    private final ConcurrentHashMap<String, Double> ema50BySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<Double>> ema50HistoryBySymbol = new ConcurrentHashMap<>();
    // 200 EMA: value only, no ring buffer / slope tracking
    private final ConcurrentHashMap<String, Double> ema200BySymbol = new ConcurrentHashMap<>();
    // Per-symbol last applied candle epoch — used by AtrService catch-up to skip bars already baked in.
    private final ConcurrentHashMap<String, Long> lastCandleEpochBySymbol = new ConcurrentHashMap<>();
    private volatile boolean seededFromCache = false;

    public EmaService(CandleAggregator candleAggregator,
                      com.rydytrader.autotrader.store.RiskSettingsStore riskSettings) {
        this.candleAggregator = candleAggregator;
        this.riskSettings = riskSettings;
    }

    /** Restore EMA values + history + ATR from disk cache before scanner init tries to seed from Fyers. */
    @PostConstruct
    public void init() {
        loadCache();
    }

    public boolean isSeededFromCache()             { return seededFromCache; }
    public boolean hasCachedSymbol(String symbol)  { return emaBySymbol.containsKey(symbol); }
    public long    getLastCandleEpoch(String sym)  { return lastCandleEpochBySymbol.getOrDefault(sym, 0L); }
    /** Public entry point so AtrService can flush the cache once after a bulk seed. */
    public void    flushCache()                    { saveCache(); }

    /** Get current EMA(20) for a symbol. Returns 0 if not enough data. */
    public double getEma(String symbol) {
        return emaBySymbol.getOrDefault(symbol, 0.0);
    }

    /**
     * Seed EMA from historical candles (called after AtrService fetches history).
     * Walks the candles incrementally and pushes every intermediate EMA value into the ring
     * buffer so the slope calculation has enough history immediately — without this, the ring
     * buffer would start with only 1 entry (the final EMA) and slope would be 0 for the first
     * few live candle closes (or forever on weekends when no live candles flow).
     * Delegates to {@link #seedFromCandles(String, List)} which accepts a multi-day list
     * directly — avoids reading from CandleAggregator.completedCandles which is now filtered
     * to today only.
     */
    public void seedFromHistory(String symbol) {
        List<CandleAggregator.CandleBar> candles = candleAggregator.getCompletedCandles(symbol);
        seedFromCandles(symbol, candles);
    }

    /**
     * Seed EMA + history ring buffer from an explicit list of candles (multi-day OK).
     * Used by AtrService to pass the raw historical fetch directly, bypassing the
     * date-filtered CandleAggregator.completedCandles deque.
     */
    public void seedFromCandles(String symbol, List<CandleAggregator.CandleBar> candles) {
        if (candles == null || candles.size() < EMA_PERIOD) return;

        // Clear the ring buffer so re-seeding (e.g. at 9:00 AM reload) starts fresh.
        Deque<Double> history = emaHistoryBySymbol.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        synchronized (history) {
            history.clear();
        }

        double k = 2.0 / (EMA_PERIOD + 1);

        // Seed with SMA of first EMA_PERIOD closes
        double sum = 0;
        for (int i = 0; i < EMA_PERIOD; i++) {
            sum += candles.get(i).close;
        }
        double ema = sum / EMA_PERIOD;
        storeEma(symbol, ema);  // first EMA after seed period

        // Apply exponential smoothing for remaining candles, storing each step in the ring buffer
        for (int i = EMA_PERIOD; i < candles.size(); i++) {
            ema = candles.get(i).close * k + ema * (1 - k);
            storeEma(symbol, ema);
        }

        // Seed 50 EMA from the same candle list (needs ≥50 bars)
        // Pushes each intermediate value into the ring buffer for pattern detection.
        Deque<Double> history50 = ema50HistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (history50) {
            history50.clear();
        }
        if (candles.size() >= EMA_MID_PERIOD) {
            double k50 = 2.0 / (EMA_MID_PERIOD + 1);
            double sum50 = 0;
            for (int i = 0; i < EMA_MID_PERIOD; i++) {
                sum50 += candles.get(i).close;
            }
            double ema50 = sum50 / EMA_MID_PERIOD;
            storeEma50(symbol, ema50);
            for (int i = EMA_MID_PERIOD; i < candles.size(); i++) {
                ema50 = candles.get(i).close * k50 + ema50 * (1 - k50);
                storeEma50(symbol, ema50);
            }
        }

        // Seed 200 EMA from the same candle list (needs ≥200 bars for proper convergence)
        if (candles.size() >= EMA_LONG_PERIOD) {
            double k200 = 2.0 / (EMA_LONG_PERIOD + 1);
            double sum200 = 0;
            for (int i = 0; i < EMA_LONG_PERIOD; i++) {
                sum200 += candles.get(i).close;
            }
            double ema200 = sum200 / EMA_LONG_PERIOD;
            for (int i = EMA_LONG_PERIOD; i < candles.size(); i++) {
                ema200 = candles.get(i).close * k200 + ema200 * (1 - k200);
            }
            ema200BySymbol.put(symbol, ema200);
        }

        // Record the final candle epoch so catch-up fetch knows where to pick up from after a
        // restart that happens before the first live candle close.
        CandleAggregator.CandleBar last = candles.get(candles.size() - 1);
        if (last != null && last.epochSec > 0) {
            lastCandleEpochBySymbol.put(symbol, last.epochSec);
        }
    }

    /** Get current EMA(50) for a symbol. Returns 0 if not enough history. */
    public double getEma50(String symbol) {
        return ema50BySymbol.getOrDefault(symbol, 0.0);
    }

    /** Number of symbols with a loaded (non-zero) EMA(50) value. */
    public int getEma50LoadedCount() {
        return (int) ema50BySymbol.values().stream().filter(v -> v > 0).count();
    }

    /** Get all EMA values (for monitoring/debugging). */
    public Map<String, Double> getAllEma() {
        return emaBySymbol;
    }

    /** Number of symbols with a loaded (non-zero) EMA(20) value. */
    public int getLoadedCount() {
        return (int) emaBySymbol.values().stream().filter(v -> v > 0).count();
    }

    /** Watchlist-scoped loaded counts for dashboard stats. */
    public int getLoadedCountFor(java.util.Collection<String> symbols) {
        int n = 0;
        for (String s : symbols) if (emaBySymbol.getOrDefault(s, 0.0) > 0) n++;
        return n;
    }
    public int getEma50LoadedCountFor(java.util.Collection<String> symbols) {
        int n = 0;
        for (String s : symbols) if (ema50BySymbol.getOrDefault(s, 0.0) > 0) n++;
        return n;
    }
    public int getEma200LoadedCountFor(java.util.Collection<String> symbols) {
        int n = 0;
        for (String s : symbols) if (ema200BySymbol.getOrDefault(s, 0.0) > 0) n++;
        return n;
    }

    /**
     * Drop any EMA entries whose symbol isn't in the current watchlist. Keeps the
     * on-disk cache file proportional to today's tradable universe.
     */
    public synchronized int pruneTo(java.util.Collection<String> watchlist) {
        java.util.Set<String> keep = new java.util.HashSet<>(watchlist);
        int before = emaBySymbol.size();
        emaBySymbol.keySet().retainAll(keep);
        ema50BySymbol.keySet().retainAll(keep);
        ema200BySymbol.keySet().retainAll(keep);
        emaHistoryBySymbol.keySet().retainAll(keep);
        ema50HistoryBySymbol.keySet().retainAll(keep);
        lastCandleEpochBySymbol.keySet().retainAll(keep);
        int removed = before - emaBySymbol.size();
        if (removed > 0) {
            log.info("[EmaService] Pruned {} stale EMA entries not in watchlist ({} remaining)", removed, emaBySymbol.size());
            saveCache();  // rewrite disk cache so pruned entries don't come back on next restart
        }
        return removed;
    }

    /** Get current EMA(200) for a symbol. Returns 0 if not enough history to seed. */
    public double getEma200(String symbol) {
        return ema200BySymbol.getOrDefault(symbol, 0.0);
    }

    /** Number of symbols with a loaded (non-zero) EMA(200) value. */
    public int getEma200LoadedCount() {
        return (int) ema200BySymbol.values().stream().filter(v -> v > 0).count();
    }

    /**
     * Returns the slope of the 20-period EMA over the last `lookback` candles, expressed as
     * percent change per candle. Positive = rising, negative = falling.
     * Returns 0 if not enough EMA history is available yet.
     */
    public double getSlopePctPerCandle(String symbol, int lookback) {
        Deque<Double> history = emaHistoryBySymbol.get(symbol);
        if (history == null || history.size() <= lookback) return 0;
        Double current = null;
        Double prev = null;
        synchronized (history) {
            // Walk from newest to oldest. peekLast = most recent. The prev value is `lookback` steps back.
            int idx = 0;
            Iterator<Double> it = history.descendingIterator();
            while (it.hasNext()) {
                Double v = it.next();
                if (idx == 0) current = v;
                if (idx == lookback) { prev = v; break; }
                idx++;
            }
        }
        if (current == null || prev == null || current <= 0 || prev <= 0) return 0;
        return ((current - prev) / prev) * 100.0 / lookback;
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        // Incremental EMA update: new_ema = close × k + prev_ema × (1 − k)
        // Requires the seed EMA to already be in place (from seedFromCandles at startup).
        // Recomputing from completedCandles is WRONG now that it's filtered to today —
        // the multi-day history is no longer available via that path.
        if (completedCandle == null || completedCandle.close <= 0) return;

        // 20 EMA incremental update
        Double prev = emaBySymbol.get(fyersSymbol);
        if (prev != null && prev > 0) {
            double k = 2.0 / (EMA_PERIOD + 1);
            double ema = completedCandle.close * k + prev * (1 - k);
            storeEma(fyersSymbol, ema);
            completedCandle.ema20 = ema; // snapshot on the bar for chart history
        }

        // 50 EMA incremental update
        Double prev50 = ema50BySymbol.get(fyersSymbol);
        if (prev50 != null && prev50 > 0) {
            double k50 = 2.0 / (EMA_MID_PERIOD + 1);
            double ema50 = completedCandle.close * k50 + prev50 * (1 - k50);
            storeEma50(fyersSymbol, ema50);
            completedCandle.ema50 = ema50;
        }

        // 200 EMA incremental update
        Double prev200 = ema200BySymbol.get(fyersSymbol);
        if (prev200 != null && prev200 > 0) {
            double k200 = 2.0 / (EMA_LONG_PERIOD + 1);
            double ema200 = completedCandle.close * k200 + prev200 * (1 - k200);
            ema200BySymbol.put(fyersSymbol, ema200);
            completedCandle.ema200 = ema200; // snapshot on the bar for chart history
        }

        // Classify EMA 20/50 pattern AFTER ring buffers are updated, snapshot on the candle
        double atrNow = atrService != null ? atrService.getAtr(fyersSymbol) : 0;
        if (atrNow > 0 && riskSettings != null) {
            String pattern = getEmaPattern(fyersSymbol,
                riskSettings.getEmaPatternLookback(),
                atrNow,
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr());
            completedCandle.emaPattern = pattern;
        }

        // Persist state so mid-day / next-day restart can reload without re-fetching Fyers history.
        lastCandleEpochBySymbol.put(fyersSymbol, completedCandle.epochSec);
        saveCache();
    }

    // ── Disk cache: mid-day restart avoids the 14-day Fyers fetch ───────────────────

    /** Build + atomically write EMA/ATR snapshot for every symbol. One burst per candle-close tick. */
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
                entry.put("ema20", e.getValue());
                Double e50 = ema50BySymbol.get(sym);
                if (e50 != null)  entry.put("ema50", e50);
                Double e200 = ema200BySymbol.get(sym);
                if (e200 != null) entry.put("ema200", e200);
                double atr = atrService != null ? atrService.getAtr(sym) : 0;
                if (atr > 0) entry.put("atr", atr);
                Long lastEpoch = lastCandleEpochBySymbol.get(sym);
                if (lastEpoch != null) entry.put("lastCandleEpoch", lastEpoch);
                entry.set("ema20History", historyArr(emaHistoryBySymbol.get(sym)));
                entry.set("ema50History", historyArr(ema50HistoryBySymbol.get(sym)));
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(root));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, path);
        } catch (Exception e) {
            log.error("[CACHE] EmaService saveCache failed: {}", e.getMessage());
        }
    }

    private ArrayNode historyArr(Deque<Double> deque) {
        ArrayNode arr = mapper.createArrayNode();
        if (deque == null) return arr;
        synchronized (deque) { for (Double v : deque) arr.add(v); }
        return arr;
    }

    /**
     * Load EMA/ATR snapshot. Fresh if savedAt date equals today OR the last trading day
     * (covers mid-day restart, evening, next-morning pre-market).
     */
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
                double ema20 = entry.path("ema20").asDouble(0);
                if (ema20 <= 0) continue;
                emaBySymbol.put(sym, ema20);
                double ema50 = entry.path("ema50").asDouble(0);
                if (ema50 > 0) ema50BySymbol.put(sym, ema50);
                double ema200 = entry.path("ema200").asDouble(0);
                if (ema200 > 0) ema200BySymbol.put(sym, ema200);
                long lastEpoch = entry.path("lastCandleEpoch").asLong(0);
                if (lastEpoch > 0) lastCandleEpochBySymbol.put(sym, lastEpoch);
                double atr = entry.path("atr").asDouble(0);
                if (atr > 0 && atrService != null) atrService.primeFromCache(sym, atr);
                emaHistoryBySymbol.put(sym, deserializeHistory(entry.get("ema20History")));
                ema50HistoryBySymbol.put(sym, deserializeHistory(entry.get("ema50History")));
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

    private Deque<Double> deserializeHistory(JsonNode arr) {
        Deque<Double> out = new ArrayDeque<>();
        if (arr != null && arr.isArray()) {
            for (JsonNode v : arr) out.addLast(v.asDouble());
            while (out.size() > HISTORY_SIZE) out.removeFirst();
        }
        return out;
    }

    /** Update the current EMA value and append to the history ring buffer for slope tracking. */
    private void storeEma(String symbol, double ema) {
        emaBySymbol.put(symbol, ema);
        Deque<Double> history = emaHistoryBySymbol.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        synchronized (history) {
            history.addLast(ema);
            while (history.size() > HISTORY_SIZE) history.removeFirst();
        }
    }

    /** Update the current EMA(50) value and append to the EMA(50) history ring buffer. */
    private void storeEma50(String symbol, double ema50) {
        ema50BySymbol.put(symbol, ema50);
        Deque<Double> history = ema50HistoryBySymbol.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        synchronized (history) {
            history.addLast(ema50);
            while (history.size() > HISTORY_SIZE) history.removeFirst();
        }
    }

    /**
     * Classify the EMA(20) / EMA(50) relationship over the last {@code lookback} candles.
     * @return "BRAIDED" (zigzag, choppy), "RAILWAY" (parallel, trending), or "" (neither)
     */
    public String getEmaPattern(String symbol, int lookback, double atr,
                                int braidedMinCrossovers, double braidedMaxSpreadAtr,
                                double railwayMaxCv, double railwayMinSpreadAtr) {
        if (atr <= 0 || lookback < 3) return "";
        Deque<Double> h20 = emaHistoryBySymbol.get(symbol);
        Deque<Double> h50 = ema50HistoryBySymbol.get(symbol);
        if (h20 == null || h50 == null) return "";

        double[] ema20Arr;
        double[] ema50Arr;
        synchronized (h20) {
            if (h20.size() < lookback) return "";
            ema20Arr = lastN(h20, lookback);
        }
        synchronized (h50) {
            if (h50.size() < lookback) return "";
            ema50Arr = lastN(h50, lookback);
        }

        // Compute spread per candle, sign changes, and |spread| stats
        double[] spread = new double[lookback];
        double sumAbs = 0;
        for (int i = 0; i < lookback; i++) {
            spread[i] = ema20Arr[i] - ema50Arr[i];
            sumAbs += Math.abs(spread[i]);
        }
        double meanAbs = sumAbs / lookback;

        int crossovers = 0;
        for (int i = 1; i < lookback; i++) {
            if ((spread[i - 1] > 0 && spread[i] < 0) || (spread[i - 1] < 0 && spread[i] > 0)) {
                crossovers++;
            }
        }

        // Braided: 2+ crossovers OR EMAs hugging zero (effectively overlapping)
        if (crossovers >= braidedMinCrossovers) return "BRAIDED";
        if (meanAbs <= braidedMaxSpreadAtr * atr) return "BRAIDED";

        // Railway: ≤1 crossover, stable |spread| magnitude, meaningful separation
        if (crossovers <= 1 && meanAbs >= railwayMinSpreadAtr * atr) {
            double sumSqDev = 0;
            for (int i = 0; i < lookback; i++) {
                double dev = Math.abs(spread[i]) - meanAbs;
                sumSqDev += dev * dev;
            }
            double std = Math.sqrt(sumSqDev / lookback);
            double cv = meanAbs > 0 ? std / meanAbs : Double.POSITIVE_INFINITY;
            if (cv <= railwayMaxCv) {
                // Direction from latest spread: 20>50 = rising, 20<50 = falling.
                // Long-term confirmation: 20 must also be on the right side of 200 — otherwise
                // it's just a short-term ripple against the dominant trend, not a true railway.
                double latestSpread = spread[lookback - 1];
                double ema20Latest = ema20Arr[lookback - 1];
                double ema200Latest = ema200BySymbol.getOrDefault(symbol, 0.0);
                if (ema200Latest <= 0) return "";  // need 200 EMA loaded to confirm
                // Slope filter via least-squares linear regression across all lookback bars.
                // Catches mid-window reversals that simple first/last comparison would miss
                // (e.g. EMA rose 8 bars then rolled over the last 3 — total rise positive,
                // but regression slope reveals the weakening trend).
                // Threshold: total projected rise = slope_per_bar × (lookback − 1) ≥ minSlopeAtr × ATR.
                double minTotalRise = riskSettings != null ? riskSettings.getRailwayMinSlopeAtr() * atr : 0;
                double ema20Slope = linearSlope(ema20Arr) * (lookback - 1);
                double ema50Slope = linearSlope(ema50Arr) * (lookback - 1);
                if (latestSpread > 0 && ema20Latest > ema200Latest
                        && ema20Slope >= minTotalRise && ema50Slope >= minTotalRise) return "RAILWAY_UP";
                if (latestSpread < 0 && ema20Latest < ema200Latest
                        && ema20Slope <= -minTotalRise && ema50Slope <= -minTotalRise) return "RAILWAY_DOWN";
            }
        }

        return "";
    }

    /** Least-squares slope of y against x=0..n-1. Returns slope in value-units per bar. */
    private static double linearSlope(double[] y) {
        int n = y.length;
        if (n < 2) return 0;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        for (int i = 0; i < n; i++) {
            sumX  += i;
            sumY  += y[i];
            sumXY += (double) i * y[i];
            sumX2 += (double) i * i;
        }
        double denom = n * sumX2 - sumX * sumX;
        if (denom == 0) return 0;
        return (n * sumXY - sumX * sumY) / denom;
    }

    /** Extract the last N values from a deque as an ordered array (oldest → newest). */
    private static double[] lastN(Deque<Double> deque, int n) {
        double[] out = new double[n];
        int size = deque.size();
        int skip = size - n;
        int i = 0;
        int outIdx = 0;
        for (Double v : deque) {
            if (i++ < skip) continue;
            out[outIdx++] = v;
        }
        return out;
    }

    /**
     * Calculate EMA for given candle history.
     * Seed with SMA of first N closes, then apply exponential smoothing.
     */
    static double calculateEma(List<CandleAggregator.CandleBar> candles, int period) {
        if (candles.size() < period) return 0;

        double k = 2.0 / (period + 1);

        // Seed: SMA of first 'period' closes
        double sum = 0;
        for (int i = 0; i < period; i++) {
            sum += candles.get(i).close;
        }
        double ema = sum / period;

        // Apply exponential smoothing for remaining candles
        for (int i = period; i < candles.size(); i++) {
            ema = candles.get(i).close * k + ema * (1 - k);
        }

        return ema;
    }
}
