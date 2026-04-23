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
 * Simple Moving Average (SMA) service for 20/50/200 periods on each candle close.
 * Replaced the prior EMA implementation: SMA is exact from a rolling close window
 * and converges to TradingView values without depending on seed depth.
 * Used by BreakoutScanner for directional trade confirmation.
 */
@Service
public class SmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(SmaService.class);
    private static final int SMA_PERIOD = 20;
    private static final int SMA_MID_PERIOD = 50;
    private static final int SMA_LONG_PERIOD = 200;

    private static final int HISTORY_SIZE = 20;  // ring buffer of recent SMA(20)/SMA(50) values for pattern calc
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

    // Rolling window of last 200 closes per symbol — used to derive SMA(20/50/200) on demand.
    private final ConcurrentHashMap<String, Deque<Double>> closesBySymbol = new ConcurrentHashMap<>();
    // Ring buffers of recent computed SMA values (for BRAIDED/RAILWAY pattern detection).
    private final ConcurrentHashMap<String, Deque<Double>> smaHistoryBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<Double>> sma50HistoryBySymbol = new ConcurrentHashMap<>();
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

    /** Get current SMA(50) for a symbol. Returns 0 if not enough history. */
    public double getSma50(String symbol) {
        return computeSmaBlended(symbol, SMA_MID_PERIOD);
    }

    /** Get current SMA(200) for a symbol. Returns 0 if not enough history to seed. */
    public double getSma200(String symbol) {
        return computeSmaBlended(symbol, SMA_LONG_PERIOD);
    }

    /**
     * Blend current LTP as the in-progress 5-min bar's partial close. Matches TradingView's
     * intra-bar SMA behaviour: SMA = (last period-1 completed closes + current LTP) / period.
     * Falls back to completed-only when LTP isn't available.
     */
    private double computeSmaBlended(String symbol, int period) {
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
     * the pattern history buffer captures the SMA value at bar close (not a blended intra-bar
     * value), and so bar-close downstream listeners see the canonical post-close SMA.
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
     * Seed SMA close window + pattern history ring buffer from an explicit list of candles.
     * We only need the last SMA_LONG_PERIOD (200) closes for state, but we walk every bar so
     * the pattern history buffer gets populated with the last HISTORY_SIZE (20) intermediate
     * SMA(20)/SMA(50) values.
     */
    public void seedFromCandles(String symbol, List<CandleAggregator.CandleBar> candles) {
        if (candles == null || candles.size() < SMA_PERIOD) return;

        Deque<Double> closes = closesBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (closes) {
            closes.clear();
        }

        Deque<Double> history20 = smaHistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (history20) {
            history20.clear();
        }
        Deque<Double> history50 = sma50HistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (history50) {
            history50.clear();
        }

        for (int i = 0; i < candles.size(); i++) {
            double close = candles.get(i).close;
            if (close <= 0) continue;
            synchronized (closes) {
                closes.addLast(close);
                while (closes.size() > SMA_LONG_PERIOD) closes.removeFirst();
            }
            // Only start recording pattern history once we have enough bars for SMA(20)
            if (i >= SMA_PERIOD - 1) {
                double sma20 = computeSmaCompleted(symbol, SMA_PERIOD);
                if (sma20 > 0) pushHistory(history20, sma20);
            }
            if (i >= SMA_MID_PERIOD - 1) {
                double sma50 = computeSmaCompleted(symbol, SMA_MID_PERIOD);
                if (sma50 > 0) pushHistory(history50, sma50);
            }
        }

        CandleAggregator.CandleBar last = candles.get(candles.size() - 1);
        if (last != null && last.epochSec > 0) {
            lastCandleEpochBySymbol.put(symbol, last.epochSec);
        }
    }

    private void pushHistory(Deque<Double> deque, double v) {
        synchronized (deque) {
            deque.addLast(v);
            while (deque.size() > HISTORY_SIZE) deque.removeFirst();
        }
    }

    /** Seed from CandleAggregator.getCompletedCandles — convenience wrapper. */
    public void seedFromHistory(String symbol) {
        List<CandleAggregator.CandleBar> candles = candleAggregator.getCompletedCandles(symbol);
        seedFromCandles(symbol, candles);
    }

    public int getLoadedCount()           { return countWithAtLeast(SMA_PERIOD); }
    public int getSma50LoadedCount()      { return countWithAtLeast(SMA_MID_PERIOD); }
    public int getSma200LoadedCount()     { return countWithAtLeast(SMA_LONG_PERIOD); }

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
    public int getSma50LoadedCountFor(java.util.Collection<String> symbols) {
        return countForWithAtLeast(symbols, SMA_MID_PERIOD);
    }
    public int getSma200LoadedCountFor(java.util.Collection<String> symbols) {
        return countForWithAtLeast(symbols, SMA_LONG_PERIOD);
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
        smaHistoryBySymbol.keySet().retainAll(keep);
        sma50HistoryBySymbol.keySet().retainAll(keep);
        lastCandleEpochBySymbol.keySet().retainAll(keep);
        int removed = before - closesBySymbol.size();
        if (removed > 0) {
            log.info("[SmaService] Pruned {} stale SMA entries not in watchlist ({} remaining)", removed, closesBySymbol.size());
            saveCache();
        }
        return removed;
    }

    /**
     * Slope of the 20-period SMA over the last `lookback` candles, expressed as
     * percent change per candle. Positive = rising, negative = falling.
     */
    public double getSlopePctPerCandle(String symbol, int lookback) {
        Deque<Double> history = smaHistoryBySymbol.get(symbol);
        if (history == null || history.size() <= lookback) return 0;
        Double current = null;
        Double prev = null;
        synchronized (history) {
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
        if (completedCandle == null || completedCandle.close <= 0) return;

        Deque<Double> closes = closesBySymbol.computeIfAbsent(fyersSymbol, s -> new ArrayDeque<>());
        synchronized (closes) {
            closes.addLast(completedCandle.close);
            while (closes.size() > SMA_LONG_PERIOD) closes.removeFirst();
        }

        double sma20 = computeSmaCompleted(fyersSymbol, SMA_PERIOD);
        if (sma20 > 0) {
            completedCandle.sma20 = sma20;
            Deque<Double> history20 = smaHistoryBySymbol.computeIfAbsent(fyersSymbol, s -> new ArrayDeque<>());
            pushHistory(history20, sma20);
        }

        double sma50 = computeSmaCompleted(fyersSymbol, SMA_MID_PERIOD);
        if (sma50 > 0) {
            completedCandle.sma50 = sma50;
            Deque<Double> history50 = sma50HistoryBySymbol.computeIfAbsent(fyersSymbol, s -> new ArrayDeque<>());
            pushHistory(history50, sma50);
        }

        double sma200 = computeSmaCompleted(fyersSymbol, SMA_LONG_PERIOD);
        if (sma200 > 0) {
            completedCandle.sma200 = sma200;
        }

        double atrNow = atrService != null ? atrService.getAtr(fyersSymbol) : 0;
        if (atrNow > 0 && riskSettings != null) {
            String pattern = getSmaPattern(fyersSymbol,
                riskSettings.getSmaPatternLookback(),
                atrNow,
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr());
            completedCandle.smaPattern = pattern;
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
                entry.set("sma20History", historyArr(smaHistoryBySymbol.get(sym)));
                entry.set("sma50History", historyArr(sma50HistoryBySymbol.get(sym)));
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

    private ArrayNode historyArr(Deque<Double> deque) {
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
                closesBySymbol.put(sym, closes);
                long lastEpoch = entry.path("lastCandleEpoch").asLong(0);
                if (lastEpoch > 0) lastCandleEpochBySymbol.put(sym, lastEpoch);
                double atr = entry.path("atr").asDouble(0);
                if (atr > 0 && atrService != null) atrService.primeFromCache(sym, atr);
                smaHistoryBySymbol.put(sym, deserializeHistory(entry.get("sma20History")));
                sma50HistoryBySymbol.put(sym, deserializeHistory(entry.get("sma50History")));
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

    private Deque<Double> deserializeHistory(JsonNode arr) {
        Deque<Double> out = new ArrayDeque<>();
        if (arr != null && arr.isArray()) {
            for (JsonNode v : arr) out.addLast(v.asDouble());
            while (out.size() > HISTORY_SIZE) out.removeFirst();
        }
        return out;
    }

    /**
     * Classify the SMA(20) / SMA(50) relationship over the last {@code lookback} candles.
     * @return "BRAIDED" (zigzag, choppy), "RAILWAY_UP"/"RAILWAY_DOWN" (parallel, trending), or ""
     */
    public String getSmaPattern(String symbol, int lookback, double atr,
                                int braidedMinCrossovers, double braidedMaxSpreadAtr,
                                double railwayMaxCv, double railwayMinSpreadAtr) {
        if (atr <= 0 || lookback < 3) return "";
        Deque<Double> h20 = smaHistoryBySymbol.get(symbol);
        Deque<Double> h50 = sma50HistoryBySymbol.get(symbol);
        if (h20 == null || h50 == null) return "";

        double[] sma20Arr;
        double[] sma50Arr;
        synchronized (h20) {
            if (h20.size() < lookback) return "";
            sma20Arr = lastN(h20, lookback);
        }
        synchronized (h50) {
            if (h50.size() < lookback) return "";
            sma50Arr = lastN(h50, lookback);
        }

        double[] spread = new double[lookback];
        double sumAbs = 0;
        for (int i = 0; i < lookback; i++) {
            spread[i] = sma20Arr[i] - sma50Arr[i];
            sumAbs += Math.abs(spread[i]);
        }
        double meanAbs = sumAbs / lookback;

        int crossovers = 0;
        for (int i = 1; i < lookback; i++) {
            if ((spread[i - 1] > 0 && spread[i] < 0) || (spread[i - 1] < 0 && spread[i] > 0)) {
                crossovers++;
            }
        }

        if (crossovers >= braidedMinCrossovers) return "BRAIDED";
        if (meanAbs <= braidedMaxSpreadAtr * atr) return "BRAIDED";

        if (crossovers <= 1 && meanAbs >= railwayMinSpreadAtr * atr) {
            double sumSqDev = 0;
            for (int i = 0; i < lookback; i++) {
                double dev = Math.abs(spread[i]) - meanAbs;
                sumSqDev += dev * dev;
            }
            double std = Math.sqrt(sumSqDev / lookback);
            double cv = meanAbs > 0 ? std / meanAbs : Double.POSITIVE_INFINITY;
            if (cv <= railwayMaxCv) {
                double latestSpread = spread[lookback - 1];
                double sma20Latest = sma20Arr[lookback - 1];
                double sma200Latest = getSma200(symbol);
                if (sma200Latest <= 0) return "";
                double minTotalRise = riskSettings != null ? riskSettings.getRailwayMinSlopeAtr() * atr : 0;
                double sma20Slope = linearSlope(sma20Arr) * (lookback - 1);
                double sma50Slope = linearSlope(sma50Arr) * (lookback - 1);
                if (latestSpread > 0 && sma20Latest > sma200Latest
                        && sma20Slope >= minTotalRise && sma50Slope >= minTotalRise) return "RAILWAY_UP";
                if (latestSpread < 0 && sma20Latest < sma200Latest
                        && sma20Slope <= -minTotalRise && sma50Slope <= -minTotalRise) return "RAILWAY_DOWN";
            }
        }

        return "";
    }

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
}
