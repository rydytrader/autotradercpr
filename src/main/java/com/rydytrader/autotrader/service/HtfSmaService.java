package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
 * Higher-timeframe (default 60-min) SMAs for long-term trend display + weekly target shift +
 * NIFTY index alignment scoring. Mirrors SmaService but listens to the separate htfAggregator.
 */
@Service
public class HtfSmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(HtfSmaService.class);
    private static final int SMA_PERIOD = 20;
    private static final int SMA_MID_PERIOD = 50;
    private static final int SMA_LONG_PERIOD = 200;
    private static final int HISTORY_SIZE = 20;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE = "../store/cache/sma-htf.json";
    private static final String LEGACY_CACHE_FILE = "../store/cache/ema-htf.json";

    private final RiskSettingsStore riskSettings;
    @Autowired private MarketHolidayService marketHolidayService;
    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, Deque<Double>> closesBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<Double>> smaHistoryBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<Double>> sma50HistoryBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastCandleEpochBySymbol = new ConcurrentHashMap<>();
    private volatile boolean seededFromCache = false;

    public HtfSmaService(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
    }

    @PostConstruct
    public void init() {
        deleteLegacyCache();
        loadCache();
    }

    private void deleteLegacyCache() {
        try {
            Path legacy = Paths.get(LEGACY_CACHE_FILE);
            if (Files.exists(legacy)) {
                Files.delete(legacy);
                log.info("[CACHE] Deleted legacy HTF EMA cache at {}", legacy);
            }
        } catch (Exception e) {
            log.warn("[CACHE] Could not delete legacy HTF EMA cache: {}", e.getMessage());
        }
    }

    public boolean isSeededFromCache()            { return seededFromCache; }
    public boolean hasCachedSymbol(String symbol) { return closesBySymbol.containsKey(symbol); }
    public long    getLastCandleEpoch(String s)   { return lastCandleEpochBySymbol.getOrDefault(s, 0L); }

    public double getSma(String symbol)    { return computeSma(symbol, SMA_PERIOD); }
    public double getSma50(String symbol)  { return computeSma(symbol, SMA_MID_PERIOD); }
    public double getSma200(String symbol) { return computeSma(symbol, SMA_LONG_PERIOD); }

    private double computeSma(String symbol, int period) {
        Deque<Double> closes = closesBySymbol.get(symbol);
        if (closes == null) return 0;
        synchronized (closes) {
            if (closes.size() < period) return 0;
            double sum = 0;
            int skip = closes.size() - period;
            int i = 0;
            int count = 0;
            for (Double v : closes) {
                if (i++ < skip) continue;
                sum += v;
                count++;
            }
            return count == period ? sum / period : 0;
        }
    }

    public int getLoadedCount()       { return countWithAtLeast(SMA_PERIOD); }
    public int getSma50LoadedCount()  { return countWithAtLeast(SMA_MID_PERIOD); }
    public int getSma200LoadedCount() { return countWithAtLeast(SMA_LONG_PERIOD); }

    private int countWithAtLeast(int period) {
        int n = 0;
        for (Deque<Double> d : closesBySymbol.values()) {
            synchronized (d) { if (d.size() >= period) n++; }
        }
        return n;
    }

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

    public synchronized int pruneTo(java.util.Collection<String> watchlist) {
        java.util.Set<String> keep = new java.util.HashSet<>(watchlist);
        int before = closesBySymbol.size();
        closesBySymbol.keySet().retainAll(keep);
        smaHistoryBySymbol.keySet().retainAll(keep);
        sma50HistoryBySymbol.keySet().retainAll(keep);
        lastCandleEpochBySymbol.keySet().retainAll(keep);
        int removed = before - closesBySymbol.size();
        if (removed > 0) {
            log.info("[HtfSmaService] Pruned {} stale HTF SMA entries not in watchlist ({} remaining)", removed, closesBySymbol.size());
            saveCache();
        }
        return removed;
    }

    /**
     * Seed HTF SMAs from historical HTF candles.
     */
    public void seedFromCandles(String symbol, List<CandleAggregator.CandleBar> candles) {
        if (candles == null || candles.size() < SMA_PERIOD) return;

        Deque<Double> closes = closesBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (closes) { closes.clear(); }
        Deque<Double> history20 = smaHistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (history20) { history20.clear(); }
        Deque<Double> history50 = sma50HistoryBySymbol.computeIfAbsent(symbol, s -> new ArrayDeque<>());
        synchronized (history50) { history50.clear(); }

        for (int i = 0; i < candles.size(); i++) {
            double close = candles.get(i).close;
            if (close <= 0) continue;
            synchronized (closes) {
                closes.addLast(close);
                while (closes.size() > SMA_LONG_PERIOD) closes.removeFirst();
            }
            if (i >= SMA_PERIOD - 1) {
                double sma20 = computeSma(symbol, SMA_PERIOD);
                if (sma20 > 0) pushHistory(history20, sma20);
            }
            if (i >= SMA_MID_PERIOD - 1) {
                double sma50 = computeSma(symbol, SMA_MID_PERIOD);
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

    public void flushCache() { saveCache(); }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (completedCandle == null || completedCandle.close <= 0) return;

        Deque<Double> closes = closesBySymbol.computeIfAbsent(fyersSymbol, s -> new ArrayDeque<>());
        synchronized (closes) {
            closes.addLast(completedCandle.close);
            while (closes.size() > SMA_LONG_PERIOD) closes.removeFirst();
        }

        double sma20 = computeSma(fyersSymbol, SMA_PERIOD);
        if (sma20 > 0) {
            Deque<Double> history20 = smaHistoryBySymbol.computeIfAbsent(fyersSymbol, s -> new ArrayDeque<>());
            pushHistory(history20, sma20);
        }
        double sma50 = computeSma(fyersSymbol, SMA_MID_PERIOD);
        if (sma50 > 0) {
            Deque<Double> history50 = sma50HistoryBySymbol.computeIfAbsent(fyersSymbol, s -> new ArrayDeque<>());
            pushHistory(history50, sma50);
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
                Long epoch = lastCandleEpochBySymbol.get(sym);
                if (epoch != null) entry.put("lastCandleEpoch", epoch);
                entry.set("sma20History", historyArr(smaHistoryBySymbol.get(sym)));
                entry.set("sma50History", historyArr(sma50HistoryBySymbol.get(sym)));
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(root));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, path);
        } catch (Exception e) {
            log.error("[CACHE] HtfSmaService saveCache failed: {}", e.getMessage());
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
                log.info("[CACHE] HTF SMA cache stale (cacheDate={}, today={}, lastTradingDay={}) — will full-fetch",
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
                smaHistoryBySymbol.put(sym, deserializeHistory(entry.get("sma20History")));
                sma50HistoryBySymbol.put(sym, deserializeHistory(entry.get("sma50History")));
                count++;
            }
            seededFromCache = count > 0;
            if (seededFromCache) {
                log.info("[CACHE] HTF SMA seed restored from cache (savedAt={}, {} symbols)", savedAtStr, count);
            }
        } catch (Exception e) {
            log.error("[CACHE] HtfSmaService loadCache failed: {}", e.getMessage());
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
     * Classify the HTF SMA(20) / SMA(50) relationship.
     */
    public String getSmaPattern(String symbol, int lookback, double atr,
                                int braidedMinCrossovers, double braidedMaxSpreadAtr,
                                double railwayMaxCv, double railwayMinSpreadAtr) {
        if (atr <= 0 || lookback < 3) return "";
        Deque<Double> h20 = smaHistoryBySymbol.get(symbol);
        Deque<Double> h50 = sma50HistoryBySymbol.get(symbol);
        if (h20 == null || h50 == null) return "";

        double[] sma20Arr, sma50Arr;
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
