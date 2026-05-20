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
 * 1-hour-timeframe equivalent of {@link EmaService}. Maintains a 20-period EMA per symbol
 * on 1-hour candle closes, used by BreakoutScanner's Stock HTF Trend Alignment filter to
 * derive whether the stock's 1-hour close sits above / below its 1-hour EMA20.
 *
 * <p>Listener target: the {@code htfAggregator} created in {@link MarketDataService} at
 * the configured {@code higherTimeframe} (60 min by default). On every 1-hour candle close
 * the EMA is stepped via the recurrence {@code ema = α·close + (1-α)·prev} with
 * {@code α = 2/21}.
 *
 * <p>Seeding: {@link #seedFromCandles(String, List)} accepts the same 5-min historical
 * candles {@link AtrService} already fetches at startup, filters to bars that end on a
 * 1-hour grid boundary (NSE session-aligned: 10:15, 11:15, …, 15:15), and walks their
 * closes through the recurrence. This gives the EMA a 3-4 trading-day warmup before
 * the first live 1-hour candle of the session closes.
 *
 * <p>Persistence: same staleness rule as {@link EmaService} — cache file is dated and
 * reloaded only when the date matches today or the last trading day.
 */
@Service
public class HtfEmaService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(HtfEmaService.class);
    private static final int    EMA_PERIOD = 20;
    private static final double ALPHA      = 2.0 / (EMA_PERIOD + 1); // 2/21 ≈ 0.0952

    private static final ZoneId IST        = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE = "../store/cache/ema-1hour.json";

    private final CandleAggregator candleAggregator;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService marketHolidayService;
    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, Double>  emaBySymbol       = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> barCountBySymbol  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long>    lastBarEpochBySymbol = new ConcurrentHashMap<>();
    private volatile boolean seededFromCache = false;

    public HtfEmaService(CandleAggregator candleAggregator) {
        this.candleAggregator = candleAggregator;
    }

    @PostConstruct
    public void init() {
        loadCache();
    }

    public boolean isSeededFromCache()             { return seededFromCache; }
    /** True only when at least {@link #EMA_PERIOD} 1-hour bars have been accumulated for the
     *  symbol — partial entries don't count, so {@link AtrService}'s catch-up shortcut won't
     *  skip the full-fetch seed for under-warmed symbols. Mirrors {@link #getEma}'s readiness gate. */
    public boolean hasCachedSymbol(String symbol)  {
        Integer bars = barCountBySymbol.get(symbol);
        return bars != null && bars >= EMA_PERIOD;
    }
    public void    flushCache()                    { saveCache(); }

    /**
     * 1-hour EMA(20) for the symbol. Returns 0 until 20 1-hour bars have been fed. During
     * market hours blends the live LTP into the in-progress 1-hour bar's partial close —
     * same approach {@link EmaService#getEma(String)} uses on the 5-min timeframe.
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

    /**
     * Seed the 1-hour EMA from a flat list of 5-min historical candles. Filters to bars
     * whose end aligns with an NSE 1-hour boundary ({@code (startMinute + 5) % 60 == 15})
     * and runs each one's close through the EMA recurrence. Skips when fewer than
     * {@link #EMA_PERIOD} 1-hour bars are available in the input.
     */
    public void seedFromCandles(String symbol, List<CandleAggregator.CandleBar> candles) {
        if (candles == null || candles.isEmpty()) return;

        Double ema = null;
        int bars = 0;
        long lastEpoch = 0;
        for (CandleAggregator.CandleBar c : candles) {
            if (c == null || c.close <= 0) continue;
            if ((c.startMinute + 5) % 60 != 15) continue;   // not a 1-hour close bar
            if (ema == null) ema = c.close;
            else             ema = ALPHA * c.close + (1 - ALPHA) * ema;
            bars++;
            if (c.epochSec > 0) lastEpoch = c.epochSec;
        }
        if (ema == null || bars < EMA_PERIOD) {
            log.info("[HtfEma] seed SKIPPED for {} — only {} 1-hour bars in {} input candles (need {})",
                symbol, bars, candles.size(), EMA_PERIOD);
            return;
        }
        emaBySymbol.put(symbol, ema);
        barCountBySymbol.put(symbol, bars);
        if (lastEpoch > 0) lastBarEpochBySymbol.put(symbol, lastEpoch);
        log.info("[HtfEma] seeded {} — {} 1-hour bars, EMA={}", symbol, bars, String.format("%.2f", ema));
    }

    public int getLoadedCount() {
        int n = 0;
        for (Integer bars : barCountBySymbol.values()) {
            if (bars != null && bars >= EMA_PERIOD) n++;
        }
        return n;
    }

    public synchronized int pruneTo(java.util.Collection<String> watchlist) {
        java.util.Set<String> keep = new java.util.HashSet<>(watchlist);
        int before = emaBySymbol.size();
        emaBySymbol.keySet().retainAll(keep);
        barCountBySymbol.keySet().retainAll(keep);
        lastBarEpochBySymbol.keySet().retainAll(keep);
        int removed = before - emaBySymbol.size();
        if (removed > 0) {
            log.info("[HtfEmaService] Pruned {} stale entries not in watchlist ({} remaining)",
                removed, emaBySymbol.size());
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
        lastBarEpochBySymbol.put(fyersSymbol, completedCandle.epochSec);
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
                Long lastEpoch = lastBarEpochBySymbol.get(sym);
                if (lastEpoch != null) entry.put("lastBarEpoch", lastEpoch);
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(root));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, path);
        } catch (Exception e) {
            log.error("[CACHE] HtfEmaService saveCache failed: {}", e.getMessage());
        }
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
                log.info("[CACHE] HTF EMA cache stale (cacheDate={}, today={}, lastTradingDay={}) — will reseed",
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
                long lastEpoch = entry.path("lastBarEpoch").asLong(0);
                if (lastEpoch > 0) lastBarEpochBySymbol.put(sym, lastEpoch);
                count++;
            }
            seededFromCache = count > 0;
            if (seededFromCache) {
                log.info("[CACHE] HTF EMA seed restored from cache (savedAt={}, {} symbols)", savedAtStr, count);
            }
        } catch (Exception e) {
            log.error("[CACHE] HtfEmaService loadCache failed: {}", e.getMessage());
        }
    }
}
