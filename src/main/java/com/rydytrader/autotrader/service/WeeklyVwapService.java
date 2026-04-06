package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tick-level weekly VWAP (ATP) accumulator for swing trading.
 * Accumulates Σ(price × volume) / Σ(volume) from Monday 9:15 onwards.
 * Resets every Monday 9:15. Persists running sums to survive restarts.
 * Also tracks week open (first tick on Monday 9:15).
 */
@Service
public class WeeklyVwapService {

    private static final Logger log = LoggerFactory.getLogger(WeeklyVwapService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STORE_FILE = "../store/config/weekly-vwap.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Per-symbol accumulator
    private final ConcurrentHashMap<String, VwapAccumulator> accumulators = new ConcurrentHashMap<>();
    // Per-symbol week open price
    private final ConcurrentHashMap<String, Double> weekOpen = new ConcurrentHashMap<>();
    // ISO week number when last reset happened
    private volatile int currentWeek = -1;
    private volatile int currentYear = -1;

    @PostConstruct
    public void init() {
        loadFromFile();
    }

    /**
     * Called on every tick from WebSocket. Captures week open (first tick after Monday reset).
     */
    public void onTick(String fyersSymbol, double price) {
        if (price <= 0) return;
        String ticker = extractTicker(fyersSymbol);
        checkWeekReset();
        // Capture week open (first tick after reset)
        weekOpen.putIfAbsent(ticker, price);
    }

    /**
     * Called on candle close. Accumulates TP × volume for VWAP using candle data
     * (which has correct delta volume, unlike raw ticks which have cumulative volume).
     */
    public void onCandleClose(String fyersSymbol, double high, double low, double close, long volume) {
        if (volume <= 0) return;
        String ticker = extractTicker(fyersSymbol);
        checkWeekReset();

        double tp = (high + low + close) / 3.0;
        VwapAccumulator acc = accumulators.computeIfAbsent(ticker, k -> new VwapAccumulator());
        acc.sumPriceVol += tp * volume;
        acc.sumVol += volume;
    }

    /** Get weekly VWAP for a symbol. Returns 0 if no data. */
    public double getWeeklyVwap(String fyersSymbol) {
        String ticker = extractTicker(fyersSymbol);
        VwapAccumulator acc = accumulators.get(ticker);
        if (acc == null || acc.sumVol == 0) return 0;
        return Math.round((acc.sumPriceVol / acc.sumVol) * 100.0) / 100.0;
    }

    /** Get week open price for a symbol. Returns 0 if not captured yet. */
    public double getWeekOpen(String fyersSymbol) {
        String ticker = extractTicker(fyersSymbol);
        return weekOpen.getOrDefault(ticker, 0.0);
    }

    /** Persist running sums to disk. Called periodically (e.g., every minute). */
    public void saveToFile() {
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("week", currentWeek);
            data.put("year", currentYear);

            Map<String, Map<String, Object>> accData = new LinkedHashMap<>();
            for (var entry : accumulators.entrySet()) {
                Map<String, Object> a = new LinkedHashMap<>();
                a.put("sumPriceVol", entry.getValue().sumPriceVol);
                a.put("sumVol", entry.getValue().sumVol);
                accData.put(entry.getKey(), a);
            }
            data.put("accumulators", accData);

            Map<String, Double> openData = new LinkedHashMap<>(weekOpen);
            data.put("weekOpen", openData);

            Path path = Paths.get(STORE_FILE);
            Files.createDirectories(path.getParent());
            Files.writeString(path, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data));
        } catch (Exception e) {
            log.error("[WeeklyVwap] Failed to save: {}", e.getMessage());
        }
    }

    private void loadFromFile() {
        try {
            Path path = Paths.get(STORE_FILE);
            if (!Files.exists(path)) return;

            JsonNode root = mapper.readTree(Files.readString(path));
            int savedWeek = root.has("week") ? root.get("week").asInt() : -1;
            int savedYear = root.has("year") ? root.get("year").asInt() : -1;

            // Only load if same week
            LocalDate today = LocalDate.now(IST);
            int todayWeek = today.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            int todayYear = today.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);

            if (savedWeek != todayWeek || savedYear != todayYear) {
                log.info("[WeeklyVwap] Saved data from week {}-{}, current {}-{} — starting fresh",
                    savedYear, savedWeek, todayYear, todayWeek);
                currentWeek = todayWeek;
                currentYear = todayYear;
                return;
            }

            currentWeek = savedWeek;
            currentYear = savedYear;

            JsonNode accNode = root.get("accumulators");
            if (accNode != null) {
                accNode.fields().forEachRemaining(entry -> {
                    VwapAccumulator acc = new VwapAccumulator();
                    acc.sumPriceVol = entry.getValue().get("sumPriceVol").asDouble();
                    acc.sumVol = entry.getValue().get("sumVol").asDouble();
                    accumulators.put(entry.getKey(), acc);
                });
            }

            JsonNode openNode = root.get("weekOpen");
            if (openNode != null) {
                openNode.fields().forEachRemaining(entry -> {
                    weekOpen.put(entry.getKey(), entry.getValue().asDouble());
                });
            }

            log.info("[WeeklyVwap] Loaded week {}-{}: {} symbols with VWAP, {} with week open",
                currentYear, currentWeek, accumulators.size(), weekOpen.size());
        } catch (Exception e) {
            log.error("[WeeklyVwap] Failed to load: {}", e.getMessage());
        }
    }

    /** Check if we need to reset for a new week. */
    private void checkWeekReset() {
        LocalDate today = LocalDate.now(IST);
        int todayWeek = today.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        int todayYear = today.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);

        if (todayWeek != currentWeek || todayYear != currentYear) {
            log.info("[WeeklyVwap] New week detected ({}-{} → {}-{}), resetting accumulators",
                currentYear, currentWeek, todayYear, todayWeek);
            accumulators.clear();
            weekOpen.clear();
            currentWeek = todayWeek;
            currentYear = todayYear;
        }
    }

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        int dash = s.indexOf('-');
        if (dash >= 0) s = s.substring(0, dash);
        return s;
    }

    private static class VwapAccumulator {
        volatile double sumPriceVol = 0;
        volatile double sumVol = 0;
    }
}
