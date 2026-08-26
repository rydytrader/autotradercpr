package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.util.FileIoUtils;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Persists per-day OHLC snapshots (NIFTY current-month FUTURES only) to
 * {@code store/data/charts/YYYY-MM-DD.json} for historical review from the calendar page.
 *
 * <p>Two write triggers:
 * <ul>
 *   <li><b>Scheduled 15:45 IST daily</b> — 5-min buffer after the extended
 *       15:40 market close (in effect from 2026-08-03). Snapshots whatever's
 *       currently in {@link CandleAggregator}'s history ring for the futures symbol.</li>
 *   <li><b>On-boot catch-up</b> — if the bot boots after 15:45 today and no snapshot
 *       file exists yet, saves immediately. Handles the case where the bot was offline
 *       at 15:45 but is booted later that evening.</li>
 * </ul>
 *
 * <p>Phase 3 shape: single-symbol payload — no CE / PE / ATM fields. Older
 * multi-symbol snapshot files still on disk continue to deserialize (unknown
 * fields ignored) but no new fields are written.
 */
@Service
public class HistoricalChartStore {

    private static final Logger log = LoggerFactory.getLogger(HistoricalChartStore.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STORAGE_DIR = "../store/data/charts";
    // Legacy synthetic futures symbol from the GDFL era. Kept as an inline
    // constant until this service is retargeted to CE + PE snapshot persistence.
    private static final String FUTURES_SYMBOL = "NSE:NIFTY-I-FUT";

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final CandleAggregator candleAggregator;

    public HistoricalChartStore(CandleAggregator candleAggregator) {
        this.candleAggregator = candleAggregator;
    }

    /** DTO written to disk. Single-symbol shape as of Phase 3. */
    public static class DailySnapshot {
        public String date;
        /** The one instrument snapshotted (Fyers-style key). Phase 3 always the
         *  NIFTY current-month futures. */
        public String symbol;
        /** Chronological list of that symbol's bars. */
        public List<Candle> candles = new ArrayList<>();
    }

    @PostConstruct
    public void boot() {
        try { ensureStorageDir(); }
        catch (Exception e) { log.warn("[HistoricalChartStore] failed to create dir: {}", e.getMessage()); }
        // Catch-up: if today > 15:45 AND today's snapshot doesn't exist, save now.
        try {
            String today = LocalDate.now(IST).toString();
            java.time.LocalTime now = java.time.ZonedDateTime.now(IST).toLocalTime();
            boolean pastCloseWindow = !now.isBefore(java.time.LocalTime.of(15, 45));
            if (pastCloseWindow && !snapshotExists(today)) {
                log.info("[HistoricalChartStore] catch-up save — {} > 15:45 and no snapshot yet", today);
                saveTodaySnapshot();
            }
        } catch (Exception e) {
            log.warn("[HistoricalChartStore] boot catch-up threw: {}", e.getMessage());
        }
    }

    /** Fires at 15:45 IST every weekday — 5-min buffer after the extended
     *  15:40 market close (in effect from 2026-08-03). Snapshots today's
     *  NIFTY futures candles. */
    @Scheduled(cron = "0 45 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledSave() {
        saveTodaySnapshot();
    }

    /** Public entry — snapshots today's futures chart to disk. Idempotent (overwrites the
     *  same file on repeat call). Callable manually via REST if needed. */
    public synchronized void saveTodaySnapshot() {
        String today = LocalDate.now(IST).toString();
        List<Candle> hist = candleAggregator.getHistory(FUTURES_SYMBOL);
        if (hist == null || hist.isEmpty()) {
            log.warn("[HistoricalChartStore] skip save — no candles in aggregator for {}", FUTURES_SYMBOL);
            return;
        }
        DailySnapshot snap = new DailySnapshot();
        snap.date    = today;
        snap.symbol  = FUTURES_SYMBOL;
        snap.candles = hist;
        writeSnapshot(today, snap);
    }

    /** Reads a stored snapshot. Empty when the file doesn't exist. */
    public java.util.Optional<DailySnapshot> loadDailySnapshot(String date) {
        if (date == null || date.isBlank()) return java.util.Optional.empty();
        Path p = pathFor(date);
        if (!Files.exists(p)) return java.util.Optional.empty();
        try {
            return java.util.Optional.of(mapper.readValue(Files.readString(p), DailySnapshot.class));
        } catch (Exception e) {
            log.warn("[HistoricalChartStore] load {} failed: {}", date, e.getMessage());
            return java.util.Optional.empty();
        }
    }

    /** Lists dates for which a snapshot exists — sorted descending (newest first). */
    public List<String> listAvailableDates() {
        try {
            Path dir = Path.of(STORAGE_DIR);
            if (!Files.exists(dir)) return Collections.emptyList();
            try (Stream<Path> stream = Files.list(dir)) {
                List<String> dates = new ArrayList<>();
                stream.forEach(f -> {
                    String name = f.getFileName().toString();
                    if (name.endsWith(".json") && name.length() == 15) {   // YYYY-MM-DD.json
                        dates.add(name.substring(0, 10));
                    }
                });
                dates.sort(Collections.reverseOrder());
                return dates;
            }
        } catch (IOException e) {
            log.warn("[HistoricalChartStore] listAvailableDates failed: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private boolean snapshotExists(String date) {
        return Files.exists(pathFor(date));
    }

    private Path pathFor(String date) {
        return Path.of(STORAGE_DIR, date + ".json");
    }

    private void ensureStorageDir() throws IOException {
        Path dir = Path.of(STORAGE_DIR);
        if (!Files.exists(dir)) Files.createDirectories(dir);
    }

    private void writeSnapshot(String date, DailySnapshot snap) {
        try {
            ensureStorageDir();
            Path dst = pathFor(date);
            Path tmp = Path.of(dst.toString() + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(snap));
            FileIoUtils.atomicMoveWithRetry(tmp, dst);
            log.info("[HistoricalChartStore] saved {} — {} bars for {}",
                date, snap.candles.size(), snap.symbol);
        } catch (IOException | RuntimeException e) {
            log.warn("[HistoricalChartStore] save {} failed: {}", date, e.getMessage());
        }
    }

    /** Canonical directory used by {@link com.rydytrader.autotrader.controller.ChartController}. */
    public static String storageDir() { return STORAGE_DIR; }
}
