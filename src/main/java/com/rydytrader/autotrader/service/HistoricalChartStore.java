package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.indicator.SuperTrend;
import com.rydytrader.autotrader.service.strategy.VwapSupertrendStrategy;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.util.FileIoUtils;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
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
 * Persists per-day CE + PE 3-min OHLC snapshots to
 * {@code store/data/charts/YYYY-MM-DD.json} for review from the calendar page.
 *
 * <p>Save triggers:
 * <ul>
 *   <li><b>Scheduled 15:45 IST daily</b> — 5-min buffer after the extended
 *       15:40 market close.</li>
 *   <li><b>On-boot catch-up</b> — if the bot boots after 15:45 today and no
 *       snapshot file exists yet, saves immediately.</li>
 * </ul>
 *
 * <p>Snapshot shape captures the chosen pair at squareoff time (the pair the
 * strategy actually traded that day): CE symbol + its 3-min bars, PE symbol +
 * its 3-min bars, plus the anchoring spotOpen and atmStrike.
 */
@Service
public class HistoricalChartStore {

    private static final Logger log = LoggerFactory.getLogger(HistoricalChartStore.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STORAGE_DIR = "../store/data/charts";

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final CandleAggregator candleAggregator;
    private final RiskSettingsStore riskSettings;
    private final ObjectProvider<VwapSupertrendStrategy> strategyProvider;

    public HistoricalChartStore(CandleAggregator candleAggregator,
                                 RiskSettingsStore riskSettings,
                                 ObjectProvider<VwapSupertrendStrategy> strategyProvider) {
        this.candleAggregator = candleAggregator;
        this.riskSettings     = riskSettings;
        this.strategyProvider = strategyProvider;
    }

    /** DTO written to disk. CE and PE bars are the aggregated N-min bars at
     *  the strategy's configured timeframe (default 3-min). ceStSeries /
     *  peStSeries are the per-bar Supertrend { t, line, isUp } aligned with
     *  the candles, so the historical modal can render the same ST overlay
     *  as the live chart. */
    public static class DailySnapshot {
        public String date;
        public double spotOpen;
        public long   atmStrike;
        public String ceSymbol;
        public List<Candle> ceCandles = new ArrayList<>();
        public List<StPoint> ceStSeries = new ArrayList<>();
        public String peSymbol;
        public List<Candle> peCandles = new ArrayList<>();
        public List<StPoint> peStSeries = new ArrayList<>();
    }
    /** Per-bar Supertrend point. {@code t} is the bar's startMillis so the
     *  frontend can align it with the candle series index-for-index. */
    public static class StPoint {
        public long    t;
        public double  line;
        public boolean isUp;
        public StPoint() {}
        public StPoint(long t, double line, boolean isUp) { this.t = t; this.line = line; this.isUp = isUp; }
    }

    @PostConstruct
    public void boot() {
        try { ensureStorageDir(); }
        catch (Exception e) { log.warn("[HistoricalChartStore] failed to create dir: {}", e.getMessage()); }
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

    /** Fires at 15:45 IST every weekday. */
    @Scheduled(cron = "0 45 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledSave() {
        saveTodaySnapshot();
    }

    /** Snapshot the day's chosen CE + PE bars to disk. Idempotent. */
    public synchronized void saveTodaySnapshot() {
        VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
        if (s == null) {
            log.warn("[HistoricalChartStore] skip save — strategy bean not available");
            return;
        }
        String ceSym = s.getChosenCeSymbol();
        String peSym = s.getChosenPeSymbol();
        if ((ceSym == null || ceSym.isBlank()) && (peSym == null || peSym.isBlank())) {
            log.warn("[HistoricalChartStore] skip save — no chosen CE / PE for today");
            return;
        }
        String today = LocalDate.now(IST).toString();
        int tf = Math.max(1, riskSettings.getVwapStCandleMinutes());
        DailySnapshot snap = new DailySnapshot();
        snap.date      = today;
        snap.spotOpen  = s.getSpotOpen();
        snap.atmStrike = s.getAtmStrike();
        snap.ceSymbol  = ceSym;
        snap.peSymbol  = peSym;
        int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
        double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
        if (ceSym != null && !ceSym.isBlank()) {
            snap.ceCandles  = candleAggregator.getHistory(ceSym, tf);
            snap.ceStSeries = buildStSeries(snap.ceCandles, atrPeriod, mult);
        }
        if (peSym != null && !peSym.isBlank()) {
            snap.peCandles  = candleAggregator.getHistory(peSym, tf);
            snap.peStSeries = buildStSeries(snap.peCandles, atrPeriod, mult);
        }
        writeSnapshot(today, snap);
    }

    /** Per-bar Supertrend aligned to {@code bars}. Emits one {@link StPoint}
     *  per bar where the ST line is defined (bars 0..atrPeriod-2 have NaN
     *  and are skipped). Matches the shape ChartController.buildStSeries
     *  produces for the live chart. */
    private static List<StPoint> buildStSeries(List<Candle> bars, int atrPeriod, double mult) {
        if (bars == null || bars.isEmpty()) return new ArrayList<>();
        SuperTrend.Series ser = SuperTrend.series(bars, atrPeriod, mult);
        List<StPoint> out = new ArrayList<>(bars.size());
        for (int i = 0; i < bars.size(); i++) {
            double line = ser.line()[i];
            if (Double.isNaN(line)) continue;
            out.add(new StPoint(bars.get(i).startMillis(),
                Math.round(line * 100.0) / 100.0,
                ser.isUp()[i]));
        }
        return out;
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

    /** Newest-first list of dates for which a snapshot exists. */
    public List<String> listAvailableDates() {
        try {
            Path dir = Path.of(STORAGE_DIR);
            if (!Files.exists(dir)) return Collections.emptyList();
            try (Stream<Path> stream = Files.list(dir)) {
                List<String> dates = new ArrayList<>();
                stream.forEach(f -> {
                    String name = f.getFileName().toString();
                    if (name.endsWith(".json") && name.length() == 15) {
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

    private boolean snapshotExists(String date) { return Files.exists(pathFor(date)); }
    private Path pathFor(String date)           { return Path.of(STORAGE_DIR, date + ".json"); }
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
            log.info("[HistoricalChartStore] saved {} — CE {} ({} bars), PE {} ({} bars)",
                date, snap.ceSymbol, snap.ceCandles.size(),
                snap.peSymbol, snap.peCandles.size());
        } catch (IOException | RuntimeException e) {
            log.warn("[HistoricalChartStore] save {} failed: {}", date, e.getMessage());
        }
    }

    public static String storageDir() { return STORAGE_DIR; }
}
