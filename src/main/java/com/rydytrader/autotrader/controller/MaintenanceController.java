package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.HistoricalChartStore;
import com.rydytrader.autotrader.service.strategy.VwapSupertrendStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Maintenance endpoints — destructive recovery actions triggered from the
 * gear-modal Maintenance tab. Wipe today's records or every historical row,
 * leaving open positions running (the strategy FSM keeps managing them).
 */
@RestController
public class MaintenanceController {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceController.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final StrategyTradeRepository tradeRepo;
    private final EventService            eventService;
    private final ObjectProvider<VwapSupertrendStrategy> strategyProvider;

    public MaintenanceController(StrategyTradeRepository tradeRepo,
                                  EventService eventService,
                                  ObjectProvider<VwapSupertrendStrategy> strategyProvider) {
        this.tradeRepo        = tradeRepo;
        this.eventService     = eventService;
        this.strategyProvider = strategyProvider;
    }

    /** Delete every {@code strategy_trades} row whose {@code sessionDate} equals
     *  today (IST), and clear today's in-memory + on-disk event log. Open
     *  positions on Fyers are NOT touched — the strategy FSM continues to
     *  manage them. */
    @PostMapping("/api/maintenance/clear-today")
    public Map<String, Object> clearToday() {
        String today = LocalDate.now(IST).toString();
        int dbCleared = 0;
        int eventsBefore = eventService.getTradeLogs().size();
        try {
            dbCleared = (int) tradeRepo.deleteBySessionDate(today);
        } catch (Exception e) {
            log.warn("[Maintenance] clearToday DB delete threw: {}", e.getMessage());
        }
        try { eventService.clearToday(); }
        catch (Exception e) { log.warn("[Maintenance] clearToday event clear threw: {}", e.getMessage()); }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("cyclesCleared", dbCleared);      // legacy field name for the UI
        out.put("eventsCleared", eventsBefore);
        out.put("dbCleared",     dbCleared);
        out.put("date",          today);
        log.info("[Maintenance] cleared today {} — dbRows={} events={}", today, dbCleared, eventsBefore);
        return out;
    }

    /** Wipe EVERY row in {@code strategy_trades} + every event in the ring +
     *  the persisted event log file + the strategy's in-memory today-trades
     *  map + realised P&L counter + every daily chart snapshot. Irreversible.
     *  Open positions on Fyers are NOT touched. */
    @PostMapping("/api/maintenance/clear-all")
    public Map<String, Object> clearAll() {
        int dbCleared = 0;
        int eventsBefore = eventService.getTradeLogs().size();
        int snapshotsCleared = 0;
        int cacheCleared = 0;
        try {
            dbCleared = tradeRepo.deleteAllRows();
        } catch (Exception e) {
            log.warn("[Maintenance] clearAll DB delete threw: {}", e.getMessage());
        }
        try { eventService.clearToday(); }
        catch (Exception e) { log.warn("[Maintenance] clearAll event clear threw: {}", e.getMessage()); }
        // Wipe the strategy's in-memory today-trades ring so the positions
        // page's todayClosedTrades() call returns [] until fresh exits fire.
        try {
            VwapSupertrendStrategy s = strategyProvider.getIfAvailable();
            if (s != null) s.clearInMemoryTradeState();
        } catch (Exception e) {
            log.warn("[Maintenance] clearAll strategy in-memory clear threw: {}", e.getMessage());
        }
        // Delete every store/data/charts/YYYY-MM-DD.json daily snapshot so
        // the calendar's historical modal has nothing to show.
        try {
            Path chartsDir = Path.of(HistoricalChartStore.storageDir());
            if (Files.exists(chartsDir) && Files.isDirectory(chartsDir)) {
                try (Stream<Path> files = Files.list(chartsDir)) {
                    for (Path f : (Iterable<Path>) files::iterator) {
                        String name = f.getFileName().toString();
                        if (name.endsWith(".json")) {
                            try { Files.delete(f); snapshotsCleared++; }
                            catch (Exception ignore) {}
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[Maintenance] clearAll snapshot delete threw: {}", e.getMessage());
        }
        // Wipe the persisted state caches in ../store/cache so a fresh boot
        // starts from scratch: vwap-supertrend FSM snapshot, candle ring,
        // and any other JSON caches sitting in that dir.
        try {
            Path cacheDir = Path.of("../store/cache");
            if (Files.exists(cacheDir) && Files.isDirectory(cacheDir)) {
                try (Stream<Path> files = Files.list(cacheDir)) {
                    for (Path f : (Iterable<Path>) files::iterator) {
                        String name = f.getFileName().toString();
                        if (name.endsWith(".json") || name.endsWith(".tmp")) {
                            try { Files.delete(f); cacheCleared++; }
                            catch (Exception ignore) {}
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[Maintenance] clearAll cache delete threw: {}", e.getMessage());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("cyclesCleared",    dbCleared);
        out.put("eventsCleared",    eventsBefore);
        out.put("snapshotsCleared", snapshotsCleared);
        out.put("cacheCleared",     cacheCleared);
        out.put("dbCleared",        dbCleared);
        log.info("[Maintenance] cleared ALL — dbRows={} events={} snapshots={} cache={}",
            dbCleared, eventsBefore, snapshotsCleared, cacheCleared);
        return out;
    }
}
