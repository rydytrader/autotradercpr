package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.EventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

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

    public MaintenanceController(StrategyTradeRepository tradeRepo,
                                  EventService eventService) {
        this.tradeRepo    = tradeRepo;
        this.eventService = eventService;
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
     *  the persisted event log file. Irreversible. Open positions on Fyers
     *  are NOT touched. */
    @PostMapping("/api/maintenance/clear-all")
    public Map<String, Object> clearAll() {
        int dbCleared = 0;
        int eventsBefore = eventService.getTradeLogs().size();
        try {
            dbCleared = tradeRepo.deleteAllRows();
        } catch (Exception e) {
            log.warn("[Maintenance] clearAll DB delete threw: {}", e.getMessage());
        }
        try { eventService.clearToday(); }
        catch (Exception e) { log.warn("[Maintenance] clearAll event clear threw: {}", e.getMessage()); }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("cyclesCleared", dbCleared);
        out.put("eventsCleared", eventsBefore);
        out.put("dbCleared",     dbCleared);
        log.info("[Maintenance] cleared ALL — dbRows={} events={}", dbCleared, eventsBefore);
        return out;
    }
}
