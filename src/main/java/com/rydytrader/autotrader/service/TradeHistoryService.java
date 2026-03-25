package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.TradeRecord;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.LocalDate;
import java.nio.file.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class TradeHistoryService {

    private static final Logger log = LoggerFactory.getLogger(TradeHistoryService.class);

    private static final DateTimeFormatter DATE_FMT   = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String            FILE_PREFIX = "trades-history-";

    private static final String LOG_DIR = "../store/data/history";

    private final RiskSettingsStore riskSettings;
    private final List<TradeRecord> trades = Collections.synchronizedList(new ArrayList<>());
    // Dedup: track last recorded exit per symbol to prevent duplicate entries
    private final java.util.concurrent.ConcurrentHashMap<String, Long> lastRecordTime = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long DEDUP_WINDOW_MS = 5000; // 5 seconds

    public TradeHistoryService(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
        try {
            Files.createDirectories(Paths.get("../store/data/history"));
        } catch (IOException e) { log.error("Error creating trade history directories", e); }
        loadTodaysTradesFromFile();
    }

    public void addRecord(TradeRecord record) {
        trades.add(0, record);
        appendToFile(record);
        log.info("[LIVE] Trade: {} | {} | P&L: {} | Charges: {} | Net: {} | {}", record.getSymbol(), record.getSide(), record.getPnl(), record.getCharges(), record.getNetPnl(), record.getResult());
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason) {
        record(symbol, side, qty, entryPrice, exitPrice, exitReason, "");
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup) {
        // Dedup: skip if same symbol was recorded within the last 5 seconds
        long now = System.currentTimeMillis();
        Long lastTime = lastRecordTime.get(symbol);
        if (lastTime != null && (now - lastTime) < DEDUP_WINDOW_MS) {
            log.info("[TradeHistory] Duplicate record skipped for {} (last recorded {}ms ago)", symbol, now - lastTime);
            return;
        }
        lastRecordTime.put(symbol, now);
        double brokerage = riskSettings.getBrokeragePerOrder();
        addRecord(new TradeRecord(symbol, side, qty, entryPrice, exitPrice, exitReason, setup, brokerage));
    }

    public List<TradeRecord> getTrades() { return new ArrayList<>(trades); }

    public List<TradeRecord> getTradesForRange(LocalDate from, LocalDate to) {
        List<TradeRecord> result = new ArrayList<>();
        LocalDate d = from;
        while (!d.isAfter(to)) {
            String file = logDir() + "/" + FILE_PREFIX + d.format(DATE_FMT) + ".csv";
            Path path = Paths.get(file);
            if (Files.exists(path)) {
                try {
                    List<String> lines = Files.readAllLines(path);
                    for (int i = 1; i < lines.size(); i++) {
                        String[] p = lines.get(i).split(",");
                        if (p.length < 7) continue;
                        try {
                            String setup = p.length >= 8 ? p[7] : "";
                            double charges = p.length >= 10 ? Double.parseDouble(p[9]) : 0;
                            if (charges > 0) {
                                result.add(new TradeRecord(p[0], p[1], p[2],
                                    Integer.parseInt(p[3]),
                                    Double.parseDouble(p[4]),
                                    Double.parseDouble(p[5]),
                                    p[6], setup, charges, true));
                            } else {
                                result.add(new TradeRecord(p[0], p[1], p[2],
                                    Integer.parseInt(p[3]),
                                    Double.parseDouble(p[4]),
                                    Double.parseDouble(p[5]),
                                    p[6], setup));
                            }
                        } catch (Exception ignored) {}
                    }
                } catch (IOException e) { log.error("Error reading trade history file", e); }
            }
            d = d.plusDays(1);
        }
        return result;
    }

    public void reloadForCurrentMode() {
        trades.clear();
        loadTodaysTradesFromFile();
    }

    public void clearToday() {
        trades.clear();
        try { Files.deleteIfExists(Paths.get(todaysFile())); } catch (IOException e) { log.error("Error clearing today's trade file", e); }
    }

    // ── FILE PATHS ────────────────────────────────────────────────────────────
    private String logDir()   { return LOG_DIR; }
    private String todaysFile() {
        return logDir() + "/" + FILE_PREFIX + LocalDate.now().format(DATE_FMT) + ".csv";
    }

    private void appendToFile(TradeRecord r) {
        try {
            String filePath = todaysFile();
            boolean isNew = !Files.exists(Paths.get(filePath));
            try (FileWriter fw = new FileWriter(filePath, true)) {
                if (isNew) fw.write("timestamp,symbol,side,qty,entryPrice,exitPrice,exitReason,setup,pnl,charges,netPnl\n");
                fw.write(String.format("%s,%s,%s,%d,%.2f,%.2f,%s,%s,%.2f,%.2f,%.2f\n",
                    r.getTimestamp(), r.getSymbol(), r.getSide(), r.getQty(),
                    r.getEntryPrice(), r.getExitPrice(), r.getExitReason(), r.getSetup(),
                    r.getPnl(), r.getCharges(), r.getNetPnl()));
            }
        } catch (IOException e) { log.error("Error appending trade record to file", e); }
    }

    private void loadTodaysTradesFromFile() {
        try {
            Path path = Paths.get(todaysFile());
            if (!Files.exists(path)) return;
            List<String> lines = Files.readAllLines(path);
            for (int i = lines.size() - 1; i >= 1; i--) {
                String[] p = lines.get(i).split(",");
                if (p.length < 7) continue;
                try {
                    String setup = p.length >= 8 ? p[7] : "";
                    double charges = p.length >= 10 ? Double.parseDouble(p[9]) : 0;
                    if (charges > 0) {
                        trades.add(new TradeRecord(p[0], p[1], p[2],
                            Integer.parseInt(p[3]),
                            Double.parseDouble(p[4]),
                            Double.parseDouble(p[5]),
                            p[6], setup, charges, true));
                    } else {
                        trades.add(new TradeRecord(p[0], p[1], p[2],
                            Integer.parseInt(p[3]),
                            Double.parseDouble(p[4]),
                            Double.parseDouble(p[5]),
                            p[6], setup));
                    }
                } catch (Exception ignored) {}
            }
            log.info("Loaded {} trade(s) from {}", trades.size(), todaysFile());
        } catch (IOException e) { log.error("Error loading today's trades from file", e); }
    }
}