package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.TradeRecord;
import com.rydytrader.autotrader.store.ModeStore;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.LocalDate;
import java.util.stream.Collectors;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class TradeHistoryService {

    private static final DateTimeFormatter DATE_FMT   = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String            FILE_PREFIX = "trades-history-";

    private static final String LOG_DIR_LIVE = "../store/live/history";
    private static final String LOG_DIR_SIM  = "../store/simulator/history";

    private final ModeStore modeStore;
    private final List<TradeRecord> trades = Collections.synchronizedList(new ArrayList<>());

    public TradeHistoryService(ModeStore modeStore) {
        this.modeStore = modeStore;
        try {
            Files.createDirectories(Paths.get("../store/live/history"));
            Files.createDirectories(Paths.get("../store/simulator/history"));
        } catch (IOException e) { e.printStackTrace(); }
        loadTodaysTradesFromFile();
    }

    public void addRecord(TradeRecord record) {
        trades.add(0, record);
        appendToFile(record);
        System.out.println("[" + modeStore.getMode() + "] Trade: "
            + record.getSymbol() + " | " + record.getSide()
            + " | Result: " + record.getResult() + " | P&L: " + record.getPnl());
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason) {
        addRecord(new TradeRecord(symbol, side, qty, entryPrice, exitPrice, exitReason, ""));
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup) {
        addRecord(new TradeRecord(symbol, side, qty, entryPrice, exitPrice, exitReason, setup));
    }

    public List<TradeRecord> getTrades() { return new ArrayList<>(trades); }

    public List<TradeRecord> getTradesForRange(LocalDate from, LocalDate to) {
        List<TradeRecord> result = new ArrayList<>();
        // Iterate each day in the range and load CSV
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
                            String setup = p.length >= 9 ? p[7] : "";
                            result.add(new TradeRecord(p[0], p[1], p[2],
                                Integer.parseInt(p[3]),
                                Double.parseDouble(p[4]),
                                Double.parseDouble(p[5]),
                                p[6], setup));
                        } catch (Exception ignored) {}
                    }
                } catch (IOException e) { e.printStackTrace(); }
            }
            d = d.plusDays(1);
        }
        return result;
    }

    public void reloadForCurrentMode() {
        trades.clear();
        loadTodaysTradesFromFile();
    }

    // ── FILE PATHS ────────────────────────────────────────────────────────────
    private String logDir()   { return modeStore.isLive() ? LOG_DIR_LIVE : LOG_DIR_SIM; }
    private String todaysFile() {
        return logDir() + "/" + FILE_PREFIX + LocalDate.now().format(DATE_FMT) + ".csv";
    }

    private void appendToFile(TradeRecord r) {
        try {
            String filePath = todaysFile();
            boolean isNew = !Files.exists(Paths.get(filePath));
            try (FileWriter fw = new FileWriter(filePath, true)) {
                if (isNew) fw.write("timestamp,symbol,side,qty,entryPrice,exitPrice,exitReason,setup,pnl\n");
                fw.write(String.format("%s,%s,%s,%d,%.2f,%.2f,%s,%s,%.2f\n",
                    r.getTimestamp(), r.getSymbol(), r.getSide(), r.getQty(),
                    r.getEntryPrice(), r.getExitPrice(), r.getExitReason(), r.getSetup(), r.getPnl()));
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    private void loadTodaysTradesFromFile() {
        try {
            Path path = Paths.get(todaysFile());
            if (!Files.exists(path)) return;
            List<String> lines = Files.readAllLines(path);
            for (int i = lines.size() - 1; i >= 1; i--) {
                String[] p = lines.get(i).split(",");
                if (p.length < 8) continue;
                try {
                    String setup = p.length >= 9 ? p[7] : "";
                    trades.add(new TradeRecord(p[0], p[1], p[2],
                        Integer.parseInt(p[3]),
                        Double.parseDouble(p[4]),
                        Double.parseDouble(p[5]),
                        p[6], setup));
                } catch (Exception ignored) {}
            }
            System.out.println("Loaded " + trades.size() + " trade(s) from " + todaysFile());
        } catch (IOException e) { e.printStackTrace(); }
    }
}