package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.store.ModeStore;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class EventService {

    private final ModeStore modeStore;
    private final TelegramService telegramService;
    private final List<String> tradeLogs = new CopyOnWriteArrayList<>();

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final String LOG_DIR_LIVE = "../store/live/events";
    private static final String LOG_DIR_SIM  = "../store/simulator/events";

    public EventService(ModeStore modeStore, TelegramService telegramService) {
        this.modeStore = modeStore;
        this.telegramService = telegramService;
        new File("../store/live/events").mkdirs();
        new File("../store/simulator/events").mkdirs();
        loadTodaysLogsFromFile();
    }

    public void log(String message) {
        String entry = LocalTime.now().format(TIME_FMT) + " - " + message;
        tradeLogs.add(entry);
        writeToFile(entry);
        if (isTelegramWorthy(message)) {
            telegramService.sendMessage(message);
        }
    }

    private boolean isTelegramWorthy(String msg) {
        if (msg.contains("[SUCCESS]") || msg.contains("[ERROR]")) return true;
        if (msg.contains("PROFIT") || msg.contains("LOSS")) return true;
        if (msg.contains("[WARNING]") && (msg.contains("UNPROTECTED") || msg.contains("failed") || msg.contains("rejected"))) return true;
        return false;
    }

    public List<String> getTradeLogs() { return tradeLogs; }

    /** Scans today's logs for the last entry fill and returns its HH:mm:ss timestamp, or "" if not found. */
    public String getEntryTimeFromLogs() {
        for (int i = tradeLogs.size() - 1; i >= 0; i--) {
            String line = tradeLogs.get(i);
            if (line.contains("order filled")) {
                // format is "HH:mm:ss - message"
                int dash = line.indexOf(" - ");
                if (dash >= 0) return line.substring(0, dash);
            }
        }
        return "";
    }

    public void reloadLogsForCurrentMode() {
        tradeLogs.clear();
        loadTodaysLogsFromFile();
    }

    public void clearToday() {
        tradeLogs.clear();
        try { Files.deleteIfExists(Paths.get(logFile())); } catch (IOException e) { e.printStackTrace(); }
    }

    private String logDir()  { return modeStore.isLive() ? LOG_DIR_LIVE : LOG_DIR_SIM; }
    private String logFile() { return logDir() + "/event-logs-" + LocalDate.now().format(DATE_FMT) + ".txt"; }

    private void loadTodaysLogsFromFile() {
        File file = new File(logFile());
        if (!file.exists()) return;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null)
                if (!line.isBlank()) tradeLogs.add(line);
            // No cap — keep all of today's logs in memory
            System.out.println("Loaded " + tradeLogs.size() + " log entries from " + file.getPath());
        } catch (IOException e) {
            System.err.println("Failed to load logs: " + e.getMessage());
        }
    }

    private void writeToFile(String entry) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile(), true))) {
            writer.write(entry);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Failed to write log: " + e.getMessage());
        }
    }
}