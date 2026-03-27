package com.rydytrader.autotrader.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    private final TelegramService telegramService;
    private final List<String> tradeLogs = new CopyOnWriteArrayList<>();

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final String LOG_FILE = "../store/event-log.txt";

    public EventService(TelegramService telegramService) {
        this.telegramService = telegramService;
        ensureLogDir();
        clearIfStale();
        loadLogsFromFile();
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
        loadLogsFromFile();
    }

    public void clearToday() {
        tradeLogs.clear();
        try { Files.deleteIfExists(Paths.get(LOG_FILE)); } catch (IOException e) { log.error("Error clearing event log", e); }
    }

    private void ensureLogDir() {
        try {
            Path parent = Paths.get(LOG_FILE).getParent();
            if (parent != null) Files.createDirectories(parent);
        } catch (IOException e) { log.error("Error creating log directory", e); }
    }

    /** If the log file's last modified date is not today, clear it. */
    private void clearIfStale() {
        try {
            Path path = Paths.get(LOG_FILE);
            if (!Files.exists(path)) return;
            LocalDate fileDate = LocalDate.ofInstant(
                    Files.getLastModifiedTime(path).toInstant(),
                    java.time.ZoneId.systemDefault());
            if (!fileDate.equals(LocalDate.now())) {
                Files.deleteIfExists(path);
                log.info("Cleared stale event log from {}", fileDate);
            }
        } catch (IOException e) {
            log.error("Error checking event log date", e);
        }
    }

    private void loadLogsFromFile() {
        File file = new File(LOG_FILE);
        if (!file.exists()) return;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null)
                if (!line.isBlank()) tradeLogs.add(line);
            log.info("Loaded {} log entries from {}", tradeLogs.size(), file.getPath());
        } catch (IOException e) {
            log.error("Failed to load logs: {}", e.getMessage());
        }
    }

    private void writeToFile(String entry) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
            writer.write(entry);
            writer.newLine();
        } catch (IOException e) {
            log.error("Failed to write log: {}", e.getMessage());
        }
    }
}
