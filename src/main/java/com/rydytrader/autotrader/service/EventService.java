package com.rydytrader.autotrader.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    private final List<String> tradeLogs = new CopyOnWriteArrayList<>();

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final String LOG_FILE = "../store/events/event-log.txt";

    public EventService() {
        ensureLogDir();
        clearIfStale();
        // Load last 500 entries immediately for fast startup, then load all async
        loadLogsFromFile();
        loadRemainingAsync();
        // First log entry of the day (only if log was cleared / fresh)
        if (tradeLogs.isEmpty()) {
            log("[INFO] Bot starting — initializing services");
        }
    }

    private static final java.time.ZoneId IST = java.time.ZoneId.of("Asia/Kolkata");
    private volatile LocalDate lastLogDate = LocalDate.now(IST);

    public void log(String message) {
        // Daily reset check: if date changed since last log, clear the file
        LocalDate today = LocalDate.now(IST);
        if (!today.equals(lastLogDate)) {
            log.info("New trading day detected ({} → {}), clearing event log", lastLogDate, today);
            tradeLogs.clear();
            try { Files.deleteIfExists(Paths.get(LOG_FILE)); } catch (IOException e) { log.error("Error clearing event log on day change", e); }
            updateLogDate();
            lastLogDate = today;
        }
        if (message == null) message = "";
        // Auto-prepend the calling class name when the caller hasn't supplied their own
        // [TAG] prefix. Saves editing 150+ call sites and keeps existing tags intact.
        if (!message.startsWith("[")) {
            String caller = detectCallerClass();
            if (caller != null) message = "[" + caller + "] " + message;
        }
        String entry = LocalTime.now().format(TIME_FMT) + " - " + message;
        tradeLogs.add(entry);
        writeToFile(entry);
    }

    /** Walks one frame back past this class to find the immediate caller's simple class name. */
    private String detectCallerClass() {
        try {
            StackTraceElement[] stack = new Throwable().getStackTrace();
            // stack[0] = detectCallerClass, stack[1] = log, stack[2] = direct caller.
            // If EventService is itself wrapped (unlikely), skip additional EventService frames.
            for (int i = 2; i < stack.length; i++) {
                String fqcn = stack[i].getClassName();
                if (fqcn.contains("EventService")) continue;
                int dot = fqcn.lastIndexOf('.');
                String simple = dot >= 0 ? fqcn.substring(dot + 1) : fqcn;
                int dollar = simple.indexOf('$');       // strip lambda / inner class suffix
                if (dollar >= 0) simple = simple.substring(0, dollar);
                return simple;
            }
        } catch (Exception ignored) {}
        return null;
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

    private static final String LOG_DATE_FILE = "../store/events/event-log.date";

    /** Clear log if the recorded trading date is not today. */
    private void clearIfStale() {
        try {
            Path datePath = Paths.get(LOG_DATE_FILE);
            Path logPath = Paths.get(LOG_FILE);
            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            LocalDate today = LocalDate.now(ist);
            String storedDate = Files.exists(datePath) ? Files.readString(datePath).trim() : "";
            log.info("Event log stored date: '{}' | today IST: {}", storedDate, today);
            if (!today.toString().equals(storedDate)) {
                if (Files.exists(logPath)) {
                    Files.deleteIfExists(logPath);
                    log.info("Cleared stale event log (was: '{}', now: '{}')", storedDate, today);
                }
                Files.writeString(datePath, today.toString());
            }
        } catch (IOException e) {
            log.error("Error checking event log date", e);
        }
    }

    /** Update the stored date marker when log is written today. */
    private void updateLogDate() {
        try {
            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            Files.writeString(Paths.get(LOG_DATE_FILE), LocalDate.now(ist).toString());
        } catch (IOException ignored) {}
    }

    private static final int FAST_LOAD_ENTRIES = 500;
    private volatile List<String> skippedEntries = null; // entries skipped during fast load

    private void loadLogsFromFile() {
        File file = new File(LOG_FILE);
        if (!file.exists()) return;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            List<String> all = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null)
                if (!line.isBlank()) all.add(line);
            int total = all.size();
            if (total <= FAST_LOAD_ENTRIES) {
                tradeLogs.addAll(all);
                skippedEntries = null;
            } else {
                // Fast load: last N entries, save earlier ones for async
                skippedEntries = new ArrayList<>(all.subList(0, total - FAST_LOAD_ENTRIES));
                for (int i = total - FAST_LOAD_ENTRIES; i < total; i++) tradeLogs.add(all.get(i));
            }
            log.info("Fast-loaded {} log entries from {} (total: {}, deferred: {})",
                tradeLogs.size(), file.getPath(), total, skippedEntries != null ? skippedEntries.size() : 0);
        } catch (IOException e) {
            log.error("Failed to load logs: {}", e.getMessage());
        }
    }

    private void loadRemainingAsync() {
        if (skippedEntries == null || skippedEntries.isEmpty()) return;
        new Thread(() -> {
            try {
                Thread.sleep(5000); // wait for server to finish starting
                List<String> deferred = skippedEntries;
                skippedEntries = null;
                // Prepend older entries before the fast-loaded ones
                List<String> merged = new ArrayList<>(deferred.size() + tradeLogs.size());
                merged.addAll(deferred);
                merged.addAll(tradeLogs);
                tradeLogs.clear();
                tradeLogs.addAll(merged);
                log.info("Async-loaded remaining {} log entries (total now: {})", deferred.size(), tradeLogs.size());
            } catch (Exception e) {
                log.error("Failed to async-load remaining logs: {}", e.getMessage());
            }
        }, "event-log-async-loader").start();
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
