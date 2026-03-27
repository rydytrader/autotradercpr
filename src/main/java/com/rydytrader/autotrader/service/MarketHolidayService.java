package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Fetches and caches NSE trading holidays.
 * Loads from NSE API on startup, caches to disk, refreshes daily.
 */
@Service
public class MarketHolidayService {

    private static final Logger log = LoggerFactory.getLogger(MarketHolidayService.class);

    private static final String HOLIDAY_API = "https://www.nseindia.com/api/holiday-master?type=trading";
    private static final String NSE_BASE_URL = "https://www.nseindia.com/";
    private static final String CACHE_FILE = "../store/config/nse-holidays.json";
    private static final DateTimeFormatter NSE_DATE_FMT = DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<LocalDate, String> holidays = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile String cachedYear = "";

    @PostConstruct
    public void init() {
        loadFromFile();
        String currentYear = String.valueOf(LocalDate.now().getYear());
        if (!currentYear.equals(cachedYear) || holidays.isEmpty()) {
            fetchFromNse();
        }
        log.info("[MarketHoliday] {} holidays loaded for {}", holidays.size(), currentYear);
    }

    /** Refresh daily at 8 AM to pick up any changes. */
    @Scheduled(cron = "0 0 8 * * MON-FRI")
    public void scheduledRefresh() {
        fetchFromNse();
    }

    /** Check if a date is an NSE trading holiday. */
    public boolean isHoliday(LocalDate date) {
        return holidays.containsKey(date);
    }

    /** Get holiday description for a date, or null if not a holiday. */
    public String getHolidayName(LocalDate date) {
        return holidays.get(date);
    }

    /** Get all holidays as list of {date, name} maps (for frontend). */
    public List<Map<String, String>> getHolidayList() {
        List<Map<String, String>> result = new ArrayList<>();
        List<LocalDate> sorted = new ArrayList<>(holidays.keySet());
        Collections.sort(sorted);
        for (LocalDate d : sorted) {
            result.add(Map.of("date", d.toString(), "name", holidays.getOrDefault(d, "")));
        }
        return result;
    }

    private void fetchFromNse() {
        try {
            String cookies = getNseCookies();
            if (cookies == null || cookies.isEmpty()) {
                log.error("[MarketHoliday] Failed to get NSE cookies");
                return;
            }

            HttpURLConnection conn = (HttpURLConnection) new URL(HOLIDAY_API).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Referer", "https://www.nseindia.com/resources/exchange-communication-holidays");
            conn.setRequestProperty("Cookie", cookies);
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(10_000);

            int status = conn.getResponseCode();
            if (status != 200) {
                log.error("[MarketHoliday] NSE API returned HTTP {}", status);
                return;
            }

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) sb.append(line);
            }

            JsonNode root = mapper.readTree(sb.toString());
            Map<LocalDate, String> newHolidays = new LinkedHashMap<>();

            // Parse only CM (Capital Market) segment — ignore settlement-only holidays (CBM etc.)
            JsonNode cmArr = root.get("CM");
            if (cmArr != null && cmArr.isArray()) {
                for (JsonNode entry : cmArr) {
                    String dateStr = entry.has("tradingDate") ? entry.get("tradingDate").asText() : "";
                    if (dateStr.isEmpty()) continue;
                    String desc = entry.has("description") ? entry.get("description").asText() : "";
                    try {
                        LocalDate date = LocalDate.parse(dateStr, NSE_DATE_FMT);
                        newHolidays.put(date, desc);
                    } catch (Exception ignored) {}
                }
            }

            if (!newHolidays.isEmpty()) {
                holidays.clear();
                holidays.putAll(newHolidays);
                cachedYear = String.valueOf(LocalDate.now().getYear());
                saveToFile();
                log.info("[MarketHoliday] Fetched {} holidays from NSE for {}", holidays.size(), cachedYear);
            }
        } catch (Exception e) {
            log.error("[MarketHoliday] Failed to fetch holidays: {}", e.getMessage());
        }
    }

    private String getNseCookies() {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(NSE_BASE_URL).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");
            conn.setRequestProperty("Accept", "text/html");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(10_000);
            conn.getResponseCode();

            StringBuilder cookies = new StringBuilder();
            List<String> setCookies = conn.getHeaderFields().getOrDefault("Set-Cookie", List.of());
            for (String cookie : setCookies) {
                if (cookies.length() > 0) cookies.append("; ");
                cookies.append(cookie.split(";")[0]);
            }
            return cookies.toString();
        } catch (Exception e) {
            log.error("[MarketHoliday] Cookie fetch error: {}", e.getMessage());
            return null;
        }
    }

    private void saveToFile() {
        try {
            Path path = Paths.get(CACHE_FILE);
            Files.createDirectories(path.getParent());
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("year", cachedYear);
            data.put("holidays", getHolidayList());
            Files.writeString(path, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data));
        } catch (Exception e) {
            log.error("[MarketHoliday] Failed to save: {}", e.getMessage());
        }
    }

    private void loadFromFile() {
        try {
            Path path = Paths.get(CACHE_FILE);
            if (!Files.exists(path)) return;

            JsonNode root = mapper.readTree(Files.readString(path));
            cachedYear = root.has("year") ? root.get("year").asText() : "";
            JsonNode arr = root.get("holidays");
            if (arr != null && arr.isArray()) {
                for (JsonNode node : arr) {
                    if (node.isObject() && node.has("date")) {
                        String date = node.get("date").asText();
                        String name = node.has("name") ? node.get("name").asText() : "";
                        holidays.put(LocalDate.parse(date), name);
                    } else if (node.isTextual()) {
                        // backward compat: old format was just date strings
                        holidays.put(LocalDate.parse(node.asText()), "");
                    }
                }
            }
            log.info("[MarketHoliday] Loaded {} holidays from cache for {}", holidays.size(), cachedYear);
        } catch (Exception e) {
            log.error("[MarketHoliday] Failed to load cache: {}", e.getMessage());
        }
    }
}
