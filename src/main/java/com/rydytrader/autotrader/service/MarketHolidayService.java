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
import java.time.*;
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
    private static final String CACHE_FILE = "../store/cache/nse-holidays.json";
    private static final String LEGACY_CACHE_FILE = "../store/config/nse-holidays.json";
    private static final DateTimeFormatter NSE_DATE_FMT = DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<LocalDate, String> holidays = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile String cachedYear = "";

    // Reuse BhavcopyService's hardened cookie flow (two-step warmup, retries, rich headers)
    // instead of duplicating a fragile single-shot fetch here.
    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private BhavcopyService bhavcopyService;

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

    // ── Market Calendar Constants & Methods ─────────────────────────────────

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    public static final int MARKET_OPEN_MINUTE = 9 * 60 + 15;   // 9:15 AM = 555
    public static final int MARKET_CLOSE_MINUTE = 15 * 60 + 30;  // 3:30 PM = 930
    public static final int PRE_MARKET_MINUTE = 9 * 60;           // 9:00 AM = 540

    /** Is the market currently open (9:15 to 15:30 on a trading day)? */
    public boolean isMarketOpen() {
        if (!isTradingDay()) return false;
        int nowMin = getNowMinute();
        return nowMin >= MARKET_OPEN_MINUTE && nowMin <= MARKET_CLOSE_MINUTE;
    }

    /** Is it pre-market session (9:00 to 9:15)? */
    public boolean isPreMarket() {
        if (!isTradingDay()) return false;
        int nowMin = getNowMinute();
        return nowMin >= PRE_MARKET_MINUTE && nowMin < MARKET_OPEN_MINUTE;
    }

    /** Is today a trading day (not weekend, not holiday)? */
    public boolean isTradingDay() {
        return isTradingDay(LocalDate.now(IST));
    }

    public boolean isTradingDay(LocalDate date) {
        DayOfWeek dow = date.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) return false;
        return !isHoliday(date);
    }

    /** Is today the last trading day of the week? (for weekly squareoff) */
    public boolean isLastTradingDayOfWeek() {
        LocalDate today = LocalDate.now(IST);
        if (!isTradingDay(today)) return false;
        // Check if any remaining weekday this week is a trading day
        LocalDate next = today.plusDays(1);
        while (next.getDayOfWeek() != DayOfWeek.SATURDAY) {
            if (isTradingDay(next)) return false;
            next = next.plusDays(1);
        }
        return true; // no more trading days this week
    }

    /** Get next trading day after the given date. */
    public LocalDate getNextTradingDay(LocalDate after) {
        LocalDate d = after.plusDays(1);
        while (!isTradingDay(d)) {
            d = d.plusDays(1);
            if (d.isAfter(after.plusDays(30))) break; // safety: max 30 days ahead
        }
        return d;
    }

    /**
     * Return the most recent trading day that has at least opened.
     * - If today is a trading day AND market has opened (>= 9:15 IST) → today
     * - Otherwise walk back day-by-day until a trading day is found
     * Used for cache freshness: "cache is fresh if it was last written on today or the last completed trading day".
     */
    public LocalDate getLastTradingDay() {
        LocalDate d = LocalDate.now(IST);
        int nowMin = getNowMinute();
        if (isTradingDay(d) && nowMin >= MARKET_OPEN_MINUTE) return d;
        d = d.minusDays(1);
        int safety = 10;
        while (!isTradingDay(d) && safety-- > 0) d = d.minusDays(1);
        return d;
    }

    /** Current time as minutes since midnight (IST). */
    public int getNowMinute() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        return now.getHour() * 60 + now.getMinute();
    }

    // ── NSE Holiday Fetch ─────────────────────────────────────────────────

    private void fetchFromNse() {
        try {
            String cookies = getNseCookies();
            if (cookies == null || cookies.isEmpty()) {
                log.error("[MarketHoliday] Failed to get NSE cookies");
                return;
            }

            HttpURLConnection conn = (HttpURLConnection) new URL(HOLIDAY_API).openConnection();
            conn.setRequestMethod("GET");
            // Full browser-like headers — NSE's Akamai bot filter checks fingerprint, not just UA.
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
            conn.setRequestProperty("Accept", "*/*");
            conn.setRequestProperty("Accept-Language", "en-US,en;q=0.9");
            conn.setRequestProperty("Accept-Encoding", "gzip, deflate, br");
            conn.setRequestProperty("Connection", "keep-alive");
            conn.setRequestProperty("Referer", "https://www.nseindia.com/resources/exchange-communication-holidays");
            conn.setRequestProperty("X-Requested-With", "XMLHttpRequest");
            conn.setRequestProperty("Sec-Fetch-Dest", "empty");
            conn.setRequestProperty("Sec-Fetch-Mode", "cors");
            conn.setRequestProperty("Sec-Fetch-Site", "same-origin");
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
        return bhavcopyService != null ? bhavcopyService.getNseCookies() : null;
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

    /** Resolve the active cache path, migrating from legacy store/config/ if needed. */
    private Path resolveCachePath() {
        Path primary = Paths.get(CACHE_FILE);
        if (Files.exists(primary)) return primary;
        Path legacy = Paths.get(LEGACY_CACHE_FILE);
        if (Files.exists(legacy)) {
            try {
                Files.createDirectories(primary.getParent());
                Files.move(legacy, primary, StandardCopyOption.REPLACE_EXISTING);
                log.info("[MIGRATE] Moved {} -> {}", legacy, primary);
                return primary;
            } catch (IOException e) {
                log.warn("[MIGRATE] Failed to move {}, reading from legacy: {}", legacy, e.getMessage());
                return legacy;
            }
        }
        return primary;  // doesn't exist yet; created on first save
    }

    private void loadFromFile() {
        try {
            Path path = resolveCachePath();
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
