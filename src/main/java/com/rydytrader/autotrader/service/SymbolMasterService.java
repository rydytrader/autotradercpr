package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SymbolMasterService {

    private static final Logger log = LoggerFactory.getLogger(SymbolMasterService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE = "../store/cache/symbol-master.json";

    private final EventService eventService;
    private final ObjectMapper mapper = new ObjectMapper();

    public SymbolMasterService(EventService eventService) {
        this.eventService = eventService;
    }

    // Column indices (0-based), verified against actual CSV sample:
    // [4] = tick size (e.g. 0.1), [9] = Fyers symbol (e.g. NSE:NIFTY26MARFUT)
    private static final int COL_TICK_SIZE = 4;
    private static final int COL_SYMBOL    = 9;

    private static final String[] CSV_URLS = {
        "https://public.fyers.in/sym_details/NSE_FO.csv",
        "https://public.fyers.in/sym_details/NSE_CM.csv"
    };

    private final ConcurrentHashMap<String, Double> tickSizes = new ConcurrentHashMap<>();

    /**
     * Load on startup. Tries disk cache first; if cache is from today, skip the two CSV downloads
     * entirely (saves 30–60 s when Fyers endpoint is slow). Otherwise refreshes from Fyers and
     * writes a fresh cache. The @Scheduled 9 AM job always re-fetches and overwrites the cache.
     */
    @PostConstruct
    public void loadOnStartup() {
        if (loadCache()) {
            String msg = "[CACHE] Symbol master restored from cache (" + tickSizes.size() + " symbols)";
            log.info(msg);
            eventService.log(msg);
            return;
        }
        loadAll();
        saveCache();
    }

    /** Reload every trading day at 9:00 AM to pick up new contract expirations. */
    @Scheduled(cron = "0 0 9 * * MON-FRI")
    public void reloadAtMarketOpen() {
        loadAll();
        saveCache();
    }

    /** Returns the tick size for the given Fyers symbol, defaulting to 0.05 if not found. */
    public double getTickSize(String symbol) {
        return tickSizes.getOrDefault(symbol, 0.05);
    }

    /** Returns number of symbols currently loaded. */
    public int getLoadedCount() {
        return tickSizes.size();
    }

    private void loadAll() {
        int loaded = 0;
        for (String csvUrl : CSV_URLS) {
            loaded += loadCsv(csvUrl);
        }
        String msg = "[SymbolMaster] Loaded " + loaded + " symbol tick sizes from Fyers symbol master";
        log.info(msg);
        eventService.log(msg);
    }

    private int loadCsv(String csvUrl) {
        int count = 0;
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(csvUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(60_000);

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()))) {
                String line;
                boolean firstLine = true;
                while ((line = br.readLine()) != null) {
                    if (firstLine) { firstLine = false; continue; }  // skip header row
                    String[] cols = line.split(",");
                    if (cols.length <= Math.max(COL_TICK_SIZE, COL_SYMBOL)) continue;
                    try {
                        String symbol = cols[COL_SYMBOL].trim();
                        double tick   = Double.parseDouble(cols[COL_TICK_SIZE].trim());
                        if (!symbol.isEmpty() && tick > 0) {
                            tickSizes.put(symbol, tick);
                            count++;
                        }
                    } catch (NumberFormatException ignored) {}
                }
            }
        } catch (Exception e) {
            log.error("[SymbolMaster] Failed to load {}: {}", csvUrl, e.getMessage());
        }
        return count;
    }

    /** Load cache if present and dated today. Returns true if loaded successfully. */
    private synchronized boolean loadCache() {
        try {
            Path path = Paths.get(CACHE_FILE);
            if (!Files.exists(path)) return false;
            JsonNode root = mapper.readTree(Files.readString(path));
            String fetchedAt = root.has("fetchedAt") ? root.get("fetchedAt").asText("") : "";
            if (fetchedAt.isEmpty()) return false;
            LocalDate cacheDate;
            try {
                cacheDate = LocalDate.parse(fetchedAt.substring(0, 10));  // YYYY-MM-DD prefix of ISO timestamp
            } catch (Exception e) {
                return false;
            }
            if (!cacheDate.equals(LocalDate.now(IST))) {
                log.info("[SymbolMaster] Cache is from {} (not today) — will refetch", cacheDate);
                return false;
            }
            JsonNode ticks = root.get("tickSizes");
            if (ticks == null || !ticks.isObject()) return false;
            ticks.fields().forEachRemaining(e -> {
                try { tickSizes.put(e.getKey(), e.getValue().asDouble()); } catch (Exception ignored) {}
            });
            return !tickSizes.isEmpty();
        } catch (Exception e) {
            log.error("[SymbolMaster] Failed to load cache: {}", e.getMessage());
            return false;
        }
    }

    /** Atomic write via .tmp + move. */
    private synchronized void saveCache() {
        try {
            Path path = Paths.get(CACHE_FILE);
            Files.createDirectories(path.getParent());
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("fetchedAt", LocalDate.now(IST).toString());
            data.put("tickSizes", tickSizes);
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(data));
            Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            log.error("[SymbolMaster] Failed to save cache: {}", e.getMessage());
        }
    }
}
