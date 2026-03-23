package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.CprLevels;
import jakarta.annotation.PostConstruct;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

@Service
public class BhavcopyService {

    private static final String CM_URL_TEMPLATE =
        "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_%s_F_0000.csv.zip";
    private static final String FO_URL_TEMPLATE =
        "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_%s_F_0000.csv.zip";
    private static final String STORE_FILE = "../store/cpr-data.json";
    private static final String NSE_BASE_URL = "https://www.nseindia.com/";
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String USER_AGENT =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, CprLevels> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CprLevels> previousCache = new ConcurrentHashMap<>();
    private volatile String cachedDate = "";
    private volatile String previousDate = "";
    private final EventService eventService;

    public BhavcopyService(EventService eventService) {
        this.eventService = eventService;
        new File("../store").mkdirs();
    }

    @PostConstruct
    public void init() {
        loadFromFile();
        String expectedDate = getLastTradingDay().toString();
        if (!expectedDate.equals(cachedDate)) {
            fetchAndCompute();
        } else {
            // If previous day data is missing, fetch it now
            if (previousCache.isEmpty() && !cache.isEmpty()) {
                System.out.println("[BhavcopyService] Previous day CPR missing, fetching...");
                try {
                    String cookies = getNseCookies();
                    Set<String> nfoSymbols = cache.keySet();
                    if (cookies != null && !cookies.isEmpty()) {
                        fetchPreviousDay(LocalDate.parse(cachedDate), cookies, nfoSymbols);
                        saveToFile();
                    }
                } catch (Exception e) {
                    System.err.println("[BhavcopyService] Failed to fetch previous day: " + e.getMessage());
                }
            }
            long narrowCount = cache.values().stream().filter(CprLevels::isNarrowCpr).count();
            long insideCount = getInsideCprStocks().size();
            System.out.println("[BhavcopyService] Loaded " + cache.size()
                + " NFO stocks from cache for " + cachedDate + " (" + narrowCount + " narrow, " + insideCount + " inside CPR)");
        }
    }

    @Scheduled(cron = "0 45 8 * * MON-FRI")
    public void scheduledFetch() {
        fetchAndCompute();
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    public CprLevels getCprLevels(String symbol) {
        return cache.get(extractTicker(symbol));
    }

    public Map<String, CprLevels> getAllCprLevels() {
        return Collections.unmodifiableMap(cache);
    }

    public List<CprLevels> getNarrowCprStocks() {
        return cache.values().stream()
            .filter(CprLevels::isNarrowCpr)
            .filter(c -> c.getClose() > 300)
            .sorted(Comparator.comparingDouble(CprLevels::getCprWidthPct))
            .collect(Collectors.toList());
    }

    public List<CprLevels> getInsideCprStocks() {
        return cache.values().stream()
            .filter(c -> c.getClose() > 300)
            .filter(c -> {
                CprLevels prev = previousCache.get(c.getSymbol());
                if (prev == null) return false;
                double todayTop = Math.max(c.getTc(), c.getBc());
                double todayBot = Math.min(c.getTc(), c.getBc());
                double prevTop  = Math.max(prev.getTc(), prev.getBc());
                double prevBot  = Math.min(prev.getTc(), prev.getBc());
                // Today's CPR must be fully contained inside yesterday's CPR
                return todayTop <= prevTop && todayBot >= prevBot;
            })
            .sorted(Comparator.comparingDouble(CprLevels::getCprWidthPct))
            .collect(Collectors.toList());
    }

    public String getCachedDate()   { return cachedDate; }
    public String getPreviousDate() { return previousDate; }
    public int getLoadedCount()     { return cache.size(); }
    public CprLevels getPreviousCpr(String symbol) { return previousCache.get(symbol); }

    // ── Core fetch logic ───────────────────────────────────────────────────────

    private void fetchAndCompute() {
        LocalDate targetDate = getLastTradingDay();

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                String dateStr = DATE_FMT.format(targetDate);
                String cookies = getNseCookies();
                if (cookies == null || cookies.isEmpty()) {
                    System.err.println("[BhavcopyService] Failed to obtain NSE session cookies");
                    return;
                }

                // Download FO Bhavcopy for NFO stock list
                String foUrl = String.format(FO_URL_TEMPLATE, dateStr);
                byte[] foZip = downloadZip(foUrl, cookies);
                if (foZip == null) {
                    System.err.println("[BhavcopyService] FO Bhavcopy not available for " + targetDate
                        + ", trying previous day");
                    targetDate = skipWeekends(targetDate.minusDays(1));
                    continue;
                }

                Set<String> nfoSymbols = extractNfoSymbols(foZip);
                System.out.println("[BhavcopyService] FO Bhavcopy yielded " + nfoSymbols.size() + " unique NFO stock symbols");
                if (nfoSymbols.isEmpty()) {
                    System.err.println("[BhavcopyService] No NFO symbols found in FO Bhavcopy for " + targetDate);
                    targetDate = skipWeekends(targetDate.minusDays(1));
                    continue;
                }

                // Download CM Bhavcopy for OHLC data
                String cmUrl = String.format(CM_URL_TEMPLATE, dateStr);
                byte[] cmZip = downloadZip(cmUrl, cookies);
                if (cmZip == null) {
                    System.err.println("[BhavcopyService] CM Bhavcopy not available for " + targetDate);
                    targetDate = skipWeekends(targetDate.minusDays(1));
                    continue;
                }

                Map<String, double[]> ohlcMap = parseCmOhlc(cmZip, nfoSymbols);

                // Log any NFO symbols not found in CM Bhavcopy
                if (ohlcMap.size() < nfoSymbols.size()) {
                    Set<String> missing = new HashSet<>(nfoSymbols);
                    missing.removeAll(ohlcMap.keySet());
                    System.out.println("[BhavcopyService] NFO symbols: " + nfoSymbols.size()
                        + ", CM matches: " + ohlcMap.size() + ", missing: " + missing);
                }

                // Compute CPR levels
                ConcurrentHashMap<String, CprLevels> newCache = new ConcurrentHashMap<>();
                for (Map.Entry<String, double[]> entry : ohlcMap.entrySet()) {
                    double[] hlc = entry.getValue();
                    newCache.put(entry.getKey(), new CprLevels(entry.getKey(), hlc[0], hlc[1], hlc[2]));
                }

                // Current cache becomes previous day (for next day's inside CPR comparison)
                if (!cache.isEmpty() && !cachedDate.equals(targetDate.toString())) {
                    previousCache.clear();
                    previousCache.putAll(cache);
                    previousDate = cachedDate;
                }

                cache.clear();
                cache.putAll(newCache);
                cachedDate = targetDate.toString();

                // If still no previous day data, fetch it explicitly
                if (previousCache.isEmpty()) {
                    fetchPreviousDay(targetDate, cookies, nfoSymbols);
                }

                saveToFile();

                long narrowCount = cache.values().stream().filter(CprLevels::isNarrowCpr).count();
                long insideCount = getInsideCprStocks().size();
                String msg = "[BhavcopyService] Loaded CPR for " + cache.size()
                    + " NFO stocks for " + cachedDate + " (" + narrowCount + " narrow, " + insideCount + " inside CPR)";
                System.out.println(msg);
                eventService.log(msg);
                return;

            } catch (Exception e) {
                System.err.println("[BhavcopyService] Error fetching Bhavcopy for " + targetDate + ": " + e.getMessage());
                targetDate = skipWeekends(targetDate.minusDays(1));
            }
        }

        System.err.println("[BhavcopyService] Failed to fetch Bhavcopy after 3 attempts, using cached data");
    }

    // ── Fetch previous trading day's bhavcopy for inside CPR ───────────────────

    private void fetchPreviousDay(LocalDate currentDate, String cookies, Set<String> nfoSymbols) {
        LocalDate prevDate = skipWeekends(currentDate.minusDays(1));
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                String dateStr = DATE_FMT.format(prevDate);
                String cmUrl = String.format(CM_URL_TEMPLATE, dateStr);
                byte[] cmZip = downloadZip(cmUrl, cookies);
                if (cmZip == null) {
                    prevDate = skipWeekends(prevDate.minusDays(1));
                    continue;
                }

                Map<String, double[]> ohlcMap = parseCmOhlc(cmZip, nfoSymbols);
                previousCache.clear();
                for (Map.Entry<String, double[]> entry : ohlcMap.entrySet()) {
                    double[] hlc = entry.getValue();
                    previousCache.put(entry.getKey(), new CprLevels(entry.getKey(), hlc[0], hlc[1], hlc[2]));
                }
                previousDate = prevDate.toString();
                System.out.println("[BhavcopyService] Loaded previous day CPR for " + previousCache.size()
                    + " stocks from " + previousDate);
                return;
            } catch (Exception e) {
                System.err.println("[BhavcopyService] Error fetching previous day Bhavcopy for " + prevDate + ": " + e.getMessage());
                prevDate = skipWeekends(prevDate.minusDays(1));
            }
        }
        System.err.println("[BhavcopyService] Failed to fetch previous day Bhavcopy after 3 attempts");
    }

    // ── NSE session cookies ────────────────────────────────────────────────────

    private String getNseCookies() {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(NSE_BASE_URL).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", USER_AGENT);
            conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            conn.setRequestProperty("Accept-Language", "en-US,en;q=0.9");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(10_000);
            conn.setInstanceFollowRedirects(true);
            conn.getResponseCode();

            StringBuilder cookies = new StringBuilder();
            List<String> setCookies = conn.getHeaderFields().getOrDefault("Set-Cookie", List.of());
            for (String cookie : setCookies) {
                if (cookies.length() > 0) cookies.append("; ");
                cookies.append(cookie.split(";")[0]);
            }
            return cookies.toString();
        } catch (Exception e) {
            System.err.println("[BhavcopyService] Failed to get NSE cookies: " + e.getMessage());
            return null;
        }
    }

    // ── ZIP download ───────────────────────────────────────────────────────────

    private byte[] downloadZip(String url, String cookies) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", USER_AGENT);
            conn.setRequestProperty("Referer", NSE_BASE_URL);
            conn.setRequestProperty("Accept", "*/*");
            conn.setRequestProperty("Accept-Language", "en-US,en;q=0.9");
            conn.setRequestProperty("Cookie", cookies);
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(60_000);

            int status = conn.getResponseCode();
            if (status != 200) {
                System.err.println("[BhavcopyService] HTTP " + status + " for " + url);
                return null;
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (InputStream is = conn.getInputStream()) {
                is.transferTo(baos);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            System.err.println("[BhavcopyService] Download failed for " + url + ": " + e.getMessage());
            return null;
        }
    }

    // ── Extract NFO symbols from FO Bhavcopy ───────────────────────────────────

    private Set<String> extractNfoSymbols(byte[] zipData) {
        Set<String> symbols = new HashSet<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
            if (zis.getNextEntry() == null) return symbols;

            BufferedReader br = new BufferedReader(new InputStreamReader(zis));
            String header = br.readLine();
            if (header == null) return symbols;

            String[] cols = header.split(",");
            int instIdx = -1, symIdx = -1, underlyingIdx = -1;
            for (int i = 0; i < cols.length; i++) {
                String col = cols[i].trim().replace("\"", "");
                if ("FinInstrmTp".equalsIgnoreCase(col)) instIdx = i;
                if ("TckrSymb".equalsIgnoreCase(col)) symIdx = i;
                if ("UndrlygVal".equalsIgnoreCase(col)) underlyingIdx = i;
            }

            // If UDiFF columns not found, try legacy column names
            if (instIdx == -1 || symIdx == -1) {
                for (int i = 0; i < cols.length; i++) {
                    String col = cols[i].trim().replace("\"", "");
                    if ("INSTRUMENT".equalsIgnoreCase(col)) instIdx = i;
                    if ("SYMBOL".equalsIgnoreCase(col)) symIdx = i;
                }
            }

            if (instIdx == -1 || symIdx == -1) {
                System.err.println("[BhavcopyService] FO CSV header not recognized: " + header);
                return symbols;
            }

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length <= Math.max(instIdx, symIdx)) continue;
                String inst = parts[instIdx].trim().replace("\"", "");
                // STF = Stock Futures (UDiFF), FUTSTK = legacy
                if ("STF".equalsIgnoreCase(inst) || "FUTSTK".equalsIgnoreCase(inst)) {
                    // Prefer UndrlygVal column (direct underlying symbol, handles 360ONE, 3MINDIA etc.)
                    // Fall back to SYMBOL (legacy) or TckrSymb extraction
                    if (underlyingIdx >= 0 && parts.length > underlyingIdx) {
                        String underlying = parts[underlyingIdx].trim().replace("\"", "");
                        if (!underlying.isEmpty()) {
                            symbols.add(underlying);
                            continue;
                        }
                    }
                    String sym = parts[symIdx].trim().replace("\"", "");
                    if ("FUTSTK".equalsIgnoreCase(inst)) {
                        symbols.add(sym);
                    } else {
                        symbols.add(extractUnderlyingFromFutTicker(sym));
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[BhavcopyService] Error parsing FO Bhavcopy: " + e.getMessage());
        }
        return symbols;
    }

    /**
     * Extracts underlying symbol from a futures ticker.
     * e.g. "RELIANCE26MAR2026FUT" → "RELIANCE"
     * Strips trailing expiry+FUT pattern: digits, month, year digits, "FUT"
     */
    private String extractUnderlyingFromFutTicker(String ticker) {
        // Remove "FUT" suffix, then strip trailing date portion (e.g. "26MAR2026" or "24MAR25")
        String s = ticker.replaceAll("\\d{2}[A-Z]{3}\\d{2,4}FUT$", "");
        return s.isEmpty() ? ticker : s;
    }

    // ── Parse CM Bhavcopy OHLC ─────────────────────────────────────────────────

    private Map<String, double[]> parseCmOhlc(byte[] zipData, Set<String> nfoSymbols) {
        Map<String, double[]> result = new LinkedHashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
            if (zis.getNextEntry() == null) return result;

            BufferedReader br = new BufferedReader(new InputStreamReader(zis));
            String header = br.readLine();
            if (header == null) return result;

            String[] cols = header.split(",");
            int symIdx = -1, seriesIdx = -1, highIdx = -1, lowIdx = -1, closeIdx = -1;

            for (int i = 0; i < cols.length; i++) {
                String col = cols[i].trim().replace("\"", "");
                switch (col) {
                    case "TckrSymb" -> symIdx = i;
                    case "SctySrs"  -> seriesIdx = i;
                    case "HghPric"  -> highIdx = i;
                    case "LwPric"   -> lowIdx = i;
                    case "ClsPric"  -> closeIdx = i;
                    // Legacy fallbacks
                    case "SYMBOL"   -> { if (symIdx == -1) symIdx = i; }
                    case "SERIES"   -> { if (seriesIdx == -1) seriesIdx = i; }
                    case "HIGH"     -> { if (highIdx == -1) highIdx = i; }
                    case "LOW"      -> { if (lowIdx == -1) lowIdx = i; }
                    case "CLOSE"    -> { if (closeIdx == -1) closeIdx = i; }
                }
            }

            if (symIdx == -1 || highIdx == -1 || lowIdx == -1 || closeIdx == -1) {
                System.err.println("[BhavcopyService] CM CSV header not recognized: " + header);
                return result;
            }

            int maxIdx = Math.max(Math.max(symIdx, seriesIdx), Math.max(Math.max(highIdx, lowIdx), closeIdx));

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length <= maxIdx) continue;

                String sym    = parts[symIdx].trim().replace("\"", "");
                String series = seriesIdx >= 0 ? parts[seriesIdx].trim().replace("\"", "") : "EQ";

                // Accept EQ, BE (trade-to-trade), BZ (suspended but traded)
                if (!"EQ".equalsIgnoreCase(series) && !"BE".equalsIgnoreCase(series)
                        && !"BZ".equalsIgnoreCase(series)) continue;
                if (!nfoSymbols.contains(sym)) continue;

                try {
                    double h = Double.parseDouble(parts[highIdx].trim().replace("\"", ""));
                    double l = Double.parseDouble(parts[lowIdx].trim().replace("\"", ""));
                    double c = Double.parseDouble(parts[closeIdx].trim().replace("\"", ""));
                    if (h > 0 && l > 0 && c > 0) {
                        result.put(sym, new double[]{h, l, c});
                    }
                } catch (NumberFormatException ignored) {}
            }
        } catch (Exception e) {
            System.err.println("[BhavcopyService] Error parsing CM Bhavcopy: " + e.getMessage());
        }
        return result;
    }

    // ── Date helpers ───────────────────────────────────────────────────────────

    private LocalDate getLastTradingDay() {
        return skipWeekends(LocalDate.now(IST).minusDays(1));
    }

    private LocalDate skipWeekends(LocalDate date) {
        while (date.getDayOfWeek() == DayOfWeek.SATURDAY || date.getDayOfWeek() == DayOfWeek.SUNDAY) {
            date = date.minusDays(1);
        }
        return date;
    }

    // ── File persistence ───────────────────────────────────────────────────────

    private void saveToFile() {
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("date", cachedDate);
            data.put("symbols", cache);
            if (!previousCache.isEmpty()) {
                data.put("previousDate", previousDate);
                data.put("previousSymbols", previousCache);
            }
            Files.writeString(Paths.get(STORE_FILE),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data));
        } catch (Exception e) {
            System.err.println("[BhavcopyService] Failed to save CPR data: " + e.getMessage());
        }
    }

    private void loadFromFile() {
        try {
            Path path = Paths.get(STORE_FILE);
            if (!Files.exists(path)) return;

            JsonNode root = mapper.readTree(Files.readString(path));
            JsonNode dateNode = root.get("date");
            JsonNode symbolsNode = root.get("symbols");
            if (dateNode == null || symbolsNode == null) return;

            cachedDate = dateNode.asText();
            symbolsNode.fields().forEachRemaining(entry -> {
                try {
                    CprLevels levels = mapper.treeToValue(entry.getValue(), CprLevels.class);
                    cache.put(entry.getKey(), levels);
                } catch (Exception ignored) {}
            });

            // Load previous day's CPR data
            JsonNode prevDateNode = root.get("previousDate");
            JsonNode prevSymbolsNode = root.get("previousSymbols");
            if (prevDateNode != null && prevSymbolsNode != null) {
                previousDate = prevDateNode.asText();
                prevSymbolsNode.fields().forEachRemaining(entry -> {
                    try {
                        CprLevels levels = mapper.treeToValue(entry.getValue(), CprLevels.class);
                        previousCache.put(entry.getKey(), levels);
                    } catch (Exception ignored) {}
                });
            }
        } catch (Exception e) {
            System.err.println("[BhavcopyService] Failed to load CPR data from file: " + e.getMessage());
        }
    }

    // ── Symbol extraction (same logic as MarginDataService) ────────────────────

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        int dash = s.indexOf('-');
        if (dash >= 0) s = s.substring(0, dash);
        return s;
    }
}
