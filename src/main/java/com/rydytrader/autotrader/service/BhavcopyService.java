package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.CprLevels;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(BhavcopyService.class);

    private static final String CM_URL_TEMPLATE =
        "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_%s_F_0000.csv.zip";
    private static final String FO_URL_TEMPLATE =
        "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_%s_F_0000.csv.zip";
    private static final String STORE_FILE = "../store/config/cpr-data.json";
    private static final String NSE_BASE_URL = "https://www.nseindia.com/";
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String USER_AGENT =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, CprLevels> cache = new ConcurrentHashMap<>();
    private volatile String cachedDate = "";

    // Rolling 5-day history (most recent first) for weekly CPR and inside-CPR detection
    private final LinkedList<DaySnapshot> dailyHistory = new LinkedList<>();
    private static final int MAX_HISTORY_DAYS = 25;

    static class DaySnapshot {
        String date;
        Map<String, CprLevels> symbols = new LinkedHashMap<>();
    }
    private final EventService eventService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private SymbolMasterService symbolMasterService;

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
            // Backfill history if we have fewer than 5 days
            if (dailyHistory.size() < MAX_HISTORY_DAYS && !cache.isEmpty()) {
                log.info("[BhavcopyService] History incomplete ({} days), backfilling...", dailyHistory.size());
                try {
                    String cookies = getNseCookies();
                    Set<String> nfoSymbols = cache.keySet();
                    if (cookies != null && !cookies.isEmpty()) {
                        backfillHistory(LocalDate.parse(cachedDate), cookies, nfoSymbols);
                        saveToFile();
                    }
                } catch (Exception e) {
                    log.error("[BhavcopyService] Failed to backfill history: {}", e.getMessage());
                }
            }
            // Purge empty history snapshots (from previous bad saves) and trigger backfill
            int purged = 0;
            var iter = dailyHistory.iterator();
            while (iter.hasNext()) {
                if (iter.next().symbols.isEmpty()) { iter.remove(); purged++; }
            }
            if (purged > 0) {
                log.info("[BhavcopyService] Purged {} empty history snapshots, will backfill", purged);
                try {
                    String cookies = getNseCookies();
                    if (cookies != null && !cookies.isEmpty()) {
                        backfillHistory(LocalDate.parse(cachedDate), cookies, cache.keySet());
                        saveToFile();
                    }
                } catch (Exception e) {
                    log.error("[BhavcopyService] Failed to backfill after purge: {}", e.getMessage());
                }
            }
            // Classify narrow/inside range types from loaded cache + history
            classifyNarrowRangeTypes(cache);
            long narrowCount = cache.values().stream().filter(CprLevels::isNarrowCpr).count();
            long insideCount = getInsideCprStocks().size();
            log.info("[BhavcopyService] Loaded {} NFO stocks from cache for {} ({} narrow, {} inside CPR, {} history days)", cache.size(), cachedDate, narrowCount, insideCount, dailyHistory.size());
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
                        .sorted(Comparator.comparingDouble(CprLevels::getCprWidthPct))
            .collect(Collectors.toList());
    }

    public List<CprLevels> getInsideCprStocks() {
        Map<String, CprLevels> prevDay = getPreviousDaySymbols();
        if (prevDay.isEmpty()) return Collections.emptyList();
        return cache.values().stream()
                        .filter(c -> {
                CprLevels prev = prevDay.get(c.getSymbol());
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

    /**
     * Get weekly narrow CPR stocks by aggregating daily OHLC from Mon-Fri of the most recent complete week.
     * Uses cachedDate's week if it's a weekend, otherwise the previous week.
     */
    public List<CprLevels> getWeeklyNarrowCprStocks() {
        List<DaySnapshot> weekDays = getLastCompleteWeekDays();
        if (weekDays.size() < 3) return Collections.emptyList();

        // The last day in the week provides the close
        DaySnapshot lastDay = weekDays.get(weekDays.size() - 1);

        List<CprLevels> result = new ArrayList<>();
        for (String symbol : lastDay.symbols.keySet()) {
            double weekHigh = Double.MIN_VALUE;
            double weekLow = Double.MAX_VALUE;
            double weekClose = lastDay.symbols.get(symbol).getClose();

            for (DaySnapshot snap : weekDays) {
                CprLevels day = snap.symbols.get(symbol);
                if (day != null) {
                    weekHigh = Math.max(weekHigh, day.getHigh());
                    weekLow = Math.min(weekLow, day.getLow());
                }
            }
            if (weekHigh == Double.MIN_VALUE || weekLow == Double.MAX_VALUE) continue;

            CprLevels weeklyCpr = new CprLevels(symbol, weekHigh, weekLow, weekClose);
            if (symbolMasterService != null) {
                double tick = symbolMasterService.getTickSize("NSE:" + symbol + "-EQ");
                weeklyCpr.roundToTick(tick);
            }
            if (weeklyCpr.isNarrowCpr()) {
                result.add(weeklyCpr);
            }
        }
        result.sort(Comparator.comparingDouble(CprLevels::getCprWidthPct));
        return result;
    }

    /**
     * Get weekly inside CPR stocks: this week's CPR is fully inside previous week's CPR.
     */
    public List<CprLevels> getWeeklyInsideCprStocks() {
        List<DaySnapshot> thisWeekDays = getLastCompleteWeekDays();
        List<DaySnapshot> prevWeekDays = getPreviousCompleteWeekDays();
        if (thisWeekDays.size() < 3 || prevWeekDays.size() < 3) return Collections.emptyList();

        DaySnapshot thisLastDay = thisWeekDays.get(thisWeekDays.size() - 1);
        DaySnapshot prevLastDay = prevWeekDays.get(prevWeekDays.size() - 1);

        List<CprLevels> result = new ArrayList<>();
        for (String symbol : thisLastDay.symbols.keySet()) {
            double thisHigh = Double.MIN_VALUE, thisLow = Double.MAX_VALUE;
            double thisClose = thisLastDay.symbols.get(symbol).getClose();
            for (DaySnapshot snap : thisWeekDays) {
                CprLevels day = snap.symbols.get(symbol);
                if (day != null) { thisHigh = Math.max(thisHigh, day.getHigh()); thisLow = Math.min(thisLow, day.getLow()); }
            }
            if (thisHigh == Double.MIN_VALUE) continue;

            CprLevels prevDayData = prevLastDay.symbols.get(symbol);
            if (prevDayData == null) continue;
            double prevHigh = Double.MIN_VALUE, prevLow = Double.MAX_VALUE;
            double prevClose = prevDayData.getClose();
            for (DaySnapshot snap : prevWeekDays) {
                CprLevels day = snap.symbols.get(symbol);
                if (day != null) { prevHigh = Math.max(prevHigh, day.getHigh()); prevLow = Math.min(prevLow, day.getLow()); }
            }
            if (prevHigh == Double.MIN_VALUE) continue;

            CprLevels thisCpr = new CprLevels(symbol, thisHigh, thisLow, thisClose);
            CprLevels prevCpr = new CprLevels(symbol, prevHigh, prevLow, prevClose);
            if (symbolMasterService != null) {
                double tick = symbolMasterService.getTickSize("NSE:" + symbol + "-EQ");
                thisCpr.roundToTick(tick);
                prevCpr.roundToTick(tick);
            }

            double thisTop = Math.max(thisCpr.getTc(), thisCpr.getBc());
            double thisBot = Math.min(thisCpr.getTc(), thisCpr.getBc());
            double prevTop = Math.max(prevCpr.getTc(), prevCpr.getBc());
            double prevBot = Math.min(prevCpr.getTc(), prevCpr.getBc());

            if (thisTop <= prevTop && thisBot >= prevBot && prevTop > prevBot) {
                result.add(thisCpr);
            }
        }
        result.sort(Comparator.comparingDouble(CprLevels::getCprWidthPct));
        return result;
    }

    /** Get the week before the last complete week. */
    private List<DaySnapshot> getPreviousCompleteWeekDays() {
        List<DaySnapshot> allDays = new ArrayList<>();
        if (!cachedDate.isEmpty()) {
            DaySnapshot today = new DaySnapshot();
            today.date = cachedDate;
            today.symbols.putAll(cache);
            allDays.add(today);
        }
        allDays.addAll(dailyHistory);
        allDays.sort(Comparator.comparing(s -> s.date));
        if (allDays.isEmpty()) return Collections.emptyList();

        LocalDate today = LocalDate.now(IST);
        boolean isWeekend = (today.getDayOfWeek() == DayOfWeek.SATURDAY || today.getDayOfWeek() == DayOfWeek.SUNDAY);
        LocalDate refDate = isWeekend ? today.minusWeeks(1) : today.minusWeeks(2);
        int targetWeek = refDate.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        int targetYear = refDate.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);

        List<DaySnapshot> weekDays = new ArrayList<>();
        for (DaySnapshot snap : allDays) {
            LocalDate d = LocalDate.parse(snap.date);
            int w = d.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            int y = d.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);
            if (w == targetWeek && y == targetYear) weekDays.add(snap);
        }
        return weekDays;
    }

    /**
     * Get all days belonging to the last COMPLETED Mon-Fri week from history + cache.
     * Weekly CPR is calculated from previous week's HLC (same as daily CPR uses previous day).
     * On weekdays (Mon-Fri): previous week's data.
     * On weekends (Sat-Sun): the just-completed week's data (same week Monday will use).
     */
    private List<DaySnapshot> getLastCompleteWeekDays() {
        // Build a combined list: current cache + history, sorted by date
        List<DaySnapshot> allDays = new ArrayList<>();
        if (!cachedDate.isEmpty()) {
            DaySnapshot today = new DaySnapshot();
            today.date = cachedDate;
            today.symbols.putAll(cache);
            allDays.add(today);
        }
        allDays.addAll(dailyHistory);
        allDays.sort(Comparator.comparing(s -> s.date));

        if (allDays.isEmpty()) return Collections.emptyList();

        // Determine the target week: the last COMPLETED week
        // Use today's date (not cached date) to decide weekday vs weekend
        LocalDate today = LocalDate.now(IST);
        DayOfWeek dow = today.getDayOfWeek();
        boolean isWeekend = (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY);

        // On weekdays: previous week. On weekends: current (just-completed) week.
        int currentWeek = today.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        int currentYear = today.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);
        int targetWeek, targetYear;
        if (isWeekend) {
            targetWeek = currentWeek;
            targetYear = currentYear;
        } else {
            // Go back to previous week
            LocalDate prevWeekDate = today.minusWeeks(1);
            targetWeek = prevWeekDate.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            targetYear = prevWeekDate.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);
        }

        List<DaySnapshot> weekDays = new ArrayList<>();
        for (DaySnapshot snap : allDays) {
            LocalDate d = LocalDate.parse(snap.date);
            int w = d.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            int y = d.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);
            if (w == targetWeek && y == targetYear) {
                weekDays.add(snap);
            }
        }
        return weekDays;
    }

    public int getHistoryDays() { return dailyHistory.size(); }

    public String getWeekDateRange() {
        List<DaySnapshot> weekDays = getLastCompleteWeekDays();
        if (weekDays.isEmpty()) return cachedDate;
        return weekDays.get(0).date + " to " + weekDays.get(weekDays.size() - 1).date;
    }

    public String getCachedDate()   { return cachedDate; }
    public String getPreviousDate() { return dailyHistory.isEmpty() ? "" : dailyHistory.getFirst().date; }
    public int getLoadedCount()     { return cache.size(); }

    /** Get today's cache (latest day). */
    public Map<String, CprLevels> getTodayCache() { return Collections.unmodifiableMap(cache); }

    /** Get all daily history snapshots (newest first, excludes today). */
    public List<Map<String, CprLevels>> getDailyHistoryMaps() {
        List<Map<String, CprLevels>> result = new ArrayList<>();
        for (DaySnapshot snap : dailyHistory) {
            result.add(snap.symbols);
        }
        return result;
    }

    /** Get all daily history dates (newest first, excludes today). */
    public List<String> getDailyHistoryDates() {
        List<String> result = new ArrayList<>();
        for (DaySnapshot snap : dailyHistory) {
            result.add(snap.date);
        }
        return result;
    }

    private Map<String, CprLevels> getPreviousDaySymbols() {
        return dailyHistory.isEmpty() ? Collections.emptyMap() : dailyHistory.getFirst().symbols;
    }
    public CprLevels getPreviousCpr(String symbol) {
        Map<String, CprLevels> prev = getPreviousDaySymbols();
        return prev.get(symbol);
    }

    // ── Core fetch logic ───────────────────────────────────────────────────────

    private void fetchAndCompute() {
        LocalDate targetDate = getLastTradingDay();

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                String dateStr = DATE_FMT.format(targetDate);
                String cookies = getNseCookies();
                if (cookies == null || cookies.isEmpty()) {
                    log.error("[BhavcopyService] Failed to obtain NSE session cookies");
                    return;
                }

                // Download FO Bhavcopy for NFO stock list
                String foUrl = String.format(FO_URL_TEMPLATE, dateStr);
                byte[] foZip = downloadZip(foUrl, cookies);
                if (foZip == null) {
                    log.error("[BhavcopyService] FO Bhavcopy not available for {}, trying previous day", targetDate);
                    targetDate = skipWeekends(targetDate.minusDays(1));
                    continue;
                }

                Set<String> nfoSymbols = extractNfoSymbols(foZip);
                log.info("[BhavcopyService] FO Bhavcopy yielded {} unique NFO stock symbols", nfoSymbols.size());
                if (nfoSymbols.isEmpty()) {
                    log.error("[BhavcopyService] No NFO symbols found in FO Bhavcopy for {}", targetDate);
                    targetDate = skipWeekends(targetDate.minusDays(1));
                    continue;
                }

                // Download CM Bhavcopy for OHLC data
                String cmUrl = String.format(CM_URL_TEMPLATE, dateStr);
                byte[] cmZip = downloadZip(cmUrl, cookies);
                if (cmZip == null) {
                    log.error("[BhavcopyService] CM Bhavcopy not available for {}", targetDate);
                    targetDate = skipWeekends(targetDate.minusDays(1));
                    continue;
                }

                Map<String, double[]> ohlcMap = parseCmOhlc(cmZip, nfoSymbols);

                // Log any NFO symbols not found in CM Bhavcopy
                if (ohlcMap.size() < nfoSymbols.size()) {
                    Set<String> missing = new HashSet<>(nfoSymbols);
                    missing.removeAll(ohlcMap.keySet());
                    log.info("[BhavcopyService] NFO symbols: {}, CM matches: {}, missing: {}", nfoSymbols.size(), ohlcMap.size(), missing);
                }

                // Compute CPR levels
                ConcurrentHashMap<String, CprLevels> newCache = new ConcurrentHashMap<>();
                for (Map.Entry<String, double[]> entry : ohlcMap.entrySet()) {
                    double[] hlc = entry.getValue();
                    CprLevels lvl = new CprLevels(entry.getKey(), hlc[0], hlc[1], hlc[2]);
                    if (hlc.length > 3) lvl.setVolume((long) hlc[3]);
                    if (hlc.length > 4) lvl.setFiftyTwoWeekHigh(hlc[4]);
                    if (hlc.length > 5) lvl.setFiftyTwoWeekLow(hlc[5]);
                    if (symbolMasterService != null) {
                        double tick = symbolMasterService.getTickSize("NSE:" + entry.getKey() + "-EQ");
                        lvl.roundToTick(tick);
                    }
                    newCache.put(entry.getKey(), lvl);
                }

                // Push current cache into daily history (rolling buffer)
                if (!cache.isEmpty() && !cachedDate.equals(targetDate.toString())) {
                    DaySnapshot snapshot = new DaySnapshot();
                    snapshot.date = cachedDate;
                    snapshot.symbols.putAll(cache);
                    dailyHistory.addFirst(snapshot);
                    while (dailyHistory.size() > MAX_HISTORY_DAYS) dailyHistory.removeLast();
                }

                // Purge empty history snapshots and backfill before classification
                dailyHistory.removeIf(snap -> snap.symbols.isEmpty());

                cache.clear();
                cache.putAll(newCache);
                cachedDate = targetDate.toString();

                // Backfill history first (needed for z-score and inside CPR)
                if (dailyHistory.size() < MAX_HISTORY_DAYS) {
                    backfillHistory(targetDate, cookies, nfoSymbols);
                }

                // Classify after backfill so z-score has sufficient history
                classifyNarrowRangeTypes(cache);

                saveToFile();

                long narrowCount = cache.values().stream().filter(CprLevels::isNarrowCpr).count();
                long insideCount = getInsideCprStocks().size();
                String msg = "[BhavcopyService] Loaded CPR for " + cache.size()
                    + " NFO stocks for " + cachedDate + " (" + narrowCount + " narrow, " + insideCount + " inside CPR)";
                log.info(msg);
                eventService.log(msg);
                return;

            } catch (Exception e) {
                log.error("[BhavcopyService] Error fetching Bhavcopy for {}: {}", targetDate, e.getMessage());
                targetDate = skipWeekends(targetDate.minusDays(1));
            }
        }

        log.error("[BhavcopyService] Failed to fetch Bhavcopy after 3 attempts, using cached data");
    }

    /**
     * Classify every stock's range as SMALL or LARGE using a per-stock 20-day z-score
     * of the raw range x = High - Low: z = (x - μ) / σ. z < -1.5 → SMALL (genuinely
     * tight bar). Otherwise → LARGE.
     */
    private void classifyNarrowRangeTypes(Map<String, CprLevels> todayCache) {
        List<Map<String, CprLevels>> history = new ArrayList<>();
        if (!cache.isEmpty()) history.add(cache);
        for (DaySnapshot snap : dailyHistory) history.add(snap.symbols);

        int window = Math.min(20, history.size());
        if (window < 10) {
            log.warn("[BhavcopyService] Only {} days of history — narrow range z-score skipped", window);
            return;
        }

        int classified = 0;
        for (CprLevels today : todayCache.values()) {
            String sym = today.getSymbol();
            List<Double> ranges = new ArrayList<>();
            for (int i = 0; i < window; i++) {
                CprLevels h = history.get(i).get(sym);
                if (h != null) {
                    double r = h.getHigh() - h.getLow();
                    if (r > 0) ranges.add(r);
                }
            }
            if (ranges.size() < 10) continue;

            double sum = 0;
            for (double r : ranges) sum += r;
            double mean = sum / ranges.size();
            double sq = 0;
            for (double r : ranges) { double d = r - mean; sq += d * d; }
            double std = Math.sqrt(sq / ranges.size());
            if (std <= 1e-9) continue;

            double x = today.getHigh() - today.getLow();
            double z = (x - mean) / std;
            today.setRangeZScore(Math.round(z * 100.0) / 100.0);
            today.setNarrowRangeType(z < -1.5 ? "SMALL" : "LARGE");
            classified++;
        }
        log.info("[BhavcopyService] Classified range z-score for {} stocks ({}-day history)", classified, window);
    }

    // ── Backfill daily history from NSE archives ──────────────────────────────

    private void backfillHistory(LocalDate currentDate, String cookies, Set<String> nfoSymbols) {
        // Collect dates already in history to avoid re-fetching (only if they have actual data)
        Set<String> existingDates = new HashSet<>();
        existingDates.add(cachedDate);
        // Remove empty snapshots (from previous bad saves) and track valid ones
        dailyHistory.removeIf(snap -> snap.symbols.isEmpty());
        for (DaySnapshot snap : dailyHistory) existingDates.add(snap.date);

        int needed = MAX_HISTORY_DAYS - dailyHistory.size();
        LocalDate date = skipWeekends(currentDate.minusDays(1));
        int fetched = 0;
        int failures = 0;

        log.info("[BhavcopyService] Backfilling history: have {} days, need {} more", dailyHistory.size(), needed);

        while (fetched < needed && failures < 5) {
            if (existingDates.contains(date.toString())) {
                date = skipWeekends(date.minusDays(1));
                continue;
            }
            try {
                String dateStr = DATE_FMT.format(date);
                String cmUrl = String.format(CM_URL_TEMPLATE, dateStr);
                byte[] cmZip = downloadZip(cmUrl, cookies);
                if (cmZip == null) {
                    failures++;
                    date = skipWeekends(date.minusDays(1));
                    continue;
                }

                Map<String, double[]> ohlcMap = parseCmOhlc(cmZip, nfoSymbols);
                DaySnapshot snapshot = new DaySnapshot();
                snapshot.date = date.toString();
                for (Map.Entry<String, double[]> entry : ohlcMap.entrySet()) {
                    double[] hlc = entry.getValue();
                    CprLevels lvl = new CprLevels(entry.getKey(), hlc[0], hlc[1], hlc[2]);
                    if (hlc.length > 3) lvl.setVolume((long) hlc[3]);
                    if (symbolMasterService != null) {
                        double tick = symbolMasterService.getTickSize("NSE:" + entry.getKey() + "-EQ");
                        lvl.roundToTick(tick);
                    }
                    snapshot.symbols.put(entry.getKey(), lvl);
                }
                dailyHistory.addLast(snapshot);
                existingDates.add(date.toString());
                fetched++;
                log.info("[BhavcopyService] Backfilled {} ({} stocks)", date, snapshot.symbols.size());
            } catch (Exception e) {
                log.error("[BhavcopyService] Error backfilling {}: {}", date, e.getMessage());
                failures++;
            }
            date = skipWeekends(date.minusDays(1));
        }
        log.info("[BhavcopyService] Backfill complete: {} days total history", dailyHistory.size());
    }

    // ── NSE session cookies ────────────────────────────────────────────────────

    public String getNseCookies() {
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
            log.error("[BhavcopyService] Failed to get NSE cookies: {}", e.getMessage());
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
                log.error("[BhavcopyService] HTTP {} for {}", status, url);
                return null;
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (InputStream is = conn.getInputStream()) {
                is.transferTo(baos);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            log.error("[BhavcopyService] Download failed for {}: {}", url, e.getMessage());
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
                log.error("[BhavcopyService] FO CSV header not recognized: {}", header);
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
            log.error("[BhavcopyService] Error parsing FO Bhavcopy: {}", e.getMessage());
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

    /**
     * Parse CM bhavcopy ZIP. Returns map of symbol → double[]{high, low, close, volume, 52wHigh, 52wLow}.
     * Volume/52W fields are 0 if not available in the CSV.
     */
    private Map<String, double[]> parseCmOhlc(byte[] zipData, Set<String> nfoSymbols) {
        Map<String, double[]> result = new LinkedHashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
            if (zis.getNextEntry() == null) return result;

            BufferedReader br = new BufferedReader(new InputStreamReader(zis));
            String header = br.readLine();
            if (header == null) return result;

            String[] cols = header.split(",");
            int symIdx = -1, seriesIdx = -1, highIdx = -1, lowIdx = -1, closeIdx = -1;
            int volumeIdx = -1, w52HighIdx = -1, w52LowIdx = -1;

            for (int i = 0; i < cols.length; i++) {
                String col = cols[i].trim().replace("\"", "");
                switch (col) {
                    case "TckrSymb" -> symIdx = i;
                    case "SctySrs"  -> seriesIdx = i;
                    case "HghPric"  -> highIdx = i;
                    case "LwPric"   -> lowIdx = i;
                    case "ClsPric"  -> closeIdx = i;
                    case "TtlTradgVol", "TtlTrdQty", "TradQnty" -> volumeIdx = i;
                    case "Hgh52Wk", "52WkHgh", "HghPric52Wk"  -> w52HighIdx = i;
                    case "Lw52Wk", "52WkLw", "LwPric52Wk"     -> w52LowIdx = i;
                    // Legacy fallbacks
                    case "SYMBOL"   -> { if (symIdx == -1) symIdx = i; }
                    case "SERIES"   -> { if (seriesIdx == -1) seriesIdx = i; }
                    case "HIGH"     -> { if (highIdx == -1) highIdx = i; }
                    case "LOW"      -> { if (lowIdx == -1) lowIdx = i; }
                    case "CLOSE"    -> { if (closeIdx == -1) closeIdx = i; }
                }
            }

            if (symIdx == -1 || highIdx == -1 || lowIdx == -1 || closeIdx == -1) {
                log.error("[BhavcopyService] CM CSV header not recognized: {}", header);
                return result;
            }

            // Log ALL column headers for debugging
            log.info("[BhavcopyService] CM CSV columns: {}", header);

            // Log which extra columns were found
            if (volumeIdx >= 0) log.info("[BhavcopyService] Found volume column at index {}", volumeIdx);
            if (w52HighIdx >= 0) log.info("[BhavcopyService] Found 52W High column at index {}", w52HighIdx);
            if (w52LowIdx >= 0) log.info("[BhavcopyService] Found 52W Low column at index {}", w52LowIdx);

            int maxIdx = Math.max(Math.max(symIdx, seriesIdx), Math.max(Math.max(highIdx, lowIdx), closeIdx));
            if (volumeIdx >= 0) maxIdx = Math.max(maxIdx, volumeIdx);
            if (w52HighIdx >= 0) maxIdx = Math.max(maxIdx, w52HighIdx);
            if (w52LowIdx >= 0) maxIdx = Math.max(maxIdx, w52LowIdx);

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
                    double vol = 0, w52h = 0, w52l = 0;
                    if (volumeIdx >= 0 && volumeIdx < parts.length) {
                        try { vol = Double.parseDouble(parts[volumeIdx].trim().replace("\"", "")); } catch (NumberFormatException ignored) {}
                    }
                    if (w52HighIdx >= 0 && w52HighIdx < parts.length) {
                        try { w52h = Double.parseDouble(parts[w52HighIdx].trim().replace("\"", "")); } catch (NumberFormatException ignored) {}
                    }
                    if (w52LowIdx >= 0 && w52LowIdx < parts.length) {
                        try { w52l = Double.parseDouble(parts[w52LowIdx].trim().replace("\"", "")); } catch (NumberFormatException ignored) {}
                    }
                    if (h > 0 && l > 0 && c > 0) {
                        result.put(sym, new double[]{h, l, c, vol, w52h, w52l});
                    }
                } catch (NumberFormatException ignored) {}
            }
        } catch (Exception e) {
            log.error("[BhavcopyService] Error parsing CM Bhavcopy: {}", e.getMessage());
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
            // Save daily history as array
            List<Map<String, Object>> historyList = new ArrayList<>();
            for (DaySnapshot snap : dailyHistory) {
                Map<String, Object> snapMap = new LinkedHashMap<>();
                snapMap.put("date", snap.date);
                snapMap.put("symbols", snap.symbols);
                historyList.add(snapMap);
            }
            data.put("dailyHistory", historyList);
            Files.writeString(Paths.get(STORE_FILE),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data));
        } catch (Exception e) {
            log.error("[BhavcopyService] Failed to save CPR data: {}", e.getMessage());
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

            // Load daily history
            JsonNode historyNode = root.get("dailyHistory");
            if (historyNode != null && historyNode.isArray()) {
                dailyHistory.clear();
                for (JsonNode snapNode : historyNode) {
                    DaySnapshot snap = new DaySnapshot();
                    snap.date = snapNode.get("date").asText();
                    JsonNode snapSymbols = snapNode.get("symbols");
                    if (snapSymbols != null) {
                        snapSymbols.fields().forEachRemaining(entry -> {
                            try {
                                CprLevels levels = mapper.treeToValue(entry.getValue(), CprLevels.class);
                                snap.symbols.put(entry.getKey(), levels);
                            } catch (Exception ignored) {}
                        });
                    }
                    dailyHistory.addLast(snap);
                }
            } else {
                // Backward compat: migrate from old previousDate/previousSymbols format
                JsonNode prevDateNode = root.get("previousDate");
                JsonNode prevSymbolsNode = root.get("previousSymbols");
                if (prevDateNode != null && prevSymbolsNode != null) {
                    DaySnapshot snap = new DaySnapshot();
                    snap.date = prevDateNode.asText();
                    prevSymbolsNode.fields().forEachRemaining(entry -> {
                        try {
                            CprLevels levels = mapper.treeToValue(entry.getValue(), CprLevels.class);
                            snap.symbols.put(entry.getKey(), levels);
                        } catch (Exception ignored) {}
                    });
                    dailyHistory.addFirst(snap);
                }
            }
            log.info("[BhavcopyService] Loaded from file: date={}, {} stocks, {} days history",
                cachedDate, cache.size(), dailyHistory.size());
        } catch (Exception e) {
            log.error("[BhavcopyService] Failed to load CPR data from file: {}", e.getMessage());
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
