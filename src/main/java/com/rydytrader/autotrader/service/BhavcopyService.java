package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

@Service
public class BhavcopyService {

    private static final Logger log = LoggerFactory.getLogger(BhavcopyService.class);

    private static final String CM_URL_TEMPLATE =
        "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_%s_F_0000.csv.zip";
    private static final String FO_URL_TEMPLATE =
        "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_%s_F_0000.csv.zip";
    // NSE index daily close file (DDMMYYYY format, plain CSV not zipped)
    private static final String IDX_URL_TEMPLATE =
        "https://nsearchives.nseindia.com/content/indices/ind_close_all_%s.csv";
    private static final DateTimeFormatter IDX_DATE_FMT = DateTimeFormatter.ofPattern("ddMMyyyy");

    /**
     * Indices we want CPR levels for. Map: bhavcopy "Index Name" → ticker key used in cache.
     * The ticker key must match what extractTicker() produces from the Fyers symbol so
     * downstream lookups (e.g. getCprLevels(extractTicker("NSE:NIFTY50-INDEX"))) work.
     * extractTicker strips the "NSE:" prefix and the "-INDEX" suffix → "NIFTY50".
     */
    private static final Map<String, String> SUPPORTED_INDICES = Map.of(
        "Nifty 50",       "NIFTY50",
        "Nifty Bank",     "NIFTYBANK",
        "Nifty Fin Service", "FINNIFTY"
    );
    private static final String STORE_FILE = "../store/cache/cpr-data.json";
    private static final String LEGACY_STORE_FILE = "../store/config/cpr-data.json";
    private static final String NIFTY50_LIST_FILE  = "../store/cache/nifty50-list.json";
    private static final String NIFTY100_LIST_FILE = "../store/cache/nifty100-list.json";
    private static final String NSE_BASE_URL = "https://www.nseindia.com/";
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String USER_AGENT =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    // Tolerate unknown properties so cached JSON written by older versions (e.g. with the
    // removed cprDayRelation field) still deserializes — otherwise every record fails and
    // the cache loads empty.
    private static final ObjectMapper mapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final ConcurrentHashMap<String, CprLevels> cache = new ConcurrentHashMap<>();
    private volatile String cachedDate = "";
    // NIFTY 50 / NIFTY 100 membership — both lists fetched from NSE once per day, cached to
    // disk as a fallback for offline/failed-fetch days. NIFTY 50 is a strict subset of NIFTY 100.
    // The bhavcopy parse intersects with the SCAN universe set (chosen via scanUniverse setting)
    // so only the relevant stocks end up in the CPR cache. NIFTY 50 membership is also tracked
    // independently because index breadth + the NIFTY 50 trend card always use the 50 set.
    private volatile Set<String> nifty50Symbols  = Collections.emptySet();
    private volatile Set<String> nifty100Symbols = Collections.emptySet();

    // Rolling 5-day history (most recent first) for weekly CPR and inside-CPR detection
    private final LinkedList<DaySnapshot> dailyHistory = new LinkedList<>();
    private static final int MAX_HISTORY_DAYS = 25;

    static class DaySnapshot {
        String date;
        Map<String, CprLevels> symbols = new LinkedHashMap<>();
    }
    private final EventService eventService;

    // Retry scheduler for post-failure delayed re-fetch (single-thread daemon).
    // Schedule: 2, 5, 15, 30, 60 min after the first failure. Cancelled once a fetch succeeds.
    private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "bhavcopy-retry");
        t.setDaemon(true);
        return t;
    });
    private volatile ScheduledFuture<?> retryFuture;
    private final AtomicInteger retryAttempt = new AtomicInteger(0);
    private static final long[] RETRY_DELAYS_MIN = { 2, 5, 15, 30, 60 };

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private SymbolMasterService symbolMasterService;

    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;

    public BhavcopyService(EventService eventService) {
        this.eventService = eventService;
        new File("../store").mkdirs();
    }

    @PostConstruct
    public void init() {
        loadFromFile();
        String expectedDate = getLastTradingDay().toString();
        if (!expectedDate.equals(cachedDate)) {
            boolean ok = fetchAndCompute();
            if (!ok) {
                // Fail-safe: cached data is stale AND NSE refresh failed. Clear the cache so
                // the scanner shows an empty list rather than yesterday's stocks — stale data
                // is worse than no data for a trading session.
                int prev = cache.size();
                cache.clear();
                cachedDate = "";
                log.error("[BhavcopyService] Bhavcopy refresh failed on new day — cleared {} stale CPR entries, scanner will show empty watchlist until retry succeeds", prev);
                eventService.log("[ERROR] Bhavcopy refresh failed on new day — stale CPR cleared. Auto-retries scheduled (2/5/15/30/60 min).");
                scheduleRetry();
            }
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
            // Only compute beta/cap/avgs if not already in the cached file (avoids slow NSE calls on restart)
            boolean needsEnrichment = cache.values().stream()
                .anyMatch(c -> c.getBeta() == 0 || c.getCapCategory() == null || c.getAvgVolume20() == 0);
            if (needsEnrichment) {
                log.info("[BhavcopyService] Enriching cache: betas + cap categories + vol/turnover averages");
                computeBetas(cache);
                computeVolumeTurnoverAverages(cache);
                try {
                    String cookies = getNseCookies();
                    if (cookies != null && !cookies.isEmpty()) fetchCapCategories(cookies, cache);
                } catch (Exception e) {
                    log.warn("[BhavcopyService] Cap category fetch failed on cache load: {}", e.getMessage());
                }
                saveToFile();
            } else {
                log.info("[BhavcopyService] Beta + cap + averages already cached — skipping enrichment");
            }
            double narrowMaxWidth = riskSettings != null ? riskSettings.getNarrowCprMaxWidth() : 0.1;
            double narrowMinWidth = riskSettings != null ? riskSettings.getNarrowCprMinWidth() : 0.0;
            long narrowCount = cache.values().stream()
                .filter(c -> !isIndex(c.getSymbol()) && c.getCprWidthPct() >= narrowMinWidth && c.getCprWidthPct() < narrowMaxWidth)
                .count();
            long insideCount = getInsideCprStocks().size();
            log.info("[BhavcopyService] Loaded {} NFO stocks from cache for {} ({} narrow @{}%, {} inside CPR, {} history days)", cache.size(), cachedDate, narrowCount, narrowMaxWidth, insideCount, dailyHistory.size());
            eventService.log("[BhavcopyService] Loaded CPR for " + cache.size() + " NFO stocks for " + cachedDate
                + " (" + narrowCount + " narrow @" + narrowMaxWidth + "%, " + insideCount + " inside CPR)");
        }
    }

    @Scheduled(cron = "0 0 8 * * MON-FRI")
    public void scheduledFetch() {
        String expectedDate = getLastTradingDay().toString();
        boolean wasStale = !expectedDate.equals(cachedDate);
        boolean ok = fetchAndCompute();
        if (!ok && wasStale) {
            int prev = cache.size();
            cache.clear();
            cachedDate = "";
            log.error("[BhavcopyService] 8 AM cron fetch failed with stale cache — cleared {} entries, scanner will show empty watchlist", prev);
            eventService.log("[ERROR] 8 AM CPR fetch failed — stale cache cleared. Manual rebuild required.");
            scheduleRetry();
        } else if (ok) {
            cancelRetry();
        }
    }

    /**
     * Schedule a delayed retry after a failed fetch. Fires at 2, 5, 15, 30, 60 min since
     * the first failure. Cancelled as soon as a fetch succeeds (either via retry or
     * the 8 AM cron). Only runs when cache is empty — if it was populated another way,
     * no retry fires.
     */
    private synchronized void scheduleRetry() {
        int n = retryAttempt.get();
        if (n >= RETRY_DELAYS_MIN.length) {
            log.warn("[BhavcopyService] Retry cap reached ({}), giving up until next restart or 8 AM cron", n);
            return;
        }
        long delayMin = RETRY_DELAYS_MIN[n];
        retryFuture = retryScheduler.schedule(() -> {
            try {
                // Skip if someone else (e.g. manual rebuild) populated the cache in the meantime.
                if (!cache.isEmpty()) {
                    log.info("[BhavcopyService] Retry skipped — cache was populated by another path");
                    cancelRetry();
                    return;
                }
                log.info("[BhavcopyService] Retry attempt {}/{} after {} min", n + 1, RETRY_DELAYS_MIN.length, delayMin);
                boolean ok = fetchAndCompute();
                if (ok) {
                    eventService.log("[SUCCESS] Bhavcopy recovered on retry " + (n + 1));
                    cancelRetry();
                } else {
                    retryAttempt.incrementAndGet();
                    scheduleRetry();
                }
            } catch (Exception e) {
                log.error("[BhavcopyService] Retry failed: {}", e.getMessage());
                retryAttempt.incrementAndGet();
                scheduleRetry();
            }
        }, delayMin, TimeUnit.MINUTES);
        log.info("[BhavcopyService] Bhavcopy retry scheduled in {} min (attempt {}/{})", delayMin, n + 1, RETRY_DELAYS_MIN.length);
    }

    private synchronized void cancelRetry() {
        retryAttempt.set(0);
        if (retryFuture != null) {
            retryFuture.cancel(false);
            retryFuture = null;
        }
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    public CprLevels getCprLevels(String symbol) {
        return cache.get(extractTicker(symbol));
    }

    public Map<String, CprLevels> getAllCprLevels() {
        return Collections.unmodifiableMap(cache);
    }

    /** True for index tickers merged into the cache alongside stocks (NIFTY50 etc.). */
    public boolean isIndex(String ticker) {
        return SUPPORTED_INDICES.containsValue(ticker);
    }

    /** True if the symbol is part of the NIFTY 50 universe — flag set during cap-category fetch. */
    public boolean isInNifty50(String ticker) {
        CprLevels cpr = cache.get(ticker);
        return cpr != null && cpr.isInNifty50();
    }

    /** True if the symbol is part of the NIFTY 100 universe (NIFTY 50 ⊂ NIFTY 100). */
    public boolean isInNifty100(String ticker) {
        CprLevels cpr = cache.get(ticker);
        return cpr != null && cpr.isInNifty100();
    }

    /** True if the symbol is in the configured scan universe (NIFTY 50 or NIFTY 100). */
    public boolean isInScanUniverse(String ticker) {
        if (riskSettings != null && "NIFTY100".equals(riskSettings.getScanUniverse())) {
            return isInNifty100(ticker);
        }
        return isInNifty50(ticker);
    }

    /** Total NIFTY 50 stocks marked in the current cache. Returns 0 if the list fetch failed. */
    public int getNifty50Count() {
        return (int) cache.values().stream().filter(CprLevels::isInNifty50).count();
    }

    /** Total stocks in the configured scan universe (NIFTY 50 or NIFTY 100). */
    public int getScanUniverseCount() {
        if (riskSettings != null && "NIFTY100".equals(riskSettings.getScanUniverse())) {
            return (int) cache.values().stream().filter(CprLevels::isInNifty100).count();
        }
        return getNifty50Count();
    }

    public List<CprLevels> getNarrowCprStocks() {
        double narrowMaxWidth = riskSettings != null ? riskSettings.getNarrowCprMaxWidth() : 0.1;
        double narrowMinWidth = riskSettings != null ? riskSettings.getNarrowCprMinWidth() : 0.0;
        return cache.values().stream()
            .filter(c -> !isIndex(c.getSymbol()))
            .filter(c -> c.getCprWidthPct() >= narrowMinWidth && c.getCprWidthPct() < narrowMaxWidth)
            .sorted(Comparator.comparingDouble(CprLevels::getCprWidthPct))
            .collect(Collectors.toList());
    }

    public List<CprLevels> getInsideCprStocks() {
        Map<String, CprLevels> prevDay = getPreviousDaySymbols();
        if (prevDay.isEmpty()) return Collections.emptyList();
        return cache.values().stream()
                        .filter(c -> !isIndex(c.getSymbol()))
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

    public String getCachedDate()   { return cachedDate; }
    public String getPreviousDate() { return dailyHistory.isEmpty() ? "" : dailyHistory.getFirst().date; }
    public int getLoadedCount()     { return cache.size(); }

    /** Watchlist-scoped count: how many of the given symbols have CPR levels loaded. */
    public int getLoadedCountFor(java.util.Collection<String> symbols) {
        int n = 0;
        for (String s : symbols) if (cache.containsKey(extractTicker(s))) n++;
        return n;
    }

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
        return prev.get(extractTicker(symbol));
    }

    /**
     * Average Daily Range (ADR) = average of (high - low) over the last N trading days.
     * Uses snapshots in dailyHistory (excludes today's in-progress day).
     * Returns 0 if fewer than minDays of data available.
     */
    public double getAverageDailyRange(String symbol, int days) {
        String ticker = extractTicker(symbol);
        int minDays = Math.max(5, days / 4); // need at least 5 days to be meaningful
        double sum = 0;
        int count = 0;
        for (DaySnapshot snap : dailyHistory) {
            if (count >= days) break;
            CprLevels lv = snap.symbols.get(ticker);
            if (lv == null) continue;
            double range = lv.getHigh() - lv.getLow();
            if (range <= 0) continue;
            sum += range;
            count++;
        }
        if (count < minDays) return 0;
        return sum / count;
    }

    // ── Core fetch logic ───────────────────────────────────────────────────────

    /** @return true if cache was populated with fresh data; false if every attempt failed. */
    private boolean fetchAndCompute() {
        LocalDate targetDate = getLastTradingDay();

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                String dateStr = DATE_FMT.format(targetDate);
                String cookies = getNseCookies();
                if (cookies == null || cookies.isEmpty()) {
                    log.error("[BhavcopyService] Failed to obtain NSE session cookies");
                    return false;
                }

                // Universe selection. NIFTY 50 ⊂ NIFTY 100 ⊂ FNO. We always fetch BOTH index
                // lists so the bhavcopy parse can intersect with whichever the user has
                // configured (scanUniverse). NIFTY 50 / NIFTY 100 guarantees FNO membership,
                // so the FO bhavcopy fetch is skipped when either list is available.
                Set<String> nifty50  = fetchOrLoadNifty50List(cookies);
                Set<String> nifty100 = fetchOrLoadNifty100List(cookies);
                String universe = riskSettings != null ? riskSettings.getScanUniverse() : "NIFTY50";
                Set<String> nfoSymbols;
                Set<String> chosen = "NIFTY100".equals(universe) ? nifty100 : nifty50;
                if (!chosen.isEmpty()) {
                    nfoSymbols = new HashSet<>(chosen);
                    log.info("[BhavcopyService] {} universe active — skipping FO bhavcopy fetch ({} symbols)",
                        universe, nfoSymbols.size());
                } else if (!nifty50.isEmpty()) {
                    // Requested NIFTY 100 but it failed to load; fall back to NIFTY 50.
                    nfoSymbols = new HashSet<>(nifty50);
                    log.warn("[BhavcopyService] {} list unavailable, falling back to NIFTY 50 ({} symbols)",
                        universe, nfoSymbols.size());
                } else {
                    // Fallback: fetch FO bhavcopy for full FNO universe
                    String foUrl = String.format(FO_URL_TEMPLATE, dateStr);
                    byte[] foZip = downloadZip(foUrl, cookies);
                    if (foZip == null) {
                        log.error("[BhavcopyService] FO Bhavcopy not available for {}, trying previous day", targetDate);
                        targetDate = skipWeekends(targetDate.minusDays(1));
                        continue;
                    }
                    nfoSymbols = extractNfoSymbols(foZip);
                    log.info("[BhavcopyService] FO Bhavcopy fallback — {} unique NFO stock symbols", nfoSymbols.size());
                    if (nfoSymbols.isEmpty()) {
                        log.error("[BhavcopyService] No NFO symbols found in FO Bhavcopy for {}", targetDate);
                        targetDate = skipWeekends(targetDate.minusDays(1));
                        continue;
                    }
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
                    if (hlc.length > 6) lvl.setTurnover(hlc[6]);
                    // Mark membership independently — N50 ⊂ N100, so an N50 stock has both flags set.
                    if (nifty50Symbols.contains(entry.getKey()))  lvl.setInNifty50(true);
                    if (nifty100Symbols.contains(entry.getKey())) lvl.setInNifty100(true);
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

                // Fetch index bhavcopy and merge into the cache so NIFTY etc. live alongside stocks.
                // Failure here is non-fatal — index alignment filter will fall back to NEUTRAL if missing.
                Map<String, CprLevels> indexLevels = fetchIndexLevels(targetDate, cookies);
                if (!indexLevels.isEmpty()) {
                    newCache.putAll(indexLevels);
                    log.info("[BhavcopyService] Loaded {} index CPR levels from index bhavcopy", indexLevels.size());
                }

                cache.clear();
                cache.putAll(newCache);
                cachedDate = targetDate.toString();

                // Backfill history first (needed for z-score and inside CPR)
                if (dailyHistory.size() < MAX_HISTORY_DAYS) {
                    backfillHistory(targetDate, cookies, nfoSymbols);
                }

                computeBetas(cache);
                computeVolumeTurnoverAverages(cache);
                fetchCapCategories(cookies, cache);

                saveToFile();

                double narrowMaxWidth = riskSettings != null ? riskSettings.getNarrowCprMaxWidth() : 0.1;
                double narrowMinWidth = riskSettings != null ? riskSettings.getNarrowCprMinWidth() : 0.0;
                long narrowCount = cache.values().stream()
                    .filter(c -> !isIndex(c.getSymbol()) && c.getCprWidthPct() >= narrowMinWidth && c.getCprWidthPct() < narrowMaxWidth)
                    .count();
                long insideCount = getInsideCprStocks().size();
                String msg = "[BhavcopyService] Loaded CPR for " + cache.size()
                    + " NFO stocks for " + cachedDate + " (" + narrowCount + " narrow @" + narrowMaxWidth
                    + "%, " + insideCount + " inside CPR)";
                log.info(msg);
                eventService.log(msg);
                return true;

            } catch (Exception e) {
                log.error("[BhavcopyService] Error fetching Bhavcopy for {}: {}", targetDate, e.getMessage());
                targetDate = skipWeekends(targetDate.minusDays(1));
            }
        }

        log.error("[BhavcopyService] Failed to fetch Bhavcopy after 3 attempts");
        return false;
    }

    // (Range classification removed — SMALL/LARGE/rangeAdrPct no longer used after the
    // watchlist filter was simplified to a single NIFTY 50 / ALL universe toggle.)

    // ── Volume/Turnover 20-day averages from daily history ───────────────────

    private void computeVolumeTurnoverAverages(Map<String, CprLevels> todayCache) {
        if (dailyHistory.size() < 5) {
            log.info("[BhavcopyService] Insufficient history for vol/turnover avg ({} days)", dailyHistory.size());
            return;
        }
        int window = Math.min(20, dailyHistory.size());
        int computed = 0;
        for (CprLevels today : todayCache.values()) {
            String sym = today.getSymbol();
            long volSum = 0;
            double tovSum = 0;
            int volDays = 0, tovDays = 0;
            for (int i = 0; i < window && i < dailyHistory.size(); i++) {
                CprLevels h = dailyHistory.get(i).symbols.get(sym);
                if (h == null) continue;
                if (h.getVolume() > 0) { volSum += h.getVolume(); volDays++; }
                if (h.getTurnover() > 0) { tovSum += h.getTurnover(); tovDays++; }
            }
            if (volDays >= 5) {
                long avgVol = volSum / volDays;
                today.setAvgVolume20(avgVol);
                if (avgVol > 0 && today.getVolume() > 0) {
                    today.setVolumeMultiple(Math.round((double) today.getVolume() / avgVol * 100.0) / 100.0);
                }
            }
            if (tovDays >= 5) {
                double avgTov = tovSum / tovDays;
                today.setAvgTurnover20(avgTov);
                if (avgTov > 0 && today.getTurnover() > 0) {
                    today.setTurnoverMultiple(Math.round(today.getTurnover() / avgTov * 100.0) / 100.0);
                }
            }
            if (volDays >= 5 || tovDays >= 5) computed++;
        }
        log.info("[BhavcopyService] Computed vol/turnover averages for {} stocks ({}-day window)", computed, window);
    }

    // ── Beta computation from 25-day daily history ────────────────────────────

    private void computeBetas(Map<String, CprLevels> todayCache) {
        List<Map<String, CprLevels>> history = new ArrayList<>();
        if (!cache.isEmpty()) history.add(cache);
        for (DaySnapshot snap : dailyHistory) history.add(snap.symbols);

        if (history.size() < 10) {
            log.info("[BhavcopyService] Insufficient history for beta ({} days, need 10+)", history.size());
            return;
        }

        // Gather NIFTY daily closes
        double[] niftyCloses = new double[history.size()];
        for (int i = 0; i < history.size(); i++) {
            CprLevels n = history.get(i).get("NIFTY50");
            niftyCloses[i] = (n != null) ? n.getClose() : 0;
        }

        long niftyDays = java.util.Arrays.stream(niftyCloses).filter(v -> v > 0).count();
        log.info("[BhavcopyService] Beta: NIFTY50 present in {}/{} history days", niftyDays, history.size());

        int computed = 0, fallback = 0;
        for (CprLevels today : todayCache.values()) {
            String sym = today.getSymbol();
            List<Double> stockRet = new ArrayList<>();
            List<Double> niftyRet = new ArrayList<>();
            for (int i = 0; i < history.size() - 1; i++) {
                CprLevels s = history.get(i).get(sym);
                CprLevels sNext = history.get(i + 1).get(sym);
                if (s == null || sNext == null || sNext.getClose() <= 0) continue;
                if (niftyCloses[i] <= 0 || niftyCloses[i + 1] <= 0) continue;
                stockRet.add((s.getClose() - sNext.getClose()) / sNext.getClose());
                niftyRet.add((niftyCloses[i] - niftyCloses[i + 1]) / niftyCloses[i + 1]);
            }
            if (stockRet.size() < 10) {
                log.info("[BhavcopyService] Beta fallback for {} — {} return pairs (need 10)", sym, stockRet.size());
                today.setBeta(1.0); fallback++; continue;
            }

            double sMean = stockRet.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double nMean = niftyRet.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double cov = 0, nVar = 0;
            for (int i = 0; i < stockRet.size(); i++) {
                double sd = stockRet.get(i) - sMean;
                double nd = niftyRet.get(i) - nMean;
                cov += sd * nd;
                nVar += nd * nd;
            }
            today.setBeta(nVar > 0 ? Math.round(cov / nVar * 100.0) / 100.0 : 1.0);
            computed++;
        }
        log.info("[BhavcopyService] Computed beta for {} stocks ({} fallback to 1.0)", computed, fallback);
    }

    // ── Market cap classification from NSE index CSVs ───────────────────────

    private static final String NIFTY50_URL  = "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv";
    private static final String NIFTY100_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv";
    private static final String MIDCAP150_URL = "https://nsearchives.nseindia.com/content/indices/ind_niftymidcap150list.csv";

    /**
     * Fetch the NIFTY 50 member list from NSE and update {@link #nifty50Symbols}. On fetch
     * failure, fall back to the disk-cached copy (last-known-good). Returns the set in use.
     * Called once per bhavcopy refresh — keeps the list ~1 day fresh, which is fine since
     * NIFTY 50 rebalances ~2× per year.
     */
    private Set<String> fetchOrLoadNifty50List(String cookies) {
        Set<String> fresh = fetchIndexSymbols(NIFTY50_URL, cookies);
        if (!fresh.isEmpty()) {
            nifty50Symbols = fresh;
            saveNifty50ToDisk(fresh);
            log.info("[BhavcopyService] NIFTY 50 list fetched from NSE: {} stocks", fresh.size());
            return fresh;
        }
        // Live fetch failed → try disk fallback
        Set<String> cached = loadNifty50FromDisk();
        if (!cached.isEmpty()) {
            nifty50Symbols = cached;
            log.warn("[BhavcopyService] NIFTY 50 live fetch failed — using disk-cached list of {} stocks", cached.size());
            return cached;
        }
        log.warn("[BhavcopyService] NIFTY 50 list unavailable (live fetch + disk cache both empty) — bhavcopy will be processed with full FNO universe");
        return Collections.emptySet();
    }

    /** Same lifecycle as {@link #fetchOrLoadNifty50List(String)} but for the NIFTY 100. Always
     *  fetched (regardless of scanUniverse setting) so the user can switch on the fly without
     *  triggering a fresh NSE fetch. NIFTY 100 rebalances ~2× per year so 1-day staleness
     *  is acceptable. */
    private Set<String> fetchOrLoadNifty100List(String cookies) {
        Set<String> fresh = fetchIndexSymbols(NIFTY100_URL, cookies);
        if (!fresh.isEmpty()) {
            nifty100Symbols = fresh;
            saveSymbolsToDisk(fresh, NIFTY100_LIST_FILE, "NIFTY 100");
            log.info("[BhavcopyService] NIFTY 100 list fetched from NSE: {} stocks", fresh.size());
            return fresh;
        }
        Set<String> cached = loadSymbolsFromDisk(NIFTY100_LIST_FILE);
        if (!cached.isEmpty()) {
            nifty100Symbols = cached;
            log.warn("[BhavcopyService] NIFTY 100 live fetch failed — using disk-cached list of {} stocks", cached.size());
            return cached;
        }
        log.warn("[BhavcopyService] NIFTY 100 list unavailable (live fetch + disk cache both empty)");
        return Collections.emptySet();
    }

    private void saveSymbolsToDisk(Set<String> symbols, String file, String label) {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(file);
            java.nio.file.Files.createDirectories(path.getParent());
            java.nio.file.Files.writeString(path, mapper.writeValueAsString(symbols));
        } catch (Exception e) {
            log.warn("[BhavcopyService] Failed to persist {} list to disk: {}", label, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> loadSymbolsFromDisk(String file) {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(file);
            if (!java.nio.file.Files.exists(path)) return Collections.emptySet();
            String json = java.nio.file.Files.readString(path);
            return mapper.readValue(json, Set.class);
        } catch (Exception e) {
            return Collections.emptySet();
        }
    }

    private void saveNifty50ToDisk(Set<String> symbols) {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(NIFTY50_LIST_FILE);
            java.nio.file.Files.createDirectories(path.getParent());
            java.nio.file.Files.writeString(path, mapper.writeValueAsString(symbols));
        } catch (Exception e) {
            log.warn("[BhavcopyService] Failed to persist NIFTY 50 list to disk: {}", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> loadNifty50FromDisk() {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(NIFTY50_LIST_FILE);
            if (!java.nio.file.Files.exists(path)) return Collections.emptySet();
            String json = java.nio.file.Files.readString(path);
            return mapper.readValue(json, Set.class);
        } catch (Exception e) {
            log.warn("[BhavcopyService] Failed to load NIFTY 50 list from disk: {}", e.getMessage());
            return Collections.emptySet();
        }
    }

    private void fetchCapCategories(String cookies, Map<String, CprLevels> todayCache) {
        // When NIFTY 50 filter is active, every cached entry is a NIFTY 50 constituent by
        // construction — all are LARGE caps. Skip the NIFTY 100 / MIDCAP 150 fetches and
        // just mark everyone LARGE.
        if (!nifty50Symbols.isEmpty()) {
            for (CprLevels cpr : todayCache.values()) {
                if (!isIndex(cpr.getSymbol())) cpr.setCapCategory("LARGE");
            }
            log.info("[BhavcopyService] Cap categories: all {} entries marked LARGE (NIFTY 50 universe)",
                todayCache.size());
            return;
        }
        // Fallback path — NIFTY 50 list unavailable, universe is full FNO. Classify via
        // NIFTY 100 (large) and MIDCAP 150 (mid); everything else is small.
        try {
            Set<String> largeCaps = fetchIndexSymbols(NIFTY100_URL, cookies);
            Set<String> midCaps = fetchIndexSymbols(MIDCAP150_URL, cookies);
            int lc = 0, mc = 0, sc = 0;
            for (CprLevels cpr : todayCache.values()) {
                String sym = cpr.getSymbol();
                if (largeCaps.contains(sym)) { cpr.setCapCategory("LARGE"); lc++; }
                else if (midCaps.contains(sym)) { cpr.setCapCategory("MID"); mc++; }
                else { cpr.setCapCategory("SMALL"); sc++; }
            }
            log.info("[BhavcopyService] Cap categories (fallback mode): {} large, {} mid, {} small", lc, mc, sc);
        } catch (Exception e) {
            log.warn("[BhavcopyService] Failed to fetch cap categories: {} — defaulting all to SMALL", e.getMessage());
            todayCache.values().forEach(c -> { if (c.getCapCategory() == null) c.setCapCategory("SMALL"); });
        }
    }

    private Set<String> fetchIndexSymbols(String url, String cookies) {
        Set<String> symbols = new HashSet<>();
        try {
            byte[] data = downloadPlain(url, cookies);
            if (data == null || data.length == 0) return symbols;
            BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)));
            String header = br.readLine();
            if (header == null) return symbols;
            // Find "Symbol" column
            String[] cols = header.split(",");
            int symIdx = -1;
            for (int i = 0; i < cols.length; i++) {
                if (cols[i].trim().replace("\"", "").equalsIgnoreCase("Symbol")) { symIdx = i; break; }
            }
            if (symIdx < 0) return symbols;
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length > symIdx) {
                    symbols.add(parts[symIdx].trim().replace("\"", ""));
                }
            }
        } catch (Exception e) {
            log.warn("[BhavcopyService] Error fetching index CSV from {}: {}", url, e.getMessage());
        }
        return symbols;
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
                    if (hlc.length > 6) lvl.setTurnover(hlc[6]);
                    if (symbolMasterService != null) {
                        double tick = symbolMasterService.getTickSize("NSE:" + entry.getKey() + "-EQ");
                        lvl.roundToTick(tick);
                    }
                    snapshot.symbols.put(entry.getKey(), lvl);
                }
                // Also fetch index bhavcopy for this day so NIFTY/BANKNIFTY/FINNIFTY have
                // historical daily CPR, enabling weekly CPR aggregation for indices.
                try {
                    Map<String, CprLevels> idxLevels = fetchIndexLevels(date, cookies);
                    if (!idxLevels.isEmpty()) {
                        snapshot.symbols.putAll(idxLevels);
                        log.info("[BhavcopyService] Backfilled indices for {}: {} indices",
                            date, idxLevels.size());
                    }
                } catch (Exception e) {
                    log.warn("[BhavcopyService] Index backfill failed for {}: {}", date, e.getMessage());
                    // non-fatal — stock data is still valid, just missing indices for this day
                }
                dailyHistory.addLast(snapshot);
                existingDates.add(date.toString());
                fetched++;
                log.info("[BhavcopyService] Backfilled {} ({} symbols)", date, snapshot.symbols.size());
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
        // NSE uses Akamai bot manager. Plain GET on the homepage sometimes returns 403.
        // Two-step warmup (homepage → /option-chain) collects bm_sz + bm_sv cookies which
        // the data archive endpoints require. 3 outer attempts with 2s/5s backoff.
        long[] backoffMs = { 0L, 2_000L, 5_000L };
        String lastError = null;
        for (int attempt = 0; attempt < backoffMs.length; attempt++) {
            if (backoffMs[attempt] > 0) {
                try { Thread.sleep(backoffMs[attempt]); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            try {
                // Step 1 — homepage warmup
                Map<String, String> jar = new LinkedHashMap<>();
                String step1 = hitAndCollectCookies(NSE_BASE_URL, null, jar);
                if (step1.startsWith("ERR")) {
                    lastError = "homepage: " + step1;
                    log.warn("[BhavcopyService] NSE cookie attempt {}/{}: {}", attempt + 1, backoffMs.length, lastError);
                    continue;
                }
                // Step 2 — secondary page that triggers bm_sv cookie (needs Referer = homepage)
                String step2 = hitAndCollectCookies(NSE_BASE_URL + "option-chain", NSE_BASE_URL, jar);
                if (step2.startsWith("ERR") && jar.isEmpty()) {
                    lastError = "option-chain: " + step2;
                    log.warn("[BhavcopyService] NSE cookie attempt {}/{}: {}", attempt + 1, backoffMs.length, lastError);
                    continue;
                }
                if (jar.isEmpty()) {
                    lastError = "HTTP " + step1 + "/" + step2 + " with no Set-Cookie headers";
                    log.warn("[BhavcopyService] NSE cookie attempt {}/{}: {}", attempt + 1, backoffMs.length, lastError);
                    continue;
                }
                StringBuilder cookies = new StringBuilder();
                for (Map.Entry<String, String> e : jar.entrySet()) {
                    if (cookies.length() > 0) cookies.append("; ");
                    cookies.append(e.getKey()).append("=").append(e.getValue());
                }
                if (attempt > 0) log.info("[BhavcopyService] NSE cookies obtained on retry {} ({} cookies)", attempt + 1, jar.size());
                else log.debug("[BhavcopyService] NSE cookies obtained ({} cookies)", jar.size());
                return cookies.toString();
            } catch (Exception e) {
                lastError = e.getMessage();
                log.warn("[BhavcopyService] NSE cookie attempt {}/{}: {}", attempt + 1, backoffMs.length, lastError);
            }
        }
        log.error("[BhavcopyService] Failed to get NSE cookies after {} attempts: {}", backoffMs.length, lastError);
        return null;
    }

    /**
     * Fire a GET with full browser-like headers, parse Set-Cookie into the shared jar,
     * return the HTTP status as a string (e.g. "200") or "ERR:&lt;msg&gt;" on exception.
     * Body is not consumed — just status + cookies.
     */
    private String hitAndCollectCookies(String url, String referer, Map<String, String> jar) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", USER_AGENT);
            conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8");
            conn.setRequestProperty("Accept-Language", "en-US,en;q=0.9");
            conn.setRequestProperty("Accept-Encoding", "gzip, deflate, br");
            conn.setRequestProperty("Connection", "keep-alive");
            conn.setRequestProperty("Upgrade-Insecure-Requests", "1");
            conn.setRequestProperty("Sec-Fetch-Dest", "document");
            conn.setRequestProperty("Sec-Fetch-Mode", "navigate");
            conn.setRequestProperty("Sec-Fetch-Site", referer == null ? "none" : "same-origin");
            conn.setRequestProperty("Sec-Fetch-User", "?1");
            conn.setRequestProperty("Cache-Control", "no-cache");
            conn.setRequestProperty("Pragma", "no-cache");
            if (referer != null) conn.setRequestProperty("Referer", referer);
            if (!jar.isEmpty()) {
                StringBuilder cookieHeader = new StringBuilder();
                for (Map.Entry<String, String> e : jar.entrySet()) {
                    if (cookieHeader.length() > 0) cookieHeader.append("; ");
                    cookieHeader.append(e.getKey()).append("=").append(e.getValue());
                }
                conn.setRequestProperty("Cookie", cookieHeader.toString());
            }
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(10_000);
            conn.setInstanceFollowRedirects(true);
            int code = conn.getResponseCode();
            List<String> setCookies = conn.getHeaderFields().getOrDefault("Set-Cookie", List.of());
            for (String c : setCookies) {
                String kv = c.split(";", 2)[0];
                int eq = kv.indexOf('=');
                if (eq > 0) jar.put(kv.substring(0, eq).trim(), kv.substring(eq + 1).trim());
            }
            return String.valueOf(code);
        } catch (Exception e) {
            return "ERR:" + e.getMessage();
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

    // ── Index Bhavcopy ─────────────────────────────────────────────────────────

    /**
     * Fetch the NSE index daily-close CSV (plain CSV, not zipped) and extract OHLC for the
     * indices we care about (NIFTY 50, NIFTY BANK, FIN NIFTY). Returns a map keyed by ticker
     * (matching extractTicker() output for the corresponding Fyers symbol) so the caller can
     * merge them straight into the equity cache.
     */
    private Map<String, CprLevels> fetchIndexLevels(LocalDate targetDate, String cookies) {
        Map<String, CprLevels> result = new LinkedHashMap<>();
        String dateStr = IDX_DATE_FMT.format(targetDate);
        String url = String.format(IDX_URL_TEMPLATE, dateStr);

        try {
            byte[] csvBytes = downloadPlain(url, cookies);
            if (csvBytes == null || csvBytes.length == 0) {
                log.warn("[BhavcopyService] Index bhavcopy not available for {} (URL {})", targetDate, url);
                return result;
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(csvBytes)));
            String header = br.readLine();
            if (header == null) return result;

            // Header columns vary slightly across NSE versions — match defensively.
            String[] cols = header.split(",");
            int nameIdx = -1, openIdx = -1, highIdx = -1, lowIdx = -1, closeIdx = -1;
            for (int i = 0; i < cols.length; i++) {
                String c = cols[i].trim().replace("\"", "").toLowerCase();
                if (nameIdx == -1 && c.contains("index name")) nameIdx = i;
                if (openIdx == -1 && c.contains("open")) openIdx = i;
                if (highIdx == -1 && c.contains("high")) highIdx = i;
                if (lowIdx == -1 && c.contains("low")) lowIdx = i;
                if (closeIdx == -1 && (c.contains("closing") || c.equals("close"))) closeIdx = i;
            }
            if (nameIdx == -1 || highIdx == -1 || lowIdx == -1 || closeIdx == -1) {
                log.error("[BhavcopyService] Index bhavcopy header not recognized: {}", header);
                return result;
            }

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length <= Math.max(highIdx, Math.max(lowIdx, Math.max(closeIdx, nameIdx)))) continue;
                String name = parts[nameIdx].trim().replace("\"", "");
                String ticker = SUPPORTED_INDICES.get(name);
                if (ticker == null) continue;
                try {
                    double h = Double.parseDouble(parts[highIdx].trim().replace("\"", ""));
                    double l = Double.parseDouble(parts[lowIdx].trim().replace("\"", ""));
                    double c = Double.parseDouble(parts[closeIdx].trim().replace("\"", ""));
                    if (h <= 0 || l <= 0 || c <= 0) continue;
                    CprLevels lvl = new CprLevels(ticker, h, l, c);
                    result.put(ticker, lvl);
                    log.info("[BhavcopyService] Index {} → {} (H={} L={} C={})", name, ticker, h, l, c);
                } catch (NumberFormatException e) {
                    log.warn("[BhavcopyService] Skipping malformed index row: {}", line);
                }
            }
        } catch (Exception e) {
            log.error("[BhavcopyService] Failed to fetch index bhavcopy from {}: {}", url, e.getMessage());
        }
        return result;
    }

    /** Download a plain (non-zipped) CSV from NSE with the same browser-like headers as the zip download. */
    private byte[] downloadPlain(String urlStr, String cookies) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", USER_AGENT);
            conn.setRequestProperty("Accept", "text/csv,application/csv,*/*");
            conn.setRequestProperty("Referer", NSE_BASE_URL);
            if (cookies != null && !cookies.isEmpty()) conn.setRequestProperty("Cookie", cookies);
            conn.setConnectTimeout(15_000);
            conn.setReadTimeout(20_000);
            conn.setInstanceFollowRedirects(true);

            int code = conn.getResponseCode();
            if (code != 200) {
                log.warn("[BhavcopyService] downloadPlain HTTP {} for {}", code, urlStr);
                return null;
            }
            try (InputStream in = conn.getInputStream();
                 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) baos.write(buf, 0, n);
                return baos.toByteArray();
            }
        } catch (Exception e) {
            log.error("[BhavcopyService] downloadPlain error: {}", e.getMessage());
            return null;
        }
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
            int volumeIdx = -1, w52HighIdx = -1, w52LowIdx = -1, turnoverIdx = -1;

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
                    case "TtlTrdVal", "TtlTradVal", "TtlTrfVal", "TrdVal", "TOTTRDVAL" -> turnoverIdx = i;
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
            if (turnoverIdx >= 0) log.info("[BhavcopyService] Found turnover column at index {}", turnoverIdx);

            int maxIdx = Math.max(Math.max(symIdx, seriesIdx), Math.max(Math.max(highIdx, lowIdx), closeIdx));
            if (volumeIdx >= 0) maxIdx = Math.max(maxIdx, volumeIdx);
            if (w52HighIdx >= 0) maxIdx = Math.max(maxIdx, w52HighIdx);
            if (w52LowIdx >= 0) maxIdx = Math.max(maxIdx, w52LowIdx);
            if (turnoverIdx >= 0) maxIdx = Math.max(maxIdx, turnoverIdx);

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
                    double tov = 0;
                    if (turnoverIdx >= 0 && turnoverIdx < parts.length) {
                        try { tov = Double.parseDouble(parts[turnoverIdx].trim().replace("\"", "")); } catch (NumberFormatException ignored) {}
                    }
                    if (h > 0 && l > 0 && c > 0) {
                        result.put(sym, new double[]{h, l, c, vol, w52h, w52l, tov});
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
            Path out = Paths.get(STORE_FILE);
            Files.createDirectories(out.getParent());
            Files.writeString(out, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data));
        } catch (Exception e) {
            log.error("[BhavcopyService] Failed to save CPR data: {}", e.getMessage());
        }
    }

    /** Resolve active cache path, migrating legacy store/config/ file on first boot. */
    private Path resolveStorePath() {
        Path primary = Paths.get(STORE_FILE);
        if (Files.exists(primary)) return primary;
        Path legacy = Paths.get(LEGACY_STORE_FILE);
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
        return primary;
    }

    private void loadFromFile() {
        try {
            Path path = resolveStorePath();
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
        s = s.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        return s;
    }
}
