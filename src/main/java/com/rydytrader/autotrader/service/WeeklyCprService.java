package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rydytrader.autotrader.dto.CprLevels;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Calculates weekly and daily CPR trends in real-time using current LTP.
 *
 * Weekly CPR levels are fetched once from Fyers history API at 9:00 AM.
 * Trends are recalculated on every candle close using LTP vs CPR levels
 * (matching Pine Script logic: close > wTop = Bullish, etc.).
 */
@Service
public class WeeklyCprService {

    private static final Logger log = LoggerFactory.getLogger(WeeklyCprService.class);
    private static final String STORE_FILE = "../store/config/weekly-cpr.json";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final BhavcopyService bhavcopyService;
    private final CandleAggregator candleAggregator;
    @org.springframework.beans.factory.annotation.Autowired
    private EventService eventService;
    private final ObjectMapper mapper = new ObjectMapper();

    // Weekly CPR levels per symbol (fixed for the week, fetched once)
    private final ConcurrentHashMap<String, WeeklyLevels> weeklyLevels = new ConcurrentHashMap<>();

    // Date on which the cached weekly levels were computed (= most recent Monday or earlier when
    // levels were fetched). Used for staleness check during loadFromFile.
    private volatile String cachedDate = "";

    public WeeklyCprService(BhavcopyService bhavcopyService,
                            CandleAggregator candleAggregator) {
        this.bhavcopyService = bhavcopyService;
        this.candleAggregator = candleAggregator;
    }

    @PostConstruct
    public void init() {
        loadFromFile();
        // If the cache was empty (first install) or stale (new trading week), compute fresh
        // weekly levels right now from BhavcopyService's local daily history. This used to
        // require a Fyers login to trigger, but since we removed the Fyers dependency the
        // computation is fully local and can run immediately at startup.
        if (weeklyLevels.isEmpty()) {
            List<String> symbols = buildStartupSymbolList();
            if (!symbols.isEmpty()) {
                log.info("[WeeklyCpr] Cache empty or stale — computing fresh weekly levels for {} symbols from local bhavcopy", symbols.size());
                fetchWeeklyTrends(symbols);
            } else {
                log.info("[WeeklyCpr] No symbols in bhavcopy cache yet — weekly levels will populate on first login/scanner init");
            }
        }
    }

    /**
     * Build a list of Fyers symbols from BhavcopyService's current cache for the startup
     * pre-compute. Wraps stock tickers as "NSE:{ticker}-EQ" and known indices as
     * "NSE:{ticker}-INDEX". This runs BEFORE any login/watchlist filtering, so it covers
     * the superset of symbols that could ever be traded — login will later narrow it down.
     */
    private List<String> buildStartupSymbolList() {
        List<String> symbols = new ArrayList<>();
        Map<String, CprLevels> all = bhavcopyService.getAllCprLevels();
        if (all == null || all.isEmpty()) return symbols;

        // Known index tickers (match what BhavcopyService.SUPPORTED_INDICES puts in the cache)
        Set<String> indexTickers = Set.of("NIFTY50", "NIFTYBANK", "FINNIFTY");
        for (String ticker : all.keySet()) {
            if (indexTickers.contains(ticker)) {
                symbols.add("NSE:" + ticker + "-INDEX");
            } else {
                symbols.add("NSE:" + ticker + "-EQ");
            }
        }
        return symbols;
    }

    /**
     * Scheduled daily refresh at 8:00 AM Mon-Fri (same slot as BhavcopyService daily fetch).
     * Re-fetches weekly levels for all symbols already in the cache, so that:
     *   - Monday morning picks up the new week's OHLC (weekly trend flips from last week to this week)
     *   - Mon-Fri reruns overwrite with same-week data (idempotent, safe no-op in middle of week)
     *   - Prevents the "bot runs all weekend, stale Monday data" gap
     *
     * Uses the currently-cached symbol set as the refresh target. New symbols added to the
     * watchlist between restarts won't be covered by this schedule — they're picked up by the
     * next scanner init (on login or restart).
     */
    @Scheduled(cron = "0 0 8 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledRefresh() {
        if (weeklyLevels.isEmpty()) {
            log.info("[WeeklyCpr] Scheduled refresh skipped — no cached symbols yet");
            return;
        }
        List<String> symbols = new ArrayList<>(weeklyLevels.keySet());
        log.info("[WeeklyCpr] Scheduled refresh: fetching weekly levels for {} cached symbols", symbols.size());
        fetchWeeklyTrends(symbols);
    }

    /**
     * Compute weekly CPR levels for all watchlist symbols by aggregating BhavcopyService's
     * locally cached daily history. Zero Fyers API calls — uses the 25-day rolling buffer that
     * BhavcopyService already maintains.
     *
     * Algorithm:
     *   1. Gather all dated daily snapshots (today's cache + rolling history)
     *   2. Group by ISO week
     *   3. Pick the "previous completed week" (weekday → second-to-last, weekend → last)
     *   4. For each symbol, aggregate weekly H/L/C from that week's daily snapshots
     *   5. Compute weekly CPR levels from the aggregate
     */
    public void fetchWeeklyTrends(List<String> fyersSymbols) {
        if (fyersSymbols == null || fyersSymbols.isEmpty()) return;

        // Build the full date → symbol-map index from BhavcopyService
        Map<String, Map<String, CprLevels>> dailyByDate = new LinkedHashMap<>();
        String todayDate = bhavcopyService.getCachedDate();
        if (todayDate != null && !todayDate.isEmpty()) {
            dailyByDate.put(todayDate, bhavcopyService.getTodayCache());
        }
        List<Map<String, CprLevels>> historyMaps = bhavcopyService.getDailyHistoryMaps();
        List<String> historyDates = bhavcopyService.getDailyHistoryDates();
        for (int i = 0; i < historyMaps.size() && i < historyDates.size(); i++) {
            dailyByDate.put(historyDates.get(i), historyMaps.get(i));
        }

        if (dailyByDate.size() < 5) {
            log.warn("[WeeklyCpr] Insufficient daily history ({} days) — need at least 5 for a full week. "
                + "Weekly levels will populate after BhavcopyService backfills more history.", dailyByDate.size());
            return;
        }

        // Group dates by ISO week
        Map<Integer, List<String>> datesByWeek = new TreeMap<>();
        for (String dateStr : dailyByDate.keySet()) {
            try {
                LocalDate date = LocalDate.parse(dateStr);
                int weekKey = date.getYear() * 100
                    + date.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
                datesByWeek.computeIfAbsent(weekKey, k -> new ArrayList<>()).add(dateStr);
            } catch (Exception ignored) {}
        }

        if (datesByWeek.size() < 2) {
            log.warn("[WeeklyCpr] Only {} unique weeks in history — need at least 2 to compute previous week",
                datesByWeek.size());
            return;
        }

        // Determine target week. On weekdays the current week is in progress so use the
        // second-to-last. On weekends the current week just ended so use the last.
        LocalDate now = LocalDate.now(IST);
        boolean weekend = now.getDayOfWeek() == DayOfWeek.SATURDAY || now.getDayOfWeek() == DayOfWeek.SUNDAY;
        List<Integer> weekKeys = new ArrayList<>(datesByWeek.keySet());
        int targetWeekIdx = weekend ? weekKeys.size() - 1 : weekKeys.size() - 2;
        if (targetWeekIdx < 0) targetWeekIdx = 0;
        int targetWeek = weekKeys.get(targetWeekIdx);
        List<String> targetDates = datesByWeek.get(targetWeek);

        log.info("[WeeklyCpr] Computing weekly CPR from {} cached days in week key={} (dates={})",
            targetDates.size(), targetWeek, targetDates);

        int success = 0;
        int missing = 0;
        weeklyLevels.clear();  // refresh the whole map so stale entries don't linger
        for (String symbol : fyersSymbols) {
            String ticker = extractTicker(symbol);
            // Aggregate high/low/close across target week's snapshots for this ticker
            double wH = Double.NEGATIVE_INFINITY;
            double wL = Double.POSITIVE_INFINITY;
            double wC = 0;
            String latestDate = null;
            for (String dateStr : targetDates) {
                CprLevels day = dailyByDate.get(dateStr).get(ticker);
                if (day == null) continue;
                if (day.getHigh() > 0) wH = Math.max(wH, day.getHigh());
                if (day.getLow()  > 0) wL = Math.min(wL, day.getLow());
                if (latestDate == null || dateStr.compareTo(latestDate) > 0) {
                    latestDate = dateStr;
                    wC = day.getClose();
                }
            }

            if (latestDate == null || wH <= 0 || wL == Double.POSITIVE_INFINITY || wC <= 0) {
                missing++;
                continue;
            }

            WeeklyLevels wl = new WeeklyLevels();
            wl.pivot = (wH + wL + wC) / 3.0;
            wl.bc = (wH + wL) / 2.0;
            wl.tc = 2.0 * wl.pivot - wl.bc;
            wl.top = Math.max(wl.tc, wl.bc);
            wl.bot = Math.min(wl.tc, wl.bc);
            wl.r1 = 2.0 * wl.pivot - wL;
            wl.s1 = 2.0 * wl.pivot - wH;
            wl.ph = wH;
            wl.pl = wL;
            weeklyLevels.put(symbol, wl);
            success++;
        }
        log.info("[WeeklyCpr] Weekly levels computed locally: {}/{} symbols ({} missing from history)",
            success, fyersSymbols.size(), missing);
        eventService.log("[INFO] Weekly trends computed for " + success + "/" + fyersSymbols.size()
            + " symbols from bhavcopy history (no API calls)");
        if (success > 0) {
            cachedDate = LocalDate.now(IST).toString();
            saveToFile();
        }
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save all weekly CPR levels to disk as JSON. File includes a `cachedDate` marker so we
     * can detect staleness on load (weekly levels are valid Mon-Fri, invalid from next Monday).
     */
    private synchronized void saveToFile() {
        try {
            File parent = new File(STORE_FILE).getParentFile();
            if (parent != null) parent.mkdirs();
            ObjectNode root = mapper.createObjectNode();
            root.put("cachedDate", cachedDate);
            root.putPOJO("symbols", weeklyLevels);
            Files.writeString(Paths.get(STORE_FILE),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
            log.info("[WeeklyCpr] Saved {} weekly levels to {}", weeklyLevels.size(), STORE_FILE);
        } catch (Exception e) {
            log.error("[WeeklyCpr] Failed to save weekly levels: {}", e.getMessage());
        }
    }

    /**
     * Load weekly CPR levels from disk on startup. Skips the load if the cached data is stale
     * (the cache was built in a previous trading week and new levels need to be computed from
     * the latest completed week).
     *
     * Staleness rule: weekly levels remain valid if cachedDate is in the same ISO week as today
     * — we check by computing the Monday of the week for both cachedDate and today and comparing.
     * A cached entry from last Tuesday is stale by this Monday because the "previous week" has shifted.
     */
    private synchronized void loadFromFile() {
        Path path = Paths.get(STORE_FILE);
        if (!Files.exists(path)) {
            log.info("[WeeklyCpr] No cached file at {} — will fetch from Fyers on scanner init", STORE_FILE);
            return;
        }
        try {
            JsonNode root = mapper.readTree(Files.readString(path));
            String storedDate = root.has("cachedDate") ? root.get("cachedDate").asText("") : "";
            if (storedDate.isEmpty() || isStale(storedDate)) {
                log.info("[WeeklyCpr] Cached file is stale (storedDate={}) — will refetch", storedDate);
                return;
            }
            JsonNode symbols = root.get("symbols");
            if (symbols == null || !symbols.isObject()) return;

            int loaded = 0;
            Iterator<Map.Entry<String, JsonNode>> fields = symbols.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                try {
                    WeeklyLevels wl = mapper.treeToValue(entry.getValue(), WeeklyLevels.class);
                    if (wl != null) {
                        weeklyLevels.put(entry.getKey(), wl);
                        loaded++;
                    }
                } catch (Exception ignored) {}
            }
            cachedDate = storedDate;
            log.info("[WeeklyCpr] Loaded {} weekly levels from cache (storedDate={})", loaded, storedDate);
        } catch (IOException e) {
            log.error("[WeeklyCpr] Failed to load weekly levels: {}", e.getMessage());
        }
    }

    /**
     * A stored cache is stale if the Monday of its ISO week is before the Monday of today's ISO
     * week. Weekly CPR levels are computed from the most recently completed week, so they need
     * to be refreshed whenever we cross a Monday boundary.
     *
     * On weekends: LocalDate.now() is Saturday/Sunday. The Monday of "this week" is the upcoming
     * Monday. Cached levels from Friday have Monday = last week's Monday = before upcoming Monday
     * → stale. Correct: on weekends we should still treat Friday's cache as current until next
     * Monday arrives. Adjust: compare to Monday of the CURRENT Mon-Fri week, not the calendar week.
     */
    private boolean isStale(String storedDate) {
        try {
            LocalDate stored = LocalDate.parse(storedDate);
            LocalDate today = LocalDate.now(IST);
            // For both dates, find the Monday of their trading week.
            // If date is Sun/Sat, the "Monday" of that trading week was 1-2 days BEFORE (last Mon).
            LocalDate storedMonday = mondayOfTradingWeek(stored);
            LocalDate todayMonday = mondayOfTradingWeek(today);
            return storedMonday.isBefore(todayMonday);
        } catch (Exception e) {
            return true;  // parse failure → treat as stale
        }
    }

    /** Return the Monday of the Mon-Fri trading week that contains `date`. */
    private LocalDate mondayOfTradingWeek(LocalDate date) {
        DayOfWeek dow = date.getDayOfWeek();
        int back;
        switch (dow) {
            case MONDAY: back = 0; break;
            case TUESDAY: back = 1; break;
            case WEDNESDAY: back = 2; break;
            case THURSDAY: back = 3; break;
            case FRIDAY: back = 4; break;
            case SATURDAY: back = 5; break;  // weekend belongs to the week that just ended
            case SUNDAY: back = 6; break;
            default: back = 0;
        }
        return date.minusDays(back);
    }

    // ── Real-time trend calculation using LTP ────────────────────────────────

    /**
     * Get weekly trend based on current LTP vs weekly CPR levels.
     * Matches Pine Script: close > wTop = Bullish, close < wBot = Bearish.
     * Strong Bullish = close > weekly R1 AND close > previous week high.
     */
    public String getWeeklyTrend(String symbol) {
        WeeklyLevels wl = weeklyLevels.get(symbol);
        if (wl == null) return "NEUTRAL";

        double ltp = getPrice(symbol);
        if (ltp <= 0) return "NEUTRAL";

        if (ltp > wl.r1 && ltp > wl.ph) return "STRONG_BULLISH";
        if (ltp < wl.s1 && ltp < wl.pl) return "STRONG_BEARISH";
        if (ltp > wl.top) return "BULLISH";
        if (ltp < wl.bot) return "BEARISH";
        return "NEUTRAL";
    }

    /**
     * Get daily trend based on current LTP vs daily CPR levels.
     * Matches Pine Script: close > dTop = Bullish, close < dBot = Bearish.
     * Strong Bullish = close > R1 AND close > PDH.
     */
    public String getDailyTrend(String symbol) {
        String ticker = extractTicker(symbol);
        CprLevels cpr = bhavcopyService.getCprLevels(ticker);
        if (cpr == null) return "NEUTRAL";

        double ltp = getPrice(symbol);
        if (ltp <= 0) return "NEUTRAL";

        double dTop = Math.max(cpr.getTc(), cpr.getBc());
        double dBot = Math.min(cpr.getTc(), cpr.getBc());

        if (ltp > cpr.getR1() && ltp > cpr.getPh()) return "STRONG_BULLISH";
        if (ltp < cpr.getS1() && ltp < cpr.getPl()) return "STRONG_BEARISH";
        if (ltp > dTop) return "BULLISH";
        if (ltp < dBot) return "BEARISH";
        return "NEUTRAL";
    }

    /**
     * Get probability based on real-time weekly + daily trends (for scanner dashboard display).
     * Returns the probability assuming current daily trend direction.
     * For actual signal generation, use getProbabilityForDirection() instead.
     */
    public String getProbability(String symbol) {
        String daily = getDailyTrend(symbol);
        boolean dBull = daily.contains("BULLISH");
        boolean dBear = daily.contains("BEARISH");
        if (dBull) return getProbabilityForDirection(symbol, true);
        if (dBear) return getProbabilityForDirection(symbol, false);
        // Daily neutral — no classification until breakout direction is known
        return "--";
    }

    /**
     * Get probability based on breakout direction + weekly trend + daily trend.
     * HPT: weekly AND daily aligned with direction.
     * Two-tier classification:
     *   HPT — weekly trend AND daily trend AND breakout direction all aligned
     *   LPT — everything else (weekly neutral/opposed, daily opposed, magnets, EV reversals)
     * @param isBuy true for buy breakout, false for sell breakout
     */
    public String getProbabilityForDirection(String symbol, boolean isBuy) {
        return getProbabilityForDirection(symbol, isBuy, false);
    }

    /**
     * Get probability based on breakout direction + weekly trend + daily trend.
     * Two-tier model:
     *   HPT — strict: weekly + daily + breakout direction all aligned
     *   LPT — anything weaker (weekly neutral, weekly opposed, daily opposed, magnets, reversals)
     * Magnet trades and EV reversals are counter-daily by design — they can never be HPT.
     * @param isBuy true for buy breakout, false for sell breakout
     * @param isMagnet true for magnet/mean-reversion trades (always LPT under the new model)
     */
    public String getProbabilityForDirection(String symbol, boolean isBuy, boolean isMagnet) {
        // Magnets and EV reversals are counter-daily by definition — always LPT.
        if (isMagnet) return "LPT";

        String weekly = getWeeklyTrend(symbol);
        String daily = getDailyTrend(symbol);
        boolean wBull = weekly.contains("BULLISH");
        boolean wBear = weekly.contains("BEARISH");
        boolean dBull = daily.contains("BULLISH");
        boolean dBear = daily.contains("BEARISH");

        if (isBuy) {
            if (wBull && dBull) return "HPT";    // strict: weekly + daily + buy direction
            return "LPT";
        } else {
            if (wBear && dBear) return "HPT";    // strict: weekly + daily + sell direction
            return "LPT";
        }
    }

    /**
     * Get current price: live LTP if available, fallback to previous close from bhavcopy
     * (for weekends/pre-market when no ticks are flowing).
     */
    private double getPrice(String symbol) {
        double ltp = candleAggregator.getLtp(symbol);
        if (ltp > 0) return ltp;
        // Fallback to previous close from BhavcopyService
        String ticker = extractTicker(symbol);
        CprLevels cpr = bhavcopyService.getCprLevels(ticker);
        return cpr != null ? cpr.getClose() : 0;
    }

    private String extractTicker(String fyersSymbol) {
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        if (s.endsWith("-EQ")) s = s.substring(0, s.length() - 3);
        else if (s.endsWith("-INDEX")) s = s.substring(0, s.length() - 6);
        return s;
    }

    // ── Inner class for weekly CPR levels ────────────────────────────────────

    public static class WeeklyLevels {
        public double pivot, tc, bc, top, bot;
        public double r1, s1, ph, pl;

        public WeeklyLevels() {}
    }
}
