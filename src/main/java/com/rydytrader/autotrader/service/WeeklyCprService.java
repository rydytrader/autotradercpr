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
public class WeeklyCprService implements CandleAggregator.CandleCloseListener,
                                          CandleAggregator.DailyResetListener {

    private static final Logger log = LoggerFactory.getLogger(WeeklyCprService.class);
    private static final String STORE_FILE = "../store/cache/weekly-cpr.json";
    private static final String LEGACY_STORE_FILE = "../store/config/weekly-cpr.json";
    private static final String TF_CLOSE_FILE = "../store/cache/tf-candle-close.json";
    private static final String LEGACY_TF_CLOSE_FILE = "../store/config/tf-candle-close.json";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final BhavcopyService bhavcopyService;
    private final CandleAggregator candleAggregator;
    private final com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;
    @org.springframework.beans.factory.annotation.Autowired
    private EventService eventService;
    private final ObjectMapper mapper = new ObjectMapper();

    // Weekly CPR levels per symbol (fixed for the week, fetched once)
    private final ConcurrentHashMap<String, WeeklyLevels> weeklyLevels = new ConcurrentHashMap<>();

    // Per-symbol candle close prices for each timeframe — trends derived from these (stable between closes).
    // Cleared on daily reset; first candle of each timeframe falls back to live LTP.
    private final ConcurrentHashMap<String, Double> lastTradingTfClose = new ConcurrentHashMap<>();  // 5-min → daily trend
    private final ConcurrentHashMap<String, Double> lastHigherTfClose = new ConcurrentHashMap<>();   // 75-min → weekly trend

    // Weekly level rejection detection on 75-min timeframe
    private final ConcurrentHashMap<String, RejectionState> weeklyRejection = new ConcurrentHashMap<>();

    public static class RejectionState {
        public String type = "NONE";      // NONE, WEEKLY_R1_PWH_REVERSAL, WEEKLY_S1_PWL_REVERSAL
        public double rejectionPrice = 0; // high of rejection candle (resistance) or low (support)
        public RejectionState() {}
    }

    // Date on which the cached weekly levels were computed (= most recent Monday or earlier when
    // levels were fetched). Used for staleness check during loadFromFile.
    private volatile String cachedDate = "";

    public WeeklyCprService(BhavcopyService bhavcopyService,
                            CandleAggregator candleAggregator,
                            com.rydytrader.autotrader.store.RiskSettingsStore riskSettings) {
        this.bhavcopyService = bhavcopyService;
        this.candleAggregator = candleAggregator;
        this.riskSettings = riskSettings;
    }

    @PostConstruct
    public void init() {
        loadFromFile();
        loadTfCloseFromFile();
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

        // Determine target week — the LAST COMPLETED week, used to compute pivot levels for THIS week.
        // If the latest week in our data is the current calendar week (mid-week or Friday EOD),
        // skip it and use the prior week. Otherwise the latest week IS already the previous week
        // (e.g. Monday morning before today's bhavcopy is added — latest data is Friday).
        LocalDate now = LocalDate.now(IST);
        int currentWeekKey = now.getYear() * 100
            + now.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        List<Integer> weekKeys = new ArrayList<>(datesByWeek.keySet());
        int latestIdx = weekKeys.size() - 1;
        boolean latestIsCurrentWeek = !weekKeys.isEmpty() && weekKeys.get(latestIdx) == currentWeekKey;
        int targetWeekIdx = latestIsCurrentWeek ? latestIdx - 1 : latestIdx;
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

    // ── Persistence: timeframe candle close maps ─────────────────────────────

    /**
     * Save both TF candle-close maps to disk. Called on every candle close and daily reset.
     * File includes today's date — stale on next day (daily reset clears the maps anyway).
     */
    private synchronized void saveTfCloseToFile() {
        try {
            File parent = new File(TF_CLOSE_FILE).getParentFile();
            if (parent != null) parent.mkdirs();
            ObjectNode root = mapper.createObjectNode();
            root.put("date", LocalDate.now(IST).toString());
            root.putPOJO("tradingTf", lastTradingTfClose);
            root.putPOJO("higherTf", lastHigherTfClose);
            root.putPOJO("rejections", weeklyRejection);
            Files.writeString(Paths.get(TF_CLOSE_FILE),
                mapper.writeValueAsString(root));
        } catch (Exception e) {
            log.error("[WeeklyCpr] Failed to save TF close data: {}", e.getMessage());
        }
    }

    /**
     * Load TF candle-close maps from disk on startup. Only loads if the stored date matches today
     * (same trading day = mid-day restart). Otherwise maps start empty (first candle falls back to LTP).
     */
    /** Resolve primary cache path, migrating from legacy store/config/ location on first boot. */
    private Path resolveCachePath(String primaryPath, String legacyPath) {
        Path primary = Paths.get(primaryPath);
        if (Files.exists(primary)) return primary;
        Path legacy = Paths.get(legacyPath);
        if (Files.exists(legacy)) {
            try {
                Files.createDirectories(primary.getParent());
                Files.move(legacy, primary, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                log.info("[MIGRATE] Moved {} -> {}", legacy, primary);
                return primary;
            } catch (IOException e) {
                log.warn("[MIGRATE] Failed to move {}, reading from legacy: {}", legacy, e.getMessage());
                return legacy;
            }
        }
        return primary;
    }

    private synchronized void loadTfCloseFromFile() {
        Path path = resolveCachePath(TF_CLOSE_FILE, LEGACY_TF_CLOSE_FILE);
        if (!Files.exists(path)) return;
        try {
            JsonNode root = mapper.readTree(Files.readString(path));
            String storedDate = root.has("date") ? root.get("date").asText("") : "";
            if (!LocalDate.now(IST).toString().equals(storedDate)) {
                log.info("[WeeklyCpr] TF close cache is from {} (not today) — starting fresh", storedDate);
                return;
            }
            JsonNode tradingTf = root.get("tradingTf");
            JsonNode higherTf = root.get("higherTf");
            int count = 0;
            if (tradingTf != null && tradingTf.isObject()) {
                tradingTf.fields().forEachRemaining(e -> {
                    try { lastTradingTfClose.put(e.getKey(), e.getValue().asDouble()); } catch (Exception ignored) {}
                });
                count += lastTradingTfClose.size();
            }
            if (higherTf != null && higherTf.isObject()) {
                higherTf.fields().forEachRemaining(e -> {
                    try { lastHigherTfClose.put(e.getKey(), e.getValue().asDouble()); } catch (Exception ignored) {}
                });
                count += lastHigherTfClose.size();
            }
            // Restore rejection states
            JsonNode rejections = root.get("rejections");
            if (rejections != null && rejections.isObject()) {
                rejections.fields().forEachRemaining(e -> {
                    try {
                        RejectionState rs = mapper.treeToValue(e.getValue(), RejectionState.class);
                        if (rs != null && !"NONE".equals(rs.type)) weeklyRejection.put(e.getKey(), rs);
                    } catch (Exception ignored) {}
                });
            }
            if (count > 0 || !weeklyRejection.isEmpty()) {
                log.info("[WeeklyCpr] Restored mid-day cache: {} trading-TF + {} higher-TF + {} rejections",
                    lastTradingTfClose.size(), lastHigherTfClose.size(), weeklyRejection.size());
            }
        } catch (Exception e) {
            log.error("[WeeklyCpr] Failed to load TF close data: {}", e.getMessage());
        }
    }

    // ── Persistence: weekly CPR levels ──────────────────────────────────────

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
        Path path = resolveCachePath(STORE_FILE, LEGACY_STORE_FILE);
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
    // ── CandleCloseListener / DailyResetListener ─────────────────────────────

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        if (completedCandle.close > 0) {
            lastTradingTfClose.put(fyersSymbol, completedCandle.close);
            saveTfCloseToFile();
        }
        // Eager reset: if this symbol has an active weekly reversal flag and LTP has
        // touched the weekly CPR zone, clear it now — don't wait for a breakout
        // candidate to trigger the lazy check inside getWeeklyRejection.
        RejectionState s = weeklyRejection.get(fyersSymbol);
        if (s != null && !"NONE".equals(s.type)) {
            getWeeklyRejection(fyersSymbol);
        }
    }

    public void onHigherTimeframeCandleClose(String fyersSymbol, double open, double high, double low, double close) {
        if (close > 0) lastHigherTfClose.put(fyersSymbol, close);

        WeeklyLevels wl = weeklyLevels.get(fyersSymbol);
        if (wl == null) { saveTfCloseToFile(); return; }

        RejectionState state = weeklyRejection.computeIfAbsent(fyersSymbol, k -> new RejectionState());

        // Nullify existing rejection: (1) candle closes beyond rejection extreme, (2) price reaches CPR
        if ("WEEKLY_R1_PWH_REVERSAL".equals(state.type)) {
            if (close >= state.rejectionPrice) {
                log.info("[WeeklyCpr] {} R1/PWH reversal NULLIFIED — close {} >= rejection high {}",
                    fyersSymbol, String.format("%.2f", close), String.format("%.2f", state.rejectionPrice));
                state.type = "NONE"; state.rejectionPrice = 0;
            } else if (close <= wl.top) {
                log.info("[WeeklyCpr] {} R1/PWH reversal NULLIFIED — close {} reached weekly CPR top {}",
                    fyersSymbol, String.format("%.2f", close), String.format("%.2f", wl.top));
                state.type = "NONE"; state.rejectionPrice = 0;
            }
        }
        if ("WEEKLY_S1_PWL_REVERSAL".equals(state.type)) {
            if (close <= state.rejectionPrice) {
                log.info("[WeeklyCpr] {} S1/PWL reversal NULLIFIED — close {} <= rejection low {}",
                    fyersSymbol, String.format("%.2f", close), String.format("%.2f", state.rejectionPrice));
                state.type = "NONE"; state.rejectionPrice = 0;
            } else if (close >= wl.bot) {
                log.info("[WeeklyCpr] {} S1/PWL reversal NULLIFIED — close {} reached weekly CPR bot {}",
                    fyersSymbol, String.format("%.2f", close), String.format("%.2f", wl.bot));
                state.type = "NONE"; state.rejectionPrice = 0;
            }
        }

        // Detect new resistance rejection (mirrors 5-min SELL_BELOW_R1_PDH magnet pattern).
        // Pattern 1: open or high reached either R1 or PH. Pattern 2: high broke the higher
        // of R1/PH while open was still below it. Either pattern, with close back inside
        // the R1/PH-to-weekly-top zone, qualifies as a rejection — red/green candle agnostic.
        double r1ph = Math.min(wl.r1, wl.ph);
        if (close < r1ph && close > wl.top
            && ((open > wl.r1 || open > wl.ph || high > wl.r1 || high > wl.ph)
                || (high > Math.max(wl.r1, wl.ph) && open < Math.max(wl.r1, wl.ph)))) {
            state.type = "WEEKLY_R1_PWH_REVERSAL";
            state.rejectionPrice = high;
            log.info("[WeeklyCpr] {} WEEKLY R1/PWH REVERSAL: O={} H={} L={} C={} (R1={} PH={} top={})",
                fyersSymbol, String.format("%.2f", open), String.format("%.2f", high),
                String.format("%.2f", low), String.format("%.2f", close),
                String.format("%.2f", wl.r1), String.format("%.2f", wl.ph), String.format("%.2f", wl.top));
        }

        // Detect new support rejection (mirrors 5-min BUY_ABOVE_S1_PDL magnet pattern).
        // Pattern 1: open or low reached either S1 or PL. Pattern 2: low broke the lower
        // of S1/PL while open was still above it. Either pattern, with close back inside
        // the S1/PL-to-weekly-bot zone, qualifies as a rejection — red/green candle agnostic.
        double s1pl = Math.max(wl.s1, wl.pl);
        if (close > s1pl && close < wl.bot
            && ((open < wl.s1 || open < wl.pl || low < wl.s1 || low < wl.pl)
                || (low < Math.min(wl.s1, wl.pl) && open > Math.min(wl.s1, wl.pl)))) {
            state.type = "WEEKLY_S1_PWL_REVERSAL";
            state.rejectionPrice = low;
            log.info("[WeeklyCpr] {} WEEKLY S1/PWL REVERSAL: O={} H={} L={} C={} (S1={} PL={} bot={})",
                fyersSymbol, String.format("%.2f", open), String.format("%.2f", high),
                String.format("%.2f", low), String.format("%.2f", close),
                String.format("%.2f", wl.s1), String.format("%.2f", wl.pl), String.format("%.2f", wl.bot));
        }

        saveTfCloseToFile();
    }

    public String getWeeklyRejection(String symbol) {
        RejectionState s = weeklyRejection.get(symbol);
        if (s == null || "NONE".equals(s.type)) return "NONE";

        // Live LTP-based reset: as soon as price touches the weekly CPR boundary, the
        // rejection thesis is complete — don't wait for the next HTF (60-min) candle close
        // to recognise it. The *rejection-extreme* reset (close above rejection high for
        // R1/PWH, below rejection low for S1/PWL) still requires a candle CLOSE — handled
        // in onHigherTimeframeCandleClose, not here.
        WeeklyLevels wl = weeklyLevels.get(symbol);
        if (wl != null) {
            double ltp = candleAggregator.getLtp(symbol);
            if (ltp > 0) {
                if ("WEEKLY_R1_PWH_REVERSAL".equals(s.type) && ltp <= wl.top) {
                    log.info("[WeeklyCpr] {} R1/PWH reversal auto-reset — LTP {} touched weekly CPR top {}",
                        symbol, String.format("%.2f", ltp), String.format("%.2f", wl.top));
                    s.type = "NONE"; s.rejectionPrice = 0;
                    saveTfCloseToFile();
                    return "NONE";
                }
                if ("WEEKLY_S1_PWL_REVERSAL".equals(s.type) && ltp >= wl.bot) {
                    log.info("[WeeklyCpr] {} S1/PWL reversal auto-reset — LTP {} touched weekly CPR bot {}",
                        symbol, String.format("%.2f", ltp), String.format("%.2f", wl.bot));
                    s.type = "NONE"; s.rejectionPrice = 0;
                    saveTfCloseToFile();
                    return "NONE";
                }
            }
        }

        return s.type;
    }

    @Override
    public void onDailyReset() {
        lastTradingTfClose.clear();
        lastHigherTfClose.clear();
        weeklyRejection.clear();
        saveTfCloseToFile();
    }

    // ── Public accessors ────────────────────────────────────────────────────

    public int getLoadedCount() { return weeklyLevels.size(); }

    /** Returns the weekly CPR levels for a symbol, or null if not loaded yet. */
    public WeeklyLevels getWeeklyLevels(String fyersSymbol) {
        return weeklyLevels.get(fyersSymbol);
    }

    public String getWeeklyTrend(String symbol) {
        WeeklyLevels wl = weeklyLevels.get(symbol);
        if (wl == null) return "NEUTRAL";

        double ltp = getWeeklyPrice(symbol);
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

        double ltp = getDailyPrice(symbol);
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

        // Weekly reversal flag: if active and trade opposes it, skip or downgrade
        String rejection = getWeeklyRejection(symbol);
        if (isBuy && "WEEKLY_R1_PWH_REVERSAL".equals(rejection)) {
            if (riskSettings.isWeeklyReversalHardSkip()) return "SKIP";
            return "LPT";
        }
        if (!isBuy && "WEEKLY_S1_PWL_REVERSAL".equals(rejection)) {
            if (riskSettings.isWeeklyReversalHardSkip()) return "SKIP";
            return "LPT";
        }

        // Weekly NEUTRAL → LPT. To skip these, disable LPT trades globally.
        if (isBuy) {
            if (wBull && dBull) return "HPT";
            return "LPT";
        } else {
            if (wBear && dBear) return "HPT";
            return "LPT";
        }
    }

    /**
     * Get current price: live LTP if available, fallback to previous close from bhavcopy
     * (for weekends/pre-market when no ticks are flowing).
     */
    /** Price for daily trend: last trading-TF (5-min) candle close, LTP fallback for first candle. */
    public double getDailyPrice(String symbol) {
        Double cc = lastTradingTfClose.get(symbol);
        if (cc != null && cc > 0) return cc;
        double ltp = candleAggregator.getLtp(symbol);
        if (ltp > 0) return ltp;
        String ticker = extractTicker(symbol);
        CprLevels cpr = bhavcopyService.getCprLevels(ticker);
        return cpr != null ? cpr.getClose() : 0;
    }

    /** Price for weekly trend: last higher-TF (60-min) candle close, LTP fallback for first candle. */
    public double getWeeklyPrice(String symbol) {
        Double cc = lastHigherTfClose.get(symbol);
        if (cc != null && cc > 0) return cc;
        double ltp = candleAggregator.getLtp(symbol);
        if (ltp > 0) return ltp;
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
