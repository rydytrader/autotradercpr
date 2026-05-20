package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.repository.SettingRepository;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks <b>Virgin CPR</b> per index (NIFTY 50 + every sector index in the DB Stock Universe).
 * When an index's session range never overlapped today's daily CPR (BC..TC) by 15:30 IST, that
 * day's CPR levels (TC, Pivot, BC) are cached for the index and act as additional hurdles in
 * the per-stock virgin-CPR filter for the next {@code virginCprExpiryDays} <b>trading</b> days.
 * The filter resolves the stock's primary index and checks that index's virgin CPR — not always
 * NIFTY's. A new virgin CPR for an index replaces any existing active record for that ticker.
 *
 * <p>Persisted as a flat JSON map at {@value #CACHE_FILE}: top-level keys are index tickers,
 * values are the snapshot ({date, tc, pivot, bc}). On first startup after the upgrade from the
 * single-snapshot format, the legacy JSON (one Snapshot at root) is migrated as the NIFTY50 entry.
 */
@Service
public class VirginCprService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(VirginCprService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE = "../store/cache/virgin-cpr.json";
    /** Legacy H2 settings key — read once at startup for migration, then removed. */
    private static final String STATE_KEY = "virginCprState";
    /** Default ticker — kept for backwards-compatible no-arg API used by the NIFTY card UI. */
    private static final String DEFAULT_TICKER = "NIFTY50";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static class Snapshot {
        public String date;   // ISO yyyy-MM-dd — day on which the virgin CPR formed
        public double tc;
        public double pivot;
        public double bc;
    }

    @Autowired private SettingRepository settingRepo;
    @Autowired private RiskSettingsStore riskSettings;
    @Autowired private CandleAggregator candleAggregator;
    @Autowired private BhavcopyService bhavcopyService;
    @Autowired private MarketHolidayService marketHolidayService;
    @Autowired private EventService eventService;
    @Autowired private TokenStore tokenStore;
    @Autowired private FyersProperties fyersProperties;

    /** ticker (e.g. "NIFTY50", "NIFTYBANK") → most recent active virgin CPR snapshot. */
    private final ConcurrentHashMap<String, Snapshot> snapshotsByTicker = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        load();
        // Register for all index 5-min candle closes so we can invalidate any active virgin CPR
        // the moment the matching index's close lands inside its BC..TC zone. Listener is
        // symbol-agnostic; the body filters to index symbols.
        if (candleAggregator != null) {
            candleAggregator.addListener(this);
        }
    }

    /**
     * 5-min candle close handler. For any completed index bar, checks whether that index has
     * an active virgin CPR snapshot and invalidates it if the bar closes inside the zone.
     * Bars on non-index symbols are ignored.
     */
    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar candle) {
        if (fyersSymbol == null || !fyersSymbol.startsWith("NSE:") || !fyersSymbol.endsWith("-INDEX")) return;
        if (candle == null || candle.close <= 0) return;
        String ticker = extractIndexTicker(fyersSymbol);
        Snapshot s = snapshotsByTicker.get(ticker);
        if (s == null || s.tc <= 0 || s.bc <= 0) return;
        double zoneTop = Math.max(s.tc, s.bc);
        double zoneBot = Math.min(s.tc, s.bc);
        if (candle.close >= zoneBot && candle.close <= zoneTop) {
            log.info("[VirginCPR] {} invalidated by 5m close at {} inside zone [{}..{}] (formed {})",
                ticker, fmt(candle.close), fmt(zoneBot), fmt(zoneTop), s.date);
            eventService.log("[INFO] " + ticker + " Virgin CPR invalidated — 5m close "
                + fmt(candle.close) + " inside zone [" + fmt(zoneBot) + ".." + fmt(zoneTop)
                + "] (formed " + s.date + ")");
            synchronized (this) {
                snapshotsByTicker.remove(ticker);
                save();
            }
        }
    }

    /**
     * Runs at 15:30:30 IST on weekdays. After the session closes, check each tracked index
     * to see if its range today overlapped its daily CPR. If not, form a virgin CPR for that
     * index (replaces any existing record for that ticker).
     */
    @Scheduled(cron = "30 30 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledDetect() {
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;
        detectAll();
    }

    /** Manual trigger — for diagnostics / out-of-hours testing. */
    public void triggerDetect() { detectAll(); }

    /** Manually clear the active virgin CPR for the default ticker (NIFTY50). */
    public synchronized void clearSnapshot() {
        clearSnapshot(DEFAULT_TICKER);
    }

    /** Manually clear the active virgin CPR for a specific index ticker. */
    public synchronized void clearSnapshot(String ticker) {
        Snapshot prev = snapshotsByTicker.remove(ticker);
        if (prev == null) return;
        save();
        log.info("[VirginCPR] {} manually cleared (was formed {})", ticker, prev.date);
        if (eventService != null) {
            eventService.log("[INFO] " + ticker + " Virgin CPR manually cleared (was formed " + prev.date + ")");
        }
    }

    /** Detect virgin CPR for every tracked index (NIFTY 50 + sector indices in the DB). */
    private synchronized void detectAll() {
        List<String> tickers = new ArrayList<>();
        tickers.add("NIFTY50");
        if (bhavcopyService != null) {
            for (String t : bhavcopyService.getAllSectoralIndexTickers()) {
                if (!tickers.contains(t)) tickers.add(t);
            }
        }
        for (String ticker : tickers) {
            try {
                detectOne(ticker);
            } catch (Exception e) {
                log.warn("[VirginCPR] {} detection failed: {}", ticker, e.getMessage());
            }
        }
        save();
    }

    /** Detect virgin CPR for a single index ticker. Replaces any existing snapshot on hit. */
    private void detectOne(String ticker) {
        String fyersSym = "NSE:" + ticker + "-INDEX";
        double sessionHigh = candleAggregator.getDayHighExcluding(fyersSym, null);
        double sessionLow  = candleAggregator.getDayLowExcluding(fyersSym, null);
        if (sessionHigh <= 0 || sessionLow <= 0) {
            log.info("[VirginCPR] {} session H/L not available — skipping (high={}, low={})",
                ticker, sessionHigh, sessionLow);
            return;
        }

        CprLevels cpr = bhavcopyService.getCprLevels(ticker);
        if (cpr == null || cpr.getTc() <= 0 || cpr.getBc() <= 0) {
            log.warn("[VirginCPR] {} CPR not loaded — skipping", ticker);
            return;
        }
        double cprTop = Math.max(cpr.getTc(), cpr.getBc());
        double cprBot = Math.min(cpr.getTc(), cpr.getBc());

        boolean untouched = sessionLow > cprTop || sessionHigh < cprBot;
        String today = LocalDate.now(IST).toString();
        if (!untouched) {
            log.info("[VirginCPR] {} {} CPR was touched (session H={} L={}, CPR {}—{}) — no virgin CPR formed",
                ticker, today, fmt(sessionHigh), fmt(sessionLow), fmt(cprBot), fmt(cprTop));
            return;
        }

        Snapshot fresh = new Snapshot();
        fresh.date = today;
        fresh.tc = cpr.getTc();
        fresh.pivot = cpr.getPivot();
        fresh.bc = cpr.getBc();
        snapshotsByTicker.put(ticker, fresh);
        log.info("[VirginCPR] {} Formed — date={} TC={} Pivot={} BC={} (session H={} L={}, CPR {}—{})",
            ticker, fresh.date, fmt(fresh.tc), fmt(fresh.pivot), fmt(fresh.bc),
            fmt(sessionHigh), fmt(sessionLow), fmt(cprBot), fmt(cprTop));
        eventService.log("[INFO] " + ticker + " Virgin CPR formed: TC=" + fmt(fresh.tc)
            + " Pivot=" + fmt(fresh.pivot) + " BC=" + fmt(fresh.bc));
    }

    /**
     * Returns the active virgin CPR for the default ticker (NIFTY50) within the configured
     * trading-day expiry window. Kept for backwards compatibility with the NIFTY card UI.
     */
    public Snapshot getActiveVirginCpr() {
        return getActiveVirginCpr(DEFAULT_TICKER);
    }

    /**
     * Returns the active virgin CPR for the given index ticker within the configured trading-day
     * expiry window, or null if expired / never formed / feature disabled. Used by the Virgin CPR
     * Hurdle filter (per-stock, resolved via the stock's primary index).
     */
    public Snapshot getActiveVirginCpr(String ticker) {
        if (ticker == null) return null;
        Snapshot s = snapshotsByTicker.get(ticker);
        if (s == null || s.date == null) return null;
        int expiryDays = riskSettings != null ? riskSettings.getVirginCprExpiryDays() : 0;
        if (expiryDays <= 0) return null;
        LocalDate formed;
        try { formed = LocalDate.parse(s.date); } catch (Exception e) { return null; }
        int tradingDaysSince = countTradingDaysAfter(formed, LocalDate.now(IST));
        if (tradingDaysSince > expiryDays) return null;
        return s;
    }

    /**
     * UI-friendly map of the active NIFTY virgin CPR (date, levels, days remaining) or null.
     * Used by the NIFTY card's virgin chip endpoint.
     */
    public Map<String, Object> getActiveStatus() {
        return getActiveStatus(DEFAULT_TICKER);
    }

    public Map<String, Object> getActiveStatus(String ticker) {
        Snapshot s = getActiveVirginCpr(ticker);
        if (s == null) return null;
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ticker", ticker);
        r.put("date", s.date);
        r.put("tc", s.tc);
        r.put("pivot", s.pivot);
        r.put("bc", s.bc);
        try {
            int used = countTradingDaysAfter(LocalDate.parse(s.date), LocalDate.now(IST));
            int remaining = Math.max(0, riskSettings.getVirginCprExpiryDays() - used);
            r.put("tradingDaysSince", used);
            r.put("daysRemaining", remaining);
        } catch (Exception e) {
            r.put("tradingDaysSince", 0);
            r.put("daysRemaining", 0);
        }
        return r;
    }

    /**
     * One-time backfill — scans the requested index's last {@code tradingDays} trading days
     * for untouched-CPR days. Saves the LATEST untouched day as the active virgin CPR for
     * that ticker (replacement rule). Defaults to NIFTY50 for backwards compatibility.
     */
    public synchronized Map<String, Object> backfill(int tradingDays) {
        return backfill(tradingDays, DEFAULT_TICKER);
    }

    public synchronized Map<String, Object> backfill(int tradingDays, String ticker) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            int calendarDays = (int) Math.ceil(tradingDays * 1.6) + 5;
            String fyersSym = "NSE:" + ticker + "-INDEX";
            List<double[]> bars = fetchIndexHistory(fyersSym, 15, calendarDays);
            if (bars.isEmpty()) {
                result.put("success", false);
                result.put("message", "No history candles fetched for " + ticker);
                return result;
            }

            TreeMap<LocalDate, double[]> dailyHLC = new TreeMap<>();
            for (double[] b : bars) {
                long epochSec = (long) b[0];
                LocalDate date = Instant.ofEpochSecond(epochSec).atZone(IST).toLocalDate();
                double[] hlc = dailyHLC.get(date);
                if (hlc == null) {
                    hlc = new double[]{ b[2], b[3], b[4] };
                    dailyHLC.put(date, hlc);
                } else {
                    if (b[2] > hlc[0]) hlc[0] = b[2];
                    if (b[3] < hlc[1]) hlc[1] = b[3];
                    hlc[2] = b[4];
                }
            }

            List<LocalDate> sortedDates = new ArrayList<>(dailyHLC.keySet());
            LocalDate today = LocalDate.now(IST);
            Snapshot latest = null;
            int virginCount = 0;
            List<String> virginDates = new ArrayList<>();

            for (int i = 1; i < sortedDates.size(); i++) {
                LocalDate prev = sortedDates.get(i - 1);
                LocalDate curr = sortedDates.get(i);
                int tdSince = countTradingDaysAfter(curr, today);
                if (tdSince <= 0 || tdSince > tradingDays) continue;

                double[] prevHLC = dailyHLC.get(prev);
                double[] currHLC = dailyHLC.get(curr);
                if (prevHLC == null || currHLC == null) continue;

                double prevHigh = prevHLC[0], prevLow = prevHLC[1], prevClose = prevHLC[2];
                double pivot = (prevHigh + prevLow + prevClose) / 3.0;
                double bc    = (prevHigh + prevLow) / 2.0;
                double tc    = 2.0 * pivot - bc;
                double cprTop = Math.max(tc, bc);
                double cprBot = Math.min(tc, bc);

                double currHigh = currHLC[0], currLow = currHLC[1];
                boolean untouched = currLow > cprTop || currHigh < cprBot;
                if (untouched) {
                    Snapshot s = new Snapshot();
                    s.date = curr.toString();
                    s.tc = tc;
                    s.pivot = pivot;
                    s.bc = bc;
                    latest = s;
                    virginCount++;
                    virginDates.add(curr.toString());
                    log.info("[VirginCPR] {} backfill found virgin on {}: TC={} Pivot={} BC={} (session H={} L={})",
                        ticker, curr, fmt(tc), fmt(pivot), fmt(bc), fmt(currHigh), fmt(currLow));
                }
            }

            if (latest != null) {
                snapshotsByTicker.put(ticker, latest);
                save();
                eventService.log("[INFO] " + ticker + " Virgin CPR backfill: kept latest from " + latest.date
                    + " (TC=" + fmt(latest.tc) + " Pivot=" + fmt(latest.pivot) + " BC=" + fmt(latest.bc) + ")");
            }
            result.put("success", true);
            result.put("ticker", ticker);
            result.put("daysScanned", tradingDays);
            result.put("virginDaysFound", virginCount);
            result.put("virginDates", virginDates);
            result.put("activeAfter", latest != null ? latest.date : null);
            result.put("message", latest != null
                ? ("Found " + virginCount + " virgin CPR day(s) for " + ticker + "; active set to " + latest.date)
                : ("No virgin CPR found in last " + tradingDays + " trading days for " + ticker));
            return result;
        } catch (Exception e) {
            log.error("[VirginCPR] {} backfill failed: {}", ticker, e.getMessage(), e);
            result.put("success", false);
            result.put("message", "Backfill error: " + e.getMessage());
            return result;
        }
    }

    /**
     * Fetch index history from Fyers /data/history. Returns rows of
     * [epochSec, open, high, low, close, volume] in chronological order.
     */
    private List<double[]> fetchIndexHistory(String fyersSymbol, int resolutionMin, int daysBack) throws Exception {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.warn("[VirginCPR] No access token — cannot fetch history");
            return new ArrayList<>();
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;
        long toEpoch = Instant.now().getEpochSecond();
        long fromEpoch = toEpoch - ((long) daysBack * 24 * 3600);
        String urlStr = "https://api-t1.fyers.in/data/history?symbol="
            + java.net.URLEncoder.encode(fyersSymbol, StandardCharsets.UTF_8)
            + "&resolution=" + resolutionMin
            + "&date_format=0"
            + "&range_from=" + fromEpoch
            + "&range_to=" + toEpoch
            + "&cont_flag=1";
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", authHeader);
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(15_000);
        int status = conn.getResponseCode();
        if (status != 200) {
            log.warn("[VirginCPR] {} Fyers history HTTP {}", fyersSymbol, status);
            return new ArrayList<>();
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }
        JsonNode root = mapper.readTree(sb.toString());
        JsonNode arr = root.get("candles");
        List<double[]> out = new ArrayList<>();
        if (arr == null || !arr.isArray()) return out;
        for (JsonNode c : arr) {
            if (c.size() < 5) continue;
            double[] row = new double[]{
                c.get(0).asDouble(),
                c.get(1).asDouble(),
                c.get(2).asDouble(),
                c.get(3).asDouble(),
                c.get(4).asDouble(),
                c.size() >= 6 ? c.get(5).asDouble() : 0
            };
            out.add(row);
        }
        return out;
    }

    /** Trading days strictly after {@code from} up to and including {@code to}. */
    private int countTradingDaysAfter(LocalDate from, LocalDate to) {
        if (!to.isAfter(from)) return 0;
        int count = 0;
        LocalDate cur = from.plusDays(1);
        while (!cur.isAfter(to)) {
            if (marketHolidayService == null || marketHolidayService.isTradingDay(cur)) count++;
            cur = cur.plusDays(1);
        }
        return count;
    }

    private static String extractIndexTicker(String fyersSymbol) {
        // Strip "NSE:" prefix and "-INDEX" suffix → "NIFTY50" / "NIFTYBANK" / etc.
        String s = fyersSymbol;
        if (s.startsWith("NSE:")) s = s.substring(4);
        if (s.endsWith("-INDEX")) s = s.substring(0, s.length() - 6);
        return s;
    }

    private void save() {
        try {
            Path path = Paths.get(CACHE_FILE);
            if (snapshotsByTicker.isEmpty()) {
                Files.deleteIfExists(path);
                return;
            }
            Files.createDirectories(path.getParent());
            ObjectNode root = mapper.createObjectNode();
            for (Map.Entry<String, Snapshot> e : snapshotsByTicker.entrySet()) {
                Snapshot s = e.getValue();
                if (s == null) continue;
                ObjectNode entry = root.putObject(e.getKey());
                entry.put("date", s.date);
                entry.put("tc", s.tc);
                entry.put("pivot", s.pivot);
                entry.put("bc", s.bc);
            }
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            Files.writeString(path, json);
        } catch (Exception e) {
            log.error("[VirginCPR] Save failed: {}", e.getMessage());
        }
    }

    private void load() {
        // Prefer the JSON cache file. Two on-disk formats are supported:
        //   1. Legacy (pre-multi-index): a single Snapshot at the root → migrated as NIFTY50.
        //   2. New: a map of ticker → Snapshot at the root.
        try {
            Path path = Paths.get(CACHE_FILE);
            if (Files.exists(path)) {
                JsonNode root = mapper.readTree(Files.readString(path));
                if (root != null && root.isObject()) {
                    if (root.has("date") && root.has("tc")) {
                        // Legacy single-snapshot format — migrate as NIFTY50.
                        Snapshot s = mapper.treeToValue(root, Snapshot.class);
                        if (s != null && s.date != null) {
                            snapshotsByTicker.put(DEFAULT_TICKER, s);
                            log.info("[VirginCPR] Migrated legacy single-snapshot cache → NIFTY50 (date={} TC={} Pivot={} BC={})",
                                s.date, fmt(s.tc), fmt(s.pivot), fmt(s.bc));
                            save(); // rewrite in new format
                        }
                    } else {
                        // New map format.
                        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
                        while (it.hasNext()) {
                            Map.Entry<String, JsonNode> e = it.next();
                            try {
                                Snapshot s = mapper.treeToValue(e.getValue(), Snapshot.class);
                                if (s != null && s.date != null) snapshotsByTicker.put(e.getKey(), s);
                            } catch (Exception ex) {
                                log.warn("[VirginCPR] Skipping malformed entry for {}: {}", e.getKey(), ex.getMessage());
                            }
                        }
                        if (!snapshotsByTicker.isEmpty()) {
                            log.info("[VirginCPR] Loaded {} index snapshot(s) from JSON: {}",
                                snapshotsByTicker.size(), snapshotsByTicker.keySet());
                        }
                    }
                    return;
                }
            }
        } catch (Exception e) {
            log.warn("[VirginCPR] JSON load failed: {}", e.getMessage());
        }
        // No JSON file — try migrating from the legacy H2 settings row (one-time).
        try {
            settingRepo.findBySettingKey(STATE_KEY).ifPresent(e -> {
                String json = e.getSettingValue();
                if (json == null || json.isBlank()) return;
                try {
                    Snapshot s = mapper.readValue(json, Snapshot.class);
                    if (s != null && s.date != null) {
                        snapshotsByTicker.put(DEFAULT_TICKER, s);
                        save();
                        settingRepo.delete(e);
                        log.info("[VirginCPR] Migrated H2 settings row -> {} as NIFTY50 (date={})", CACHE_FILE, s.date);
                    }
                } catch (Exception ex) {
                    log.warn("[VirginCPR] Legacy H2 row parse failed: {}", ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("[VirginCPR] Load failed: {}", e.getMessage());
        }
    }

    private static String fmt(double v) { return String.format("%.2f", v); }
}
