package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tracks NIFTY's <b>Virgin CPR</b> — when the index's session range never overlapped
 * today's daily CPR (BC..TC) by 15:30 IST, the day's CPR levels (TC, Pivot, BC) are
 * cached and act as additional hurdles in the NIFTY 5m hurdle filter for the next
 * {@code virginCprExpiryDays} <b>trading</b> days. (Not added to the 1h HTF hurdle
 * filter — daily-level concept stays at the daily-CPR gate.) A new virgin CPR
 * replaces any existing active record.
 *
 * <p>Persisted as a flat JSON file at {@value #CACHE_FILE} (matches the
 * {@code nse-holidays.json} cache pattern). On first startup after upgrading from
 * the legacy H2 settings row, the row is migrated to the JSON file and then deleted.
 */
@Service
public class VirginCprService {

    private static final Logger log = LoggerFactory.getLogger(VirginCprService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String CACHE_FILE = "../store/cache/virgin-cpr.json";
    /** Legacy H2 settings key — read once at startup for migration, then removed. */
    private static final String STATE_KEY = "virginCprState";
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

    private volatile Snapshot snapshot;

    @PostConstruct
    public void init() {
        load();
    }

    /**
     * Runs at 15:30:30 IST on weekdays. After the session closes, check if NIFTY's
     * range today overlapped daily CPR. If not, form a virgin CPR (replaces any
     * existing record).
     */
    @Scheduled(cron = "30 30 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledDetect() {
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;
        detect();
    }

    /** Manual trigger — for diagnostics / out-of-hours testing. */
    public void triggerDetect() { detect(); }

    private synchronized void detect() {
        try {
            String niftySym = IndexTrendService.NIFTY_SYMBOL;
            double sessionHigh = candleAggregator.getDayHighExcluding(niftySym, null);
            double sessionLow  = candleAggregator.getDayLowExcluding(niftySym, null);
            if (sessionHigh <= 0 || sessionLow <= 0) {
                log.info("[VirginCPR] Session H/L not available — skipping detection (high={}, low={})", sessionHigh, sessionLow);
                return;
            }

            CprLevels cpr = bhavcopyService.getCprLevels("NIFTY50");
            if (cpr == null || cpr.getTc() <= 0 || cpr.getBc() <= 0) {
                log.warn("[VirginCPR] NIFTY CPR not loaded — skipping detection");
                return;
            }
            double cprTop = Math.max(cpr.getTc(), cpr.getBc());
            double cprBot = Math.min(cpr.getTc(), cpr.getBc());

            boolean untouched = sessionLow > cprTop || sessionHigh < cprBot;
            String today = LocalDate.now(IST).toString();
            if (!untouched) {
                log.info("[VirginCPR] {} CPR was touched (session H={} L={}, CPR {}—{}) — no virgin CPR formed",
                    today, fmt(sessionHigh), fmt(sessionLow), fmt(cprBot), fmt(cprTop));
                return;
            }

            Snapshot fresh = new Snapshot();
            fresh.date = today;
            fresh.tc = cpr.getTc();
            fresh.pivot = cpr.getPivot();
            fresh.bc = cpr.getBc();
            this.snapshot = fresh;
            save();
            log.info("[VirginCPR] Formed — date={} TC={} Pivot={} BC={} (session H={} L={}, CPR {}—{})",
                fresh.date, fmt(fresh.tc), fmt(fresh.pivot), fmt(fresh.bc),
                fmt(sessionHigh), fmt(sessionLow), fmt(cprBot), fmt(cprTop));
            eventService.log("[INFO] NIFTY Virgin CPR formed: TC=" + fmt(fresh.tc)
                + " Pivot=" + fmt(fresh.pivot) + " BC=" + fmt(fresh.bc));
        } catch (Exception e) {
            log.warn("[VirginCPR] Detection failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Returns the current active virgin CPR (within the configured trading-day expiry
     * window) or null if expired / never formed / feature disabled.
     */
    public Snapshot getActiveVirginCpr() {
        Snapshot s = snapshot;
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
     * UI-friendly map of the active virgin CPR (date, levels, days remaining) or null
     * if no active record.
     */
    public Map<String, Object> getActiveStatus() {
        Snapshot s = getActiveVirginCpr();
        if (s == null) return null;
        Map<String, Object> r = new LinkedHashMap<>();
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
     * One-time backfill — scans NIFTY's last {@code tradingDays} trading days for any
     * untouched-CPR days. Saves the LATEST untouched day as the active virgin CPR
     * (replacement rule). Returns a brief result message for the admin endpoint.
     */
    public synchronized Map<String, Object> backfill(int tradingDays) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            // Fetch enough calendar days to cover tradingDays trading days + one prior day for
            // the very first day's CPR computation. tradingDays * 1.6 + 5 is comfortably wide.
            int calendarDays = (int) Math.ceil(tradingDays * 1.6) + 5;
            List<double[]> bars = fetchNiftyHistory(15, calendarDays);
            if (bars.isEmpty()) {
                result.put("success", false);
                result.put("message", "No history candles fetched");
                return result;
            }

            // Group bars by IST date → daily H/L/C (last bar's close = session close).
            TreeMap<LocalDate, double[]> dailyHLC = new TreeMap<>();
            for (double[] b : bars) {
                long epochSec = (long) b[0];
                LocalDate date = Instant.ofEpochSecond(epochSec).atZone(IST).toLocalDate();
                double[] hlc = dailyHLC.get(date);
                if (hlc == null) {
                    hlc = new double[]{ b[2], b[3], b[4] }; // [high, low, close]
                    dailyHLC.put(date, hlc);
                } else {
                    if (b[2] > hlc[0]) hlc[0] = b[2];
                    if (b[3] < hlc[1]) hlc[1] = b[3];
                    hlc[2] = b[4]; // last bar's close becomes session close
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
                // Skip days too far back (older than the requested window) and also today
                // (today's CPR-touched determination is the job of the live scheduled detector).
                if (tdSince <= 0 || tdSince > tradingDays) continue;

                double[] prevHLC = dailyHLC.get(prev);
                double[] currHLC = dailyHLC.get(curr);
                if (prevHLC == null || currHLC == null) continue;

                // Compute curr-day CPR from prev-day HLC (same formula as CprLevels).
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
                    latest = s; // overwrite — we want the most recent virgin
                    virginCount++;
                    virginDates.add(curr.toString());
                    log.info("[VirginCPR] Backfill found virgin on {}: TC={} Pivot={} BC={} (session H={} L={})",
                        curr, fmt(tc), fmt(pivot), fmt(bc), fmt(currHigh), fmt(currLow));
                }
            }

            if (latest != null) {
                this.snapshot = latest;
                save();
                eventService.log("[INFO] Virgin CPR backfill: kept latest from " + latest.date
                    + " (TC=" + fmt(latest.tc) + " Pivot=" + fmt(latest.pivot) + " BC=" + fmt(latest.bc) + ")");
            }
            result.put("success", true);
            result.put("daysScanned", tradingDays);
            result.put("virginDaysFound", virginCount);
            result.put("virginDates", virginDates);
            result.put("activeAfter", latest != null ? latest.date : null);
            result.put("message", latest != null
                ? ("Found " + virginCount + " virgin CPR day(s); active virgin set to " + latest.date)
                : ("No virgin CPR found in last " + tradingDays + " trading days"));
            return result;
        } catch (Exception e) {
            log.error("[VirginCPR] Backfill failed: {}", e.getMessage(), e);
            result.put("success", false);
            result.put("message", "Backfill error: " + e.getMessage());
            return result;
        }
    }

    /**
     * Fetch NIFTY 50 history from Fyers /data/history. Returns rows of
     * [epochSec, open, high, low, close, volume] in chronological order.
     */
    private List<double[]> fetchNiftyHistory(int resolutionMin, int daysBack) throws Exception {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.warn("[VirginCPR] No access token — cannot fetch history");
            return new ArrayList<>();
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;
        long toEpoch = Instant.now().getEpochSecond();
        long fromEpoch = toEpoch - ((long) daysBack * 24 * 3600);
        String urlStr = "https://api-t1.fyers.in/data/history?symbol="
            + java.net.URLEncoder.encode(IndexTrendService.NIFTY_SYMBOL, StandardCharsets.UTF_8)
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
            log.warn("[VirginCPR] Fyers history HTTP {}", status);
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
                c.get(0).asDouble(),  // epochSec
                c.get(1).asDouble(),  // open
                c.get(2).asDouble(),  // high
                c.get(3).asDouble(),  // low
                c.get(4).asDouble(),  // close
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

    private void save() {
        if (snapshot == null) return;
        try {
            Path path = Paths.get(CACHE_FILE);
            Files.createDirectories(path.getParent());
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(snapshot);
            Files.writeString(path, json);
        } catch (Exception e) {
            log.error("[VirginCPR] Save failed: {}", e.getMessage());
        }
    }

    private void load() {
        // Prefer the JSON cache file. If absent, attempt a one-time migration from the legacy
        // H2 settings row (the prior persistence path) and remove the row after a successful
        // copy so the two paths don't drift.
        try {
            Path path = Paths.get(CACHE_FILE);
            if (Files.exists(path)) {
                snapshot = mapper.readValue(Files.readString(path), Snapshot.class);
                if (snapshot != null) {
                    log.info("[VirginCPR] Loaded from JSON: date={} TC={} Pivot={} BC={}",
                        snapshot.date, fmt(snapshot.tc), fmt(snapshot.pivot), fmt(snapshot.bc));
                }
                return;
            }
        } catch (Exception e) {
            log.warn("[VirginCPR] JSON load failed: {}", e.getMessage());
        }
        // No JSON file — try migrating from the legacy H2 settings row.
        try {
            settingRepo.findBySettingKey(STATE_KEY).ifPresent(e -> {
                String json = e.getSettingValue();
                if (json == null || json.isBlank()) return;
                try {
                    snapshot = mapper.readValue(json, Snapshot.class);
                    if (snapshot != null) {
                        save(); // write JSON file
                        settingRepo.delete(e);
                        log.info("[VirginCPR] Migrated H2 settings row -> {} (date={})", CACHE_FILE, snapshot.date);
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
