package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.config.FyersProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ATR(14) calculation service.
 * Fetches historical 15-min candles from Fyers /data/history at 9:00 AM.
 * Updates ATR live from CandleAggregator completed candles.
 */
@Service
public class AtrService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(AtrService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int DEFAULT_ATR_PERIOD = 14;

    private int getAtrPeriod() {
        int period = riskSettings.getAtrPeriod();
        return period > 0 ? period : DEFAULT_ATR_PERIOD;
    }

    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final RiskSettingsStore riskSettings;
    private final CandleAggregator candleAggregator;
    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private EmaService emaService;
    @org.springframework.beans.factory.annotation.Autowired
    private EventService eventService;
    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, Double> atrBySymbol = new ConcurrentHashMap<>();

    public AtrService(TokenStore tokenStore,
                      FyersProperties fyersProperties,
                      RiskSettingsStore riskSettings,
                      CandleAggregator candleAggregator) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.riskSettings = riskSettings;
        this.candleAggregator = candleAggregator;
    }

    /**
     * Fetch ATR for all watchlist symbols. Called at 9:00 AM or on restart.
     */
    public void fetchAtrForSymbols(List<String> fyersSymbols) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[AtrService] No access token, cannot fetch ATR");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;
        int timeframe = riskSettings.getScannerTimeframe();

        log.info("[AtrService] Fetching ATR({}) for {} symbols ({}min candles)", getAtrPeriod(), fyersSymbols.size(), timeframe);

        int success = 0;
        List<String> failed = new ArrayList<>();
        for (String symbol : fyersSymbols) {
            try {
                List<CandleAggregator.CandleBar> candles = fetchHistoricalCandles(symbol, timeframe, authHeader);
                if (candles.size() >= getAtrPeriod()) {
                    double atr = calculateAtr(candles, getAtrPeriod());
                    atrBySymbol.put(symbol, atr);
                    // Seed candle aggregator with historical candles
                    candleAggregator.seedCandles(symbol, candles);
                    // Seed EMA from the raw multi-day historical list (bypasses the
                    // date-filtered completedCandles deque so slope/position work on holidays).
                    if (emaService != null) emaService.seedFromCandles(symbol, candles);
                    success++;
                } else {
                    log.warn("[AtrService] Only {} candles for {} (need {})", candles.size(), symbol, getAtrPeriod());
                    failed.add(symbol);
                }
                Thread.sleep(300);
            } catch (Exception e) {
                log.error("[AtrService] Failed to fetch ATR for {}: {}", symbol, e.getMessage());
                failed.add(symbol);
            }
        }

        // Retry failed symbols with longer delay
        if (!failed.isEmpty()) {
            log.info("[AtrService] Retrying {} failed symbols after 2s delay...", failed.size());
            try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            for (String symbol : failed) {
                try {
                    List<CandleAggregator.CandleBar> candles = fetchHistoricalCandles(symbol, timeframe, authHeader);
                    if (candles.size() >= getAtrPeriod()) {
                        double atr = calculateAtr(candles, getAtrPeriod());
                        atrBySymbol.put(symbol, atr);
                        candleAggregator.seedCandles(symbol, candles);
                        if (emaService != null) emaService.seedFromCandles(symbol, candles);
                        success++;
                        log.info("[AtrService] Retry succeeded for {}", symbol);
                    }
                    Thread.sleep(500);
                } catch (Exception e) {
                    log.error("[AtrService] Retry failed for {}: {}", symbol, e.getMessage());
                }
            }
        }

        log.info("[AtrService] ATR loaded for {}/{} symbols", success, fyersSymbols.size());
        eventService.log("[INFO] ATR + EMA loaded for " + success + "/" + fyersSymbols.size() + " symbols ("
            + riskSettings.getScannerTimeframe() + "min candles)");
    }

    /**
     * Fetch historical candles from Fyers /data/history API.
     */
    /** Fetch today's morning candles (from market open to now) for validation. */
    public List<CandleAggregator.CandleBar> fetchTodayCandles(String symbol) {
        try {
            String accessToken = tokenStore.getAccessToken();
            if (accessToken == null || accessToken.isEmpty()) return Collections.emptyList();
            String authHeader = fyersProperties.getClientId() + ":" + accessToken;
            return fetchHistoricalCandles(symbol, riskSettings.getScannerTimeframe(), authHeader);
        } catch (Exception e) {
            log.error("[AtrService] fetchTodayCandles failed for {}: {}", symbol, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Fetch HTF candles for all watchlist symbols and seed HtfEmaService.
     * Fyers /data/history only supports standard resolutions (1,2,3,5,10,15,20,30,45,60,120,180,240,D),
     * so we fetch 15-min bars and locally aggregate into HTF bars using the same
     * bucket alignment as CandleAggregator (minuteOfDay / htfMin * htfMin).
     * 60 days back → ~200 HTF bars for EMA(200) convergence.
     */
    public void fetchHtfEmaForSymbols(List<String> fyersSymbols, HtfEmaService htfEmaService) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[AtrService] No access token, cannot fetch HTF EMAs");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;
        int htfTimeframe = riskSettings.getHigherTimeframe();
        // Fyers /data/history natively supports these intraday resolutions.
        // For supported values we fetch directly (no aggregation), with a longer
        // window for EMA(200) to fully converge (~3× period after SMA seed).
        // For unsupported values (e.g. 75) we fall back to fetching 15-min and aggregating.
        Set<Integer> nativeResolutions = Set.of(1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 120, 180, 240);
        boolean nativeFetch = nativeResolutions.contains(htfTimeframe);
        int baseTimeframe = nativeFetch ? htfTimeframe : 15;
        // Fyers /data/history caps intraday requests at ~100 days per call regardless of
        // resolution — going beyond returns HTTP 422. Stay under that limit.
        // 60-min × 100 days ≈ 700 bars → EMA(200) gets ~500 iterations past SMA seed (well-converged).
        // 15-min × 90 days for the aggregated 75-min path.
        int daysBack = nativeFetch && htfTimeframe >= 60 ? 100 : 90;

        if (!nativeFetch && (htfTimeframe % baseTimeframe != 0 || htfTimeframe < baseTimeframe)) {
            log.warn("[AtrService] HTF timeframe {} not a multiple of {} — cannot seed HTF EMAs",
                htfTimeframe, baseTimeframe);
            return;
        }

        // Native long-history calls (365 days of 60-min) are heavier; pace slower to stay under
        // Fyers' rate limit. Aggregated path uses smaller (90-day) responses, can go a bit faster.
        long perCallDelayMs = nativeFetch ? 700L : 350L;

        log.info("[AtrService] Fetching {}min bars for HTF({}min) EMAs — {} mode, {} symbols, {} days back, {}ms gap",
            baseTimeframe, htfTimeframe, nativeFetch ? "native" : "aggregated",
            fyersSymbols.size(), daysBack, perCallDelayMs);

        int success = 0;
        List<String> failed = new ArrayList<>();
        for (String symbol : fyersSymbols) {
            boolean rateLimited = false;
            try {
                List<CandleAggregator.CandleBar> base = fetchHistoricalCandles(symbol, baseTimeframe, authHeader, daysBack);
                List<CandleAggregator.CandleBar> htf = nativeFetch ? base : aggregateToHtf(base, htfTimeframe);
                if (htf.size() >= 20) {
                    htfEmaService.seedFromCandles(symbol, htf);
                    success++;
                } else {
                    log.warn("[AtrService] Only {} HTF bars for {} (need ≥20)", htf.size(), symbol);
                    failed.add(symbol);
                }
            } catch (Exception e) {
                String msg = e.getMessage();
                if (msg != null && msg.startsWith("HTTP 422")) {
                    log.warn("[AtrService] Skipping HTF seed for {} — {}", symbol, msg);
                } else if (msg != null && msg.startsWith("HTTP 429")) {
                    log.warn("[AtrService] Rate-limited on {} — backing off 5s", symbol);
                    failed.add(symbol);
                    rateLimited = true;
                } else {
                    log.error("[AtrService] Failed to fetch HTF EMA for {}: {}", symbol, msg);
                    failed.add(symbol);
                }
            }
            // Always pace, even on failure — back-to-back failures otherwise hammer the API.
            try { Thread.sleep(rateLimited ? 5000L : perCallDelayMs); } catch (InterruptedException ignored) {}
        }

        if (!failed.isEmpty()) {
            log.info("[AtrService] {} symbols failed first pass — retrying after 10s with 1.5s gap", failed.size());
            try { Thread.sleep(10000); } catch (InterruptedException ignored) {}
            List<String> retryList = new ArrayList<>(failed);
            for (String symbol : retryList) {
                boolean rateLimited = false;
                try {
                    List<CandleAggregator.CandleBar> base = fetchHistoricalCandles(symbol, baseTimeframe, authHeader, daysBack);
                    List<CandleAggregator.CandleBar> htf = nativeFetch ? base : aggregateToHtf(base, htfTimeframe);
                    if (htf.size() >= 20) {
                        htfEmaService.seedFromCandles(symbol, htf);
                        success++;
                    }
                } catch (Exception e) {
                    String msg = e.getMessage();
                    if (msg != null && msg.startsWith("HTTP 429")) {
                        log.warn("[AtrService] Retry rate-limited for HTF {} — backing off 8s", symbol);
                        rateLimited = true;
                    } else {
                        log.error("[AtrService] Retry failed for HTF {}: {}", symbol, msg);
                    }
                }
                try { Thread.sleep(rateLimited ? 8000L : 1500L); } catch (InterruptedException ignored) {}
            }
        }

        log.info("[AtrService] Seeded HTF EMAs for {}/{} symbols (20: {} loaded, 50: {}, 200: {})",
            success, fyersSymbols.size(),
            htfEmaService.getLoadedCount(),
            htfEmaService.getEma50LoadedCount(),
            htfEmaService.getEma200LoadedCount());
    }

    /**
     * Aggregate base-timeframe bars into HTF bars session-aligned to 9:15 (market open).
     * Matches CandleAggregator.getCandleStartMinute() exactly so seed and live HTF bars
     * share the same bucket boundaries.
     *   htf=60: 9:15, 10:15, 11:15, 12:15, 13:15, 14:15, 15:15 (15-min tail)
     *   htf=75: 9:15, 10:30, 11:45, 13:00, 14:15 (75-min tail ends at 15:30)
     */
    private List<CandleAggregator.CandleBar> aggregateToHtf(List<CandleAggregator.CandleBar> base, int htfMin) {
        if (base == null || base.isEmpty()) return Collections.emptyList();
        final long marketOpen = MarketHolidayService.MARKET_OPEN_MINUTE;
        List<CandleAggregator.CandleBar> result = new ArrayList<>();
        CandleAggregator.CandleBar current = null;
        long currentKey = Long.MIN_VALUE;

        for (CandleAggregator.CandleBar c : base) {
            if (c == null || c.open <= 0) continue;
            LocalDate date = Instant.ofEpochSecond(c.epochSec).atZone(IST).toLocalDate();
            long dayEpoch = date.atStartOfDay(IST).toEpochSecond();
            long bucketStartMin;
            if (c.startMinute < marketOpen) {
                bucketStartMin = (c.startMinute / htfMin) * htfMin;
            } else {
                long offset = c.startMinute - marketOpen;
                bucketStartMin = marketOpen + (offset / htfMin) * htfMin;
            }
            long key = dayEpoch * 10000L + bucketStartMin;

            if (current == null || key != currentKey) {
                if (current != null) result.add(current);
                current = new CandleAggregator.CandleBar();
                current.startMinute = bucketStartMin;
                current.epochSec = dayEpoch + bucketStartMin * 60L;
                current.open = c.open;
                current.high = c.high;
                current.low = c.low;
                current.close = c.close;
                current.volume = c.volume;
                currentKey = key;
            } else {
                current.high = Math.max(current.high, c.high);
                current.low = Math.min(current.low, c.low);
                current.close = c.close;
                current.volume += c.volume;
            }
        }
        if (current != null) result.add(current);
        return result;
    }

    private List<CandleAggregator.CandleBar> fetchHistoricalCandles(String symbol, int timeframeMin, String authHeader) throws Exception {
        return fetchHistoricalCandles(symbol, timeframeMin, authHeader, 14);
    }

    private List<CandleAggregator.CandleBar> fetchHistoricalCandles(String symbol, int timeframeMin, String authHeader, int daysBack) throws Exception {
        // Resolution: "15" for 15-min candles, "75" for HTF
        String resolution = String.valueOf(timeframeMin);

        // Date range: caller-configurable. 14 days default for 5-min (~750 candles).
        // HTF 75-min needs ~60 days for 200 EMA convergence (~200 bars).
        long toEpoch = Instant.now().getEpochSecond();
        long fromEpoch = toEpoch - ((long) daysBack * 24 * 3600);

        String urlStr = "https://api-t1.fyers.in/data/history?symbol=" + java.net.URLEncoder.encode(symbol, java.nio.charset.StandardCharsets.UTF_8)
            + "&resolution=" + resolution
            + "&date_format=0"
            + "&range_from=" + fromEpoch
            + "&range_to=" + toEpoch
            + "&cont_flag=1";

        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", authHeader);
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);

        int status = conn.getResponseCode();
        if (status != 200) {
            throw new IOException("HTTP " + status + " for " + symbol);
        }

        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }

        JsonNode root = mapper.readTree(sb.toString());
        JsonNode candles = root.get("candles");
        if (candles == null || !candles.isArray()) {
            return Collections.emptyList();
        }

        List<CandleAggregator.CandleBar> result = new ArrayList<>();
        for (JsonNode c : candles) {
            // [timestamp, open, high, low, close, volume]
            if (c.size() < 5) continue;
            CandleAggregator.CandleBar bar = new CandleAggregator.CandleBar();
            long epochSec = c.get(0).asLong();
            LocalTime lt = Instant.ofEpochSecond(epochSec).atZone(IST).toLocalTime();
            bar.startMinute = lt.getHour() * 60L + lt.getMinute();
            bar.epochSec = epochSec;
            bar.open = c.get(1).asDouble();
            bar.high = c.get(2).asDouble();
            bar.low = c.get(3).asDouble();
            bar.close = c.get(4).asDouble();
            if (c.size() >= 6) bar.volume = c.get(5).asLong();
            result.add(bar);
        }

        return result;
    }

    /**
     * Calculate ATR using Simple Moving Average of True Range.
     */
    private double calculateAtr(List<CandleAggregator.CandleBar> candles, int period) {
        if (candles.size() < period + 1) return 0; // need period+1 for Wilder's (first ATR is SMA)

        // First ATR = SMA of first 'period' true ranges
        double sum = 0;
        int start = candles.size() - period;
        for (int i = start; i < candles.size(); i++) {
            CandleAggregator.CandleBar prev = i > 0 ? candles.get(i - 1) : null;
            sum += candles.get(i).trueRange(prev);
        }
        double atr = sum / period;

        // Wilder's smoothing for remaining candles: ATR = ((prevATR × (period-1)) + currentTR) / period
        // Since we use the last 'period' candles, apply smoothing from start to end
        // Re-calculate using full available history for proper smoothing
        if (candles.size() > period + 1) {
            // Seed with SMA of first 'period' true ranges
            sum = 0;
            for (int i = 1; i <= period; i++) {
                sum += candles.get(i).trueRange(candles.get(i - 1));
            }
            atr = sum / period;
            // Apply Wilder's smoothing for the rest
            for (int i = period + 1; i < candles.size(); i++) {
                double tr = candles.get(i).trueRange(candles.get(i - 1));
                atr = ((atr * (period - 1)) + tr) / period;
            }
        }

        return atr;
    }

    // ── CandleCloseListener — update ATR on each new candle ──────────────────

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        // Incremental Wilder's ATR update: atr = ((prev_atr * (period-1)) + trueRange) / period
        // Requires the seed ATR to already be in place (from fetchAtrForSymbols at startup).
        // Recomputing from completedCandles is WRONG now that it's filtered to today — the
        // multi-day history required for proper Wilder smoothing is no longer available there.
        Double prev = atrBySymbol.get(fyersSymbol);
        if (prev == null || prev <= 0) return; // not seeded yet
        if (completedCandle == null) return;

        // True range needs the PREVIOUS candle's close; take it from the last completed candle
        // in the (today-only) deque. If today has only 1 candle, fall back to high-low (first TR).
        List<CandleAggregator.CandleBar> candles = candleAggregator.getCompletedCandles(fyersSymbol);
        double tr;
        if (candles.size() >= 2) {
            CandleAggregator.CandleBar prevBar = candles.get(candles.size() - 2);
            tr = completedCandle.trueRange(prevBar);
        } else {
            tr = completedCandle.high - completedCandle.low;
        }
        if (tr <= 0) return;

        int period = getAtrPeriod();
        double atr = ((prev * (period - 1)) + tr) / period;
        atrBySymbol.put(fyersSymbol, atr);
        completedCandle.atr = atr; // snapshot on the bar for historical analysis
    }

    // ── Public API ───────────────────────────────────────────────────────────

    public double getAtr(String symbol) {
        return atrBySymbol.getOrDefault(symbol, 0.0);
    }

    public int getLoadedCount() { return atrBySymbol.size(); }

    public Map<String, Double> getAllAtr() {
        return Collections.unmodifiableMap(atrBySymbol);
    }

    public boolean hasAtr(String symbol) {
        return atrBySymbol.containsKey(symbol);
    }
}
