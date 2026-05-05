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
    private SmaService smaService;
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

    /** Called by SmaService.loadCache() to rehydrate ATR values alongside SMA state. */
    public void primeFromCache(String symbol, double atr) {
        if (atr > 0) atrBySymbol.put(symbol, atr);
    }

    /**
     * Fetch ATR for all watchlist symbols. Called at 9:00 AM or on restart.
     * When SmaService has pre-loaded state from its disk cache, cached symbols
     * route through a 1-day catch-up fetch instead of the 14-day full seed.
     */
    public void fetchAtrForSymbols(List<String> fyersSymbols) {
        fetchAtrForSymbols(fyersSymbols, false);
    }

    /**
     * @param forceFetch When true, the catch-up path's "skip when cache is current" optimization
     *                   is bypassed and every symbol re-fetches + re-seeds. Used by the 9:25
     *                   opening refresh which must overwrite live-tick-built morning bars even
     *                   when the cache claims to already have them.
     */
    public void fetchAtrForSymbols(List<String> fyersSymbols, boolean forceFetch) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[AtrService] No access token, cannot fetch ATR");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;
        int timeframe = riskSettings.getScannerTimeframe();

        if (smaService != null && smaService.isSeededFromCache()) {
            List<String> fullFetch = new ArrayList<>();
            List<String> catchUp   = new ArrayList<>();
            for (String s : fyersSymbols) {
                if (smaService.hasCachedSymbol(s) && atrBySymbol.getOrDefault(s, 0.0) > 0) catchUp.add(s);
                else fullFetch.add(s);
            }
            log.info("[CACHE] ATR/SMA: catch-up for {} cached symbols, full fetch for {} new symbols{}",
                catchUp.size(), fullFetch.size(), forceFetch ? " [FORCE]" : "");
            doCatchUpFetch(catchUp, authHeader, timeframe, forceFetch);
            if (!fullFetch.isEmpty()) doFullFetch(fullFetch, authHeader, timeframe);
            return;
        }

        doFullFetch(fyersSymbols, authHeader, timeframe);
    }

    /**
     * Full seed flow for symbols without an SMA cache entry. Uses live-aggregated bars from
     * CandleAggregator's loaded disk state when available (matches TradingView exactly), and
     * tops up from Fyers /data/history only for the gap before what we have on disk. This
     * keeps today's bars in the SMA deque sourced from WebSocket aggregation, not from Fyers'
     * historical OHLC which carries small per-bar snapshot drift on indices.
     */
    private void doFullFetch(List<String> fyersSymbols, String authHeader, int timeframe) {
        log.info("[AtrService] Full fetch ATR({}) for {} symbols ({}min candles)",
            getAtrPeriod(), fyersSymbols.size(), timeframe);

        int success = 0;
        int diskOnly = 0;
        List<String> failed = new ArrayList<>();
        for (String symbol : fyersSymbols) {
            try {
                // Build candle list: disk priors + today's bars first (oldest → newest), then
                // Fyers /data/history filling in older bars only if disk is too thin.
                List<CandleAggregator.CandleBar> diskBars = new ArrayList<>();
                diskBars.addAll(candleAggregator.getPriorDayCandles(symbol));
                diskBars.addAll(candleAggregator.getCompletedCandles(symbol));
                diskBars.sort((a, b) -> Long.compare(a.epochSec, b.epochSec));

                List<CandleAggregator.CandleBar> candles;
                boolean fyersFetched = false;
                // Disk alone covers SMA200 + ATR(14) requirement → skip Fyers fetch entirely.
                if (diskBars.size() >= 200) {
                    candles = diskBars;
                } else {
                    // Fetch from Fyers for the missing older window. Merge: keep disk bars as
                    // authoritative; Fyers fills in only what's older than the oldest disk bar.
                    List<CandleAggregator.CandleBar> fyersBars = fetchHistoricalCandles(symbol, timeframe, authHeader);
                    fyersFetched = true;
                    long oldestDiskEpoch = diskBars.isEmpty() ? Long.MAX_VALUE : diskBars.get(0).epochSec;
                    candles = new ArrayList<>();
                    for (CandleAggregator.CandleBar c : fyersBars) {
                        if (c.epochSec < oldestDiskEpoch) candles.add(c);
                    }
                    candles.addAll(diskBars);  // disk bars stay authoritative for the recent window
                }

                if (candles.size() >= getAtrPeriod()) {
                    double atr = calculateAtr(candles, getAtrPeriod());
                    atrBySymbol.put(symbol, atr);
                    // Only call seedCandles when we just fetched from Fyers; otherwise the
                    // CandleAggregator's loaded disk state already represents the truth.
                    if (fyersFetched) candleAggregator.seedCandles(symbol, candles);
                    if (smaService != null) smaService.seedFromCandles(symbol, candles);
                    success++;
                    if (!fyersFetched) diskOnly++;
                } else {
                    log.warn("[AtrService] Only {} candles for {} (need {})", candles.size(), symbol, getAtrPeriod());
                    failed.add(symbol);
                }
                if (fyersFetched) Thread.sleep(300);
            } catch (Exception e) {
                log.error("[AtrService] Failed to fetch ATR for {}: {}", symbol, e.getMessage());
                failed.add(symbol);
            }
        }
        log.info("[AtrService] doFullFetch: {} disk-only, {} needed Fyers", diskOnly, success - diskOnly);

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
                        if (smaService != null) smaService.seedFromCandles(symbol, candles);
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
        eventService.log("[INFO] ATR + SMA loaded for " + success + "/" + fyersSymbols.size() + " symbols ("
            + riskSettings.getScannerTimeframe() + "min candles)");

        // Persist the fresh seed so the next restart (even before the first candle close)
        // can reload from disk instead of re-fetching 14 days of history.
        if (smaService != null) smaService.flushCache();
    }

    /**
     * Catch-up fetch for cache-seeded symbols. Pulls 5 days so:
     *   1. CandleAggregator.priorDayCandles gets populated / refreshed — the priors disk
     *      cache may be missing, empty, or from a stale day, so we rebuild the 20-bar
     *      volume baseline from a multi-day window.
     *   2. Bars closed between cache-save and restart get replayed through
     *      {@link SmaService#onCandleClose} / {@link #onCandleClose} so SMA/ATR stay incremental.
     * The epochSec ≤ lastCandleEpoch guard prevents re-applying bars already baked into the
     * SMA cache. Does NOT call seedFromCandles — that resets SMA and wipes the cache.
     */
    private void doCatchUpFetch(List<String> symbols, String authHeader, int timeframe, boolean forceFetch) {
        if (symbols.isEmpty()) return;
        // Skip the REST call when the cached lastCandleEpoch already covers the latest possible
        // completed bar — except when forceFetch is set (9:25 opening refresh path), which must
        // overwrite even-when-current to correct corrupt live-tick-built morning bars.
        List<String> needsFetch;
        if (forceFetch) {
            needsFetch = new ArrayList<>(symbols);
        } else {
            long latestPossibleStart = latestPossibleBarStart(timeframe);
            needsFetch = new ArrayList<>();
            for (String s : symbols) {
                long lastEpoch = smaService != null ? smaService.getLastCandleEpoch(s) : 0;
                if (lastEpoch < latestPossibleStart) needsFetch.add(s);
            }
        }
        int upToDate = symbols.size() - needsFetch.size();
        if (needsFetch.isEmpty()) {
            log.info("[CACHE] ATR/SMA catch-up: all {} symbols already current — no fetch needed", upToDate);
            eventService.log("[INFO] ATR/SMA restored from cache; all " + upToDate + " symbols already current");
            return;
        }
        log.info("[CACHE] ATR/SMA catch-up: {} symbols already current, catching up for {} {}",
            upToDate, needsFetch.size(), forceFetch ? "(FORCE: re-seeding all)" : "stale");

        long latestPossibleStart = latestPossibleBarStart(timeframe);
        int candleCountFromDisk = 0;
        int candleCountFromFyers = 0;
        int symbolsFromDiskOnly = 0;
        int symbolsNeededFyers = 0;

        for (String symbol : needsFetch) {
            try {
                long lastEpoch = smaService != null ? smaService.getLastCandleEpoch(symbol) : 0;

                // STEP 1 — Try to fill the gap from CandleAggregator's loaded disk state. These
                // bars are LIVE-aggregated (built from WebSocket ticks captured during prior
                // sessions) and match TradingView exactly — unlike Fyers /data/history which
                // produces small per-bar drift on indices due to vendor snapshot timing. We use
                // disk bars whenever they cover the gap so the SMA deque stays clean of vendor
                // drift.
                List<CandleAggregator.CandleBar> diskBars = new ArrayList<>();
                diskBars.addAll(candleAggregator.getPriorDayCandles(symbol));
                diskBars.addAll(candleAggregator.getCompletedCandles(symbol));
                diskBars.sort((a, b) -> Long.compare(a.epochSec, b.epochSec));

                int appliedFromDisk = 0;
                for (CandleAggregator.CandleBar c : diskBars) {
                    if (c.epochSec <= lastEpoch) continue;  // already in deque
                    if (smaService != null) smaService.onCandleClose(symbol, c);
                    this.onCandleClose(symbol, c);
                    appliedFromDisk++;
                }
                candleCountFromDisk += appliedFromDisk;

                long newLastEpoch = smaService != null ? smaService.getLastCandleEpoch(symbol) : 0;

                // STEP 2 — If disk bars closed the gap, we're done. Otherwise (bot was offline
                // beyond what we have on disk), fall back to Fyers /data/history for the remaining
                // bars. This is the legitimate case: long offline, new symbol, first-ever startup.
                if (!forceFetch && newLastEpoch >= latestPossibleStart) {
                    symbolsFromDiskOnly++;
                    continue;
                }

                List<CandleAggregator.CandleBar> bars = fetchHistoricalCandles(symbol, timeframe, authHeader, 5);
                if (forceFetch) candleAggregator.seedCandles(symbol, bars);
                int applied = 0;
                for (CandleAggregator.CandleBar c : bars) {
                    if (c.epochSec <= newLastEpoch) continue;  // already covered by disk + prior epoch
                    if (smaService != null) smaService.onCandleClose(symbol, c);
                    this.onCandleClose(symbol, c);
                    applied++;
                }
                candleCountFromFyers += applied;
                symbolsNeededFyers++;
                Thread.sleep(150);
            } catch (Exception e) {
                log.warn("[CACHE] Catch-up failed for {}: {}", symbol, e.getMessage());
            }
        }
        log.info("[CACHE] Catch-up applied {} bars from disk ({} symbols), {} bars from Fyers ({} symbols)",
            candleCountFromDisk, symbolsFromDiskOnly, candleCountFromFyers, symbolsNeededFyers);
        eventService.log("[INFO] ATR/SMA catch-up: " + candleCountFromDisk + " bars from live cache + "
            + candleCountFromFyers + " bars from Fyers (" + symbolsFromDiskOnly + " disk-only, "
            + symbolsNeededFyers + " needed Fyers)");
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
     * Fetch HTF candles for all watchlist symbols and seed HtfSmaService.
     * Fyers /data/history only supports standard resolutions (1,2,3,5,10,15,20,30,45,60,120,180,240,D),
     * so we fetch 15-min bars and locally aggregate into HTF bars using the same
     * bucket alignment as CandleAggregator (minuteOfDay / htfMin * htfMin).
     * 60 days back → ~200 HTF bars for SMA(200) coverage.
     */
    public void fetchHtfSmaForSymbols(List<String> fyersSymbols, HtfSmaService htfSmaService) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[AtrService] No access token, cannot fetch HTF SMAs");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;
        int htfTimeframe = riskSettings.getHigherTimeframe();
        // Fyers /data/history natively supports these intraday resolutions.
        Set<Integer> nativeResolutions = Set.of(1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 120, 180, 240);
        boolean nativeFetch = nativeResolutions.contains(htfTimeframe);
        int baseTimeframe = nativeFetch ? htfTimeframe : 15;
        // Fyers /data/history caps intraday requests at ~100 days per call regardless of
        // resolution — going beyond returns HTTP 422. Stay under that limit.
        // 60-min × 100 days ≈ 700 bars → SMA(200) has a 200-close window plus 500 extra for pattern history.
        // 15-min × 90 days for the aggregated path.
        int daysBack = nativeFetch && htfTimeframe >= 60 ? 100 : 90;

        if (!nativeFetch && (htfTimeframe % baseTimeframe != 0 || htfTimeframe < baseTimeframe)) {
            log.warn("[AtrService] HTF timeframe {} not a multiple of {} — cannot seed HTF SMAs",
                htfTimeframe, baseTimeframe);
            return;
        }

        // If HtfSmaService was seeded from its disk cache, split the work: catch-up for
        // cached symbols (1-day fetch + incremental onCandleClose replay), full 100-day
        // fetch only for symbols new to the watchlist.
        if (htfSmaService.isSeededFromCache()) {
            List<String> fullFetch = new ArrayList<>();
            List<String> catchUp   = new ArrayList<>();
            for (String s : fyersSymbols) {
                if (htfSmaService.hasCachedSymbol(s)) catchUp.add(s);
                else fullFetch.add(s);
            }
            log.info("[CACHE] HTF SMA: catch-up for {} cached, full fetch for {} new",
                catchUp.size(), fullFetch.size());
            doHtfCatchUpFetch(catchUp, htfSmaService, authHeader, baseTimeframe, htfTimeframe, nativeFetch);
            if (fullFetch.isEmpty()) return;
            fyersSymbols = fullFetch;  // fall through: full fetch for only the new symbols
        }

        // Native long-history calls (100 days of 60-min) are heavier; pace slower to stay under
        // Fyers' rate limit. Aggregated path uses smaller (90-day) responses, can go a bit faster.
        long perCallDelayMs = nativeFetch ? 700L : 350L;

        log.info("[AtrService] Fetching {}min bars for HTF({}min) SMAs — {} mode, {} symbols, {} days back, {}ms gap",
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
                    htfSmaService.seedFromCandles(symbol, htf);
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
                    log.error("[AtrService] Failed to fetch HTF SMA for {}: {}", symbol, msg);
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
                        htfSmaService.seedFromCandles(symbol, htf);
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

        log.info("[AtrService] Seeded HTF SMAs for {}/{} symbols (20: {} loaded, 50: {}, 200: {})",
            success, fyersSymbols.size(),
            htfSmaService.getLoadedCount(),
            htfSmaService.getSma50LoadedCount(),
            htfSmaService.getSma200LoadedCount());

        // Persist fresh HTF seed so the next restart reloads from disk.
        htfSmaService.flushCache();
    }

    /**
     * Compute the start epoch of the latest possible COMPLETED bar at this moment, bounded
     * by today's market hours. Used by both catch-up paths to decide whether a REST fetch
     * could possibly yield anything new.
     *
     * Session-aligned: bar buckets start at MARKET_OPEN (9:15) and step by timeframeMinutes.
     * Returns -1 if no complete bar exists yet today (pre-9:15 + bucketSec, or pre-market
     * with no good fallback to previous trading day).
     */
    private long latestPossibleBarStart(int timeframeMinutes) {
        long bucketSec = timeframeMinutes * 60L;
        java.time.LocalDate today = java.time.LocalDate.now(IST);
        long marketOpenSec  = today.atTime(MarketHolidayService.MARKET_OPEN_MINUTE / 60,
                                           MarketHolidayService.MARKET_OPEN_MINUTE % 60).atZone(IST).toEpochSecond();
        long marketCloseSec = today.atTime(MarketHolidayService.MARKET_CLOSE_MINUTE / 60,
                                           MarketHolidayService.MARKET_CLOSE_MINUTE % 60).atZone(IST).toEpochSecond();
        long nowSec = java.time.Instant.now().getEpochSecond();

        // Effective time = clamp(now, marketOpen, marketClose) — past close means no new bars
        long effectiveSec = Math.min(Math.max(nowSec, marketOpenSec), marketCloseSec);
        long elapsed = effectiveSec - marketOpenSec;
        long completedBuckets = elapsed / bucketSec;
        if (completedBuckets <= 0) return -1L;
        return marketOpenSec + (completedBuckets - 1) * bucketSec;
    }

    /**
     * HTF catch-up fetch: pull 5 days of HTF bars and replay any newer than the cached
     * lastCandleEpoch through HtfSmaService.onCandleClose. 5-day window ensures recovery
     * across weekends / long gaps even if the htfAggregator's completedCandles is empty.
     */
    private void doHtfCatchUpFetch(List<String> symbols, HtfSmaService htfSmaService, String authHeader,
                                   int baseTimeframe, int htfTimeframe, boolean nativeFetch) {
        if (symbols.isEmpty()) return;
        // Skip the REST call when the cached lastCandleEpoch already covers the latest possible
        // completed bar given current time + market hours. After 15:30, no new bars form today
        // so anyone with the last session bar is current — no need to ping Fyers. Mid-session,
        // anyone with the most recently closed bar is current.
        long latestPossibleStart = latestPossibleBarStart(htfTimeframe);
        List<String> needsFetch = new ArrayList<>();
        for (String s : symbols) {
            long lastEpoch = htfSmaService.getLastCandleEpoch(s);
            if (lastEpoch < latestPossibleStart) needsFetch.add(s);
        }
        int upToDate = symbols.size() - needsFetch.size();
        if (needsFetch.isEmpty()) {
            log.info("[CACHE] HTF catch-up: all {} symbols already current — no fetch needed", upToDate);
            return;
        }
        log.info("[CACHE] HTF catch-up: {} symbols already current, fetching for {} stale", upToDate, needsFetch.size());
        int candleCount = 0;
        for (String symbol : needsFetch) {
            try {
                List<CandleAggregator.CandleBar> base = fetchHistoricalCandles(symbol, baseTimeframe, authHeader, 5);
                List<CandleAggregator.CandleBar> htf  = nativeFetch ? base : aggregateToHtf(base, htfTimeframe);
                long lastEpoch = htfSmaService.getLastCandleEpoch(symbol);
                for (CandleAggregator.CandleBar c : htf) {
                    if (c.epochSec <= lastEpoch) continue;
                    htfSmaService.onCandleClose(symbol, c);
                    candleCount++;
                }
                Thread.sleep(200);
            } catch (Exception e) {
                log.warn("[CACHE] HTF catch-up failed for {}: {}", symbol, e.getMessage());
            }
        }
        log.info("[CACHE] HTF catch-up applied {} bars across {} symbols", candleCount, needsFetch.size());
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
        // HTF 75-min needs ~60 days for 200 SMA to have a full window (~200 bars).
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

    /** Count of symbols in the given list that have a non-zero ATR. Used for dashboard stats. */
    public int getLoadedCountFor(java.util.Collection<String> symbols) {
        int n = 0;
        for (String s : symbols) if (atrBySymbol.getOrDefault(s, 0.0) > 0) n++;
        return n;
    }

    /**
     * Drop any ATR entry whose symbol isn't in the current watchlist. Keeps the cache
     * proportional to today's tradable universe instead of growing cumulatively across days.
     */
    public int pruneTo(java.util.Collection<String> watchlist) {
        java.util.Set<String> keep = new java.util.HashSet<>(watchlist);
        int before = atrBySymbol.size();
        atrBySymbol.keySet().retainAll(keep);
        int removed = before - atrBySymbol.size();
        if (removed > 0) log.info("[AtrService] Pruned {} stale ATR entries not in watchlist ({} remaining)", removed, atrBySymbol.size());
        return removed;
    }

    public Map<String, Double> getAllAtr() {
        return Collections.unmodifiableMap(atrBySymbol);
    }

    public boolean hasAtr(String symbol) {
        return atrBySymbol.containsKey(symbol);
    }
}
