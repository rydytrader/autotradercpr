package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Fetches NIFTY's option chain on a 15-minute cadence during market hours and exposes the
 * strike with maximum Call OI (intraday resistance) and maximum Put OI (intraday support).
 *
 * <p>Used by:
 * <ul>
 *   <li>{@link IndexTrendService} — display chips on the NIFTY card.</li>
 *   <li>{@link BreakoutScanner#checkNiftyHurdle} — extends the hurdle candidate set so the
 *       NIFTY HTF Hurdle filter can reject trades when NIFTY's prior 1h close hasn't
 *       cleared the relevant Max Call OI (buys) / Max Put OI (sells) strike.</li>
 * </ul>
 *
 * <p>Failure-tolerant — on auth / HTTP / parse errors we retain the last cached values so
 * a transient broker outage doesn't zero out the filter. Skips fetch on holidays and
 * outside market hours.
 */
@Service
public class NiftyOptionOiService {

    private static final Logger log = LoggerFactory.getLogger(NiftyOptionOiService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final int STRIKE_COUNT = 30; // ±30 strikes around ATM

    private final FyersClientRouter fyersClient;
    private final FyersProperties fyersProperties;
    private final TokenStore tokenStore;
    private final EventService eventService;
    private final MarketHolidayService marketHolidayService;

    private volatile double maxCallOiStrike = 0;
    private volatile long   maxCallOi       = 0;
    private volatile double maxPutOiStrike  = 0;
    private volatile long   maxPutOi        = 0;
    private volatile Instant lastUpdated;
    private volatile boolean firstSuccessLogged = false;

    public NiftyOptionOiService(FyersClientRouter fyersClient,
                                FyersProperties fyersProperties,
                                TokenStore tokenStore,
                                EventService eventService,
                                MarketHolidayService marketHolidayService) {
        this.fyersClient = fyersClient;
        this.fyersProperties = fyersProperties;
        this.tokenStore = tokenStore;
        this.eventService = eventService;
        this.marketHolidayService = marketHolidayService;
    }

    public double getMaxCallOiStrike() { return maxCallOiStrike; }
    public long   getMaxCallOi()       { return maxCallOi; }
    public double getMaxPutOiStrike()  { return maxPutOiStrike; }
    public long   getMaxPutOi()        { return maxPutOi; }

    public String getLastUpdatedFormatted() {
        if (lastUpdated == null) return "";
        return LocalTime.ofInstant(lastUpdated, IST).format(HHMMSS);
    }

    /**
     * Fires once after the entire Spring context is ready (TokenStore has loaded the access
     * token from disk, MarketDataService is up, etc.). Ensures a mid-session restart picks up
     * the latest OI immediately without waiting for the next 15-minute cron tick.
     *
     * <p>No market-hours gate — Fyers returns the closing OI snapshot after market close,
     * which is still useful (carries into next day's planning).
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initialFetch() {
        try { refresh(); } catch (Exception ignored) {}
    }

    /** Runs at HH:00:30, HH:15:30, HH:30:30, HH:45:30 IST during market hours, Mon-Fri. */
    @Scheduled(cron = "30 0/15 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledRefresh() {
        if (marketHolidayService != null && !marketHolidayService.isMarketOpen()) return;
        refresh();
    }

    /** Manual trigger for diagnostics / out-of-hours testing. */
    public void triggerRefresh() { refresh(); }

    private synchronized void refresh() {
        try {
            String accessToken = tokenStore.getAccessToken();
            if (accessToken == null || accessToken.isBlank()) {
                log.info("[NiftyOI] Skipping refresh — no access token yet (will retry on next 15-min cron tick)");
                return;
            }
            String authHeader = fyersProperties.getClientId() + ":" + accessToken;
            JsonNode root = fyersClient.getOptionChain(IndexTrendService.NIFTY_SYMBOL, STRIKE_COUNT, authHeader);
            if (root == null) {
                log.warn("[NiftyOI] Empty response from Fyers options-chain-v3");
                return;
            }

            // One-shot diagnostic dump of the actual response shape until the first successful
            // parse. Lets us adjust field names if Fyers' v3 schema differs from our defaults.
            if (!firstSuccessLogged) {
                String sample = root.toString();
                if (sample.length() > 1500) sample = sample.substring(0, 1500) + "…";
                log.info("[NiftyOI] First response sample: {}", sample);
            }

            // Locate the optionsChain array — defensive across response shapes.
            JsonNode chain = root.path("data").path("optionsChain");
            if (chain == null || !chain.isArray() || chain.isEmpty()) {
                chain = root.path("optionsChain"); // fallback for slightly different shape
            }
            if (chain == null || !chain.isArray() || chain.isEmpty()) {
                java.util.List<String> rootKeys = new java.util.ArrayList<>();
                root.fieldNames().forEachRemaining(rootKeys::add);
                java.util.List<String> dataKeys = new java.util.ArrayList<>();
                if (root.has("data")) root.get("data").fieldNames().forEachRemaining(dataKeys::add);
                log.warn("[NiftyOI] options-chain-v3 returned no optionsChain array; root keys: {} data keys: {}",
                    rootKeys, dataKeys);
                return;
            }

            double bestCallStrike = 0; long bestCallOi = 0;
            double bestPutStrike  = 0; long bestPutOi  = 0;
            int ceSeen = 0, peSeen = 0, skipped = 0;

            for (JsonNode row : chain) {
                String type = row.path("option_type").asText("");
                if (type.isEmpty()) type = row.path("type").asText(""); // alt key
                double strike = row.path("strike_price").asDouble(0);
                if (strike <= 0) strike = row.path("strikePrice").asDouble(0);
                long oi = row.path("oi").asLong(0);
                if (oi <= 0) oi = row.path("openInterest").asLong(0);
                if (strike <= 0 || oi <= 0) { skipped++; continue; }

                if ("CE".equalsIgnoreCase(type)) {
                    ceSeen++;
                    if (oi > bestCallOi) { bestCallOi = oi; bestCallStrike = strike; }
                } else if ("PE".equalsIgnoreCase(type)) {
                    peSeen++;
                    if (oi > bestPutOi) { bestPutOi = oi; bestPutStrike = strike; }
                }
            }

            if (bestCallOi <= 0 && bestPutOi <= 0) {
                JsonNode firstRow = chain.size() > 0 ? chain.get(0) : null;
                java.util.List<String> firstRowKeys = new java.util.ArrayList<>();
                if (firstRow != null) firstRow.fieldNames().forEachRemaining(firstRowKeys::add);
                log.warn("[NiftyOI] No CE/PE rows with positive OI parsed from {} entries (ceSeen={}, peSeen={}, skipped={}) firstRowKeys={}",
                    chain.size(), ceSeen, peSeen, skipped, firstRowKeys);
                return;
            }

            // Atomic-ish update of cached snapshot.
            this.maxCallOiStrike = bestCallStrike;
            this.maxCallOi       = bestCallOi;
            this.maxPutOiStrike  = bestPutStrike;
            this.maxPutOi        = bestPutOi;
            this.lastUpdated     = Instant.now();

            log.info("[NiftyOI] maxCall={} ({}) maxPut={} ({})",
                bestCallStrike, bestCallOi, bestPutStrike, bestPutOi);
            if (!firstSuccessLogged) {
                eventService.log("[INFO] NIFTY OI loaded: maxCall=" + (long) bestCallStrike
                    + " maxPut=" + (long) bestPutStrike);
                firstSuccessLogged = true;
            }
        } catch (Exception e) {
            log.warn("[NiftyOI] Refresh failed: {}", e.getMessage());
        }
    }
}
