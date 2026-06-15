package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.CamarillaLevels;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Computes and caches Camarilla pivot levels (H1–H6, L1–L6, PP) for NIFTY each session.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>On boot ({@code @PostConstruct}): load today's cached levels from disk if present;
 *       otherwise schedule an async fetch.</li>
 *   <li>Daily at 09:05 IST ({@code @Scheduled cron}): refresh from Fyers history.</li>
 *   <li>On any {@link #getNiftyLevels()} call when the cache is empty/stale: fire an async
 *       refresh and return the currently-cached value (possibly null) immediately.</li>
 * </ul>
 *
 * <p>Disk cache: {@code ../store/data/camarilla-nifty.json}.
 */
@Service
public class CamarillaService {

    private static final Logger log = LoggerFactory.getLogger(CamarillaService.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/data/camarilla-nifty.json";

    private final FyersClientRouter fyersClient;
    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;
    private final ObjectMapper      mapper = new ObjectMapper().findAndRegisterModules();

    private volatile CamarillaLevels cached = null;
    private final AtomicBoolean refreshInFlight = new AtomicBoolean(false);

    public CamarillaService(FyersClientRouter fyersClient,
                            TokenStore tokenStore,
                            FyersProperties fyersProperties) {
        this.fyersClient     = fyersClient;
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
    }

    @PostConstruct
    public void init() {
        loadFromDisk();
        if (cached == null || !cached.sessionDate().equals(todayIst())) {
            triggerAsyncRefresh();
        } else {
            log.info("[CamarillaService] loaded today's cached levels from disk — skipping fetch");
        }
    }

    /** Cron daily at 09:05 IST. NSE pre-open completes at 09:00 so by 09:05 the prior-day
     *  daily candle is settled. */
    @Scheduled(cron = "0 5 9 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledRefresh() {
        log.info("[CamarillaService] daily refresh fired");
        triggerAsyncRefresh();
    }

    /** Non-blocking accessor — returns cached levels (possibly null on cold start) and fires
     *  an async refresh when the cache is empty or stale. */
    public CamarillaLevels getNiftyLevels() {
        if (cached == null || !cached.sessionDate().equals(todayIst())) {
            triggerAsyncRefresh();
        }
        return cached;
    }

    private void triggerAsyncRefresh() {
        if (!refreshInFlight.compareAndSet(false, true)) return;
        CompletableFuture.runAsync(() -> {
            try { refresh(); }
            catch (Exception e) { log.warn("[CamarillaService] async refresh failed: {}", e.getMessage()); }
            finally { refreshInFlight.set(false); }
        });
    }

    /** Synchronous refresh — only called from the background task. */
    private void refresh() {
        if (!tokenStore.isTokenAvailable()) {
            log.info("[CamarillaService] skip refresh — Fyers token not available");
            return;
        }
        LocalDate today = todayIst();
        LocalDate priorDate = priorTradingDay(today);
        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();

        // Pull 10 calendar days of daily candles ending today so we always have at least one
        // settled session even after long weekends / holidays.
        LocalDate from = today.minusDays(10);
        JsonNode root;
        try { root = fyersClient.getHistory(NIFTY_SYMBOL, "D", from.toString(), today.toString(), auth); }
        catch (Exception e) {
            log.warn("[CamarillaService] history fetch failed: {}", e.getMessage());
            return;
        }
        if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
            log.warn("[CamarillaService] history response missing candles");
            return;
        }
        JsonNode candles = root.get("candles");
        // Fyers returns rows as [epochSec, open, high, low, close, volume]. Find the latest
        // row whose date is strictly before today — that's the prior trading day.
        double priorHigh = 0, priorLow = 0, priorClose = 0;
        LocalDate resolvedPrior = null;
        for (int i = candles.size() - 1; i >= 0; i--) {
            JsonNode row = candles.get(i);
            if (!row.isArray() || row.size() < 5) continue;
            long epochSec = row.get(0).asLong();
            LocalDate d = ZonedDateTime.ofInstant(java.time.Instant.ofEpochSecond(epochSec), IST).toLocalDate();
            if (!d.isBefore(today)) continue;   // skip today's row if present
            priorHigh  = row.get(2).asDouble();
            priorLow   = row.get(3).asDouble();
            priorClose = row.get(4).asDouble();
            resolvedPrior = d;
            break;
        }
        if (resolvedPrior == null || priorHigh <= 0 || priorLow <= 0 || priorClose <= 0) {
            log.warn("[CamarillaService] no usable prior-day candle in response (size={})", candles.size());
            return;
        }
        // If our calendar-walked guess differs, log it — usually means a holiday we don't track.
        if (!resolvedPrior.equals(priorDate)) {
            log.info("[CamarillaService] prior trading day resolved as {} (calendar guess was {})",
                resolvedPrior, priorDate);
        }
        CamarillaLevels fresh = CamarillaLevels.compute(today, resolvedPrior, priorHigh, priorLow, priorClose);
        cached = fresh;
        saveToDisk(fresh);
        log.info("[CamarillaService] computed pivots: PP={} H3={} H4={} H5={} L3={} L4={} L5={} (priorDate={})",
            fresh.pp(), fresh.h3(), fresh.h4(), fresh.h5(), fresh.l3(), fresh.l4(), fresh.l5(), fresh.priorDate());
    }

    private static LocalDate todayIst() {
        return LocalDate.now(IST);
    }

    /** Walk backwards skipping weekends. Doesn't know about NSE holidays — the refresh()
     *  loop above corrects this by picking the actual latest pre-today row in the history. */
    private static LocalDate priorTradingDay(LocalDate today) {
        LocalDate d = today.minusDays(1);
        while (d.getDayOfWeek().getValue() >= 6) d = d.minusDays(1);
        return d;
    }

    // ── Disk cache ────────────────────────────────────────────────────────────

    private void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            String json = Files.readString(p);
            CamarillaLevels levels = mapper.readValue(json, CamarillaLevels.class);
            if (levels != null) cached = levels;
        } catch (IOException e) {
            log.warn("[CamarillaService] failed to load disk cache: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk(CamarillaLevels levels) {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(levels));
            Files.move(tmp, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("[CamarillaService] failed to save disk cache: {}", e.getMessage());
        }
    }
}
