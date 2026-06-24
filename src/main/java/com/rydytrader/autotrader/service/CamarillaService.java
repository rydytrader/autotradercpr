package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.CamarillaLevels;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.service.strategy.BalancedAtmSelector;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-symbol Camarilla pivot level cache. Each entry is computed from a symbol's prior-day
 * daily OHLC and is valid for the current IST session.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>On boot ({@code @PostConstruct}): load today's cached level map from disk if present.</li>
 *   <li>{@link #warmUpAroundAtm(long)} — fan-out fetch for ~10 strikes per side around an ATM
 *       (≈ 42 option symbols total). Called by the strategy at boot and on daily 09:05 cron.</li>
 *   <li>{@link #getLevels(String)} — non-blocking lookup. Returns the cached entry (possibly
 *       null) and triggers an async per-symbol refresh on miss.</li>
 * </ul>
 *
 * <p>Disk cache: {@code ../store/data/camarilla-levels.json} — JSON map of symbol → levels.
 */
@Service
public class CamarillaService {

    private static final Logger log = LoggerFactory.getLogger(CamarillaService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/data/camarilla-levels.json";
    private static final long   STRIKE_STEP = 50L;
    private static final int    STRIKES_PER_SIDE = 10;
    /** NIFTY spot index symbol — used as the v2 trigger feed for setup detection.
     *  Camarilla levels for this symbol need to be ready before the first 5-min
     *  spot bar of the new session closes. Pre-fetched on boot and at 08:00 IST. */
    private static final String NIFTY_SPOT_SYMBOL = "NSE:NIFTY50-INDEX";

    private final FyersClientRouter   fyersClient;
    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final BalancedAtmSelector atmSelector;
    private final ObjectMapper        mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .findAndRegisterModules();

    private final Map<String, CamarillaLevels> bySymbol = new ConcurrentHashMap<>();
    private final Map<String, AtomicBoolean>   refreshGates = new ConcurrentHashMap<>();
    private final AtomicBoolean warmUpInFlight = new AtomicBoolean(false);

    public CamarillaService(FyersClientRouter fyersClient,
                            TokenStore tokenStore,
                            FyersProperties fyersProperties,
                            BalancedAtmSelector atmSelector) {
        this.fyersClient     = fyersClient;
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
        this.atmSelector     = atmSelector;
    }

    @PostConstruct
    public void init() {
        loadFromDisk();
        int kept = 0;
        for (Map.Entry<String, CamarillaLevels> e : bySymbol.entrySet()) {
            if (e.getValue() != null && e.getValue().sessionDate().equals(todayIst())) kept++;
        }
        log.info("[CamarillaService] booted — {} cached level entries valid for today", kept);
        // v2 — fetch NIFTY spot levels at boot so the trend tooltip + setup detector
        // are armed before the first 5-min bar closes, regardless of when the server
        // came up (post-09:30 restart still gets levels populated within ~1 second).
        // Runs async so boot isn't blocked on Fyers REST.
        triggerAsyncRefresh(NIFTY_SPOT_SYMBOL);
    }

    /** Cron daily at 08:00 IST — well before AtmTracker's 09:30 resolve. Prior-day daily
     *  candle is settled long before this point. Refreshes the NIFTY spot levels so
     *  by the time AtmChange fires at 09:30, the cache is already warm and the first
     *  trigger bar at 09:35 can evaluate immediately. */
    @Scheduled(cron = "0 0 8 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledRefresh() {
        log.info("[CamarillaService] daily refresh fired — priming NIFTY spot levels for today");
        triggerAsyncRefresh(NIFTY_SPOT_SYMBOL);
    }

    /** Returns cached levels for {@code symbol}, possibly null if not yet warmed. On miss,
     *  triggers an async per-symbol fetch — the next caller usually finds it populated. */
    public CamarillaLevels getLevels(String symbol) {
        if (symbol == null || symbol.isBlank()) return null;
        CamarillaLevels lv = bySymbol.get(symbol);
        if (lv == null || !lv.sessionDate().equals(todayIst())) {
            triggerAsyncRefresh(symbol);
            return lv;  // best-effort: return stale or null
        }
        return lv;
    }

    /** Snapshot of every cached entry (for /api/camarilla/levels). */
    public Map<String, CamarillaLevels> snapshot() {
        return Map.copyOf(bySymbol);
    }

    /** Fan-out warm-up. For each strike in [atmStrike − 10×50 … atmStrike + 10×50], resolve
     *  the CE+PE symbols via the option chain, then fetch each symbol's prior-day OHLC and
     *  compute its Camarilla levels. Runs async — non-blocking for the caller. */
    public void warmUpAroundAtm(long atmStrike) {
        if (!warmUpInFlight.compareAndSet(false, true)) return;
        CompletableFuture.runAsync(() -> {
            try { doWarmUp(atmStrike); }
            catch (Exception e) { log.warn("[CamarillaService] warm-up failed: {}", e.getMessage()); }
            finally { warmUpInFlight.set(false); }
        });
    }

    private void doWarmUp(long atmStrike) {
        if (!tokenStore.isTokenAvailable()) {
            log.info("[CamarillaService] skip warm-up — Fyers token not available");
            return;
        }
        // ONE chain fetch — walk all 21 strikes from a single response. Previously we
        // called resolveStrikeAtLevel(strike) inside a loop, which fetched the chain 21
        // times (once per strike) even though all strikes are in the same chain response.
        java.util.NavigableMap<Long, BalancedAtmSelector.ChainStrike> chain = atmSelector.fetchChainStrikes();
        if (chain.isEmpty()) {
            log.warn("[CamarillaService] warm-up skipped — chain fetch returned empty");
            return;
        }
        LocalDate today = todayIst();
        int fetched = 0, cacheHit = 0;
        for (int i = -STRIKES_PER_SIDE; i <= STRIKES_PER_SIDE; i++) {
            long strike = atmStrike + i * STRIKE_STEP;
            BalancedAtmSelector.ChainStrike row = chain.get(strike);
            if (row == null) continue;
            int[] result = maybeFetch(row.ceSymbol(), today);
            fetched  += result[0];
            cacheHit += result[1];
            result    = maybeFetch(row.peSymbol(), today);
            fetched  += result[0];
            cacheHit += result[1];
        }
        saveToDisk();
        log.info("[CamarillaService] warm-up ATM={} fetched={} cache-hit={}",
            atmStrike, fetched, cacheHit);
    }

    /** Cache-hit short-circuit. Returns {@code [fetchedCount, cacheHitCount]} so the
     *  caller can roll up counts for the warm-up log line. Skips the history REST call
     *  entirely when the symbol already has today's levels in the cache. */
    private int[] maybeFetch(String sym, LocalDate today) {
        if (sym == null || sym.isBlank()) return new int[]{0, 0};
        CamarillaLevels existing = bySymbol.get(sym);
        if (existing != null && today.equals(existing.sessionDate())) {
            return new int[]{0, 1};                          // cache hit
        }
        return new int[]{fetchAndStore(sym) ? 1 : 0, 0};     // cache miss
    }

    private void triggerAsyncRefresh(String symbol) {
        AtomicBoolean gate = refreshGates.computeIfAbsent(symbol, k -> new AtomicBoolean(false));
        if (!gate.compareAndSet(false, true)) return;
        CompletableFuture.runAsync(() -> {
            try { fetchAndStore(symbol); saveToDisk(); }
            catch (Exception e) { log.warn("[CamarillaService] async refresh failed for {}: {}", symbol, e.getMessage()); }
            finally { gate.set(false); }
        });
    }

    /** Synchronous per-symbol fetch + compute. Returns true on success. */
    private boolean fetchAndStore(String symbol) {
        if (symbol == null || symbol.isBlank() || !tokenStore.isTokenAvailable()) return false;
        LocalDate today = todayIst();
        // Cache-hit short-circuit — if today's levels are already cached, skip the REST call
        // entirely. Defends both the warm-up path AND the async on-demand refresh path so we
        // never re-fetch history for a symbol whose levels are still valid for this session.
        CamarillaLevels cached = bySymbol.get(symbol);
        if (cached != null && today.equals(cached.sessionDate())) return true;
        // Pull 10 calendar days of daily candles so we always have a settled session.
        LocalDate from = today.minusDays(10);
        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
        JsonNode root;
        try { root = fyersClient.getHistory(symbol, "D", from.toString(), today.toString(), auth); }
        catch (Exception e) {
            log.warn("[CamarillaService] history fetch failed for {}: {}", symbol, e.getMessage());
            return false;
        }
        if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
            // Edge-of-window strikes (far ITM / far OTM weekly options) frequently have
            // no historical daily candles from Fyers — illiquid or freshly listed contracts.
            // Non-actionable noise at warm-up; demoted to DEBUG.
            log.debug("[CamarillaService] history response missing candles for {}", symbol);
            return false;
        }
        JsonNode candles = root.get("candles");
        double priorHigh = 0, priorLow = 0, priorClose = 0;
        LocalDate resolvedPrior = null;
        for (int i = candles.size() - 1; i >= 0; i--) {
            JsonNode row = candles.get(i);
            if (!row.isArray() || row.size() < 5) continue;
            long epochSec = row.get(0).asLong();
            LocalDate d = ZonedDateTime.ofInstant(java.time.Instant.ofEpochSecond(epochSec), IST).toLocalDate();
            if (!d.isBefore(today)) continue;
            priorHigh  = row.get(2).asDouble();
            priorLow   = row.get(3).asDouble();
            priorClose = row.get(4).asDouble();
            resolvedPrior = d;
            break;
        }
        if (resolvedPrior == null || priorHigh <= 0 || priorLow <= 0 || priorClose <= 0) {
            // Same illiquid-strike scenario as above — Fyers returned a candles array but
            // none of the rows have usable OHLC. Demoted to DEBUG; the symbol just won't
            // get Camarilla levels and the strategy already short-circuits on null levels.
            log.debug("[CamarillaService] no usable prior-day candle for {} (size={})", symbol, candles.size());
            return false;
        }
        CamarillaLevels fresh = CamarillaLevels.compute(today, resolvedPrior, priorHigh, priorLow, priorClose);
        bySymbol.put(symbol, fresh);
        return true;
    }

    private static LocalDate todayIst() {
        return LocalDate.now(IST);
    }

    // ── Disk cache ────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            String json = Files.readString(p);
            Map<String, CamarillaLevels> map = mapper.readValue(json,
                mapper.getTypeFactory().constructMapType(java.util.LinkedHashMap.class,
                    String.class, CamarillaLevels.class));
            if (map != null) {
                bySymbol.clear();
                bySymbol.putAll(map);
            }
        } catch (IOException e) {
            log.warn("[CamarillaService] failed to load disk cache: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(bySymbol));
            Files.move(tmp, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("[CamarillaService] failed to save disk cache: {}", e.getMessage());
        }
    }
}
