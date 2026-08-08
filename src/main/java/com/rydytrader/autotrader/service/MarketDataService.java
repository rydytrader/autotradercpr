package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.TickData;
import com.rydytrader.autotrader.manager.PositionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Lean market data service.
 *
 * <p>Since the strip-Fyers-data refactor, this service no longer maintains a Fyers
 * WebSocket. Ticks arrive entirely from {@link #pushLtpTick} — invoked by
 * {@code GdflDataWebSocket} for options + NIFTY futures. This class serves three
 * responsibilities:
 *
 * <ol>
 *   <li>Cache the latest per-symbol tick ({@code currentTicks}) for LTP/change/VWAP
 *       lookups by strategy code, controllers, and the market-ticker REST endpoint.</li>
 *   <li>Fan out every incoming {@link LtpTick} to registered listeners
 *       (e.g. {@link CandleAggregator}).</li>
 *   <li>Broadcast tick snapshots to browser SSE clients for the top-of-page ticker.</li>
 * </ol>
 */
@Service
public class MarketDataService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataService.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService   marketHolidayService;

    // Tick state
    private final ConcurrentHashMap<String, TickData> currentTicks = new ConcurrentHashMap<>();
    /** Per-symbol last exchange dissemination timestamp (epoch seconds), used by
     *  {@link CandleAggregator} to bucket by exchange time (not local receive time). */
    private final ConcurrentHashMap<String, Long> lastExchFeedTimeSec = new ConcurrentHashMap<>();
    /** Latest exchange-dissemination timestamp (epoch seconds) observed from the
     *  alternate feed (GDFL). Populated only via {@link #pushLtpTick}. */
    private volatile long lastAltFeedExchFeedTimeSec = 0;

    /** Last exchange-dissemination timestamp (epoch seconds) observed for {@code fyersSymbol}.
     *  Returns 0 when no tick has been seen or the parser couldn't extract the timestamp. */
    public long getLastExchFeedTime(String fyersSymbol) {
        if (fyersSymbol == null) return 0;
        Long v = lastExchFeedTimeSec.get(fyersSymbol);
        return v == null ? 0 : v;
    }

    /** Latest exchange-dissemination timestamp (epoch seconds) — the best available
     *  "exchange now" reference on the server side. Used by the chart page to run its
     *  3-min bar countdown on exchange time instead of the local wall clock. Prefers
     *  the alternate feed's clock (GDFL {@code ServerTime}) when available. Returns 0
     *  if no ticks have arrived from either feed. */
    public long getLatestExchFeedTimeSec() {
        if (lastAltFeedExchFeedTimeSec > 0) return lastAltFeedExchFeedTimeSec;
        long max = 0;
        for (Long v : lastExchFeedTimeSec.values()) {
            if (v != null && v > max) max = v;
        }
        return max;
    }

    // SSE emitters
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /** Raw LTP tick — fanned out on every push from the alternate-feed source.
     *  {@code atp} is the session VWAP, 0 when the feed doesn't carry it.
     *  {@code exchFeedTimeSec} / {@code lastTradedTimeSec} may be 0 if the source
     *  couldn't extract them; consumers should fall back {@code LTT → EFT → wall-clock}. */
    public record LtpTick(String fyersSymbol, double ltp, double atp,
                          long exchFeedTimeSec, long lastTradedTimeSec) {}
    private final CopyOnWriteArrayList<java.util.function.Consumer<LtpTick>> ltpListeners = new CopyOnWriteArrayList<>();

    /** Registers a listener that receives every LTP tick. Callers must not block —
     *  listeners run inline on the feeding thread. */
    public void addLtpListener(java.util.function.Consumer<LtpTick> l) {
        if (l != null) ltpListeners.add(l);
    }

    /** Symbols owned by an alternate feed (GDFL). Retained as a no-cost inventory the
     *  GDFL side can toggle without any effect on this service's ingress — there is
     *  no Fyers-side ingress to drop anymore. */
    private final Set<String> altFeedOwnedSymbols = ConcurrentHashMap.newKeySet();

    /** Marks {@code fyersSymbol} as owned by an alternate feed. Idempotent. */
    public void addAltFeedOwnedSymbol(String fyersSymbol) {
        if (fyersSymbol != null && !fyersSymbol.isBlank()) altFeedOwnedSymbols.add(fyersSymbol);
    }

    /** Releases {@code fyersSymbol} back from alt-feed ownership. */
    public void removeAltFeedOwnedSymbol(String fyersSymbol) {
        if (fyersSymbol != null) altFeedOwnedSymbols.remove(fyersSymbol);
    }

    /** Wipes the ownership set — called on day rollover from the alternate-feed service. */
    public void clearAltFeedOwnedSymbols() {
        altFeedOwnedSymbols.clear();
    }

    /** True when {@code fyersSymbol} is currently registered as alt-feed-owned. */
    public boolean isAltFeedOwned(String fyersSymbol) {
        return fyersSymbol != null && altFeedOwnedSymbols.contains(fyersSymbol);
    }

    /** Fans an LtpTick to every registered listener and updates the local caches
     *  ({@code currentTicks}, {@code lastExchFeedTimeSec}) so downstream reads see
     *  consistent state. Invoked by alternate-feed clients (GDFL). */
    public void pushLtpTick(LtpTick evt) {
        if (evt == null || evt.ltp() <= 0) return;
        String fyersSymbol = evt.fyersSymbol();
        if (fyersSymbol == null || fyersSymbol.isBlank()) return;
        TickData tick = currentTicks.computeIfAbsent(fyersSymbol, k -> {
            TickData t = new TickData();
            t.setFyersSymbol(k);
            t.setShortName(deriveShortName(k));
            return t;
        });
        tick.setLtp(evt.ltp());
        if (evt.atp() > 0) tick.setVwap(evt.atp());
        String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
        tick.setLastTickDate(today);
        tick.recalcChange();
        if (evt.exchFeedTimeSec() > 0) {
            lastExchFeedTimeSec.put(fyersSymbol, evt.exchFeedTimeSec());
            if (evt.exchFeedTimeSec() > lastAltFeedExchFeedTimeSec) {
                lastAltFeedExchFeedTimeSec = evt.exchFeedTimeSec();
            }
        }
        dirty = true;
        for (var l : ltpListeners) {
            try { l.accept(evt); } catch (Exception ignored) {}
        }
    }

    // Scheduler + SSE state
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile boolean dirty   = false;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public synchronized void start() {
        if (running) stop();
        running = true;
        scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(this::flushSse, 500, 500, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::sendKeepalive, 15, 15, TimeUnit.SECONDS);
        log.info("[MarketData] Started (SSE scheduler only — no Fyers WS)");
    }

    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        currentTicks.clear();
        log.info("[MarketData] Stopped");
    }

    // ── SSE ───────────────────────────────────────────────────────────────────

    public void addEmitter(SseEmitter emitter) {
        emitters.add(emitter);
    }

    public void removeEmitter(SseEmitter emitter) {
        emitters.remove(emitter);
    }

    public void sendSnapshot(SseEmitter emitter) {
        try {
            List<Map<String, Object>> payload = buildTickerPayload();
            if (!payload.isEmpty()) {
                emitter.send(SseEmitter.event().name("ticker").data(mapper.writeValueAsString(payload)));
            }
        } catch (Exception e) {
            removeEmitter(emitter);
        }
    }

    private void flushSse() {
        if (!dirty || emitters.isEmpty()) return;
        dirty = false;
        List<Map<String, Object>> tickerPayload = buildTickerPayload();
        String tickerJson = null;
        if (!tickerPayload.isEmpty()) {
            try { tickerJson = mapper.writeValueAsString(tickerPayload); } catch (Exception ignored) {}
        }
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                if (tickerJson != null) emitter.send(SseEmitter.event().name("ticker").data(tickerJson));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        for (SseEmitter d : dead) {
            try { d.complete(); } catch (Exception ignored) {}
            removeEmitter(d);
        }
    }

    private List<Map<String, Object>> buildTickerPayload() {
        List<Map<String, Object>> out = new ArrayList<>();
        Set<String> posSymbols = PositionManager.getAllSymbols();
        for (TickData tick : currentTicks.values()) {
            if (tick.getLtp() <= 0) continue;
            // Hide option legs from the scrolling ticker.
            if (isOptionSymbol(tick.getFyersSymbol())) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("symbol", tick.getShortName() != null && !tick.getShortName().isEmpty()
                ? tick.getShortName() : tick.getFyersSymbol());
            m.put("fyers", tick.getFyersSymbol());
            m.put("lp",  Math.round(tick.getLtp() * 100.0) / 100.0);
            m.put("ch",  Math.round(tick.getChange() * 100.0) / 100.0);
            m.put("chp", Math.round(tick.getChangePercent() * 100.0) / 100.0);
            m.put("position", posSymbols.contains(tick.getFyersSymbol()));
            out.add(m);
        }
        return out;
    }

    private void sendKeepalive() {
        if (emitters.isEmpty()) return;
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("keepalive").data("ping"));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        for (SseEmitter d : dead) {
            try { d.complete(); } catch (Exception ignored) {}
            removeEmitter(d);
        }
    }

    // ── Subscriptions (no-ops after Fyers-data strip) ─────────────────────────

    /** No-op — retained for API compatibility with callers that used to add symbols to
     *  the Fyers WS subscription set. All tick delivery is GDFL-side now. */
    public void subscribeAdditional(Collection<String> symbols) {
        // no-op
    }

    /** No-op counterpart to {@link #subscribeAdditional}. */
    public void unsubscribeAdditional(Collection<String> symbols) {
        // no-op
    }

    private String deriveShortName(String fyersSymbol) {
        try {
            String afterColon = fyersSymbol.split(":")[1];
            return afterColon.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        } catch (Exception e) {
            return fyersSymbol;
        }
    }

    /** True when the Fyers symbol is an option leg (CE/PE on any underlying). */
    private static boolean isOptionSymbol(String fyersSymbol) {
        if (fyersSymbol == null || fyersSymbol.isEmpty()) return false;
        String upper = fyersSymbol.toUpperCase();
        if (upper.endsWith("-EQ") || upper.endsWith("-INDEX") || upper.endsWith("-MF")
            || upper.endsWith("-BE") || upper.endsWith("-BL") || upper.endsWith("-SM")
            || upper.endsWith("FUT")) return false;
        return upper.matches(".*[CP]\\d+$")
            || upper.matches(".*\\d+(CE|PE)$");
    }

    // ── Public accessors ──────────────────────────────────────────────────────

    public double getLtp(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null || tick.getLtp() <= 0) return 0;
        String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
        if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        return tick.getLtp();
    }

    /** Display-only LTP: returns the last known LTP regardless of session date. */
    public double getDisplayLtp(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return tick == null ? 0 : tick.getLtp();
    }
    public double getDisplayChange(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return tick == null ? 0 : tick.getChange();
    }
    public double getDisplayChangePct(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return tick == null ? 0 : tick.getChangePercent();
    }

    /** Session VWAP for a symbol — accumulated by the exchange from market open. */
    public double getVwap(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null || tick.getVwap() <= 0) return 0;
        String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
        if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        return tick.getVwap();
    }

    /** Seed prev-close + LTP for a symbol before any tick has arrived. */
    public void seedTickData(String fyersSymbol, double ltp, double prevClose) {
        if (fyersSymbol == null || fyersSymbol.isEmpty()) return;
        TickData tick = currentTicks.computeIfAbsent(fyersSymbol, k -> {
            TickData t = new TickData();
            t.setFyersSymbol(k);
            t.setShortName(deriveShortName(k));
            return t;
        });
        if (ltp > 0) tick.setLtp(ltp);
        if (prevClose > 0) tick.setPrevClose(prevClose);
        tick.setLastTickDate(java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString());
        tick.recalcChange();
        dirty = true;
    }

    public double getChangePercent(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null) return 0;
        boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();
        if (tradingDay) {
            String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
            if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        }
        return tick.getChangePercent();
    }

    public double getChange(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        if (tick == null) return 0;
        boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();
        if (tradingDay) {
            String today = LocalDate.now(ZoneId.of("Asia/Kolkata")).toString();
            if (tick.getLastTickDate() != null && !today.equals(tick.getLastTickDate())) return 0;
        }
        return tick.getChange();
    }

    public double getDayOpen(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getOpen() > 0) ? tick.getOpen() : 0;
    }

    public double getDayHigh(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getHigh() > 0) ? tick.getHigh() : 0;
    }

    public double getDayLow(String fyersSymbol) {
        TickData tick = currentTicks.get(fyersSymbol);
        return (tick != null && tick.getLow() > 0) ? tick.getLow() : 0;
    }

    public int     getEmitterCount() { return emitters.size(); }
    public int     getTickCount() { return currentTicks.size(); }
}
