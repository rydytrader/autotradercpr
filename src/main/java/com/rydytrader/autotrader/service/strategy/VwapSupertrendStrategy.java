package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.fyers.FyersClient;
import com.rydytrader.autotrader.indicator.SuperTrend;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.util.NiftyExpiryResolver;
import com.rydytrader.autotrader.util.NiftyOptionSymbolBuilder;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * VWAP + Supertrend options-buying strategy.
 *
 * <p>On the FIRST NIFTY spot tick each morning (≥ 09:15:00 IST), captures
 * {@code spotOpen}, computes ATM = round(spotOpen / 50) × 50, subscribes ±N
 * strikes for both CE and PE via {@code MarketDataService.subscribeAdditional}.
 * After a 15 s warm-up, picks the CE and PE nearest to the configured target
 * premium (default ₹250) as the tracked pair.
 *
 * <p>Historical bars for both chosen symbols are pulled via
 * {@code FyersClient.getHistory} REST to prime Supertrend so its output is
 * valid from BAR 1 of today's session.
 *
 * <p>On every N-min bar close for either chosen symbol (default 3-min): enters
 * a MARKET buy when the candle is a VWAP-bounce green bar
 * ({@code low ≤ VWAP AND close > VWAP AND close > open}) AND Supertrend is up.
 * SL = the entry bar's low. Exit trail = Supertrend flip. Unlimited re-entries
 * (each fresh VWAP-bounce green bar re-arms). CE and PE tracked independently;
 * both may be open at the same time.
 */
@Service
public class VwapSupertrendStrategy implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(VwapSupertrendStrategy.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HHMM = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    /** NIFTY spot symbol on Fyers — for the market-open tick. */
    private static final String NIFTY_SPOT_SYM = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_INTERVAL = 50L;
    /** NIFTY lot size — matches OptionBuying constant. Fyers changes this once
     *  per year; hardcoded for now. */
    private static final int    LOT_SIZE = 65;
    /** Delay between subscribing ±N strikes and picking the nearest-to-target
     *  pair. Gives Fyers enough time to deliver first LTPs on every subscribed
     *  strike. */
    private static final long   PAIR_PICK_DELAY_MS = 15_000L;

    private final CandleAggregator    candleAggregator;
    private final MarketDataService   marketDataService;
    private final OrderService        orderService;
    private final EventService        eventService;
    private final RiskSettingsStore   riskSettings;
    private final FyersClient         fyersClient;
    private final TokenStore          tokenStore;
    private final FyersProperties     fyersProperties;
    private final MarketHolidayService holidays;
    private final ObjectProvider<OrderEventService> orderEventServiceProvider;

    enum FsmState { BOOT, STRIKES_SUBSCRIBING, ARMED, DONE_FOR_DAY }
    enum LegState { WAITING, PENDING_ENTRY, IN_POSITION }

    /** Per-leg state — one instance for CE, one for PE. Guarded by the
     *  enclosing {@code VwapSupertrendStrategy}'s intrinsic monitor. */
    private static class Leg {
        volatile String   chosenSymbol;
        volatile LegState state = LegState.WAITING;
        volatile String   entryOrderId;
        volatile double   fillPrice;
        volatile double   slPrice;
        volatile int      qty;
        volatile long     entryBarStartMs;
        void reset() {
            state = LegState.WAITING;
            entryOrderId = null;
            fillPrice = 0;
            slPrice = 0;
            qty = 0;
            entryBarStartMs = 0;
        }
    }

    private final Leg ceLeg = new Leg();
    private final Leg peLeg = new Leg();

    private volatile FsmState fsm = FsmState.BOOT;
    private volatile double spotOpen = 0;
    private volatile long   atmStrike = 0;
    private volatile long   strikesSubscribedAtMs = 0;
    private volatile String todayKey = "";
    /** Total closed trades today for liveNetPnlToday accumulation. */
    private final AtomicReference<Double> realisedPnlToday = new AtomicReference<>(0.0);
    private final Map<Long, ClosedTrade> tradesTodayById = new ConcurrentHashMap<>();

    private record ClosedTrade(String side, String symbol, double entry, double exit,
                                int qty, long closedMs, String reason) {}

    public VwapSupertrendStrategy(CandleAggregator candleAggregator,
                                   MarketDataService marketDataService,
                                   OrderService orderService,
                                   EventService eventService,
                                   RiskSettingsStore riskSettings,
                                   FyersClient fyersClient,
                                   TokenStore tokenStore,
                                   FyersProperties fyersProperties,
                                   MarketHolidayService holidays,
                                   ObjectProvider<OrderEventService> orderEventServiceProvider) {
        this.candleAggregator          = candleAggregator;
        this.marketDataService         = marketDataService;
        this.orderService              = orderService;
        this.eventService              = eventService;
        this.riskSettings              = riskSettings;
        this.fyersClient               = fyersClient;
        this.tokenStore                = tokenStore;
        this.fyersProperties           = fyersProperties;
        this.holidays                  = holidays;
        this.orderEventServiceProvider = orderEventServiceProvider;
    }

    @PostConstruct
    public void boot() {
        todayKey = LocalDate.now(IST).toString();
        // NIFTY spot tick — needed to detect the market-open price for ATM anchor.
        marketDataService.subscribeAdditional(Collections.singletonList(NIFTY_SPOT_SYM));
        marketDataService.addLtpListener(this::onTick);
        OrderEventService oes = orderEventServiceProvider.getIfAvailable();
        if (oes != null) oes.addFillListener(this::onOrderFill);
        log.info("[VwapSupertrend] booted — waiting for first {} tick ≥ 09:15 IST", NIFTY_SPOT_SYM);
    }

    // ── LTP tick path ───────────────────────────────────────────────────────

    void onTick(MarketDataService.LtpTick t) {
        if (t == null) return;
        String sym = t.fyersSymbol();
        if (sym == null) return;

        // Spot-open capture — arms the strategy for today.
        if (fsm == FsmState.BOOT && NIFTY_SPOT_SYM.equals(sym) && t.ltp() > 0) {
            LocalTime nowIst = ZonedDateTime.now(IST).toLocalTime();
            if (nowIst.isBefore(LocalTime.of(9, 15))) return;
            captureSpotOpenAndSubscribeStrikes(t.ltp());
            return;
        }

        // Per-leg LTP-based SL check.
        if (fsm == FsmState.ARMED) {
            checkSlIfOpen(ceLeg, sym, t.ltp(), "CE");
            checkSlIfOpen(peLeg, sym, t.ltp(), "PE");
        }
    }

    private synchronized void checkSlIfOpen(Leg leg, String sym, double ltp, String sideLabel) {
        if (leg.state != LegState.IN_POSITION) return;
        if (leg.chosenSymbol == null || !leg.chosenSymbol.equals(sym)) return;
        if (leg.slPrice <= 0 || ltp <= 0) return;
        if (ltp <= leg.slPrice) {
            fireExit(leg, sideLabel, "SL_HIT_CANDLE_LOW",
                "LTP " + fmt(ltp) + " ≤ SL " + fmt(leg.slPrice));
        }
    }

    // ── Spot-open + strike subscription ─────────────────────────────────────

    private synchronized void captureSpotOpenAndSubscribeStrikes(double openTickLtp) {
        if (fsm != FsmState.BOOT) return;
        spotOpen  = openTickLtp;
        atmStrike = Math.round(spotOpen / (double) STRIKE_INTERVAL) * STRIKE_INTERVAL;
        int range = Math.max(1, riskSettings.getVwapStStrikesRange());

        LocalDate today = LocalDate.now(IST);
        LocalDate expiry = NiftyExpiryResolver.currentWeeklyExpiry(today, holidays);

        List<String> allSymbols = new ArrayList<>(range * 2 * 2);
        for (int i = -range; i <= range; i++) {
            long strike = atmStrike + i * STRIKE_INTERVAL;
            if (strike <= 0) continue;
            allSymbols.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "CE"));
            allSymbols.add(NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "PE"));
        }
        marketDataService.subscribeAdditional(allSymbols);
        strikesSubscribedAtMs = System.currentTimeMillis();
        fsm = FsmState.STRIKES_SUBSCRIBING;
        event("[INFO]", "VwapST",
            "Spot open captured — spotOpen=" + fmt(spotOpen)
                + " atmStrike=" + atmStrike
                + " expiry=" + expiry
                + " subscribed " + allSymbols.size() + " strikes (±" + range + ")");
    }

    // ── Scheduler tick — pair pick + squareoff cutoff ──────────────────────

    @Override
    public void tick() {
        if (!riskSettings.isVwapStEnabled()) return;
        String today = LocalDate.now(IST).toString();
        if (!today.equals(todayKey)) rolloverIfNewDay(today);

        if (fsm == FsmState.STRIKES_SUBSCRIBING
                && strikesSubscribedAtMs > 0
                && System.currentTimeMillis() - strikesSubscribedAtMs >= PAIR_PICK_DELAY_MS) {
            pickPairAndWarmup();
        }

        if (fsm == FsmState.ARMED) {
            String squareoffTime = riskSettings.getVwapStSquareOffTime();
            if (squareoffTime != null && !squareoffTime.isBlank()) {
                LocalTime cutoff = LocalTime.parse(squareoffTime);
                if (ZonedDateTime.now(IST).toLocalTime().isAfter(cutoff)) {
                    forceClose("SQUAREOFF");
                }
            }
        }
    }

    // ── Pair pick + history warmup ──────────────────────────────────────────

    private synchronized void pickPairAndWarmup() {
        if (fsm != FsmState.STRIKES_SUBSCRIBING) return;
        int range = Math.max(1, riskSettings.getVwapStStrikesRange());
        double target = Math.max(1.0, riskSettings.getVwapStTargetPremium());
        LocalDate today = LocalDate.now(IST);
        LocalDate expiry = NiftyExpiryResolver.currentWeeklyExpiry(today, holidays);

        String bestCe = null, bestPe = null;
        double bestCeDiff = Double.MAX_VALUE, bestPeDiff = Double.MAX_VALUE;
        double bestCeLtp = 0, bestPeLtp = 0;
        for (int i = -range; i <= range; i++) {
            long strike = atmStrike + i * STRIKE_INTERVAL;
            if (strike <= 0) continue;
            String ceSym = NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "CE");
            String peSym = NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, strike, "PE");
            double ceLtp = marketDataService.getLtp(ceSym);
            double peLtp = marketDataService.getLtp(peSym);
            if (ceLtp > 0 && Math.abs(ceLtp - target) < bestCeDiff) {
                bestCeDiff = Math.abs(ceLtp - target);
                bestCe = ceSym;
                bestCeLtp = ceLtp;
            }
            if (peLtp > 0 && Math.abs(peLtp - target) < bestPeDiff) {
                bestPeDiff = Math.abs(peLtp - target);
                bestPe = peSym;
                bestPeLtp = peLtp;
            }
        }
        if (bestCe == null || bestPe == null) {
            event("[WARNING]", "VwapST",
                "Pair pick FAILED — no LTPs yet (bestCe=" + bestCe + " bestPe=" + bestPe + "). Will retry next tick.");
            strikesSubscribedAtMs = System.currentTimeMillis(); // reset wait
            return;
        }
        ceLeg.chosenSymbol = bestCe;
        peLeg.chosenSymbol = bestPe;
        event("[SUCCESS]", "VwapST",
            "Chosen pair — CE=" + bestCe + " (ltp=" + fmt(bestCeLtp) + ")"
                + "  PE=" + bestPe + " (ltp=" + fmt(bestPeLtp) + ")"
                + "  target=" + fmt(target));

        // Wire candle-close listeners on chosen symbols.
        candleAggregator.subscribe(bestCe, c -> onBarClose(ceLeg, "CE", c));
        candleAggregator.subscribe(bestPe, c -> onBarClose(peLeg, "PE", c));

        // History warmup — pull 1-min bars for the past 3 days so Supertrend is
        // valid from BAR 1 of today's session. Failure on a leg logs and continues.
        warmupHistory(bestCe, "CE");
        warmupHistory(bestPe, "PE");

        fsm = FsmState.ARMED;
        event("[INFO]", "VwapST", "ARMED — monitoring 3-min bars on chosen CE and PE");
    }

    private void warmupHistory(String sym, String sideLabel) {
        try {
            LocalDate to   = LocalDate.now(IST);
            LocalDate from = to.minusDays(3);
            JsonNode resp = fyersClient.getHistory(sym, "1", from.format(ISO_DATE), to.format(ISO_DATE), authHeader());
            JsonNode candles = resp == null ? null : resp.path("candles");
            if (candles == null || !candles.isArray() || candles.size() == 0) {
                event("[WARNING]", "VwapST",
                    sideLabel + " history warmup returned no bars for " + sym
                        + " — Supertrend will be UNAVAILABLE for the first ~33 min of today's session");
                return;
            }
            List<Candle> bars = new ArrayList<>(candles.size());
            for (JsonNode row : candles) {
                if (!row.isArray() || row.size() < 6) continue;
                long epochSec = row.get(0).asLong(0);
                double o = row.get(1).asDouble(0);
                double h = row.get(2).asDouble(0);
                double l = row.get(3).asDouble(0);
                double c = row.get(4).asDouble(0);
                long   v = row.get(5).asLong(0);
                if (epochSec <= 0 || o <= 0) continue;
                bars.add(new Candle(o, h, l, c, v, epochSec * 1000L, 0.0));
            }
            candleAggregator.prependHistory(sym, bars);
            event("[INFO]", "VwapST",
                sideLabel + " history warmup — prepended " + bars.size() + " 1-min bars for " + sym);
        } catch (Exception e) {
            event("[WARNING]", "VwapST",
                sideLabel + " history warmup FAILED for " + sym + " — " + e.getMessage());
        }
    }

    // ── Bar close signal handler ────────────────────────────────────────────

    /** Fires whenever a new 1-min bar appends via CandleAggregator for the
     *  chosen symbol. We check for the CONFIGURED-timeframe bar close (default
     *  3-min) inside — CandleAggregator's ring stores 1-min bars but
     *  {@code getHistory(sym, N)} aggregates them into N-min buckets. */
    private synchronized void onBarClose(Leg leg, String sideLabel, Candle latestOneMin) {
        if (fsm != FsmState.ARMED || !riskSettings.isVwapStEnabled()) return;
        int tf = Math.max(1, riskSettings.getVwapStCandleMinutes());
        // Trigger check only on bars that ALIGN with the configured timeframe
        // boundary (i.e. the last 1-min bar of an N-min bucket). For 3-min,
        // that's when (istMinuteOfDay - 555 [09:15]) % 3 == 2 → 09:18, 09:21…
        long istMs = latestOneMin.startMillis() + 19_800_000L;   // UTC ms → IST ms
        int minuteOfDay = (int) ((istMs % 86_400_000L) / 60_000L);
        if ((minuteOfDay - (9 * 60 + 15)) % tf != tf - 1) return;

        List<Candle> bars = candleAggregator.getHistory(leg.chosenSymbol, tf);
        if (bars.isEmpty()) return;
        int atrPeriod = Math.max(2, riskSettings.getVwapStAtrPeriod());
        double mult   = Math.max(0.1, riskSettings.getVwapStMultiplier());
        if (bars.size() < atrPeriod + 1) {
            // Not enough for Supertrend yet — history warmup may still be
            // prepending or option didn't trade prior sessions. Bail silently.
            return;
        }
        Candle bar = bars.get(bars.size() - 1);
        SuperTrend.State st = SuperTrend.at(bars, atrPeriod, mult);

        boolean wickBelowVwap = bar.low()  <= bar.vwap();
        boolean closeAboveVwap = bar.close() > bar.vwap();
        boolean greenCandle    = bar.close() > bar.open();
        boolean stUp           = st.available() && st.isUp();

        log.info("[VwapSupertrend] {} bar close — o={} h={} l={} c={} vwap={} st_line={} st_up={} wick_below_vwap={} green={} legState={}",
            sideLabel, fmt(bar.open()), fmt(bar.high()), fmt(bar.low()), fmt(bar.close()),
            fmt(bar.vwap()), fmt(st.line()), stUp, wickBelowVwap, greenCandle, leg.state);

        // Exit trail — ST flipped against position → market exit.
        if (leg.state == LegState.IN_POSITION && st.available() && !st.isUp()) {
            fireExit(leg, sideLabel, "ST_FLIP_EXIT",
                "Supertrend flipped down on " + tf + "-min close (line=" + fmt(st.line()) + ")");
            return;
        }

        // Entry — VWAP-bounce green bar + ST up + leg is idle.
        if (leg.state == LegState.WAITING && wickBelowVwap && closeAboveVwap && greenCandle && stUp) {
            fireEntry(leg, sideLabel, bar);
        }
    }

    // ── Entry / exit ────────────────────────────────────────────────────────

    private void fireEntry(Leg leg, String sideLabel, Candle triggerBar) {
        int lots = Math.max(1, riskSettings.getVwapStLotsPerLeg());
        int qty = lots * LOT_SIZE;
        try {
            OrderDTO placed = orderService.placeOrder(leg.chosenSymbol, qty, 1, 0.0, "INTRADAY");
            if (placed == null || placed.getId() == null || placed.getId().isBlank()) {
                event("[ERROR]", "VwapST",
                    sideLabel + " ENTRY placeOrder rejected — response=" + (placed == null ? "null" : placed.getMessage()));
                return;
            }
            leg.entryOrderId    = placed.getId();
            leg.slPrice         = triggerBar.low();
            leg.qty             = qty;
            leg.entryBarStartMs = triggerBar.startMillis();
            leg.state           = LegState.PENDING_ENTRY;
            event("[SUCCESS]", "VwapST",
                sideLabel + " ENTRY placed — sym=" + leg.chosenSymbol + " qty=" + qty
                    + " triggerBarStart=" + ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(triggerBar.startMillis()), IST).toLocalTime()
                    + " slPrice=" + fmt(leg.slPrice) + " orderId=" + placed.getId());
        } catch (Exception e) {
            event("[ERROR]", "VwapST", sideLabel + " ENTRY threw — " + e.getMessage());
        }
    }

    private synchronized void fireExit(Leg leg, String sideLabel, String reason, String detail) {
        if (leg.state == LegState.WAITING) return;
        String sym = leg.chosenSymbol;
        int qty = Math.max(1, leg.qty);
        double entry = leg.fillPrice;
        try {
            OrderDTO placed = orderService.placeExitOrder(sym, qty, -1, "INTRADAY");
            String orderId = placed != null ? placed.getId() : "";
            event("[WARNING]", "VwapST",
                sideLabel + " EXIT placed — reason=" + reason
                    + " qty=" + qty + " orderId=" + orderId
                    + " detail=" + detail);
            // Approximate P&L using current LTP; refined when the exit fill lands.
            double exitLtp = marketDataService.getLtp(sym);
            if (exitLtp > 0 && entry > 0) {
                double pnl = (exitLtp - entry) * qty;
                realisedPnlToday.updateAndGet(v -> v + pnl);
                long id = System.currentTimeMillis();
                tradesTodayById.put(id, new ClosedTrade(sideLabel, sym, entry, exitLtp, qty, id, reason));
            }
        } catch (Exception e) {
            event("[ERROR]", "VwapST", sideLabel + " EXIT threw — " + e.getMessage());
        } finally {
            leg.reset();
        }
    }

    // ── Fill listener ───────────────────────────────────────────────────────

    void onOrderFill(String orderId, double fillPrice) {
        if (orderId == null) return;
        synchronized (this) {
            if (orderId.equals(ceLeg.entryOrderId) && ceLeg.state == LegState.PENDING_ENTRY) {
                ceLeg.fillPrice = fillPrice;
                ceLeg.state     = LegState.IN_POSITION;
                event("[SUCCESS]", "VwapST",
                    "CE FILL — sym=" + ceLeg.chosenSymbol + " @ " + fmt(fillPrice) + " slPrice=" + fmt(ceLeg.slPrice));
            } else if (orderId.equals(peLeg.entryOrderId) && peLeg.state == LegState.PENDING_ENTRY) {
                peLeg.fillPrice = fillPrice;
                peLeg.state     = LegState.IN_POSITION;
                event("[SUCCESS]", "VwapST",
                    "PE FILL — sym=" + peLeg.chosenSymbol + " @ " + fmt(fillPrice) + " slPrice=" + fmt(peLeg.slPrice));
            }
        }
    }

    // ── Rollover ────────────────────────────────────────────────────────────

    private synchronized void rolloverIfNewDay(String today) {
        todayKey = today;
        ceLeg.reset();
        peLeg.reset();
        spotOpen = 0;
        atmStrike = 0;
        strikesSubscribedAtMs = 0;
        fsm = FsmState.BOOT;
        realisedPnlToday.set(0.0);
        tradesTodayById.clear();
        log.info("[VwapSupertrend] rolled over to new day {} — waiting for spot open tick", today);
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id()           { return "vwap-supertrend"; }
    @Override public String displayName()  { return "VWAP + Supertrend"; }
    @Override public String description()  {
        return "Buy ~₹" + fmt(riskSettings.getVwapStTargetPremium())
            + " CE/PE on VWAP-bounce green bar + Supertrend up. SL = entry bar low. Exit = ST flip.";
    }
    @Override public String currentState() {
        return fsm.name() + " (CE=" + ceLeg.state.name() + ", PE=" + peLeg.state.name() + ")";
    }
    @Override public boolean isEnabled()   { return riskSettings.isVwapStEnabled(); }
    @Override public double  liveNetPnlToday() {
        double closed = realisedPnlToday.get() == null ? 0 : realisedPnlToday.get();
        double open = 0;
        if (ceLeg.state == LegState.IN_POSITION && ceLeg.chosenSymbol != null) {
            double ltp = marketDataService.getLtp(ceLeg.chosenSymbol);
            if (ltp > 0 && ceLeg.fillPrice > 0) open += (ltp - ceLeg.fillPrice) * ceLeg.qty;
        }
        if (peLeg.state == LegState.IN_POSITION && peLeg.chosenSymbol != null) {
            double ltp = marketDataService.getLtp(peLeg.chosenSymbol);
            if (ltp > 0 && peLeg.fillPrice > 0) open += (ltp - peLeg.fillPrice) * peLeg.qty;
        }
        return closed + open;
    }
    @Override public double liveChargesToday() {
        // Flat brokerage × 2 sides × 2 (buy + sell) per fully closed trade.
        double flat = riskSettings.getBrokeragePerOrder();
        return tradesTodayById.size() * flat * 2;
    }
    @Override public List<Map<String, Object>> todayClosedTrades() {
        List<Map<String, Object>> out = new ArrayList<>(tradesTodayById.size());
        for (ClosedTrade t : tradesTodayById.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("grossPnl",       (t.exit - t.entry) * t.qty);
            m.put("charges",        riskSettings.getBrokeragePerOrder() * 2);
            m.put("netPnl",         (t.exit - t.entry) * t.qty - riskSettings.getBrokeragePerOrder() * 2);
            m.put("closedAtMillis", t.closedMs);
            m.put("closeReason",    t.reason);
            m.put("side",           t.side);
            m.put("symbol",         t.symbol);
            out.add(m);
        }
        return out;
    }
    @Override public synchronized boolean forceClose(String reason) {
        boolean acted = false;
        if (ceLeg.state != LegState.WAITING) {
            fireExit(ceLeg, "CE", "FORCE_" + reason, reason);
            acted = true;
        }
        if (peLeg.state != LegState.WAITING) {
            fireExit(peLeg, "PE", "FORCE_" + reason, reason);
            acted = true;
        }
        fsm = FsmState.DONE_FOR_DAY;
        return acted;
    }
    @Override public synchronized void resetToIdle(String reason) {
        ceLeg.reset();
        peLeg.reset();
        fsm = spotOpen > 0 ? FsmState.ARMED : FsmState.BOOT;
        event("[INFO]", "VwapST", "Reset to idle — " + reason);
    }

    // ── Public accessors for chart endpoint ─────────────────────────────────

    public String getChosenCeSymbol() { return ceLeg.chosenSymbol; }
    public String getChosenPeSymbol() { return peLeg.chosenSymbol; }
    public double getSpotOpen()       { return spotOpen; }
    public long   getAtmStrike()      { return atmStrike; }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private String authHeader() {
        return fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
    }
    private static String fmt(double v) {
        return String.format("%.1f", v);
    }
    private void event(String level, String tag, String msg) {
        eventService.log(level + " [" + tag + "] " + msg);
    }
}
