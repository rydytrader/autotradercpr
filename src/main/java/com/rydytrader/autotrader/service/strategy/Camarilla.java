package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.dto.CamarillaLevels;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.CamarillaService;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Camarilla options-selling strategy. Singleton — one state machine, one set of settings.
 *
 * <p>Four entry setups based on prior-day Camarilla pivots:
 * <ol>
 *   <li><b>H3 Reversal</b> — red 5-min candle closes BELOW H3 (after high ≥ H3) → sell CE at H3
 *       strike. Target: spot reaches L3. SL: subsequent candle closes ABOVE entry-candle high.</li>
 *   <li><b>L3 Reversal</b> — green 5-min candle closes ABOVE L3 (after low ≤ L3) → sell PE at L3
 *       strike. Target: spot reaches H3. SL: subsequent candle closes BELOW entry-candle low.</li>
 *   <li><b>H4 Breakout</b>  — green 5-min candle closes ABOVE H4 → sell PE at H4 strike.
 *       Target: spot reaches H5. SL: subsequent candle closes BELOW entry-candle low.</li>
 *   <li><b>L4 Breakdown</b> — red 5-min candle closes BELOW L4 → sell CE at L4 strike.
 *       Target: spot reaches L5. SL: subsequent candle closes ABOVE entry-candle high.</li>
 * </ol>
 *
 * <p>One active trade at a time. After close (target / SL / 15:15), back to IDLE and can fire
 * again from any setup. State persists to {@code ../store/data/camarilla-state.json}.
 */
@Service
public class Camarilla implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(Camarilla.class);
    private static final String STRATEGY_ID = "camarilla";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/data/camarilla-state.json";
    private static final int LOT_SIZE = 75;
    private static final int RECENT_EVENTS_LIMIT = 30;

    public enum LifecycleState { IDLE, ENTRY_PLACED, OPEN, DONE_FOR_DAY }
    public enum ActiveSetup { H3_REVERSAL, L3_REVERSAL, H4_BREAKOUT, L4_BREAKDOWN }

    private final CamarillaService     camarillaService;
    private final CandleAggregator     candleAggregator;
    private final BalancedAtmSelector  atmSelector;
    private final MarketDataService    marketDataService;
    private final OrderService         orderService;
    private final EventService         eventService;
    private final RiskSettingsStore    riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    static { /* records have a Jackson module via findAndRegisterModules */ }

    // ── State (persisted) ────────────────────────────────────────────────────
    private volatile State state = new State();

    public Camarilla(CamarillaService camarillaService,
                     CandleAggregator candleAggregator,
                     BalancedAtmSelector atmSelector,
                     MarketDataService marketDataService,
                     OrderService orderService,
                     EventService eventService,
                     RiskSettingsStore riskSettings,
                     ObjectProvider<StrategyTradeRepository> tradeRepoProvider) {
        this.camarillaService  = camarillaService;
        this.candleAggregator  = candleAggregator;
        this.atmSelector       = atmSelector;
        this.marketDataService = marketDataService;
        this.orderService      = orderService;
        this.eventService      = eventService;
        this.riskSettings      = riskSettings;
        this.tradeRepoProvider = tradeRepoProvider;
    }

    @PostConstruct
    public void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        candleAggregator.onCandleClose(this::onCandleClose);
        log.info("[Camarilla] booted — enabled={}, lots={}, squareoff={}",
            riskSettings.isCamarillaEnabled(), riskSettings.getCamarillaLotsPerLeg(),
            riskSettings.getCamarillaSquareOffTime());
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id() { return STRATEGY_ID; }
    @Override public String displayName() { return "Camarilla"; }
    @Override public String description() { return "H3/L3 reversals + H4/L4 breakouts on NIFTY weekly options"; }
    @Override public String currentState() { return state.lifecycle.name(); }
    @Override public boolean isEnabled() { return riskSettings.isCamarillaEnabled(); }

    @Override
    public boolean forceClose(String reason) {
        synchronized (this) {
            if (state.lifecycle == LifecycleState.IDLE || state.lifecycle == LifecycleState.DONE_FOR_DAY) return false;
            closeNow(reason == null ? "MANUAL" : reason);
            return true;
        }
    }

    @Override
    public void resetToIdle(String reason) {
        synchronized (this) {
            state.lifecycle = LifecycleState.IDLE;
            state.activeSetup = null;
            state.symbol = "";
            state.qty = 0;
            state.entryPrice = 0;
            state.entryOrderId = "";
            state.openMillis = 0;
            state.entryCandleHigh = 0;
            state.entryCandleLow = 0;
            state.entryCandleStartMillis = 0;
            state.targetSpotLevel = 0;
            state.targetAbove = false;
            saveToDisk();
            event("[INFO]", "reset to IDLE — " + (reason == null ? "" : reason));
        }
    }

    @Override
    public java.util.List<java.util.Map<String, Object>> todayClosedTrades() {
        rolloverIfNewDay();
        synchronized (this) { return new ArrayList<>(state.todayClosedTrades); }
    }

    @Override
    public double liveNetPnlToday() {
        // Today's persisted closes + open MTM (if any) − accrued charges.
        rolloverIfNewDay();
        synchronized (this) {
            double net = 0;
            for (Map<String, Object> m : state.todayClosedTrades) net += asDouble(m.get("netPnl"));
            if (state.lifecycle == LifecycleState.OPEN || state.lifecycle == LifecycleState.ENTRY_PLACED) {
                double mtm = computeOpenMtm();
                net += mtm - perCycleCharges(state.entryPrice * state.qty, currentExitTurnover());
            }
            return round2(net);
        }
    }

    @Override
    public double liveChargesToday() {
        rolloverIfNewDay();
        synchronized (this) {
            double ch = 0;
            for (Map<String, Object> m : state.todayClosedTrades) ch += asDouble(m.get("charges"));
            if (state.lifecycle == LifecycleState.OPEN || state.lifecycle == LifecycleState.ENTRY_PLACED) {
                ch += perCycleCharges(state.entryPrice * state.qty, currentExitTurnover());
            }
            return round2(ch);
        }
    }

    @Override
    public void tick() {
        rolloverIfNewDay();
        watchSquareoff();
    }

    @Override
    public void fastSlCheck() {
        watchTarget();
    }

    // ── Candle close handler — entries + SL ──────────────────────────────────

    public void onCandleClose(Candle c) {
        if (!isEnabled()) return;
        synchronized (this) {
            rolloverIfNewDay();
            switch (state.lifecycle) {
                case IDLE          -> evaluateEntries(c);
                case OPEN          -> evaluateSlOnCandle(c);
                case ENTRY_PLACED  -> {
                    // Entry order still pending fill — give the OCO/fill listener a tick to land.
                    // The 5-min boundary is the wrong place to bail; do nothing here.
                }
                case DONE_FOR_DAY  -> { /* no action */ }
            }
        }
    }

    private void evaluateEntries(Candle c) {
        // Daily caps
        int maxTrades = riskSettings.getCamarillaMaxTradesPerDay();
        if (maxTrades > 0 && state.tradesToday >= maxTrades) return;
        int pauseAfter = riskSettings.getCamarillaPauseAfterNLosses();
        if (pauseAfter > 0 && state.consecutiveLosses >= pauseAfter) return;

        CamarillaLevels lv = camarillaService.getNiftyLevels();
        if (lv == null) return;

        boolean red   = c.isRed();
        boolean green = c.isGreen();

        // H3 reversal — red candle close BELOW H3 after touching H3 from above.
        if (riskSettings.isCamarillaH3RevEnabled()
            && red && c.high() >= lv.h3() && c.close() < lv.h3()) {
            fire(ActiveSetup.H3_REVERSAL, lv.h3(), lv.l3(), false, c);
            return;
        }
        // L3 reversal — green candle close ABOVE L3 after touching L3 from below.
        if (riskSettings.isCamarillaL3RevEnabled()
            && green && c.low() <= lv.l3() && c.close() > lv.l3()) {
            fire(ActiveSetup.L3_REVERSAL, lv.l3(), lv.h3(), true, c);
            return;
        }
        // H4 breakout — green candle close ABOVE H4.
        if (riskSettings.isCamarillaH4BoEnabled()
            && green && c.close() > lv.h4()) {
            fire(ActiveSetup.H4_BREAKOUT, lv.h4(), lv.h5(), true, c);
            return;
        }
        // L4 breakdown — red candle close BELOW L4.
        if (riskSettings.isCamarillaL4BdEnabled()
            && red && c.close() < lv.l4()) {
            fire(ActiveSetup.L4_BREAKDOWN, lv.l4(), lv.l5(), false, c);
        }
    }

    /** Place the entry order for {@code setup}. {@code strikeLevel} is the Camarilla level we
     *  use as the strike (rounded to nearest 50). {@code targetSpot} is the level the spot
     *  must reach to take profit; {@code targetAbove} is true when target is above current
     *  spot, false when below. */
    private void fire(ActiveSetup setup, double strikeLevel, double targetSpot,
                      boolean targetAbove, Candle entryCandle) {
        BalancedAtmSelector.StrikeAtLevel strikes = atmSelector.resolveStrikeAtLevel(strikeLevel);
        if (strikes == null) {
            log.warn("[Camarilla] could not resolve strike for level {} — skipping {}", strikeLevel, setup);
            event("[WARNING]", setup + " skipped — option chain unresolved at " + strikeLevel);
            return;
        }
        boolean sellCe = (setup == ActiveSetup.H3_REVERSAL || setup == ActiveSetup.L4_BREAKDOWN);
        String symbol = sellCe ? strikes.ceSymbol() : strikes.peSymbol();
        double indicativeLtp = sellCe ? strikes.ceLtp() : strikes.peLtp();
        int qty = riskSettings.getCamarillaLotsPerLeg() * LOT_SIZE;
        String productType = riskSettings.getCamarillaOrderType();

        log.info("[Camarilla] {} fired — selling {} at strike {} (level {}) qty={} indicativeLtp={}",
            setup, symbol, strikes.resolvedStrike(), strikeLevel, qty, indicativeLtp);
        event("[INFO]", setup + " fired — sell " + symbol + " qty " + qty
            + " (strike " + strikes.resolvedStrike() + ", level " + round2(strikeLevel) + ")");

        OrderDTO order = orderService.placeOrder(symbol, qty, -1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isEmpty()) {
            log.warn("[Camarilla] entry order rejected for {} — staying IDLE", symbol);
            event("[ERROR]", "entry order rejected for " + symbol);
            return;
        }

        try { marketDataService.subscribeAdditional(Collections.singletonList(symbol)); }
        catch (Exception ignored) {}

        state.lifecycle              = LifecycleState.ENTRY_PLACED;
        state.activeSetup            = setup;
        state.symbol                 = symbol;
        state.qty                    = qty;
        state.entryPrice             = 0;   // populated when fill lands
        state.entryOrderId           = order.getId();
        state.openMillis             = System.currentTimeMillis();
        state.entryCandleHigh        = entryCandle.high();
        state.entryCandleLow         = entryCandle.low();
        state.entryCandleStartMillis = entryCandle.startMillis();
        state.targetSpotLevel        = targetSpot;
        state.targetAbove            = targetAbove;
        state.tradesToday++;
        saveToDisk();
    }

    private void evaluateSlOnCandle(Candle c) {
        if (state.lifecycle != LifecycleState.OPEN) return;
        // Skip the entry candle itself — only subsequent candles can SL.
        if (c.startMillis() <= state.entryCandleStartMillis) return;
        boolean ceShort = state.activeSetup == ActiveSetup.H3_REVERSAL
                       || state.activeSetup == ActiveSetup.L4_BREAKDOWN;
        boolean peShort = state.activeSetup == ActiveSetup.L3_REVERSAL
                       || state.activeSetup == ActiveSetup.H4_BREAKOUT;
        if (ceShort && c.close() > state.entryCandleHigh) {
            event("[WARNING]", state.activeSetup + " SL_HIT — candle close " + c.close()
                + " > entry candle high " + state.entryCandleHigh);
            closeNow("SL_HIT");
        } else if (peShort && c.close() < state.entryCandleLow) {
            event("[WARNING]", state.activeSetup + " SL_HIT — candle close " + c.close()
                + " < entry candle low " + state.entryCandleLow);
            closeNow("SL_HIT");
        }
    }

    /** Target check runs on every fast tick — closes the trade when NIFTY spot reaches the
     *  target Camarilla level for the active setup. */
    public synchronized void watchTarget() {
        if (state.lifecycle != LifecycleState.OPEN) return;
        double spot;
        try { spot = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { return; }
        if (spot <= 0) return;

        if (state.targetAbove  && spot >= state.targetSpotLevel) {
            event("[SUCCESS]", state.activeSetup + " TARGET_HIT — spot " + round2(spot)
                + " >= target " + round2(state.targetSpotLevel));
            closeNow("TARGET_HIT");
        } else if (!state.targetAbove && spot <= state.targetSpotLevel) {
            event("[SUCCESS]", state.activeSetup + " TARGET_HIT — spot " + round2(spot)
                + " <= target " + round2(state.targetSpotLevel));
            closeNow("TARGET_HIT");
        }
    }

    /** Time-based squareoff — flatten any open trade at the configured IST time. */
    public synchronized void watchSquareoff() {
        if (state.lifecycle != LifecycleState.OPEN && state.lifecycle != LifecycleState.ENTRY_PLACED) return;
        String hhmm = riskSettings.getCamarillaSquareOffTime();
        if (hhmm == null || hhmm.isBlank()) return;
        LocalTime cutoff;
        try { cutoff = LocalTime.parse(hhmm); }
        catch (Exception e) { return; }
        if (ZonedDateTime.now(IST).toLocalTime().isAfter(cutoff)) {
            event("[INFO]", "TIMED_EXIT — clock reached " + hhmm);
            closeNow("TIMED_EXIT");
        }
    }

    /** Close the active position via market exit, persist the row, archive the cycle, return
     *  to IDLE. */
    private synchronized void closeNow(String reason) {
        if (state.lifecycle == LifecycleState.IDLE || state.lifecycle == LifecycleState.DONE_FOR_DAY) return;
        if (state.symbol == null || state.symbol.isEmpty()) {
            // No real position — just reset state.
            state.lifecycle = LifecycleState.IDLE;
            saveToDisk();
            return;
        }
        // Buy to close (side = +1).
        OrderDTO close = orderService.placeExitOrder(state.symbol, state.qty, 1);
        double exitPrice = 0;
        if (close != null) {
            try { exitPrice = marketDataService.getLtp(state.symbol); }
            catch (Exception ignored) {}
        }
        double sellTurnover = state.entryPrice * state.qty;
        double buyTurnover  = exitPrice * state.qty;
        // For a short option, P&L = (entry − exit) × qty.
        double gross   = (state.entryPrice - exitPrice) * state.qty;
        double charges = perCycleCharges(sellTurnover, buyTurnover);
        double net     = gross - charges;

        persistTradeRow(reason, state.qty, gross, charges, net, reason.equals("SL_HIT") ? 1 : 0);

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("setup",          state.activeSetup == null ? "" : state.activeSetup.name());
        cycle.put("symbol",         state.symbol);
        cycle.put("qty",            state.qty);
        cycle.put("entryPrice",     round2(state.entryPrice));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    reason);
        cycle.put("closedAtMillis", System.currentTimeMillis());
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 50) state.todayClosedTrades.remove(0);

        if (net < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
        event(net >= 0 ? "[SUCCESS]" : "[WARNING]",
            "closed (" + reason + ") net=" + round2(net) + " gross=" + round2(gross));

        // Reset position state but keep tradesToday / consecutiveLosses for the day caps.
        state.lifecycle              = LifecycleState.IDLE;
        state.activeSetup            = null;
        state.symbol                 = "";
        state.qty                    = 0;
        state.entryPrice             = 0;
        state.entryOrderId           = "";
        state.openMillis             = 0;
        state.entryCandleHigh        = 0;
        state.entryCandleLow         = 0;
        state.entryCandleStartMillis = 0;
        state.targetSpotLevel        = 0;
        state.targetAbove            = false;
        saveToDisk();
    }

    private void persistTradeRow(String reason, int qty, double gross, double charges, double net, int slHits) {
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo == null) return;
            StrategyTradeEntity row = new StrategyTradeEntity();
            row.setStrategyId(STRATEGY_ID);
            row.setSessionDate(LocalDate.now(IST).toString());
            row.setClosedAtMillis(System.currentTimeMillis());
            row.setQty(qty);
            row.setGrossPnl(round2(gross));
            row.setCharges(round2(charges));
            row.setNetPnl(round2(net));
            row.setCloseReason(reason);
            row.setSlHitCount(slHits);
            repo.save(row);
        } catch (Exception e) {
            log.warn("[Camarilla] persist trade failed: {}", e.getMessage());
        }
    }

    // ── Day rollover ─────────────────────────────────────────────────────────

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        synchronized (this) {
            if (today.equals(state.dayKey)) return;
            state.dayKey = today;
            state.tradesToday = 0;
            state.consecutiveLosses = 0;
            state.todayClosedTrades.clear();
            // If we have a stale OPEN position from yesterday, force IDLE (broker handled it via
            // intraday auto-square or operator).
            if (state.lifecycle == LifecycleState.OPEN || state.lifecycle == LifecycleState.ENTRY_PLACED) {
                state.lifecycle = LifecycleState.IDLE;
            }
            saveToDisk();
        }
    }

    // ── Charges ──────────────────────────────────────────────────────────────

    private double perCycleCharges(double sellTurnover, double buyTurnover) {
        double broker = riskSettings.getBrokeragePerOrder() * 2; // entry + exit
        double stt = sellTurnover * riskSettings.getSttRate() / 100.0;
        double total = sellTurnover + buyTurnover;
        double exch = total * riskSettings.getExchangeRate() / 100.0;
        double sebi = total / 1e7 * riskSettings.getSebiRate();
        double stamp = buyTurnover * riskSettings.getStampDutyRate() / 100.0;
        double gst = (broker + exch) * riskSettings.getGstRate() / 100.0;
        return round2(broker + stt + exch + sebi + stamp + gst);
    }

    private double currentExitTurnover() {
        try {
            double ltp = marketDataService.getLtp(state.symbol);
            if (ltp > 0) return ltp * state.qty;
        } catch (Exception ignored) {}
        return state.entryPrice * state.qty;
    }

    private double computeOpenMtm() {
        if (state.symbol == null || state.symbol.isEmpty() || state.entryPrice <= 0) return 0;
        try {
            double ltp = marketDataService.getLtp(state.symbol);
            if (ltp <= 0) return 0;
            return (state.entryPrice - ltp) * state.qty;
        } catch (Exception e) {
            return 0;
        }
    }

    // ── Dashboard payload (consumed by CamarillaController + Trade page) ─────

    public synchronized Map<String, Object> dashboardState() {
        rolloverIfNewDay();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("lifecycle",        state.lifecycle.name());
        m.put("activeSetup",      state.activeSetup == null ? null : state.activeSetup.name());
        m.put("symbol",           state.symbol);
        m.put("qty",              state.qty);
        m.put("entryPrice",       round2(state.entryPrice));
        m.put("openMillis",       state.openMillis);
        m.put("entryCandleHigh",  round2(state.entryCandleHigh));
        m.put("entryCandleLow",   round2(state.entryCandleLow));
        m.put("targetSpotLevel",  round2(state.targetSpotLevel));
        m.put("targetAbove",      state.targetAbove);
        m.put("tradesToday",      state.tradesToday);
        m.put("consecutiveLosses",state.consecutiveLosses);
        m.put("dayKey",           state.dayKey);
        m.put("todayClosedTrades",new ArrayList<>(state.todayClosedTrades));
        m.put("recentEvents",     new ArrayList<>(state.recentEvents));
        if (state.symbol != null && !state.symbol.isEmpty()) {
            double ltp = 0;
            try { ltp = marketDataService.getLtp(state.symbol); } catch (Exception ignored) {}
            m.put("symbolLtp",   round2(ltp));
            m.put("openMtm",     round2(computeOpenMtm()));
        }
        try {
            double spot = marketDataService.getLtp(NIFTY_SYMBOL);
            m.put("niftySpot", round2(spot));
        } catch (Exception ignored) {}
        return m;
    }

    // ── State persistence ───────────────────────────────────────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class State {
        public LifecycleState lifecycle = LifecycleState.IDLE;
        public ActiveSetup    activeSetup;
        public String  symbol = "";
        public int     qty;
        public double  entryPrice;
        public String  entryOrderId = "";
        public long    openMillis;
        public double  entryCandleHigh;
        public double  entryCandleLow;
        public long    entryCandleStartMillis;
        public double  targetSpotLevel;
        public boolean targetAbove;
        public String  dayKey = "";
        public int     tradesToday;
        public int     consecutiveLosses;
        public List<Map<String, Object>> todayClosedTrades = new ArrayList<>();
        public List<Map<String, Object>> recentEvents      = new ArrayList<>();
    }

    private void event(String severity, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts",       System.currentTimeMillis());
        e.put("severity", severity);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [camarilla] " + message);
    }

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) state = s;
        } catch (IOException e) {
            log.warn("[Camarilla] failed to load state: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
            Files.move(tmp, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("[Camarilla] failed to save state: {}", e.getMessage());
        }
    }

    // ── Fill notification — called by OrderEventService when the entry order fills ──
    public synchronized void onEntryFilled(String orderId, double fillPrice) {
        if (!orderId.equals(state.entryOrderId)) return;
        if (state.lifecycle != LifecycleState.ENTRY_PLACED) return;
        state.entryPrice = fillPrice;
        state.lifecycle  = LifecycleState.OPEN;
        event("[SUCCESS]", "entry filled @ " + round2(fillPrice));
        saveToDisk();
    }

    // ── Misc utility ────────────────────────────────────────────────────────

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
    private static double asDouble(Object o) {
        if (o instanceof Number) return ((Number) o).doubleValue();
        if (o == null) return 0;
        try { return Double.parseDouble(o.toString()); } catch (Exception e) { return 0; }
    }
}
