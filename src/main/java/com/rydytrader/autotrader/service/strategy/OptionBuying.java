package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.indicator.BollingerBands;
import com.rydytrader.autotrader.indicator.FloorPivots;
import com.rydytrader.autotrader.indicator.Rsi;
import com.rydytrader.autotrader.indicator.SuperTrend;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.DailyOhlcCache;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OPTION BUYING strategy — Dharanidharan Ganesan's 4-indicator framework on
 * 3-min NIFTY spot bars. Buys ATM CE on bullish setups, ATM PE on bearish.
 *
 * <p><b>Entry — all four must be TRUE on the same 3-min NIFTY bar close:</b>
 * <ol>
 *   <li>SuperTrend (10, 2) — direction. Bull: close &gt; ST line + ST is up.
 *       Bear: close &lt; ST line + ST is down.</li>
 *   <li>RSI (14) — momentum. Bull: RSI &gt; 70. Bear: RSI &lt; 30.</li>
 *   <li>Standard Floor Pivots — structure. Bull: close &gt; R1. Bear: close &lt; S1.</li>
 *   <li>Bollinger Bands (20, 2) — trigger. Bull: close &gt; upper band. Bear: close &lt; lower band.</li>
 * </ol>
 *
 * <p><b>ATM resolution — FRESH at each fire.</b> Unlike OPTION SELLING which
 * locks a session ATM at 09:18 and holds it, OPTION BUYING computes
 * {@code round(currentNiftyLtp / 50) * 50} at the moment of the fire and
 * buys THAT strike's CE (bullish) or PE (bearish). This matters — NIFTY can
 * drift hundreds of points during a session, and an option buyer wants the
 * momentary at-the-money leg, not a stale session anchor.
 *
 * <p><b>Exit — SuperTrend trailing.</b> On every subsequent 3-min NIFTY bar
 * close, re-evaluate SuperTrend. Long CE exits when ST flips to down. Long
 * PE exits when ST flips to up. Fast-path {@code fastSlCheck} enforces a
 * hard {@code optionBuyingHardSlPct} backstop (default 40 % of entry
 * premium) so a gap-down doesn't wait for the next bar close.
 *
 * <p><b>Time gates.</b> Entries fire only in
 * {@code optionBuyingTradingStartTime}..{@code optionBuyingTradingEndTime}.
 * Any open positions are force-flattened at {@code optionBuyingSquareOffTime}.
 * A daily cap of {@code optionBuyingMaxTradesPerDay} (default 6) throttles
 * over-firing.
 */
@Service
public class OptionBuying implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(OptionBuying.class);
    private static final ZoneId  IST = ZoneId.of("Asia/Kolkata");

    public  static final String STRATEGY_ID  = "option-buying";
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final int    LOT_SIZE     = 65;    // NIFTY lot
    private static final String STATE_FILE   = "../store/cache/option-buying-state.json";
    private static final int    RECENT_EVENTS_LIMIT = 500;
    private static final int    RECENT_CYCLES_LIMIT = 100;

    // Indicator settings — hard-coded per Ganesan Appendix A.
    private static final int    SUPERTREND_ATR       = 10;
    private static final double SUPERTREND_MULT      = 2.0;
    private static final int    RSI_PERIOD           = 14;
    private static final double RSI_BULL_THRESHOLD   = 70.0;
    private static final double RSI_BEAR_THRESHOLD   = 30.0;
    private static final int    BB_PERIOD            = 20;
    private static final double BB_STD               = 2.0;
    /** Minimum bars in the aggregator before we can compute all four
     *  indicators. Max of (SuperTrend ATR + 1), (RSI + 1), BB. */
    private static final int    MIN_BARS_FOR_INDICATORS =
        Math.max(BB_PERIOD, Math.max(SUPERTREND_ATR + 1, RSI_PERIOD + 1));

    private final CandleAggregator                        candleAggregator;
    private final BalancedAtmSelector                     atmSelector;
    private final MarketDataService                       marketDataService;
    private final OrderService                            orderService;
    private final EventService                            eventService;
    private final RiskSettingsStore                       riskSettings;
    private final DailyOhlcCache                          dailyOhlcCache;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;

    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules();

    private volatile State state = new State();

    public OptionBuying(CandleAggregator candleAggregator,
                        BalancedAtmSelector atmSelector,
                        MarketDataService marketDataService,
                        OrderService orderService,
                        EventService eventService,
                        RiskSettingsStore riskSettings,
                        DailyOhlcCache dailyOhlcCache,
                        ObjectProvider<StrategyTradeRepository> tradeRepoProvider) {
        this.candleAggregator  = candleAggregator;
        this.atmSelector       = atmSelector;
        this.marketDataService = marketDataService;
        this.orderService      = orderService;
        this.eventService      = eventService;
        this.riskSettings      = riskSettings;
        this.dailyOhlcCache    = dailyOhlcCache;
        this.tradeRepoProvider = tradeRepoProvider;
    }

    @PostConstruct
    public void boot() {
        loadFromDisk();
        // Subscribe to NIFTY spot 3-min closes. Aggregator already receives NIFTY
        // (OptionSelling subscribes it too) — a second subscribe is a no-op on the
        // Fyers WS side but registers our onCandleClose callback.
        candleAggregator.subscribe(NIFTY_SYMBOL, c -> onCandleClose(NIFTY_SYMBOL, c));
        log.info("[OptionBuying] booted — enabled={}, lots={}, start={}, end={}, squareoff={}, dayKey={}, openPositions={}",
            isEnabled(), riskSettings.getOptionBuyingLotsPerLeg(),
            riskSettings.getOptionBuyingTradingStartTime(),
            riskSettings.getOptionBuyingTradingEndTime(),
            riskSettings.getOptionBuyingSquareOffTime(),
            state.dayKey, state.openPositions.size());
    }

    // ── Strategy interface ─────────────────────────────────────────────────

    @Override public String id()          { return STRATEGY_ID; }
    @Override public String displayName() { return "OPTION BUYING"; }
    @Override public String description() { return "NIFTY 3-min · 4-indicator (ST/RSI/Pivots/BB) · CE/PE long"; }
    @Override public boolean isEnabled()  { return riskSettings.isOptionBuyingEnabled(); }

    @Override public String currentState() {
        if (state.openPositions.isEmpty()) return "IDLE";
        return "IN_POSITION(" + state.openPositions.size() + ")";
    }

    @Override
    public synchronized boolean forceClose(String reason) {
        if (state.openPositions.isEmpty()) return false;
        event("[WARNING]", "System", "forceClose — " + (reason == null ? "" : reason));
        for (Position p : new ArrayList<>(state.openPositions.values())) {
            closePosition(p, "FORCE_CLOSE");
        }
        return true;
    }

    @Override
    public synchronized void resetToIdle(String reason) {
        state.openPositions.clear();
        state.doneForDay = false;
        event("[INFO]", "System", "reset — " + (reason == null ? "" : reason));
        saveToDisk();
    }

    @Override
    public synchronized List<Map<String, Object>> todayClosedTrades() {
        rolloverIfNewDay();
        return new ArrayList<>(state.todayClosedTrades);
    }

    @Override
    public synchronized double liveNetPnlToday() {
        rolloverIfNewDay();
        double net = 0;
        for (Map<String, Object> m : state.todayClosedTrades) net += asDouble(m.get("netPnl"));
        for (Position p : state.openPositions.values()) net += openMtm(p);
        return round2(net);
    }

    @Override
    public synchronized double liveChargesToday() {
        rolloverIfNewDay();
        double ch = 0;
        for (Map<String, Object> m : state.todayClosedTrades) ch += asDouble(m.get("charges"));
        return round2(ch);
    }

    @Override
    public void tick() {
        rolloverIfNewDay();
        watchSquareoff();
    }

    @Override
    public void fastSlCheck() {
        if (state.openPositions.isEmpty()) return;
        // Hard % SL — options can gap 40%+ in seconds during a reversal, and
        // the SuperTrend exit only fires on 3-min bar close. This backstop
        // triggers on any tick.
        double hardSlPct = riskSettings.getOptionBuyingHardSlPct();
        if (hardSlPct <= 0) return;
        for (Position p : new ArrayList<>(state.openPositions.values())) {
            double ltp;
            try { ltp = marketDataService.getLtp(p.symbol); }
            catch (Exception e) { continue; }
            if (ltp <= 0) continue;
            // Long position — SL fires when LTP drops far enough below entry.
            double lossPct = (p.entryPrice - ltp) / p.entryPrice * 100.0;
            if (lossPct >= hardSlPct) {
                event("[ERROR]", "Exit",
                    shortSym(p.symbol) + " HARD_SL_HIT @ " + round2(ltp)
                    + " (entry=" + round2(p.entryPrice) + ", loss=" + round2(lossPct) + "%)");
                closePosition(p, "HARD_SL_HIT");
            }
        }
    }

    // ── Candle close handler — the FSM entry point ──────────────────────────

    public void onCandleClose(String symbol, Candle c) {
        if (!NIFTY_SYMBOL.equals(symbol)) return;
        synchronized (this) {
            if (!isEnabled()) return;
            rolloverIfNewDay();
            if (state.doneForDay) return;

            // 1. Exit check on any open position: has NIFTY's SuperTrend flipped
            //    against the trade? Do this BEFORE evaluating new entries so a
            //    reversal bar can close the loser and (potentially) open the
            //    opposite side on the same close.
            evaluateExits(c);

            // 2. Entry gate.
            if (!inTradingWindow()) return;
            if (state.tradesToday >= riskSettings.getOptionBuyingMaxTradesPerDay()) return;

            List<Candle> bars = candleAggregator.getHistory(NIFTY_SYMBOL);
            if (bars.size() < MIN_BARS_FOR_INDICATORS) return;

            SuperTrend.State st  = SuperTrend.at(bars, SUPERTREND_ATR, SUPERTREND_MULT);
            double rsi           = Rsi.at(bars, RSI_PERIOD);
            BollingerBands.State bb = BollingerBands.at(bars, BB_PERIOD, BB_STD);
            Optional<DailyOhlcCache.DailyBar> prev = dailyOhlcCache.previousDay(NIFTY_SYMBOL);
            if (!st.available() || !bb.available() || prev.isEmpty() || rsi <= 0) {
                log.debug("[OptionBuying] indicators unavailable — st={} bb={} rsi={} prev={}",
                    st.available(), bb.available(), rsi, prev.isPresent());
                return;
            }
            FloorPivots pivots = FloorPivots.from(prev.get().high(), prev.get().low(), prev.get().close());

            double close = c.close();
            // Bullish 4-condition
            boolean stBull   = st.isUp() && close > st.line();
            boolean rsiBull  = rsi > RSI_BULL_THRESHOLD;
            boolean pivBull  = close > pivots.r1();
            boolean bbBull   = close > bb.upper();
            // Bearish 4-condition
            boolean stBear   = !st.isUp() && close < st.line();
            boolean rsiBear  = rsi < RSI_BEAR_THRESHOLD;
            boolean pivBear  = close < pivots.s1();
            boolean bbBear   = close < bb.lower();

            if (stBull && rsiBull && pivBull && bbBull) {
                event("[SUCCESS]", "Signal",
                    "BULLISH 4/4 @ NIFTY " + round2(close)
                    + " (ST=" + round2(st.line())
                    + ", RSI=" + round2(rsi)
                    + ", R1=" + round2(pivots.r1())
                    + ", BB↑=" + round2(bb.upper()) + ")");
                fire(true, close, st, rsi, pivots, bb, c);
            } else if (stBear && rsiBear && pivBear && bbBear) {
                event("[SUCCESS]", "Signal",
                    "BEARISH 4/4 @ NIFTY " + round2(close)
                    + " (ST=" + round2(st.line())
                    + ", RSI=" + round2(rsi)
                    + ", S1=" + round2(pivots.s1())
                    + ", BB↓=" + round2(bb.lower()) + ")");
                fire(false, close, st, rsi, pivots, bb, c);
            }
            saveToDisk();
        }
    }

    // ── Fire ────────────────────────────────────────────────────────────────

    private void fire(boolean bullish, double niftyClose, SuperTrend.State st, double rsi,
                       FloorPivots pivots, BollingerBands.State bb, Candle bar) {
        // Resolve ATM FRESH from current NIFTY LTP (not the session-locked ATM
        // that OPTION SELLING uses). See project_option_buying_plan.md.
        double niftyLtp;
        try { niftyLtp = marketDataService.getLtp(NIFTY_SYMBOL); }
        catch (Exception e) { niftyLtp = niftyClose; }
        if (niftyLtp <= 0) niftyLtp = niftyClose;

        BalancedAtmSelector.StrikeAtLevel row = atmSelector.resolveStrikeAtLevel(niftyLtp);
        if (row == null) {
            event("[ERROR]", "Entry", "ATM chain unavailable @ NIFTY " + round2(niftyLtp));
            return;
        }
        String symbol = bullish ? row.ceSymbol() : row.peSymbol();
        double refLtp = bullish ? row.ceLtp()    : row.peLtp();

        // No same-symbol stacking.
        for (Position p : state.openPositions.values()) {
            if (p != null && symbol.equals(p.symbol)) {
                event("[WARNING]", "Entry",
                    "skip — position already open on " + shortSym(symbol));
                return;
            }
        }

        double entryLtp;
        try { entryLtp = marketDataService.getLtp(symbol); }
        catch (Exception e) { entryLtp = refLtp; }
        if (entryLtp <= 0) entryLtp = refLtp;
        if (entryLtp <= 0) {
            event("[ERROR]", "Entry", "no entry price available for " + shortSym(symbol));
            return;
        }

        int qty = riskSettings.getOptionBuyingLotsPerLeg() * LOT_SIZE;
        String productType = riskSettings.getOptionBuyingOrderType();

        // BUY order — side = +1. OrderService.placeOrder passes side to Fyers.
        OrderDTO order = orderService.placeOrder(symbol, qty, +1, 0, productType);
        if (order == null || order.getId() == null || order.getId().isBlank()) {
            event("[ERROR]", "Entry", "order rejected for " + shortSym(symbol));
            return;
        }
        try { marketDataService.subscribeAdditional(java.util.Collections.singletonList(symbol)); }
        catch (Exception ignored) {}

        Position p = new Position();
        p.symbol         = symbol;
        p.side           = bullish ? "CE_BUY" : "PE_BUY";
        p.qty            = qty;
        p.entryPrice     = entryLtp;
        p.entryOrderId   = order.getId();
        p.openMillis     = System.currentTimeMillis();
        p.entryCandleMs  = bar.startMillis();
        p.productType    = productType;
        p.atmStrike      = row.resolvedStrike();
        p.niftyAtEntry   = niftyLtp;
        p.superTrendAtEntry = st.line();
        p.rsiAtEntry     = rsi;

        state.openPositions.put(symbol, p);
        state.tradesToday++;
        event("[SUCCESS]", "Entry",
            "BUY " + (bullish ? "CE " : "PE ") + shortSym(symbol)
            + " ×" + (qty / LOT_SIZE) + "L @ " + round2(entryLtp)
            + " (ATM " + row.resolvedStrike() + ", NIFTY " + round2(niftyLtp)
            + ", trade " + state.tradesToday + "/" + riskSettings.getOptionBuyingMaxTradesPerDay() + ")");
    }

    // ── Exits ───────────────────────────────────────────────────────────────

    /** On every 3-min NIFTY close, re-evaluate SuperTrend and exit any position
     *  whose direction is against the trade. Bull position exits when ST flips
     *  down; bear position exits when ST flips up. */
    private void evaluateExits(Candle nifty) {
        if (state.openPositions.isEmpty()) return;
        List<Candle> bars = candleAggregator.getHistory(NIFTY_SYMBOL);
        if (bars.size() < MIN_BARS_FOR_INDICATORS) return;
        SuperTrend.State st = SuperTrend.at(bars, SUPERTREND_ATR, SUPERTREND_MULT);
        if (!st.available()) return;
        for (Position p : new ArrayList<>(state.openPositions.values())) {
            boolean bull = "CE_BUY".equals(p.side);
            boolean flipped = bull ? !st.isUp() : st.isUp();
            if (flipped) {
                event("[WARNING]", "Exit",
                    shortSym(p.symbol) + " SUPERTREND_FLIP @ NIFTY " + round2(nifty.close())
                    + " (ST=" + round2(st.line()) + ", isUp=" + st.isUp() + ")");
                closePosition(p, "SUPERTREND_FLIP");
            }
        }
    }

    /** Timed squareoff — flatten every open position at
     *  {@code optionBuyingSquareOffTime} regardless of SL / trend state. */
    private synchronized void watchSquareoff() {
        if (state.openPositions.isEmpty()) return;
        String hhmm = riskSettings.getOptionBuyingSquareOffTime();
        if (hhmm == null || hhmm.isBlank()) return;
        LocalTime cutoff;
        try { cutoff = LocalTime.parse(hhmm); }
        catch (Exception e) { return; }
        if (ZonedDateTime.now(IST).toLocalTime().isAfter(cutoff)) {
            event("[INFO]", "Squareoff",
                "TIMED_EXIT — clock reached " + hhmm + ", flattening " + state.openPositions.size());
            for (Position p : new ArrayList<>(state.openPositions.values())) {
                closePosition(p, "TIMED_EXIT");
            }
        }
    }

    private boolean closePosition(Position p, String reason) {
        if (p == null) return false;
        String productType = (p.productType == null || p.productType.isBlank())
            ? riskSettings.getOptionBuyingOrderType() : p.productType;
        // For a long position, exit is a SELL — side = -1.
        OrderDTO close = orderService.placeExitOrder(p.symbol, p.qty, -1, productType);
        double exitPrice = 0;
        String exitOrderId = close == null ? null : close.getId();
        if (exitOrderId != null && !exitOrderId.isBlank()) {
            for (int attempt = 0; attempt < 5; attempt++) {
                try {
                    orderService.invalidateTradebookCache();
                    double filled = orderService.getFilledPriceByOrderId(exitOrderId);
                    if (filled > 0) { exitPrice = filled; break; }
                    Thread.sleep(300);
                } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                catch (Exception ignored) {}
            }
        }
        if (exitPrice <= 0) {
            try { exitPrice = marketDataService.getLtp(p.symbol); }
            catch (Exception ignored) {}
        }
        double gross    = (exitPrice - p.entryPrice) * p.qty;   // long: sell - buy
        double charges  = perCycleCharges(p.entryPrice * p.qty, exitPrice * p.qty);
        double net      = gross - charges;
        long closedMs   = System.currentTimeMillis();

        persistTradeRow(p, gross, charges, net, exitPrice, closedMs, reason);

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("strategyId",     STRATEGY_ID);
        cycle.put("setup",          p.side);
        cycle.put("symbol",         p.symbol);
        cycle.put("side",           "BUY");
        cycle.put("qty",            p.qty);
        cycle.put("entryPrice",     round2(p.entryPrice));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    reason);
        cycle.put("closedAtMillis", closedMs);
        cycle.put("openedAtMillis", p.openMillis);
        cycle.put("entryCandleMs",  p.entryCandleMs);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > RECENT_CYCLES_LIMIT) state.todayClosedTrades.remove(0);

        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Exit",
            shortSym(p.symbol) + " closed (" + reason + ") net=" + round2(net)
            + " gross=" + round2(gross) + " exit=" + round2(exitPrice));

        state.openPositions.remove(p.symbol);
        saveToDisk();
        return true;
    }

    // ── Persistence + accessors ─────────────────────────────────────────────

    /** Entry price of the currently-open leg on {@code symbol}, or 0 when no
     *  position is open on that side. Used by the Chart page to draw an entry
     *  horizontal line alongside the SuperTrend line. */
    public double getOpenEntryPrice(String fyersSymbol) {
        if (fyersSymbol == null || fyersSymbol.isBlank()) return 0;
        Position p = state.openPositions.get(fyersSymbol);
        return p != null ? p.entryPrice : 0;
    }

    /** Number of entries fired today. */
    public int getTradesToday() { return state.tradesToday; }

    /** Snapshot for the UI dashboard: today's open positions + closed cycles +
     *  event log + counts. Bag-of-values, matches OptionSelling's /state shape
     *  so the frontend can be strategy-agnostic. */
    public synchronized Map<String, Object> stateSnapshot() {
        rolloverIfNewDay();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("strategy",         STRATEGY_ID);
        m.put("enabled",          isEnabled());
        m.put("state",            currentState());
        m.put("tradesToday",      state.tradesToday);
        m.put("maxTradesPerDay",  riskSettings.getOptionBuyingMaxTradesPerDay());
        m.put("doneForDay",       state.doneForDay);
        m.put("liveNetPnl",       round2(liveNetPnlToday()));
        m.put("liveCharges",      round2(liveChargesToday()));
        List<Map<String, Object>> openRows = new ArrayList<>();
        for (Position p : state.openPositions.values()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("symbol",        p.symbol);
            row.put("side",          p.side);
            row.put("qty",           p.qty);
            row.put("entryPrice",    round2(p.entryPrice));
            row.put("openMillis",    p.openMillis);
            row.put("entryCandleMs", p.entryCandleMs);
            row.put("atmStrike",     p.atmStrike);
            row.put("niftyAtEntry",  round2(p.niftyAtEntry));
            row.put("mtm",           round2(openMtm(p)));
            openRows.add(row);
        }
        m.put("openPositions",    openRows);
        m.put("todayClosedTrades", new ArrayList<>(state.todayClosedTrades));
        m.put("recentEvents",     new ArrayList<>(state.recentEvents));
        return m;
    }

    private void persistTradeRow(Position p, double gross, double charges, double net,
                                 double exitPrice, long closedMs, String reason) {
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo == null) return;
            StrategyTradeEntity row = new StrategyTradeEntity();
            row.setStrategyId(STRATEGY_ID);
            row.setSymbol(p.symbol);
            row.setSetup(p.side);
            row.setSessionDate(LocalDate.now(IST).toString());
            row.setClosedAtMillis(closedMs);
            row.setOpenedAtMillis(p.openMillis);
            row.setInstrument("NIFTY");
            row.setQty(p.qty);
            row.setGrossPnl(round2(gross));
            row.setCharges(round2(charges));
            row.setNetPnl(round2(net));
            row.setCloseReason(reason);
            row.setSlHitCount("HARD_SL_HIT".equals(reason) ? 1 : 0);
            row.setEntryPrice(round2(p.entryPrice));
            row.setExitPrice(round2(exitPrice));
            row.setEntryCandleMs(p.entryCandleMs);
            row.setExitCandleMs(closedMs);
            repo.save(row);
        } catch (Exception e) {
            log.warn("[OptionBuying] persist trade failed: {}", e.getMessage());
        }
    }

    private double openMtm(Position p) {
        if (p == null || p.qty <= 0 || p.entryPrice <= 0) return 0;
        double ltp;
        try { ltp = marketDataService.getLtp(p.symbol); }
        catch (Exception e) { return 0; }
        if (ltp <= 0) return 0;
        return (ltp - p.entryPrice) * p.qty;    // long
    }

    /** Simple per-cycle charges — brokerage per side + STT on sell + GST +
     *  SEBI + stamp. Same shape as OptionSelling.perCycleCharges but with
     *  the buy-side stamp duty (bought first). */
    private double perCycleCharges(double buyTurnover, double sellTurnover) {
        double brokerage = 2 * riskSettings.getBrokeragePerOrder();
        double stt       = sellTurnover * riskSettings.getSttRate() / 100.0;   // STT on sell
        double exch      = (buyTurnover + sellTurnover) * riskSettings.getExchangeRate() / 100.0;
        double gst       = (brokerage + exch) * riskSettings.getGstRate() / 100.0;
        double sebi      = (buyTurnover + sellTurnover) * riskSettings.getSebiRate() / 1e7;
        double stamp     = buyTurnover * riskSettings.getStampDutyRate() / 100.0;
        return round2(brokerage + stt + exch + gst + sebi + stamp);
    }

    // ── Time gates + rollover ───────────────────────────────────────────────

    private boolean inTradingWindow() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        LocalTime start = parseHhmm(riskSettings.getOptionBuyingTradingStartTime(), LocalTime.of(9, 24));
        LocalTime end   = parseHhmm(riskSettings.getOptionBuyingTradingEndTime(),   LocalTime.of(14, 30));
        return !now.isBefore(start) && now.isBefore(end);
    }

    private LocalTime parseHhmm(String s, LocalTime fallback) {
        if (s == null || s.isBlank()) return fallback;
        try { return LocalTime.parse(s); }
        catch (Exception e) { return fallback; }
    }

    private synchronized void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        state.dayKey = today;
        state.tradesToday = 0;
        state.doneForDay  = false;
        state.todayClosedTrades.clear();
        if (state.recentEvents != null) state.recentEvents.clear();
        // Drop any leftover positions from yesterday — should be flat via
        // squareoff but defensive.
        state.openPositions.clear();
    }

    // ── Event log ───────────────────────────────────────────────────────────

    private void event(String severity, String source, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts",       System.currentTimeMillis());
        e.put("severity", severity);
        e.put("source",   source);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [option-buying:" + source + "] " + message);
    }

    // ── Persistence ─────────────────────────────────────────────────────────

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s == null) return;
            state = s;
            if (state.openPositions == null)    state.openPositions    = new ConcurrentHashMap<>();
            if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
            if (state.recentEvents == null)     state.recentEvents     = new ArrayList<>();
        } catch (IOException e) {
            log.warn("[OptionBuying] load failed: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            java.io.File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(state));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[OptionBuying] save failed: {}", e.getMessage());
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private static double asDouble(Object v) {
        if (v instanceof Number n) return n.doubleValue();
        if (v instanceof String s) { try { return Double.parseDouble(s); } catch (Exception e) { return 0; } }
        return 0;
    }

    private static String shortSym(String s) {
        if (s == null) return "";
        int idx = s.indexOf("NIFTY");
        return idx >= 0 ? s.substring(idx + 5) : s;
    }

    // ── State POJOs ─────────────────────────────────────────────────────────

    public static class State {
        public String dayKey = "";
        public int    tradesToday = 0;
        public boolean doneForDay = false;
        public Map<String, Position> openPositions = new ConcurrentHashMap<>();
        public List<Map<String, Object>> todayClosedTrades = new ArrayList<>();
        public List<Map<String, Object>> recentEvents = new ArrayList<>();
    }

    public static class Position {
        public String symbol = "";
        public String side   = "";        // "CE_BUY" or "PE_BUY"
        public int    qty    = 0;
        public double entryPrice = 0;
        public String entryOrderId = "";
        public long   openMillis = 0;
        public long   entryCandleMs = 0;
        public String productType = "";
        public long   atmStrike = 0;
        public double niftyAtEntry = 0;
        public double superTrendAtEntry = 0;
        public double rsiAtEntry = 0;
    }
}
