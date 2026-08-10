package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.Candle;
import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.entity.StrategyTradeEntity;
import com.rydytrader.autotrader.gdfl.GdflService;
import com.rydytrader.autotrader.gdfl.GdflSymbolMapper;
import com.rydytrader.autotrader.repository.StrategyTradeRepository;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.util.NiftyExpiryResolver;
import com.rydytrader.autotrader.util.NiftyOptionSymbolBuilder;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OPTIONS SELLING strategy — VWAP-rejection / crossover premium seller on the
 * NIFTY current-month FUTURES 5-min bar close.
 *
 * <p><b>Trigger</b> — on each 5-min futures bar close where the bar's range spans
 * VWAP ({@code low ≤ vwap ≤ high}) AND close is on a side of VWAP:
 * <ul>
 *   <li>{@code close < vwap} → SELL ATM CE (open short call).</li>
 *   <li>{@code close > vwap} → SELL ATM PE (open short put).</li>
 *   <li>{@code close == vwap} → no trigger.</li>
 * </ul>
 *
 * <p><b>Serial per side</b> — at most ONE open CE + ONE open PE at any moment.
 * Re-triggers while a same-side position is open are silently skipped.
 *
 * <p><b>Session caps</b> — {@code optionSellingMaxCeSellsPerDay} +
 * {@code optionSellingMaxPeSellsPerDay}. Rejections don't burn slots — counter
 * only advances when the entry order returns {@code ok} with an orderId.
 *
 * <p><b>Exit paths per position</b>:
 * <ul>
 *   <li>SL — 2 consecutive futures 5-min bars close on the opposite side of VWAP
 *       (counter starts from the bar AFTER the entry candle).</li>
 *   <li>Squareoff at {@code optionSellingSquareOffTime} (default 15:15 IST) —
 *       catches both {@code IN_POSITION} and {@code PENDING_ENTRY} defensively.</li>
 *   <li>No option-price target — pure SL / squareoff.</li>
 * </ul>
 *
 * <p><b>P&L for a short</b> — cycle netPnl = {@code (entryPrice - exitPrice) × qty}
 * minus per-cycle charges (sellTurnover = entryPrice × qty, buyTurnover =
 * exitPrice × qty). Open-position MTM = {@code (entryPrice - currentLtp) × qty}.
 *
 * <p>State persisted at {@code store/cache/option-selling-state.json} so a mid-day
 * JVM restart resumes correctly.
 */
@Service
public class OptionSelling implements Strategy {

    private static final Logger log = LoggerFactory.getLogger(OptionSelling.class);
    private static final String STRATEGY_ID = "option-selling";
    /** GDFL delivers NIFTY current-month futures ticks under this symbol. */
    private static final String NIFTY_SYMBOL = GdflSymbolMapper.FYERS_NIFTY_FUTURES;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_FILE = "../store/cache/option-selling-state.json";
    /** NIFTY option lot size — 65 (post 2025 revision). */
    private static final int    LOT_SIZE = 65;
    private static final int    RECENT_EVENTS_LIMIT = 60;
    /** ATM step for NIFTY option strikes (nearest 50). */
    private static final long   ATM_STEP = 50L;

    /** Per-position FSM state. Mini state-machine, one instance per open position. */
    public enum PosState { PENDING_ENTRY, IN_POSITION, DONE }

    private final CandleAggregator      candleAggregator;
    private final MarketDataService     marketDataService;
    private final OrderService          orderService;
    private final EventService          eventService;
    private final RiskSettingsStore     riskSettings;
    private final ObjectProvider<StrategyTradeRepository> tradeRepoProvider;
    private final ObjectProvider<OrderEventService>       orderEventServiceProvider;
    private final ObjectProvider<GdflService>             gdflServiceProvider;
    private final ObjectProvider<MarketHolidayService>    holidayServiceProvider;

    private final ObjectMapper mapper = new ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

    private volatile State state = new State();

    public OptionSelling(CandleAggregator candleAggregator,
                         MarketDataService marketDataService,
                         OrderService orderService,
                         EventService eventService,
                         RiskSettingsStore riskSettings,
                         ObjectProvider<StrategyTradeRepository> tradeRepoProvider,
                         ObjectProvider<OrderEventService> orderEventServiceProvider,
                         ObjectProvider<GdflService> gdflServiceProvider,
                         ObjectProvider<MarketHolidayService> holidayServiceProvider) {
        this.candleAggregator           = candleAggregator;
        this.marketDataService          = marketDataService;
        this.orderService               = orderService;
        this.eventService               = eventService;
        this.riskSettings               = riskSettings;
        this.tradeRepoProvider          = tradeRepoProvider;
        this.orderEventServiceProvider  = orderEventServiceProvider;
        this.gdflServiceProvider        = gdflServiceProvider;
        this.holidayServiceProvider     = holidayServiceProvider;
    }

    // ── Boot / lifecycle ────────────────────────────────────────────────────

    @PostConstruct
    public void boot() {
        loadFromDisk();
        rolloverIfNewDay();
        pruneStaleEventsBeforeToday();

        // Subscribe the NIFTY current-month FUTURES symbol. GdflService already
        // subscribes it — this attaches our aggregator listener so we get 5-min
        // bar closes. CandleAggregator.subscribe is multi-listener safe so
        // OPTION BUYING's parallel subscription keeps working.
        candleAggregator.subscribe(NIFTY_SYMBOL, c -> onFuturesBar(NIFTY_SYMBOL, c));

        // Re-attach candle listeners for any open option positions restored from
        // disk so the aggregator keeps bucketing their ticks. Callback is a no-op
        // — we only need CandleAggregator's subscribeAdditional side-effect.
        for (SellingPosition p : state.openPositions.values()) {
            if (p != null && p.symbol != null && !p.symbol.isBlank()) {
                candleAggregator.subscribe(p.symbol, c -> {});
            }
        }

        // Fill listener — additional listener alongside OPTION BUYING's. We
        // filter by our own order IDs inside the callback so foreign fills are
        // ignored cheaply.
        OrderEventService oes = orderEventServiceProvider == null ? null : orderEventServiceProvider.getIfAvailable();
        if (oes != null) {
            oes.addFillListener(this::onOrderFill);
        } else {
            log.warn("[OptionSelling] OrderEventService not available at boot — fill capture will fall back to tradebook polling on exit");
        }

        log.info("[OptionSelling] boot — registered aggregator listener for {} (5-min bars). GDFL subscribe happens separately in GdflService on WS auth.", NIFTY_SYMBOL);
        log.info("[OptionSelling] booted — optionSellingEnabled={} openPositions={} ceEntriesToday={} peEntriesToday={} squareoff={}",
            riskSettings.isOptionSellingEnabled(),
            state.openPositions.size(),
            state.ceEntriesToday,
            state.peEntriesToday,
            riskSettings.getOptionSellingSquareOffTime());
    }

    private void pruneStaleEventsBeforeToday() {
        if (state.recentEvents == null || state.recentEvents.isEmpty()) return;
        long startOfTodayMillis = LocalDate.now(IST).atStartOfDay(IST).toInstant().toEpochMilli();
        int before = state.recentEvents.size();
        state.recentEvents.removeIf(e -> {
            Object ts = e.get("ts");
            return !(ts instanceof Number) || ((Number) ts).longValue() < startOfTodayMillis;
        });
        int removed = before - state.recentEvents.size();
        if (removed > 0) {
            log.info("[OptionSelling] pruned {} stale event(s) from before today's 00:00 IST", removed);
            saveToDisk();
        }
    }

    // ── Strategy interface ──────────────────────────────────────────────────

    @Override public String id() { return STRATEGY_ID; }
    @Override public String displayName() { return "NIFTY Options Selling"; }
    @Override public String description() { return "NIFTY futures 5-min VWAP rejection · sell ATM CE / PE"; }

    @Override public String currentState() {
        int open = state.openPositions.size();
        if (open == 0) return "IDLE";
        return "OPEN(" + open + ")";
    }

    @Override public boolean isEnabled() { return riskSettings.isOptionSellingEnabled(); }

    @Override
    public boolean forceClose(String reason) {
        // PortfolioRiskService calls this when aggregate liveNetPnlToday breaches
        // the daily budget — flatten every open OPTIONS SELLING position.
        String tag = reason == null ? "MANUAL" : reason;
        synchronized (this) {
            boolean anyClosed = false;
            for (SellingPosition p : new ArrayList<>(state.openPositions.values())) {
                if (p == null) continue;
                if (p.state == PosState.PENDING_ENTRY || p.state == PosState.IN_POSITION) {
                    fireExit(p, tag);
                    anyClosed = true;
                }
            }
            return anyClosed;
        }
    }

    /** Symbol-scoped force close — used by the /squareoff endpoint when the
     *  operator picks a single position from the Live Positions table. */
    public boolean forceCloseSymbol(String symbol, String reason) {
        if (symbol == null || symbol.isBlank()) return false;
        synchronized (this) {
            SellingPosition p = state.openPositions.get(symbol);
            if (p == null) return false;
            if (p.state != PosState.PENDING_ENTRY && p.state != PosState.IN_POSITION) return false;
            fireExit(p, reason == null ? "MANUAL" : reason);
            return true;
        }
    }

    @Override
    public void resetToIdle(String reason) {
        synchronized (this) {
            java.util.Set<String> uniqueSymbols = new java.util.HashSet<>();
            for (SellingPosition p : state.openPositions.values()) {
                if (p != null && p.symbol != null) uniqueSymbols.add(p.symbol);
            }
            for (String sym : uniqueSymbols) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            saveToDisk();
            event("[INFO]", "System", "reset — " + (reason == null ? "" : reason));
        }
    }

    @Override
    public List<Map<String, Object>> todayClosedTrades() {
        rolloverIfNewDay();
        synchronized (this) { return new ArrayList<>(state.todayClosedTrades); }
    }

    @Override
    public double liveNetPnlToday() {
        rolloverIfNewDay();
        synchronized (this) {
            double net = 0;
            for (Map<String, Object> m : state.todayClosedTrades) net += asDouble(m.get("netPnl"));
            for (SellingPosition p : state.openPositions.values()) {
                if (p == null) continue;
                net += openPositionMtm(p) - cycleChargesFor(p);
            }
            return round2(net);
        }
    }

    private double cycleChargesFor(SellingPosition p) {
        if (p == null || p.entryPrice <= 0) return 0.0;
        double entryTurnover = p.entryPrice * p.qty;
        double exitTurnover  = currentExitTurnover(p);
        // Short cycle: sell turnover = entry, buy turnover = exit
        return perCycleCharges(entryTurnover, exitTurnover);
    }

    @Override
    public double liveChargesToday() {
        rolloverIfNewDay();
        synchronized (this) {
            double ch = 0;
            for (Map<String, Object> m : state.todayClosedTrades) ch += asDouble(m.get("charges"));
            for (SellingPosition p : state.openPositions.values()) {
                ch += cycleChargesFor(p);
            }
            return round2(ch);
        }
    }

    @Override
    public void tick() {
        rolloverIfNewDay();
        checkSquareoff();
    }

    /** Called from the ~5 s scheduler. If wall-clock has crossed
     *  {@code optionSellingSquareOffTime}, flatten every open position via a
     *  MARKET BUY-TO-COVER. IN_POSITION → normal exit. PENDING_ENTRY → we place
     *  the exit anyway (in case Fyers filled the entry between our checks and
     *  we just didn't get the fill push yet); the tradebook will reconcile net
     *  position to zero. Squareoff runs regardless of the master kill switch —
     *  a live position must be flattened at the cutoff even if the operator
     *  turned the strategy OFF after entry. */
    private void checkSquareoff() {
        if (state.openPositions.isEmpty()) return;
        String sqStr = riskSettings.getOptionSellingSquareOffTime();
        if (sqStr == null || sqStr.isBlank()) return;
        LocalTime cutoff;
        try {
            cutoff = LocalTime.parse(sqStr);
        } catch (Exception e) {
            log.warn("[OptionSelling] bad optionSellingSquareOffTime '{}' — skipping", sqStr);
            return;
        }
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        if (now.isBefore(cutoff)) return;
        synchronized (this) {
            for (SellingPosition p : new ArrayList<>(state.openPositions.values())) {
                if (p == null) continue;
                if (p.state != PosState.PENDING_ENTRY && p.state != PosState.IN_POSITION) continue;
                event("[WARNING]", "Squareoff", "SQUAREOFF at " + sqStr + " — flattening " + p.symbol);
                fireExit(p, "SQUAREOFF");
            }
        }
    }

    @Override
    public void fastSlCheck() {
        // No tick-based SL for OPTIONS SELLING — SL is bar-close on the futures
        // leg, evaluated inside onFuturesBar.
    }

    // ── Candle close handler — SL check + trigger evaluation ─────────────────

    /** Registered on {@link CandleAggregator#subscribe} for the NIFTY futures
     *  symbol. Called on every 5-min bar close. Order matters:
     *  <ol>
     *    <li>Update each open position's SL counter (2 consecutive opposite-side
     *        closes → exit). The trigger bar itself is excluded because the
     *        counter starts from the bar AFTER entry — see
     *        {@link SellingPosition#entryBarStartMs}.</li>
     *    <li>Evaluate new triggers — CE-SELL / PE-SELL — subject to master kill
     *        switch, start-time gate, squareoff-time gate, per-side capacity,
     *        and per-side session cap.</li>
     *  </ol> */
    public void onFuturesBar(String symbol, Candle c) {
        if (c == null) return;
        if (!NIFTY_SYMBOL.equals(symbol)) return;
        double vwap = c.vwap();
        log.info("[OptionSelling] futures {}-min bar closed — {} o={} h={} l={} c={} vwap={} startMs={} open={}",
            CandleAggregator.BUCKET_MINUTES, symbol,
            c.open(), c.high(), c.low(), c.close(), vwap, c.startMillis(), state.openPositions.size());

        rolloverIfNewDay();

        synchronized (this) {
            // (1) SL check for every IN_POSITION. Skip positions whose
            // entryBarStartMs == this bar's startMillis (trigger bar excluded).
            if (vwap > 0) {
                for (SellingPosition p : new ArrayList<>(state.openPositions.values())) {
                    if (p == null || p.state != PosState.IN_POSITION) continue;
                    if (p.entryBarStartMs > 0 && p.entryBarStartMs == c.startMillis()) continue;
                    updateSlCounter(p, c);
                }
            }

            // (2) Trigger check. Master kill switch blocks NEW entries only.
            if (!riskSettings.isOptionSellingEnabled()) return;
            if (vwap <= 0) return;
            if (!isWithinTriggerWindow()) return;

            evaluateTrigger(c);
        }
    }

    /** Increment / reset a position's consecutive-opposite-close counter. */
    private void updateSlCounter(SellingPosition p, Candle c) {
        double close = c.close();
        double vwap  = c.vwap();
        boolean onCorrectSide;
        // Short CE: correct side means futures close BELOW VWAP (bearish thesis holds).
        // Short PE: correct side means futures close ABOVE VWAP (bullish thesis holds).
        if ("CE".equals(p.side)) {
            onCorrectSide = close < vwap;
        } else {
            onCorrectSide = close > vwap;
        }
        if (onCorrectSide) {
            if (p.consecutiveOppositeCloses > 0) {
                event("[INFO]", "SL-Watch",
                    p.symbol + " — futures closed on correct side (" + round2(close) + " vs vwap " + round2(vwap)
                        + ") — SL counter reset from " + p.consecutiveOppositeCloses + " to 0");
            }
            p.consecutiveOppositeCloses = 0;
            saveToDisk();
            return;
        }
        p.consecutiveOppositeCloses++;
        event("[WARNING]", "SL-Watch",
            p.symbol + " — futures closed on OPPOSITE side (" + round2(close) + " vs vwap " + round2(vwap)
                + ") — SL counter " + p.consecutiveOppositeCloses + "/2");
        if (p.consecutiveOppositeCloses >= 2) {
            fireExit(p, "SL_2_OPPOSITE_CLOSES");
        } else {
            saveToDisk();
        }
    }

    /** Check start-time and squareoff-time gates. Bars closing outside this
     *  window are skipped for NEW entries. Existing positions still get SL
     *  evaluation regardless. */
    private boolean isWithinTriggerWindow() {
        LocalTime now = ZonedDateTime.now(IST).toLocalTime();
        String startStr = riskSettings.getOptionSellingStartTime();
        if (startStr != null && !startStr.isBlank()) {
            try {
                LocalTime start = LocalTime.parse(startStr);
                if (now.isBefore(start)) return false;
            } catch (Exception ignored) {}
        }
        String sqStr = riskSettings.getOptionSellingSquareOffTime();
        if (sqStr != null && !sqStr.isBlank()) {
            try {
                LocalTime cutoff = LocalTime.parse(sqStr);
                if (!now.isBefore(cutoff)) return false;
            } catch (Exception ignored) {}
        }
        return true;
    }

    /** Evaluate whether this bar fires a CE-SELL or PE-SELL entry. Requires:
     *  <ul>
     *    <li>{@code low ≤ vwap ≤ high} — bar range spans VWAP (crossover or wick
     *        rejection).</li>
     *    <li>{@code close < vwap} → CE-SELL; {@code close > vwap} → PE-SELL.
     *        Equality is skipped.</li>
     *    <li>Same-side slot is free (serial per side).</li>
     *    <li>Per-side session cap not hit.</li>
     *  </ul> */
    private void evaluateTrigger(Candle c) {
        double close = c.close();
        double high  = c.high();
        double low   = c.low();
        double vwap  = c.vwap();

        boolean crossedVwap = low <= vwap && vwap <= high;
        if (!crossedVwap) return;

        if (close < vwap) {
            if (hasOpenPosition("CE")) {
                event("[INFO]", "Setup",
                    "CE-SELL trigger skipped — CE slot already open (close=" + round2(close)
                        + " vwap=" + round2(vwap) + ")");
                return;
            }
            int cap = riskSettings.getOptionSellingMaxCeSellsPerDay();
            if (cap > 0 && state.ceEntriesToday >= cap) {
                event("[INFO]", "Setup",
                    "CE-SELL cap hit (" + state.ceEntriesToday + "/" + cap + ") — trigger skipped");
                return;
            }
            fireEntry("CE", c);
        } else if (close > vwap) {
            if (hasOpenPosition("PE")) {
                event("[INFO]", "Setup",
                    "PE-SELL trigger skipped — PE slot already open (close=" + round2(close)
                        + " vwap=" + round2(vwap) + ")");
                return;
            }
            int cap = riskSettings.getOptionSellingMaxPeSellsPerDay();
            if (cap > 0 && state.peEntriesToday >= cap) {
                event("[INFO]", "Setup",
                    "PE-SELL cap hit (" + state.peEntriesToday + "/" + cap + ") — trigger skipped");
                return;
            }
            fireEntry("PE", c);
        }
        // close == vwap → no trigger (edge case, silently skipped)
    }

    private boolean hasOpenPosition(String side) {
        for (SellingPosition p : state.openPositions.values()) {
            if (p == null) continue;
            if (!side.equals(p.side)) continue;
            if (p.state == PosState.PENDING_ENTRY || p.state == PosState.IN_POSITION) return true;
        }
        return false;
    }

    /** Place a SELL market entry on the ATM strike of the requested side, register
     *  the position in {@code openPositions}, and increment the per-side counter.
     *  Rejections don't advance the counter. */
    private void fireEntry(String side, Candle c) {
        double close = c.close();
        long atmStrike = Math.round(close / (double) ATM_STEP) * ATM_STEP;
        MarketHolidayService holidays = holidayServiceProvider == null ? null : holidayServiceProvider.getIfAvailable();
        LocalDate expiry = NiftyExpiryResolver.currentWeeklyExpiry(LocalDate.now(IST), holidays);
        String fyersSymbol;
        try {
            fyersSymbol = NiftyOptionSymbolBuilder.buildFyersSymbol(expiry, atmStrike, side);
        } catch (Exception e) {
            event("[ERROR]", "Setup", "Symbol build failed for expiry=" + expiry + " strike=" + atmStrike + " side=" + side + ": " + e.getMessage());
            return;
        }

        // On-demand GDFL subscribe so option ticks land in MarketDataService for
        // open-position MTM tracking. Also mark it altFeed-owned so the primary
        // Fyers data feed doesn't fight over it.
        GdflService gdfl = gdflServiceProvider == null ? null : gdflServiceProvider.getIfAvailable();
        if (gdfl != null) {
            boolean subscribed = gdfl.subscribeSymbolOnDemand(fyersSymbol);
            log.info("[OptionSelling] GDFL subscribe for {} → {}", fyersSymbol, subscribed);
        } else {
            marketDataService.addAltFeedOwnedSymbol(fyersSymbol);
        }
        // Register a no-op candle listener so CandleAggregator starts bucketing
        // this option's ticks. We don't act on the option's own bar closes —
        // SL is bar-close on the futures — but keeping the aggregator active
        // means LTP + MTM refresh reliably.
        candleAggregator.subscribe(fyersSymbol, cc -> {});

        int qty = riskSettings.getOptionSellingLotsPerLeg() * LOT_SIZE;
        String productType = riskSettings.getOptionSellingOrderType();
        // Fyers side codes: 1 = BUY, -1 = SELL. Market order (type=2) via
        // placeOrder(sym, qty, side, stoploss=0, productType). SELL opens a short.
        OrderDTO placed = orderService.placeOrder(fyersSymbol, qty, -1, 0.0, productType);
        if (placed == null || !"ok".equals(placed.getStatus()) || placed.getId() == null || placed.getId().isBlank()) {
            String msg = placed == null ? "placeOrder returned null" : placed.getMessage();
            event("[ERROR]", "Reject",
                side + "-SELL entry rejected for " + fyersSymbol + " — no slot burned (" + msg + ")");
            return;
        }

        SellingPosition p = new SellingPosition();
        p.symbol            = fyersSymbol;
        p.side              = side;
        p.strike            = atmStrike;
        p.qty               = qty;
        p.state             = PosState.PENDING_ENTRY;
        p.entryOrderId      = placed.getId();
        p.productType       = productType;
        p.openMillis        = System.currentTimeMillis();
        p.entryBarStartMs   = c.startMillis();
        p.consecutiveOppositeCloses = 0;
        state.openPositions.put(fyersSymbol, p);

        if ("CE".equals(side)) state.ceEntriesToday++;
        else                   state.peEntriesToday++;

        event("[SUCCESS]", "Setup",
            "[Setup] " + side + " SELL — ATM " + atmStrike
                + " (futures close " + round2(close) + ", VWAP " + round2(c.vwap()) + ")"
                + " qty=" + qty + " orderId=" + placed.getId());
        saveToDisk();
    }

    /** Places a MARKET BUY-TO-COVER exit for {@code p} and marks it DONE. The
     *  actual fill (and cycle P&L booking) lands via {@link #onOrderFill}. */
    private void fireExit(SellingPosition p, String reason) {
        if (p == null || p.symbol == null || p.symbol.isBlank()) return;
        if (p.state == PosState.DONE) return;
        String productType = (p.productType == null || p.productType.isBlank())
            ? riskSettings.getOptionSellingOrderType()
            : p.productType;
        // Fyers side codes: 1 = BUY, -1 = SELL. BUY-TO-COVER = side +1.
        OrderDTO closed = orderService.placeExitOrder(p.symbol, p.qty, +1, productType);
        if (closed != null && "ok".equals(closed.getStatus()) && closed.getId() != null && !closed.getId().isBlank()) {
            p.exitOrderId = closed.getId();
            event("[SUCCESS]", "Exit",
                p.symbol + " — " + reason + " — BUY-TO-COVER (orderId=" + closed.getId() + ")");
        } else {
            String msg = closed == null ? "placeExitOrder returned null" : closed.getMessage();
            event("[ERROR]", "Exit",
                p.symbol + " — " + reason + " — exit order FAILED (" + msg + "). Manual intervention required.");
            // Don't remove from openPositions on failure — leave it so the
            // operator sees it in the UI and can force-close manually.
            return;
        }
        // Mark DONE — position is removed from openPositions when the exit fill
        // lands in onOrderFill (so MTM tracking keeps working until the broker
        // confirms the flatten). If the fill push never arrives, the squareoff
        // path or manual intervention picks it up.
        saveToDisk();
    }

    // ── Order fill listener — captures entry price + books exit P&L ─────────

    /** Registered on {@link OrderEventService#addFillListener}. Two transitions
     *  land here:
     *  <ul>
     *    <li>Entry order fills — populate {@code entryPrice}, promote state to
     *        {@code IN_POSITION}.</li>
     *    <li>Exit order fills — compute realised P&L, append cycle to
     *        {@code todayClosedTrades}, persist DB row, remove position from
     *        {@code openPositions}.</li>
     *    <li>Unknown order id — belongs to OPTIONS BUYING or another strategy;
     *        silently ignored.</li>
     *  </ul> */
    void onOrderFill(String orderId, double price) {
        if (orderId == null || orderId.isBlank() || price <= 0) return;
        synchronized (this) {
            // Entry fill lookup — scan openPositions.
            for (SellingPosition p : state.openPositions.values()) {
                if (p == null) continue;
                if (orderId.equals(p.entryOrderId) && p.state == PosState.PENDING_ENTRY) {
                    p.entryPrice = price;
                    p.state      = PosState.IN_POSITION;
                    event("[SUCCESS]", "Fill",
                        p.symbol + " — entry SELL filled @ Rs." + round2(price)
                            + " (qty=" + p.qty + ")");
                    saveToDisk();
                    return;
                }
            }
            // Exit fill lookup — scan openPositions.
            for (SellingPosition p : new ArrayList<>(state.openPositions.values())) {
                if (p == null) continue;
                if (orderId.equals(p.exitOrderId)) {
                    bookCycleClose(p, price, resolveCloseReason(p));
                    return;
                }
            }
            // Unknown orderId — foreign strategy fill, silently ignore.
        }
    }

    /** Reason attribution for a cycle close is best-effort — the fireExit path
     *  writes it into the event log at exit time. For the DB row we default to
     *  "FSM_EXIT" (parallels OPTION BUYING) — the event log is the source of
     *  truth for the specific trigger. */
    private String resolveCloseReason(SellingPosition p) {
        return "FSM_EXIT";
    }

    private void bookCycleClose(SellingPosition p, double exitPrice, String reason) {
        double entry = p.entryPrice;
        double gross = (entry - exitPrice) * p.qty;  // short cycle
        double sellTurnover = entry     * p.qty;      // short entry = sell
        double buyTurnover  = exitPrice * p.qty;      // short exit  = buy
        double charges = perCycleCharges(sellTurnover, buyTurnover);
        double net = gross - charges;
        long closedAtMillis = System.currentTimeMillis();

        Map<String, Object> cycle = new LinkedHashMap<>();
        cycle.put("strategyId",     STRATEGY_ID);
        cycle.put("setup",          p.side == null ? "" : (p.side + "_SELL"));
        cycle.put("symbol",         p.symbol);
        cycle.put("side",           "SELL");
        cycle.put("qty",            p.qty);
        cycle.put("entryPrice",     round2(entry));
        cycle.put("exitPrice",      round2(exitPrice));
        cycle.put("grossPnl",       round2(gross));
        cycle.put("charges",        round2(charges));
        cycle.put("netPnl",         round2(net));
        cycle.put("closeReason",    reason);
        cycle.put("closedAtMillis", closedAtMillis);
        cycle.put("openedAtMillis", p.openMillis);
        cycle.put("entryCandleMs",  p.entryBarStartMs);
        state.todayClosedTrades.add(cycle);
        while (state.todayClosedTrades.size() > 100) state.todayClosedTrades.remove(0);

        persistTradeRow(STRATEGY_ID, p.symbol, p.side + "_SELL", reason, p.qty,
            gross, charges, net,
            "SL_2_OPPOSITE_CLOSES".equals(reason) ? 1 : 0,
            closedAtMillis, p.openMillis, entry, exitPrice,
            p.entryBarStartMs, 0L);

        event(net >= 0 ? "[SUCCESS]" : "[WARNING]", "Fill",
            p.symbol + " exit BUY-TO-COVER filled @ Rs." + round2(exitPrice)
                + " — gross Rs." + round2(gross)
                + " charges Rs." + round2(charges)
                + " net Rs." + round2(net));

        p.state = PosState.DONE;
        state.openPositions.remove(p.symbol);
        // Drop candle subscription for this option leg (never the futures leg).
        if (!NIFTY_SYMBOL.equals(p.symbol)) {
            candleAggregator.unsubscribe(p.symbol);
        }
        saveToDisk();
    }

    private void persistTradeRow(String strategyId, String symbol, String setup, String reason, int qty,
                                 double gross, double charges, double net, int slHits,
                                 long closedAtMillis, long openedAtMillis,
                                 double entryPrice, double exitPrice,
                                 long entryCandleMs, long exitCandleMs) {
        try {
            StrategyTradeRepository repo = tradeRepoProvider == null ? null : tradeRepoProvider.getIfAvailable();
            if (repo == null) return;
            LocalDate sessionDate = LocalDate.now(IST);
            StrategyTradeEntity row = new StrategyTradeEntity();
            row.setStrategyId(strategyId == null ? STRATEGY_ID : strategyId);
            row.setSymbol(symbol);
            row.setSetup(setup);
            row.setSessionDate(sessionDate.toString());
            row.setClosedAtMillis(closedAtMillis);
            row.setOpenedAtMillis(openedAtMillis);
            row.setInstrument("NIFTY");
            row.setQty(qty);
            row.setGrossPnl(round2(gross));
            row.setCharges(round2(charges));
            row.setNetPnl(round2(net));
            row.setCloseReason(reason);
            row.setSlHitCount(slHits);
            row.setEntryPrice(entryPrice > 0 ? round2(entryPrice) : null);
            row.setExitPrice(exitPrice   > 0 ? round2(exitPrice)  : null);
            row.setEntryCandleMs(entryCandleMs > 0 ? entryCandleMs : null);
            row.setExitCandleMs (exitCandleMs  > 0 ? exitCandleMs  : null);
            repo.save(row);
        } catch (Exception e) {
            log.warn("[OptionSelling] persist trade failed: {}", e.getMessage());
        }
    }

    // ── Day rollover ─────────────────────────────────────────────────────────

    @Scheduled(cron = "0 0 6 * * *", zone = "Asia/Kolkata")
    public synchronized void scheduledDailyReset() {
        String today = LocalDate.now(IST).toString();
        log.info("[OptionSelling] 06:00 IST daily reset (was dayKey={})", state.dayKey);
        state.dayKey = today;
        state.ceEntriesToday = 0;
        state.peEntriesToday = 0;
        state.todayClosedTrades.clear();
        if (state.recentEvents != null) state.recentEvents.clear();
        java.util.Set<String> uniqSymbols = new java.util.HashSet<>();
        for (SellingPosition p : state.openPositions.values()) {
            if (p != null && p.symbol != null) uniqSymbols.add(p.symbol);
        }
        for (String sym : uniqSymbols) {
            candleAggregator.unsubscribe(sym);
        }
        state.openPositions.clear();
        saveToDisk();
    }

    private void rolloverIfNewDay() {
        String today = LocalDate.now(IST).toString();
        if (today.equals(state.dayKey)) return;
        synchronized (this) {
            if (today.equals(state.dayKey)) return;
            state.dayKey = today;
            state.ceEntriesToday = 0;
            state.peEntriesToday = 0;
            state.todayClosedTrades.clear();
            if (state.recentEvents != null) state.recentEvents.clear();
            java.util.Set<String> uniqSymbolsRoll = new java.util.HashSet<>();
            for (SellingPosition p : state.openPositions.values()) {
                if (p != null && p.symbol != null) uniqSymbolsRoll.add(p.symbol);
            }
            for (String sym : uniqSymbolsRoll) {
                candleAggregator.unsubscribe(sym);
            }
            state.openPositions.clear();
            saveToDisk();
        }
    }

    // ── Charges ──────────────────────────────────────────────────────────────

    /** Per-cycle charges — mirrors OptionBuying's implementation. Duplicated
     *  intentionally: the plan explicitly permits it ("do NOT reach across
     *  strategies"). Costs the same regulatory formula: broker × 2 + STT (sell
     *  side) + exchange + SEBI + stamp + GST. */
    private double perCycleCharges(double sellTurnover, double buyTurnover) {
        double broker = riskSettings.getBrokeragePerOrder() * 2;
        double stt = sellTurnover * riskSettings.getSttRate() / 100.0;
        double total = sellTurnover + buyTurnover;
        double exch = total * riskSettings.getExchangeRate() / 100.0;
        double sebi = total / 1e7 * riskSettings.getSebiRate();
        double stamp = buyTurnover * riskSettings.getStampDutyRate() / 100.0;
        double gst = (broker + exch) * riskSettings.getGstRate() / 100.0;
        return round2(broker + stt + exch + sebi + stamp + gst);
    }

    private double currentExitTurnover(SellingPosition p) {
        try {
            double ltp = marketDataService.getLtp(p.symbol);
            if (ltp > 0) return ltp * p.qty;
        } catch (Exception ignored) {}
        return p.entryPrice * p.qty;
    }

    /** MTM (rupees) for a short position: {@code (entryPrice - ltp) × qty}.
     *  Zero when entryPrice hasn't been captured yet (PENDING_ENTRY) or the LTP
     *  hasn't landed. */
    private double openPositionMtm(SellingPosition p) {
        if (p == null || p.entryPrice <= 0) return 0;
        try {
            double ltp = marketDataService.getLtp(p.symbol);
            if (ltp <= 0) return 0;
            return (p.entryPrice - ltp) * p.qty;
        } catch (Exception e) {
            return 0;
        }
    }

    /** Rupees consumed by realised losses (closed cycles) + current unrealised
     *  drawdown on open positions. Positive = money burned. */
    private double consumedRiskToday() {
        double consumed = 0.0;
        synchronized (this) {
            for (Map<String, Object> m : state.todayClosedTrades) {
                double pnl = asDouble(m.get("netPnl"));
                if (pnl < 0) consumed += -pnl;
            }
            for (SellingPosition p : state.openPositions.values()) {
                double mtm = openPositionMtm(p);
                if (mtm < 0) consumed += -mtm;
            }
        }
        return consumed;
    }

    // ── Dashboard payload (consumed by OptionSellingController + Trade page) ─

    public synchronized Map<String, Object> dashboardState() {
        rolloverIfNewDay();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("strategy",          STRATEGY_ID);
        m.put("enabled",           isEnabled());
        m.put("lifecycle",         currentState());
        m.put("dayKey",            state.dayKey);
        m.put("ceEntriesToday",    state.ceEntriesToday);
        m.put("peEntriesToday",    state.peEntriesToday);
        m.put("maxCeSellsPerDay",  riskSettings.getOptionSellingMaxCeSellsPerDay());
        m.put("maxPeSellsPerDay",  riskSettings.getOptionSellingMaxPeSellsPerDay());
        m.put("squareOffTime",     riskSettings.getOptionSellingSquareOffTime());
        m.put("startTime",         riskSettings.getOptionSellingStartTime());

        // Open positions — one row per open OPTIONS SELLING short.
        List<Map<String, Object>> rows = new ArrayList<>();
        for (SellingPosition p : state.openPositions.values()) {
            if (p == null) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            double ltp = 0;
            try { ltp = marketDataService.getLtp(p.symbol); } catch (Exception ignored) {}
            double mtm = openPositionMtm(p);
            row.put("symbol",         p.symbol);
            row.put("setup",          p.side == null ? "" : (p.side + "_SELL"));
            row.put("side",           p.side == null ? "" : (p.side + "_SELL"));
            row.put("qty",            p.qty);
            row.put("entryPrice",     round2(p.entryPrice));
            row.put("ltp",            round2(ltp));
            row.put("mtm",            round2(mtm));
            row.put("targetLevel",    0.0);   // no target for OPTIONS SELLING
            row.put("slLevel",        0.0);   // SL is bar-close based, not price-level
            row.put("breakevenMoved", false);
            row.put("isShort",        true);
            row.put("openMillis",     p.openMillis);
            row.put("entryCandleMs",  p.entryBarStartMs);
            row.put("state",          p.state == null ? "" : p.state.name());
            row.put("consecutiveOppositeCloses", p.consecutiveOppositeCloses);
            rows.add(row);
        }
        m.put("openPositions", rows);

        Map<String, Object> risk = new LinkedHashMap<>();
        risk.put("consumedRisk",    round2(consumedRiskToday()));
        risk.put("dailyRiskBudget", round2(riskSettings.getPortfolioMaxDailyLoss()));
        m.put("risk", risk);

        m.put("todayClosedTrades", new ArrayList<>(state.todayClosedTrades));
        m.put("recentEvents",      new ArrayList<>(state.recentEvents));
        return m;
    }

    // ── State persistence ───────────────────────────────────────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class State {
        public String dayKey = "";
        /** Session count of CE-SELL entries successfully sent (rejections excluded).
         *  Resets at IST day rollover. */
        public int    ceEntriesToday;
        public int    peEntriesToday;
        /** Open positions keyed by Fyers option symbol. Serial per side is
         *  enforced by {@link OptionSelling#hasOpenPosition} — the map itself
         *  can hold at most 1 CE + 1 PE by construction. */
        public Map<String, SellingPosition> openPositions = new ConcurrentHashMap<>();
        public List<Map<String, Object>> todayClosedTrades = new ArrayList<>();
        public List<Map<String, Object>> recentEvents      = new ArrayList<>();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SellingPosition {
        public String   symbol = "";
        /** {@code "CE"} or {@code "PE"} — direction of the short leg. */
        public String   side   = "";
        public long     strike;
        public int      qty;
        public PosState state = PosState.PENDING_ENTRY;
        public String   entryOrderId = "";
        /** Fyers-reported fill price on the entry. Populated in
         *  {@link OptionSelling#onOrderFill} at the PENDING_ENTRY → IN_POSITION
         *  transition. */
        public double   entryPrice;
        public String   exitOrderId = "";
        public long     openMillis;
        /** Bucket-start millis of the futures 5-min bar that fired the trigger.
         *  Used to EXCLUDE the trigger bar from the SL counter (counter starts
         *  from the bar AFTER entry). */
        public long     entryBarStartMs;
        /** Consecutive futures 5-min bar closes on the OPPOSITE side of VWAP
         *  relative to this position's short thesis. Resets on any correct-side
         *  close. On ≥ 2 → SL exit. */
        public int      consecutiveOppositeCloses;
        public String   productType = "";
    }

    // ── Event log ────────────────────────────────────────────────────────────

    /** Public event-log wrapper for external callers (e.g. the kill-switch toggle in
     *  OptionSellingController). */
    public void postEvent(String severity, String source, String message) {
        event(severity, source, message);
    }

    private void event(String severity, String source, String message) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts",       System.currentTimeMillis());
        e.put("severity", severity);
        e.put("source",   source);
        e.put("message",  message);
        state.recentEvents.add(0, e);
        while (state.recentEvents.size() > RECENT_EVENTS_LIMIT) state.recentEvents.remove(state.recentEvents.size() - 1);
        if (eventService != null) eventService.log(severity + " [option-selling:" + source + "] " + message);
    }

    // ── Persistence ─────────────────────────────────────────────────────────

    private synchronized void loadFromDisk() {
        try {
            Path p = Path.of(STATE_FILE);
            if (!Files.exists(p)) return;
            State s = mapper.readValue(Files.readString(p), State.class);
            if (s != null) {
                state = s;
                if (state.openPositions == null)     state.openPositions     = new ConcurrentHashMap<>();
                if (state.todayClosedTrades == null) state.todayClosedTrades = new ArrayList<>();
                if (state.recentEvents == null)      state.recentEvents      = new ArrayList<>();
            }
        } catch (IOException e) {
            log.warn("[OptionSelling] failed to load state: {}", e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        try {
            Path dst = Path.of(STATE_FILE);
            File parent = dst.toFile().getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            Path tmp = Path.of(STATE_FILE + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state));
            com.rydytrader.autotrader.util.FileIoUtils.atomicMoveWithRetry(tmp, dst);
        } catch (IOException e) {
            log.warn("[OptionSelling] failed to save state: {}", e.getMessage());
        }
    }

    // ── Misc utility ────────────────────────────────────────────────────────

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private static double asDouble(Object o) {
        if (o instanceof Number) return ((Number) o).doubleValue();
        if (o == null) return 0;
        try { return Double.parseDouble(o.toString()); } catch (Exception e) { return 0; }
    }
}
