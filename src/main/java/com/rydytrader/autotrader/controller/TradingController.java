package com.rydytrader.autotrader.controller;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.dto.ProcessedSignal;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.AtrService;
import com.rydytrader.autotrader.service.BhavcopyService;
import com.rydytrader.autotrader.service.SmaService;
import com.rydytrader.autotrader.service.BreakoutScanner;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.LatencyTracker;
import com.rydytrader.autotrader.service.MarginDataService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.service.SignalProcessor;
import com.rydytrader.autotrader.service.TradeHistoryService;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TradingStateStore;

@RestController
public class TradingController {

    private static final Logger log = LoggerFactory.getLogger(TradingController.class);

    private final PollingService      pollingService;
    private final OrderService        orderService;
    private final EventService        eventService;
    private final TradeHistoryService tradeHistoryService;
    private final PositionStateStore  positionStateStore;
    private final RiskSettingsStore   riskSettings;
    private final TradingStateStore   tradingState;
    private final SignalProcessor     signalProcessor;
    private final MarketDataService   marketDataService;
    private final OrderEventService   orderEventService;
    private final CandleAggregator    candleAggregator;
    private final AtrService          atrService;
    private final LatencyTracker      latencyTracker;
    private final BreakoutScanner     breakoutScanner;
    private final MarginDataService   marginDataService;
    private final SmaService          smaService;
    private final BhavcopyService     bhavcopyService;

    public TradingController(PollingService pollingService,
                              OrderService orderService,
                              EventService eventService,
                              TradeHistoryService tradeHistoryService,
                              PositionStateStore positionStateStore,
                              RiskSettingsStore riskSettings,
                              TradingStateStore tradingState,
                              SignalProcessor signalProcessor,
                              MarketDataService marketDataService,
                              OrderEventService orderEventService,
                              CandleAggregator candleAggregator,
                              AtrService atrService,
                              LatencyTracker latencyTracker,
                              BreakoutScanner breakoutScanner,
                              MarginDataService marginDataService,
                              SmaService smaService,
                              BhavcopyService bhavcopyService) {
        this.pollingService      = pollingService;
        this.orderService        = orderService;
        this.eventService        = eventService;
        this.tradeHistoryService = tradeHistoryService;
        this.positionStateStore  = positionStateStore;
        this.riskSettings        = riskSettings;
        this.tradingState        = tradingState;
        this.signalProcessor     = signalProcessor;
        this.marketDataService   = marketDataService;
        this.orderEventService   = orderEventService;
        this.candleAggregator    = candleAggregator;
        this.atrService          = atrService;
        this.latencyTracker      = latencyTracker;
        this.breakoutScanner     = breakoutScanner;
        this.marginDataService   = marginDataService;
        this.smaService          = smaService;
        this.bhavcopyService     = bhavcopyService;
    }

    // ── PLACE ORDER ───────────────────────────────────────────────────────────
    // Payload: { "signal": "BUY"|"SELL", "symbol": "NSE:NIFTY25JUNFUT",
    //            "quantity": 50, "stoploss": 25450.0, "target": 25700.0 }
    @PostMapping("/placeorder")
    public ResponseEntity<String> receiveSignal(@RequestBody Map<String, Object> payload) {

        // Extract symbol + setup early for log context — used by all pre-processor skip logs
        // below so they match the "SYMBOL SETUP — reason" format used by SignalProcessor.
        String symbolForLog = payload.containsKey("symbol") ? payload.get("symbol").toString() : "";
        String setupForLog  = payload.containsKey("setup")  ? payload.get("setup").toString()  : "";
        String sigCtx       = symbolForLog + (setupForLog.isEmpty() ? "" : " " + setupForLog);

        // ── KILL SWITCH ──────────────────────────────────────────────────────────
        if (!tradingState.isTradingEnabled()) {
            String msg = "Signal ignored — trading is disabled (kill switch active)";
            eventService.log("[SIGNAL] " + sigCtx + " ignored — trading is disabled (kill switch active)");
            return ResponseEntity.ok(msg);
        }

        // ── RISK CHECKS ───────────────────────────────────────────────────────────
        // 1. Trading hours
        LocalTime now   = LocalTime.now();
        LocalTime start = LocalTime.parse(riskSettings.getTradingStartTime());
        LocalTime end   = LocalTime.parse(riskSettings.getTradingEndTime());
        if (now.isBefore(start) || now.isAfter(end)) {
            String msg = "Signal ignored — outside trading hours " + riskSettings.getTradingStartTime()
                       + "–" + riskSettings.getTradingEndTime() + " [now: " + now + "]";
            eventService.log("[SIGNAL] " + sigCtx + " ignored — outside trading hours "
                + riskSettings.getTradingStartTime() + "–" + riskSettings.getTradingEndTime() + " [now: " + now + "]");
            return ResponseEntity.ok(msg);
        }

        // 2. Risk exposure check (only in risk-based mode, not fixed quantity)
        int fixedQty = riskSettings.getFixedQuantity();
        double riskPerTrade = riskSettings.getRiskPerTrade();
        double maxLoss = riskSettings.getMaxDailyLoss(); // auto-calculated: totalCapital × maxRiskPerDayPct / 100
        if (fixedQty == -1 && riskPerTrade > 0 && maxLoss > 0) {
            // Open position risk (0 if SL has been trailed past entry — profit locked)
            double openRisk = 0;
            for (String sym : PositionManager.getAllSymbols()) {
                Map<String, Object> state = positionStateStore.load(sym);
                if (state != null && state.containsKey("slPrice") && state.containsKey("avgPrice")) {
                    double sl = Double.parseDouble(state.get("slPrice").toString());
                    double avg = Double.parseDouble(state.get("avgPrice").toString());
                    int qty = Integer.parseInt(state.get("qty").toString());
                    String side = state.containsKey("side") ? state.get("side").toString() : "";
                    // Only count risk if SL is on the loss side of entry
                    double risk = 0;
                    if ("LONG".equals(side) && sl < avg) {
                        risk = qty * (avg - sl);
                    } else if ("SHORT".equals(side) && sl > avg) {
                        risk = qty * (sl - avg);
                    }
                    // If SL >= entry (LONG) or SL <= entry (SHORT), risk = 0 (profit locked)
                    openRisk += risk;
                }
            }
            // Consumed risk: net realized P&L (profits replenish the budget)
            // If net positive (winning day), consumed risk = 0 (full budget available)
            double netRealizedPnl = tradeHistoryService.getTrades().stream()
                .mapToDouble(t -> t.getNetPnl()).sum();
            double consumedRisk = netRealizedPnl < 0 ? Math.abs(netRealizedPnl) : 0;
            double totalRisk = openRisk + consumedRisk;
            if (totalRisk + riskPerTrade > maxLoss) {
                String msg = "Signal ignored — risk exposure limit reached (open: ₹" + (int)openRisk
                    + " + losses: ₹" + (int)consumedRisk
                    + " + new: ₹" + (int)riskPerTrade + " > limit: ₹" + (int)maxLoss + ")";
                eventService.log("[SIGNAL] " + sigCtx + " ignored — risk exposure limit reached (open: ₹"
                    + (int)openRisk + " + losses: ₹" + (int)consumedRisk
                    + " + new: ₹" + (int)riskPerTrade + " > limit: ₹" + (int)maxLoss + ")");
                return ResponseEntity.ok(msg);
            }
        }

        ProcessedSignal ps = signalProcessor.process(payload);
        String psSymbol = ps.getSymbol();
        latencyTracker.mark(psSymbol, ps.getSetup(), LatencyTracker.Stage.SIGNAL_PROCESSED);
        if (ps.isRejected()) {
            latencyTracker.cancel(psSymbol);
            // Silent rejections (native-LPT when LPT disabled, etc.) skip the event log —
            // reserved for expected skips the user has already opted out of via settings.
            if (!ps.isSilent()) {
                // Include the candlestick pattern (e.g. ENGULFING_RETEST) so the trader can
                // see which pattern formed AND why downstream filtering rejected it.
                Object routeObj = payload.get("scannerNote");
                String routeSuffix = (routeObj instanceof String && !((String) routeObj).isEmpty())
                    ? " [" + routeObj + "]" : "";
                eventService.log("[SIGNAL] " + psSymbol + " " + ps.getSetup() + routeSuffix + " filtered — " + ps.getRejectionReason());
            }
            return ResponseEntity.ok("Signal filtered: " + (ps.getRejectionReason() != null ? ps.getRejectionReason() : "silent"));
        }
        String signal      = ps.getSignal();
        String symbol      = ps.getSymbol();
        int    quantity    = ps.getQuantity();
        double stoploss    = ps.getStoploss();
        double target      = ps.getTarget();
        String setup       = ps.getSetup();
        String probability = ps.getProbability();
        double atr         = ps.getAtr();
        double atrMult     = ps.getAtrMultiplier();
        String description = ps.getDescription();
        boolean rescueShifted = ps.isRescueShifted();
        boolean useStructuralSl = ps.isUseStructuralSl();

        log.info("Signal received: {} | SL: {} | Target: {} | Setup: {}", signal, stoploss, target, setup);

        if (signal.equals("BUY") && !PositionManager.getPosition(symbol).equals("LONG")) {

            latencyTracker.mark(symbol, setup, LatencyTracker.Stage.ORDER_PLACED);
            OrderDTO order = orderService.placeOrder(symbol, quantity, 1, stoploss);
            latencyTracker.mark(symbol, setup, LatencyTracker.Stage.ORDER_RESPONSE);
            if (order == null || order.getId() == null || order.getId().isEmpty()) {
                String msg = order != null ? order.getMessage() : "null response";
                eventService.log("[ERROR] BUY order placement failed for " + symbol + ": " + msg);
                return ResponseEntity.ok("Order failed: " + msg);
            }

            // Monitor entry fill, then place SL + Target OCO
            // exitSide = -1 (SELL to exit a LONG)
            pollingService.monitorEntryAndPlaceOCO(order, symbol, quantity, "LONG", -1, stoploss, target, setup, atr, atrMult, description, rescueShifted, useStructuralSl);
            pollingService.setProbability(symbol, probability);

        } else if (signal.equals("SELL") && !PositionManager.getPosition(symbol).equals("SHORT")) {

            latencyTracker.mark(symbol, setup, LatencyTracker.Stage.ORDER_PLACED);
            OrderDTO order = orderService.placeOrder(symbol, quantity, -1, stoploss);
            latencyTracker.mark(symbol, setup, LatencyTracker.Stage.ORDER_RESPONSE);
            if (order == null || order.getId() == null || order.getId().isEmpty()) {
                String msg = order != null ? order.getMessage() : "null response";
                eventService.log("[ERROR] SELL order placement failed for " + symbol + ": " + msg);
                return ResponseEntity.ok("Order failed: " + msg);
            }

            // exitSide = 1 (BUY to exit a SHORT)
            pollingService.monitorEntryAndPlaceOCO(order, symbol, quantity, "SHORT", 1, stoploss, target, setup, atr, atrMult, description, rescueShifted, useStructuralSl);
            pollingService.setProbability(symbol, probability);

        } else {
            String msg = "Signal ignored — existing position: " + PositionManager.getPosition(symbol);
            log.info(msg);
            eventService.log("[SIGNAL] " + sigCtx + " ignored — existing position: " + PositionManager.getPosition(symbol));
        }

        return ResponseEntity.ok("Signal processed");
    }

    // ── SQUARE OFF ───────────────────────────────────────────────────────────────
    @PostMapping("/api/squareoff")
    public ResponseEntity<Map<String, Object>> squareOff(@RequestBody Map<String, Object> payload) {
        String symbol   = payload.get("symbol").toString();
        int    quantity = Integer.parseInt(payload.get("quantity").toString());

        boolean success = pollingService.squareOff(symbol, quantity);
        return ResponseEntity.ok(Map.of("ok", success));
    }

    // ── POSITIONS ─────────────────────────────────────────────────────────────
    @GetMapping("/api/positions")
    public Map<String, Object> getPositions() {
        List<Map<String, Object>> positions = pollingService.fetchPositions().stream().map(p -> {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            // Load persisted state first — authoritative source for qty/SL/target after T1 fills
            Map<String, Object> state = positionStateStore.load(p.getSymbol());
            // Use state as authoritative source for qty and avg after T1 fill. Fyers polling
            // can briefly return stale pre-T1 qty, and we don't want to trust t1Filled flag
            // alone (it can lag the remainingQty update). Checking remainingQty > 0 covers
            // both signals — if state has positive remainingQty, T1 has booked and the
            // remaining leg is what's open.
            int displayQty = p.getQty();
            double avgForPnl = p.getAvgPrice();
            if (state != null) {
                try {
                    int remaining = Integer.parseInt(state.getOrDefault("remainingQty", "0").toString());
                    if (remaining > 0) displayQty = remaining;
                } catch (NumberFormatException ignored) {}
                try {
                    double stateAvg = Double.parseDouble(state.getOrDefault("avgPrice", "0").toString());
                    if (stateAvg > 0) avgForPnl = stateAvg;
                } catch (NumberFormatException ignored) {}
            }
            // Overlay live LTP from WebSocket if available
            double liveLtp = marketDataService.getLtp(p.getSymbol());
            double ltp = liveLtp > 0 ? liveLtp : p.getLtp();
            double pnl = "LONG".equals(p.getSide())
                ? (ltp - avgForPnl) * displayQty
                : (avgForPnl - ltp) * displayQty;
            m.put("symbol",    p.getSymbol());
            m.put("qty",       displayQty);
            m.put("side",      p.getSide());
            // Use avgForPnl (state-authoritative) so the displayed avg matches the P&L calc.
            // Fyers polling returns a re-weighted avg after T1 sells (e.g. 490 instead of the
            // 500 entry price), which alternated with the state value as polling/state synced
            // at different rates. State avgPrice is the entry cost basis for the remaining qty.
            m.put("avgPrice",  avgForPnl);
            m.put("ltp",       ltp);
            m.put("pnl",       pnl);
            m.put("setup",     p.getSetup());
            m.put("entryTime", p.getEntryTime());
            m.put("description", state != null ? state.getOrDefault("description", "") : "");
            // Probability: prefer in-memory cache (live during the session), fall back to the
            // persisted PositionStateStore value so the table stays correct after restarts.
            String probDisplay = pollingService.getProbability(p.getSymbol());
            if ((probDisplay == null || probDisplay.isEmpty()) && state != null) {
                Object stateProb = state.get("probability");
                if (stateProb != null) probDisplay = stateProb.toString();
            }
            m.put("probability", probDisplay != null ? probDisplay : "");
            // SL and target prices from persisted state
            if (state != null) {
                try { m.put("slPrice", Double.parseDouble(state.getOrDefault("slPrice", "0").toString())); } catch (NumberFormatException e) { m.put("slPrice", 0.0); }
                try { m.put("targetPrice", Double.parseDouble(state.getOrDefault("targetPrice", "0").toString())); } catch (NumberFormatException e) { m.put("targetPrice", 0.0); }
            } else {
                m.put("slPrice", 0.0);
                m.put("targetPrice", 0.0);
            }
            // Split target fields
            if (state != null) {
                try { m.put("target1Price", Double.parseDouble(state.getOrDefault("target1Price", "0").toString())); } catch (NumberFormatException e) { m.put("target1Price", 0.0); }
                try { m.put("target2Price", Double.parseDouble(state.getOrDefault("target2Price", "0").toString())); } catch (NumberFormatException e) { m.put("target2Price", 0.0); }
                m.put("t1Filled", Boolean.TRUE.equals(state.get("t1Filled")));
                try { m.put("remainingQty", Integer.parseInt(state.getOrDefault("remainingQty", "0").toString())); } catch (NumberFormatException e) { m.put("remainingQty", 0); }
            }
            m.put("leverage", marginDataService.getLeverage(p.getSymbol()));
            m.put("slTrailed", marketDataService.isTrailed(p.getSymbol()));
            return m;
        }).collect(java.util.stream.Collectors.toList());
        double realizedPnl   = tradeHistoryService.getTrades().stream()
            .mapToDouble(t -> t.getNetPnl()).sum();
        double unrealizedPnl = positions.stream()
            .mapToDouble(p -> ((Number) p.get("pnl")).doubleValue()).sum();
        double netDayPnl     = realizedPnl + unrealizedPnl;

        // Risk budget info (0 if SL trailed past entry — profit locked)
        double openRisk = 0;
        for (String sym : PositionManager.getAllSymbols()) {
            Map<String, Object> state = positionStateStore.load(sym);
            if (state != null && state.containsKey("slPrice") && state.containsKey("avgPrice")) {
                double sl = Double.parseDouble(state.get("slPrice").toString());
                double avg = Double.parseDouble(state.get("avgPrice").toString());
                int qty = Integer.parseInt(state.get("qty").toString());
                String side = state.containsKey("side") ? state.get("side").toString() : "";
                double risk = 0;
                if ("LONG".equals(side) && sl < avg) {
                    risk = qty * (avg - sl);
                } else if ("SHORT".equals(side) && sl > avg) {
                    risk = qty * (sl - avg);
                }
                openRisk += risk;
            }
        }
        // Consumed risk: net realized P&L (profits replenish the budget)
        double netRealizedPnl2 = tradeHistoryService.getTrades().stream()
            .mapToDouble(t -> t.getNetPnl()).sum();
        double consumedRisk = netRealizedPnl2 < 0 ? Math.abs(netRealizedPnl2) : 0;
        double maxDailyLoss = riskSettings.getMaxDailyLoss();

        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("positions",    positions);
        result.put("lastSync",     pollingService.getLastSyncTime());
        result.put("realizedPnl",  Math.round(realizedPnl   * 100.0) / 100.0);
        result.put("unrealizedPnl",Math.round(unrealizedPnl * 100.0) / 100.0);
        result.put("netDayPnl",    Math.round(netDayPnl     * 100.0) / 100.0);
        result.put("openRisk",     Math.round(openRisk      * 100.0) / 100.0);
        result.put("consumedRisk", Math.round(consumedRisk  * 100.0) / 100.0);
        result.put("maxDailyLoss", Math.round(maxDailyLoss  * 100.0) / 100.0);
        return result;
    }

    // ── STATUS ────────────────────────────────────────────────────────────────
    @GetMapping("/status")
    public Map<String, String> getStatus() {
        return Map.of("status", pollingService.getConnectionStatus());
    }

    @GetMapping("/api/health")
    public Map<String, Object> getHealth() {
        Map<String, Object> health = new java.util.LinkedHashMap<>();

        // WebSocket status
        health.put("dataWs", marketDataService.isConnected() ? "CONNECTED" : marketDataService.isReconnecting() ? "RECONNECTING" : "DISCONNECTED");
        health.put("orderWs", orderEventService.isConnected() ? "CONNECTED" : orderEventService.isReconnecting() ? "RECONNECTING" : "DISCONNECTED");

        // Candle boundary checker
        health.put("candleChecker", candleAggregator.isBoundaryCheckerAlive() ? "ALIVE" : "DEAD");
        health.put("lastScanCount", breakoutScanner.getLastScanCount());
        health.put("lastScanTime", breakoutScanner.getLastScanTime());

        // Scanner
        health.put("signalSource", riskSettings.getSignalSource());
        health.put("watchlistCount", marketDataService.getWatchlist().size());
        health.put("atrLoaded", atrService.getLoadedCountFor(marketDataService.getWatchlist()));

        // OCO monitors
        health.put("ocoMonitors", pollingService.getOcoMonitorCount());
        health.put("ocoSymbols", pollingService.getOcoMonitoredSymbols());

        // Positions
        health.put("openPositions", pollingService.getOpenPositionCount());

        // Connection status
        health.put("status", pollingService.getConnectionStatus());
        health.put("lastSync", pollingService.getLastSyncTime());

        // SSE emitters
        health.put("sseClients", marketDataService.getEmitterCount());

        // Latency
        health.put("avgLatency", latencyTracker.getAverages());

        return health;
    }

    @GetMapping("/api/monitoring")
    public Map<String, Object> getMonitoring() {
        Map<String, Object> m = new java.util.LinkedHashMap<>();

        // WebSocket Status
        Map<String, Object> ws = new java.util.LinkedHashMap<>();
        ws.put("dataWs", marketDataService.isConnected() ? "CONNECTED" : marketDataService.isReconnecting() ? "RECONNECTING" : "DISCONNECTED");
        ws.put("dataLastConnect", marketDataService.getLastConnectTime());
        ws.put("dataLastDisconnect", marketDataService.getLastDisconnectTime());
        ws.put("dataReconnects", marketDataService.getReconnectCountToday());
        ws.put("orderWs", orderEventService.isConnected() ? "CONNECTED" : orderEventService.isReconnecting() ? "RECONNECTING" : "DISCONNECTED");
        ws.put("orderLastConnect", orderEventService.getLastConnectTime());
        ws.put("orderLastDisconnect", orderEventService.getLastDisconnectTime());
        ws.put("orderReconnects", orderEventService.getReconnectCountToday());
        ws.put("sseClients", marketDataService.getEmitterCount());
        m.put("websocket", ws);

        // Scanner
        Map<String, Object> scanner = new java.util.LinkedHashMap<>();
        scanner.put("scanUniverse", riskSettings.getScanUniverse());
        scanner.put("universeSize", bhavcopyService.getScanUniverseCount());
        scanner.put("nifty50Count", bhavcopyService.getNifty50Count());
        scanner.put("watchlistCount", marketDataService.getWatchlist().size());
        scanner.put("watchlistSymbols", marketDataService.getWatchlist().stream()
            .map(s -> s.replaceAll("^(NSE|BSE):", "").replaceAll("-EQ$", ""))
            .collect(java.util.stream.Collectors.toList()));
        scanner.put("atrLoaded", atrService.getLoadedCountFor(marketDataService.getWatchlist()));
        scanner.put("activeCandles", candleAggregator.getActiveCandleCount());
        scanner.put("candleChecker", candleAggregator.isBoundaryCheckerAlive() ? "ALIVE" : "DEAD");
        scanner.put("candleCheckerRestarts", candleAggregator.getRestartCount());
        scanner.put("candleCheckerLastRestart", candleAggregator.getLastRestartTime());
        scanner.put("lastScanCount", breakoutScanner.getLastScanCount());
        scanner.put("lastScanTime", breakoutScanner.getLastScanTime());
        scanner.put("tradedToday", breakoutScanner.getTradedCountToday());
        scanner.put("filteredToday", breakoutScanner.getFilteredCountToday());
        scanner.put("smaLoaded", smaService.getLoadedCountFor(marketDataService.getWatchlist()));
        scanner.put("firstCandleLoaded", candleAggregator.getFirstCandleCloseCountFor(marketDataService.getWatchlist()));
        scanner.put("validationPass", marketDataService.getValidationPass());
        scanner.put("validationFail", marketDataService.getValidationFail());
        scanner.put("validationTotal", marketDataService.getValidationTotal());
        m.put("scanner", scanner);

        // Positions & OCO
        Map<String, Object> trading = new java.util.LinkedHashMap<>();
        trading.put("openPositions", pollingService.getOpenPositionCount());
        trading.put("ocoPolling", pollingService.getOcoMonitorCount());
        trading.put("ocoPollingSymbols", pollingService.getOcoMonitoredSymbols());
        trading.put("ocoWebSocket", orderEventService.getTrackedOcoCount());
        // Risk exposure
        double openRisk = 0, consumedRisk = 0;
        for (String sym : com.rydytrader.autotrader.manager.PositionManager.getAllSymbols()) {
            Map<String, Object> state = positionStateStore.load(sym);
            if (state != null && state.containsKey("slPrice") && state.containsKey("avgPrice")) {
                double sl = Double.parseDouble(state.get("slPrice").toString());
                double avg = Double.parseDouble(state.get("avgPrice").toString());
                int qty = Integer.parseInt(state.get("qty").toString());
                String side = state.containsKey("side") ? state.get("side").toString() : "";
                if ("LONG".equals(side) && sl < avg) openRisk += qty * (avg - sl);
                else if ("SHORT".equals(side) && sl > avg) openRisk += qty * (sl - avg);
            }
        }
        double netPnlAll = tradeHistoryService.getTrades().stream().mapToDouble(t -> t.getNetPnl()).sum();
        consumedRisk = netPnlAll < 0 ? Math.abs(netPnlAll) : 0;
        double maxLoss = riskSettings.getMaxDailyLoss();
        trading.put("openRisk", Math.round(openRisk * 100.0) / 100.0);
        trading.put("consumedRisk", Math.round(consumedRisk * 100.0) / 100.0);
        trading.put("maxDailyLoss", Math.round(maxLoss * 100.0) / 100.0);
        trading.put("remainingBudget", Math.round((maxLoss - openRisk - consumedRisk) * 100.0) / 100.0);
        m.put("trading", trading);

        // Latency
        Map<String, Object> latency = new java.util.LinkedHashMap<>();
        latency.put("averages", latencyTracker.getAverages());
        latency.put("recent", latencyTracker.getCompleted());
        m.put("latency", latency);

        // System
        Map<String, Object> system = new java.util.LinkedHashMap<>();
        long uptimeMs = java.lang.management.ManagementFactory.getRuntimeMXBean().getUptime();
        long uptimeSec = uptimeMs / 1000;
        system.put("uptime", String.format("%dh %dm %ds", uptimeSec / 3600, (uptimeSec % 3600) / 60, uptimeSec % 60));
        system.put("status", pollingService.getConnectionStatus());
        m.put("system", system);

        return m;
    }


    // ── LOGS ──────────────────────────────────────────────────────────────────
    @GetMapping("/logs")
    public List<String> getLogs() {
        return eventService.getTradeLogs();
    }

    // ── TRADES ───────────────────────────────────────────────────────────────────
    @GetMapping("/api/trades")
    public List<Map<String, Object>> getTrades() {
        return tradeHistoryService.getTrades().stream().map(t -> {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            // Frontend expects HH:mm:ss only — strip the optional yyyy-MM-dd prefix.
            String ts = t.getTimestamp();
            int spaceIdx = ts != null ? ts.indexOf(' ') : -1;
            m.put("timestamp",  spaceIdx >= 0 ? ts.substring(spaceIdx + 1) : ts);
            m.put("symbol",     t.getSymbol());
            m.put("side",       t.getSide());
            m.put("qty",        t.getQty());
            m.put("entryPrice", t.getEntryPrice());
            m.put("exitPrice",  t.getExitPrice());
            m.put("exitReason", t.getExitReason());
            m.put("setup",      t.getSetup());
            m.put("pnl",        t.getPnl());
            m.put("charges",    t.getCharges());
            m.put("netPnl",     t.getNetPnl());
            m.put("result",     t.getResult());
            m.put("description", t.getDescription() != null ? t.getDescription() : "");
            m.put("probability", t.getProbability() != null ? t.getProbability() : "");
            return m;
        }).collect(java.util.stream.Collectors.toList());
    }

    // ── JOURNAL ──────────────────────────────────────────────────────────────────
    @GetMapping("/api/journal")
    public Map<String, Object> getJournal(
            @RequestParam String from,
            @RequestParam String to) {

        java.time.LocalDate dFrom = java.time.LocalDate.parse(from);
        java.time.LocalDate dTo   = java.time.LocalDate.parse(to);
        java.util.List<com.rydytrader.autotrader.dto.TradeRecord> trades =
            tradeHistoryService.getTradesForRange(dFrom, dTo);

        int total  = trades.size();
        int wins   = (int) trades.stream().filter(t -> "PROFIT".equals(t.getResult())).count();
        int losses = (int) trades.stream().filter(t -> "LOSS".equals(t.getResult())).count();
        int breakeven = (int) trades.stream().filter(t -> "BREAKEVEN".equals(t.getResult())).count();
        double netPnl  = trades.stream().mapToDouble(t -> t.getNetPnl()).sum();
        double totalCharges = trades.stream().mapToDouble(t -> t.getCharges()).sum();
        double grossPnl = trades.stream().mapToDouble(t -> t.getPnl()).sum();
        double winRate = total == 0 ? 0 : Math.round(wins * 1000.0 / total) / 10.0;

        // Use netPnl (after charges). PROFIT (>0), LOSS (<0), BREAKEVEN (==0). Breakeven excluded
        // from PF. Same classification used by JournalService.computeMetrics so all pages match.
        double netWin    = trades.stream().filter(t -> "PROFIT".equals(t.getResult())).mapToDouble(t -> t.getNetPnl()).sum();
        double netLoss   = Math.abs(trades.stream().filter(t -> "LOSS".equals(t.getResult())).mapToDouble(t -> t.getNetPnl()).sum());
        double pf        = netLoss == 0 ? (netWin > 0 ? 99 : 0) : Math.round(netWin / netLoss * 100.0) / 100.0;

        double avgWin  = wins == 0 ? 0 : Math.round(netWin / wins * 100.0) / 100.0;
        double avgLoss = losses == 0 ? 0 : Math.round(-netLoss / losses * 100.0) / 100.0;
        double maxWin  = trades.stream().mapToDouble(t -> t.getNetPnl()).filter(p -> p > 0).max().orElse(0);
        double maxLoss = trades.stream().mapToDouble(t -> t.getNetPnl()).filter(p -> p < 0).min().orElse(0);
        double avgRR   = avgLoss == 0 ? 0 : Math.round(Math.abs(avgWin / avgLoss) * 100.0) / 100.0;
        double expectancy = total == 0 ? 0 : Math.round(netPnl / total * 100.0) / 100.0;

        // Consecutive wins/losses — BREAKEVEN trades break both streaks
        int maxCW = 0, maxCL = 0, cw = 0, cl = 0;
        for (var t : trades) {
            if      ("PROFIT".equals(t.getResult())) { cw++; cl = 0; maxCW = Math.max(maxCW, cw); }
            else if ("LOSS".equals(t.getResult()))   { cl++; cw = 0; maxCL = Math.max(maxCL, cl); }
            else                                      { cw = 0; cl = 0; }
        }

        // Equity curve
        java.util.List<Map<String, Object>> eq = new java.util.ArrayList<>();
        double cum = 0;
        for (var t : trades) {
            cum += t.getNetPnl();
            eq.add(Map.of("timestamp", t.getTimestamp(), "cumPnl", Math.round(cum * 100.0) / 100.0));
        }

        // Daily P&L
        java.util.Map<String, Double> dailyMap = new java.util.LinkedHashMap<>();
        java.time.LocalDate d = dFrom;
        while (!d.isAfter(dTo)) { dailyMap.put(d.toString(), 0.0); d = d.plusDays(1); }
        for (var t : trades) {
            // timestamp is HH:mm:ss, use today's date context — group by file date
        }
        // Re-read with dates from filenames
        java.util.List<Map<String, Object>> daily = new java.util.ArrayList<>();
        java.time.LocalDate dd = dFrom;
        while (!dd.isAfter(dTo)) {
            java.util.List<com.rydytrader.autotrader.dto.TradeRecord> dt =
                tradeHistoryService.getTradesForRange(dd, dd);
            double dpnl = dt.stream().mapToDouble(t -> t.getNetPnl()).sum();
            if (!dt.isEmpty() || !dd.isAfter(java.time.LocalDate.now())) {
                daily.add(Map.of("date", dd.toString(), "pnl", Math.round(dpnl * 100.0) / 100.0));
            }
            dd = dd.plusDays(1);
        }

        // Result counts
        Map<String, Long> rc = new java.util.LinkedHashMap<>();
        rc.put("PROFIT",    trades.stream().filter(t -> "PROFIT".equals(t.getResult())).count());
        rc.put("LOSS",      trades.stream().filter(t -> "LOSS".equals(t.getResult())).count());
        rc.put("BREAKEVEN", trades.stream().filter(t -> "BREAKEVEN".equals(t.getResult())).count());

        // Side breakdown
        Map<String, Object> sb = new java.util.LinkedHashMap<>();
        for (String side : new String[]{"LONG","SHORT"}) {
            final String s = side;
            var st = trades.stream().filter(t -> s.equals(t.getSide())).collect(java.util.stream.Collectors.toList());
            int sw = (int) st.stream().filter(t -> "PROFIT".equals(t.getResult())).count();
            double sp = st.stream().mapToDouble(t -> t.getNetPnl()).sum();
            Map<String, Object> sideMap = new java.util.LinkedHashMap<>();
            sideMap.put("trades",  st.size());
            sideMap.put("winRate", st.isEmpty() ? 0 : Math.round(sw*1000.0/st.size())/10.0);
            sideMap.put("pnl",     Math.round(sp*100.0)/100.0);
            sb.put(side, sideMap);
        }

        // Reason breakdown
        Map<String, Object> rb = new java.util.LinkedHashMap<>();
        for (String r : new String[]{"SL","TRAILING_SL","TARGET","AUTO_SQUAREOFF","MANUAL"}) {
            var rt = trades.stream().filter(t -> r.equals(t.getExitReason())).collect(java.util.stream.Collectors.toList());
            double rp = rt.stream().mapToDouble(t -> t.getNetPnl()).sum();
            double ra = rt.isEmpty() ? 0 : Math.round(rp / rt.size() * 100.0) / 100.0;
            rb.put(r, Map.of("count", rt.size(), "pnl", Math.round(rp*100.0)/100.0, "avgPnl", ra));
        }

        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("totalTrades",    total);
        result.put("wins",           wins);
        result.put("losses",         losses);
        result.put("breakeven",      breakeven);
        result.put("grossPnl",       Math.round(grossPnl*100.0)/100.0);
        result.put("totalCharges",   Math.round(totalCharges*100.0)/100.0);
        result.put("netPnl",         Math.round(netPnl*100.0)/100.0);
        result.put("winRate",        winRate);
        result.put("profitFactor",   pf);
        result.put("avgWin",         avgWin);
        result.put("avgLoss",        avgLoss);
        result.put("maxWin",         Math.round(maxWin*100.0)/100.0);
        result.put("maxLoss",        Math.round(maxLoss*100.0)/100.0);
        result.put("avgRR",          avgRR);
        result.put("expectancy",     expectancy);
        result.put("maxConsecWins",  maxCW);
        result.put("maxConsecLosses",maxCL);
        result.put("equityCurve",    eq);
        result.put("dailyPnl",       daily);
        result.put("resultCounts",   rc);
        result.put("sideBreakdown",  sb);
        result.put("reasonBreakdown",rb);

        // Setup breakdown
        java.util.List<String> allSetups = java.util.Arrays.asList(
            "BUY_ABOVE_CPR","BUY_ABOVE_R1_PDH","BUY_ABOVE_R2","BUY_ABOVE_R3","BUY_ABOVE_R4","BUY_ABOVE_S1_PDL",
            "SELL_BELOW_CPR","SELL_BELOW_S1_PDL","SELL_BELOW_S2","SELL_BELOW_S3","SELL_BELOW_S4","SELL_BELOW_R1_PDH"
        );
        Map<String, Object> setupMap = new java.util.LinkedHashMap<>();
        for (String setup : allSetups) {
            final String su = setup;
            var sut = trades.stream().filter(t -> su.equals(t.getSetup())).collect(java.util.stream.Collectors.toList());
            if (sut.isEmpty()) continue;
            int suw = (int) sut.stream().filter(t -> "PROFIT".equals(t.getResult())).count();
            double sup = sut.stream().mapToDouble(t -> t.getNetPnl()).sum();
            double sua = sut.isEmpty() ? 0 : Math.round(sup / sut.size() * 100.0) / 100.0;
            Map<String, Object> sd = new java.util.LinkedHashMap<>();
            sd.put("count",   sut.size());
            sd.put("winRate", Math.round(suw * 1000.0 / sut.size()) / 10.0);
            sd.put("pnl",     Math.round(sup * 100.0) / 100.0);
            sd.put("avgPnl",  sua);
            setupMap.put(setup, sd);
        }
        result.put("setupBreakdown", setupMap);

        // Symbol breakdown
        java.util.List<String> allSymbols = trades.stream()
            .map(t -> t.getSymbol())
            .filter(s -> s != null && !s.isEmpty())
            .distinct()
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        Map<String, Object> symbolMap = new java.util.LinkedHashMap<>();
        for (String sym : allSymbols) {
            final String sy = sym;
            var syt = trades.stream().filter(t -> sy.equals(t.getSymbol())).collect(java.util.stream.Collectors.toList());
            int syw = (int) syt.stream().filter(t -> "PROFIT".equals(t.getResult())).count();
            double syp = syt.stream().mapToDouble(t -> t.getNetPnl()).sum();
            double sya = syt.isEmpty() ? 0 : Math.round(syp / syt.size() * 100.0) / 100.0;
            Map<String, Object> sd = new java.util.LinkedHashMap<>();
            sd.put("count",   syt.size());
            sd.put("winRate", Math.round(syw * 1000.0 / syt.size()) / 10.0);
            sd.put("pnl",     Math.round(syp * 100.0) / 100.0);
            sd.put("avgPnl",  sya);
            symbolMap.put(sym, sd);
        }
        result.put("symbolBreakdown", symbolMap);

        // Time breakdown — bucket by 1-hour slots (9:15–10:15, 10:15–11:15, ...)
        // Pre-fill all market hour slots so empty ones show as 0
        Map<String, Object> timeMap = new java.util.LinkedHashMap<>();
        int[] slotStarts = {9*60+15, 10*60+15, 11*60+15, 12*60+15, 13*60+15, 14*60+15};
        for (int start : slotStarts) {
            int end = start + 60;
            String key = String.format("%02d:%02d–%02d:%02d", start/60, start%60, end/60, end%60);
            Map<String, Object> empty = new java.util.LinkedHashMap<>();
            empty.put("count", 0); empty.put("wins", 0); empty.put("pnl", 0.0);
            timeMap.put(key, empty);
        }
        for (var t : trades) {
            String ts = t.getTimestamp(); // HH:mm:ss or yyyy-MM-dd HH:mm:ss
            if (ts == null || ts.length() < 5) continue;
            int spaceIdx = ts.indexOf(' ');
            String time = spaceIdx >= 0 ? ts.substring(spaceIdx + 1) : ts;
            if (time.length() < 5) continue;
            int hour = Integer.parseInt(time.substring(0, 2));
            int min = Integer.parseInt(time.substring(3, 5));
            int totalMin = hour * 60 + min;
            // Find the matching 1-hour slot
            String bucket = null;
            for (int start : slotStarts) {
                if (totalMin >= start && totalMin < start + 60) {
                    int end = start + 60;
                    bucket = String.format("%02d:%02d–%02d:%02d", start/60, start%60, end/60, end%60);
                    break;
                }
            }
            if (bucket == null) continue;
            @SuppressWarnings("unchecked")
            Map<String, Object> tm = (Map<String, Object>) timeMap.computeIfAbsent(bucket, k -> {
                Map<String, Object> m = new java.util.LinkedHashMap<>();
                m.put("count", 0); m.put("wins", 0); m.put("pnl", 0.0);
                return m;
            });
            tm.put("count", (int) tm.get("count") + 1);
            if ("PROFIT".equals(t.getResult())) tm.put("wins", (int) tm.get("wins") + 1);
            tm.put("pnl", (double) tm.get("pnl") + t.getNetPnl());
        }
        // Add winRate and round pnl
        for (var te : timeMap.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> tm = (Map<String, Object>) te.getValue();
            int tc = (int) tm.get("count");
            int tw = (int) tm.get("wins");
            tm.put("winRate", tc > 0 ? Math.round(tw * 1000.0 / tc) / 10.0 : 0);
            tm.put("pnl", Math.round((double) tm.get("pnl") * 100.0) / 100.0);
            tm.put("avgPnl", tc > 0 ? Math.round((double) tm.get("pnl") / tc * 100.0) / 100.0 : 0);
        }
        result.put("timeBreakdown", timeMap);

        // Probability breakdown — HPT / LPT
        Map<String, Object> probMap = new java.util.LinkedHashMap<>();
        for (String p : new String[]{"HPT","LPT"}) {
            final String pf2 = p;
            var pt = trades.stream().filter(t -> pf2.equals(t.getProbability())).collect(java.util.stream.Collectors.toList());
            if (pt.isEmpty()) continue;
            int pw = (int) pt.stream().filter(t -> "PROFIT".equals(t.getResult())).count();
            double pp = pt.stream().mapToDouble(t -> t.getNetPnl()).sum();
            double pa = Math.round(pp / pt.size() * 100.0) / 100.0;
            double pWin = pt.stream().filter(t -> t.getNetPnl() > 0).mapToDouble(t -> t.getNetPnl()).sum();
            double pLoss = Math.abs(pt.stream().filter(t -> t.getNetPnl() < 0).mapToDouble(t -> t.getNetPnl()).sum());
            double ppf = pLoss == 0 ? (pWin > 0 ? 99 : 0) : Math.round(pWin / pLoss * 100.0) / 100.0;
            Map<String, Object> pd = new java.util.LinkedHashMap<>();
            pd.put("count",   pt.size());
            pd.put("wins",    pw);
            pd.put("winRate", Math.round(pw * 1000.0 / pt.size()) / 10.0);
            pd.put("pnl",     Math.round(pp * 100.0) / 100.0);
            pd.put("avgPnl",  pa);
            pd.put("profitFactor", ppf);
            probMap.put(p, pd);
        }
        result.put("probabilityBreakdown", probMap);

        // Max drawdown & drawdown curve
        double peak = 0, maxDD = 0, cumDD = 0;
        java.util.List<Map<String, Object>> ddCurve = new java.util.ArrayList<>();
        for (var t : trades) {
            cumDD += t.getNetPnl();
            if (cumDD > peak) peak = cumDD;
            double drawdown = peak - cumDD;
            if (drawdown > maxDD) maxDD = drawdown;
            ddCurve.add(Map.of("timestamp", t.getTimestamp(), "drawdown", Math.round(drawdown * 100.0) / 100.0));
        }
        result.put("maxDrawdown", Math.round(maxDD * 100.0) / 100.0);
        result.put("drawdownCurve", ddCurve);

        return result;
    }

    // ── NET DAY P&L ───────────────────────────────────────────────────────────
    @GetMapping("/net-day-pnl")
    public Map<String, Double> getNetDayPnl() {
        return Map.of("netDayPnl", orderService.getNetDayPnl());
    }

}