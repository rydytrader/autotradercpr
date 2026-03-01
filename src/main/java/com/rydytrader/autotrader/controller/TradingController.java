package com.rydytrader.autotrader.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.rydytrader.autotrader.dto.OrderDTO;
import com.rydytrader.autotrader.dto.PositionsDTO;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.mock.MockState;
import com.rydytrader.autotrader.service.EventService;
import com.rydytrader.autotrader.service.OrderService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.service.TradeHistoryService;
import com.rydytrader.autotrader.store.ModeStore;

@RestController
public class TradingController {

    private final PollingService pollingService;
    private final OrderService orderService;
    private final EventService eventService;
    private final ModeStore modeStore;
    private final MockState mockState;
    private final TradeHistoryService tradeHistoryService;

    public TradingController(PollingService pollingService,
                              OrderService orderService,
                              EventService eventService,
                              ModeStore modeStore,
                              MockState mockState,
                              TradeHistoryService tradeHistoryService) {
        this.pollingService      = pollingService;
        this.orderService        = orderService;
        this.eventService        = eventService;
        this.modeStore           = modeStore;
        this.mockState           = mockState;
        this.tradeHistoryService = tradeHistoryService;
    }

    // ── PLACE ORDER ───────────────────────────────────────────────────────────
    // Payload: { "signal": "BUY"|"SELL", "symbol": "NSE:NIFTY25JUNFUT",
    //            "quantity": 50, "stoploss": 25450.0, "target": 25700.0 }
    @PostMapping("/placeorder")
    public ResponseEntity<String> receiveSignal(@RequestBody Map<String, Object> payload) {

        String signal   = payload.get("signal").toString();
        String symbol   = payload.get("symbol").toString();
        int    quantity = Integer.parseInt(payload.get("quantity").toString());
        double stoploss = Double.parseDouble(payload.get("stoploss").toString());
        double target   = Double.parseDouble(payload.get("target").toString());
        String setup    = payload.containsKey("setup") ? payload.get("setup").toString() : "";

        System.out.println("Signal received: " + signal + " | SL: " + stoploss + " | Target: " + target + " | Setup: " + setup);

        if (signal.equals("BUY") && !PositionManager.getPosition().equals("LONG")) {

            OrderDTO order = orderService.placeOrder(symbol, quantity, 1, stoploss);
            eventService.log("BUY signal received for " + symbol + " | Setup: " + setup + " | SL: " + stoploss + " | Target: " + target);

            // Monitor entry fill, then place SL + Target OCO
            // exitSide = -1 (SELL to exit a LONG)
            pollingService.monitorEntryAndPlaceOCO(order, symbol, quantity, "LONG", -1, stoploss, target, setup);

        } else if (signal.equals("SELL") && !PositionManager.getPosition().equals("SHORT")) {

            OrderDTO order = orderService.placeOrder(symbol, quantity, -1, stoploss);
            eventService.log("SELL signal received for " + symbol + " | Setup: " + setup + " | SL: " + stoploss + " | Target: " + target);

            // exitSide = 1 (BUY to exit a SHORT)
            pollingService.monitorEntryAndPlaceOCO(order, symbol, quantity, "SHORT", 1, stoploss, target, setup);

        } else {
            String msg = "Signal ignored — existing position: " + PositionManager.getPosition();
            System.out.println(msg);
            eventService.log(msg);
        }

        return ResponseEntity.ok("Signal processed");
    }

    // ── SQUARE OFF ───────────────────────────────────────────────────────────────
    @PostMapping("/api/squareoff")
    public ResponseEntity<Map<String, Object>> squareOff(@RequestBody Map<String, Object> payload) {
        String symbol   = payload.get("symbol").toString();
        int    quantity = Integer.parseInt(payload.get("quantity").toString());

        if (!modeStore.isLive()) {
            // Simulator mode — use MockState directly (same as simulator panel square-off)
            Map<String, Object> pos = mockState.getPosition();
            if (pos == null) return ResponseEntity.ok(Map.of("ok", false, "reason", "No open position"));
            double entryPrice = Double.parseDouble(pos.get("netAvgPrice").toString());
            double exitPrice  = mockState.getCurrentPrice();
            int    netQty     = Integer.parseInt(pos.get("netQty").toString());
            String side       = netQty > 0 ? "LONG" : "SHORT";
            String setup = pollingService.getCurrentSetup();
            PositionManager.setPosition("NONE");
            mockState.triggerManualSquareOff();
            tradeHistoryService.record(symbol, side, Math.abs(netQty), entryPrice, exitPrice, "MANUAL", setup);
            eventService.log("Square off at exit: " + exitPrice + " — SL and Target cancelled");
            return ResponseEntity.ok(Map.of("ok", true));
        }

        // Live mode
        boolean success = pollingService.squareOff(symbol, quantity);
        return ResponseEntity.ok(Map.of("ok", success));
    }

    // ── POSITIONS ─────────────────────────────────────────────────────────────
    @GetMapping("/api/positions")
    public List<Map<String, Object>> getPositions() {
        return pollingService.fetchPositions().stream().map(p -> {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("symbol",   p.getSymbol());
            m.put("qty",      p.getQty());
            m.put("side",     p.getSide());
            m.put("avgPrice", p.getAvgPrice());
            m.put("ltp",      p.getLtp());
            m.put("pnl",      p.getPnl());
            m.put("setup",     p.getSetup());
            m.put("entryTime", p.getEntryTime());
            return m;
        }).collect(java.util.stream.Collectors.toList());
    }

    // ── STATUS ────────────────────────────────────────────────────────────────
    @GetMapping("/status")
    public Map<String, String> getStatus() {
        return Map.of("status", pollingService.getConnectionStatus());
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
            m.put("timestamp",  t.getTimestamp());
            m.put("symbol",     t.getSymbol());
            m.put("side",       t.getSide());
            m.put("qty",        t.getQty());
            m.put("entryPrice", t.getEntryPrice());
            m.put("exitPrice",  t.getExitPrice());
            m.put("exitReason", t.getExitReason());
            m.put("setup",      t.getSetup());
            m.put("pnl",        t.getPnl());
            m.put("result",     t.getResult());
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
        double netPnl  = trades.stream().mapToDouble(t -> t.getPnl()).sum();
        double winRate = total == 0 ? 0 : Math.round(wins * 1000.0 / total) / 10.0;

        double grossWin  = trades.stream().filter(t -> t.getPnl() > 0).mapToDouble(t -> t.getPnl()).sum();
        double grossLoss = Math.abs(trades.stream().filter(t -> t.getPnl() < 0).mapToDouble(t -> t.getPnl()).sum());
        double pf        = grossLoss == 0 ? (grossWin > 0 ? 99 : 0) : Math.round(grossWin / grossLoss * 100.0) / 100.0;

        double avgWin  = wins == 0 ? 0 : Math.round(grossWin / wins * 100.0) / 100.0;
        double avgLoss = losses == 0 ? 0 : Math.round(-grossLoss / losses * 100.0) / 100.0;
        double maxWin  = trades.stream().mapToDouble(t -> t.getPnl()).filter(p -> p > 0).max().orElse(0);
        double maxLoss = trades.stream().mapToDouble(t -> t.getPnl()).filter(p -> p < 0).min().orElse(0);
        double avgRR   = avgLoss == 0 ? 0 : Math.round(Math.abs(avgWin / avgLoss) * 100.0) / 100.0;
        double expectancy = total == 0 ? 0 : Math.round(netPnl / total * 100.0) / 100.0;

        // Consecutive wins/losses
        int maxCW = 0, maxCL = 0, cw = 0, cl = 0;
        for (var t : trades) {
            if ("PROFIT".equals(t.getResult())) { cw++; cl = 0; maxCW = Math.max(maxCW, cw); }
            else                                 { cl++; cw = 0; maxCL = Math.max(maxCL, cl); }
        }

        // Equity curve
        java.util.List<Map<String, Object>> eq = new java.util.ArrayList<>();
        double cum = 0;
        for (var t : trades) {
            cum += t.getPnl();
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
            double dpnl = dt.stream().mapToDouble(t -> t.getPnl()).sum();
            if (!dt.isEmpty() || !dd.isAfter(java.time.LocalDate.now())) {
                daily.add(Map.of("date", dd.toString(), "pnl", Math.round(dpnl * 100.0) / 100.0));
            }
            dd = dd.plusDays(1);
        }

        // Result counts
        Map<String, Long> rc = new java.util.LinkedHashMap<>();
        rc.put("PROFIT",    trades.stream().filter(t -> "PROFIT".equals(t.getResult())).count());
        rc.put("LOSS",      trades.stream().filter(t -> "LOSS".equals(t.getResult())).count());
        rc.put("BREAKEVEN", trades.stream().filter(t -> t.getPnl() == 0).count());

        // Side breakdown
        Map<String, Object> sb = new java.util.LinkedHashMap<>();
        for (String side : new String[]{"LONG","SHORT"}) {
            final String s = side;
            var st = trades.stream().filter(t -> s.equals(t.getSide())).collect(java.util.stream.Collectors.toList());
            int sw = (int) st.stream().filter(t -> "PROFIT".equals(t.getResult())).count();
            double sp = st.stream().mapToDouble(t -> t.getPnl()).sum();
            Map<String, Object> sideMap = new java.util.LinkedHashMap<>();
            sideMap.put("trades",  st.size());
            sideMap.put("winRate", st.isEmpty() ? 0 : Math.round(sw*1000.0/st.size())/10.0);
            sideMap.put("pnl",     Math.round(sp*100.0)/100.0);
            sb.put(side, sideMap);
        }

        // Reason breakdown
        Map<String, Object> rb = new java.util.LinkedHashMap<>();
        for (String r : new String[]{"SL","TARGET","MANUAL"}) {
            var rt = trades.stream().filter(t -> r.equals(t.getExitReason())).collect(java.util.stream.Collectors.toList());
            double rp = rt.stream().mapToDouble(t -> t.getPnl()).sum();
            double ra = rt.isEmpty() ? 0 : Math.round(rp / rt.size() * 100.0) / 100.0;
            rb.put(r, Map.of("count", rt.size(), "pnl", Math.round(rp*100.0)/100.0, "avgPnl", ra));
        }

        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("totalTrades",    total);
        result.put("wins",           wins);
        result.put("losses",         losses);
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
            "BUY_ABOVE_CPR","BUY_ABOVE_R1_PDH","BUY_ABOVE_R2","BUY_ABOVE_S1_PDL",
            "SELL_BELOW_CPR","SELL_BELOW_S1_PDL","SELL_BELOW_S2","SELL_BELOW_R1_PDH"
        );
        Map<String, Object> setupMap = new java.util.LinkedHashMap<>();
        for (String setup : allSetups) {
            final String su = setup;
            var sut = trades.stream().filter(t -> su.equals(t.getSetup())).collect(java.util.stream.Collectors.toList());
            if (sut.isEmpty()) continue;
            int suw = (int) sut.stream().filter(t -> "PROFIT".equals(t.getResult())).count();
            double sup = sut.stream().mapToDouble(t -> t.getPnl()).sum();
            double sua = sut.isEmpty() ? 0 : Math.round(sup / sut.size() * 100.0) / 100.0;
            Map<String, Object> sd = new java.util.LinkedHashMap<>();
            sd.put("count",   sut.size());
            sd.put("winRate", Math.round(suw * 1000.0 / sut.size()) / 10.0);
            sd.put("pnl",     Math.round(sup * 100.0) / 100.0);
            sd.put("avgPnl",  sua);
            setupMap.put(setup, sd);
        }
        result.put("setupBreakdown", setupMap);
        return result;
    }

    // ── NET DAY P&L ───────────────────────────────────────────────────────────
    @GetMapping("/net-day-pnl")
    public Map<String, Double> getNetDayPnl() {
        return Map.of("netDayPnl", orderService.getNetDayPnl());
    }
}