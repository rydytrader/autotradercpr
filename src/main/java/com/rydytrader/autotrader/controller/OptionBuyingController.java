package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.strategy.OptionBuying;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST endpoints for the OPTION BUYING strategy.
 * <ul>
 *   <li>{@code GET  /api/option-buying/state}     — live snapshot (open positions,
 *       today's cycles, event log, counters).</li>
 *   <li>{@code GET  /api/option-buying/indicators} — NIFTY SuperTrend / RSI / BB / pivots.</li>
 *   <li>{@code POST /api/option-buying/squareoff} — flatten every open position.</li>
 *   <li>{@code POST /api/option-buying/reset}     — recovery: drop in-memory positions
 *       without exit orders (in case broker is out of sync).</li>
 * </ul>
 *
 * <p>Strategy is always enabled — no kill-switch endpoint. Use /squareoff or
 * /reset for emergency stops.
 */
@RestController
public class OptionBuyingController {

    private final OptionBuying strategy;

    public OptionBuyingController(OptionBuying strategy) {
        this.strategy = strategy;
    }

    @GetMapping("/api/option-buying/state")
    public Map<String, Object> getState() {
        return strategy.stateSnapshot();
    }

    /** NIFTY spot indicator snapshot for the header strip on /positions:
     *  SuperTrend line + direction, Bollinger upper/mid/lower, RSI, and R1/S1
     *  from yesterday's daily pivots. Cheap to compute (single indicator pass
     *  per call), safe to poll every few seconds. */
    @GetMapping("/api/option-buying/indicators")
    public Map<String, Object> getIndicators() {
        return strategy.indicatorsSnapshot();
    }

    @PostMapping("/api/option-buying/squareoff")
    public Map<String, Object> squareoff() {
        boolean closed = strategy.forceClose("MANUAL");
        return Map.of("ok", true, "closedSomething", closed);
    }

    @PostMapping("/api/option-buying/reset")
    public Map<String, Object> reset() {
        strategy.resetToIdle("MANUAL");
        return Map.of("ok", true);
    }
    // /enable endpoint retired — OPTION BUYING is always enabled. Use
    // /squareoff or /reset for emergency stops.
}
