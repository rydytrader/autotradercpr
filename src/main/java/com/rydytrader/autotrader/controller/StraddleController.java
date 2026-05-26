package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.RollingStraddleService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Live read-only status of {@link RollingStraddleService}. Consumed by the
 * "Rolling Straddle" tab on the Settings page (5s JS polling).
 */
@RestController
public class StraddleController {

    private final RollingStraddleService straddleService;

    public StraddleController(RollingStraddleService straddleService) {
        this.straddleService = straddleService;
    }

    @GetMapping("/api/straddle/status")
    public Map<String, Object> status() {
        return straddleService.getStatus();
    }

    /** Consolidated payload for the Home dashboard — state + leg MTM + trigger band + recent rolls. */
    @GetMapping("/api/straddle/dashboard")
    public Map<String, Object> dashboard() {
        return straddleService.getDashboard();
    }

    /** Manual reset to IDLE — clears stale DONE_FOR_DAY after a same-day rejection so the
     *  service can re-arm without a server restart. Does not place orders. */
    @PostMapping("/api/straddle/reset")
    public Map<String, Object> reset() {
        straddleService.resetToIdle("manual");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", true);
        out.put("message", "Straddle state reset to IDLE. Next scheduler tick will evaluate entry time.");
        return out;
    }

    /** Manual squareoff — flatten both legs at market and park the day. */
    @PostMapping("/api/straddle/close")
    public Map<String, Object> closeBoth() {
        boolean ok = straddleService.forceCloseBothLegs("MANUAL_DASHBOARD");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok);
        out.put("message", ok
            ? "Both legs squared off. State → DONE_FOR_DAY."
            : "Nothing to square off — no active straddle.");
        return out;
    }
}
