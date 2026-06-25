package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.NiftyRsiService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the NIFTY 14-period RSI series for the trade page chart.
 *
 * <ul>
 *   <li>{@code GET /api/nifty-rsi/history} — today's 5-min RSI samples since
 *       09:15 IST plus the latest computed value.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/nifty-rsi")
public class NiftyRsiController {

    private final NiftyRsiService service;

    public NiftyRsiController(NiftyRsiService service) {
        this.service = service;
    }

    @GetMapping("/history")
    public ResponseEntity<NiftyRsiService.History> history() {
        return ResponseEntity.ok(service.history());
    }
}
