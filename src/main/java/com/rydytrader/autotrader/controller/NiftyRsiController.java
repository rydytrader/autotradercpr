package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.NiftyRsiService;
import com.rydytrader.autotrader.service.NiftyRsiStreamBroker;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Exposes the NIFTY 14-period RSI series for the trade page chart.
 *
 * <ul>
 *   <li>{@code GET /api/nifty-rsi/history} — today's 5-min RSI samples since
 *       09:15 IST plus the latest computed value. Used on initial page load
 *       and manual refresh.</li>
 *   <li>{@code GET /api/nifty-rsi/stream} — SSE stream pushing the live RSI
 *       tip + last-closed canonical sample every 1 second. Used for the
 *       always-on chart updates.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/nifty-rsi")
public class NiftyRsiController {

    private final NiftyRsiService      service;
    private final NiftyRsiStreamBroker streamBroker;

    public NiftyRsiController(NiftyRsiService service,
                              NiftyRsiStreamBroker streamBroker) {
        this.service      = service;
        this.streamBroker = streamBroker;
    }

    @GetMapping("/history")
    public ResponseEntity<NiftyRsiService.History> history() {
        return ResponseEntity.ok(service.history());
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L); // no timeout
        streamBroker.addEmitter(emitter);
        return emitter;
    }
}
