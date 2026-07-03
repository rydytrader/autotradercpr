package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.BankNiftyRsiService;
import com.rydytrader.autotrader.service.BankNiftyRsiStreamBroker;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * BANKNIFTY companion of {@link NiftyRsiController}. Exposes the BANKNIFTY
 * 14-period RSI series for the second trade-page chart.
 *
 * <ul>
 *   <li>{@code GET /api/banknifty-rsi/history}</li>
 *   <li>{@code GET /api/banknifty-rsi/stream}</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/banknifty-rsi")
public class BankNiftyRsiController {

    private final BankNiftyRsiService      service;
    private final BankNiftyRsiStreamBroker streamBroker;

    public BankNiftyRsiController(BankNiftyRsiService service,
                                   BankNiftyRsiStreamBroker streamBroker) {
        this.service      = service;
        this.streamBroker = streamBroker;
    }

    @GetMapping("/history")
    public ResponseEntity<BankNiftyRsiService.History> history() {
        return ResponseEntity.ok(service.history());
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);
        streamBroker.addEmitter(emitter);
        return emitter;
    }
}
