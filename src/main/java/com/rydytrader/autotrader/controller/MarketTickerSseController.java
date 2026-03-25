package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.MarketDataService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * SSE endpoint for real-time ticker updates.
 * Browsers connect to /api/market-ticker/stream and receive periodic tick events.
 */
@RestController
public class MarketTickerSseController {

    private final MarketDataService marketDataService;

    public MarketTickerSseController(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    @GetMapping(value = "/api/market-ticker/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamTicker() {
        SseEmitter emitter = new SseEmitter(0L); // no timeout

        marketDataService.addEmitter(emitter);

        emitter.onCompletion(() -> marketDataService.removeEmitter(emitter));
        emitter.onTimeout(() -> marketDataService.removeEmitter(emitter));
        emitter.onError(e -> marketDataService.removeEmitter(emitter));

        // Send current snapshot immediately so ticker populates without delay
        marketDataService.sendSnapshot(emitter);

        return emitter;
    }
}
