package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.IndexTrend;
import com.rydytrader.autotrader.service.IndexTrendService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoint for the NIFTY index trend snapshot. Polled by the scanner page
 * every 5 seconds to drive the NIFTY card display + color highlighting.
 */
@RestController
@RequestMapping("/api/index")
public class IndexTrendController {

    private final IndexTrendService indexTrendService;

    public IndexTrendController(IndexTrendService indexTrendService) {
        this.indexTrendService = indexTrendService;
    }

    @GetMapping("/nifty")
    public IndexTrend getNifty() {
        return indexTrendService.getNiftyTrend();
    }
}
