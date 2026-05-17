package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.IndexTrend;
import com.rydytrader.autotrader.service.BreakoutScanner;
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
    private final BreakoutScanner   breakoutScanner;

    public IndexTrendController(IndexTrendService indexTrendService, BreakoutScanner breakoutScanner) {
        this.indexTrendService = indexTrendService;
        this.breakoutScanner   = breakoutScanner;
    }

    @GetMapping("/nifty")
    public IndexTrend getNifty() {
        IndexTrend trend = indexTrendService.getNiftyTrend();
        // Enrich with the single nearest NIFTY hurdle in trade direction. Direction
        // follows the trend state: bullish flavours look at resistance above LTP,
        // bearish flavours at support below. SIDEWAYS / NEUTRAL leave hurdle = null.
        String state = trend.getState();
        Boolean isBuy = null;
        if ("BULLISH".equals(state) || "BULLISH_REVERSAL".equals(state)) isBuy = true;
        else if ("BEARISH".equals(state) || "BEARISH_REVERSAL".equals(state)) isBuy = false;
        if (isBuy != null) {
            BreakoutScanner.HurdleStatus st = breakoutScanner.getNiftyNearestHurdle(isBuy);
            if (st != null) {
                trend.setHurdle(new IndexTrend.HurdleInfo(st.level(), st.category(), st.state()));
            }
        }
        return trend;
    }
}
