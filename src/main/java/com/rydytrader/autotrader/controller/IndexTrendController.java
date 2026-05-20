package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.dto.IndexTrend;
import com.rydytrader.autotrader.service.AtrService;
import com.rydytrader.autotrader.service.BhavcopyService;
import com.rydytrader.autotrader.service.BreakoutScanner;
import com.rydytrader.autotrader.service.IndexTrendService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST endpoints for index trend snapshots. {@link #getNifty()} returns the full NIFTY 50
 * card payload; {@link #getKeyIndices()} returns a compact array of the four key indices
 * (NIFTY 50, NIFTY BANK, NIFTY IT, NIFTY PHARMA) used by the scanner page's top header.
 */
@RestController
@RequestMapping("/api/index")
public class IndexTrendController {

    /** The fixed top-of-scanner index strip — same order on every page load. */
    private static final List<String> KEY_INDEX_TICKERS = List.of(
        "NIFTY50", "NIFTYBANK", "NIFTYIT", "NIFTYPHARMA", "NIFTYAUTO"
    );

    private final IndexTrendService indexTrendService;
    private final BreakoutScanner   breakoutScanner;
    private final AtrService        atrService;
    private final BhavcopyService   bhavcopyService;
    private final MarketDataService marketDataService;
    private final RiskSettingsStore riskSettings;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService marketHolidayService;

    public IndexTrendController(IndexTrendService indexTrendService,
                                 BreakoutScanner breakoutScanner,
                                 AtrService atrService,
                                 BhavcopyService bhavcopyService,
                                 MarketDataService marketDataService,
                                 RiskSettingsStore riskSettings) {
        this.indexTrendService = indexTrendService;
        this.breakoutScanner   = breakoutScanner;
        this.atrService        = atrService;
        this.bhavcopyService   = bhavcopyService;
        this.marketDataService = marketDataService;
        this.riskSettings      = riskSettings;
    }

    @GetMapping("/nifty")
    public IndexTrend getNifty() {
        IndexTrend trend = indexTrendService.getNiftyTrend();
        // NIFTY ATR (scanner-timeframe candles, 14-period Wilder) — shown as a tooltip on
        // the LTP element so the user can see the current NIFTY volatility scale.
        trend.setAtr(atrService.getAtr(IndexTrendService.NIFTY_SYMBOL));
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

    /**
     * Returns the four key indices for the scanner page header (NIFTY 50, NIFTY BANK,
     * NIFTY IT, NIFTY PHARMA). Each entry carries: ticker, fyersSymbol, displayName, LTP,
     * change (pts), changePct, CPR width %, CPR width category (NARROW / WIDE), trend
     * state (CPR-only). Polled by the scanner page every 5 seconds.
     */
    @GetMapping("/key-indices")
    public List<Map<String, Object>> getKeyIndices() {
        List<Map<String, Object>> out = new ArrayList<>();
        boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();

        for (String ticker : KEY_INDEX_TICKERS) {
            CprLevels idx = bhavcopyService.getCprLevels(ticker);
            if (idx == null) continue;
            String fyersSym = "NSE:" + ticker + "-INDEX";
            double ltp = marketDataService.getLtp(fyersSym);
            double prevClose = idx.getClose();
            double changePct, changePts;
            if (tradingDay && ltp > 0 && prevClose > 0) {
                changePct = ((ltp - prevClose) / prevClose) * 100.0;
                changePts = ltp - prevClose;
            } else {
                changePct = marketDataService.getChangePercent(fyersSym);
                changePts = marketDataService.getChange(fyersSym);
                if (changePct == 0 && changePts == 0 && prevClose > 0) {
                    CprLevels priorDay = bhavcopyService.getPreviousCpr(ticker);
                    if (priorDay != null && priorDay.getClose() > 0) {
                        changePts = prevClose - priorDay.getClose();
                        changePct = (changePts / priorDay.getClose()) * 100.0;
                    }
                }
            }
            String state = indexTrendService.getTrendStateForTicker(ticker);
            double widthPct = idx.getCprWidthPct();
            // Adaptive CPR state — replaces the legacy static NARROW/WIDE band.
            BhavcopyService.AdaptiveCprResult adaptive = bhavcopyService.getAdaptiveCpr(ticker);
            String widthCategory = switch (adaptive.state()) {
                case DYNAMIC_SQUEEZE       -> "NARROW";
                case STANDARD_EXPANSION    -> "STANDARD";
                case VOLATILITY_EXHAUSTION -> "EXHAUSTION";
                case INSUFFICIENT_DATA     -> "WARMUP";
            };

            String displayName = bhavcopyService.getIndexDisplayName(ticker);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ticker",           ticker);
            m.put("symbol",           fyersSym);
            m.put("displayName",      displayName != null ? displayName : ticker);
            m.put("ltp",              Math.round((ltp > 0 ? ltp : prevClose) * 100.0) / 100.0);
            m.put("change",           Math.round(changePts * 100.0) / 100.0);
            m.put("changePct",        Math.round(changePct * 100.0) / 100.0);
            m.put("cprWidthPct",      Math.round(widthPct * 1000.0) / 1000.0);
            m.put("cprWidthCategory", widthCategory);
            m.put("tc",               Math.round(idx.getTc() * 100.0) / 100.0);
            m.put("bc",               Math.round(idx.getBc() * 100.0) / 100.0);
            m.put("state",            state);
            out.add(m);
        }
        return out;
    }
}
