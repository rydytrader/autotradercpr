package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.dto.IndexTrend;
import com.rydytrader.autotrader.service.AtrService;
import com.rydytrader.autotrader.service.BhavcopyService;
import com.rydytrader.autotrader.service.BreakoutScanner;
import com.rydytrader.autotrader.service.CandleAggregator;
import com.rydytrader.autotrader.service.EmaService;
import com.rydytrader.autotrader.service.HtfEmaService;
import com.rydytrader.autotrader.service.IndexTrendService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.WeeklyCprService;
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
    private final CandleAggregator  candleAggregator;
    private final EmaService        emaService;
    private final HtfEmaService     htfEmaService;
    private final WeeklyCprService  weeklyCprService;
    @org.springframework.beans.factory.annotation.Autowired
    private MarketHolidayService marketHolidayService;

    public IndexTrendController(IndexTrendService indexTrendService,
                                 BreakoutScanner breakoutScanner,
                                 AtrService atrService,
                                 BhavcopyService bhavcopyService,
                                 MarketDataService marketDataService,
                                 RiskSettingsStore riskSettings,
                                 CandleAggregator candleAggregator,
                                 EmaService emaService,
                                 HtfEmaService htfEmaService,
                                 WeeklyCprService weeklyCprService) {
        this.indexTrendService = indexTrendService;
        this.breakoutScanner   = breakoutScanner;
        this.atrService        = atrService;
        this.bhavcopyService   = bhavcopyService;
        this.marketDataService = marketDataService;
        this.riskSettings      = riskSettings;
        this.candleAggregator  = candleAggregator;
        this.emaService        = emaService;
        this.htfEmaService     = htfEmaService;
        this.weeklyCprService  = weeklyCprService;
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
            String state = indexTrendService.getTrendStateForTicker(ticker, riskSettings.isEnableIndex5mEma20Check());
            double widthPct = idx.getCprWidthPct();
            // Adaptive CPR state — replaces the legacy static NARROW/WIDE band.
            BhavcopyService.AdaptiveCprResult adaptive = bhavcopyService.getAdaptiveCpr(ticker);
            String widthCategory = switch (adaptive.state()) {
                case NARROW            -> "NARROW";
                case AVERAGE           -> "AVERAGE";
                case WIDE              -> "WIDE";
                case INSUFFICIENT_DATA -> "WARMUP";
            };

            String displayName = bhavcopyService.getIndexDisplayName(ticker);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ticker",           ticker);
            m.put("symbol",           fyersSym);
            m.put("displayName",      displayName != null ? displayName : ticker);
            m.put("ltp",              Math.round((ltp > 0 ? ltp : prevClose) * 100.0) / 100.0);
            m.put("change",           Math.round(changePts * 100.0) / 100.0);
            m.put("changePct",        Math.round(changePct * 100.0) / 100.0);
            m.put("atr",              Math.round(atrService.getAtr(fyersSym) * 100.0) / 100.0);
            m.put("cprWidthPct",      Math.round(widthPct * 1000.0) / 1000.0);
            m.put("cprWidthCategory", widthCategory);
            // Adaptive payload mirrors the stock-card schema so the chip hover tooltip can
            // surface the same width / avg / ratio / samples breakdown.
            Map<String, Object> adaptivePayload = new LinkedHashMap<>();
            adaptivePayload.put("state",       adaptive.state().name());
            adaptivePayload.put("stateTag",    widthCategory);
            adaptivePayload.put("widthPct",    Math.round(adaptive.todayWidthPct() * 1000.0) / 1000.0);
            adaptivePayload.put("avgWidthPct", Math.round(adaptive.avgWidthPct()   * 1000.0) / 1000.0);
            adaptivePayload.put("widthRatio",  Math.round(adaptive.widthRatio()    * 1000.0) / 1000.0);
            adaptivePayload.put("samplesUsed", adaptive.samplesUsed());
            m.put("adaptive", adaptivePayload);
            m.put("tc",               Math.round(idx.getTc() * 100.0) / 100.0);
            m.put("bc",               Math.round(idx.getBc() * 100.0) / 100.0);
            m.put("state",            state);
            // Last completed 5-min close vs daily CPR → D-CPR bias, mirrors the stock card.
            // 5-min EMA20 surfaced for the same EMA chip the stock card uses.
            CandleAggregator.CandleBar idxLastBar = candleAggregator.getLastCompletedCandle(fyersSym);
            double idxLastClose = idxLastBar != null ? idxLastBar.close : 0;
            String cprBias = "INSIDE";
            if (idxLastClose > 0 && idx.getTc() > 0 && idx.getBc() > 0) {
                double cprTopV = Math.max(idx.getTc(), idx.getBc());
                double cprBotV = Math.min(idx.getTc(), idx.getBc());
                if      (idxLastClose > cprTopV) cprBias = "BULLISH";
                else if (idxLastClose < cprBotV) cprBias = "BEARISH";
            }
            m.put("cprBias", cprBias);
            double idxEma20 = emaService != null ? emaService.getEma(fyersSym) : 0;
            m.put("ema20",  Math.round(idxEma20 * 100.0) / 100.0);
            m.put("lastClose", Math.round(idxLastClose * 100.0) / 100.0);
            // HTF (1-hour) state + factors so the index mini-card can mirror the stock card's
            // W-CPR row (weekly CPR bias + 1h EMA + HTF chip). Same strict 2-factor rule.
            m.put("htfState", indexTrendService.getHtfTrendStateForTicker(ticker, riskSettings.isEnableIndexHtf1hEma20Check()));
            Double idxHtfClose = candleAggregator.getLast1HourClose(fyersSym);
            double idxHtfCloseVal = idxHtfClose != null ? idxHtfClose : 0;
            m.put("htfClose", Math.round(idxHtfCloseVal * 100.0) / 100.0);
            WeeklyCprService.WeeklyLevels weeklyLv = weeklyCprService.getWeeklyLevels(fyersSym);
            double weeklyTop = weeklyLv != null ? weeklyLv.top : 0;
            double weeklyBot = weeklyLv != null ? weeklyLv.bot : 0;
            m.put("weeklyCprTop", Math.round(weeklyTop * 100.0) / 100.0);
            m.put("weeklyCprBot", Math.round(weeklyBot * 100.0) / 100.0);
            String htfCprBias = "INSIDE";
            if (idxHtfCloseVal > 0 && weeklyTop > 0 && weeklyBot > 0) {
                if      (idxHtfCloseVal > weeklyTop) htfCprBias = "BULLISH";
                else if (idxHtfCloseVal < weeklyBot) htfCprBias = "BEARISH";
            }
            m.put("htfCprBias", htfCprBias);
            double idxHtfEma20 = htfEmaService != null ? htfEmaService.getEma(fyersSym) : 0;
            m.put("htfEma20", Math.round(idxHtfEma20 * 100.0) / 100.0);
            // Open Range — high/low of the index's bars in the first {openRangeMinutes}
            // after 9:15 IST. Used by the Primary-Index Open Range Filter; also rendered
            // as the index mini-card's OR Trend chip. Only meaningful on the live trading
            // day (helpers return 0 otherwise).
            int orMins = riskSettings.getOpenRangeMinutes();
            double orHigh = tradingDay ? candleAggregator.getOpenRangeHigh(fyersSym, orMins) : 0;
            double orLow  = tradingDay ? candleAggregator.getOpenRangeLow(fyersSym, orMins)  : 0;
            long orEndMinute = MarketHolidayService.MARKET_OPEN_MINUTE + orMins;
            CandleAggregator.CandleBar orRefBar = candleAggregator.getLastCompletedCandle(fyersSym);
            long latestCloseMinute = orRefBar != null
                ? orRefBar.startMinute + riskSettings.getScannerTimeframe() : 0;
            double orRefClose = orRefBar != null ? orRefBar.close : 0;
            String orTrend;
            if (orHigh <= 0 || orLow <= 0 || latestCloseMinute < orEndMinute) orTrend = "FORMING";
            else if (orRefClose > orHigh)                                     orTrend = "BULLISH";
            else if (orRefClose < orLow)                                      orTrend = "BEARISH";
            else                                                               orTrend = "INSIDE";
            m.put("orHigh", Math.round(orHigh * 100.0) / 100.0);
            m.put("orLow",  Math.round(orLow  * 100.0) / 100.0);
            m.put("orTrend", orTrend);
            m.put("orMinutes", orMins);
            out.add(m);
        }
        return out;
    }
}
