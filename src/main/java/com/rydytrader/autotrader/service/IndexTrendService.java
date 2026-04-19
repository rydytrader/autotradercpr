package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.IndexTrend;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Computes a composite trend state for an index (NIFTY 50 today) by combining
 * four signals: weekly CPR trend, daily CPR trend, 20 EMA position (LTP vs EMA),
 * and 20 EMA slope. Final score is mapped to one of five states:
 *
 *     STRONG_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONG_BEARISH
 *
 * Used by:
 *   1. NIFTY card on the scanner page (display)
 *   2. BreakoutScanner index alignment filter (downgrade HPT → LPT when opposed)
 *
 * Score weighting:
 *   - weekly trend    : ±2 (strong) / ±1 (mild) / 0
 *   - daily trend     : ±2 (strong) / ±1 (mild) / 0
 *   - EMA 20 position : ±1 (LTP above/below 20 EMA)
 *   - EMA 20 slope    : ±2 (steep) / ±1 (mild) / 0
 *   - EMA 200 position: ±1 (LTP above/below 200 EMA) — long-term trend
 *   - Open=High/Low   : -1 (O=H, bearish) / +1 (O=L, bullish) / 0
 *   - EMA crossover   : +1 (20 EMA > 200 EMA) / -1 (20 EMA < 200 EMA) / 0
 *
 * Score range: -10 to +10
 */
@Service
public class IndexTrendService {

    private static final Logger log = LoggerFactory.getLogger(IndexTrendService.class);

    public static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    public static final String NIFTY_DISPLAY = "NIFTY 50";

    private final WeeklyCprService weeklyCprService;
    private final EmaService emaService;
    private final MarketDataService marketDataService;
    private final RiskSettingsStore riskSettings;
    private final BhavcopyService bhavcopyService;
    private final AtrService atrService;
    private final HtfEmaService htfEmaService;

    public IndexTrendService(WeeklyCprService weeklyCprService,
                             EmaService emaService,
                             MarketDataService marketDataService,
                             RiskSettingsStore riskSettings,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             HtfEmaService htfEmaService) {
        this.weeklyCprService = weeklyCprService;
        this.emaService = emaService;
        this.marketDataService = marketDataService;
        this.riskSettings = riskSettings;
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.htfEmaService = htfEmaService;
    }

    public IndexTrend getNiftyTrend() {
        return computeFor(NIFTY_SYMBOL, NIFTY_DISPLAY);
    }

    private IndexTrend computeFor(String symbol, String displayName) {
        IndexTrend trend = new IndexTrend();
        trend.setSymbol(symbol);
        trend.setDisplayName(displayName);

        // Live LTP from WS ticks, fall back to previous-day bhavcopy close on holidays / weekends
        double ltp = marketDataService.getLtp(symbol);
        if (ltp <= 0) {
            String ticker = symbol;
            int colon = ticker.indexOf(':');
            if (colon >= 0) ticker = ticker.substring(colon + 1);
            if (ticker.endsWith("-INDEX")) ticker = ticker.substring(0, ticker.length() - 6);
            else if (ticker.endsWith("-EQ")) ticker = ticker.substring(0, ticker.length() - 3);
            var cpr = bhavcopyService.getCprLevels(ticker);
            if (cpr != null) ltp = cpr.getClose();
        }
        double ema = emaService.getEma(symbol);
        double ema50 = emaService.getEma50(symbol);
        double ema200 = emaService.getEma200(symbol);
        String weekly = weeklyCprService.getWeeklyTrend(symbol);
        String daily = weeklyCprService.getDailyTrend(symbol);

        trend.setLtp(ltp);
        trend.setEma(ema);
        trend.setEma50(ema50);
        trend.setEma200(ema200);
        trend.setWeeklyTrend(weekly);
        trend.setDailyTrend(daily);

        // Weekly reversal flag — zero out weekly score when active
        boolean reversalActive = !"NONE".equals(weeklyCprService.getWeeklyRejection(symbol));
        trend.setWeeklyReversalActive(reversalActive);

        // Compute component scores
        int weeklyScore = reversalActive ? 0 : scoreTrend(weekly);
        int dailyScore = scoreTrend(daily);
        int emaPositionScore = scoreEmaPosition(ltp, ema);
        int ema200PositionScore = scoreEmaPosition(ltp, ema200);
        // EMA crossover vs 200: +2 if both 20 and 50 above 200 (strong bullish structure),
        //                       +1 if only 20 above 200, -2/-1 symmetric for bearish, 0 otherwise.
        int emaCrossoverScore = 0;
        if (ema > 0 && ema200 > 0) {
            boolean e20Bull = ema > ema200;
            boolean e50Bull = ema50 > 0 && ema50 > ema200;
            boolean e20Bear = ema < ema200;
            boolean e50Bear = ema50 > 0 && ema50 < ema200;
            if (e20Bull && e50Bull) emaCrossoverScore = 2;
            else if (e20Bull) emaCrossoverScore = 1;
            else if (e20Bear && e50Bear) emaCrossoverScore = -2;
            else if (e20Bear) emaCrossoverScore = -1;
        }

        // EMA 20/50 pattern: R-RTP +1, F-RTP -1, BRAIDED/none 0
        String emaPattern = "";
        if (ema > 0 && ema50 > 0) {
            emaPattern = emaService.getEmaPattern(symbol,
                riskSettings.getEmaPatternLookback(),
                atrService.getAtr(symbol),
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr());
        }
        int emaPatternScore = "RAILWAY_UP".equals(emaPattern) ? 2
                             : "RAILWAY_DOWN".equals(emaPattern) ? -2 : 0;

        // Total = Weekly ±2 + Daily ±2 + EMA20 pos ±1 + EMA200 pos ±1 + Cross ±2 + Pattern ±2 = ±10
        int total = weeklyScore + dailyScore + emaPositionScore
                  + ema200PositionScore + emaCrossoverScore + emaPatternScore;

        trend.setWeeklyScore(weeklyScore);
        trend.setDailyScore(dailyScore);
        trend.setEmaPositionScore(emaPositionScore);
        trend.setEma200PositionScore(ema200PositionScore);
        trend.setEmaCrossoverScore(emaCrossoverScore);
        trend.setEmaPattern(emaPattern);
        trend.setEmaPatternScore(emaPatternScore);
        trend.setTotalScore(total);

        // HTF (75-min) EMAs — display only, no score contribution
        double htfEma20 = htfEmaService.getEma(symbol);
        double htfEma50 = htfEmaService.getEma50(symbol);
        double htfEma200 = htfEmaService.getEma200(symbol);
        String htfPat = (htfEma20 > 0 && htfEma50 > 0)
            ? htfEmaService.getEmaPattern(symbol,
                riskSettings.getEmaPatternLookback(),
                atrService.getAtr(symbol),
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr())
            : "";
        trend.setHtfEma20(htfEma20);
        trend.setHtfEma50(htfEma50);
        trend.setHtfEma200(htfEma200);
        trend.setHtfEmaPattern(htfPat);

        // Classify
        trend.setState(classify(total));

        // Mark as available only if we have at least the price data + one structural signal
        boolean available = ltp > 0 && (weeklyScore != 0 || dailyScore != 0 || emaPositionScore != 0
                                        || "NEUTRAL".equals(weekly) || "NEUTRAL".equals(daily));
        trend.setDataAvailable(available);

        return trend;
    }

    private int scoreTrend(String trendStr) {
        if (trendStr == null) return 0;
        if ("STRONG_BULLISH".equals(trendStr))  return 2;
        if ("STRONG_BEARISH".equals(trendStr))  return -2;
        if ("BULLISH".equals(trendStr))         return 1;
        if ("BEARISH".equals(trendStr))         return -1;
        return 0;  // NEUTRAL or unknown
    }

    /** EMA position: +1 if LTP above 20 EMA, -1 if below, 0 if equal or data missing. */
    private int scoreEmaPosition(double ltp, double ema) {
        if (ltp <= 0 || ema <= 0) return 0;
        if (ltp > ema) return 1;
        if (ltp < ema) return -1;
        return 0;
    }

    private String classify(int score) {
        if (score >= riskSettings.getIndexStrongBullishThreshold()) return "STRONG_BULLISH";
        if (score >= riskSettings.getIndexBullishThreshold())       return "BULLISH";
        if (score <= riskSettings.getIndexStrongBearishThreshold()) return "STRONG_BEARISH";
        if (score <= riskSettings.getIndexBearishThreshold())       return "BEARISH";
        return "NEUTRAL";
    }

    /**
     * Returns true if the given trade direction (buy or sell) is OPPOSED to the
     * current NIFTY trend. Used by BreakoutScanner to decide HPT → LPT downgrade.
     * Only returns true for BEARISH/STRONG_BEARISH (vs buy) or BULLISH/STRONG_BULLISH (vs sell).
     */
    public boolean isOpposedToNifty(boolean isBuy) {
        IndexTrend trend = getNiftyTrend();
        if (!trend.isDataAvailable()) return false;
        String state = trend.getState();
        if (isBuy) {
            return "BEARISH".equals(state) || "STRONG_BEARISH".equals(state);
        } else {
            return "BULLISH".equals(state) || "STRONG_BULLISH".equals(state);
        }
    }
}
