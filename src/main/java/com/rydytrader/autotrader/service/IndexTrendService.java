package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.IndexTrend;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Computes a composite trend state for an index (NIFTY 50 today) by combining
 * four signals: weekly CPR trend, daily CPR trend, 20 SMA position (LTP vs SMA),
 * and 20 SMA slope. Final score is mapped to one of five states:
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
 *   - SMA 20 position : ±1 (LTP above/below 20 SMA)
 *   - SMA 200 position: ±1 (LTP above/below 200 SMA)
 *   - SMA crossover   : +2/+1/-1/-2 (20 vs 200 / 50 vs 200)
 *   - SMA 20/50 pattern: ±2 (RAILWAY_UP/DOWN) / 0
 *
 * Score range: -10 to +10
 */
@Service
public class IndexTrendService {

    private static final Logger log = LoggerFactory.getLogger(IndexTrendService.class);

    public static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    public static final String NIFTY_DISPLAY = "NIFTY 50";

    private final WeeklyCprService weeklyCprService;
    private final SmaService smaService;
    private final MarketDataService marketDataService;
    private final RiskSettingsStore riskSettings;
    private final BhavcopyService bhavcopyService;
    private final AtrService atrService;
    private final HtfSmaService htfSmaService;

    public IndexTrendService(WeeklyCprService weeklyCprService,
                             SmaService smaService,
                             MarketDataService marketDataService,
                             RiskSettingsStore riskSettings,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             HtfSmaService htfSmaService) {
        this.weeklyCprService = weeklyCprService;
        this.smaService = smaService;
        this.marketDataService = marketDataService;
        this.riskSettings = riskSettings;
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.htfSmaService = htfSmaService;
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
        double sma = smaService.getSma(symbol);
        double sma50 = smaService.getSma50(symbol);
        double sma200 = smaService.getSma200(symbol);
        String weekly = weeklyCprService.getWeeklyTrend(symbol);
        String daily = weeklyCprService.getDailyTrend(symbol);

        trend.setLtp(ltp);
        trend.setSma(sma);
        trend.setSma50(sma50);
        trend.setSma200(sma200);
        trend.setWeeklyTrend(weekly);
        trend.setDailyTrend(daily);

        // Weekly reversal flag — zero out weekly score when active
        boolean reversalActive = !"NONE".equals(weeklyCprService.getWeeklyRejection(symbol));
        trend.setWeeklyReversalActive(reversalActive);

        // Compute component scores
        int weeklyScore = reversalActive ? 0 : scoreTrend(weekly);
        int dailyScore = scoreTrend(daily);
        int smaPositionScore = scoreSmaPosition(ltp, sma);
        int sma200PositionScore = scoreSmaPosition(ltp, sma200);
        // SMA crossover vs 200: +2 if both 20 and 50 above 200 (strong bullish structure),
        //                       +1 if only 20 above 200, -2/-1 symmetric for bearish, 0 otherwise.
        int smaCrossoverScore = 0;
        if (sma > 0 && sma200 > 0) {
            boolean s20Bull = sma > sma200;
            boolean s50Bull = sma50 > 0 && sma50 > sma200;
            boolean s20Bear = sma < sma200;
            boolean s50Bear = sma50 > 0 && sma50 < sma200;
            if (s20Bull && s50Bull) smaCrossoverScore = 2;
            else if (s20Bull) smaCrossoverScore = 1;
            else if (s20Bear && s50Bear) smaCrossoverScore = -2;
            else if (s20Bear) smaCrossoverScore = -1;
        }

        // SMA 20/50 pattern: R-RTP +2, F-RTP -2, BRAIDED/none 0
        String smaPattern = "";
        if (sma > 0 && sma50 > 0) {
            smaPattern = smaService.getSmaPattern(symbol,
                riskSettings.getSmaPatternLookback(),
                atrService.getAtr(symbol),
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr());
        }
        int smaPatternScore = "RAILWAY_UP".equals(smaPattern) ? 2
                             : "RAILWAY_DOWN".equals(smaPattern) ? -2 : 0;

        // Total = Weekly ±2 + Daily ±2 + SMA20 pos ±1 + SMA200 pos ±1 + Cross ±2 + Pattern ±2 = ±10
        int total = weeklyScore + dailyScore + smaPositionScore
                  + sma200PositionScore + smaCrossoverScore + smaPatternScore;

        trend.setWeeklyScore(weeklyScore);
        trend.setDailyScore(dailyScore);
        trend.setSmaPositionScore(smaPositionScore);
        trend.setSma200PositionScore(sma200PositionScore);
        trend.setSmaCrossoverScore(smaCrossoverScore);
        trend.setSmaPattern(smaPattern);
        trend.setSmaPatternScore(smaPatternScore);
        trend.setTotalScore(total);

        // HTF (60-min) SMAs — display only, no score contribution
        double htfSma20 = htfSmaService.getSma(symbol);
        double htfSma50 = htfSmaService.getSma50(symbol);
        double htfSma200 = htfSmaService.getSma200(symbol);
        String htfPat = (htfSma20 > 0 && htfSma50 > 0)
            ? htfSmaService.getSmaPattern(symbol,
                riskSettings.getSmaPatternLookback(),
                atrService.getAtr(symbol),
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr())
            : "";
        trend.setHtfSma20(htfSma20);
        trend.setHtfSma50(htfSma50);
        trend.setHtfSma200(htfSma200);
        trend.setHtfSmaPattern(htfPat);

        // Classify
        trend.setState(classify(total));

        // Mark as available only if we have at least the price data + one structural signal
        boolean available = ltp > 0 && (weeklyScore != 0 || dailyScore != 0 || smaPositionScore != 0
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

    /** SMA position: +1 if LTP above 20 SMA, -1 if below, 0 if equal or data missing. */
    private int scoreSmaPosition(double ltp, double sma) {
        if (ltp <= 0 || sma <= 0) return 0;
        if (ltp > sma) return 1;
        if (ltp < sma) return -1;
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
