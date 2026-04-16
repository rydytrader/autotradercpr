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
 *
 * Score range: -8 to +8
 */
@Service
public class IndexTrendService {

    private static final Logger log = LoggerFactory.getLogger(IndexTrendService.class);

    public static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    public static final String NIFTY_DISPLAY = "NIFTY 50";

    private static final int SLOPE_LOOKBACK_CANDLES = 5;

    // EMA slope thresholds (% per candle)
    private static final double SLOPE_STRONG = 0.02;   // > 0.02% / candle = strong trend
    private static final double SLOPE_MILD   = 0.005;  // > 0.005% / candle = mild trend

    private final WeeklyCprService weeklyCprService;
    private final EmaService emaService;
    private final MarketDataService marketDataService;
    private final RiskSettingsStore riskSettings;
    private final BhavcopyService bhavcopyService;

    public IndexTrendService(WeeklyCprService weeklyCprService,
                             EmaService emaService,
                             MarketDataService marketDataService,
                             RiskSettingsStore riskSettings,
                             BhavcopyService bhavcopyService) {
        this.weeklyCprService = weeklyCprService;
        this.emaService = emaService;
        this.marketDataService = marketDataService;
        this.riskSettings = riskSettings;
        this.bhavcopyService = bhavcopyService;
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
        double ema200 = emaService.getEma200(symbol);
        String weekly = weeklyCprService.getWeeklyTrend(symbol);
        String daily = weeklyCprService.getDailyTrend(symbol);
        double slopePct = emaService.getSlopePctPerCandle(symbol, SLOPE_LOOKBACK_CANDLES);

        trend.setLtp(ltp);
        trend.setEma(ema);
        trend.setEma200(ema200);
        trend.setWeeklyTrend(weekly);
        trend.setDailyTrend(daily);
        trend.setEmaSlopePct(slopePct);

        // Weekly reversal flag — zero out weekly score when active
        boolean reversalActive = !"NONE".equals(weeklyCprService.getWeeklyRejection(symbol));
        trend.setWeeklyReversalActive(reversalActive);

        // Compute component scores
        int weeklyScore = reversalActive ? 0 : scoreTrend(weekly);
        int dailyScore = scoreTrend(daily);
        int emaPositionScore = scoreEmaPosition(ltp, ema);
        int slopeScore = scoreSlope(slopePct);
        int ema200PositionScore = scoreEmaPosition(ltp, ema200);
        int total = weeklyScore + dailyScore + emaPositionScore + slopeScore + ema200PositionScore;

        trend.setWeeklyScore(weeklyScore);
        trend.setDailyScore(dailyScore);
        trend.setEmaPositionScore(emaPositionScore);
        trend.setSlopeScore(slopeScore);
        trend.setEma200PositionScore(ema200PositionScore);
        trend.setTotalScore(total);

        // Classify
        trend.setState(classify(total));

        // Mark as available only if we have at least the price data + one structural signal
        boolean available = ltp > 0 && (weeklyScore != 0 || dailyScore != 0 || emaPositionScore != 0 || slopeScore != 0
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

    private int scoreSlope(double slopePct) {
        if (slopePct >= SLOPE_STRONG)   return 2;
        if (slopePct >= SLOPE_MILD)     return 1;
        if (slopePct <= -SLOPE_STRONG)  return -2;
        if (slopePct <= -SLOPE_MILD)    return -1;
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
