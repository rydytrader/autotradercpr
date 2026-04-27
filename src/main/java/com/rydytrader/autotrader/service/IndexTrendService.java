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
 *   - weekly trend    : ±2 / ±1 / 0
 *   - daily trend     : ±3 / ±2 / 0   (bumped — daily CPR outweighs lagging SMA crossover)
 *   - SMA 20 position : ±1 (LTP above/below 20 SMA)
 *   - SMA 200 position: ±1 (LTP above/below 200 SMA)
 *   - SMA crossover   : ±1 / 0        (binary — lagging signal, reduced weight)
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
    @org.springframework.beans.factory.annotation.Autowired
    private CandleAggregator candleAggregator;

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

        // Compute component scores
        int weeklyScore = scoreTrend(weekly);
        int dailyScore = scoreDailyTrend(daily);

        // SMA Price (mirrors stock-card enableSmaTrendCheck): binary — LTP must be above ALL of
        // 20/50/200 for full bull (or below all for full bear). Anything else = 0. No partial tier.
        int smaPositionScore = scoreSmaPositionAll(ltp, sma, sma50, sma200, false);

        // SMA Alignment (mirrors stock-card enableSmaAlignmentCheck): 20>50>200 = full bull stack;
        // one pair correct (20>50 or 50>200) but not both = partial bull; mirror for bearish.
        // The legacy standalone 20-vs-200 cross score is folded into this rule.
        int smaAlignmentScore = scoreSmaAlignment(sma, sma50, sma200);

        // SMA Pattern (mirrors stock-card requireRtpPattern): R-RTP +2, F-RTP -2, BRAIDED/none 0.
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
        // Pattern: R-RTP +3 (rising railway), F-RTP -3 (falling railway), no RTP -1 (chop/no
        // trend penalty — absence of railway is a soft negative for trend conviction).
        int smaPatternScore = "RAILWAY_UP".equals(smaPattern) ? 3
                             : "RAILWAY_DOWN".equals(smaPattern) ? -3 : -1;

        // HTF (60-min) SMA scoring — uses 20 and 50 only (NOT 200). Matches the HTF Price &
        // Alignment filters which also skip the 200 SMA. Binary scoring (no partial tier
        // possible with just 2 SMAs).
        double htfSma20 = htfSmaService.getSma(symbol);
        double htfSma50 = htfSmaService.getSma50(symbol);
        double htfSma200 = htfSmaService.getSma200(symbol); // display only, not in score
        int htfPriceScore     = scoreHtfPrice(ltp, htfSma20, htfSma50);
        int htfAlignmentScore = scoreHtfAlignment(htfSma20, htfSma50);
        String htfPat = (htfSma20 > 0 && htfSma50 > 0)
            ? htfSmaService.getSmaPattern(symbol,
                riskSettings.getSmaPatternLookback(),
                atrService.getAtr(symbol),
                riskSettings.getBraidedMinCrossovers(),
                riskSettings.getBraidedMaxSpreadAtr(),
                riskSettings.getRailwayMaxCv(),
                riskSettings.getRailwayMinSpreadAtr())
            : "";
        int htfPatternScore = "RAILWAY_UP".equals(htfPat) ? 3
                            : "RAILWAY_DOWN".equals(htfPat) ? -3 : -1;

        // Total = Weekly ±3 + Daily ±3 + 5m(Price ±2 + Align ±2 + Pattern ±3) + HTF(Price ±2 + Align ±2 + Pattern ±3) = ±20
        // CPR and Pattern weighted heavier (±3) than Price/Align (±2). CPR drives structural bias;
        // Pattern (R-RTP/F-RTP) confirms persistent trend. Price/Align are intermediate confirmation.
        int total = weeklyScore + dailyScore
                  + smaPositionScore + smaAlignmentScore + smaPatternScore
                  + htfPriceScore + htfAlignmentScore + htfPatternScore;

        trend.setWeeklyScore(weeklyScore);
        trend.setDailyScore(dailyScore);
        trend.setSmaPositionScore(smaPositionScore);
        trend.setSma200PositionScore(0); // legacy DTO field, no longer scored independently
        trend.setSmaCrossoverScore(smaAlignmentScore); // reuse old DTO field for 5m alignment score
        trend.setSmaPattern(smaPattern);
        trend.setSmaPatternScore(smaPatternScore);
        trend.setHtfSma20(htfSma20);
        trend.setHtfSma50(htfSma50);
        trend.setHtfSma200(htfSma200);
        trend.setHtfSmaPattern(htfPat);
        trend.setHtfPriceScore(htfPriceScore);
        trend.setHtfAlignmentScore(htfAlignmentScore);
        trend.setHtfPatternScore(htfPatternScore);
        trend.setTotalScore(total);

        // ── NIFTY breadth: how many NIFTY 50 stocks are up vs down today ──
        // Iterates the bhavcopy NIFTY 50 universe, compares each stock's live LTP against
        // its prev-day close. Cheap (~50 map lookups) — fine to compute on every poll.
        int advancers = 0, decliners = 0, breadthCount = 0;
        for (var cpr : bhavcopyService.getAllCprLevels().values()) {
            if (!cpr.isInNifty50() || bhavcopyService.isIndex(cpr.getSymbol())) continue;
            double prev = cpr.getClose();
            if (prev <= 0) continue;
            double live = marketDataService.getLtp("NSE:" + cpr.getSymbol() + "-EQ");
            if (live <= 0) continue;
            breadthCount++;
            if (live > prev) advancers++;
            else if (live < prev) decliners++;
        }
        trend.setBreadthAdvancers(advancers);
        trend.setBreadthDecliners(decliners);
        trend.setBreadthTotal(breadthCount);

        // ── Trend state (ADD-driven, 7 tiers) ─────────────────────────────────
        // ADD = advancers count, scaled to a virtual 50-stock universe so the bands
        // hold even when not all 50 NIFTY 50 stocks have ticked yet:
        //   addScore = round(advancers × 50 / breadthCount)
        //
        //   45-50  EXTREME_BULLISH  (≥90% of NIFTY 50 advancing — very strong rally)
        //   38-44  VERY_BULLISH     (76-88% advancing)
        //   28-37  BULLISH          (56-74% advancing — clear majority)
        //   22-27  NEUTRAL          (44-54% — split market)
        //   13-21  BEARISH          (26-42% advancing)
        //    6-12  VERY_BEARISH     (12-24% advancing)
        //    0-5   EXTREME_BEARISH  (≤10% — broad-based selloff)
        // Change% chain: candleAggregator (per-tick) → MarketDataService (currentTicks, retained
        // across full bot lifetime — same source as the scrolling ticker). Stops the NIFTY card
        // showing 0.00% after market close just because the per-tick map hasn't fired today.
        double changePct = candleAggregator != null ? candleAggregator.getChangePct(symbol) : 0;
        if (changePct == 0) changePct = marketDataService.getChangePercent(symbol);
        trend.setChangePct(Math.round(changePct * 100.0) / 100.0);
        int addScore = 0;
        String state;
        if (breadthCount > 0) {
            addScore = (int) Math.round(advancers * 50.0 / breadthCount);
            if      (addScore >= 45) state = "EXTREME_BULLISH";
            else if (addScore >= 38) state = "VERY_BULLISH";
            else if (addScore >= 28) state = "BULLISH";
            else if (addScore >= 22) state = "NEUTRAL";
            else if (addScore >= 13) state = "BEARISH";
            else if (addScore >=  6) state = "VERY_BEARISH";
            else                     state = "EXTREME_BEARISH";
        } else {
            // No live LTPs from any NIFTY 50 stock yet (pre-market or WS not subscribed).
            // Fall back to the legacy composite score so the card shows something.
            state = classify(total);
        }
        trend.setAddScore(addScore);
        trend.setState(state);

        // State is now price-driven via HSM tick's change%. LTP > 0 is enough.
        boolean available = ltp > 0;
        trend.setDataAvailable(available);

        return trend;
    }

    private int scoreTrend(String trendStr) {
        if (trendStr == null) return 0;
        if ("STRONG_BULLISH".equals(trendStr))  return 3;
        if ("STRONG_BEARISH".equals(trendStr))  return -3;
        if ("BULLISH".equals(trendStr))         return 1;
        if ("BEARISH".equals(trendStr))         return -1;
        return 0;  // NEUTRAL or unknown
    }

    /** Daily trend scale — same weight as weekly (±2 max). No special treatment. */
    private int scoreDailyTrend(String trendStr) {
        return scoreTrend(trendStr);
    }

    /** SMA Price (LTP vs all three SMAs): mirrors enableSmaTrendCheck on stock cards.
     *  When {@code partialAllowed=true}: ±2 full / ±1 partial (above-or-below 2 of 3) / 0 mixed.
     *  When {@code partialAllowed=false}: binary — ±2 full only when above/below all three; 0 otherwise.
     *  The 5-min check uses binary mode (matches the stock-card filter, which is pass/fail). */
    private int scoreSmaPositionAll(double ltp, double sma20, double sma50, double sma200, boolean partialAllowed) {
        if (ltp <= 0 || sma20 <= 0 || sma50 <= 0 || sma200 <= 0) return 0;
        int above = (ltp > sma20 ? 1 : 0) + (ltp > sma50 ? 1 : 0) + (ltp > sma200 ? 1 : 0);
        if (above == 3) return 2;     // full bullish — LTP above all three
        if (above == 0) return -2;    // full bearish — LTP below all three
        if (!partialAllowed) return 0;
        if (above == 2) return 1;
        return -1;                    // above == 1 → below 2 of 3
    }

    /** HTF Price (LTP vs 1h SMA 20 and 50 only — 200 excluded by design, matches the HTF
     *  Price filter on stock cards). Binary: ±2 above-or-below both, 0 if split or missing data. */
    private int scoreHtfPrice(double ltp, double htfSma20, double htfSma50) {
        if (ltp <= 0 || htfSma20 <= 0 || htfSma50 <= 0) return 0;
        if (ltp > htfSma20 && ltp > htfSma50) return 2;
        if (ltp < htfSma20 && ltp < htfSma50) return -2;
        return 0;
    }

    /** HTF Alignment (1h SMA 20 vs 50 only — 200 excluded by design, matches the HTF Alignment
     *  filter on stock cards). Binary: ±2 if 20>50 or 20<50, 0 if equal/missing. */
    private int scoreHtfAlignment(double htfSma20, double htfSma50) {
        if (htfSma20 <= 0 || htfSma50 <= 0) return 0;
        if (htfSma20 > htfSma50) return 2;
        if (htfSma20 < htfSma50) return -2;
        return 0;
    }

    /** SMA Alignment (stack ordering 20/50/200): mirrors enableSmaAlignmentCheck on stock cards.
     *  Full ±2 when 20>50>200 (or 20<50<200).
     *  Partial ±1 when one of the two stack pairs (20>50 or 50>200) is bullish/bearish but
     *  not both. Zero on mixed/equal. */
    private int scoreSmaAlignment(double sma20, double sma50, double sma200) {
        if (sma20 <= 0 || sma50 <= 0 || sma200 <= 0) return 0;
        int bullPairs = (sma20 > sma50 ? 1 : 0) + (sma50 > sma200 ? 1 : 0);
        int bearPairs = (sma20 < sma50 ? 1 : 0) + (sma50 < sma200 ? 1 : 0);
        int net = bullPairs - bearPairs;
        if (net >  2) net =  2;
        if (net < -2) net = -2;
        return net;
    }

    private String classify(int score) {
        if (score >= riskSettings.getIndexStrongBullishThreshold()) return "STRONG_BULLISH";
        if (score >= riskSettings.getIndexBullishThreshold())       return "BULLISH";
        if (score <= riskSettings.getIndexStrongBearishThreshold()) return "STRONG_BEARISH";
        if (score <= riskSettings.getIndexBearishThreshold())       return "BEARISH";
        return "NEUTRAL";
    }

    /**
     * Returns true if the given trade direction is OPPOSED to the current NIFTY trend.
     * Buys are opposed on any of the 3 bearish tiers; sells are opposed on any of the
     * 3 bullish tiers. NEUTRAL never opposes. Used by BreakoutScanner to decide
     * HPT → LPT downgrade (or hard-skip when indexAlignmentHardSkip is on).
     */
    public boolean isOpposedToNifty(boolean isBuy) {
        IndexTrend trend = getNiftyTrend();
        if (!trend.isDataAvailable()) return false;
        String state = trend.getState();
        if (state == null) return false;
        if (isBuy) {
            return state.endsWith("BEARISH"); // BEARISH, VERY_BEARISH, EXTREME_BEARISH
        } else {
            return state.endsWith("BULLISH"); // BULLISH, VERY_BULLISH, EXTREME_BULLISH
        }
    }
}
