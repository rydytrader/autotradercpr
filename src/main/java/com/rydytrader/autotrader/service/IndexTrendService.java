package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.IndexTrend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

/**
 * Computes the NIFTY 50 trend snapshot for the scanner-page card.
 *
 * <p>Three factors are tracked, all <b>sticky</b> — refreshed only at NIFTY's 5-min
 * candle close:
 * <ul>
 *   <li>{@code cprBullish}      — NIFTY LTP vs daily CPR (above top / below bottom / inside)</li>
 *   <li>{@code smaPriceBullish} — NIFTY LTP vs 5-min SMA 20 and 50 (above both / below both / between)</li>
 *   <li>{@code smaAlignBullish} — 5-min SMA 20 vs SMA 50 (20 &gt; 50 / 20 &lt; 50)</li>
 * </ul>
 *
 * <p>State combinations:
 * <pre>
 *   All 3 factors bullish                                → BULLISH
 *   All 3 factors bearish                                → BEARISH
 *   CPR bearish, both SMA factors bullish                → BULLISH_REVERSAL  (downtrend rolling over)
 *   CPR bullish, both SMA factors bearish                → BEARISH_REVERSAL  (uptrend rolling over)
 *   CPR null (inside CPR or no LTP)                      → NEUTRAL
 *   Otherwise (mixed)                                    → SIDEWAYS
 * </pre>
 *
 * <p>Live values (LTP, change%, breadth) update every poll for display, but the cached
 * factors only change on a NIFTY 5-min boundary — eliminates flicker between bars.
 */
@Service
public class IndexTrendService implements CandleAggregator.CandleCloseListener,
                                          CandleAggregator.DailyResetListener {

    private static final Logger log = LoggerFactory.getLogger(IndexTrendService.class);

    public static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    public static final String NIFTY_DISPLAY = "NIFTY 50";

    private final MarketDataService marketDataService;
    private final BhavcopyService bhavcopyService;
    @org.springframework.beans.factory.annotation.Autowired
    private CandleAggregator candleAggregator;
    @org.springframework.beans.factory.annotation.Autowired
    private SmaService smaService;
    @org.springframework.beans.factory.annotation.Autowired
    private com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private NiftyOptionOiService niftyOptionOiService;

    // Sticky cached factors — refreshed only on NIFTY 5-min candle close.
    // null = not yet computed or insufficient data (treated as missing / mixed in state combo).
    private volatile Boolean cachedCprBullish;
    private volatile Boolean cachedSmaPriceBullish;
    private volatile Boolean cachedSmaAlignBullish;
    private volatile String  cachedState = "NEUTRAL";
    // True once we've received the first NIFTY 5-min candle close for the current trading day.
    // Until then, getNiftyTrend() recomputes live from current LTP / SMA so the user sees a
    // directional signal during the opening bar. Reset on daily reset.
    private volatile boolean firstCloseReceived = false;

    public IndexTrendService(MarketDataService marketDataService,
                             BhavcopyService bhavcopyService) {
        this.marketDataService = marketDataService;
        this.bhavcopyService = bhavcopyService;
    }

    @PostConstruct
    public void registerCandleListener() {
        // Self-register so we don't have to thread IndexTrendService through MarketDataService's
        // constructor (avoids the circular dep MarketDataService↔IndexTrendService).
        if (candleAggregator != null) {
            candleAggregator.addListener(this);
        }
    }

    /**
     * Fired on every 5-min candle close. We only react to the NIFTY index symbol — all other
     * symbols' closes are no-ops here.
     */
    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar candle) {
        if (fyersSymbol == null) return;
        if (NIFTY_SYMBOL.equals(fyersSymbol)) {
            recomputeStates();
            firstCloseReceived = true;
        }
    }

    @Override
    public void onDailyReset() {
        // New trading day — clear sticky state so the first 5-min bar runs in live-LTP
        // bootstrap mode again until the day's first NIFTY candle closes (~9:20 IST).
        cachedCprBullish      = null;
        cachedSmaPriceBullish = null;
        cachedSmaAlignBullish = null;
        cachedState           = "NEUTRAL";
        firstCloseReceived    = false;
    }

    /** Pure live snapshot of the 3 factors + combined state. No side effects. */
    private record TrendSnapshot(Boolean cprBullish, Boolean smaPriceBullish,
                                 Boolean smaAlignBullish, String state) {}

    /**
     * Compute the 3 factors and combined state from the latest values (live LTP + blended
     * SMA 20/50). Pure — no caching. Used by both the UI ({@link #getNiftyTrend()}, refreshes
     * every poll) and the candle-close listener (which writes the result to the sticky cache).
     */
    private TrendSnapshot computeSnapshot() {
        double ltp = marketDataService.getLtp(NIFTY_SYMBOL);

        // Factor 1: CPR — NIFTY LTP vs daily CPR (above top / below bottom / inside)
        Boolean cprBullish = null;
        if (ltp > 0) {
            var cprLevels = bhavcopyService.getCprLevels("NIFTY50");
            if (cprLevels != null && cprLevels.getTc() > 0 && cprLevels.getBc() > 0) {
                double cprTop = Math.max(cprLevels.getTc(), cprLevels.getBc());
                double cprBot = Math.min(cprLevels.getTc(), cprLevels.getBc());
                if (ltp > cprTop) cprBullish = Boolean.TRUE;
                else if (ltp < cprBot) cprBullish = Boolean.FALSE;
                // else inside CPR → leave null (NEUTRAL)
            }
        }

        // Factors 2 & 3: NIFTY 5-min SMA 20 / 50
        //
        // SMA Price factor uses the SMA-50 line (anchored to alignment) as the trigger to flip:
        //   • Bullish alignment (20 > 50) AND price above SMA-50  → bullish (price can dip below
        //     SMA-20 and the trend is still considered intact — only a close below SMA-50 flips
        //     it to sideways).
        //   • Bearish alignment (20 < 50) AND price below SMA-50  → bearish (mirror).
        //   • Otherwise → null (alignment sideways, or price has broken SMA-50 against trend).
        //
        // This is wider than requiring price above BOTH SMAs — keeps the trend "stickier"
        // through normal pullbacks toward SMA-20 without flipping sideways.
        Boolean smaPriceBullish = null;
        Boolean smaAlignBullish = null;
        double sma20 = smaService != null ? smaService.getSma(NIFTY_SYMBOL)   : 0;
        double sma50 = smaService != null ? smaService.getSma50(NIFTY_SYMBOL) : 0;
        if (ltp > 0 && sma20 > 0 && sma50 > 0) {
            boolean alignBullish = sma20 > sma50;
            boolean alignBearish = sma20 < sma50;

            if (alignBullish && ltp > sma50)      smaPriceBullish = Boolean.TRUE;
            else if (alignBearish && ltp < sma50) smaPriceBullish = Boolean.FALSE;
            // else null — alignment sideways or price broke SMA-50 against trend direction

            if (alignBullish)      smaAlignBullish = Boolean.TRUE;
            else if (alignBearish) smaAlignBullish = Boolean.FALSE;
        }

        // State combination:
        //   • CPR determined (above/below): CPR + both SMA factors must align for BULLISH/BEARISH;
        //     CPR-vs-SMA-disagreement (CPR bear + SMAs bull, or vice versa) → REVERSAL state;
        //     mixed SMAs → SIDEWAYS.
        //   • CPR undetermined (inside daily CPR or no LTP): fall through to SMA factors —
        //     both SMA factors bullish → BULLISH; both bearish → BEARISH; SMAs not seeded → NEUTRAL;
        //     SMAs mixed → SIDEWAYS. This lets the SMA structure drive the trend during NIFTY
        //     consolidation periods inside CPR, instead of forcing NEUTRAL.
        String state;
        if (cprBullish == null) {
            if (Boolean.TRUE.equals(smaPriceBullish) && Boolean.TRUE.equals(smaAlignBullish)) {
                state = "BULLISH";
            } else if (Boolean.FALSE.equals(smaPriceBullish) && Boolean.FALSE.equals(smaAlignBullish)) {
                state = "BEARISH";
            } else if (smaPriceBullish == null && smaAlignBullish == null) {
                state = "NEUTRAL"; // no data yet (SMAs not seeded)
            } else {
                state = "SIDEWAYS"; // inside CPR with SMAs mixed → genuine no-trend
            }
        } else if (Boolean.TRUE.equals(cprBullish)
                && Boolean.TRUE.equals(smaPriceBullish)
                && Boolean.TRUE.equals(smaAlignBullish)) {
            state = "BULLISH";
        } else if (Boolean.FALSE.equals(cprBullish)
                && Boolean.FALSE.equals(smaPriceBullish)
                && Boolean.FALSE.equals(smaAlignBullish)) {
            state = "BEARISH";
        } else if (Boolean.FALSE.equals(cprBullish)
                && Boolean.TRUE.equals(smaPriceBullish)
                && Boolean.TRUE.equals(smaAlignBullish)) {
            // CPR bearish, but both SMA factors flipped bullish — early reversal of a downtrend.
            state = "BULLISH_REVERSAL";
        } else if (Boolean.TRUE.equals(cprBullish)
                && Boolean.FALSE.equals(smaPriceBullish)
                && Boolean.FALSE.equals(smaAlignBullish)) {
            // CPR bullish, but both SMA factors flipped bearish — early reversal of an uptrend.
            state = "BEARISH_REVERSAL";
        } else {
            state = "SIDEWAYS";
        }
        return new TrendSnapshot(cprBullish, smaPriceBullish, smaAlignBullish, state);
    }

    /**
     * Sticky update path — called only by {@link #onCandleClose} at NIFTY 5-min boundaries.
     * Snapshots the live values and writes them to the cache so the filter (which reads
     * {@link #getStickyState()}) sees a value that only changes at bar boundaries.
     */
    private void recomputeStates() {
        TrendSnapshot s = computeSnapshot();
        String prev = cachedState;
        cachedCprBullish      = s.cprBullish();
        cachedSmaPriceBullish = s.smaPriceBullish();
        cachedSmaAlignBullish = s.smaAlignBullish();
        cachedState           = s.state();
        if (!s.state().equals(prev)) {
            log.info("[IndexTrend] NIFTY state {} → {} (cpr={} smaPrice={} smaAlign={})",
                prev, s.state(), s.cprBullish(), s.smaPriceBullish(), s.smaAlignBullish());
        }
    }

    /** Sticky NIFTY trend state — only updates at NIFTY 5-min candle close. Used by filters
     *  (NIFTY Index Alignment) so trade decisions don't oscillate tick-to-tick within a bar. */
    public String getStickyState() { return cachedState != null ? cachedState : "NEUTRAL"; }

    public IndexTrend getNiftyTrend() {
        // Live snapshot for the UI — chips and state shown on the card update every poll so they
        // stay in sync with the displayed live-blended SMA numbers and current LTP-vs-CPR check.
        // The cached fields used by filters are NOT touched here; they only update at NIFTY 5-min
        // candle close via recomputeStates(). Filters read getStickyState() for sticky behavior.
        TrendSnapshot live = computeSnapshot();

        IndexTrend trend = new IndexTrend();
        trend.setSymbol(NIFTY_SYMBOL);
        trend.setDisplayName(NIFTY_DISPLAY);

        // Live LTP for display (stale-day guarded). Falls back to bhavcopy prev close pre-market.
        double liveTickLtp = marketDataService.getLtp(NIFTY_SYMBOL);
        double ltp = liveTickLtp;
        if (ltp <= 0) {
            var fallbackCpr = bhavcopyService.getCprLevels("NIFTY50");
            if (fallbackCpr != null) ltp = fallbackCpr.getClose();
        }
        trend.setLtp(liveTickLtp);

        // Live breadth (advancers/decliners across NIFTY 50). Updates every poll — display only.
        int advancers = 0, decliners = 0, breadthCount = 0;
        for (var cpr : bhavcopyService.getAllCprLevels().values()) {
            if (!cpr.isInNifty50() || bhavcopyService.isIndex(cpr.getSymbol())) continue;
            double prev = cpr.getClose();
            if (prev <= 0) continue;
            double liveLtp = marketDataService.getLtp("NSE:" + cpr.getSymbol() + "-EQ");
            if (liveLtp <= 0) continue;
            breadthCount++;
            if (liveLtp > prev) advancers++;
            else if (liveLtp < prev) decliners++;
        }
        trend.setBreadthAdvancers(advancers);
        trend.setBreadthDecliners(decliners);
        trend.setBreadthTotal(breadthCount);
        int addScore = breadthCount > 0 ? (int) Math.round(advancers * 50.0 / breadthCount) : 0;
        trend.setAddScore(addScore);

        // Live change% for display
        double changePct = candleAggregator != null ? candleAggregator.getChangePct(NIFTY_SYMBOL) : 0;
        if (changePct == 0) changePct = marketDataService.getChangePercent(NIFTY_SYMBOL);
        trend.setChangePct(Math.round(changePct * 100.0) / 100.0);

        // Live factors for UI display — recomputed on each call so chips and state on the card
        // match the live-blended SMA numbers and current LTP-vs-CPR check.
        trend.setCprBullish(live.cprBullish());
        trend.setSmaPriceBullish(live.smaPriceBullish());
        trend.setSmaAlignBullish(live.smaAlignBullish());
        trend.setState(live.state());

        // Live SMA snapshot for the card display — not sticky.
        if (smaService != null) {
            trend.setSma20(smaService.getSma(NIFTY_SYMBOL));
            trend.setSma50(smaService.getSma50(NIFTY_SYMBOL));
        }

        // CPR width category — NARROW if below the scanner's narrowCprMaxWidth (the upper end
        // of the "narrow" band; narrowCprMinWidth is the lower bound but is typically 0), WIDE
        // if at or above narrowCprMaxWidth. Display-only on the NIFTY card.
        var niftyCpr = bhavcopyService.getCprLevels("NIFTY50");
        if (niftyCpr != null && niftyCpr.getCprWidthPct() > 0 && riskSettings != null) {
            double widthPct = niftyCpr.getCprWidthPct();
            double narrowMax = riskSettings.getNarrowCprMaxWidth();
            String category = widthPct < narrowMax ? "NARROW" : "WIDE";
            trend.setCprWidthPct(Math.round(widthPct * 1000.0) / 1000.0);
            trend.setCprWidthCategory(category);
        }

        // NIFTY option-chain Max OI strikes (refreshed every 15 min by NiftyOptionOiService).
        // Display-only on the card; also consumed by BreakoutScanner.checkNiftyHurdle as
        // additional hurdle candidates. Zero values until first successful fetch.
        if (niftyOptionOiService != null) {
            trend.setMaxCallOiStrike(niftyOptionOiService.getMaxCallOiStrike());
            trend.setMaxCallOi(niftyOptionOiService.getMaxCallOi());
            trend.setMaxPutOiStrike(niftyOptionOiService.getMaxPutOiStrike());
            trend.setMaxPutOi(niftyOptionOiService.getMaxPutOi());
            trend.setOiLastUpdated(niftyOptionOiService.getLastUpdatedFormatted());
        }

        trend.setDataAvailable(ltp > 0);
        return trend;
    }
}
