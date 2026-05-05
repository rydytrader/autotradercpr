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

    /**
     * Recompute the 3 sticky factors and combined state from the most recent values
     * (post-close LTP, 5-min SMA 20/50). Called from {@link #onCandleClose} at every NIFTY
     * 5-min boundary. Updates the cached fields atomically.
     */
    private void recomputeStates() {
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
        Boolean smaPriceBullish = null;
        Boolean smaAlignBullish = null;
        double sma20 = smaService != null ? smaService.getSma(NIFTY_SYMBOL)   : 0;
        double sma50 = smaService != null ? smaService.getSma50(NIFTY_SYMBOL) : 0;
        if (ltp > 0 && sma20 > 0 && sma50 > 0) {
            if (ltp > sma20 && ltp > sma50)      smaPriceBullish = Boolean.TRUE;
            else if (ltp < sma20 && ltp < sma50) smaPriceBullish = Boolean.FALSE;
            // else price between SMAs → null (treated as not-bullish for the all-green check)

            if (sma20 > sma50)      smaAlignBullish = Boolean.TRUE;
            else if (sma20 < sma50) smaAlignBullish = Boolean.FALSE;
        }

        // State combination — all 3 factors must align; otherwise SIDEWAYS (or NEUTRAL when
        // CPR is undetermined, e.g. inside CPR or no LTP).
        String state;
        if (cprBullish == null) {
            state = "NEUTRAL";
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

        // Atomic update — log only if state actually changed
        String prev = cachedState;
        cachedCprBullish      = cprBullish;
        cachedSmaPriceBullish = smaPriceBullish;
        cachedSmaAlignBullish = smaAlignBullish;
        cachedState           = state;
        if (!state.equals(prev)) {
            log.info("[IndexTrend] NIFTY state {} → {} (cpr={} smaPrice={} smaAlign={})",
                prev, state, cprBullish, smaPriceBullish, smaAlignBullish);
        }
    }

    public IndexTrend getNiftyTrend() {
        // First 5 minutes of the session (or after a mid-day restart) — no candle close has
        // fired yet for NIFTY today, so the cached states are stale/null. Recompute live from
        // current LTP each call so the user has a directional signal during the opening bar.
        // Once the first NIFTY/futures 5-min candle closes (~9:20 IST), the listener takes
        // over and this fallback stops firing — cached values are sticky between bars.
        if (!firstCloseReceived) {
            recomputeStates();
        }

        IndexTrend trend = new IndexTrend();
        trend.setSymbol(NIFTY_SYMBOL);
        trend.setDisplayName(NIFTY_DISPLAY);

        // Live LTP for display (stale-day guarded). Falls back to bhavcopy prev close pre-market.
        double liveTickLtp = marketDataService.getLtp(NIFTY_SYMBOL);
        double ltp = liveTickLtp;
        if (ltp <= 0) {
            var cpr = bhavcopyService.getCprLevels("NIFTY50");
            if (cpr != null) ltp = cpr.getClose();
        }
        trend.setLtp(liveTickLtp);

        // Live breadth (advancers/decliners across NIFTY 50). Updates every poll — display only.
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
        int addScore = breadthCount > 0 ? (int) Math.round(advancers * 50.0 / breadthCount) : 0;
        trend.setAddScore(addScore);

        // Live change% for display
        double changePct = candleAggregator != null ? candleAggregator.getChangePct(NIFTY_SYMBOL) : 0;
        if (changePct == 0) changePct = marketDataService.getChangePercent(NIFTY_SYMBOL);
        trend.setChangePct(Math.round(changePct * 100.0) / 100.0);

        // Cached (sticky) trend factors — only change at NIFTY 5-min candle boundaries
        trend.setCprBullish(cachedCprBullish);
        trend.setSmaPriceBullish(cachedSmaPriceBullish);
        trend.setSmaAlignBullish(cachedSmaAlignBullish);
        trend.setState(cachedState != null ? cachedState : "NEUTRAL");

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

        trend.setDataAvailable(ltp > 0);
        return trend;
    }
}
