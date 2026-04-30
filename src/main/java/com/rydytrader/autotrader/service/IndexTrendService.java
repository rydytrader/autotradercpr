package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.IndexTrend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

/**
 * Computes the NIFTY 50 trend snapshot for the scanner-page card.
 *
 * <p>Three signals are tracked, all <b>sticky</b> — refreshed only at 5-min candle close:
 * <ul>
 *   <li>{@code cprBullish}    — NIFTY LTP vs daily CPR (above top / below bottom / inside)</li>
 *   <li>{@code futVwapBullish}— near-month NIFTY futures LTP vs futures VWAP</li>
 *   <li>{@code state}         — 5-state combination of the two above</li>
 * </ul>
 *
 * <p>State combinations (reversal name = direction the market is reversing <i>toward</i>):
 * <pre>
 *   CPR bullish + VWAP bullish  → BULLISH
 *   CPR bearish + VWAP bearish  → BEARISH
 *   CPR bullish + VWAP bearish  → BEAR_REVERSAL (above CPR but VWAP rolling over → sell side)
 *   CPR bearish + VWAP bullish  → BULL_REVERSAL (below CPR but VWAP turning up → buy side)
 *   any signal missing/inside CPR → NEUTRAL
 * </pre>
 *
 * <p>Live values (LTP, change%, breadth) update every poll for display, but the three
 * cached states only change on a 5-min boundary — eliminates flicker between bars.
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

    // Sticky cached signals — refreshed only on 5-min candle close for NIFTY index or futures.
    // null = not yet computed or insufficient data.
    private volatile Boolean cachedCprBullish;
    private volatile Boolean cachedFutVwapBullish;
    private volatile String  cachedState = "NEUTRAL";
    // True once we've received the first candle close for NIFTY (or its near-month future)
    // for the current trading day. Until then, getNiftyTrend() recomputes live from current
    // LTP — gives the user a directional signal during the first 5-min bar before any close
    // has fired. Reset on daily reset.
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
     * Fired on every 5-min candle close. We only react to NIFTY index and near-month futures
     * symbols — all other symbols' closes are no-ops here.
     */
    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar candle) {
        if (fyersSymbol == null) return;
        String niftyFut = marketDataService.computeNearMonthNiftyFuturesSymbol(
            java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")));
        if (NIFTY_SYMBOL.equals(fyersSymbol) || niftyFut.equals(fyersSymbol)) {
            recomputeStates();
            firstCloseReceived = true;
        }
    }

    @Override
    public void onDailyReset() {
        // New trading day — clear sticky state so the first 5-min bar runs in live-LTP
        // bootstrap mode again until the day's first NIFTY/futures candle closes (~9:20 IST).
        cachedCprBullish     = null;
        cachedFutVwapBullish = null;
        cachedState          = "NEUTRAL";
        firstCloseReceived   = false;
    }

    /**
     * Recompute the 3 sticky states from the most recent values (post-close LTP, futures
     * LTP/VWAP). Called from {@link #onCandleClose} at every 5-min boundary that involves
     * NIFTY or its near-month future. Updates the cached fields atomically.
     */
    private void recomputeStates() {
        // CPR signal: NIFTY LTP vs daily CPR (above top / below bottom / inside)
        Boolean cprBullish = null;
        double ltp = marketDataService.getLtp(NIFTY_SYMBOL);
        if (ltp > 0) {
            String ticker = "NIFTY50";
            var cprLevels = bhavcopyService.getCprLevels(ticker);
            if (cprLevels != null && cprLevels.getTc() > 0 && cprLevels.getBc() > 0) {
                double cprTop = Math.max(cprLevels.getTc(), cprLevels.getBc());
                double cprBot = Math.min(cprLevels.getTc(), cprLevels.getBc());
                if (ltp > cprTop) cprBullish = Boolean.TRUE;
                else if (ltp < cprBot) cprBullish = Boolean.FALSE;
                // else inside CPR → leave null
            }
        }

        // VWAP signal: near-month NIFTY futures LTP vs futures VWAP
        Boolean futVwapBullish = null;
        if (candleAggregator != null) {
            String futSym = marketDataService.computeNearMonthNiftyFuturesSymbol(
                java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")));
            double futLtp  = candleAggregator.getLtp(futSym);
            double futVwap = candleAggregator.getAtp(futSym);
            if (futLtp > 0 && futVwap > 0) {
                futVwapBullish = futLtp > futVwap;
            }
        }

        // 5-state combination — reversal name = direction the market is reversing TOWARD.
        //   above CPR + VWAP turning down  → BEAR_REVERSAL (sell side)
        //   below CPR + VWAP turning up    → BULL_REVERSAL (buy side)
        String state;
        if (cprBullish == null || futVwapBullish == null) {
            state = "NEUTRAL";
        } else if (cprBullish && futVwapBullish) {
            state = "BULLISH";
        } else if (!cprBullish && !futVwapBullish) {
            state = "BEARISH";
        } else if (cprBullish && !futVwapBullish) {
            state = "BEAR_REVERSAL";
        } else {
            state = "BULL_REVERSAL";
        }

        // Atomic update — log only if state actually changed
        String prev = cachedState;
        cachedCprBullish     = cprBullish;
        cachedFutVwapBullish = futVwapBullish;
        cachedState          = state;
        if (!state.equals(prev)) {
            log.info("[IndexTrend] NIFTY state {} → {} (cpr={} vwap={})",
                prev, state, cprBullish, futVwapBullish);
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

        // Cached (sticky) trend signals — only change at 5-min candle boundaries
        trend.setCprBullish(cachedCprBullish);
        trend.setFutVwapBullish(cachedFutVwapBullish);
        trend.setState(cachedState != null ? cachedState : "NEUTRAL");

        trend.setDataAvailable(ltp > 0);
        return trend;
    }
}
