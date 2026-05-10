package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.IndexTrend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

/**
 * Computes the NIFTY 50 trend snapshot for the scanner-page card and downstream filters.
 *
 * <p>Two sticky factors, both refreshed only at NIFTY's 5-min candle close — read from
 * the just-closed candles, never live LTP:
 * <ul>
 *   <li>{@code cprBullish}     — NIFTY index 5-min close vs daily CPR
 *       (above top / below bottom / inside)</li>
 *   <li>{@code futVwapBullish} — NIFTY futures 5-min close vs that bar's stamped VWAP
 *       (Fyers ATP at finalize). Same-bar coherent — both numbers come from the same
 *       futures {@link CandleAggregator.CandleBar}.</li>
 * </ul>
 *
 * <p>State combinations:
 * <pre>
 *   CPR bullish + futures vs VWAP bullish    → BULLISH
 *   CPR bearish + futures vs VWAP bearish    → BEARISH
 *   CPR bearish + futures vs VWAP bullish    → BULLISH_REVERSAL  (downtrend rolling over)
 *   CPR bullish + futures vs VWAP bearish    → BEARISH_REVERSAL  (uptrend rolling over)
 *   either factor null + other determined    → SIDEWAYS
 *   both factors null                        → NEUTRAL
 * </pre>
 *
 * <p>The UI card endpoint {@link #getNiftyTrend()} also returns sticky values (not live
 * recomputation), so the chip and state shown on the page match exactly what filters
 * and exits see and don't flicker tick-to-tick within a bar.
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
    private com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private NiftyOptionOiService niftyOptionOiService;

    // Sticky cached factors + supporting values — refreshed only on NIFTY 5-min candle close.
    // null on a Boolean = not yet computed or insufficient data.
    private volatile Boolean cachedCprBullish;
    private volatile Boolean cachedFutVwapBullish;
    private volatile String  cachedFutSymbol = "";
    private volatile double  cachedNiftyClose;
    private volatile double  cachedFutClose;
    private volatile double  cachedFutVwap;
    private volatile String  cachedState = "NEUTRAL";
    // True once we've received the first NIFTY 5-min candle close for the current trading day.
    // Until then, both factors stay null and state stays NEUTRAL — no live tick-driven updates.
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
        // New trading day — clear sticky state. State stays NEUTRAL until the first NIFTY
        // 5-min candle close populates the cache (~9:20 IST).
        cachedCprBullish     = null;
        cachedFutVwapBullish = null;
        cachedFutSymbol      = "";
        cachedNiftyClose     = 0;
        cachedFutClose       = 0;
        cachedFutVwap        = 0;
        cachedState          = "NEUTRAL";
        firstCloseReceived   = false;
    }

    /** Pure snapshot of the 2 factors + supporting values + combined state. No side effects. */
    private record TrendSnapshot(Boolean cprBullish, Boolean futVwapBullish,
                                 String futSymbol,
                                 double niftyClose, double futClose, double futVwap,
                                 String state) {}

    /**
     * Reads the just-closed 5-min candles for both NIFTY index and NIFTY futures and computes
     * the two factors. Called only from {@link #onCandleClose} at NIFTY's 5-min boundary, so
     * the "last completed candle" IS the bar that just fired this listener — same-bar
     * coherent values for both NIFTY's CPR comparison and the futures VWAP comparison.
     */
    private TrendSnapshot computeSnapshot() {
        // Factor 1: NIFTY index 5-min close vs daily CPR
        Boolean cprBullish = null;
        double niftyClose = 0;
        CandleAggregator.CandleBar niftyBar = candleAggregator != null
            ? candleAggregator.getLastCompletedCandle(NIFTY_SYMBOL) : null;
        if (niftyBar != null && niftyBar.close > 0) {
            niftyClose = niftyBar.close;
            var cpr = bhavcopyService.getCprLevels("NIFTY50");
            if (cpr != null && cpr.getTc() > 0 && cpr.getBc() > 0) {
                double top = Math.max(cpr.getTc(), cpr.getBc());
                double bot = Math.min(cpr.getTc(), cpr.getBc());
                if (niftyClose > top)      cprBullish = Boolean.TRUE;
                else if (niftyClose < bot) cprBullish = Boolean.FALSE;
                // else inside CPR → leave null
            }
        }

        // Factor 2: NIFTY futures 5-min close vs that bar's stamped VWAP (same-bar snapshot)
        String futSym = marketDataService.computeNearMonthNiftyFuturesSymbol(
            java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")));
        Boolean futVwapBullish = null;
        double futClose = 0, futVwap = 0;
        CandleAggregator.CandleBar futBar = candleAggregator != null
            ? candleAggregator.getLastCompletedCandle(futSym) : null;
        if (futBar != null && futBar.close > 0 && futBar.vwap > 0) {
            futClose = futBar.close;
            futVwap  = futBar.vwap;
            if (futClose > futVwap)      futVwapBullish = Boolean.TRUE;
            else if (futClose < futVwap) futVwapBullish = Boolean.FALSE;
            // futClose == futVwap → leave null
        }

        // State combination
        String state;
        if (cprBullish == null && futVwapBullish == null)      state = "NEUTRAL";
        else if (cprBullish == null || futVwapBullish == null) state = "SIDEWAYS";
        else if (cprBullish && futVwapBullish)                 state = "BULLISH";
        else if (!cprBullish && !futVwapBullish)               state = "BEARISH";
        else if (!cprBullish && futVwapBullish)                state = "BULLISH_REVERSAL";
        else                                                    state = "BEARISH_REVERSAL";

        return new TrendSnapshot(cprBullish, futVwapBullish, futSym,
                                 niftyClose, futClose, futVwap, state);
    }

    /**
     * Sticky update path — called only by {@link #onCandleClose} at NIFTY 5-min boundaries.
     * Snapshots the just-closed candle values and writes them to the cache.
     */
    private void recomputeStates() {
        TrendSnapshot s = computeSnapshot();
        String prev = cachedState;
        cachedCprBullish     = s.cprBullish();
        cachedFutVwapBullish = s.futVwapBullish();
        cachedFutSymbol      = s.futSymbol() != null ? s.futSymbol() : "";
        cachedNiftyClose     = s.niftyClose();
        cachedFutClose       = s.futClose();
        cachedFutVwap        = s.futVwap();
        cachedState          = s.state();
        if (!s.state().equals(prev)) {
            log.info("[IndexTrend] NIFTY state {} → {} (cpr={} futVwap={} niftyClose={} futClose={} futVwap={})",
                prev, s.state(), s.cprBullish(), s.futVwapBullish(),
                s.niftyClose(), s.futClose(), s.futVwap());
        }
    }

    /** Sticky NIFTY trend state — only updates at NIFTY 5-min candle close. Used by filters
     *  (NIFTY Index Alignment) so trade decisions don't oscillate tick-to-tick within a bar. */
    public String getStickyState() { return cachedState != null ? cachedState : "NEUTRAL"; }

    public IndexTrend getNiftyTrend() {
        // Sticky values for the UI — NO live recomputation. Chip and state on the card update
        // only at NIFTY 5-min candle close, identical to what filters and exits read.
        IndexTrend trend = new IndexTrend();
        trend.setSymbol(NIFTY_SYMBOL);
        trend.setDisplayName(NIFTY_DISPLAY);

        // Live LTP for fallback display only (still needed by some downstream UI bits, e.g.
        // the page's NIFTY-symbol header). The trend factors do NOT use it.
        double liveTickLtp = marketDataService.getLtp(NIFTY_SYMBOL);
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

        // Trend factors + supporting values — STICKY (set at last NIFTY 5-min close).
        trend.setCprBullish(cachedCprBullish);
        trend.setFutVwapBullish(cachedFutVwapBullish);
        trend.setNiftyClose(cachedNiftyClose);
        trend.setFutSymbol(cachedFutSymbol);
        trend.setFutClose(cachedFutClose);
        trend.setFutVwap(cachedFutVwap);
        trend.setState(cachedState);

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

        // NIFTY option-chain Max OI strikes — kept on the DTO for the NIFTY HTF Hurdle filter
        // that consumes them. The scanner card no longer renders them.
        if (niftyOptionOiService != null) {
            trend.setMaxCallOiStrike(niftyOptionOiService.getMaxCallOiStrike());
            trend.setMaxCallOi(niftyOptionOiService.getMaxCallOi());
            trend.setMaxPutOiStrike(niftyOptionOiService.getMaxPutOiStrike());
            trend.setMaxPutOi(niftyOptionOiService.getMaxPutOi());
            trend.setOiLastUpdated(niftyOptionOiService.getLastUpdatedFormatted());
        }

        // dataAvailable = first NIFTY 5-min close has populated the cache.
        trend.setDataAvailable(firstCloseReceived);
        return trend;
    }
}
