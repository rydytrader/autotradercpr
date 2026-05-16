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
 *   CPR bullish + futures vs VWAP bullish              → BULLISH
 *   CPR bearish + futures vs VWAP bearish              → BEARISH
 *   NIFTY close > EMA20 + futures vs VWAP bullish      → BULLISH_REVERSAL (downtrend rolling over)
 *   NIFTY close < EMA20 + futures vs VWAP bearish      → BEARISH_REVERSAL (uptrend rolling over)
 *   either factor null + other determined              → SIDEWAYS
 *   both factors null                                  → NEUTRAL
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
    @org.springframework.beans.factory.annotation.Autowired
    private EmaService emaService;
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
        }
    }

    @Override
    public void onDailyReset() {
        // Intentionally NOT clearing the sticky cache on daily reset. The cache reflects
        // "last known state" — on weekends and holidays we want the user to see the previous
        // session's last 5-min-close trend state, not a blank NEUTRAL card. The cache is
        // overwritten naturally at the first NIFTY 5-min close of the next trading day
        // (~9:20 IST on Monday), so stale values self-correct as soon as fresh data flows.
    }

    /** Pure snapshot of the 2 factors + supporting values + combined state. No side effects. */
    private record TrendSnapshot(Boolean cprBullish, Boolean futVwapBullish,
                                 String futSymbol,
                                 double niftyClose, double futClose, double futVwap,
                                 String state) {}

    /**
     * Resolve "the last completed bar we know about" for a symbol — prefers today's most
     * recent completed 5-min candle, falls back to the last bar of the prior trading day
     * when today hasn't produced any bars yet (Monday morning before the first 5-min close,
     * or right after a server restart pre-market). Returns null if neither is available.
     */
    private CandleAggregator.CandleBar lastAvailableBar(String symbol) {
        if (candleAggregator == null || symbol == null) return null;
        CandleAggregator.CandleBar today = candleAggregator.getLastCompletedCandle(symbol);
        if (today != null && today.close > 0) return today;
        java.util.List<CandleAggregator.CandleBar> priors = candleAggregator.getPriorDayCandles(symbol);
        if (priors == null || priors.isEmpty()) return null;
        return priors.get(priors.size() - 1);
    }

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
        CandleAggregator.CandleBar niftyBar = lastAvailableBar(NIFTY_SYMBOL);
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
        CandleAggregator.CandleBar futBar = lastAvailableBar(futSym);
        if (futBar != null && futBar.close > 0 && futBar.vwap > 0) {
            futClose = futBar.close;
            futVwap  = futBar.vwap;
            if (futClose > futVwap)      futVwapBullish = Boolean.TRUE;
            else if (futClose < futVwap) futVwapBullish = Boolean.FALSE;
            // futClose == futVwap → leave null
        }

        // EMA 20 factor — optional, gated by enableNiftyEma20Factor. When enabled, NIFTY's
        // last close vs EMA20 contributes as one of the optional trend factors. When
        // disabled, drops out of the state calculation entirely.
        boolean emaEnabled = riskSettings != null && riskSettings.isEnableNiftyEma20Factor();
        double ema20 = (emaEnabled && emaService != null) ? emaService.getEma(NIFTY_SYMBOL) : 0;
        Boolean emaBullish = null;
        if (emaEnabled && niftyClose > 0 && ema20 > 0) {
            if (niftyClose > ema20)      emaBullish = Boolean.TRUE;
            else if (niftyClose < ema20) emaBullish = Boolean.FALSE;
        }

        // FUT VWAP factor — optional, gated by enableNiftyFutVwapFactor (parallel to the
        // EMA 20 factor). When OFF, trend reduces to CPR (plus EMA20 if that factor is on).
        boolean futVwapEnabled = riskSettings == null || riskSettings.isEnableNiftyFutVwapFactor();

        // Effective optional votes — null when the factor is disabled OR no data yet. The
        // state machine below treats null as "doesn't participate."
        Boolean futVwapVote = futVwapEnabled ? futVwapBullish : null;
        Boolean emaVote     = emaEnabled     ? emaBullish     : null;

        // Tally bullish vs bearish among the active optional factors (futVwap, EMA20).
        int optBull = 0, optBear = 0;
        if (Boolean.TRUE.equals(futVwapVote))  optBull++;
        if (Boolean.FALSE.equals(futVwapVote)) optBear++;
        if (Boolean.TRUE.equals(emaVote))      optBull++;
        if (Boolean.FALSE.equals(emaVote))     optBear++;
        boolean anyOptionalEnabled = futVwapEnabled || emaEnabled;

        // State combination — works across all 4 (futVwap × EMA20) toggle combinations.
        // NEUTRAL: no CPR signal (inside CPR or no data).
        // BULLISH / BEARISH: CPR aligned AND every active optional factor agrees with CPR.
        // BULLISH_REVERSAL / BEARISH_REVERSAL: CPR disagrees but ALL active optional factors
        //   point the same direction (requires at least one optional factor enabled).
        // SIDEWAYS: factors disagree / partial data → no actionable bias.
        String state;
        if (cprBullish == null) {
            state = "NEUTRAL";
        } else if (cprBullish && optBear == 0 && (optBull > 0 || !anyOptionalEnabled)) {
            state = "BULLISH";
        } else if (!cprBullish && optBull == 0 && (optBear > 0 || !anyOptionalEnabled)) {
            state = "BEARISH";
        } else if (!cprBullish && optBull > 0 && optBear == 0) {
            state = "BULLISH_REVERSAL";
        } else if (cprBullish && optBear > 0 && optBull == 0) {
            state = "BEARISH_REVERSAL";
        } else {
            state = "SIDEWAYS";
        }

        return new TrendSnapshot(cprBullish, futVwapBullish, futSym,
                                 niftyClose, futClose, futVwap, state);
    }

    /**
     * Sticky update path — called only by {@link #onCandleClose} at NIFTY 5-min boundaries.
     * Snapshots the just-closed candle values and writes them to the cache.
     */
    /** Force a sticky-state recompute from the most recent inputs. Called from
     *  SettingsController right after a trend-factor toggle change so the NIFTY card
     *  reflects the new setting immediately instead of waiting for the next 5-min close. */
    public void recomputeStates() {
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
        // Lazy bootstrap — if the cache is empty (server restarted on a weekend / pre-market,
        // no NIFTY 5-min close has fired yet), try a one-shot recompute from whatever bars
        // CandleAggregator has seeded from history. This gives the user the last-known
        // session's trend state on weekends even right after a restart.
        if ("NEUTRAL".equals(cachedState) && cachedCprBullish == null
                && cachedFutVwapBullish == null && cachedNiftyClose == 0) {
            recomputeStates();
        }

        // Sticky values for the UI — NO live recomputation beyond the lazy bootstrap above.
        // Chip and state on the card update only at NIFTY 5-min candle close, identical to
        // what filters and exits read.
        IndexTrend trend = new IndexTrend();
        trend.setSymbol(NIFTY_SYMBOL);
        trend.setDisplayName(NIFTY_DISPLAY);

        // LTP for the card. Live tick if flowing; otherwise fall back to bhavcopy's prev
        // close so pre-market / weekends still show a meaningful number instead of 0.
        double liveTickLtp = marketDataService.getLtp(NIFTY_SYMBOL);
        double displayLtp = liveTickLtp;
        if (displayLtp <= 0) {
            var fallbackCpr = bhavcopyService.getCprLevels("NIFTY50");
            if (fallbackCpr != null) displayLtp = fallbackCpr.getClose();
        }
        trend.setLtp(displayLtp);

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
        Boolean dispCpr      = cachedCprBullish;
        Boolean dispFutVwap  = cachedFutVwapBullish;
        double  dispNiftyClose = cachedNiftyClose;
        double  dispFutClose   = cachedFutClose;
        double  dispFutVwapVal = cachedFutVwap;
        String  dispFutSym     = cachedFutSymbol;
        String  dispState      = cachedState;

        // Live-LTP fallback for the UI only. After a restart the sticky cache stays NEUTRAL
        // until the next NIFTY 5-min boundary fires — that's a 0-5 min window where the
        // card would otherwise show nothing. Fill in any null factor from live LTP + live
        // ATP (running session VWAP) so the user sees an immediate read. Sticky cache and
        // getStickyState() (used by filters/exits) are untouched — they keep updating only
        // at 5-min closes to avoid intra-bar oscillation.
        if (dispCpr == null || dispFutVwap == null) {
            // Factor 1 — NIFTY LTP vs daily CPR
            if (dispCpr == null) {
                double niftyLtp = marketDataService.getLtp(NIFTY_SYMBOL);
                var cpr = bhavcopyService.getCprLevels("NIFTY50");
                if (niftyLtp > 0 && cpr != null && cpr.getTc() > 0 && cpr.getBc() > 0) {
                    double top = Math.max(cpr.getTc(), cpr.getBc());
                    double bot = Math.min(cpr.getTc(), cpr.getBc());
                    if (niftyLtp > top)      dispCpr = Boolean.TRUE;
                    else if (niftyLtp < bot) dispCpr = Boolean.FALSE;
                    dispNiftyClose = niftyLtp;
                }
            }
            // Factor 2 — futures LTP vs futures running VWAP (Fyers ATP)
            if (dispFutVwap == null) {
                String futSym = marketDataService.computeNearMonthNiftyFuturesSymbol(
                    java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")));
                double futLtp = marketDataService.getLtp(futSym);
                double futAtp = candleAggregator != null ? candleAggregator.getAtp(futSym) : 0;
                if (futLtp > 0 && futAtp > 0) {
                    if (futLtp > futAtp)      dispFutVwap = Boolean.TRUE;
                    else if (futLtp < futAtp) dispFutVwap = Boolean.FALSE;
                    dispFutClose   = futLtp;
                    dispFutVwapVal = futAtp;
                    dispFutSym     = futSym;
                }
            }
            // Re-derive the display state from the augmented factors using the same logic as
            // computeSnapshot — EMA20 optionally gates the reversals.
            boolean emaEnabledLive = riskSettings != null && riskSettings.isEnableNiftyEma20Factor();
            double ema20Live = (emaEnabledLive && emaService != null) ? emaService.getEma(NIFTY_SYMBOL) : 0;
            Boolean emaBullishLive = null;
            if (emaEnabledLive && dispNiftyClose > 0 && ema20Live > 0) {
                if (dispNiftyClose > ema20Live)      emaBullishLive = Boolean.TRUE;
                else if (dispNiftyClose < ema20Live) emaBullishLive = Boolean.FALSE;
            }
            if (dispCpr == null && dispFutVwap == null) {
                dispState = "NEUTRAL";
            } else if (dispCpr != null && dispFutVwap != null && dispCpr && dispFutVwap) {
                dispState = "BULLISH";
            } else if (dispCpr != null && dispFutVwap != null && !dispCpr && !dispFutVwap) {
                dispState = "BEARISH";
            } else if (emaEnabledLive
                    && Boolean.TRUE.equals(emaBullishLive) && Boolean.TRUE.equals(dispFutVwap)) {
                dispState = "BULLISH_REVERSAL";
            } else if (emaEnabledLive
                    && Boolean.FALSE.equals(emaBullishLive) && Boolean.FALSE.equals(dispFutVwap)) {
                dispState = "BEARISH_REVERSAL";
            } else if (!emaEnabledLive
                    && Boolean.FALSE.equals(dispCpr) && Boolean.TRUE.equals(dispFutVwap)) {
                dispState = "BULLISH_REVERSAL";
            } else if (!emaEnabledLive
                    && Boolean.TRUE.equals(dispCpr) && Boolean.FALSE.equals(dispFutVwap)) {
                dispState = "BEARISH_REVERSAL";
            } else {
                dispState = "SIDEWAYS";
            }
        }

        trend.setCprBullish(dispCpr);
        trend.setFutVwapBullish(dispFutVwap);
        trend.setNiftyClose(dispNiftyClose);
        trend.setFutSymbol(dispFutSym);
        trend.setFutClose(dispFutClose);
        trend.setFutVwap(dispFutVwapVal);
        trend.setState(dispState);

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

        // dataAvailable gates the whole card render in the UI. True if we have any LTP
        // (live tick OR bhavcopy fallback for weekends / pre-market) — the trend chips
        // themselves render placeholder ("CPR - —", "FUT ↔ VWAP", state NEUTRAL) until the
        // first 5-min close populates the sticky cache, but the card structure stays visible.
        trend.setDataAvailable(displayLtp > 0);

        // NIFTY 5-min EMA 20 for the card chip. Always populated when EmaService has enough
        // data, so the chip stays visible after market close (no need for live SSE ticks).
        // ema20FactorEnabled mirrors the user setting — UI hides the chip when this is off.
        boolean emaFactorEnabled = riskSettings != null && riskSettings.isEnableNiftyEma20Factor();
        trend.setEma20FactorEnabled(emaFactorEnabled);
        if (emaFactorEnabled && emaService != null) {
            double ema20Val = emaService.getEma(NIFTY_SYMBOL);
            trend.setEma20(Math.round(ema20Val * 100.0) / 100.0);
        }
        // FUT VWAP factor flag — UI hides the FUT VWAP chip when this is off. Cached close
        // and vwap values are still set above (so the chip can render its number when
        // visible); only the trend-state contribution is gated.
        boolean futVwapFactorEnabled = riskSettings != null && riskSettings.isEnableNiftyFutVwapFactor();
        trend.setFutVwapFactorEnabled(futVwapFactorEnabled);
        return trend;
    }
}
