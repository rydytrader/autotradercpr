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
 * the just-closed candle, never live LTP. Mirrors the per-sector state machine in
 * {@link #getSectorTrendForTicker(String)}:
 * <ul>
 *   <li>{@code cprBullish} — NIFTY index 5-min close vs daily CPR (above top / below
 *       bottom / inside).</li>
 *   <li>{@code emaBullish} — NIFTY index 5-min close vs 20-period 5-min EMA. Always-on,
 *       no user toggle.</li>
 * </ul>
 *
 * <p>State combinations:
 * <pre>
 *   CPR bullish + EMA not bearish    → BULLISH
 *   CPR bearish + EMA not bullish    → BEARISH
 *   CPR bearish + EMA bullish        → BULLISH_REVERSAL (downtrend rolling over)
 *   CPR bullish + EMA bearish        → BEARISH_REVERSAL (uptrend rolling over)
 *   CPR null (inside / no data)      → NEUTRAL
 *   otherwise                        → SIDEWAYS
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
    private volatile Boolean cachedEmaBullish;
    private volatile double  cachedNiftyClose;
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
    private record TrendSnapshot(Boolean cprBullish, Boolean emaBullish,
                                 double niftyClose,
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
     * Reads the just-closed 5-min NIFTY index candle and computes the two factors
     * (CPR + EMA20). Called only from {@link #onCandleClose} at NIFTY's 5-min boundary,
     * so the "last completed candle" IS the bar that just fired this listener.
     * Mirrors the per-sector state machine in {@link #getSectorTrendForTicker(String)}.
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

        // Factor 2: NIFTY index 5-min close vs 5-min EMA20. Always-on, no user toggle.
        double ema20 = emaService != null ? emaService.getEma(NIFTY_SYMBOL) : 0;
        Boolean emaBullish = null;
        if (niftyClose > 0 && ema20 > 0) {
            if (niftyClose > ema20)      emaBullish = Boolean.TRUE;
            else if (niftyClose < ema20) emaBullish = Boolean.FALSE;
        }

        String state = deriveState(cprBullish, emaBullish);
        return new TrendSnapshot(cprBullish, emaBullish, niftyClose, state);
    }

    /**
     * 2-factor state machine — identical to {@link #getSectorTrendForTicker(String)}
     * except null-CPR returns NEUTRAL (the sector helper returns INSIDE) so the NIFTY
     * card / downstream filters keep their pre-existing state vocabulary.
     */
    private static String deriveState(Boolean cprBullish, Boolean emaBullish) {
        if (cprBullish == null) return "NEUTRAL";
        if (Boolean.TRUE.equals(cprBullish)  && !Boolean.FALSE.equals(emaBullish)) return "BULLISH";
        if (Boolean.FALSE.equals(cprBullish) && !Boolean.TRUE.equals(emaBullish))  return "BEARISH";
        if (Boolean.FALSE.equals(cprBullish) && Boolean.TRUE.equals(emaBullish))   return "BULLISH_REVERSAL";
        if (Boolean.TRUE.equals(cprBullish)  && Boolean.FALSE.equals(emaBullish))  return "BEARISH_REVERSAL";
        return "SIDEWAYS";
    }

    /**
     * Sticky update path — called only by {@link #onCandleClose} at NIFTY 5-min boundaries.
     * Snapshots the just-closed candle values and writes them to the cache.
     */
    /** Force a sticky-state recompute from the most recent inputs. Kept for callers
     *  (e.g. SettingsController) that previously refreshed the cache after a toggle. */
    public void recomputeStates() {
        TrendSnapshot s = computeSnapshot();
        String prev = cachedState;
        cachedCprBullish = s.cprBullish();
        cachedEmaBullish = s.emaBullish();
        cachedNiftyClose = s.niftyClose();
        cachedState      = s.state();
        if (!s.state().equals(prev)) {
            log.info("[IndexTrend] NIFTY state {} → {} (cpr={} ema={} niftyClose={})",
                prev, s.state(), s.cprBullish(), s.emaBullish(), s.niftyClose());
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
                && cachedEmaBullish == null && cachedNiftyClose == 0) {
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
        Boolean dispCpr        = cachedCprBullish;
        Boolean dispEma        = cachedEmaBullish;
        double  dispNiftyClose = cachedNiftyClose;
        String  dispState      = cachedState;

        // Live-LTP fallback for the UI only. After a restart the sticky cache stays NEUTRAL
        // until the next NIFTY 5-min boundary fires — that's a 0-5 min window where the card
        // would otherwise show nothing. Fill in the CPR factor from live LTP so the user sees
        // an immediate read. Sticky cache and getStickyState() (used by filters/exits) are
        // untouched — they keep updating only at 5-min closes to avoid intra-bar oscillation.
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
            // Re-derive EMA factor against the live close we just took, when possible.
            double ema20Live = emaService != null ? emaService.getEma(NIFTY_SYMBOL) : 0;
            if (dispNiftyClose > 0 && ema20Live > 0) {
                if (dispNiftyClose > ema20Live)      dispEma = Boolean.TRUE;
                else if (dispNiftyClose < ema20Live) dispEma = Boolean.FALSE;
            }
            dispState = deriveState(dispCpr, dispEma);
        }

        trend.setCprBullish(dispCpr);
        trend.setNiftyClose(dispNiftyClose);
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

        // NIFTY 5-min EMA 20 for the card chip. EMA20 is always-on now — no user toggle —
        // so the chip is always populated when EmaService has enough data, keeping it
        // visible after market close (no need for live SSE ticks).
        if (emaService != null) {
            double ema20Val = emaService.getEma(NIFTY_SYMBOL);
            trend.setEma20(Math.round(ema20Val * 100.0) / 100.0);
        }
        return trend;
    }

    /**
     * Compute the combined trend state for any sector index ticker (e.g. "NIFTYBANK").
     * Two-factor: LTP vs daily CPR zone + LTP vs 5-min EMA20. Returns one of:
     *   BULLISH, BEARISH, BULLISH_REVERSAL, BEARISH_REVERSAL, INSIDE, NEUTRAL.
     * Same state machine the Sector Trends modal uses. Used by the Sector Alignment
     * filter in BreakoutScanner so per-stock entries can be gated by their sector's
     * trend, parallel to the existing NIFTY Index Alignment filter.
     */
    public String getSectorTrendForTicker(String sectorIndexTicker) {
        if (sectorIndexTicker == null || sectorIndexTicker.isEmpty()) return "NEUTRAL";
        var cpr = bhavcopyService.getCprLevels(sectorIndexTicker);
        if (cpr == null) return "NEUTRAL";
        String fyersSym = "NSE:" + sectorIndexTicker + "-INDEX";
        double ltp = marketDataService.getLtp(fyersSym);
        double prevClose = cpr.getClose();
        double refLtp = ltp > 0 ? ltp : prevClose;
        if (refLtp <= 0) return "NEUTRAL";

        double top = (cpr.getTc() > 0 && cpr.getBc() > 0) ? Math.max(cpr.getTc(), cpr.getBc()) : 0;
        double bot = (cpr.getTc() > 0 && cpr.getBc() > 0) ? Math.min(cpr.getTc(), cpr.getBc()) : 0;
        double ema20 = emaService != null ? emaService.getEma(fyersSym) : 0;

        Boolean cprBullish = null;
        if (top > 0 && bot > 0) {
            if (refLtp > top)      cprBullish = Boolean.TRUE;
            else if (refLtp < bot) cprBullish = Boolean.FALSE;
        }
        Boolean emaBullish = null;
        if (ema20 > 0) {
            if (refLtp > ema20)      emaBullish = Boolean.TRUE;
            else if (refLtp < ema20) emaBullish = Boolean.FALSE;
        }
        if (cprBullish == null) return "INSIDE";
        if (Boolean.TRUE.equals(cprBullish)  && !Boolean.FALSE.equals(emaBullish)) return "BULLISH";
        if (Boolean.FALSE.equals(cprBullish) && !Boolean.TRUE.equals(emaBullish))  return "BEARISH";
        if (Boolean.FALSE.equals(cprBullish) && Boolean.TRUE.equals(emaBullish))   return "BULLISH_REVERSAL";
        if (Boolean.TRUE.equals(cprBullish)  && Boolean.FALSE.equals(emaBullish))  return "BEARISH_REVERSAL";
        return "SIDEWAYS";
    }

    /**
     * Look up the sector index that a stock belongs to and return its trend state.
     * Returns NEUTRAL when the stock has no sector mapping, no sector-index mapping,
     * or the sector index has no CPR/LTP data yet — in all those cases the alignment
     * filter should fall through (fail-open).
     */
    public String getSectorTrendForStock(String fyersSymbol) {
        if (fyersSymbol == null) return "NEUTRAL";
        // Strip "NSE:" prefix and "-EQ"/"-INDEX" suffix to get the bare ticker.
        String ticker = fyersSymbol;
        int colon = ticker.indexOf(':');
        if (colon >= 0) ticker = ticker.substring(colon + 1);
        ticker = ticker.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
        String sector = bhavcopyService.getSector(ticker);
        if (sector == null || sector.isEmpty()) return "NEUTRAL";
        String sectorTicker = bhavcopyService.getSectorIndexTicker(sector);
        if (sectorTicker == null) return "NEUTRAL";
        return getSectorTrendForTicker(sectorTicker);
    }
}
