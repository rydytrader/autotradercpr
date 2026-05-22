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
    @org.springframework.beans.factory.annotation.Autowired
    private HtfEmaService htfEmaService;
    @org.springframework.beans.factory.annotation.Autowired
    private WeeklyCprService weeklyCprService;

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
     * Reads the just-closed 5-min NIFTY index candle and derives the trend state from
     * <b>two factors</b>: last 5-min close vs daily CPR + last 5-min close vs 5-min EMA20.
     * Mirrors the Stock HTF Trend Alignment state machine so the index alignment chip
     * surfaces BULLISH / BEARISH / BULLISH_REVERSAL / BEARISH_REVERSAL / INSIDE / SIDEWAYS /
     * NEUTRAL — same vocabulary as the per-stock 1-hour trend state. EMA20 uses the
     * STEPPED value (no live LTP blend) so the state stays stable between 5-min boundaries
     * and only flips at the next candle close.
     */
    private TrendSnapshot computeSnapshot() {
        Boolean cprBullish = null;
        boolean insideCpr = false;
        double niftyClose = 0;
        double cprTop = 0, cprBot = 0;
        CandleAggregator.CandleBar niftyBar = lastAvailableBar(NIFTY_SYMBOL);
        if (niftyBar != null && niftyBar.close > 0) {
            niftyClose = niftyBar.close;
            var cpr = bhavcopyService.getCprLevels("NIFTY50");
            if (cpr != null && cpr.getTc() > 0 && cpr.getBc() > 0) {
                cprTop = Math.max(cpr.getTc(), cpr.getBc());
                cprBot = Math.min(cpr.getTc(), cpr.getBc());
                if (niftyClose > cprTop)      cprBullish = Boolean.TRUE;
                else if (niftyClose < cprBot) cprBullish = Boolean.FALSE;
                else                          insideCpr = true;
            }
        }
        double ema20 = emaService != null ? emaService.getSteppedEma(NIFTY_SYMBOL) : 0;
        Boolean emaBullish = null;
        if (niftyClose > 0 && ema20 > 0) {
            if (niftyClose > ema20)      emaBullish = Boolean.TRUE;
            else if (niftyClose < ema20) emaBullish = Boolean.FALSE;
        }
        String state = deriveTrendState(niftyClose, cprTop, cprBot, ema20);
        return new TrendSnapshot(cprBullish, emaBullish, niftyClose, state);
    }

    /**
     * CPR-only state machine. Replaces the prior 2-factor (CPR + EMA20) derivation for the
     * <i>index</i> trend (NIFTY 50 + sector indices). Produces four states only:
     *   • cprBullish == TRUE  → BULLISH
     *   • cprBullish == FALSE → BEARISH
     *   • cprBullish == null, insideCpr == true  → INSIDE  (hard reject in alignment)
     *   • cprBullish == null, insideCpr == false → NEUTRAL (fail-open — no data yet)
     */
    private static String deriveCprOnlyState(Boolean cprBullish, boolean insideCpr) {
        if (cprBullish == null) return insideCpr ? "INSIDE" : "NEUTRAL";
        return Boolean.TRUE.equals(cprBullish) ? "BULLISH" : "BEARISH";
    }

    /**
     * Public 2-factor state helper retained for <b>per-stock HTF alignment</b> paths only —
     * the stock's 1-hour close vs weekly CPR + 1-hour EMA20 still uses the full six-state
     * machine (BULLISH / BEARISH / BULLISH_REVERSAL / BEARISH_REVERSAL / INSIDE / SIDEWAYS /
     * NEUTRAL). Note: the <i>index</i> trend is now CPR-only — do not use this for index
     * paths; use {@link #deriveCprOnlyState} instead.
     *
     * <p>Returns NEUTRAL when close ≤ 0 or CPR data is missing so the caller can fail-open.
     */
    public static String deriveTrendState(double close, double cprTop, double cprBot, double ema20) {
        if (close <= 0) return "NEUTRAL";
        Boolean cprBullish = null;
        boolean insideCpr = false;
        if (cprTop > 0 && cprBot > 0) {
            double top = Math.max(cprTop, cprBot);
            double bot = Math.min(cprTop, cprBot);
            if (close > top)      cprBullish = Boolean.TRUE;
            else if (close < bot) cprBullish = Boolean.FALSE;
            else                  insideCpr = true;
        }
        if (cprBullish == null) return insideCpr ? "INSIDE" : "NEUTRAL";
        // Strict 2-factor: BULLISH requires both close > CPR top AND close > EMA20;
        // BEARISH requires both close < CPR bot AND close < EMA20. Any disagreement or
        // a missing EMA → SIDEWAYS (trade blocked by alignment filter). Reversal states
        // dropped — early-reversal entries no longer escape the gate.
        if (ema20 <= 0) return "SIDEWAYS";
        boolean emaUp   = close > ema20;
        boolean emaDown = close < ema20;
        if (Boolean.TRUE.equals(cprBullish)  && emaUp)   return "BULLISH";
        if (Boolean.FALSE.equals(cprBullish) && emaDown) return "BEARISH";
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
     *  (NIFTY Index Alignment) so trade decisions don't oscillate tick-to-tick within a bar.
     *
     *  <p>Lazy bootstrap: when the cache is empty (server just restarted pre-market and no
     *  NIFTY 5-min boundary has fired yet), recompute once from the last available bar so
     *  callers like the /api/index/key-indices endpoint see the same on-demand-derived state
     *  that sector indices show — instead of NEUTRAL until the next NIFTY close. Sector
     *  indices already work pre-market via getSectorTrendForTicker; this brings NIFTY 50
     *  to parity. */
    public String getStickyState() {
        // cachedState is initialized to "NEUTRAL" (string, not null) — so the sentinel
        // for "bootstrap needed" is "NEUTRAL" plus all factor caches unset. Same check
        // pattern getNiftyTrend uses.
        if ("NEUTRAL".equals(cachedState) && cachedCprBullish == null
                && cachedEmaBullish == null && cachedNiftyClose == 0) {
            recomputeStates();
        }
        return cachedState != null ? cachedState : "NEUTRAL";
    }

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
        double  dispNiftyClose = cachedNiftyClose;
        String  dispState      = cachedState;

        // Live-LTP fallback for the UI only. After a restart the sticky cache stays NEUTRAL
        // until the next NIFTY 5-min boundary fires — that's a 0-5 min window where the card
        // would otherwise show nothing. Fill in the CPR factor from live LTP so the user sees
        // an immediate read. Sticky cache and getStickyState() (used by filters/exits) are
        // untouched — they keep updating only at 5-min closes to avoid intra-bar oscillation.
        if (dispCpr == null) {
            boolean dispInsideCpr = false;
            double niftyLtp = marketDataService.getLtp(NIFTY_SYMBOL);
            var cpr = bhavcopyService.getCprLevels("NIFTY50");
            if (niftyLtp > 0 && cpr != null && cpr.getTc() > 0 && cpr.getBc() > 0) {
                double top = Math.max(cpr.getTc(), cpr.getBc());
                double bot = Math.min(cpr.getTc(), cpr.getBc());
                if (niftyLtp > top)      dispCpr = Boolean.TRUE;
                else if (niftyLtp < bot) dispCpr = Boolean.FALSE;
                else                     dispInsideCpr = true;
                dispNiftyClose = niftyLtp;
            }
            dispState = deriveCprOnlyState(dispCpr, dispInsideCpr);
        }

        trend.setCprBullish(dispCpr);
        trend.setNiftyClose(dispNiftyClose);
        trend.setState(dispState);

        // CPR state — sourced from the adaptive classifier (3-state model based on the
        // 14-day SMA baselines for CPR width and True Range). Replaces the legacy static
        // NARROW/WIDE band. The card surfaces the state name (SQUEEZE/STANDARD/EXHAUSTION/WARMUP)
        // via setCprWidthCategory, and the raw widthPct via setCprWidthPct.
        var niftyCpr = bhavcopyService.getCprLevels("NIFTY50");
        if (niftyCpr != null && niftyCpr.getCprWidthPct() > 0) {
            trend.setCprWidthPct(Math.round(niftyCpr.getCprWidthPct() * 1000.0) / 1000.0);
        }
        BhavcopyService.AdaptiveCprResult adaptive = bhavcopyService.getAdaptiveCpr("NIFTY50");
        String category = switch (adaptive.state()) {
            case NARROW            -> "NARROW";
            case AVERAGE           -> "AVERAGE";
            case WIDE              -> "WIDE";
            case INSUFFICIENT_DATA -> "WARMUP";
        };
        trend.setCprWidthCategory(category);

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
        // 2-factor state matches the NIFTY sticky-state computation. Reference price is
        // the last completed 5-min close (stepped, stable between boundaries); when no
        // 5-min bar has finalized yet (pre-market), fall back to LTP → bhavcopy prev close.
        double refClose = 0;
        if (candleAggregator != null) {
            CandleAggregator.CandleBar bar = lastAvailableBar(fyersSym);
            if (bar != null && bar.close > 0) refClose = bar.close;
        }
        if (refClose <= 0) {
            double ltp = marketDataService.getLtp(fyersSym);
            refClose = ltp > 0 ? ltp : cpr.getClose();
        }
        if (refClose <= 0) return "NEUTRAL";
        if (cpr.getTc() <= 0 || cpr.getBc() <= 0) return "NEUTRAL";

        double top = Math.max(cpr.getTc(), cpr.getBc());
        double bot = Math.min(cpr.getTc(), cpr.getBc());
        double ema20 = emaService != null ? emaService.getSteppedEma(fyersSym) : 0;
        return deriveTrendState(refClose, top, bot, ema20);
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
        String primaryIndex = bhavcopyService.getPrimaryIndexTicker(ticker);
        if (primaryIndex == null || primaryIndex.isEmpty()) return "NEUTRAL";
        return getTrendStateForTicker(primaryIndex);
    }

    /**
     * Canonical accessor for the trend state of any index ticker. Returns the sticky
     * cached state for NIFTY 50 (updated only on its 5-min candle close) and on-demand
     * computed state for any sector index ticker. Consumed by the primary-index
     * alignment filter so the call site doesn't have to branch on NIFTY vs sector.
     */
    public String getTrendStateForTicker(String ticker) {
        if (ticker == null) return "NEUTRAL";
        if ("NIFTY50".equals(ticker)) return getStickyState();
        return getSectorTrendForTicker(ticker);
    }

    /**
     * HTF (1-hour) trend state for any index ticker. Same strict 2-factor rule the
     * stock card uses: state = BULLISH iff (1h close > weekly CPR top AND > 1h EMA20),
     * BEARISH iff both below, SIDEWAYS iff factors disagree, INSIDE inside CPR zone,
     * NEUTRAL if any factor is missing. Used by the Index HTF Alignment filter and by
     * the index mini-card HTF chip.
     */
    public String getHtfTrendStateForTicker(String ticker) {
        if (ticker == null || ticker.isEmpty()) return "NEUTRAL";
        if (weeklyCprService == null || htfEmaService == null || candleAggregator == null) return "NEUTRAL";
        String fyersSym = "NSE:" + ticker + "-INDEX";
        WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(fyersSym);
        if (wl == null || wl.top <= 0 || wl.bot <= 0) return "NEUTRAL";
        Double htfClose = candleAggregator.getLast1HourClose(fyersSym);
        if (htfClose == null || htfClose <= 0) return "NEUTRAL";
        double htfEma = htfEmaService.getEma(fyersSym);
        return deriveTrendState(htfClose, wl.top, wl.bot, htfEma);
    }
}
