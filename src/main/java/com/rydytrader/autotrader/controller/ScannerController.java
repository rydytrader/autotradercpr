package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.manager.PositionManager;
import com.rydytrader.autotrader.service.*;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * REST endpoints for the scanner dashboard.
 */
@RestController
public class ScannerController {

    private static final Logger log = LoggerFactory.getLogger(ScannerController.class);

    private final MarketDataService marketDataService;
    private final BhavcopyService bhavcopyService;
    private final AtrService atrService;
    private final WeeklyCprService weeklyCprService;
    private final CandleAggregator candleAggregator;
    private final BreakoutScanner breakoutScanner;
    private final RiskSettingsStore riskSettings;
    private final MarginDataService marginDataService;
    private final TradeHistoryService tradeHistoryService;
    private final EmaService emaService;
    private final IndexTrendService indexTrendService;
    private final MarketHolidayService marketHolidayService;
    @org.springframework.beans.factory.annotation.Autowired
    private SymbolMasterService symbolMasterService;
    @org.springframework.beans.factory.annotation.Autowired
    @org.springframework.context.annotation.Lazy
    private HtfEmaService htfEmaService;

    public ScannerController(MarketDataService marketDataService,
                             BhavcopyService bhavcopyService,
                             AtrService atrService,
                             WeeklyCprService weeklyCprService,
                             CandleAggregator candleAggregator,
                             BreakoutScanner breakoutScanner,
                             RiskSettingsStore riskSettings,
                             MarginDataService marginDataService,
                             TradeHistoryService tradeHistoryService,
                             EmaService emaService,
                             IndexTrendService indexTrendService,
                             MarketHolidayService marketHolidayService) {
        this.marketDataService = marketDataService;
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.weeklyCprService = weeklyCprService;
        this.candleAggregator = candleAggregator;
        this.breakoutScanner = breakoutScanner;
        this.riskSettings = riskSettings;
        this.marginDataService = marginDataService;
        this.tradeHistoryService = tradeHistoryService;
        this.emaService = emaService;
        this.indexTrendService = indexTrendService;
        this.marketHolidayService = marketHolidayService;
    }

    /**
     * Live snapshot of the NSE sectoral indices we track CPR for. Used by the scrolling
     * market ticker to show sector LTP + change% + CPR bias chip. Empty list if cookies
     * / bhavcopy haven't loaded yet.
     */
    /**
     * Counts how many distinct stocks in the current watchlist map to each sectoral index
     * ticker (e.g. {@code NIFTYBANK → 5}). Mirrors the {@link #getWatchlist} universe +
     * filter gates so the Sector Trends modal only shows indices the user is actually
     * scanning against, with the per-sector stock count.
     */
    private Map<String, Integer> watchlistSectorIndexCounts() {
        Map<String, Set<String>> symbolsByTicker = new LinkedHashMap<>();
        double insideMaxWidth = riskSettings.getInsideCprMaxWidth();

        java.util.function.Consumer<CprLevels> recordSector = (cpr) -> {
            // Each stock maps to its primary index (NIFTY 50 OR a sector index) via the
            // Stock Universe master table. Count stocks per index for the Indices panel.
            String ticker = bhavcopyService.getPrimaryIndexTicker(cpr.getSymbol());
            if (ticker == null || ticker.isEmpty()) return;
            // Dedupe by stock symbol — a stock that hits both narrow and inside passes
            // mustn't be double-counted toward its index.
            symbolsByTicker.computeIfAbsent(ticker, k -> new HashSet<>()).add(cpr.getSymbol());
        };

        // Adaptive state pass — same gates as getWatchlist.
        for (CprLevels cpr : bhavcopyService.getAllCprLevels().values()) {
            if (bhavcopyService.isIndex(cpr.getSymbol())) continue;
            if (!bhavcopyService.isInScanUniverse(cpr.getSymbol())) continue;
            if (!isAdaptiveStateEnabled(bhavcopyService.getAdaptiveCpr(cpr.getSymbol()).state())) continue;
            if (!marketDataService.passesWatchlistFilters(cpr)) continue;
            recordSector.accept(cpr);
        }
        // Inside CPR pass — same gates as getWatchlist (no narrow-width upper cap).
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            if (!bhavcopyService.isInScanUniverse(cpr.getSymbol())) continue;
            if (insideMaxWidth > 0 && cpr.getCprWidthPct() > insideMaxWidth) continue;
            if (!marketDataService.passesWatchlistFilters(cpr)) continue;
            recordSector.accept(cpr);
        }
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> e : symbolsByTicker.entrySet()) {
            counts.put(e.getKey(), e.getValue().size());
        }
        return counts;
    }

    /** True when the user has the corresponding state toggle enabled in Settings.
     *  INSUFFICIENT_DATA is always excluded. */
    private boolean isAdaptiveStateEnabled(BhavcopyService.CprState state) {
        if (state == null) return false;
        return switch (state) {
            case NARROW            -> riskSettings.isEnableCprStateA();
            case AVERAGE           -> riskSettings.isEnableCprStateB();
            case WIDE              -> riskSettings.isEnableCprStateC();
            case INSUFFICIENT_DATA -> false;
        };
    }

    /** Short uppercase tag for the {@code cprTypes} list / card badge. */
    private static String adaptiveStateTag(BhavcopyService.CprState state) {
        if (state == null) return "UNKNOWN";
        return switch (state) {
            case NARROW            -> "NARROW";
            case AVERAGE           -> "AVERAGE";
            case WIDE              -> "WIDE";
            case INSUFFICIENT_DATA -> "WARMUP";
        };
    }

    @GetMapping("/api/scanner/sectors")
    public List<Map<String, Object>> getSectoralIndices() {
        // Only surface indices whose sector has at least one stock in the current
        // watchlist — the rest aren't relevant to what the user is scanning.
        Map<String, Integer> watchlistCounts = watchlistSectorIndexCounts();
        List<Map<String, Object>> out = new ArrayList<>();
        for (String ticker : bhavcopyService.getAllSectoralIndexTickers()) {
            if (!watchlistCounts.containsKey(ticker)) continue;
            CprLevels idx = bhavcopyService.getCprLevels(ticker);
            if (idx == null) continue;
            String fyersSym = "NSE:" + ticker + "-INDEX";
            double ltp = marketDataService.getLtp(fyersSym);
            double prevClose = idx.getClose();
            double changePct, changePts;
            // Live-compute branch only on actual trading days. Stale Saturday mock-session ticks
            // can leave currentTicks with ltp = Friday's close (because the mock didn't move
            // the index), which would make `ltp - prevClose = 0` and produce a fake "0% change".
            // On non-trading days, always go through the cached-tick / bhavcopy fallback.
            boolean isTradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();
            if (isTradingDay && ltp > 0 && prevClose > 0) {
                changePct = ((ltp - prevClose) / prevClose) * 100.0;
                changePts = ltp - prevClose;
            } else {
                // Weekend / holiday / fresh restart: try the cached tick first (preserves
                // Friday's session change when the bot was running through Friday's close).
                changePct = marketDataService.getChangePercent(fyersSym);
                changePts = marketDataService.getChange(fyersSym);
                // If no cached tick (bot restarted on a non-trading day), fall back to
                // bhavcopy snapshots: latest close (Friday) minus the prior snapshot's
                // close (Thursday) = Friday's actual session change.
                if (changePct == 0 && changePts == 0 && prevClose > 0) {
                    CprLevels priorDay = bhavcopyService.getPreviousCpr(ticker);
                    if (priorDay != null && priorDay.getClose() > 0) {
                        changePts = prevClose - priorDay.getClose();
                        changePct = (changePts / priorDay.getClose()) * 100.0;
                    }
                }
            }
            double top = idx.getTc() > 0 && idx.getBc() > 0 ? Math.max(idx.getTc(), idx.getBc()) : 0;
            double bot = idx.getTc() > 0 && idx.getBc() > 0 ? Math.min(idx.getTc(), idx.getBc()) : 0;
            double refLtp = ltp > 0 ? ltp : prevClose;
            double ema20 = emaService.getEma(fyersSym);

            // Two-factor trend: LTP vs daily CPR (top/bot zone) AND LTP vs 5-min EMA20.
            // Mirrors NIFTY's state machine but with only two factors (no futures VWAP).
            Boolean cprBullish = null;
            if (refLtp > 0 && top > 0 && bot > 0) {
                if (refLtp > top)      cprBullish = Boolean.TRUE;
                else if (refLtp < bot) cprBullish = Boolean.FALSE;
            }
            Boolean emaBullish = null;
            if (refLtp > 0 && ema20 > 0) {
                if (refLtp > ema20)      emaBullish = Boolean.TRUE;
                else if (refLtp < ema20) emaBullish = Boolean.FALSE;
            }
            String state;
            if (refLtp <= 0) {
                state = "NEUTRAL";
            } else if (cprBullish == null) {
                state = "INSIDE";
            } else if (Boolean.TRUE.equals(cprBullish) && !Boolean.FALSE.equals(emaBullish)) {
                state = "BULLISH";
            } else if (Boolean.FALSE.equals(cprBullish) && !Boolean.TRUE.equals(emaBullish)) {
                state = "BEARISH";
            } else if (Boolean.FALSE.equals(cprBullish) && Boolean.TRUE.equals(emaBullish)) {
                state = "BULLISH_REVERSAL";  // below CPR but reclaimed EMA20
            } else if (Boolean.TRUE.equals(cprBullish) && Boolean.FALSE.equals(emaBullish)) {
                state = "BEARISH_REVERSAL";  // above CPR but lost EMA20
            } else {
                state = "SIDEWAYS";
            }

            String displayName = bhavcopyService.getIndexDisplayName(ticker);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ticker",      ticker);
            m.put("symbol",      fyersSym);
            m.put("displayName", displayName != null ? displayName : ticker);
            m.put("ltp",         Math.round((ltp > 0 ? ltp : prevClose) * 100.0) / 100.0);
            m.put("prevClose",   Math.round(prevClose * 100.0) / 100.0);
            m.put("change",      Math.round(changePts * 100.0) / 100.0);
            m.put("changePct",   Math.round(changePct * 100.0) / 100.0);
            m.put("cprTop",      Math.round(top * 100.0) / 100.0);
            m.put("cprBot",      Math.round(bot * 100.0) / 100.0);
            m.put("ema20",       Math.round(ema20 * 100.0) / 100.0);
            m.put("state",       state);
            m.put("stockCount",  watchlistCounts.get(ticker));
            out.add(m);
        }
        return out;
    }

    @GetMapping("/api/scanner/watchlist")
    public List<Map<String, Object>> getWatchlist() {
        List<Map<String, Object>> result = new ArrayList<>();
        Set<String> positionSymbols = PositionManager.getAllSymbols();

        // Build set of inside CPR symbols for cross-referencing
        Set<String> insideSymbols = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            insideSymbols.add(cpr.getSymbol());
        }

        // Universe gate — the DB-backed Stock Universe (Settings → Stock Universe) is the
        // single source of truth. isInScanUniverse() returns true for stocks with
        // enabled=true in the stocks table, false otherwise.

        // Collect stocks by adaptive CPR state — toggle-gated per state (A / B / C).
        // INSUFFICIENT_DATA is silently excluded (fewer than 14 days of history).
        Set<String> seen = new HashSet<>();
        for (CprLevels cpr : bhavcopyService.getAllCprLevels().values()) {
            if (bhavcopyService.isIndex(cpr.getSymbol())) continue; // NIFTY50/NIFTYBANK etc.
            if (!bhavcopyService.isInScanUniverse(cpr.getSymbol())) continue;
            BhavcopyService.AdaptiveCprResult adaptive = bhavcopyService.getAdaptiveCpr(cpr.getSymbol());
            if (!isAdaptiveStateEnabled(adaptive.state())) continue;
            if (!marketDataService.passesWatchlistFilters(cpr)) continue;

            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            List<String> types = new ArrayList<>();
            types.add(adaptiveStateTag(adaptive.state()));
            if (insideSymbols.contains(cpr.getSymbol())) types.add("INSIDE");
            Map<String, Object> card = buildCard(fyers, cpr, adaptiveStateTag(adaptive.state()), positionSymbols);
            card.put("cprTypes", types);
            // Surface the adaptive numbers so the chip tooltip can display the underlying ratios.
            Map<String, Object> adaptivePayload = new LinkedHashMap<>();
            adaptivePayload.put("state",         adaptive.state().name());
            adaptivePayload.put("stateTag",      adaptiveStateTag(adaptive.state()));
            adaptivePayload.put("widthPct",      Math.round(adaptive.todayWidthPct()  * 1000.0) / 1000.0);
            adaptivePayload.put("avgWidthPct",   Math.round(adaptive.avgWidthPct()    * 1000.0) / 1000.0);
            adaptivePayload.put("widthRatio",    Math.round(adaptive.widthRatio()     * 1000.0) / 1000.0);
            adaptivePayload.put("cprNarrow",     adaptive.cprNarrow());
            adaptivePayload.put("samplesUsed",   adaptive.samplesUsed());
            card.put("adaptive", adaptivePayload);
            result.add(card);
            seen.add(fyers);
        }

        // Collect inside-only CPR stocks (geometric INSIDE — today's CPR within yesterday's).
        // The adaptive state toggle (NARROW/AVERAGE/WIDE) still gates these: if the user
        // disables WIDE and a stock is INSIDE-AND-WIDE, the card is excluded — disabled
        // means disabled, regardless of the orthogonal INSIDE classifier. Width filter +
        // price filters still apply. INSUFFICIENT_DATA (warmup) stocks pass through since
        // the toggle doesn't reach them — they show as INSIDE-only.
        double insideMaxWidth = riskSettings.getInsideCprMaxWidth();
        for (CprLevels cpr : bhavcopyService.getInsideCprStocks()) {
            String fyers = "NSE:" + cpr.getSymbol() + "-EQ";
            if (seen.contains(fyers)) continue;
            if (!bhavcopyService.isInScanUniverse(cpr.getSymbol())) continue;
            if (insideMaxWidth > 0 && cpr.getCprWidthPct() > insideMaxWidth) continue;
            if (!marketDataService.passesWatchlistFilters(cpr)) continue;

            BhavcopyService.AdaptiveCprResult adaptive = bhavcopyService.getAdaptiveCpr(cpr.getSymbol());
            // Apply the adaptive-state toggle gate. Warmup stocks (no classified state)
            // are allowed through — they show as INSIDE-only and aren't reached by the toggle.
            if (adaptive.state() != BhavcopyService.CprState.INSUFFICIENT_DATA
                    && !isAdaptiveStateEnabled(adaptive.state())) continue;
            List<String> types = new ArrayList<>();
            types.add("INSIDE");
            // Tag the adaptive state alongside INSIDE if it isn't WARMUP — gives the user
            // both classifications on one card.
            if (adaptive.state() != BhavcopyService.CprState.INSUFFICIENT_DATA) {
                types.add(adaptiveStateTag(adaptive.state()));
            }
            Map<String, Object> card = buildCard(fyers, cpr, "INSIDE", positionSymbols);
            card.put("cprTypes", types);
            Map<String, Object> adaptivePayload = new LinkedHashMap<>();
            adaptivePayload.put("state",         adaptive.state().name());
            adaptivePayload.put("stateTag",      adaptiveStateTag(adaptive.state()));
            adaptivePayload.put("widthPct",      Math.round(adaptive.todayWidthPct()  * 1000.0) / 1000.0);
            adaptivePayload.put("avgWidthPct",   Math.round(adaptive.avgWidthPct()    * 1000.0) / 1000.0);
            adaptivePayload.put("widthRatio",    Math.round(adaptive.widthRatio()     * 1000.0) / 1000.0);
            adaptivePayload.put("cprNarrow",     adaptive.cprNarrow());
            adaptivePayload.put("samplesUsed",   adaptive.samplesUsed());
            card.put("adaptive", adaptivePayload);
            result.add(card);
            seen.add(fyers);
        }

        // Default sort alphabetically by symbol for stable scanner ordering
        result.sort((a, b) -> String.valueOf(a.get("symbol")).compareTo(String.valueOf(b.get("symbol"))));
        return result;
    }

    private Map<String, Object> buildCard(String fyersSymbol, CprLevels levels, String cprType, Set<String> positionSymbols) {
        Map<String, Object> card = new LinkedHashMap<>();
        card.put("symbol", fyersSymbol);
        card.put("shortName", levels.getSymbol());
        card.put("cprType", cprType);
        // Universe membership flags drive the scanner-page client-side N50 vs All filter.
        card.put("inNifty50", levels.isInNifty50());
        card.put("inNifty100", levels.isInNifty100());
        // Primary index — sourced from the Settings → Stock Universe master table. Each
        // stock maps to one index (NIFTY 50 OR a sector index) that drives all three
        // alignment / hurdle filters. Card JSON exposes primary-index fields directly
        // (legacy "sector"-prefixed fields removed — frontend uses primaryIndex* now).
        String primaryIndexName = bhavcopyService.getPrimaryIndexName(levels.getSymbol());
        String primaryIndexTicker = bhavcopyService.getPrimaryIndexTicker(levels.getSymbol());
        card.put("primaryIndexName", primaryIndexName != null ? primaryIndexName : "");
        card.put("primaryIndexTicker", primaryIndexTicker != null ? primaryIndexTicker : "");

        // Primary-index trend state — 2-factor classification matching the Sector Trends
        // modal (LTP vs daily CPR + LTP vs 5-min EMA20). Yields BULLISH / BEARISH /
        // BULLISH_REVERSAL / BEARISH_REVERSAL / INSIDE / SIDEWAYS / NEUTRAL — same state the
        // alignment filter uses. Falls back to bhavcopy snapshot close on weekends.
        String sectorIndexTicker = primaryIndexTicker;
        String sectorState = "";
        double sectorChangePct = 0;
        double sectorTop = 0, sectorBot = 0, sectorEma20 = 0;
        if (sectorIndexTicker != null) {
            CprLevels idx = bhavcopyService.getCprLevels(sectorIndexTicker);
            if (idx != null) {
                String sectorFyersSym = "NSE:" + sectorIndexTicker + "-INDEX";
                double prevClose = idx.getClose();
                double idxLtp = marketDataService.getLtp(sectorFyersSym);
                boolean tradingDay = marketHolidayService == null || marketHolidayService.isTradingDay();

                // Reference price: live LTP on a trading day, prevClose otherwise.
                double refPrice = (tradingDay && idxLtp > 0) ? idxLtp : prevClose;

                // Change% for tooltip — same logic as the modal endpoint.
                if (tradingDay && idxLtp > 0 && prevClose > 0) {
                    sectorChangePct = (idxLtp - prevClose) / prevClose * 100.0;
                } else if (prevClose > 0) {
                    CprLevels priorDay = bhavcopyService.getPreviousCpr(sectorIndexTicker);
                    if (priorDay != null && priorDay.getClose() > 0) {
                        sectorChangePct = (prevClose - priorDay.getClose()) / priorDay.getClose() * 100.0;
                    }
                }

                // CPR zone (used for the bias) + EMA20 (kept for the chip tooltip's EMA row).
                sectorTop   = (idx.getTc() > 0 && idx.getBc() > 0) ? Math.max(idx.getTc(), idx.getBc()) : 0;
                sectorBot   = (idx.getTc() > 0 && idx.getBc() > 0) ? Math.min(idx.getTc(), idx.getBc()) : 0;
                sectorEma20 = emaService != null ? emaService.getEma(sectorFyersSym) : 0;

                // CPR-only state: above CPR top → BULLISH, below CPR bot → BEARISH, inside → INSIDE.
                // Matches the NIFTY card + sector trends modal (EMA20 is no longer a trend factor
                // for indices — kept on the tooltip as a separate data point only).
                if (refPrice > 0 && sectorTop > 0 && sectorBot > 0) {
                    if (refPrice > sectorTop)      sectorState = "BULLISH";
                    else if (refPrice < sectorBot) sectorState = "BEARISH";
                    else                           sectorState = "INSIDE";
                } else if (refPrice > 0) {
                    sectorState = "NEUTRAL";
                }
            }
        }
        card.put("primaryIndexState",     sectorState);
        card.put("primaryIndexChangePct", Math.round(sectorChangePct * 100.0) / 100.0);
        card.put("primaryIndexCprTop",    Math.round(sectorTop   * 100.0) / 100.0);
        card.put("primaryIndexCprBot",    Math.round(sectorBot   * 100.0) / 100.0);
        card.put("primaryIndexEma20",     Math.round(sectorEma20 * 100.0) / 100.0);
        // Primary-index Fyers symbol — used to make the chip clickable (opens the chart
        // modal for the index, parallel to the chart button on each card).
        if (sectorIndexTicker != null) {
            card.put("primaryIndexSymbol", "NSE:" + sectorIndexTicker + "-INDEX");
        }

        // LTP separated into two values:
        //   liveTickLtp — the LTP from today's WS ticks (0 if none, e.g. pre-market new day).
        //                 Both source maps already apply a stale-day guard so yesterday's
        //                 cached LTP doesn't bleed in.
        //   ltp         — internal value used downstream (probability, OR status). Falls back
        //                 to bhavcopy prev close so probability logic has a non-zero reference.
        // The card displays liveTickLtp directly: pre-market on a new day → "0.00", once
        // ticks start arriving → today's actual price.
        double liveTickLtp = candleAggregator.getLtp(fyersSymbol);
        if (liveTickLtp <= 0) liveTickLtp = marketDataService.getLtp(fyersSymbol);
        double ltp = liveTickLtp > 0 ? liveTickLtp : levels.getClose();
        double changePct = candleAggregator.getChangePct(fyersSymbol);
        if (changePct == 0) changePct = marketDataService.getChangePercent(fyersSymbol);
        card.put("ltp", Math.round(liveTickLtp * 100.0) / 100.0);
        card.put("changePercent", Math.round(changePct * 100.0) / 100.0);

        // Current candle OHLC
        CandleAggregator.CandleBar currentCandle = candleAggregator.getCurrentCandle(fyersSymbol);
        if (currentCandle != null) {
            card.put("candleOpen", r(currentCandle.open));
            card.put("candleHigh", r(currentCandle.high));
            card.put("candleLow", r(currentCandle.low));
        }

        card.put("atr", Math.round(atrService.getAtr(fyersSymbol) * 100.0) / 100.0);
        // Today's True Range (gap-inclusive) + 14-day daily ATR — drive the "Today's TR"
        // chip on the card. Color is computed client-side against the live exhaustion
        // threshold from /api/scanner/status.
        double dayHigh = candleAggregator.getDayHigh(fyersSymbol);
        double dayLow  = candleAggregator.getDayLow(fyersSymbol);
        double prevCloseToday = levels.getClose();
        double todayTr = 0;
        if (dayHigh > 0 && dayLow > 0 && prevCloseToday > 0) {
            todayTr = Math.max(dayHigh - dayLow,
                Math.max(Math.abs(dayHigh - prevCloseToday), Math.abs(dayLow - prevCloseToday)));
        }
        card.put("todayTr",  Math.round(todayTr * 100.0) / 100.0);
        card.put("dailyAtr", Math.round(bhavcopyService.getDailyAtr(levels.getSymbol()) * 100.0) / 100.0);
        // Open Range — high/low of bars in the first {@code openRangeMinutes} after 9:15 IST.
        // Card surfaces these even when the filter toggle is off (informational chip).
        int orMins = riskSettings.getOpenRangeMinutes();
        double orHigh = candleAggregator.getOpenRangeHigh(fyersSymbol, orMins);
        double orLow  = candleAggregator.getOpenRangeLow(fyersSymbol, orMins);
        long orEndMinute = MarketHolidayService.MARKET_OPEN_MINUTE + orMins;
        CandleAggregator.CandleBar orRefBar = candleAggregator.getLastCompletedCandle(fyersSymbol);
        // OR is finalized once a bar has CLOSED at or past orEndMinute (the last OR bar
        // closes exactly at orEndMinute). Use the latest bar's close minute, not its start.
        long latestCloseMinute = orRefBar != null
            ? orRefBar.startMinute + riskSettings.getScannerTimeframe()
            : 0;
        double orRefClose = orRefBar != null ? orRefBar.close : 0;
        String orTrend;
        if (orHigh <= 0 || orLow <= 0 || latestCloseMinute < orEndMinute) orTrend = "FORMING";
        else if (orRefClose > orHigh)                                     orTrend = "BULLISH";
        else if (orRefClose < orLow)                                      orTrend = "BEARISH";
        else                                                               orTrend = "INSIDE";
        card.put("orHigh", r(orHigh));
        card.put("orLow",  r(orLow));
        card.put("orTrend", orTrend);
        card.put("orMinutes", orMins);
        // EMAs rounded to 2 decimals only — TV does not snap EMA values to tick size, so
        // tick-rounding here would cause a small visible mismatch on stocks with non-0.01 ticks.
        card.put("ema20",  Math.round(emaService.getEma(fyersSymbol)    * 100.0) / 100.0);
        // EMA-trend reference price: last completed candle close, LTP fallback for the
        // first candle of the day. Mirrors how WeeklyCprService computes daily/weekly trend.
        card.put("price5m",  Math.round(weeklyCprService.getDailyPrice(fyersSymbol)  * 100.0) / 100.0);
        // "Day Open" on stock card = the open print (close of first 5-min candle), NOT the
        // 9:15 auction open. Open print is the price the day "settles" into and drives the
        // IV/OV/EV classification, so it's the more meaningful reference for traders.
        // Falls back to actual day open before 9:20 (firstCandleClose not yet available).
        double openPrint = candleAggregator.getFirstCandleClose(fyersSymbol);
        if (openPrint <= 0) openPrint = candleAggregator.getDayOpen(fyersSymbol);
        card.put("dayOpen", Math.round(openPrint * 100.0) / 100.0);

        // Open classification: IV (Inside Value), OV (Outside Value), EV (Extended Value)
        double firstClose = candleAggregator.getFirstCandleClose(fyersSymbol);
        String openClass = null;
        if (firstClose > 0) {
            double r1 = levels.getR1(), r2 = levels.getR2();
            double s1 = levels.getS1(), s2 = levels.getS2();
            double pdh = levels.getPh(), pdl = levels.getPl();
            double upperBound = Math.max(r1, pdh);
            double lowerBound = Math.min(s1, pdl);
            if (firstClose >= r2 || firstClose <= s2) {
                openClass = "EV";
            } else if (firstClose > upperBound || firstClose < lowerBound) {
                openClass = "OV";
            } else {
                openClass = "IV";
            }
        }
        card.put("openClass", openClass);

        card.put("candleVolume", candleAggregator.getCurrentCandleVolume(fyersSymbol));
        // Last completed 5-min candle volume + its 20-bar rolling average — surfaced on
        // the EMA 20 (5 M) chip's mouse-over so the trader can eyeball whether the most
        // recent closed bar carried real participation.
        CandleAggregator.CandleBar lastClosed5m = candleAggregator.getLastCompletedCandle(fyersSymbol);
        card.put("lastCandleVolume", lastClosed5m != null ? lastClosed5m.volume : 0L);
        card.put("avgVolume20",      Math.round(candleAggregator.getAvgVolume(fyersSymbol, 20)));
        card.put("weeklyTrend", weeklyCprService.getWeeklyTrend(fyersSymbol));
        card.put("dailyTrend", weeklyCprService.getDailyTrend(fyersSymbol));
        card.put("probability", computeCardProbability(fyersSymbol, ltp));

        // CPR levels
        Map<String, Object> lvls = new LinkedHashMap<>();
        lvls.put("r4", r(levels.getR4())); lvls.put("r3", r(levels.getR3()));
        lvls.put("r2", r(levels.getR2())); lvls.put("r1", r(levels.getR1()));
        lvls.put("ph", r(levels.getPh())); lvls.put("pivot", r(levels.getPivot()));
        lvls.put("tc", r(levels.getTc())); lvls.put("bc", r(levels.getBc()));
        lvls.put("s1", r(levels.getS1())); lvls.put("pl", r(levels.getPl()));
        lvls.put("s2", r(levels.getS2())); lvls.put("s3", r(levels.getS3()));
        lvls.put("s4", r(levels.getS4()));
        card.put("levels", lvls);

        // CPR bias + 2-factor trend state — mirrors NIFTY card's deriveState logic.
        //   CPR bullish (close > max(TC,BC)) AND EMA bullish (close > EMA20)  → BULLISH
        //   CPR bearish (close < min(TC,BC)) AND EMA bearish (close < EMA20)  → BEARISH
        //   CPR bearish AND EMA bullish                                       → BULLISH_REVERSAL
        //   CPR bullish AND EMA bearish                                       → BEARISH_REVERSAL
        //   close inside CPR zone                                             → INSIDE
        //   no close data                                                     → NEUTRAL
        // Computed off the last completed 5-min candle close (or previous trading-day
        // close as fallback when no candle is cached), so the chip only flips at 5-min
        // boundaries — not on every tick.
        String cprBias = "INSIDE";
        double cprTop = Math.max(levels.getTc(), levels.getBc());
        double cprBotBias = Math.min(levels.getTc(), levels.getBc());
        CandleAggregator.CandleBar lastBar = candleAggregator.getLastCompletedCandle(fyersSymbol);
        double biasClose = lastBar != null ? lastBar.close : levels.getClose();
        double ema20Val = emaService.getEma(fyersSymbol);
        if (biasClose > 0 && cprTop > 0 && cprBotBias > 0) {
            if (biasClose > cprTop)          cprBias = "BULLISH";
            else if (biasClose < cprBotBias) cprBias = "BEARISH";
        }
        // Trend state — centralized 2-factor strict logic in IndexTrendService.deriveTrendState
        // (BULLISH iff close > CPR top AND close > EMA20; BEARISH iff both below; mismatched
        // factors → SIDEWAYS). No REVERSAL states.
        String trendState = IndexTrendService.deriveTrendState(biasClose, cprTop, cprBotBias, ema20Val);
        card.put("cprBias", cprBias);
        card.put("trendState", trendState);

        // HTF (1-hour) versions of the same three fields — driven by the stock's 1-hour close
        // vs weekly CPR (TC/BC) + 1-hour EMA20. Feeds the second CPR/EMA/TREND row on the
        // scanner card and matches the inputs of the Stock HTF Trend Alignment filter.
        String htfCprBias = "INSIDE";
        String htfTrendState = "NEUTRAL";
        double htfEma20Val = htfEmaService != null ? htfEmaService.getEma(fyersSymbol) : 0;
        WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(fyersSymbol);
        Double htfCloseVal = candleAggregator.getLast1HourClose(fyersSymbol);
        if (htfCloseVal != null && htfCloseVal > 0 && wl != null && wl.top > 0 && wl.bot > 0) {
            if (htfCloseVal > wl.top)      htfCprBias = "BULLISH";
            else if (htfCloseVal < wl.bot) htfCprBias = "BEARISH";
            htfTrendState = IndexTrendService.deriveTrendState(htfCloseVal, wl.top, wl.bot, htfEma20Val);
        }
        card.put("htfCprBias", htfCprBias);
        card.put("htfTrendState", htfTrendState);
        card.put("htfEma20", r(htfEma20Val));
        card.put("htfClose", htfCloseVal != null ? r(htfCloseVal) : 0);

        // Weekly CPR levels for the CPR Levels panel's "Weekly" sub-section. Exposes the same
        // shape as the daily levels map so the template can iterate using a shared renderer.
        if (wl != null) {
            Map<String, Object> wlMap = new LinkedHashMap<>();
            wlMap.put("r4",    r(wl.r4));
            wlMap.put("r3",    r(wl.r3));
            wlMap.put("r2",    r(wl.r2));
            wlMap.put("r1",    r(wl.r1));
            wlMap.put("ph",    r(wl.ph));
            wlMap.put("tc",    r(wl.tc));
            wlMap.put("pivot", r(wl.pivot));
            wlMap.put("bc",    r(wl.bc));
            wlMap.put("pl",    r(wl.pl));
            wlMap.put("s1",    r(wl.s1));
            wlMap.put("s2",    r(wl.s2));
            wlMap.put("s3",    r(wl.s3));
            wlMap.put("s4",    r(wl.s4));
            card.put("weeklyLevels", wlMap);
        }

        // Per-stock hurdle chip — single nearest hurdle from the three sources
        // (stock HTF, primary-index HTF, primary-index 5m).
        //   • Bullish flavours → buy-side levels above LTP.
        //   • Bearish flavours → sell-side levels below LTP.
        //   • INSIDE → no clear direction; compute both buy and sell candidates and surface
        //     the geographically nearer one (whichever side the stock is closer to).
        //   • SIDEWAYS / NEUTRAL → leave hurdle = null (no informative side to show).
        BreakoutScanner.HurdleStatus hurdleStatus = null;
        if ("BULLISH".equals(trendState) || "BULLISH_REVERSAL".equals(trendState)) {
            hurdleStatus = breakoutScanner.getStockNearestHurdle(fyersSymbol, true);
        } else if ("BEARISH".equals(trendState) || "BEARISH_REVERSAL".equals(trendState)) {
            hurdleStatus = breakoutScanner.getStockNearestHurdle(fyersSymbol, false);
        } else if ("INSIDE".equals(trendState)) {
            BreakoutScanner.HurdleStatus buy  = breakoutScanner.getStockNearestHurdle(fyersSymbol, true);
            BreakoutScanner.HurdleStatus sell = breakoutScanner.getStockNearestHurdle(fyersSymbol, false);
            if (buy == null) hurdleStatus = sell;
            else if (sell == null) hurdleStatus = buy;
            else hurdleStatus = (buy.distance() <= sell.distance()) ? buy : sell;
        }
        if (hurdleStatus != null) {
            Map<String, Object> hurdle = new LinkedHashMap<>();
            hurdle.put("level", hurdleStatus.level());
            hurdle.put("category", hurdleStatus.category());
            hurdle.put("state", hurdleStatus.state());
            card.put("hurdle", hurdle);
        }

        // Broken levels
        Set<String> broken = breakoutScanner.getBrokenLevels(fyersSymbol);
        card.put("brokenLevels", broken != null ? new ArrayList<>(broken) : Collections.emptyList());

        // Last signal
        BreakoutScanner.SignalInfo sig = breakoutScanner.getLastSignal(fyersSymbol);
        if (sig != null) {
            Map<String, String> sigMap = new LinkedHashMap<>();
            sigMap.put("setup", sig.setup);
            sigMap.put("time", sig.time);
            sigMap.put("status", sig.status);
            sigMap.put("detail", sig.detail != null ? sig.detail : "");
            card.put("lastSignal", sigMap);
        } else {
            card.put("lastSignal", null);
        }

        // signalHistory dropped from the card payload — was bloating /api/scanner/watchlist
        // to multi-MB late in the day. The full audit trail is still available via
        // /api/signal-trail (used by the Signal Trail page) and lastSignal above carries
        // the most recent entry for the card title.

        card.put("hasPosition", positionSymbols.contains(fyersSymbol));
        card.put("cprWidthPct", Math.round(levels.getCprWidthPct() * 1000.0) / 1000.0);

        return card;
    }

    private double r(double v) { return Math.round(v * 100.0) / 100.0; }

    /** Round derived prices (EMAs etc.) to the symbol's tick size for display.
     *  Filter math still uses the raw value via EmaService. */
    private static double roundToTick(double value, double tick) {
        if (tick <= 0) return Math.round(value * 100.0) / 100.0;
        return Math.round(value / tick) * tick;
    }

    /**
     * Resolve the current trading week as a Mon→Fri pair. On weekends this rolls
     * back to the most recent completed trading week so the chart still has data
     * to draw. Holidays inside Mon-Fri are simply absent from the bar stream —
     * we still anchor the visible window at Mon-Fri so the user sees the full
     * week layout with gaps where holidays fell.
     */
    private java.time.LocalDate[] currentTradingWeek(java.time.ZoneId ist) {
        java.time.LocalDate today = java.time.LocalDate.now(ist);
        java.time.DayOfWeek dow = today.getDayOfWeek();
        java.time.LocalDate monday;
        if (dow == java.time.DayOfWeek.SATURDAY) {
            monday = today.minusDays(5);
        } else if (dow == java.time.DayOfWeek.SUNDAY) {
            monday = today.minusDays(6);
        } else {
            monday = today.minusDays(dow.getValue() - 1);  // dow=1 (Mon) → minus 0
        }
        return new java.time.LocalDate[] { monday, monday.plusDays(4) };
    }

    /**
     * Keep only bars whose IST date falls in [from, to] inclusive. Used to clip
     * the multi-day historical fetch down to the current trading week before
     * aggregation. Bars are returned in original order (sorting is the caller's
     * concern — the fetcher already returns chronological order).
     */
    private List<CandleAggregator.CandleBar> filterToWeek(List<CandleAggregator.CandleBar> bars,
                                                          java.time.LocalDate from,
                                                          java.time.LocalDate to,
                                                          java.time.ZoneId ist) {
        List<CandleAggregator.CandleBar> out = new ArrayList<>(bars.size());
        for (CandleAggregator.CandleBar c : bars) {
            java.time.LocalDate d = java.time.Instant.ofEpochSecond(c.epochSec).atZone(ist).toLocalDate();
            if (!d.isBefore(from) && !d.isAfter(to)) out.add(c);
        }
        return out;
    }

    /**
     * Aggregates 5-min bars into 1-hour bars. NSE hours run 9:15 → 15:30 IST so the
     * natural hourly buckets start at minute :15 (9:15, 10:15, ..., 14:15) and the
     * day-close partial bar (15:15 → 15:30) becomes its own short bar. Within each
     * bucket: open = first bar open, high = max high, low = min low, close = last
     * bar close, volume = sum. Bars are assumed already chronologically sorted.
     */
    private List<CandleAggregator.CandleBar> aggregateToHourly(List<CandleAggregator.CandleBar> bars) {
        List<CandleAggregator.CandleBar> out = new ArrayList<>();
        if (bars == null || bars.isEmpty()) return out;
        java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
        CandleAggregator.CandleBar bucket = null;
        long bucketKey = -1;
        for (CandleAggregator.CandleBar c : bars) {
            java.time.ZonedDateTime zdt = java.time.Instant.ofEpochSecond(c.epochSec).atZone(ist);
            // Hourly bucket key: yyyymmdd * 100 + hour-of-bar-start. Bars whose start
            // minute is < :15 fold into the previous hour's bucket (e.g., 09:00 → 8h
            // theoretically — but NSE starts at 09:15 so this branch never runs in
            // practice). Use the "hour the bar's start belongs to" treating :15 as the
            // start-of-hour boundary.
            int year = zdt.getYear();
            int dayOfYear = zdt.getDayOfYear();
            int hour = zdt.getHour();
            int minute = zdt.getMinute();
            if (minute < 15) hour -= 1;
            long key = ((long) year * 1000L + dayOfYear) * 100L + hour;
            if (bucket == null || key != bucketKey) {
                if (bucket != null) out.add(bucket);
                bucket = new CandleAggregator.CandleBar();
                // Bucket start: minute :15 of `hour`.
                java.time.ZonedDateTime bucketStart = zdt.withMinute(15).withSecond(0).withNano(0).withHour(Math.max(0, hour));
                bucket.epochSec = bucketStart.toEpochSecond();
                bucket.startMinute = hour * 60L + 15;
                bucket.open = c.open;
                bucket.high = c.high;
                bucket.low = c.low;
                bucket.close = c.close;
                bucket.volume = c.volume;
                bucketKey = key;
            } else {
                if (c.high > bucket.high) bucket.high = c.high;
                if (c.low  < bucket.low)  bucket.low  = c.low;
                bucket.close = c.close;
                bucket.volume += c.volume;
            }
        }
        if (bucket != null) out.add(bucket);
        return out;
    }

    private Map<String, Object> barToMap(CandleAggregator.CandleBar c, boolean forming) {
        Map<String, Object> bar = new LinkedHashMap<>();
        bar.put("t", c.epochSec * 1000L);
        bar.put("o", r(c.open));
        bar.put("h", r(c.high));
        bar.put("l", r(c.low));
        bar.put("c", r(c.close));
        bar.put("v", c.volume);
        if (forming) bar.put("forming", true);
        return bar;
    }

    private Map<String, Object> point(long tMs, double value) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("t", tMs);
        p.put("v", r(value));
        return p;
    }

    private void addIndicatorPoints(CandleAggregator.CandleBar c,
                                    List<Map<String, Object>> vwapSeries,
                                    List<Map<String, Object>> ema20Series) {
        long tMs = c.epochSec * 1000L;
        if (c.vwap > 0) vwapSeries.add(point(tMs, c.vwap));
        if (c.ema20 > 0) ema20Series.add(point(tMs, c.ema20));
    }

    private void computeIndicatorsAndBuild(List<CandleAggregator.CandleBar> history,
                                           java.time.LocalDate displayDate,
                                           java.time.ZoneId ist,
                                           List<Map<String, Object>> candleList,
                                           List<Map<String, Object>> vwapSeries,
                                           List<Map<String, Object>> ema20Series) {
        // Rolling-window EMA: maintain an exponentially-weighted average over closes. Walks
        // the full merged history (prior-day warmup + today) so each plotted point has a
        // correctly-warmed-up EMA value. Period 20, alpha = 2/21.
        final int emaPeriod = 20;
        final double alpha = 2.0 / (emaPeriod + 1);
        Double ema20Val = null;
        int barsForEma = 0;
        double dayCumPV = 0, dayCumV = 0;
        java.time.LocalDate curDay = null;
        for (CandleAggregator.CandleBar c : history) {
            java.time.LocalDate d = java.time.Instant.ofEpochSecond(c.epochSec).atZone(ist).toLocalDate();
            if (!d.equals(curDay)) {
                dayCumPV = 0;
                dayCumV = 0;
                curDay = d;
            }
            double tp = (c.high + c.low + c.close) / 3.0;
            if (c.volume > 0) {
                dayCumPV += tp * c.volume;
                dayCumV += c.volume;
            }
            double vwap = dayCumV > 0 ? dayCumPV / dayCumV : c.close;
            if (c.close > 0) {
                if (ema20Val == null) ema20Val = c.close;
                else                  ema20Val = alpha * c.close + (1 - alpha) * ema20Val;
                barsForEma++;
            }
            double ema20 = barsForEma >= emaPeriod && ema20Val != null ? ema20Val : 0;
            if (d.equals(displayDate)) {
                candleList.add(barToMap(c, false));
                long tMs = c.epochSec * 1000L;
                vwapSeries.add(point(tMs, vwap));
                if (ema20  > 0) ema20Series.add(point(tMs, ema20));
            }
        }
    }

    /**
     * Compute card-level probability preview using all pre-checkable conditions (8 of 10).
     * More accurate than the basic weekly+daily check — includes EMA, VWAP, crossover, NIFTY.
     * Direction derived from daily trend (bullish → buy preview, bearish → sell preview).
     */
    private String computeCardProbability(String symbol, double ltp) {
        String daily = weeklyCprService.getDailyTrend(symbol);
        boolean isBuy = daily.contains("BULLISH");
        boolean isSell = daily.contains("BEARISH");
        if (!isBuy && !isSell) return "--"; // daily neutral — direction unknown

        // Pure CPR alignment: LTF (5-min/LTP vs daily CPR) + HTF (weekly state).
        // HPT = both aligned, MPT = only one aligned, null = LTF opposed → "--".
        // EMA / VWAP / NIFTY filters intentionally NOT applied here — those are
        // execution-time gates in the real pipeline; the card forecast reflects
        // CPR alignment only.
        String baseProb = weeklyCprService.getProbabilityForDirection(symbol, isBuy);
        return baseProb != null ? baseProb : "--";
    }

    /**
     * EOD Analysis: post-mortem audit of every breakout signal today.
     * Returns the full signalHistory (TRADED + every FILTERED rejection with structured filterName)
     * either for one symbol or aggregated across all symbols, plus summary counts and a top-blockers map.
     */
    @GetMapping("/api/signal-trail")
    public Map<String, Object> getEodAnalysis(@RequestParam(required = false) String symbol) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("asOfDate", java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Kolkata"))
            .toLocalDate().toString());

        Map<String, List<BreakoutScanner.SignalInfo>> all = breakoutScanner.getSignalHistoryAll();

        // Distinct list of symbols with at least one signal today (for the dropdown).
        List<String> symbols = new ArrayList<>(all.keySet());
        Collections.sort(symbols);
        result.put("symbols", symbols);
        result.put("selected", symbol != null && !symbol.isEmpty() ? symbol : "ALL");

        // Build the row list — either filtered to one symbol or flattened across all.
        List<Map<String, Object>> rows = new ArrayList<>();
        if (symbol != null && !symbol.isEmpty() && !"ALL".equalsIgnoreCase(symbol)) {
            for (BreakoutScanner.SignalInfo si : all.getOrDefault(symbol, Collections.emptyList())) {
                rows.add(buildEodRow(symbol, si));
            }
        } else {
            for (var entry : all.entrySet()) {
                String sym = entry.getKey();
                for (BreakoutScanner.SignalInfo si : entry.getValue()) {
                    rows.add(buildEodRow(sym, si));
                }
            }
        }
        // Sort by time ascending (HH:mm:ss strings sort lexicographically same as chronologically).
        rows.sort((a, b) -> String.valueOf(a.get("time")).compareTo(String.valueOf(b.get("time"))));
        // Stamp serial numbers post-sort.
        for (int i = 0; i < rows.size(); i++) rows.get(i).put("srNo", i + 1);
        result.put("rows", rows);

        // Summary: counts by status + map of filterName → count for the top-blockers chips.
        int traded = 0, filtered = 0, errors = 0;
        Map<String, Integer> byFilter = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            String st = String.valueOf(r.get("status"));
            if ("TRADED".equals(st)) traded++;
            else if ("FILTERED".equals(st)) filtered++;
            else if ("ERROR".equals(st)) errors++;
            String fn = String.valueOf(r.getOrDefault("filterName", ""));
            String detail = String.valueOf(r.getOrDefault("detail", ""));
            // Persisted entries from before the filterName capture work shipped have an empty
            // filterName but still status=FILTERED. Backfill from the detail text so they show
            // a real category (TRADING_HOURS / RISK_LIMIT / etc.) instead of UNKNOWN.
            if (fn.isEmpty() && ("FILTERED".equals(st) || "ERROR".equals(st))) {
                fn = classifyByDetail(detail);
                r.put("filterName", fn);
            }
            // EMA_TREND was overloaded before the split — entries whose detail says "not aligned"
            // were really alignment failures. Reclassify by the source-of-truth detail text.
            if ("EMA_TREND".equals(fn) && detail != null && detail.toLowerCase().contains("not aligned")) {
                fn = "EMA_ALIGNMENT";
                r.put("filterName", fn);
            }
            // LEVEL_COUNT was renamed to EMA_20_DISTANCE — reclassify legacy entries so they
            // show under the new chip name without needing a state-file rewrite.
            if ("LEVEL_COUNT".equals(fn)) {
                fn = "EMA_20_DISTANCE";
                r.put("filterName", fn);
            }
            if (!fn.isEmpty()) byFilter.merge(fn, 1, Integer::sum);
        }
        // Sort byFilter descending by count (LinkedHashMap rebuild).
        Map<String, Integer> byFilterSorted = byFilter.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue,
                (a, b) -> a, LinkedHashMap::new));

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalSignals", rows.size());
        summary.put("traded", traded);
        summary.put("filtered", filtered);
        summary.put("errors", errors);
        summary.put("byFilter", byFilterSorted);
        result.put("summary", summary);
        return result;
    }

    /** Backfill heuristic for legacy SignalHistory entries without a filterName field, plus
     *  classifier for the full set of SignalProcessor rejection reasons. The legacy
     *  LPT-downgrade composite messages still map back to their original buckets so old
     *  signal-trail entries surface the right filter category. */
    private String classifyByDetail(String detail) {
        if (detail == null || detail.isEmpty()) return "UNKNOWN";
        String s = detail.toLowerCase();

        // Legacy composite "Probability downgraded to LPT (X)" lines — backfill to the
        // appropriate inner bucket. Live trades no longer produce these strings (LPT
        // downgrade removed) but persisted signal-history entries still reference them.
        if (s.contains("probability downgraded to lpt") || s.contains("→ lpt")) {
            if (s.contains("htf hurdle"))              return "HTF_HURDLE";
            if (s.contains("nifty opposed"))           return "NIFTY_OPPOSED";
            if (s.contains("inside-or"))               return "INSIDE_OR";
            if (s.contains("ev reversal"))             return "EV_REVERSAL";
            return "NIFTY_OPPOSED";
        }
        // New hard-reject phrasing for NIFTY opposed.
        if (s.contains("opposes nifty composite"))     return "NIFTY_OPPOSED";

        // Order-layer (TradingController) gates
        if (s.contains("outside trading hours"))                  return "TRADING_HOURS";
        if (s.contains("risk exposure") || s.contains("daily loss")) return "RISK_LIMIT";
        if (s.contains("kill switch"))                             return "KILL_SWITCH";

        // Pre-trade structural gates
        if (s.contains("extended-level"))                          return "EXTENDED_LEVEL";
        if (s.contains("is inside") && s.contains("zone"))         return "DH_DL_ZONE";
        if (s.contains("invalid atr"))                             return "INVALID_ATR";
        if (s.contains("wrong side of entry"))                     return "WRONG_SIDE_TARGET";

        // Candle-shape & volume rejections
        if (s.contains("small candle"))                            return "SMALL_CANDLE";
        if (s.contains("opposite wick pressure"))                  return "OPPOSITE_WICK";
        if (s.contains("large candle body"))                       return "LARGE_CANDLE";
        if (s.contains("low volume"))                              return "LOW_VOLUME";

        // EV / OR gates
        if (s.contains("mean-reversion setup only allowed"))       return "MEAN_REVERSION_DAY";
        if (s.contains("opposes or break"))                        return "EV_OR_OPPOSED";
        if (s.contains("inside or range"))                         return "EV_OR_INSIDE";
        if (s.contains("ev ") && s.contains("detected"))           return "EV_GAP_OPPOSED";

        // Risk / reward / profit gates
        if (s.contains("risk/reward") || s.contains("risk\\reward")) return "RISK_REWARD";
        if (s.contains("absolute profit too low"))                 return "MIN_PROFIT";

        // Order placement
        if (s.contains("order failed") || s.contains("rejected by broker")) return "ORDER_FAILED";

        // Probability disable
        if (s.contains("hpt not enabled") || s.contains("lpt not enabled") || s.contains("mpt not enabled"))
            return "PROB_DISABLED";

        // BreakoutScanner-side filters
        if (s.contains("zone(s) away"))                            return "EMA_20_DISTANCE";
        if (s.contains("too far from broken zone"))                return "LEVEL_PROXIMITY";
        // Distinguish price-vs-EMA (EMA_TREND) from stack-ordering (EMA_ALIGNMENT) within
        // the unified "blocked by 5-min EMA trend" log line. Also accepts the legacy
        // "sma trend" / "smas" wording so today's pre-rename log lines still classify.
        if ((s.contains("blocked by 5-min ema trend") || s.contains("blocked by 5-min sma trend"))
                && s.contains("not aligned")) return "EMA_ALIGNMENT";
        if (s.contains("not aligned (need 20"))                    return "EMA_ALIGNMENT";
        if (s.contains("blocked by 5-min ema trend") || s.contains("blocked by 5-min sma trend")) return "EMA_TREND";
        if (s.contains("close not above all emas") || s.contains("close not below all emas")
                || s.contains("close not above all smas") || s.contains("close not below all smas")) return "EMA_TREND";
        if (s.contains("nifty") && s.contains("opposes"))          return "NIFTY_OPPOSED";

        // Pre-scanner gates that early-return before any setup detection
        if (s.contains("position") && s.contains("already open"))  return "POSITION_OPEN";
        if (s.contains("already traded today") || s.contains("level already traded")) return "LEVEL_BROKEN";

        return "UNKNOWN";
    }

    private Map<String, Object> buildEodRow(String symbol, BreakoutScanner.SignalInfo si) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("symbol", symbol);
        row.put("time", si.time != null ? si.time : "");
        row.put("setup", si.setup != null ? si.setup : "");
        row.put("status", si.status != null ? si.status : "");
        row.put("filterName", si.filterName != null ? si.filterName : "");
        row.put("pattern", si.pattern != null ? si.pattern : "");
        row.put("price", Math.round(si.price * 100.0) / 100.0);
        row.put("detail", si.detail != null ? si.detail : "");
        return row;
    }

    @GetMapping("/api/scanner/status")
    public Map<String, Object> getScannerStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("tradingDay", marketHolidayService.isTradingDay());
        status.put("signalSource", riskSettings.getSignalSource());
        status.put("watchlistCount", marketDataService.getWatchlist().size());
        status.put("universeSize", bhavcopyService.getScanUniverseCount());
        status.put("atrLoaded", atrService.getLoadedCountFor(marketDataService.getWatchlist()));
        status.put("emaLoaded", emaService.getLoadedCountFor(marketDataService.getWatchlist()));
        status.put("firstCandleLoaded", candleAggregator.getFirstCandleCloseCountFor(marketDataService.getWatchlist()));
        status.put("validationPass", marketDataService.getValidationPass());
        status.put("validationFail", marketDataService.getValidationFail());
        status.put("validationTotal", marketDataService.getValidationTotal());
        status.put("timeframe", riskSettings.getScannerTimeframe());
        status.put("higherTimeframe", riskSettings.getHigherTimeframe());
        status.put("enableHpt", riskSettings.isEnableHpt());
        status.put("enableMpt", riskSettings.isEnableMpt());
        // enableEmaTrend removed — EMA factor is hard-baked into strict trend state.
        // Used by scanner.html to conditionally render trend-related slots on the cards
        // so the chip + detail rows match which filters are actually gating. Disabling a
        // filter hides its dedicated chip/row.
        status.put("enableIndexAlignment",      riskSettings.isEnableIndexAlignment());
        status.put("enableIndexHtfAlignment",   riskSettings.isEnableIndexHtfAlignment());
        status.put("enableStockHtfAlignment",   riskSettings.isEnableStockHtfAlignment());
        status.put("enableOpenRangeFilter",     riskSettings.isEnableOpenRangeFilter());
        status.put("enableIndexOpenRangeFilter", riskSettings.isEnableIndexOpenRangeFilter());
        status.put("minPrice", riskSettings.getScanMinPrice());
        status.put("maxPrice", riskSettings.getScanMaxPrice());
        status.put("cprWidthSqueezeMult",  riskSettings.getCprWidthSqueezeMult());
        status.put("cprWidthWideMult",     riskSettings.getCprWidthWideMult());
        status.put("insideMaxWidth",       riskSettings.getInsideCprMaxWidth());
        // Live threshold the scanner cards use to color the "Today's TR" chip — so the
        // color flips immediately when the user tunes the setting (no page reload needed).
        status.put("enableDailyAtrExhaustionFilter", riskSettings.isEnableDailyAtrExhaustionFilter());
        status.put("dailyAtrExhaustionMult",         riskSettings.getDailyAtrExhaustionMult());
        return status;
    }

    @GetMapping("/api/scanner/chart")
    public Map<String, Object> getChartData(@RequestParam String symbol,
                                             @RequestParam(required = false, defaultValue = "5") int interval) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("symbol", symbol);
        result.put("interval", interval);

        boolean tradingDay = marketHolidayService.isTradingDay();
        List<Map<String, Object>> candleList = new ArrayList<>();
        List<Map<String, Object>> vwapSeries = new ArrayList<>();
        List<Map<String, Object>> ema20Series = new ArrayList<>();

        // 1-hour panes always show the current trading week (Mon→Fri) with hourly
        // bars instead of just today's session, so the user can see weekly structure
        // and how price is interacting with weekly CPR. This means we MUST hit the
        // historical fetch even on a live trading day — the in-memory CandleAggregator
        // only holds today + yesterday's session.
        if (interval == 60) {
            try {
                java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
                java.time.LocalDate[] week = currentTradingWeek(ist);  // [monday, friday]
                // Explicit Mon 9:15 → Fri 15:30 window. Surfaced to the frontend so the
                // timeline anchors at the full week even on Mon/Tue when later days
                // have no bars yet — the chart still LOOKS like a Mon-Fri view with the
                // remaining days as empty space on the right.
                long weekStartSec = week[0].atTime(9, 15).atZone(ist).toEpochSecond();
                long weekEndSec   = week[1].atTime(15, 30).atZone(ist).toEpochSecond();
                result.put("weekStart", weekStartSec * 1000L);
                result.put("weekEnd",   weekEndSec   * 1000L);
                List<CandleAggregator.CandleBar> hist = atrService.fetchTodayCandles(symbol);
                if (hist != null && !hist.isEmpty()) {
                    List<CandleAggregator.CandleBar> weekBars = filterToWeek(hist, week[0], week[1], ist);
                    List<CandleAggregator.CandleBar> hourly = aggregateToHourly(weekBars);
                    java.time.LocalDate displayDate = week[1]; // anchor at week-end for the EMA seeding date logic
                    computeIndicatorsAndBuild(hourly, displayDate, ist, candleList, vwapSeries, ema20Series);
                    result.put("dataDate", week[0].toString() + "..." + week[1].toString());
                }
                result.put("dataSource", tradingDay ? "live-weekly" : "historical-weekly");
            } catch (Exception e) {
                result.put("dataSource", "error");
                result.put("error", e.getMessage());
            }
        } else if (tradingDay) {
            // Live path: compute indicators progressively over prior-day warmup + today so
            // every today's bar has a fully-warmed-up EMA (bar 1 included).
            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            java.time.LocalDate today = java.time.LocalDate.now(ist);
            List<CandleAggregator.CandleBar> priors = candleAggregator.getPriorDayCandles(symbol);
            List<CandleAggregator.CandleBar> todays  = candleAggregator.getCompletedCandles(symbol);
            List<CandleAggregator.CandleBar> merged = new ArrayList<>();
            if (priors != null) merged.addAll(priors);
            if (todays != null) merged.addAll(todays);
            computeIndicatorsAndBuild(merged, today, ist, candleList, vwapSeries, ema20Series);

            // Forming (current, still-open) candle — append with live indicator values
            CandleAggregator.CandleBar current = candleAggregator.getCurrentCandle(symbol);
            if (current != null && current.open > 0) {
                candleList.add(barToMap(current, true));
                long tMs = current.epochSec * 1000L;
                double liveVwap = candleAggregator.getAtp(symbol);
                double liveEma20 = emaService.getEma(symbol);
                if (liveVwap > 0) vwapSeries.add(point(tMs, liveVwap));
                if (liveEma20 > 0) ema20Series.add(point(tMs, liveEma20));
            }
            result.put("dataSource", "live");
        } else {
            // Non-trading day: fetch multi-day historical, compute indicators progressively, show most recent trading day
            try {
                List<CandleAggregator.CandleBar> hist = atrService.fetchTodayCandles(symbol);
                if (!hist.isEmpty()) {
                    java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
                    java.time.LocalDate latestDate = null;
                    for (CandleAggregator.CandleBar c : hist) {
                        java.time.LocalDate d = java.time.Instant.ofEpochSecond(c.epochSec).atZone(ist).toLocalDate();
                        if (latestDate == null || d.isAfter(latestDate)) latestDate = d;
                    }
                    if (latestDate != null) {
                        // Compute indicators progressively (Fyers API doesn't return them)
                        computeIndicatorsAndBuild(hist, latestDate, ist, candleList, vwapSeries, ema20Series);
                        result.put("dataDate", latestDate.toString());
                    }
                }
                result.put("dataSource", "historical");
            } catch (Exception e) {
                result.put("dataSource", "error");
                result.put("error", e.getMessage());
            }
        }
        result.put("candles", candleList);
        result.put("vwapSeries", vwapSeries);
        result.put("ema20Series", ema20Series);
        result.put("tradingDay", tradingDay);

        // CPR levels:
        //   interval=60       → weekly CPR (the chart spans Mon→Fri so weekly bands are the
        //                       structurally-correct overlay).
        //   interval=5 live   → daily CPR cached for today (computed from yesterday's OHLC).
        //   interval=5 hist   → previous-day CPR (the one that was active on the displayed day).
        Map<String, Object> cpr = null;
        if (interval == 60) {
            WeeklyCprService.WeeklyLevels wl = weeklyCprService.getWeeklyLevels(symbol);
            if (wl != null && wl.pivot > 0) {
                cpr = new LinkedHashMap<>();
                cpr.put("top", r(Math.max(wl.tc, wl.bc)));
                cpr.put("pivot", r(wl.pivot));
                cpr.put("bottom", r(Math.min(wl.tc, wl.bc)));
                cpr.put("r1", r(wl.r1));
                cpr.put("r2", r(wl.r2));
                cpr.put("r3", r(wl.r3));
                cpr.put("r4", r(wl.r4));
                cpr.put("s1", r(wl.s1));
                cpr.put("s2", r(wl.s2));
                cpr.put("s3", r(wl.s3));
                cpr.put("s4", r(wl.s4));
                cpr.put("pdh", r(wl.ph));   // previous week high
                cpr.put("pdl", r(wl.pl));   // previous week low
                cpr.put("scope", "weekly");
            }
        } else {
            CprLevels lv = tradingDay ? bhavcopyService.getCprLevels(symbol) : bhavcopyService.getPreviousCpr(symbol);
            if (lv != null) {
                cpr = new LinkedHashMap<>();
                cpr.put("top", r(Math.max(lv.getTc(), lv.getBc())));
                cpr.put("pivot", r(lv.getPivot()));
                cpr.put("bottom", r(Math.min(lv.getTc(), lv.getBc())));
                cpr.put("r1", r(lv.getR1()));
                cpr.put("r2", r(lv.getR2()));
                cpr.put("r3", r(lv.getR3()));
                cpr.put("r4", r(lv.getR4()));
                cpr.put("s1", r(lv.getS1()));
                cpr.put("s2", r(lv.getS2()));
                cpr.put("s3", r(lv.getS3()));
                cpr.put("s4", r(lv.getS4()));
                cpr.put("pdh", r(lv.getPh()));
                cpr.put("pdl", r(lv.getPl()));
                cpr.put("scope", "daily");
                // Opening Range high/low — first {openRangeMinutes} of today's session. Only
                // meaningful on the live trading day; on historical / weekend views the helpers
                // return 0 (no completed bars today). The chart layer hides 0-value lines.
                if (tradingDay) {
                    int orMins = riskSettings.getOpenRangeMinutes();
                    cpr.put("orHigh", r(candleAggregator.getOpenRangeHigh(symbol, orMins)));
                    cpr.put("orLow",  r(candleAggregator.getOpenRangeLow(symbol, orMins)));
                    cpr.put("orMinutes", orMins);
                }
            }
        }
        if (cpr != null) result.put("cpr", cpr);

        // Indicators (current values)
        result.put("ltp", r(candleAggregator.getLtp(symbol)));
        result.put("vwap", r(candleAggregator.getAtp(symbol)));
        result.put("ema20", r(emaService.getEma(symbol)));

        // Trades for this symbol (today's trades only, for live mode)
        List<Map<String, Object>> trades = new ArrayList<>();
        if (tradingDay) {
            for (com.rydytrader.autotrader.dto.TradeRecord tr : tradeHistoryService.getTrades()) {
                if (!symbol.equals(tr.getSymbol())) continue;
                Map<String, Object> t = new LinkedHashMap<>();
                t.put("setup", tr.getSetup());
                t.put("side", tr.getSide());
                t.put("entryPrice", tr.getEntryPrice());
                t.put("exitPrice", tr.getExitPrice());
                t.put("exitReason", tr.getExitReason());
                t.put("netPnl", tr.getNetPnl());
                t.put("qty", tr.getQty());
                // exit time from timestamp (format "HH:mm:ss")
                t.put("exitTime", timeToEpochMs(tr.getTimestamp()));
                // entry time — try to parse from description first line like "HH:mm:ss [ENTRY]"
                t.put("entryTime", extractEntryTimeMs(tr.getDescription(), tr.getTimestamp()));
                trades.add(t);
            }
        }
        result.put("trades", trades);

        // Timeframe minutes (for client to know candle duration)
        result.put("timeframeMinutes", riskSettings.getScannerTimeframe());
        return result;
    }

    private long timeToEpochMs(String hms) {
        if (hms == null || hms.isEmpty()) return 0L;
        try {
            java.time.ZoneId ist = java.time.ZoneId.of("Asia/Kolkata");
            java.time.LocalDate today = java.time.LocalDate.now(ist);
            java.time.LocalTime t = java.time.LocalTime.parse(hms);
            return today.atTime(t).atZone(ist).toEpochSecond() * 1000L;
        } catch (Exception e) {
            return 0L;
        }
    }

    private long extractEntryTimeMs(String description, String fallbackTimestamp) {
        if (description != null) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\d{2}:\\d{2}:\\d{2})\\s+\\[ENTRY\\]").matcher(description);
            if (m.find()) return timeToEpochMs(m.group(1));
        }
        return timeToEpochMs(fallbackTimestamp);
    }

    @GetMapping("/api/scanner/tv-watchlist")
    public ResponseEntity<String> getTvWatchlist() {
        // Group the watchlist by each stock's primary index. Output uses TradingView's
        // ###Section header syntax — one section per index, with the index ticker itself
        // listed first followed by its member stocks. Section order: NIFTY 50 always
        // first, then remaining indices descending by stock count. Indices with no
        // member stocks are skipped except NIFTY 50, which is always emitted.
        Map<String, String> indexNameByTicker = new LinkedHashMap<>();
        Map<String, List<String>> stocksByIndex = new LinkedHashMap<>();

        for (Map<String, Object> card : getWatchlist()) {
            Object sym = card.get("symbol");
            if (sym == null) continue;
            String stockSym = sym.toString().replaceAll("-EQ$", "").replace("-", "_");

            Object pTicker = card.get("primaryIndexTicker");
            Object pName   = card.get("primaryIndexName");
            String idxTicker = (pTicker != null && !pTicker.toString().isEmpty()) ? pTicker.toString() : "NIFTY50";
            String idxName   = (pName   != null && !pName.toString().isEmpty())   ? pName.toString()   : "NIFTY 50";

            indexNameByTicker.put(idxTicker, idxName);
            stocksByIndex.computeIfAbsent(idxTicker, k -> new ArrayList<>()).add(stockSym);
        }

        // Sort: NIFTY 50 first, then by stock count descending. Tie-break alphabetically.
        List<String> ordered = new ArrayList<>(stocksByIndex.keySet());
        ordered.sort((a, b) -> {
            if ("NIFTY50".equals(a) && !"NIFTY50".equals(b)) return -1;
            if ("NIFTY50".equals(b) && !"NIFTY50".equals(a)) return 1;
            int cmp = Integer.compare(stocksByIndex.get(b).size(), stocksByIndex.get(a).size());
            return cmp != 0 ? cmp : a.compareTo(b);
        });

        // NIFTY 50 is always present in the export even if no stocks map to it.
        if (!ordered.contains("NIFTY50")) {
            ordered.add(0, "NIFTY50");
            indexNameByTicker.putIfAbsent("NIFTY50", "NIFTY 50");
        }

        StringBuilder csv = new StringBuilder();
        for (String idxTicker : ordered) {
            List<String> stocks = stocksByIndex.getOrDefault(idxTicker, Collections.emptyList());
            if (stocks.isEmpty() && !"NIFTY50".equals(idxTicker)) continue;
            String sectionName = indexNameByTicker.getOrDefault(idxTicker, idxTicker);
            csv.append("###").append(sectionName).append(",");
            csv.append("NSE:").append(toTradingViewIndexSymbol(idxTicker)).append(",");
            for (String s : stocks) csv.append(s).append(",");
        }

        String filename = "watchlist-" + java.time.LocalDate.now() + ".txt";
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
            .contentType(MediaType.TEXT_PLAIN)
            .body(csv.toString());
    }

    /**
     * Map our Fyers-style index ticker (NIFTY50, NIFTYBANK, FINNIFTY, …) to the symbol
     * TradingView actually understands when imported into an NSE watchlist. Falls back
     * to the input ticker if no mapping exists. Used only by the TV-watchlist export.
     */
    private static final Map<String, String> FYERS_TO_TV_INDEX = Map.ofEntries(
        Map.entry("NIFTY50",          "NIFTY"),
        Map.entry("NIFTYBANK",        "BANKNIFTY"),
        Map.entry("NIFTYAUTO",        "CNXAUTO"),
        Map.entry("NIFTYIT",          "CNXIT"),
        Map.entry("NIFTYPHARMA",      "CNXPHARMA"),
        Map.entry("NIFTYFMCG",        "CNXFMCG"),
        Map.entry("NIFTYMETAL",       "CNXMETAL"),
        Map.entry("NIFTYENERGY",      "CNXENERGY"),
        Map.entry("NIFTYOILANDGAS",   "NIFTY_OIL_AND_GAS"),
        Map.entry("NIFTYHEALTHCARE",  "NIFTY_HEALTHCARE"),
        Map.entry("FINNIFTY",         "CNXFINANCE"),
        Map.entry("NIFTYREALTY",      "CNXREALTY"),
        Map.entry("NIFTYMEDIA",       "CNXMEDIA"),
        Map.entry("NIFTYCOMMODITIES", "CNXCOMMODITIES"),
        Map.entry("NIFTYINFRA",       "CNXINFRA"),
        Map.entry("NIFTYSERVSECTOR",  "CNXSERVICE"),
        Map.entry("NIFTYCONSRDURBL",  "NIFTY_CONSR_DURBL"),
        Map.entry("NIFTYCONSUMPTION", "CNXCONSUMPTION")
    );

    private static String toTradingViewIndexSymbol(String fyersTicker) {
        return FYERS_TO_TV_INDEX.getOrDefault(fyersTicker, fyersTicker);
    }

    @GetMapping("/api/scanner/fyers-watchlist")
    public ResponseEntity<String> getFyersWatchlist() {
        // Export in Fyers import format: one symbol per line, e.g. "NSE:RELIANCE-EQ"
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> card : getWatchlist()) {
            Object sym = card.get("symbol");
            if (sym != null) {
                sb.append(sym.toString()).append("\n");
            }
        }
        String filename = "fyers-watchlist-" + java.time.LocalDate.now() + ".txt";
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
            .contentType(MediaType.TEXT_PLAIN)
            .body(sb.toString());
    }

    @GetMapping("/api/scanner/simulate-qty")
    public Map<String, Object> simulateQty() {
        Map<String, Object> result = new LinkedHashMap<>();
        double riskPerTrade = riskSettings.getRiskPerTrade();
        double capitalPerTrade = riskSettings.getCapitalPerTrade();
        double atrMultiplier = riskSettings.getAtrMultiplier();
        int fixedQty = riskSettings.getFixedQuantity();

        result.put("riskPerTrade", riskPerTrade);
        result.put("capitalPerTrade", capitalPerTrade);
        result.put("atrMultiplier", atrMultiplier);
        result.put("fixedQuantity", fixedQty);

        List<Map<String, Object>> stocks = new ArrayList<>();
        for (String fyersSymbol : marketDataService.getWatchlist()) {
            String ticker = fyersSymbol.replaceAll("^(NSE|BSE|MCX):", "").replaceAll("-(EQ|INDEX)$", "");
            double ltp = candleAggregator.getLtp(fyersSymbol);
            if (ltp <= 0) {
                CprLevels cpr = bhavcopyService.getCprLevels(ticker);
                if (cpr != null) ltp = cpr.getClose();
            }
            double atr = atrService.getAtr(fyersSymbol);
            int leverage = marginDataService.getLeverage(fyersSymbol);

            Map<String, Object> s = new LinkedHashMap<>();
            s.put("symbol", ticker);
            s.put("ltp", r(ltp));
            s.put("atr", r(atr));
            s.put("leverage", leverage);

            if (fixedQty != -1) {
                int qty = Math.max(2, fixedQty % 2 != 0 ? fixedQty + 1 : fixedQty);
                s.put("qty", qty);
                s.put("mode", "FIXED");
                s.put("slDist", r(atr * atrMultiplier));
                s.put("riskQty", "--");
                s.put("capitalCapQty", "--");
                s.put("capitalUsed", r(ltp * qty));
                s.put("riskAmount", r(atr * atrMultiplier * qty));
            } else if (atr > 0 && ltp > 0) {
                double slDist = atr * atrMultiplier;
                int riskQty = (int) (riskPerTrade / slDist);
                double effectiveCapital = (capitalPerTrade * leverage) / 2.0;
                int capitalCapQty = (int) (effectiveCapital / ltp);
                int rawQty = Math.min(riskQty, capitalCapQty);
                int qty = Math.max(2, (rawQty / 2) * 2);
                boolean capped = riskQty > capitalCapQty;

                s.put("qty", qty);
                s.put("mode", capped ? "CAPITAL-CAPPED" : "RISK-BASED");
                s.put("slDist", r(slDist));
                s.put("riskQty", riskQty);
                s.put("capitalCapQty", capitalCapQty);
                s.put("capitalUsed", r(ltp * qty));
                s.put("riskAmount", r(slDist * qty));
            } else {
                s.put("qty", 2);
                s.put("mode", "MIN (no ATR)");
                s.put("slDist", 0);
                s.put("riskQty", 0);
                s.put("capitalCapQty", 0);
                s.put("capitalUsed", r(ltp * 2));
                s.put("riskAmount", 0);
            }
            stocks.add(s);
        }
        result.put("stocks", stocks);
        return result;
    }

    @PostMapping("/api/scanner/rebuild")
    public Map<String, Object> rebuildWatchlist() {
        int count = marketDataService.rebuildWatchlist();
        return Map.of("ok", true, "watchlistCount", count);
    }

}
