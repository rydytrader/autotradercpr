package com.rydytrader.autotrader.service;

import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Enriches the {@link OptionOiTracker}'s raw max-OI snapshot with live LTP +
 * ΔLTP% from {@link MarketDataService} and classifies each side's buildup
 * (Long Buildup / Short Buildup / Short Covering / Long Unwinding).
 *
 * <p>Shared by:
 * <ul>
 *   <li>{@code OptionOiController} — {@code GET /api/option-oi/max-buildup} for
 *       direct REST consumers.</li>
 *   <li>{@code Camarilla.dashboardState()} — embeds the snapshot in the SSE
 *       broadcast so the Live Positions header chips update every ~2 s.</li>
 * </ul>
 *
 * <p>All work is in-memory — no Fyers REST calls. ΔOI is intraday (since 09:15
 * baseline), ΔLTP% is vs prior-day close (from {@code getDisplayChangePct}).
 */
@Service
public class OptionOiBuildupService {

    private final OptionOiTracker   tracker;
    private final MarketDataService marketData;

    public OptionOiBuildupService(OptionOiTracker tracker, MarketDataService marketData) {
        this.tracker    = tracker;
        this.marketData = marketData;
    }

    /** Build the JSON-shaped map served to the UI. Always returns a populated
     *  {@code asOf} + {@code ce}/{@code pe} entries (null when no data yet). */
    public Map<String, Object> currentEnriched() {
        Map<String, Object> out = new LinkedHashMap<>();
        OptionOiTracker.MaxOiBuildup max = tracker.currentMaxOi();
        out.put("ce", enrich(max == null ? null : max.ce()));
        out.put("pe", enrich(max == null ? null : max.pe()));
        return out;
    }

    private Map<String, Object> enrich(OptionOiTracker.SideMaxOi side) {
        if (side == null || side.strike() == 0) return null;
        double ltp = 0, ltpChgPct = 0;
        if (side.symbol() != null && !side.symbol().isBlank()) {
            try { ltp       = marketData.getLtp(side.symbol()); }                catch (Exception ignored) {}
            try { ltpChgPct = marketData.getDisplayChangePct(side.symbol()); }   catch (Exception ignored) {}
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("strike",    side.strike());
        m.put("symbol",    side.symbol() == null ? "" : side.symbol());
        m.put("oi",        side.oi());
        m.put("oiChange",  side.oiChange());
        m.put("ltp",       round2(ltp));
        m.put("ltpChgPct", round2(ltpChgPct));
        m.put("buildup",   classifyBuildup(ltpChgPct, side.oiChange()));
        return m;
    }

    /** Buildup taxonomy: price↑ / OI↑ → Long Buildup ; price↓ / OI↑ → Short Buildup ;
     *  price↑ / OI↓ → Short Covering ; price↓ / OI↓ → Long Unwinding.
     *  Returns {@code "Neutral"} when either input is flat. */
    public static String classifyBuildup(double ltpChgPct, long oiChange) {
        if (ltpChgPct == 0 || oiChange == 0) return "Neutral";
        boolean priceUp = ltpChgPct > 0;
        boolean oiUp    = oiChange > 0;
        if (priceUp  && oiUp)  return "Long Buildup";
        if (!priceUp && oiUp)  return "Short Buildup";
        if (!priceUp && !oiUp) return "Long Unwinding";
        return "Short Covering";
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
