package com.rydytrader.autotrader.service.strategy;

import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Walks the NIFTY weekly option chain to pick strikes for the ATM VWAP strategy.
 *
 * <p>Two entry points:
 * <ul>
 *   <li>{@link #select(double)} — spot-based ATM (NIFTY/50 rounded to nearest). Returns the
 *       strike's CE + PE symbols and LTPs. The synthetic-futures-via-parity variant was
 *       retired because the strategy short-sells CE and PE on independent triggers — they
 *       never fire at the same instant, so put-call premium balance offers no value over
 *       plain spot rounding. Spot ATM is simpler, matches retail convention, and avoids the
 *       intra-bar drift that put-call parity introduces.</li>
 *   <li>{@link #resolveStrikeAtLevel(double)} — workhorse: given an arbitrary price
 *       level, returns the nearest tradable strike along with its CE+PE symbols and
 *       current LTPs.</li>
 * </ul>
 *
 * <p>Since the strip-Fyers-data refactor, chain data is synthesized locally — strikes are
 * enumerated around a base ATM (NIFTY step = 50) and the Fyers option-symbol string is
 * built directly from the current-week Tuesday expiry (SEBI's weekly-expiry rule; walked
 * back one trading day when Tuesday is an NSE holiday). LTPs come from
 * {@link MarketDataService#getLtp}, which is populated by whatever tick feed is currently
 * subscribed (GDFL in production). Strikes that haven't been subscribed yet return LTP=0.
 */
@Service
public class BalancedAtmSelector {

    private static final Logger log = LoggerFactory.getLogger(BalancedAtmSelector.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_STEP  = 50L;
    /** Number of strike steps on each side of ATM to synthesize per chain fetch. */
    private static final int    CHAIN_HALF_WIDTH = 30;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final MarketDataService     marketDataService;
    private final MarketHolidayService  marketHolidayService;

    public BalancedAtmSelector(MarketDataService marketDataService,
                               MarketHolidayService marketHolidayService) {
        this.marketDataService     = marketDataService;
        this.marketHolidayService  = marketHolidayService;
    }

    /** Outcome of one ATM selection. {@code spotAtm} is the naïve spot/50 baseline — kept
     *  for transparency so callers can see how far the parity-based pick moved from
     *  "obvious". {@code chosenAtm} is what the bot would trade. */
    public record AtmSelection(
        long    spotAtm,
        long    chosenAtm,
        double  ceLtpAtChosen,
        double  peLtpAtChosen,
        double  premiumGapAtChosen,
        String  ceSymbolAtChosen,
        String  peSymbolAtChosen
    ) {}

    /** Resolved strike + per-leg symbols + per-leg LTPs at a price level. Returned by
     *  {@link #resolveStrikeAtLevel(double)}; caller picks which side (CE or PE) to trade. */
    public record StrikeAtLevel(
        double  requestedLevel,
        long    resolvedStrike,
        String  ceSymbol,
        String  peSymbol,
        double  ceLtp,
        double  peLtp
    ) {}

    /** Per-strike chain row exposed to callers that want to walk the entire chain without
     *  re-fetching it for each strike. Mirrors the private {@code ChainRow} fields but in a
     *  public, immutable record so cross-package consumers can read it safely. */
    public record ChainStrike(long strike, String ceSymbol, double ceLtp,
                              String peSymbol, double peLtp) {}

    /**
     * Spot-based ATM for the given live NIFTY LTP — strike = round(NIFTY / 50). Returns the
     * strike's CE and PE symbols and current LTPs. Returns {@code null} when the option chain
     * is unavailable or the ATM strike is unquoted.
     */
    public AtmSelection select(double niftyLtp) {
        if (niftyLtp <= 0) return null;
        long spotAtm = Math.round(niftyLtp / STRIKE_STEP) * STRIKE_STEP;

        NavigableMap<Long, ChainRow> chain = fetchChain();
        if (chain == null || chain.isEmpty()) return null;

        ChainRow atmRow = chain.get(spotAtm);
        if (atmRow == null
            || atmRow.ceSym == null || atmRow.ceSym.isEmpty()
            || atmRow.peSym == null || atmRow.peSym.isEmpty()) {
            log.warn("[atm-selector] spot-ATM strike {} missing in chain — skipping", spotAtm);
            return null;
        }

        return new AtmSelection(
            spotAtm,
            spotAtm,                              // chosen == spot (no synthetic adjustment)
            atmRow.ce,
            atmRow.pe,
            Math.abs(atmRow.ce - atmRow.pe),
            atmRow.ceSym,
            atmRow.peSym
        );
    }

    /**
     * Pick the strike closest to {@code level} that has BOTH CE and PE symbols, and return
     * its symbols + LTPs. Used by the ATM VWAP strategy to translate the first-bar-close
     * NIFTY price into the ATM option pair. Returns {@code null} when the chain is empty.
     */
    public StrikeAtLevel resolveStrikeAtLevel(double level) {
        if (level <= 0) return null;
        NavigableMap<Long, ChainRow> chain = fetchChain();
        if (chain == null || chain.isEmpty()) return null;

        long rounded = Math.round(level / (double) STRIKE_STEP) * STRIKE_STEP;
        long chosen = nearestStrikeWithBothLegs(chain, rounded);
        if (chosen <= 0) {
            log.warn("[atm-selector] resolveStrikeAtLevel({}): no strike found in synthetic chain", level);
            return null;
        }
        ChainRow row = chain.get(chosen);
        return new StrikeAtLevel(level, chosen, row.ceSym, row.peSym, row.ce, row.pe);
    }

    /**
     * OTM-aware sibling of {@link #resolveStrikeAtLevel}. Rounds the level in the direction
     * that keeps the desired option side strictly out-of-the-money, given that the setup
     * fires when spot has moved to the far side of the level:
     *
     * <ul>
     *   <li>{@code side = "PE"} &rarr; floor(level / STRIKE_STEP) &times; STRIKE_STEP —
     *       returns the highest strike &le; level. PE at that strike is OTM when spot &gt; level.</li>
     *   <li>{@code side = "CE"} &rarr; ceil(level / STRIKE_STEP) &times; STRIKE_STEP —
     *       returns the lowest strike &ge; level. CE at that strike is OTM when spot &lt; level.</li>
     * </ul>
     *
     * <p>If the computed floor/ceil strike isn't in the synthetic chain window,
     * {@link #nearestStrikeWithBothLegs} walks outward to the nearest available strike —
     * same safe-fallback behaviour as {@link #resolveStrikeAtLevel}.
     *
     * <p>For an unrecognised side, falls back to {@link #resolveStrikeAtLevel}.
     */
    public StrikeAtLevel resolveOtmStrikeAtLevel(double level, String side) {
        return resolveOtmStrikeAtLevel(NIFTY_SYMBOL, STRIKE_STEP, level, side);
    }

    /** Per-instrument overload — takes the spot symbol + strike step. Kept for signature
     *  compatibility; only NIFTY is exercised today (STRIKE_STEP=50, weekly chain). */
    public StrikeAtLevel resolveOtmStrikeAtLevel(String spotSymbol, long strikeStep,
                                                  double level, String side) {
        if (level <= 0) return null;
        long targetStrike;
        if ("PE".equalsIgnoreCase(side)) {
            targetStrike = (long) Math.floor(level / (double) strikeStep) * strikeStep;
        } else if ("CE".equalsIgnoreCase(side)) {
            targetStrike = (long) Math.ceil(level / (double) strikeStep) * strikeStep;
        } else {
            return resolveStrikeAtLevel(level);
        }
        NavigableMap<Long, ChainRow> chain = fetchChain(spotSymbol);
        if (chain == null || chain.isEmpty()) return null;
        long chosen = nearestStrikeWithBothLegs(chain, targetStrike);
        if (chosen <= 0) {
            log.warn("[atm-selector] resolveOtmStrikeAtLevel({}, {}, {}): no strike found in synthetic chain",
                spotSymbol, level, side);
            return null;
        }
        ChainRow row = chain.get(chosen);
        return new StrikeAtLevel(level, chosen, row.ceSym, row.peSym, row.ce, row.pe);
    }

    /** Walk outward from {@code requested} until we find a strike whose CE AND PE symbols
     *  are populated. With the synthetic chain every strike inside the window is populated,
     *  so this typically returns {@code requested} directly; the walk only kicks in when the
     *  requested strike is outside the ±{@value #CHAIN_HALF_WIDTH} window. */
    private long nearestStrikeWithBothLegs(NavigableMap<Long, ChainRow> chain, long requested) {
        if (chain.containsKey(requested) && isQuoted(chain.get(requested))) return requested;
        Long above = chain.higherKey(requested);
        Long below = chain.lowerKey(requested);
        while (above != null || below != null) {
            long candidate;
            if (above == null) { candidate = below; below = chain.lowerKey(below); }
            else if (below == null) { candidate = above; above = chain.higherKey(above); }
            else if ((above - requested) <= (requested - below)) {
                candidate = above; above = chain.higherKey(above);
            } else {
                candidate = below; below = chain.lowerKey(below);
            }
            if (isQuoted(chain.get(candidate))) return candidate;
        }
        return 0;
    }

    private static boolean isQuoted(ChainRow row) {
        if (row == null) return false;
        // Synthetic-chain era: "quoted" == symbols populated. LTPs may still be 0 for
        // strikes that haven't been subscribed on the tick feed yet.
        return row.ceSym != null && !row.ceSym.isEmpty()
            && row.peSym != null && !row.peSym.isEmpty();
    }

    /** Public bulk-fetch entry point: returns the entire synthetic chain (ATM ± 30 strikes)
     *  keyed by strike. Cross-package callers use this when they want to walk MANY strikes
     *  from a single response instead of calling {@link #resolveStrikeAtLevel(double)}
     *  once per strike. Returns an empty map when NIFTY spot LTP is unavailable. */
    public NavigableMap<Long, ChainStrike> fetchChainStrikes() {
        NavigableMap<Long, ChainRow> raw = fetchChain();
        NavigableMap<Long, ChainStrike> out = new TreeMap<>();
        if (raw == null) return out;
        for (Map.Entry<Long, ChainRow> e : raw.entrySet()) {
            ChainRow r = e.getValue();
            out.put(e.getKey(), new ChainStrike(e.getKey(),
                r.ceSym == null ? "" : r.ceSym, r.ce,
                r.peSym == null ? "" : r.peSym, r.pe));
        }
        return out;
    }

    private static class ChainRow {
        double ce, pe;
        String ceSym = "", peSym = "";
    }

    private NavigableMap<Long, ChainRow> fetchChain() {
        return fetchChain(NIFTY_SYMBOL);
    }

    /** Per-symbol synthetic chain build. Enumerates strikes ATM ± {@value #CHAIN_HALF_WIDTH}
     *  from the current NIFTY spot LTP, synthesizes each CE/PE symbol using this week's
     *  Tuesday expiry, and reads any available LTP from {@link MarketDataService}. */
    private NavigableMap<Long, ChainRow> fetchChain(String spotSymbol) {
        NavigableMap<Long, ChainRow> byStrike = new TreeMap<>();
        double spot = marketDataService.getLtp(spotSymbol);
        if (spot <= 0) spot = marketDataService.getDisplayLtp(spotSymbol);
        if (spot <= 0) {
            log.debug("[atm-selector] synthetic chain skipped — no spot LTP for {}", spotSymbol);
            return byStrike;
        }
        long baseAtm = Math.round(spot / (double) STRIKE_STEP) * STRIKE_STEP;
        String expiryTag = currentWeeklyExpiryTag();
        if (expiryTag == null) {
            log.warn("[atm-selector] synthetic chain skipped — could not resolve weekly expiry tag");
            return byStrike;
        }
        String underlying = underlyingFromSpotSymbol(spotSymbol);
        for (int n = -CHAIN_HALF_WIDTH; n <= CHAIN_HALF_WIDTH; n++) {
            long strike = baseAtm + (long) n * STRIKE_STEP;
            if (strike <= 0) continue;
            String ceSym = "NSE:" + underlying + expiryTag + strike + "CE";
            String peSym = "NSE:" + underlying + expiryTag + strike + "PE";
            ChainRow r = new ChainRow();
            r.ceSym = ceSym;
            r.peSym = peSym;
            r.ce    = marketDataService.getLtp(ceSym);
            r.pe    = marketDataService.getLtp(peSym);
            byStrike.put(strike, r);
        }
        return byStrike;
    }

    /** Peel the {@code NSE:} prefix and {@code -INDEX} suffix off a spot symbol
     *  (e.g. {@code NSE:NIFTY50-INDEX} → {@code NIFTY}). NIFTY-weekly options use the
     *  bare underlying tag (no digits) — {@code NIFTY50} becomes {@code NIFTY}. */
    private static String underlyingFromSpotSymbol(String spotSymbol) {
        String s = spotSymbol;
        if (s.startsWith("NSE:")) s = s.substring(4);
        int dash = s.indexOf('-');
        if (dash > 0) s = s.substring(0, dash);
        // NIFTY50 → NIFTY, BANKNIFTY stays.
        if (s.startsWith("NIFTY") && s.length() > 5 && Character.isDigit(s.charAt(5))) {
            s = "NIFTY";
        }
        return s;
    }

    /** Fyers weekly-expiry tag encoding the current-week Tuesday (SEBI's NIFTY weekly
     *  expiry day). Format: {@code YYMDD} where M is {@code 1-9} for Jan-Sep and
     *  {@code O|N|D} for Oct/Nov/Dec. Walks back one day if that Tuesday is an NSE
     *  holiday (uses {@link MarketHolidayService}). Returns {@code null} on any
     *  failure to compute. */
    private String currentWeeklyExpiryTag() {
        try {
            LocalDate today = LocalDate.now(IST);
            int daysUntilTue = (DayOfWeek.TUESDAY.getValue() - today.getDayOfWeek().getValue() + 7) % 7;
            LocalDate expiry = today.plusDays(daysUntilTue);
            // Walk back one trading day when Tuesday is a market holiday (NIFTY's
            // documented weekly-expiry-day slide rule).
            if (marketHolidayService != null && marketHolidayService.isHoliday(expiry)) {
                expiry = expiry.minusDays(1);
            }
            int yy = expiry.getYear() % 100;
            int mm = expiry.getMonthValue();
            int dd = expiry.getDayOfMonth();
            char monthCh;
            if (mm >= 1 && mm <= 9) monthCh = (char) ('0' + mm);
            else if (mm == 10) monthCh = 'O';
            else if (mm == 11) monthCh = 'N';
            else if (mm == 12) monthCh = 'D';
            else return null;
            return String.format("%02d%c%02d", yy, monthCh, dd);
        } catch (Exception e) {
            log.warn("[atm-selector] weekly expiry tag build failed: {}", e.getMessage());
            return null;
        }
    }
}
