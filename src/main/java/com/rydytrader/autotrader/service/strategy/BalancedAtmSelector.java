package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
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
 */
@Service
public class BalancedAtmSelector {

    private static final Logger log = LoggerFactory.getLogger(BalancedAtmSelector.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_STEP  = 50L;

    private final FyersClientRouter fyersClient;
    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;

    public BalancedAtmSelector(FyersClientRouter fyersClient,
                               TokenStore tokenStore,
                               FyersProperties fyersProperties) {
        this.fyersClient     = fyersClient;
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
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
        if (atmRow == null || atmRow.ce <= 0 || atmRow.pe <= 0) {
            log.warn("[atm-selector] spot-ATM strike {} missing or unquoted in chain — skipping",
                spotAtm);
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
     * Pick the strike closest to {@code level} that has BOTH CE and PE quoted, and return its
     * symbols + LTPs. Used by the ATM VWAP strategy to translate the first-bar-close NIFTY
     * price into the ATM option pair. Returns {@code null} when the chain is empty or no
     * quoted strike exists.
     */
    public StrikeAtLevel resolveStrikeAtLevel(double level) {
        if (level <= 0) return null;
        NavigableMap<Long, ChainRow> chain = fetchChain();
        if (chain == null || chain.isEmpty()) return null;

        long rounded = Math.round(level / (double) STRIKE_STEP) * STRIKE_STEP;
        long chosen = nearestStrikeWithBothLegs(chain, rounded);
        if (chosen <= 0) {
            log.warn("[atm-selector] resolveStrikeAtLevel({}): no quoted strike found", level);
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
     * <p>If the computed floor/ceil strike isn't quoted in the current chain,
     * {@link #nearestStrikeWithBothLegs} walks outward to the nearest liquid strike — same
     * safe-fallback behaviour as {@link #resolveStrikeAtLevel}.
     *
     * <p>For an unrecognised side, falls back to {@link #resolveStrikeAtLevel}.
     */
    public StrikeAtLevel resolveOtmStrikeAtLevel(double level, String side) {
        return resolveOtmStrikeAtLevel(NIFTY_SYMBOL, STRIKE_STEP, level, side);
    }

    /** Per-instrument overload — takes the spot symbol + strike step so the
     *  same routine works for both NIFTY (STRIKE_STEP=50, weekly chain) and
     *  BANKNIFTY (STRIKE_STEP=100, monthly chain). Fyers returns the nearest
     *  expiry when the timestamp param is empty, which naturally picks
     *  BankNifty's monthly chain. */
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
            log.warn("[atm-selector] resolveOtmStrikeAtLevel({}, {}, {}): no quoted strike found",
                spotSymbol, level, side);
            return null;
        }
        ChainRow row = chain.get(chosen);
        return new StrikeAtLevel(level, chosen, row.ceSym, row.peSym, row.ce, row.pe);
    }

    /** Walk outward from {@code requested} until we find a strike whose CE AND PE are both
     *  quoted (non-empty symbol, positive LTP). Returns 0 when none in the chain qualifies. */
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
        return row.ce > 0 && row.pe > 0
            && row.ceSym != null && !row.ceSym.isEmpty()
            && row.peSym != null && !row.peSym.isEmpty();
    }

    /** Pick the strike whose LTP on the requested side is closest to {@code targetPremium}.
     *  Used by StrangleAdjust to find "the CE strike currently priced at ~₹50" (and
     *  the mirrored PE side). Skips strikes whose side is unquoted (0 LTP or blank symbol).
     *  Returns null when the chain is empty or nothing quotes on the requested side. */
    public StrikeAtLevel resolveStrikeByTargetPremium(String spotSymbol, String side, double targetPremium) {
        if (spotSymbol == null || spotSymbol.isBlank()) return null;
        if (side == null || (!"CE".equalsIgnoreCase(side) && !"PE".equalsIgnoreCase(side))) return null;
        if (targetPremium <= 0) return null;
        NavigableMap<Long, ChainRow> chain = fetchChain(spotSymbol);
        if (chain == null || chain.isEmpty()) return null;
        boolean isCe = "CE".equalsIgnoreCase(side);
        long bestStrike = 0;
        double bestDiff = Double.MAX_VALUE;
        ChainRow bestRow = null;
        for (Map.Entry<Long, ChainRow> e : chain.entrySet()) {
            ChainRow r = e.getValue();
            if (r == null) continue;
            double ltp = isCe ? r.ce : r.pe;
            String sym = isCe ? r.ceSym : r.peSym;
            if (ltp <= 0 || sym == null || sym.isEmpty()) continue;
            double diff = Math.abs(ltp - targetPremium);
            if (diff < bestDiff) {
                bestDiff = diff;
                bestStrike = e.getKey();
                bestRow = r;
            }
        }
        if (bestRow == null) {
            log.warn("[atm-selector] resolveStrikeByTargetPremium({}, {}, {}): no quoted strike found",
                spotSymbol, side, targetPremium);
            return null;
        }
        return new StrikeAtLevel(targetPremium, bestStrike,
            bestRow.ceSym == null ? "" : bestRow.ceSym,
            bestRow.peSym == null ? "" : bestRow.peSym,
            bestRow.ce, bestRow.pe);
    }

    /** Resolve the strike N strike-steps away from {@code fromStrike} in the given direction
     *  ("UP" = deeper OTM CE / higher strike, "DOWN" = deeper OTM PE / lower strike). Walks
     *  outward if the exact target isn't quoted. Returns null when the chain is empty or no
     *  strike in the direction qualifies. */
    public StrikeAtLevel resolveStrikeNAway(String spotSymbol, long strikeStep,
                                             long fromStrike, int nSteps, String direction) {
        if (spotSymbol == null || spotSymbol.isBlank()) return null;
        if (nSteps <= 0 || strikeStep <= 0) return null;
        boolean up = "UP".equalsIgnoreCase(direction);
        long target = up ? (fromStrike + (long) nSteps * strikeStep)
                         : (fromStrike - (long) nSteps * strikeStep);
        NavigableMap<Long, ChainRow> chain = fetchChain(spotSymbol);
        if (chain == null || chain.isEmpty()) return null;
        long chosen = nearestStrikeWithBothLegs(chain, target);
        if (chosen <= 0) {
            log.warn("[atm-selector] resolveStrikeNAway({}, {}, {}, {}): no quoted strike found",
                spotSymbol, fromStrike, nSteps, direction);
            return null;
        }
        ChainRow row = chain.get(chosen);
        return new StrikeAtLevel(target, chosen, row.ceSym, row.peSym, row.ce, row.pe);
    }

    /** Public bulk-fetch entry point: returns the entire current chain as a NavigableMap
     *  keyed by strike. Cross-package callers use this when they want to walk MANY strikes
     *  from a single chain response instead of calling {@link #resolveStrikeAtLevel(double)}
     *  once per strike (which would re-fetch the chain every time). Returns an empty map
     *  when the chain is unavailable. */
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

    /** Dynamic next-weekly-expiry lookup for the given spot symbol. Pulls a fresh
     *  Fyers option chain (defaults to the nearest expiry when {@code expiryTs}
     *  is empty), takes any strike's symbol, and parses the expiry date out of it.
     *  Handles NIFTY, SENSEX (BSE), and any other index whose option symbols
     *  encode the underlying name followed by {@code YY{M/MMM}DD}.
     *
     *  <p>Returns null on any failure (chain unavailable, no parseable symbol,
     *  bad token). Caller should handle null by skipping DTE-gated entries.
     *
     *  <p>No caching — the strategy calls this at most a handful of times per
     *  session (entryIfDue on the slow scheduler tick). If the pattern changes,
     *  wrap in a per-day cache. */
    public LocalDate resolveNextExpiry(String spotSymbol) {
        if (spotSymbol == null || spotSymbol.isBlank()) return null;
        NavigableMap<Long, ChainRow> chain = fetchChain(spotSymbol);
        if (chain == null || chain.isEmpty()) return null;
        String underlying = spotSymbol.contains("SENSEX") ? "SENSEX"
                          : spotSymbol.contains("NIFTY")  ? "NIFTY"
                          : null;
        if (underlying == null) return null;
        for (ChainRow r : chain.values()) {
            String sym = (r.ceSym != null && !r.ceSym.isEmpty()) ? r.ceSym : r.peSym;
            if (sym == null || sym.isEmpty()) continue;
            LocalDate d = parseExpiryDate(sym, underlying);
            if (d != null) return d;
        }
        return null;
    }

    /** Symbol-agnostic Fyers expiry parser. Handles both weekly ({@code YY{1-9,O,N,D}DD})
     *  and monthly ({@code YY{JAN..DEC}}) formats. For monthly, returns the last
     *  Tuesday of the month (SEBI's current weekly + monthly expiry weekday for both
     *  NIFTY and SENSEX). Returns null on parse failure. */
    static LocalDate parseExpiryDate(String sym, String underlying) {
        if (sym == null || underlying == null) return null;
        int idx = sym.indexOf(underlying);
        if (idx < 0) return null;
        String tail = sym.substring(idx + underlying.length());
        if (tail.length() < 5) return null;
        try {
            int yr = Integer.parseInt(tail.substring(0, 2));
            int yearFull = 2000 + yr;
            // Monthly format first (3-letter month abbrev).
            String maybeMonth = tail.substring(2, Math.min(5, tail.length()));
            Integer monthIdx = MONTH_ABBREVS.get(maybeMonth);
            if (monthIdx != null) return lastTuesdayOfMonth(yearFull, monthIdx);
            // Weekly — 1-char month + 2-char day.
            char mc = tail.charAt(2);
            int m;
            if (mc >= '1' && mc <= '9') m = mc - '0';
            else if (mc == 'O') m = 10;
            else if (mc == 'N') m = 11;
            else if (mc == 'D') m = 12;
            else return null;
            int day = Integer.parseInt(tail.substring(3, 5));
            return LocalDate.of(yearFull, m, day);
        } catch (Exception e) {
            return null;
        }
    }

    private static final Map<String, Integer> MONTH_ABBREVS = Map.ofEntries(
        Map.entry("JAN", 1),  Map.entry("FEB", 2),  Map.entry("MAR", 3),
        Map.entry("APR", 4),  Map.entry("MAY", 5),  Map.entry("JUN", 6),
        Map.entry("JUL", 7),  Map.entry("AUG", 8),  Map.entry("SEP", 9),
        Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12)
    );

    private static LocalDate lastTuesdayOfMonth(int year, int month) {
        LocalDate d = LocalDate.of(year, month, 1).withDayOfMonth(
            LocalDate.of(year, month, 1).lengthOfMonth());
        while (d.getDayOfWeek() != java.time.DayOfWeek.TUESDAY) d = d.minusDays(1);
        return d;
    }

    private NavigableMap<Long, ChainRow> fetchChain() {
        return fetchChain(NIFTY_SYMBOL);
    }

    /** Per-symbol chain fetch. Empty {@code expiryTs} → Fyers returns nearest
     *  expiry, which naturally maps to NIFTY weekly OR BANKNIFTY monthly
     *  depending on the symbol. */
    private NavigableMap<Long, ChainRow> fetchChain(String spotSymbol) {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getOptionChain(spotSymbol, 30, auth);
            if (root == null) return null;
            JsonNode data = root.has("data") ? root.get("data") : null;
            JsonNode chain = data != null && data.has("optionsChain") ? data.get("optionsChain")
                : (root.has("optionsChain") ? root.get("optionsChain") : null);
            if (chain == null || !chain.isArray()) return null;

            NavigableMap<Long, ChainRow> byStrike = new TreeMap<>();
            for (JsonNode row : chain) {
                double strikeD = row.has("strike_price") ? row.get("strike_price").asDouble()
                    : row.has("strikePrice") ? row.get("strikePrice").asDouble() : 0;
                if (strikeD <= 0) continue;
                String optType = row.has("option_type") ? row.get("option_type").asText()
                    : row.has("optionType") ? row.get("optionType").asText() : "";
                if (optType.isEmpty()) continue;
                String sym = row.has("symbol") ? row.get("symbol").asText() : "";
                double ltp = row.has("ltp") ? row.get("ltp").asDouble()
                    : row.has("lp") ? row.get("lp").asDouble() : 0;
                long strike = Math.round(strikeD);
                ChainRow r = byStrike.computeIfAbsent(strike, k -> new ChainRow());
                if ("CE".equalsIgnoreCase(optType)) { r.ce = ltp; r.ceSym = sym; }
                else if ("PE".equalsIgnoreCase(optType)) { r.pe = ltp; r.peSym = sym; }
            }
            return byStrike;
        } catch (Exception e) {
            log.warn("[atm-selector] Chain fetch failed: {}", e.getMessage());
            return null;
        }
    }
}
