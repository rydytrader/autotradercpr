package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Walks the NIFTY weekly option chain to pick strikes for the Camarilla strategy.
 *
 * <p>Two entry points:
 * <ul>
 *   <li>{@link #select(double)} — spot-based ATM (NIFTY/50 rounded to nearest). Returns the
 *       strike's CE + PE symbols and LTPs. The synthetic-futures-via-parity variant was
 *       retired because the strategy short-sells CE and PE on independent triggers — they
 *       never fire at the same instant, so put-call premium balance offers no value over
 *       plain spot rounding. Spot ATM is simpler, matches retail convention, and avoids the
 *       intra-bar drift that put-call parity introduces.</li>
 *   <li>{@link #resolveStrikeAtLevel(double)} — Camarilla's workhorse: given an arbitrary price
 *       level (H3, H4, L3, L4 etc.), returns the nearest tradable strike along with its CE+PE
 *       symbols and current LTPs.</li>
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

    /** Resolved strike + per-leg symbols + per-leg LTPs at a Camarilla level. Returned by
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
     * symbols + LTPs. Used by the Camarilla strategy to translate a pivot level (H3, H4, L3,
     * L4, ...) into a tradable option pair. Returns {@code null} when the chain is empty or
     * no quoted strike exists.
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

    /** Public bulk-fetch entry point: returns the entire current chain as a NavigableMap
     *  keyed by strike. Cross-package callers (e.g. CamarillaService warm-up) use this when
     *  they want to walk MANY strikes from a single chain response instead of calling
     *  {@link #resolveStrikeAtLevel(double)} once per strike (which would re-fetch the
     *  chain every time). Returns an empty map when the chain is unavailable. */
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
