package com.rydytrader.autotrader.service.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Walks the NIFTY weekly option chain to pick strikes for the Camarilla strategy.
 *
 * <p>Two entry points:
 * <ul>
 *   <li>{@link #select(double)} — synthetic-futures ATM via put-call parity. Kept because it's
 *       still useful for any future strategy that wants the truly balanced strike (and because
 *       the chain fetch + parsing is shared with {@link #resolveStrikeAtLevel(double)}).</li>
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

    /**
     * Compute the synthetic-futures ATM for the given live NIFTY LTP. Returns {@code null}
     * when the option chain is unavailable or the spot-ATM strike is unquoted.
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

        double forward = spotAtm + (atmRow.ce - atmRow.pe);
        long   chosen  = Math.round(forward / (double) STRIKE_STEP) * STRIKE_STEP;

        ChainRow chosenRow = chain.get(chosen);
        if (chosenRow == null || chosenRow.ce <= 0 || chosenRow.pe <= 0) {
            log.warn("[atm-selector] synthetic strike {} unquoted — falling back to spot-ATM {}",
                chosen, spotAtm);
            chosen = spotAtm; chosenRow = atmRow;
        }

        return new AtmSelection(
            spotAtm,
            chosen,
            chosenRow.ce,
            chosenRow.pe,
            Math.abs(chosenRow.ce - chosenRow.pe),
            chosenRow.ceSym,
            chosenRow.peSym
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

    private static class ChainRow {
        double ce, pe;
        String ceSym = "", peSym = "";
    }

    private NavigableMap<Long, ChainRow> fetchChain() {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            JsonNode root = fyersClient.getOptionChain(NIFTY_SYMBOL, 30, auth);
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
