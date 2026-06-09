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
 * Picks the truly balanced ATM strike for a NIFTY straddle using put-call parity.
 *
 * <p>Naïvely rounding spot to the nearest strike step yields asymmetric premiums when spot
 * sits between strikes — the resulting CE and PE LTPs can be 50+ points apart, which is not
 * a delta-neutral straddle. Put-call parity gives the synthetic forward:
 *
 * <pre>F = K + (CE − PE)  at the spot-ATM strike K</pre>
 *
 * <p>Rounding F to the nearest strike step yields the truly balanced ATM. This single-method
 * selection is robust on liquid weekly NIFTY — parity holds tightly, and using only the
 * spot-ATM quote makes us immune to wide bid-ask on adjacent strikes.
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
     *  for transparency on the dashboard so the operator can see how far the parity-based
     *  pick moved from "obvious". {@code chosenAtm} is what the bot will actually trade. */
    public record AtmSelection(
        long    spotAtm,
        long    chosenAtm,
        double  ceLtpAtChosen,
        double  peLtpAtChosen,
        double  premiumGapAtChosen,
        String  ceSymbolAtChosen,
        String  peSymbolAtChosen
    ) {}

    /**
     * Compute the synthetic-futures ATM for the given live NIFTY LTP. {@code niftyLtp} is
     * passed in (rather than read internally) so callers can hand off a cached snapshot
     * during preview polls without a re-read.
     *
     * <p>Returns {@code null} when the option chain is unavailable (network error, empty
     * payload, missing spot-ATM row, unquoted ATM strike). Caller treats null as "skip".
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

        // Put-call parity:  F = K + (CE − PE)   →   round to strike step.
        double forward = spotAtm + (atmRow.ce - atmRow.pe);
        long   chosen  = Math.round(forward / (double) STRIKE_STEP) * STRIKE_STEP;

        ChainRow chosenRow = chain.get(chosen);
        if (chosenRow == null || chosenRow.ce <= 0 || chosenRow.pe <= 0) {
            // Selector landed on a strike with no quotes — fall back to spot-ATM so we still
            // have a tradeable pair, but log the anomaly.
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

    /** Result of {@link #resolveStrikeSymbols(long, long)} — Fyers CE/PE symbols and current
     *  LTPs at the requested strikes. Strangle entry uses this to look up the OTM legs once
     *  the ATM has been chosen by {@link #select(double)}. */
    public record StrikeSymbols(
        long   callStrike,
        long   putStrike,
        String ceSymbol,
        String peSymbol,
        double ceLtp,
        double peLtp,
        long   resolvedCallStrike,   // == callStrike when found; nearest available when not
        long   resolvedPutStrike     // == putStrike  when found; nearest available when not
    ) {}

    /** Fetch the option chain once and look up CE+PE symbols at the requested call/put
     *  strikes — used by ShortStrangle for its OTM legs. When the exact requested strike
     *  is not present in the chain (typical for far-OTM in thin sessions), falls back to
     *  the nearest available strike on the relevant side; {@link StrikeSymbols#resolvedCallStrike}
     *  and {@link StrikeSymbols#resolvedPutStrike} carry the actual strike used. Returns
     *  {@code null} if the chain can't be fetched. */
    public StrikeSymbols resolveStrikeSymbols(long callStrike, long putStrike) {
        NavigableMap<Long, ChainRow> chain = fetchChain();
        if (chain == null || chain.isEmpty()) return null;

        long resolvedCall = nearestStrikeWithLeg(chain, callStrike, true);
        long resolvedPut  = nearestStrikeWithLeg(chain, putStrike,  false);
        if (resolvedCall <= 0 || resolvedPut <= 0) {
            log.warn("[atm-selector] Could not resolve strike symbols: requested call={} put={}, "
                + "resolved call={} put={}", callStrike, putStrike, resolvedCall, resolvedPut);
            return null;
        }
        ChainRow callRow = chain.get(resolvedCall);
        ChainRow putRow  = chain.get(resolvedPut);
        return new StrikeSymbols(
            callStrike, putStrike,
            callRow.ceSym, putRow.peSym,
            callRow.ce,    putRow.pe,
            resolvedCall,  resolvedPut
        );
    }

    /** Closest strike to {@code requested} that has a non-empty symbol on the requested
     *  leg side. Scans outward in both directions through the chain map. */
    private long nearestStrikeWithLeg(NavigableMap<Long, ChainRow> chain, long requested, boolean isCall) {
        if (chain.containsKey(requested)) {
            ChainRow row = chain.get(requested);
            String sym = isCall ? row.ceSym : row.peSym;
            if (sym != null && !sym.isEmpty()) return requested;
        }
        // Walk outward in both directions, choosing the nearer side that has a quoted symbol.
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
            ChainRow row = chain.get(candidate);
            String sym = isCall ? row.ceSym : row.peSym;
            if (sym != null && !sym.isEmpty()) return candidate;
        }
        return 0;
    }

    // ── Internal ──────────────────────────────────────────────────────────────

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
