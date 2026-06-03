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
