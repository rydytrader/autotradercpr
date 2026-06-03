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
 * Picks the truly balanced ATM strike for a NIFTY straddle.
 *
 * <p>Naïvely rounding spot to the nearest strike step yields asymmetric premiums when spot
 * sits between strikes — the resulting CE and PE LTPs can be 50+ points apart, which is not
 * a delta-neutral straddle. This selector uses two independent methods and reports whether
 * they agree:
 *
 * <ol>
 *   <li><b>Premium-balance</b> — scan ±N strikes around spot-ATM, pick the strike with the
 *       smallest |CE − PE|. Purely empirical; vulnerable to wide quotes on individual rows.</li>
 *   <li><b>Synthetic-futures (put-call parity)</b> — F = K + (CE − PE) at spot-ATM, then
 *       round F to the nearest strike step. Robust to single-strike noise but assumes parity.</li>
 * </ol>
 *
 * <p>When both methods land on the same strike (or within one step) we say they <b>agree</b>
 * and that strike is taken. When they disagree the caller is told and decides — schedule a
 * retry, defer to the operator, or accept the synthetic answer as the canonical choice.
 */
@Service
public class BalancedAtmSelector {

    private static final Logger log = LoggerFactory.getLogger(BalancedAtmSelector.class);
    private static final String NIFTY_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_STEP  = 50L;
    /** Window scanned around the spot-rounded ATM (in strike steps). 2 covers ±100 points,
     *  which is wider than any single tick of NIFTY can move between read and order. */
    private static final int    SCAN_WINDOW  = 2;

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

    /** Full result of one selection attempt — exposes every intermediate so the UI / logs
     *  can explain to the operator why the chosen strike differs from the naïve spot-ATM. */
    public record AtmSelection(
        long    spotAtm,            // Math.round(spot/50)*50 — the naïve baseline
        long    premiumBalanceAtm,  // Approach 1
        long    syntheticAtm,       // Approach 2
        long    chosenAtm,          // syntheticAtm when agreement; syntheticAtm anyway on disagreement
        boolean agree,              // |Approach1 − Approach2| ≤ STRIKE_STEP
        double  ceLtpAtChosen,
        double  peLtpAtChosen,
        double  premiumGapAtChosen, // |CE − PE| at chosenAtm
        String  ceSymbolAtChosen,
        String  peSymbolAtChosen,
        String  diagnostic          // human one-liner — empty when agree
    ) {}

    /**
     * Build one selection from the current chain. {@code niftyLtp} is provided rather than
     * read inside so the caller can pass either the live ticker LTP or a cached snapshot
     * (e.g. for a UI preview that lags the WS by a fraction of a second).
     *
     * <p>Returns {@code null} when the chain can't be fetched / parsed (network error,
     * empty payload, no ATM strike present). Caller treats null the same as "skip entry".
     */
    public AtmSelection select(double niftyLtp) {
        if (niftyLtp <= 0) return null;
        long spotAtm = Math.round(niftyLtp / STRIKE_STEP) * STRIKE_STEP;

        NavigableMap<Long, ChainRow> chain = fetchChain();
        if (chain == null || chain.isEmpty()) return null;

        ChainRow atmRow = chain.get(spotAtm);
        if (atmRow == null || atmRow.ce <= 0 || atmRow.pe <= 0) {
            log.warn("[atm-selector] spot-ATM strike {} missing or unquoted in chain — skipping selection",
                spotAtm);
            return null;
        }

        // ── Approach 1 — premium-balance scan ────────────────────────────────
        long   balancedAtm = spotAtm;
        double bestGap     = Math.abs(atmRow.ce - atmRow.pe);
        for (long k = spotAtm - SCAN_WINDOW * STRIKE_STEP;
                  k <= spotAtm + SCAN_WINDOW * STRIKE_STEP;
                  k += STRIKE_STEP) {
            ChainRow r = chain.get(k);
            if (r == null || r.ce <= 0 || r.pe <= 0) continue;
            double gap = Math.abs(r.ce - r.pe);
            if (gap < bestGap) { bestGap = gap; balancedAtm = k; }
        }

        // ── Approach 2 — synthetic-futures / put-call parity ─────────────────
        double forward     = spotAtm + (atmRow.ce - atmRow.pe);
        long   syntheticAtm = Math.round(forward / (double) STRIKE_STEP) * STRIKE_STEP;

        // ── Agreement ────────────────────────────────────────────────────────
        boolean agree = Math.abs(balancedAtm - syntheticAtm) <= STRIKE_STEP;

        // ── Chosen strike — synthetic-futures is the primary (parity-based, robust to
        // single noisy quotes). On disagreement the caller's retry / confirm logic
        // gates whether to actually USE this number.
        long chosenAtm = syntheticAtm;
        ChainRow chosenRow = chain.get(chosenAtm);
        if (chosenRow == null) { chosenAtm = spotAtm; chosenRow = atmRow; }

        String diagnostic = agree ? "" :
            "balanced=" + balancedAtm + " synthetic=" + syntheticAtm
            + " (gap " + Math.round(Math.abs(balancedAtm - syntheticAtm)) + " pts)";

        return new AtmSelection(
            spotAtm,
            balancedAtm,
            syntheticAtm,
            chosenAtm,
            agree,
            chosenRow.ce,
            chosenRow.pe,
            Math.abs(chosenRow.ce - chosenRow.pe),
            chosenRow.ceSym,
            chosenRow.peSym,
            diagnostic
        );
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

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
