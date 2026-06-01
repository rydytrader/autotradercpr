package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Snapshot view of the current NIFTY weekly options chain — ATM ± N strikes — surfaced to
 * the navbar ⊞ modal. Wraps {@link FyersClientRouter#getOptionChain}, slices the rows
 * around ATM, and tags the in-window max CE OI / max PE OI strikes so the UI can highlight
 * the OI walls.
 */
@RestController
@RequestMapping("/api/option-chain")
public class OptionChainController {

    private static final Logger log = LoggerFactory.getLogger(OptionChainController.class);

    private static final String DEFAULT_SYMBOL = "NSE:NIFTY50-INDEX";
    private static final long   STRIKE_STEP    = 50;

    private final FyersClientRouter fyersClient;
    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;

    public OptionChainController(FyersClientRouter fyersClient,
                                 TokenStore tokenStore,
                                 FyersProperties fyersProperties) {
        this.fyersClient     = fyersClient;
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
    }

    @GetMapping
    public ResponseEntity<?> get(@RequestParam(defaultValue = DEFAULT_SYMBOL) String symbol,
                                 @RequestParam(defaultValue = "10")           int strikes) {
        if (!tokenStore.isTokenAvailable()) {
            return ResponseEntity.status(401).body(Map.of("error", "not_logged_in"));
        }
        if (strikes < 1)  strikes = 1;
        if (strikes > 25) strikes = 25;

        String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
        int fetchCount = Math.max(30, strikes * 2 + 5);

        JsonNode root;
        try {
            root = fyersClient.getOptionChain(symbol, fetchCount, auth);
        } catch (Exception e) {
            log.warn("[option-chain] fetch failed: {}", e.getMessage());
            return ResponseEntity.status(502).body(Map.of(
                "error", "fyers_unavailable",
                "message", e.getMessage() == null ? "" : e.getMessage()
            ));
        }

        if (root == null) {
            return ResponseEntity.status(502).body(Map.of("error", "empty_response"));
        }
        JsonNode data  = root.has("data") ? root.get("data") : null;
        JsonNode chain = data != null && data.has("optionsChain") ? data.get("optionsChain")
            : (root.has("optionsChain") ? root.get("optionsChain") : null);
        if (chain == null || !chain.isArray() || chain.size() == 0) {
            return ResponseEntity.status(502).body(Map.of("error", "empty_chain"));
        }

        double spot = extractSpot(chain, symbol);
        if (spot <= 0) {
            spot = extractSpotFallback(symbol, auth);
        }
        if (spot <= 0) {
            return ResponseEntity.status(502).body(Map.of("error", "spot_unresolved"));
        }
        long atm = Math.round(spot / (double) STRIKE_STEP) * STRIKE_STEP;

        NavigableMap<Long, Leg[]> byStrike = groupByStrike(chain);
        List<Map<String, Object>> outRows = sliceWindow(byStrike, atm, strikes);

        // Resistance = max CE OI on strikes ABOVE spot (call writers defending a ceiling).
        // Support    = max PE OI on strikes BELOW spot (put writers defending a floor).
        // CE OI at ITM strikes (below spot) and PE OI at ITM strikes (above spot) is mostly
        // delta-hedging inventory — not a wall, so we exclude those from the search.
        // Tiebreaker: closest to ATM wins, then highest OI.
        long peakCeOiAbove = 0, peakPeOiBelow = 0;
        for (Map<String, Object> r : outRows) {
            long strike = (long) r.get("strike");
            if (strike > atm) peakCeOiAbove = Math.max(peakCeOiAbove, legOi(r, "ce"));
            if (strike < atm) peakPeOiBelow = Math.max(peakPeOiBelow, legOi(r, "pe"));
        }
        List<Long> resistanceStrikes = pickDirectionalWalls(outRows, atm, "ce", peakCeOiAbove, true,  2);
        List<Long> supportStrikes    = pickDirectionalWalls(outRows, atm, "pe", peakPeOiBelow, false, 2);

        String expiry = resolveExpiry(outRows);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("underlying",   symbol);
        body.put("spot",         round2(spot));
        body.put("atmStrike",    atm);
        body.put("expiry",       expiry);
        body.put("asOf",         LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        body.put("maxCeOiStrikes", resistanceStrikes);
        body.put("maxPeOiStrikes", supportStrikes);
        body.put("rows",         outRows);
        return ResponseEntity.ok(body);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    /** The chain typically carries one row that represents the underlying itself — strike=0
     *  / option_type empty / symbol = the underlying. Its {@code ltp} is the spot price. */
    private double extractSpot(JsonNode chain, String underlyingSymbol) {
        for (JsonNode row : chain) {
            String sym = textField(row, "symbol");
            String optType = textField(row, "option_type", "optionType");
            double strike = doubleField(row, "strike_price", "strikePrice");
            if (sym.equalsIgnoreCase(underlyingSymbol)
                || (optType.isEmpty() && (strike == 0 || strike == -1))) {
                double ltp = doubleField(row, "ltp", "lp");
                if (ltp > 0) return ltp;
            }
        }
        return 0;
    }

    private double extractSpotFallback(String symbol, String auth) {
        try {
            JsonNode q = fyersClient.getQuotes(symbol, auth);
            if (q == null) return 0;
            JsonNode d = q.has("d") ? q.get("d") : null;
            if (d != null && d.isArray() && d.size() > 0) {
                JsonNode v = d.get(0).has("v") ? d.get(0).get("v") : null;
                if (v != null && v.has("lp")) return v.get("lp").asDouble(0);
            }
        } catch (Exception e) {
            log.warn("[option-chain] spot fallback /quotes failed: {}", e.getMessage());
        }
        return 0;
    }

    private NavigableMap<Long, Leg[]> groupByStrike(JsonNode chain) {
        NavigableMap<Long, Leg[]> out = new TreeMap<>();
        for (JsonNode row : chain) {
            double strikeD = doubleField(row, "strike_price", "strikePrice");
            String optType = textField(row, "option_type", "optionType");
            if (strikeD <= 0 || optType.isEmpty()) continue;
            long strike = Math.round(strikeD);
            Leg[] pair = out.computeIfAbsent(strike, k -> new Leg[2]);
            Leg leg = new Leg();
            leg.symbol   = textField(row, "symbol");
            leg.ltp      = doubleField(row, "ltp", "lp");
            leg.chgPct   = doubleField(row, "ltpchp", "chp", "change_percent");
            leg.volume   = longField(row, "volume", "vol", "tradedQty");
            leg.oi       = longField(row, "oi");
            leg.oiChange = longField(row, "oich", "oichange", "oi_change", "change_oi");
            leg.activity = classifyActivity(leg.chgPct, leg.oiChange);
            if ("CE".equalsIgnoreCase(optType)) pair[0] = leg;
            else if ("PE".equalsIgnoreCase(optType)) pair[1] = leg;
        }
        return out;
    }

    private List<Map<String, Object>> sliceWindow(NavigableMap<Long, Leg[]> byStrike,
                                                  long atm, int strikes) {
        List<Map<String, Object>> out = new ArrayList<>();
        long lo = atm - (long) strikes * STRIKE_STEP;
        long hi = atm + (long) strikes * STRIKE_STEP;
        for (long s = lo; s <= hi; s += STRIKE_STEP) {
            Leg[] pair = byStrike.get(s);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("strike", s);
            row.put("isAtm",  s == atm);
            row.put("ce",     legToMap(pair != null ? pair[0] : null));
            row.put("pe",     legToMap(pair != null ? pair[1] : null));
            out.add(row);
        }
        return out;
    }

    private Map<String, Object> legToMap(Leg leg) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (leg == null) {
            m.put("ltp", null); m.put("chgPct", null); m.put("volume", 0L); m.put("oi", 0L); m.put("oiChange", 0L); m.put("activity", ""); m.put("symbol", "");
        } else {
            m.put("ltp",      leg.ltp > 0 ? round2(leg.ltp) : null);
            m.put("chgPct",   leg.ltp > 0 ? round2(leg.chgPct) : null);
            m.put("volume",   leg.volume);
            m.put("oi",       leg.oi);
            m.put("oiChange", leg.oiChange);
            m.put("activity", leg.activity);
            m.put("symbol",   leg.symbol);
        }
        return m;
    }

    /** Classic price × OI flow read on a single option leg:
     *  <ul>
     *    <li><b>LB</b> Long Build-up — price up, OI up   → fresh longs entering</li>
     *    <li><b>SB</b> Short Build-up — price down, OI up → fresh shorts entering</li>
     *    <li><b>LU</b> Long Unwinding — price down, OI down → longs closing</li>
     *    <li><b>SC</b> Short Covering — price up, OI down  → shorts buying back</li>
     *  </ul>
     *  Returns {@code ""} when either input is flat (no signal). */
    private static String classifyActivity(double chgPct, long oiChange) {
        if (chgPct == 0 || oiChange == 0) return "";
        boolean priceUp = chgPct > 0;
        boolean oiUp    = oiChange > 0;
        if (priceUp  && oiUp)  return "LB";
        if (!priceUp && oiUp)  return "SB";
        if (!priceUp && !oiUp) return "LU";
        return "SC";
    }

    private static final double WALL_THRESHOLD = 0.5;

    /** Number of strike steps R2/S2 must sit within of R1/S1. Keeps the secondary wall
     *  visually tied to the primary one rather than picking a high-OI outlier far away. */
    private static final int R2_PROXIMITY_STRIKES = 4;

    /** Top-{@code topN} strikes on one side of ATM whose OI on {@code legKey} clears
     *  {@link #WALL_THRESHOLD} of the directional peak OI:
     *  <ul>
     *    <li><b>R1/S1</b> — closest qualifying strike to ATM (OI breaks ties).</li>
     *    <li><b>R2/S2 onwards</b> — highest OI among qualifying strikes within
     *        {@link #R2_PROXIMITY_STRIKES} steps of R1/S1 (closer to R1 breaks OI ties).</li>
     *  </ul> */
    private List<Long> pickDirectionalWalls(List<Map<String, Object>> rows, long atm,
                                            String legKey, long peakOi, boolean aboveAtm,
                                            int topN) {
        if (peakOi <= 0 || topN <= 0) return Collections.emptyList();
        long threshold = Math.max(1, (long) (peakOi * WALL_THRESHOLD));
        List<long[]> candidates = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            long strike = (long) r.get("strike");
            if (aboveAtm  && strike <= atm) continue;
            if (!aboveAtm && strike >= atm) continue;
            long oi = legOi(r, legKey);
            if (oi < threshold) continue;
            candidates.add(new long[]{strike, oi, Math.abs(strike - atm)});
        }
        if (candidates.isEmpty()) return Collections.emptyList();

        // R1 / S1 — closest to ATM, OI breaks ties.
        candidates.sort((a, b) -> {
            if (a[2] != b[2]) return Long.compare(a[2], b[2]);
            return Long.compare(b[1], a[1]);
        });
        List<Long> out = new ArrayList<>(topN);
        long[] firstRow = candidates.remove(0);
        long r1Strike = firstRow[0];
        out.add(r1Strike);
        if (topN <= 1) return out;

        // R2 / S2 onwards — further from ATM than R1 (next level out), within proximity, then
        // rank by OI desc (closer to R1 breaks OI ties).
        long proximityRange = (long) R2_PROXIMITY_STRIKES * STRIKE_STEP;
        List<long[]> near = new ArrayList<>();
        for (long[] c : candidates) {
            long s = c[0];
            if (aboveAtm  && s <= r1Strike) continue;  // R2 must sit above R1
            if (!aboveAtm && s >= r1Strike) continue;  // S2 must sit below S1
            if (Math.abs(s - r1Strike) > proximityRange) continue;
            near.add(c);
        }
        near.sort((a, b) -> {
            if (a[1] != b[1]) return Long.compare(b[1], a[1]);             // higher OI first
            return Long.compare(Math.abs(a[0] - r1Strike), Math.abs(b[0] - r1Strike)); // closer to R1
        });
        for (int i = 0; i < Math.min(topN - 1, near.size()); i++) {
            out.add(near.get(i)[0]);
        }
        return out;
    }

    /** Closest-to-{@code atm} strike on one side of ATM whose OI on {@code legKey} is at
     *  least {@link #WALL_THRESHOLD} of the directional peak OI. Used for support /
     *  resistance — pass {@code aboveAtm=true} to search strikes above spot (CE wall
     *  → resistance), {@code aboveAtm=false} for strikes below spot (PE wall → support). */
    private long pickDirectionalWall(List<Map<String, Object>> rows, long atm, String legKey,
                                     long peakOi, boolean aboveAtm) {
        if (peakOi <= 0) return 0;
        long threshold = Math.max(1, (long) (peakOi * WALL_THRESHOLD));
        long bestStrike = 0;
        long bestDist   = Long.MAX_VALUE;
        long bestOi     = 0;
        for (Map<String, Object> r : rows) {
            long strike = (long) r.get("strike");
            if (aboveAtm  && strike <= atm) continue;
            if (!aboveAtm && strike >= atm) continue;
            long oi = legOi(r, legKey);
            if (oi < threshold) continue;
            long dist = Math.abs(strike - atm);
            if (dist < bestDist || (dist == bestDist && oi > bestOi)) {
                bestDist = dist;
                bestOi   = oi;
                bestStrike = strike;
            }
        }
        return bestStrike;
    }

    /** Closest-to-{@code atm} strike whose OI on {@code legKey} is at least
     *  {@link #WALL_THRESHOLD} of the window's peak OI. Returns 0 if no row passes. */
    private long pickWall(List<Map<String, Object>> rows, long atm, String legKey, long peakOi) {
        if (peakOi <= 0) return 0;
        long threshold = Math.max(1, (long) (peakOi * WALL_THRESHOLD));
        long bestStrike = 0;
        long bestDist   = Long.MAX_VALUE;
        long bestOi     = 0;
        for (Map<String, Object> r : rows) {
            long oi = legOi(r, legKey);
            if (oi < threshold) continue;
            long strike = (long) r.get("strike");
            long dist   = Math.abs(strike - atm);
            // Closer wins; on ties, higher OI wins.
            if (dist < bestDist || (dist == bestDist && oi > bestOi)) {
                bestDist   = dist;
                bestOi     = oi;
                bestStrike = strike;
            }
        }
        return bestStrike;
    }

    @SuppressWarnings("unchecked")
    private long legOi(Map<String, Object> row, String legKey) {
        Object leg = row.get(legKey);
        if (!(leg instanceof Map)) return 0;
        Object oi = ((Map<String, Object>) leg).get("oi");
        return oi instanceof Number ? ((Number) oi).longValue() : 0;
    }

    @SuppressWarnings("unchecked")
    private String resolveExpiry(List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            for (String k : new String[]{"ce", "pe"}) {
                Object leg = row.get(k);
                if (!(leg instanceof Map)) continue;
                Object sym = ((Map<String, Object>) leg).get("symbol");
                if (sym instanceof String && !((String) sym).isEmpty()) {
                    String exp = parseExpiryFromSymbol((String) sym);
                    if (!exp.isEmpty()) return exp;
                }
            }
        }
        return "";
    }

    /** Mirrors {@code ShortStraddle.parseExpiryFromSymbol} — kept local so the controller
     *  doesn't reach across the strategy package for an 18-line helper. Format example:
     *  {@code NSE:NIFTY50M28FEB25C24850} → tail {@code 28FEB25...} → not the form used;
     *  the actual Fyers weekly form is {@code 25604} for 2026-06-04 (YY M-char DD). */
    static String parseExpiryFromSymbol(String fyersSymbol) {
        if (fyersSymbol == null) return "";
        try {
            int hash = fyersSymbol.indexOf("NIFTY");
            if (hash < 0) return "";
            String tail = fyersSymbol.substring(hash + 5);
            if (tail.length() < 5) return "";
            int yr = Integer.parseInt(tail.substring(0, 2));
            char monthCh = tail.charAt(2);
            int month;
            if (monthCh >= '1' && monthCh <= '9') month = monthCh - '0';
            else if (monthCh == 'O') month = 10;
            else if (monthCh == 'N') month = 11;
            else if (monthCh == 'D') month = 12;
            else return "";
            int day = Integer.parseInt(tail.substring(3, 5));
            return LocalDate.of(2000 + yr, month, day).toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static String textField(JsonNode row, String... keys) {
        for (String k : keys) {
            if (row.has(k) && !row.get(k).isNull()) return row.get(k).asText("");
        }
        return "";
    }

    private static double doubleField(JsonNode row, String... keys) {
        for (String k : keys) {
            if (row.has(k) && !row.get(k).isNull()) return row.get(k).asDouble(0);
        }
        return 0;
    }

    private static long longField(JsonNode row, String... keys) {
        for (String k : keys) {
            if (row.has(k) && !row.get(k).isNull()) return row.get(k).asLong(0);
        }
        return 0;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static class Leg {
        String symbol = "";
        double ltp;
        double chgPct;
        long   volume;
        long   oi;
        long   oiChange;
        String activity = "";
    }
}
