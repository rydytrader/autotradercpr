package com.rydytrader.autotrader.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.util.BlackScholes;
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
    private static final long   NIFTY_STRIKE_STEP = 50;

    /** Strike interval for the given index spot symbol. NIFTY only after
     *  BankNifty was retired. Kept as a lookup for signature stability. */
    private static long strikeStepFor(String symbol) {
        return NIFTY_STRIKE_STEP;
    }

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

        long strikeStep = strikeStepFor(symbol);
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
        long atm = Math.round(spot / (double) strikeStep) * strikeStep;

        NavigableMap<Long, Leg[]> byStrike = groupByStrike(chain);

        // Greeks: Fyers' chain endpoint doesn't ship greeks for NIFTY options. Compute delta
        // ourselves via Black-Scholes — invert the LTP to an implied vol, then plug back into
        // the BSM delta formula. Needs spot + time-to-expiry. Skips silently if we can't
        // resolve those; the UI shows "—" for the affected rows.
        double yearsToExpiry = resolveYearsToExpiry(byStrike);
        if (yearsToExpiry > 0) fillDeltas(byStrike, spot, yearsToExpiry);

        // Synthetic-futures ATM (put-call parity) was used by the straddle bot to pick a
        // strike that accounts for forward bias; the ATM VWAP strategy trades the plain
        // spot-rounded ATM, so the parity pill just confuses the chain view. Disabled —
        // syntheticAtm=0 makes every row's isSyntheticAtm=false, so the ATM pill never
        // renders. The gold band on the spot-ATM row stays (driven by isAtm against atm).
        long syntheticAtm = 0;

        List<Map<String, Object>> outRows = sliceWindow(byStrike, atm, strikes, syntheticAtm, strikeStep);

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
        List<Long> resistanceStrikes = pickDirectionalWalls(outRows, atm, "ce", peakCeOiAbove, true,  2, strikeStep);
        List<Long> supportStrikes    = pickDirectionalWalls(outRows, atm, "pe", peakPeOiBelow, false, 2, strikeStep);

        String expiry = resolveExpiry(outRows);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("underlying",        symbol);
        body.put("spot",              round2(spot));
        body.put("atmStrike",         atm);           // naïve spot-rounded ATM — slice + gold band anchor here
        body.put("syntheticAtmStrike", syntheticAtm); // retired — always 0; field kept for response-shape compat
        body.put("expiry",            expiry);
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
            leg.delta    = extractDelta(row);
            if ("CE".equalsIgnoreCase(optType)) pair[0] = leg;
            else if ("PE".equalsIgnoreCase(optType)) pair[1] = leg;
        }
        return out;
    }

    private List<Map<String, Object>> sliceWindow(NavigableMap<Long, Leg[]> byStrike,
                                                  long atm, int strikes, long syntheticAtm, long strikeStep) {
        List<Map<String, Object>> out = new ArrayList<>();
        long lo = atm - (long) strikes * strikeStep;
        long hi = atm + (long) strikes * strikeStep;
        for (long s = lo; s <= hi; s += strikeStep) {
            Leg[] pair = byStrike.get(s);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("strike",          s);
            row.put("isAtm",           s == atm);
            row.put("isSyntheticAtm",  syntheticAtm > 0 && s == syntheticAtm);
            row.put("ce",              legToMap(pair != null ? pair[0] : null));
            row.put("pe",              legToMap(pair != null ? pair[1] : null));
            out.add(row);
        }
        return out;
    }

    private Map<String, Object> legToMap(Leg leg) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (leg == null) {
            m.put("ltp", null); m.put("delta", null); m.put("chgPct", null); m.put("volume", 0L); m.put("oi", 0L); m.put("oiChange", 0L); m.put("activity", ""); m.put("symbol", "");
        } else {
            m.put("ltp",      leg.ltp > 0 ? round2(leg.ltp) : null);
            m.put("delta",    leg.delta == null ? null : round4(leg.delta));
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
                                            int topN, long strikeStep) {
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
        long proximityRange = (long) R2_PROXIMITY_STRIKES * strikeStep;
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

    /** Parse the expiry ISO date out of a Fyers option symbol. Handles both formats
     *  Fyers uses for NIFTY options:
     *  <ul>
     *    <li><b>Weekly</b> {@code NSE:NIFTYYYMDDXXXXXCE} — 1-char month
     *        (1-9 for Jan-Sep, O/N/D for Oct/Nov/Dec), 2-char day.</li>
     *    <li><b>Monthly</b> {@code NSE:NIFTYYYMMMXXXXXCE} — 3-letter month abbrev
     *        (JAN, FEB, ..., DEC). Day defaults to the last Tuesday of that month
     *        (NIFTY's current weekly+monthly expiry day).</li>
     *  </ul>
     *  Disambiguation: try the 3-letter monthly format first (chars [2..5] match a
     *  known month abbrev), else fall through to single-char weekly format.
     *  Public so other controllers (manual terminal, etc.) share the same parser. */
    public static String parseExpiryFromSymbol(String fyersSymbol) {
        if (fyersSymbol == null) return "";
        try {
            int hash = fyersSymbol.indexOf("NIFTY");
            if (hash < 0) return "";
            String tail = fyersSymbol.substring(hash + 5);
            if (tail.length() < 5) return "";
            int yr = Integer.parseInt(tail.substring(0, 2));
            int yearFull = 2000 + yr;
            // Monthly format first (3-letter month abbrev).
            if (tail.length() >= 5) {
                String maybeMonth = tail.substring(2, 5);
                Integer monthIdx = MONTH_ABBREVS.get(maybeMonth);
                if (monthIdx != null) {
                    return lastTuesdayOfMonth(yearFull, monthIdx).toString();
                }
            }
            // Weekly format fallback (1-char month + 2-char day).
            char monthCh = tail.charAt(2);
            int month;
            if (monthCh >= '1' && monthCh <= '9') month = monthCh - '0';
            else if (monthCh == 'O') month = 10;
            else if (monthCh == 'N') month = 11;
            else if (monthCh == 'D') month = 12;
            else return "";
            int day = Integer.parseInt(tail.substring(3, 5));
            return LocalDate.of(yearFull, month, day).toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static final Map<String, Integer> MONTH_ABBREVS = Map.ofEntries(
        Map.entry("JAN", 1),  Map.entry("FEB", 2),  Map.entry("MAR", 3),
        Map.entry("APR", 4),  Map.entry("MAY", 5),  Map.entry("JUN", 6),
        Map.entry("JUL", 7),  Map.entry("AUG", 8),  Map.entry("SEP", 9),
        Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12)
    );

    /** Last Tuesday of the given month — NIFTY's current weekly + monthly expiry day. */
    private static LocalDate lastTuesdayOfMonth(int year, int month) {
        LocalDate d = LocalDate.of(year, month, 1)
            .withDayOfMonth(LocalDate.of(year, month, 1).lengthOfMonth());
        while (d.getDayOfWeek() != java.time.DayOfWeek.TUESDAY) {
            d = d.minusDays(1);
        }
        return d;
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

    private static double round4(double v) {
        return Math.round(v * 10000.0) / 10000.0;
    }

    // ── Greeks (Fyers doesn't ship them — compute via shared BSM utility) ──────

    /** Years to expiry, computed from any quoted symbol in the chain. Returns 0 when no
     *  symbol parses cleanly. Uses a 365-day calendar (NIFTY's convention). Logs a sample
     *  unparseable symbol so the operator can extend {@link #parseExpiryFromSymbol} when
     *  Fyers introduces a new format. */
    private double resolveYearsToExpiry(NavigableMap<Long, Leg[]> byStrike) {
        String sampleUnparsed = null;
        for (Leg[] pair : byStrike.values()) {
            for (Leg l : pair) {
                if (l == null || l.symbol == null || l.symbol.isEmpty()) continue;
                String exp = parseExpiryFromSymbol(l.symbol);
                if (exp.isEmpty()) {
                    if (sampleUnparsed == null) sampleUnparsed = l.symbol;
                    continue;
                }
                try {
                    LocalDate expDate = LocalDate.parse(exp);
                    LocalDate today   = LocalDate.now();
                    long days = java.time.temporal.ChronoUnit.DAYS.between(today, expDate);
                    if (days <= 0) days = 1;   // intraday on expiry day — keep T > 0 so BSM is well-defined
                    return days / 365.0;
                } catch (Exception ignored) {}
            }
        }
        if (sampleUnparsed != null) {
            log.warn("[option-chain] expiry parse failed for all chain symbols (sample: {}). "
                + "Delta column will show — for every row. Extend parseExpiryFromSymbol to handle this format.",
                sampleUnparsed);
        }
        return 0;
    }

    /** Fill {@code leg.delta} on every quoted leg in the chain by inverting LTP → implied
     *  vol via bisection, then plugging that IV into the BSM delta formula. Legs with no
     *  quote (ltp = 0) are skipped — they stay {@code null} and render "—". */
    private static void fillDeltas(NavigableMap<Long, Leg[]> byStrike, double spot, double tYears) {
        double r = BlackScholes.DEFAULT_RISK_FREE_RATE;
        for (Map.Entry<Long, Leg[]> e : byStrike.entrySet()) {
            long strike = e.getKey();
            Leg[] pair = e.getValue();
            if (pair == null) continue;
            if (pair[0] != null && pair[0].ltp > 0) {
                double iv = BlackScholes.impliedVol(spot, strike, tYears, r, pair[0].ltp, true);
                if (iv > 0) pair[0].delta = BlackScholes.delta(spot, strike, tYears, r, iv, true);
            }
            if (pair[1] != null && pair[1].ltp > 0) {
                double iv = BlackScholes.impliedVol(spot, strike, tYears, r, pair[1].ltp, false);
                if (iv > 0) pair[1].delta = BlackScholes.delta(spot, strike, tYears, r, iv, false);
            }
        }
    }

    /** Best-effort delta extraction from a Fyers chain row. Fyers' v3 option chain reports
     *  greeks in several shapes across symbols / versions:
     *  <ul>
     *    <li>Inline keys: {@code delta} / {@code option_delta} / {@code optdelta}</li>
     *    <li>Nested objects: {@code option_greeks.delta} / {@code greeks.delta}</li>
     *  </ul>
     *  Returns {@code null} when no candidate is present, so the UI shows "—". */
    private static Double extractDelta(JsonNode row) {
        for (String k : new String[]{"delta", "option_delta", "optdelta"}) {
            if (row.has(k) && !row.get(k).isNull()) {
                double v = row.get(k).asDouble(Double.NaN);
                if (!Double.isNaN(v)) return v;
            }
        }
        for (String k : new String[]{"option_greeks", "greeks", "optionGreeks"}) {
            if (row.has(k) && row.get(k).isObject()) {
                JsonNode g = row.get(k);
                if (g.has("delta") && !g.get("delta").isNull()) {
                    double v = g.get("delta").asDouble(Double.NaN);
                    if (!Double.isNaN(v)) return v;
                }
            }
        }
        return null;
    }

    private static class Leg {
        String symbol = "";
        double ltp;
        double chgPct;
        long   volume;
        long   oi;
        long   oiChange;
        String activity = "";
        Double delta;   // nullable — null when Fyers doesn't include greeks for this row
    }
}
