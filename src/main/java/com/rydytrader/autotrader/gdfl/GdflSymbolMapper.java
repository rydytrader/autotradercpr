package com.rydytrader.autotrader.gdfl;

import com.rydytrader.autotrader.controller.OptionChainController;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates between Fyers's option-symbol format and GDFL's contractwise
 * {@code InstrumentIdentifier} format.
 *
 * <p>Fyers's symbol string encodes the expiry (monthly {@code NIFTY26JUL} or weekly
 * {@code NIFTY2670824200CE}). We decode it via
 * {@link OptionChainController#parseExpiryFromSymbol} — the same parser the option-chain
 * UI uses — and reformat as GDFL's {@code DDMMMYY} tag:
 *
 * <ul>
 *   <li>Fyers {@code NSE:NIFTY26JUL24200CE} → expiry {@code 2026-07-28} (last Tuesday of
 *       July 2026) → GDFL {@code NIFTY28JUL2624200CE}</li>
 *   <li>Fyers {@code NSE:NIFTY2670824200CE} → expiry {@code 2026-07-08} → GDFL
 *       {@code NIFTY08JUL2624200CE}</li>
 * </ul>
 *
 * <p>Nothing here is operator-maintained; the expiry comes straight from the Fyers
 * symbol Fyers already chose for us. The reverse mapping (GDFL → Fyers) is stored on
 * first translate() call so incoming ticks route back without re-parsing.
 */
@Component
public class GdflSymbolMapper {

    /** {@code NIFTY} + expiry chars (ALWAYS exactly 5 — monthly YYMMM like
     *  {@code 26JUL} or weekly YYMDD like {@code 26804}) + strike (digits) +
     *  {@code CE|PE}. The fixed-length {@code .{5}} anchor is what stops
     *  {@code \d+} from greedy-swallowing the numeric expiry digits into the
     *  strike on weekly-format symbols. Group 1 = strike; group 2 = CE/PE. */
    private static final Pattern FYERS_TAIL =
        Pattern.compile("NIFTY.{5}(\\d+)(CE|PE)$");
    /** GDFL uses uppercase month abbreviations: {@code 28JUL26}. */
    private static final DateTimeFormatter GDFL_EXPIRY_FMT =
        DateTimeFormatter.ofPattern("ddMMMuu", Locale.ENGLISH);

    /** Internal Fyers-style name for the NIFTY current-month futures. Since we're not
     *  placing futures orders (we only trade options), this doesn't need to match any
     *  real Fyers identifier — it's just a stable key for the tick fanout. GDFL side
     *  is the continuous {@link #GDFL_NIFTY_FUTURES} which auto-rolls at expiry
     *  server-side, so we don't compute expiry dates or handle rollovers. */
    public static final String FYERS_NIFTY_FUTURES = "NSE:NIFTY-I-FUT";

    /** GDFL "continuous current-month" identifier for NIFTY futures. GDFL points
     *  this at whichever contract is current — flips server-side on expiry day. */
    public static final String GDFL_NIFTY_FUTURES  = "NIFTY-I";

    /** GDFL identifier → Fyers symbol, populated by {@link #fyersToGdfl} on translation
     *  so every tick routes back without re-parsing. Seeded with the NIFTY futures
     *  pair at construction so a tick can route back even before the first
     *  fyersToGdfl call has run for it. */
    private final ConcurrentHashMap<String, String> gdflToFyersMap = new ConcurrentHashMap<>();

    public GdflSymbolMapper() {
        gdflToFyersMap.put(GDFL_NIFTY_FUTURES, FYERS_NIFTY_FUTURES);
    }

    /** Converts a Fyers option symbol to the GDFL contractwise identifier, using the
     *  expiry embedded in the Fyers symbol itself. Returns {@code null} on any parse
     *  failure. Also registers the reverse mapping so incoming ticks can be routed. */
    public String fyersToGdfl(String fyersSymbol) {
        if (fyersSymbol == null || fyersSymbol.isBlank()) return null;
        // Fast path — NIFTY current-month futures uses the continuous GDFL identifier,
        // NOT an expiry-encoded string. Bypass the option-symbol parser below.
        if (FYERS_NIFTY_FUTURES.equals(fyersSymbol)) {
            gdflToFyersMap.put(GDFL_NIFTY_FUTURES, fyersSymbol);
            return GDFL_NIFTY_FUTURES;
        }
        // 1. Expiry from the Fyers symbol (yyyy-MM-dd or empty on parse failure).
        String isoExpiry = OptionChainController.parseExpiryFromSymbol(fyersSymbol);
        if (isoExpiry == null || isoExpiry.isEmpty()) return null;
        LocalDate expiry;
        try { expiry = LocalDate.parse(isoExpiry); }
        catch (Exception e) { return null; }
        String gdflExpiryTag = expiry.format(GDFL_EXPIRY_FMT).toUpperCase(Locale.ENGLISH);

        // 2. Strike + CE/PE from the Fyers symbol (strip NSE: prefix first).
        String s = fyersSymbol.startsWith("NSE:") ? fyersSymbol.substring(4) : fyersSymbol;
        Matcher m = FYERS_TAIL.matcher(s);
        if (!m.find()) return null;
        String strike = m.group(1);
        String side   = m.group(2);

        // 3. Assemble in GDFL's day-first format.
        String gdfl = "NIFTY" + gdflExpiryTag + strike + side;
        gdflToFyersMap.put(gdfl, fyersSymbol);
        return gdfl;
    }

    /** GDFL identifier → Fyers symbol. Returns {@code null} if unregistered. */
    public String gdflToFyers(String gdflIdentifier) {
        if (gdflIdentifier == null) return null;
        return gdflToFyersMap.get(gdflIdentifier);
    }

    /** Clears the reverse-mapping table on day rollover. */
    public void clear() {
        gdflToFyersMap.clear();
    }
}
