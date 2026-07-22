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

    /** {@code NIFTY} + arbitrary expiry chars + strike (digits) + {@code CE|PE}. Used
     *  to lift the STRIKE + CE/PE off the Fyers symbol after we've stripped the
     *  {@code NSE:} prefix. Group 1 = strike; group 2 = CE/PE. */
    private static final Pattern FYERS_TAIL =
        Pattern.compile("NIFTY.*?(\\d+)(CE|PE)$");
    /** GDFL uses uppercase month abbreviations: {@code 28JUL26}. */
    private static final DateTimeFormatter GDFL_EXPIRY_FMT =
        DateTimeFormatter.ofPattern("ddMMMuu", Locale.ENGLISH);

    /** GDFL identifier → Fyers symbol, populated by {@link #fyersToGdfl} on translation
     *  so every tick routes back without re-parsing. */
    private final ConcurrentHashMap<String, String> gdflToFyersMap = new ConcurrentHashMap<>();

    /** Converts a Fyers option symbol to the GDFL contractwise identifier, using the
     *  expiry embedded in the Fyers symbol itself. Returns {@code null} on any parse
     *  failure. Also registers the reverse mapping so incoming ticks can be routed. */
    public String fyersToGdfl(String fyersSymbol) {
        if (fyersSymbol == null || fyersSymbol.isBlank()) return null;
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
