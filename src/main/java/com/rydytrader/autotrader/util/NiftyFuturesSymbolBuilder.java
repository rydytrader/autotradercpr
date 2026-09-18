package com.rydytrader.autotrader.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Builds Fyers-format NIFTY monthly futures symbols from the expiry date.
 *
 * <p>Fyers format: {@code NSE:NIFTY<yy><MMM>FUT} — {@code <yy>} (2 digits) +
 * 3-letter uppercase month + {@code FUT}. Example: expiry 2026-09-30 (last
 * Tuesday of Sep 2026) → {@code NSE:NIFTY26SEPFUT}.
 */
public final class NiftyFuturesSymbolBuilder {

    private NiftyFuturesSymbolBuilder() {}

    private static final DateTimeFormatter YY  = DateTimeFormatter.ofPattern("uu",  Locale.ENGLISH);
    private static final DateTimeFormatter MMM = DateTimeFormatter.ofPattern("MMM", Locale.ENGLISH);

    public static String buildFyersSymbol(LocalDate expiry) {
        if (expiry == null) throw new IllegalArgumentException("expiry is null");
        String yy  = expiry.format(YY);
        String mmm = expiry.format(MMM).toUpperCase(Locale.ENGLISH);
        return "NSE:NIFTY" + yy + mmm + "FUT";
    }
}
