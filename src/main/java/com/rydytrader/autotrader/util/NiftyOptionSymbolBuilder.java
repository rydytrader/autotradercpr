package com.rydytrader.autotrader.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Builds Fyers-format NIFTY option symbols from ({@code expiry}, {@code strike},
 * {@code side}).
 *
 * <p>Fyers uses two shapes for NIFTY:
 * <ul>
 *   <li><b>Weekly</b> {@code NSE:NIFTY<yy><M><dd><strike><CE|PE>} — the "5 chars"
 *       between {@code NIFTY} and the strike are {@code <yy>} (2 digits) +
 *       {@code <M>} (1 char — {@code 1-9} for Jan-Sep, {@code O} for Oct, {@code N}
 *       for Nov, {@code D} for Dec) + {@code <dd>} (2 digits). Example: expiry
 *       2026-08-04 → tail {@code 26804}, strike 24500 CE → {@code NSE:NIFTY2680424500CE}.</li>
 *   <li><b>Monthly</b> {@code NSE:NIFTY<yy><MMM><strike><CE|PE>} — {@code <yy>} +
 *       3-letter uppercase month ({@code JAN}, {@code FEB}, …). Example: expiry
 *       2026-08-25 (last Tuesday of Aug) → tail {@code 26AUG}, strike 24500 CE →
 *       {@code NSE:NIFTY26AUG24500CE}.</li>
 * </ul>
 *
 * <p>Both tails are exactly 5 chars — that's the invariant
 * {@link com.rydytrader.autotrader.gdfl.GdflSymbolMapper#FYERS_TAIL} anchors on
 * ({@code .{5}}) and it's what {@link OptionSymbolUtils#parseExpiryFromSymbol}
 * expects on the reverse-parse path.
 *
 * <p>Weekly vs monthly is decided by whether the expiry is the last Tuesday of
 * its month — that's when Fyers switches to the {@code MMM} tail (weekly and
 * monthly expiries coincide on that Tuesday). Any earlier Tuesday in the month
 * gets the weekly tail.
 */
public final class NiftyOptionSymbolBuilder {

    private NiftyOptionSymbolBuilder() {}

    private static final DateTimeFormatter YY  = DateTimeFormatter.ofPattern("uu",  Locale.ENGLISH);
    private static final DateTimeFormatter DD  = DateTimeFormatter.ofPattern("dd",  Locale.ENGLISH);
    private static final DateTimeFormatter MMM = DateTimeFormatter.ofPattern("MMM", Locale.ENGLISH);

    /** Builds the Fyers-format symbol string ({@code NSE:NIFTY…CE|PE}). */
    public static String buildFyersSymbol(LocalDate expiry, long strike, String side) {
        if (expiry == null) throw new IllegalArgumentException("expiry is null");
        if (strike <= 0)    throw new IllegalArgumentException("strike must be positive");
        if (side == null)   throw new IllegalArgumentException("side is null");
        String s = side.trim().toUpperCase(Locale.ENGLISH);
        if (!"CE".equals(s) && !"PE".equals(s)) {
            throw new IllegalArgumentException("side must be CE or PE, got: " + side);
        }
        String yy = expiry.format(YY);
        String tail;
        if (isLastTuesdayOfMonth(expiry)) {
            // Monthly format: NIFTY<yy><MMM><strike><CE|PE>
            String mmm = expiry.format(MMM).toUpperCase(Locale.ENGLISH);
            tail = yy + mmm;
        } else {
            // Weekly format: NIFTY<yy><M><dd><strike><CE|PE>
            String monthChar = weeklyMonthChar(expiry.getMonthValue());
            String dd = expiry.format(DD);
            tail = yy + monthChar + dd;
        }
        return "NSE:NIFTY" + tail + strike + s;
    }

    /** Fyers weekly-format month char: {@code 1-9} for Jan-Sep, {@code O} for Oct,
     *  {@code N} for Nov, {@code D} for Dec. Matches
     *  {@link OptionSymbolUtils#parseExpiryFromSymbol}'s decoder. */
    private static String weeklyMonthChar(int month) {
        return switch (month) {
            case 1  -> "1"; case 2  -> "2"; case 3 -> "3"; case 4 -> "4"; case 5 -> "5";
            case 6  -> "6"; case 7  -> "7"; case 8 -> "8"; case 9 -> "9";
            case 10 -> "O"; case 11 -> "N"; case 12 -> "D";
            default -> throw new IllegalArgumentException("bad month: " + month);
        };
    }

    /** True when {@code d} is the last Tuesday of its calendar month. */
    private static boolean isLastTuesdayOfMonth(LocalDate d) {
        if (d.getDayOfWeek() != java.time.DayOfWeek.TUESDAY) return false;
        return d.plusDays(7).getMonthValue() != d.getMonthValue();
    }
}
