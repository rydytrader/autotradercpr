package com.rydytrader.autotrader.util;

import java.time.LocalDate;
import java.util.Map;

/**
 * Parsers for Fyers's NIFTY option-symbol formats. Extracted from the retired
 * {@code OptionChainController} so downstream consumers (currently
 * {@code GdflSymbolMapper}) don't drag a controller import into non-web code.
 */
public final class OptionSymbolUtils {

    private OptionSymbolUtils() {}

    private static final Map<String, Integer> MONTH_ABBREVS = Map.ofEntries(
        Map.entry("JAN", 1),  Map.entry("FEB", 2),  Map.entry("MAR", 3),
        Map.entry("APR", 4),  Map.entry("MAY", 5),  Map.entry("JUN", 6),
        Map.entry("JUL", 7),  Map.entry("AUG", 8),  Map.entry("SEP", 9),
        Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12)
    );

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
     *  known month abbrev), else fall through to single-char weekly format. */
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

    /** Last Tuesday of the given month — NIFTY's current weekly + monthly expiry day. */
    private static LocalDate lastTuesdayOfMonth(int year, int month) {
        LocalDate d = LocalDate.of(year, month, 1)
            .withDayOfMonth(LocalDate.of(year, month, 1).lengthOfMonth());
        while (d.getDayOfWeek() != java.time.DayOfWeek.TUESDAY) {
            d = d.minusDays(1);
        }
        return d;
    }
}
