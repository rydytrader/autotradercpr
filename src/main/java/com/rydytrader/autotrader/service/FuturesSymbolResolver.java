package com.rydytrader.autotrader.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.util.Locale;

/**
 * Resolves the Fyers symbol for the current near-month NIFTY futures contract.
 *
 * <p>Fyers naming convention: {@code NSE:NIFTY{YY}{MMM}FUT} where {@code MMM} is
 * the 3-letter month code (JAN, FEB, MAR, …) and {@code YY} is the 2-digit year.
 * Example: {@code NSE:NIFTY26JUNFUT} for June 2026.
 *
 * <p>NSE monthly futures expire on the last Thursday of each month. This resolver
 * picks "current month" before that expiry date and "next month" on/after it,
 * so the symbol always refers to the actively-traded near contract.
 *
 * <p><b>Holiday adjustment is NOT implemented in this first cut.</b> When the
 * last Thursday is an NSE holiday, the real expiry rolls to the prior Wednesday
 * and a manual override may be needed. Operator can flag a wrong symbol via the
 * event log; future revisions can consult {@code MarketHolidayService} here.
 */
@Service
public class FuturesSymbolResolver {

    private static final Logger log = LoggerFactory.getLogger(FuturesSymbolResolver.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    /** Near-month NIFTY future as of {@code asOf} IST date. */
    public String nearMonthNiftyFuture(LocalDate asOf) {
        LocalDate target = asOf.isAfter(lastThursdayOf(asOf.getYear(), asOf.getMonthValue()))
            ? asOf.plusMonths(1)
            : asOf;
        String yy  = String.format("%02d", target.getYear() % 100);
        String mmm = target.getMonth().getDisplayName(TextStyle.SHORT, Locale.ENGLISH).toUpperCase();
        String sym = "NSE:NIFTY" + yy + mmm + "FUT";
        log.debug("[FuturesSymbolResolver] {} → {}", asOf, sym);
        return sym;
    }

    /** Today's near-month NIFTY future (IST-based). */
    public String nearMonthNiftyFuture() {
        return nearMonthNiftyFuture(LocalDate.now(IST));
    }

    /** Last Thursday of the given year+month. */
    static LocalDate lastThursdayOf(int year, int month) {
        LocalDate last = LocalDate.of(year, month, 1)
            .withDayOfMonth(LocalDate.of(year, month, 1).lengthOfMonth());
        while (last.getDayOfWeek() != DayOfWeek.THURSDAY) last = last.minusDays(1);
        return last;
    }
}
