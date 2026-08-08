package com.rydytrader.autotrader.util;

import com.rydytrader.autotrader.service.MarketHolidayService;

import java.time.DayOfWeek;
import java.time.LocalDate;

/**
 * Resolves the current NIFTY weekly-options expiry for a given trading day.
 *
 * <p>NIFTY weekly options expire on <b>Tuesday</b>. Rules:
 * <ol>
 *   <li>Find this ISO-week's Tuesday.</li>
 *   <li>If {@code today} is strictly after that Tuesday, roll forward to next Tuesday.</li>
 *   <li>If that Tuesday is an NSE trading holiday, walk BACKWARD one day at a time
 *       until a trading day is found (NSE convention: on-holiday expiries move to the
 *       previous trading day).</li>
 * </ol>
 *
 * <p>{@link MarketHolidayService} may be null (defensive — the resolver silently
 * skips the holiday walk-back in that case and returns the raw Tuesday).
 */
public final class NiftyExpiryResolver {

    private NiftyExpiryResolver() {}

    public static LocalDate currentWeeklyExpiry(LocalDate today, MarketHolidayService holidays) {
        if (today == null) throw new IllegalArgumentException("today is null");
        // 1. Walk forward from today until we hit Tuesday (0..6 days).
        LocalDate tue = today;
        while (tue.getDayOfWeek() != DayOfWeek.TUESDAY) {
            tue = tue.plusDays(1);
        }
        // 2. If today IS a Tuesday, tue == today — that's this week's expiry.
        //    If today is Wednesday..Monday, tue is the next Tuesday which is correct.
        //    Nothing to roll forward here — the walk above already lands on the
        //    upcoming Tuesday, never one in the past.
        // 3. Holiday walk-back — if the Tuesday itself is a holiday, use the prior
        //    trading day (NSE convention). Bounded by 10 iterations as a safety net
        //    against a mis-configured holiday list.
        if (holidays != null) {
            int guard = 10;
            while (guard-- > 0 && holidays.isHoliday(tue)) {
                tue = tue.minusDays(1);
            }
        }
        return tue;
    }
}
