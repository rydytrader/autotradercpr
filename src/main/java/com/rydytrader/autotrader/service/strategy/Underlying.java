package com.rydytrader.autotrader.service.strategy;

import java.time.DayOfWeek;

/**
 * Options underlying — currently NIFTY (NSE) or SENSEX (BSE). Bundles the per-instrument
 * constants the straddle/strangle strategies use to pick strikes, size orders, and parse
 * expiry codes from Fyers option symbols.
 *
 * <p>Values as of 2026-09:
 * <ul>
 *   <li>NIFTY — {@code NSE:NIFTY50-INDEX}, lot 65, strike step 50, weekly expiry Tuesday,
 *       Fyers symbol prefix {@code NIFTY}.</li>
 *   <li>SENSEX — {@code BSE:SENSEX-INDEX}, lot 20, strike step 100, weekly expiry Tuesday,
 *       Fyers symbol prefix {@code SENSEX}.</li>
 * </ul>
 * The weekly expiry day drives {@code nextExpectedWeeklyExpiry()} in the strategies and
 * the last-day-of-month lookup in {@code parseExpiryFromSymbol}. If NSE / BSE shift days
 * again, update this enum in one place.
 */
public enum Underlying {
    NIFTY ("NSE:NIFTY50-INDEX", "NIFTY",  65, 50L,  DayOfWeek.TUESDAY),
    SENSEX("BSE:SENSEX-INDEX",  "SENSEX", 20, 100L, DayOfWeek.THURSDAY);

    private final String     indexSymbol;
    private final String     symbolPrefix;
    private final int        lotSize;
    private final long       strikeStep;
    private final DayOfWeek  expiryDayOfWeek;

    Underlying(String indexSymbol, String symbolPrefix, int lotSize, long strikeStep,
               DayOfWeek expiryDayOfWeek) {
        this.indexSymbol     = indexSymbol;
        this.symbolPrefix    = symbolPrefix;
        this.lotSize         = lotSize;
        this.strikeStep      = strikeStep;
        this.expiryDayOfWeek = expiryDayOfWeek;
    }

    public String     indexSymbol()     { return indexSymbol; }
    public String     symbolPrefix()    { return symbolPrefix; }
    public int        lotSize()         { return lotSize; }
    public long       strikeStep()      { return strikeStep; }
    public DayOfWeek  expiryDayOfWeek() { return expiryDayOfWeek; }

    /** Parses a settings-store string into an {@code Underlying}. Case-insensitive; falls
     *  back to NIFTY on blank / unknown values so existing instances (which don't yet have
     *  the setting persisted) keep behaving as before. */
    public static Underlying fromName(String name) {
        if (name == null) return NIFTY;
        try { return Underlying.valueOf(name.trim().toUpperCase()); }
        catch (IllegalArgumentException e) { return NIFTY; }
    }
}
