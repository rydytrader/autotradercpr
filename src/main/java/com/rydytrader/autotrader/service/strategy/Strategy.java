package com.rydytrader.autotrader.service.strategy;

import java.util.List;
import java.util.Map;

/**
 * Common contract every options-selling strategy implements. The dashboard, settings modal,
 * and analytics pages drive themselves from this abstraction — there's no place that hardcodes
 * a particular strategy. Adding a new strategy = one new class that implements this interface;
 * {@link StrategyRegistry} exposes it to the scheduler, controllers and UI.
 *
 * <p>Since 2026-10-27 the straddle / strangle instances are <b>manual multi-strike position
 * managers</b>: the operator opens short legs and hedge legs by hand (any number per side), and
 * the strategy automates only the per-leg SL, timed squareoff / expiry settlement and the
 * day-level books. Leg-level operations are addressed by the leg's {@code id} as surfaced in
 * the dashboard payload's {@code legs} list.
 */
public interface Strategy {

    /** Stable URL-safe identifier (kebab-case). Used as path segment, settings prefix, log
     *  prefix, order tag suffix, and session-row strategy_id column. Must be unique. */
    String id();

    /** Human-readable name shown in the UI (nav, settings tab, dashboard heading). */
    String displayName();

    /** One-line description shown under the dashboard title. Default: empty string. */
    default String description() { return ""; }

    /** Current lifecycle state as a String (strategy-specific enum names), e.g.
     *  ARMED / OPEN / DONE_FOR_DAY. */
    String currentState();

    /** Lightweight status — state, legs, expiry, key settings. Cheap to compute,
     *  safe to call frequently. Used by the strategy-sidebar icon active state. */
    Map<String, Object> getStatus();

    /** Full dashboard payload — status + per-leg LTP/MTM/SL + charges + cycle events.
     *  Polled every second by the per-strategy dashboard page. Must include a
     *  {@code dashboardShape} key telling the UI which template conditionals to apply. */
    Map<String, Object> getDashboard();

    /** Manual squareoff — flatten every open leg and park DONE_FOR_DAY. Returns true if there
     *  was something to close. */
    boolean forceClose(String reason);

    /** Close one leg by its id (short or hedge). Returns true when an open leg was found and a
     *  close order was placed. */
    default boolean closeOneLeg(String legId, String reason) { return false; }

    /** Operator "+ OPEN CE / PE": sell a fresh short leg on {@code side} ({@code "CE"} / {@code "PE"})
     *  at {@code symbol} (null/empty = ATM pick), with the Fyers {@code product} ({@code INTRADAY} /
     *  {@code MARGIN}), {@code lots} and the SL % of entry. Returns {@code "OK"} or a reason code
     *  ({@code REJECTED}, {@code ABORTED}, {@code PAST_SQUAREOFF}, {@code MARKET_CLOSED}, {@code INVALID},
     *  {@code UNSUPPORTED}). */
    default String openOneLeg(String side, String symbol, String product, int lots, double slPct) { return "UNSUPPORTED"; }

    /** Strike rows for the + STRADDLE / + STRANGLE dialogs: {@code {spot, atm, lotSize,
     *  rows[{strike, ceSymbol, ceLtp, ceMoneyness, peSymbol, peLtp, peMoneyness, isAtm}]}}. */
    default Map<String, Object> pairCandidates() { return Map.of(); }
    /** Same, for a chosen expiry (ISO date from the payload's {@code expiries}); null = nearest. */
    default Map<String, Object> pairCandidates(String expiry) { return pairCandidates(); }

    /** Sell a CE and a PE together (straddle when both strikes match, strangle otherwise).
     *  Returns {@code {ce: code, pe: code}} using the same codes as {@link #openOneLeg}. */
    default Map<String, String> openPair(String ceSymbol, String peSymbol, String product, int lots, double slPct) {
        return Map.of("ce", "UNSUPPORTED", "pe", "UNSUPPORTED");
    }

    /** Strike choices for the "+ OPEN" dialog: {@code {spot, defaultSymbol, lots, lotSize,
     *  candidates[{symbol, strike, ltp, moneyness, isDefault}]}}. Empty map when unsupported. */
    default Map<String, Object> openLegCandidates(String side) { return Map.of(); }
    default Map<String, Object> openLegCandidates(String side, String expiry) { return openLegCandidates(side); }

    /** Operator +/- on a leg's SL trigger: {@code dir} +1 raise one step, -1 lower, 0 clear the
     *  manual override. Returns {@code "OK"} or a reason code. */
    default String adjustLegSl(String legId, int dir) { return "UNSUPPORTED"; }

    /** Operator typed an SL price for the leg (inline edit). Returns {@code "OK"} or a reason code
     *  ({@code NOT_OPEN}, {@code NO_TRIGGER}, {@code INVALID}, {@code BELOW_LTP}). */
    default String setLegSl(String legId, double price) { return "UNSUPPORTED"; }

    /** Operator +/- on a leg's lot count: {@code dir} +1 adds one lot, -1 exits one lot
     *  (last lot = full close). Returns {@code "OK"}, {@code "CLOSED"} or a reason code. */
    default String adjustLegLots(String legId, int dir) { return "UNSUPPORTED"; }

    /** Operator COST button: toggle the leg's SL between its configured trigger and its entry
     *  premium (break-even). Returns {@code "OK"} or a reason code. */
    default String toggleLegCost(String legId) { return "UNSUPPORTED"; }

    /** Per-leg premium series for the row-click chart modal:
     *  {@code {legId, symbol, entry, slTrigger, samples[{t, v}]}}. */
    default Map<String, Object> legChart(String legId) { return Map.of(); }

    /** Manual hedge legs. {@code hedgeCandidates} → strike picker payload for {@code side};
     *  {@code openHedgeLeg} buys a wing (symbol null = ₹target pick); {@code closeHedgeLeg}
     *  sells the leg with that id back. */
    default Map<String, Object> hedgeCandidates(String side) { return Map.of(); }
    default Map<String, Object> hedgeCandidates(String side, String expiry) { return hedgeCandidates(side); }
    default String openHedgeLeg(String side, String symbol, String product, int lots) { return "UNSUPPORTED"; }
    default String closeHedgeLeg(String legId) { return "UNSUPPORTED"; }

    /** Re-open a closed leg: same side / kind / symbol. {@code product} empty, {@code lots} ≤ 0 or
     *  {@code slPct} < 0 fall back to the closed leg's own values. Same codes as {@link #openOneLeg},
     *  plus {@code ALREADY_OPEN} and {@code EXPIRED}. */
    default String reopenLeg(String legId, String product, int lots, double slPct) { return "UNSUPPORTED"; }

    /** Operator "clear trade data" hook — zero today's in-memory P&L / SL / charge counters
     *  so the live overlay doesn't re-synthesise today after the DB rows were deleted.
     *  Returns false (no-op) when legs are open or the strategy doesn't track day state. */
    default boolean resetTodayCounters() { return false; }

    /** Hard-stop the strategy for the day. Closes any open positions AND parks DONE_FOR_DAY
     *  regardless of current state. Used by the portfolio kill switch. */
    default void parkDoneForDay(String reason) {
        forceClose(reason);
    }

    /** Force in-memory state → ARMED. Returns {@code true} when the reset was applied,
     *  {@code false} when refused (typically because legs are still open at the broker —
     *  resetting would orphan them). */
    boolean resetToIdle(String reason);

    /** Schema for the settings modal. Each entry describes one configurable field. Field
     *  types: "time" (HH:mm), "int", "double", "percent", "rupees", "boolean", "select". */
    List<Map<String, Object>> getSettingsSchema();

    /** Current saved values for the fields described in {@link #getSettingsSchema()}. */
    Map<String, Object> getSettingsValues();

    /** Persist new values for the fields described in {@link #getSettingsSchema()}. */
    void saveSettings(Map<String, Object> values);

    /** Operator-facing short code shown as the sidebar label. Default = {@link #id()}. */
    default String shortCode() { return id(); }

    /** True when the operator has flipped the strategy on. Default true. */
    default boolean isEnabled() { return true; }

    /** Tiny icon for the left sidebar nav. Default = first letter of id() uppercase. */
    default String navIcon() {
        String id = id();
        return id == null || id.isEmpty() ? "?" : id.substring(0, 1).toUpperCase();
    }

    /** ISO yyyy-MM-dd of the option series this strategy is currently trading against, or
     *  the empty string when nothing has been resolved yet. */
    default String currentWeeklyExpiry() { return ""; }

    /** Underlying instrument the strategy is configured against (e.g. NIFTY / SENSEX). */
    default String underlyingName() { return ""; }

    /** Today's already-closed trades read from in-memory state — analytics live overlay. */
    default java.util.List<java.util.Map<String, Object>> todayClosedTrades() {
        return java.util.List.of();
    }

    /** Running net day P&L (realised + open MTM − charges). Used by the portfolio kill switch. */
    default double liveNetPnlToday() { return 0; }

    /** Total accrued / projected charges for today. */
    default double liveChargesToday() { return 0; }

    /** Strategy type discriminator — {@code "STRADDLE"} (default) or {@code "STRANGLE"}. */
    default String strategyType() { return "STRADDLE"; }

    /** Scheduler entry point — invoked every 5 s. */
    default void tick() {}

    /** Fast-path SL check — invoked every 500 ms. */
    default void fastSlCheck() {}

    /** 1-minute sampling for the per-leg premium charts. */
    default void sampleCombinedPremium() {}
}
