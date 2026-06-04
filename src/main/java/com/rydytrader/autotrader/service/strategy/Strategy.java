package com.rydytrader.autotrader.service.strategy;

import java.util.List;
import java.util.Map;

/**
 * Common contract every options-selling strategy implements. The dashboard, settings modal,
 * and analytics pages drive themselves from this abstraction — there's no place that hardcodes
 * "rolling straddle" anymore. Adding a new strategy = one new {@code @Service} class that
 * implements this interface; Spring auto-discovers it via {@link StrategyRegistry}.
 */
public interface Strategy {

    /** Stable URL-safe identifier (kebab-case). Used as path segment, settings prefix, log
     *  prefix, order tag suffix, and session-row strategy_id column. Must be unique. */
    String id();

    /** Human-readable name shown in the UI (nav, settings tab, dashboard heading). */
    String displayName();

    /** One-line description shown under the dashboard title — explains what the strategy does
     *  at a glance. Default: empty string (no subtitle). Override per concrete strategy. */
    default String description() { return ""; }

    /** Current lifecycle state as a String (strategy-specific enum names). Examples:
     *  IDLE / OPEN / WAITING_TO_ROLL / DONE_FOR_DAY, or OPEN_BOTH / OPEN_CE_ONLY / etc. */
    String currentState();

    /** Lightweight status — state, symbols, qty, expiry, key settings. Cheap to compute,
     *  safe to call frequently. Used by the strategy-sidebar icon active state. */
    Map<String, Object> getStatus();

    /** Full dashboard payload — status + leg LTPs + MTM + charges + samples + cycle events.
     *  Polled every 5s by the per-strategy dashboard page. Returned shape MAY differ between
     *  strategies, but must include a {@code dashboardShape} key telling the UI which template
     *  conditionals to apply. */
    Map<String, Object> getDashboard();

    /** Manual squareoff — flatten all open legs and park DONE_FOR_DAY. Returns true if there
     *  was something to close. */
    boolean forceClose(String reason);

    /** Close just one leg of a multi-leg position. {@code leg} is either {@code "CE"} or
     *  {@code "PE"} (case-insensitive). Returns true when an open leg was found and a close
     *  order was placed. Default returns false for strategies that don't have separately
     *  closeable legs — only ShortStraddle overrides. */
    default boolean closeOneLeg(String leg, String reason) { return false; }

    /** Manual same-day restart after the strategy has reached its DONE_FOR_DAY terminal
     *  state. Drives the dashboard's {@code + NEW STRADDLE} button. Returns {@code "OK"}
     *  when an entry was placed, a gate name (e.g. {@code "MAX_LOSS_HIT"}) when one of the
     *  scheduler's preconditions blocked the restart, or {@code "UNSUPPORTED"} for strategies
     *  that don't support multi-cycle days. Only ShortStraddle overrides. */
    default String restartFromDoneForDay(String reason) { return "UNSUPPORTED"; }

    /** Hard-stop the strategy for the day. Closes any open positions AND parks DONE_FOR_DAY
     *  regardless of current state — so a strategy that's IDLE (waiting for entry time)
     *  won't fire new entries later in the session. Used by the portfolio kill switch.
     *
     *  <p>Default falls back to {@link #forceClose} which only handles strategies with open
     *  positions; concrete strategies must override to guarantee the DONE_FOR_DAY transition. */
    default void parkDoneForDay(String reason) {
        forceClose(reason);
    }

    /** Manual recovery — flip in-memory state back to IDLE so the next scheduler tick can
     *  re-evaluate entry conditions. Does not touch broker positions. */
    void resetToIdle(String reason);

    /** Schema for the settings modal. Each entry describes one configurable field that this
     *  strategy reads. The UI renders the form dynamically and POSTs back via the generic
     *  settings endpoint. Field types: "time" (HH:mm), "int", "double", "percent",
     *  "rupees", "boolean". */
    List<Map<String, Object>> getSettingsSchema();

    /** Current saved values for the fields described in {@link #getSettingsSchema()}. Keys
     *  match the schema entries' {@code key}. Returned values match the schema {@code type}
     *  (String / int / double / bool). The UI populates the form from this map. */
    Map<String, Object> getSettingsValues();

    /** Persist new values for the fields described in {@link #getSettingsSchema()}. The UI
     *  posts a map of {schemaKey → submittedValue} (typically String); the strategy parses
     *  each per its schema type and saves to wherever it reads from (legacy direct field or
     *  the generic strategyConfigs map). */
    void saveSettings(Map<String, Object> values);

    /** Operator-facing short code (e.g. {@code "9:20"}) shown as the sidebar label and on
     *  per-instance pages. Default = {@link #id()}; concrete instances return the entity's
     *  {@code shortCode} field directly. */
    default String shortCode() { return id(); }

    /** True when the operator has flipped the strategy on (Straddles tab → Enable). The
     *  sidebar and {@code /api/strategies} response filter on this so disabled instances
     *  don't take up space — they're still queryable via {@code /api/straddles} for the
     *  Straddles tab itself. Default true so non-runtime strategies don't accidentally
     *  hide; concrete instances override to read their runtime flag. */
    default boolean isEnabled() { return true; }

    /** Tiny icon for the left sidebar nav. Default = first letter of id() uppercase.
     *  Concrete strategies may override to return an emoji or symbol character. */
    default String navIcon() {
        String id = id();
        return id == null || id.isEmpty() ? "?" : id.substring(0, 1).toUpperCase();
    }

    /** ISO yyyy-MM-dd of the option series this strategy is currently trading against, or
     *  the empty string when nothing has been resolved yet (pre-entry, before option chain
     *  fetched). Used by analytics to scope the "current expiry" period filter. */
    default String currentWeeklyExpiry() { return ""; }

    /** Today's already-closed straddles read from in-memory state — used by the Analytics page
     *  live overlay so today's metrics reflect closes that haven't been persisted to the
     *  {@code straddle_trades} table yet (leg-sl persists only when state reaches DONE_FOR_DAY).
     *
     *  <p>Each map should carry: {@code grossPnl}, {@code charges} (optional), {@code netPnl}
     *  (optional — defaults to gross when charges missing), {@code closedAtMillis}, {@code closeReason}.
     *  Default: empty list. */
    default java.util.List<java.util.Map<String, Object>> todayClosedTrades() {
        return java.util.List.of();
    }

    /** Lightweight accessor for this strategy's running net day P&L (realised + open MTM −
     *  charges). Used by the portfolio-wide kill switch which sums across all strategies on
     *  every tick. Default 0 — strategies with positions override. */
    default double liveNetPnlToday() { return 0; }

    /** This strategy's total accrued / projected charges for today (brokerage + STT + GST +
     *  exchange + SEBI + stamp on every leg that has hit the broker, including the still-open
     *  leg's projected SL-exit charges if any). Used by the analytics live overlay so the
     *  per-day Charges + Gross numbers for today match what the strategy's dashboard shows.
     *  Default 0 — strategies without positions today report no charges. */
    default double liveChargesToday() { return 0; }
}
