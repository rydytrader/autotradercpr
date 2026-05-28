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

    /** Current lifecycle state as a String (strategy-specific enum names). Examples:
     *  IDLE / OPEN / WAITING_TO_ROLL / DONE_FOR_DAY, or OPEN_BOTH / OPEN_CE_ONLY / etc. */
    String currentState();

    /** Lightweight status — state, symbols, qty, expiry, key settings. Cheap to compute,
     *  safe to call frequently. Used by the strategy-sidebar icon active state. */
    Map<String, Object> getStatus();

    /** Full dashboard payload — status + leg LTPs + MTM + charges + samples + roll events.
     *  Polled every 5s by the per-strategy dashboard page. Returned shape MAY differ between
     *  strategies, but must include a {@code dashboardShape} key telling the UI which template
     *  conditionals to apply (e.g. "combined-sl-roll" vs "leg-sl"). */
    Map<String, Object> getDashboard();

    /** Manual squareoff — flatten all open legs and park DONE_FOR_DAY. Returns true if there
     *  was something to close. */
    boolean forceClose(String reason);

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
     *  {@code straddle_trades} table yet (combined-sl-roll persists on every close;
     *  leg-sl persists only when state reaches DONE_FOR_DAY).
     *
     *  <p>Each map should carry: {@code grossPnl}, {@code charges} (optional), {@code netPnl}
     *  (optional — defaults to gross when charges missing), {@code closedAtMillis}, {@code closeReason}.
     *  Default: empty list. */
    default java.util.List<java.util.Map<String, Object>> todayClosedTrades() {
        return java.util.List.of();
    }
}
