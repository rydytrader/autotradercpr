package com.rydytrader.autotrader.service.strategy;

import java.util.List;
import java.util.Map;

/**
 * Common contract every options-selling strategy implements. Kept after the multi-instance
 * Straddle/Strangle code was stripped so {@code AnalyticsService} and the portfolio
 * kill-switch can call generic methods on the singleton strategy without coupling.
 *
 * <p>OptionScalping is the only implementation; the interface is here to keep that boundary clean
 * and to ease future extension.
 */
public interface Strategy {

    /** Stable identifier (e.g. {@code "option-scalping"}) used as the {@code strategy_id} column on
     *  persisted trade and session rows. */
    String id();

    /** Human-readable name shown in the UI. */
    String displayName();

    /** One-line description shown under page titles. Default: empty string. */
    default String description() { return ""; }

    /** Current lifecycle state as a String (strategy-specific enum names). */
    String currentState();

    /** Manual squareoff — flatten all open legs and stop entries for the day. Returns true
     *  when there was something to close. */
    boolean forceClose(String reason);

    /** Manual recovery — flip in-memory state back to a safe pre-entry state so the next
     *  scheduler tick can re-evaluate entry conditions. Does not touch broker positions. */
    void resetToIdle(String reason);

    /** True when the operator has flipped the strategy on in Settings. Used by the
     *  portfolio kill switch to skip disabled strategies. Default true. */
    default boolean isEnabled() { return true; }

    /** Today's already-closed trades from in-memory state — used by the Analytics page live
     *  overlay so today's metrics reflect closes that haven't been persisted yet.
     *
     *  <p>Each map carries: {@code grossPnl}, {@code charges} (optional), {@code netPnl}
     *  (optional — defaults to gross when charges missing), {@code closedAtMillis},
     *  {@code closeReason}. Default empty. */
    default java.util.List<java.util.Map<String, Object>> todayClosedTrades() {
        return java.util.List.of();
    }

    /** Live net day P&L (realised + open MTM − charges). Used by the portfolio kill switch
     *  to sum exposures across strategies. Default 0. */
    default double liveNetPnlToday() { return 0; }

    /** Accrued + projected charges for today (brokerage + STT + GST + exchange + SEBI +
     *  stamp). Used by the analytics live overlay. Default 0. */
    default double liveChargesToday() { return 0; }

    /** Scheduler entry point — invoked on the slow loop (~5 s) by `OptionScalpingScheduler`. */
    default void tick() {}

    /** Fast-path check — invoked on the fast loop (~500 ms) for low-latency exits. */
    default void fastSlCheck() {}
}
