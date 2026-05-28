package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * One row per trading-day per-strategy session. Persisted on day rollover for historical
 * analytics. After the multi-strategy refactor, the row is uniquely identified by
 * {@code (strategyId, sessionDate)} — one row per strategy per date.
 *
 * <p>The unique constraint on {@code sessionDate} alone is still in place in the existing
 * schema (only one strategy was writing rows before). It will be replaced with a composite
 * unique on {@code (strategyId, sessionDate)} when the second strategy starts writing rows
 * (Phase 5 of the multi-strategy plan).
 */
@Entity
@Table(name = "straddle_sessions",
       uniqueConstraints = @UniqueConstraint(name = "uk_strategy_session_date",
                                              columnNames = {"strategy_id", "session_date"}))
public class StraddleSessionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Strategy that owned this session. SQL-level DEFAULT lets H2 add this column to a
     *  populated table without violating NOT NULL — existing rows inherit the default value
     *  immediately. New rows get the Java field initializer ("combined-sl-roll") which the
     *  concrete strategy overrides with its own id() when writing a row. */
    @Column(name = "strategy_id", nullable = false, length = 40,
            columnDefinition = "VARCHAR(40) DEFAULT 'combined-sl-roll' NOT NULL")
    private String strategyId = "combined-sl-roll";

    /** ISO yyyy-MM-dd. No longer unique on its own — multiple strategies can write a row for
     *  the same date. Composite uniqueness with strategyId enforced via {@link Table}. */
    @Column(name = "session_date", nullable = false, length = 10)
    private String sessionDate;

    @Column(nullable = false)
    private int entries;        // number of initial entries (almost always 1)

    @Column(nullable = false)
    private int rolls;          // rollCount achieved during the day

    @Column(nullable = false, length = 24)
    private String finalState;  // OPEN / MAX_ROLLS_HOLD / DONE_FOR_DAY at squareoff

    @Column
    private double niftyOpen;
    @Column
    private double niftyHigh;
    @Column
    private double niftyLow;
    @Column
    private double niftyClose;

    /** Sum of premium collected on every SELL fill of the day. */
    @Column
    private double premiumCollected;
    /** Sum of premium paid back on every BUY (close) fill of the day. */
    @Column
    private double premiumPaidBack;
    /** Gross P&L = premiumCollected - premiumPaidBack. Computed on day-finalisation. */
    @Column
    private double grossPnl;
    /** Estimated charges (brokerage + STT + GST + exchange + SEBI + stamp). */
    @Column
    private double charges;
    /** Net P&L = grossPnl - charges. */
    @Column
    private double netPnl;

    @Column(nullable = false)
    private long createdAt;

    public StraddleSessionEntity() {}

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getStrategyId() { return strategyId; }
    public void setStrategyId(String strategyId) { this.strategyId = strategyId; }
    public String getSessionDate() { return sessionDate; }
    public void setSessionDate(String sessionDate) { this.sessionDate = sessionDate; }
    public int getEntries() { return entries; }
    public void setEntries(int entries) { this.entries = entries; }
    public int getRolls() { return rolls; }
    public void setRolls(int rolls) { this.rolls = rolls; }
    public String getFinalState() { return finalState; }
    public void setFinalState(String finalState) { this.finalState = finalState; }
    public double getNiftyOpen() { return niftyOpen; }
    public void setNiftyOpen(double niftyOpen) { this.niftyOpen = niftyOpen; }
    public double getNiftyHigh() { return niftyHigh; }
    public void setNiftyHigh(double niftyHigh) { this.niftyHigh = niftyHigh; }
    public double getNiftyLow() { return niftyLow; }
    public void setNiftyLow(double niftyLow) { this.niftyLow = niftyLow; }
    public double getNiftyClose() { return niftyClose; }
    public void setNiftyClose(double niftyClose) { this.niftyClose = niftyClose; }
    public double getPremiumCollected() { return premiumCollected; }
    public void setPremiumCollected(double premiumCollected) { this.premiumCollected = premiumCollected; }
    public double getPremiumPaidBack() { return premiumPaidBack; }
    public void setPremiumPaidBack(double premiumPaidBack) { this.premiumPaidBack = premiumPaidBack; }
    public double getGrossPnl() { return grossPnl; }
    public void setGrossPnl(double grossPnl) { this.grossPnl = grossPnl; }
    public double getCharges() { return charges; }
    public void setCharges(double charges) { this.charges = charges; }
    public double getNetPnl() { return netPnl; }
    public void setNetPnl(double netPnl) { this.netPnl = netPnl; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
}
