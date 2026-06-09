package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * One row per trading-day per-strategy session. Persisted on day rollover for historical
 * analytics. Holds rows for both short straddle AND short strangle instances — they're
 * differentiated by {@code strategyId} (e.g. {@code inst-7}) on each row.
 *
 * <p>Uniquely identified by {@code (strategyId, sessionDate)} — one row per strategy per
 * date. Table was previously named {@code straddle_sessions}; renamed to {@code strategy_sessions}
 * via {@code SchemaMigration} so the name matches what it actually holds.
 */
@Entity
@Table(name = "strategy_sessions",
       uniqueConstraints = @UniqueConstraint(name = "uk_strategy_session_date",
                                              columnNames = {"strategy_id", "session_date"}))
public class StrategySessionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Strategy that owned this session. SQL-level DEFAULT preserved as the legacy
     *  {@code combined-sl-roll} id so any pre-existing rows / DDL adds remain valid; the
     *  Java field initializer is the live default for new rows, which the concrete strategy
     *  overrides via {@code id()} when writing. */
    @Column(name = "strategy_id", nullable = false, length = 40,
            columnDefinition = "VARCHAR(40) DEFAULT 'combined-sl-roll' NOT NULL")
    private String strategyId = "short-straddle";

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

    public StrategySessionEntity() {}

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
