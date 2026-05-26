package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * One row per trading-day rolling-straddle session. Persisted on day rollover (or first-tick-
 * next-day) for historical analytics. Replaces the equity {@code trades} table for the
 * options-only bot.
 */
@Entity
@Table(name = "straddle_sessions")
public class StraddleSessionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 10)
    private String sessionDate; // ISO yyyy-MM-dd

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
