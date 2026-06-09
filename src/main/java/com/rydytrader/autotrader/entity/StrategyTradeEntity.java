package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * One row per individual options-strategy cycle (entry → close). Holds rows for both short
 * straddle AND short strangle instances — they're differentiated by {@code strategyId} on
 * each row. Granularity of the Analytics Home page — wins / losses / streaks / edge / costs
 * are all computed at this level.
 *
 * <p>One row written per cycle when the last leg closes (timed squareoff, SL, or manual).
 * The strategy's legs may close at different times; {@code closedAtMillis} reflects the
 * last-leg close.
 *
 * <p>Charges are computed per cycle so per-cycle net P&L is exact, not allocated. Table
 * was previously named {@code straddle_trades}; renamed to {@code strategy_trades} via
 * {@code SchemaMigration} so the name matches what it actually holds.
 */
@Entity
@Table(name = "strategy_trades",
       indexes = @Index(name = "idx_strategy_trades_strat_date",
                        columnList = "strategy_id, session_date"))
public class StrategyTradeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "strategy_id", nullable = false, length = 40)
    private String strategyId;

    /** ISO yyyy-MM-dd of the trading day. */
    @Column(name = "session_date", nullable = false, length = 10)
    private String sessionDate;

    /** Epoch millis at the moment the cycle finished closing. */
    @Column(name = "closed_at_millis", nullable = false)
    private long closedAtMillis;

    /** Per-leg quantity (lots × NIFTY lot size). Both legs share the same qty. */
    @Column
    private int qty;

    /** Sum of CE + PE leg P&L for this cycle. (ceEntry − ceClose) × qty + (peEntry − peClose) × qty. */
    @Column(name = "gross_pnl")
    private double grossPnl;

    /** Charges allocated to this cycle — brokerage + STT + exchange + SEBI + stamp + GST.
     *  Computed from this cycle's sell-side + buy-side premium turnover. */
    @Column
    private double charges;

    /** {@code grossPnl − charges}. */
    @Column(name = "net_pnl")
    private double netPnl;

    /** Why the cycle ended: SL_HIT, TARGET_HIT, TIMED_SQUAREOFF, MAX_LOSS_HIT, MANUAL,
     *  STALE_DAY_RESET, CE_SL_HIT, PE_SL_HIT, etc. */
    @Column(name = "close_reason", length = 40)
    private String closeReason;

    /** Number of per-leg SL hits during this cycle (0, 1 or 2). 0 = both legs ran to the
     *  timed squareoff or manual close without breaching SL. 1 = exactly one leg got stopped
     *  (the other was carried to squareoff). 2 = both legs got SL'd. Used by the Analytics
     *  page to surface the day-mix histogram. Nullable so rows written before the column
     *  existed don't blow up — treated as 0 by readers. */
    @Column(name = "sl_hit_count")
    private Integer slHitCount;

    public StrategyTradeEntity() {}

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getStrategyId() { return strategyId; }
    public void setStrategyId(String v) { this.strategyId = v; }
    public String getSessionDate() { return sessionDate; }
    public void setSessionDate(String v) { this.sessionDate = v; }
    public long getClosedAtMillis() { return closedAtMillis; }
    public void setClosedAtMillis(long v) { this.closedAtMillis = v; }
    public int getQty() { return qty; }
    public void setQty(int v) { this.qty = v; }
    public double getGrossPnl() { return grossPnl; }
    public void setGrossPnl(double v) { this.grossPnl = v; }
    public double getCharges() { return charges; }
    public void setCharges(double v) { this.charges = v; }
    public double getNetPnl() { return netPnl; }
    public void setNetPnl(double v) { this.netPnl = v; }
    public String getCloseReason() { return closeReason; }
    public void setCloseReason(String v) { this.closeReason = v; }
    public Integer getSlHitCount() { return slHitCount; }
    public void setSlHitCount(Integer v) { this.slHitCount = v; }
}
