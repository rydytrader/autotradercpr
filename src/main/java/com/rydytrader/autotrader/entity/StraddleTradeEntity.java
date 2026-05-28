package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * One row per individual short-straddle cycle (entry → close). Granularity of the Analytics
 * Home page — wins / losses / streaks / edge / costs are all computed at this level.
 *
 * <p>How each strategy writes rows:
 * <ul>
 *   <li><b>combined-sl-roll</b> — one row per {@code closeBothLegs()} call. If the day had
 *       two rolls, that day produces three rows (entry-close, roll1-close, roll2-close).</li>
 *   <li><b>leg-sl</b> — one row per day, written when the straddle reaches DONE_FOR_DAY
 *       (last leg closed or timed squareoff). The straddle's legs may close at different times;
 *       the row's {@code closedAtMillis} reflects the last-leg close.</li>
 * </ul>
 *
 * <p>Charges are computed per cycle so per-straddle net P&L is exact, not allocated.
 */
@Entity
@Table(name = "straddle_trades",
       indexes = @Index(name = "idx_straddle_trades_strat_date",
                        columnList = "strategy_id, session_date"))
public class StraddleTradeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "strategy_id", nullable = false, length = 40)
    private String strategyId;

    /** ISO yyyy-MM-dd of the trading day. */
    @Column(name = "session_date", nullable = false, length = 10)
    private String sessionDate;

    /** Epoch millis at the moment the straddle finished closing. */
    @Column(name = "closed_at_millis", nullable = false)
    private long closedAtMillis;

    /** Per-leg quantity (lots × NIFTY lot size). Both legs share the same qty. */
    @Column
    private int qty;

    /** Sum of CE + PE leg P&L for this straddle. (ceEntry − ceClose) × qty + (peEntry − peClose) × qty. */
    @Column(name = "gross_pnl")
    private double grossPnl;

    /** Charges allocated to this cycle — brokerage + STT + exchange + SEBI + stamp + GST.
     *  Computed from this cycle's sell-side + buy-side premium turnover. */
    @Column
    private double charges;

    /** {@code grossPnl − charges}. */
    @Column(name = "net_pnl")
    private double netPnl;

    /** Why the straddle ended: SL_HIT, TARGET_HIT, TIMED_SQUAREOFF, MAX_LOSS_HIT, MANUAL,
     *  STALE_DAY_RESET, CE_SL_HIT (leg-sl), PE_SL_HIT (leg-sl), etc. */
    @Column(name = "close_reason", length = 40)
    private String closeReason;

    public StraddleTradeEntity() {}

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
}
