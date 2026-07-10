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

    /** Fyers option symbol of the leg this cycle traded (e.g. NSE:NIFTY...CE). Nullable for
     *  legacy rows persisted before this column existed — readers must treat null as "—". */
    @Column(name = "symbol", length = 80)
    private String symbol;

    /** The setup that opened this cycle — e.g. {@code H3_REVERSAL} or {@code L4_BREAKDOWN}.
     *  Nullable for legacy rows persisted before this column existed — readers backfill from
     *  the in-memory ring for today's rows where present, fall back to "—" otherwise. */
    @Column(name = "setup", length = 32)
    private String setup;

    /** Epoch millis when the entry order was placed. Used by the AM/PM-of-day analytics
     *  breakdown. Boxed Long so legacy rows persisted before this column existed —
     *  which DB-side hold NULL — load without exploding the primitive setter. Null = unknown
     *  (analytics falls back to {@link #closedAtMillis} for those rows). */
    @Column(name = "opened_at_millis")
    private Long openedAtMillis;

    /** OI bias state captured at the moment the entry order was placed —
     *  STRONG_BEARISH_BIAS / STRONG_BULLISH_BIAS / NEUTRAL / STALE / no-baseline-yet.
     *  Null for legacy rows. Readers bucket null as "Unknown" in the OI-confirmation
     *  breakdown alongside Strong / Weak. */
    @Column(name = "entry_oi_bias", length = 32)
    private String entryOiBias;

    /** Retired: replaced by {@link #instrument} for the analytics split. Column kept so
     *  ddl-auto=update doesn't error on existing DBs; no new rows populate it. */
    @Column(name = "was_expiry_day")
    private Boolean wasExpiryDay;

    /** The instrument that traded this cycle — {@code NIFTY} or {@code BANKNIFTY}. Drives
     *  the analytics "NIFTY vs BankNifty" breakdown. Derived from the leg symbol at
     *  write time (NSE:BANKNIFTY* → BANKNIFTY, NSE:NIFTY* → NIFTY). Null for legacy rows;
     *  readers backfill by inspecting {@link #symbol} for those rows. */
    @Column(name = "instrument", length = 16)
    private String instrument;

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

    /** Per-leg average entry price (option premium the bot sold at, or bought at for a BUY).
     *  Nullable so rows persisted before this column existed load without exploding the primitive
     *  setter; readers render null / 0 as "—". */
    @Column(name = "entry_price")
    private Double entryPrice;

    /** Per-leg exit price at the moment the cycle closed. Nullable for the same reason as
     *  {@link #entryPrice}. */
    @Column(name = "exit_price")
    private Double exitPrice;

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
    public String getSymbol() { return symbol; }
    public void setSymbol(String v) { this.symbol = v; }
    public String getSetup() { return setup; }
    public void setSetup(String v) { this.setup = v; }
    public Long   getOpenedAtMillis() { return openedAtMillis; }
    public void   setOpenedAtMillis(Long v) { this.openedAtMillis = v; }
    public void   setOpenedAtMillis(long v) { this.openedAtMillis = Long.valueOf(v); }
    public String getEntryOiBias() { return entryOiBias; }
    public void   setEntryOiBias(String v) { this.entryOiBias = v; }
    public Boolean getWasExpiryDay() { return wasExpiryDay; }
    public void    setWasExpiryDay(Boolean v) { this.wasExpiryDay = v; }
    public String  getInstrument()      { return instrument; }
    public void    setInstrument(String v) { this.instrument = v; }
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
    public Double getEntryPrice() { return entryPrice; }
    public void   setEntryPrice(Double v) { this.entryPrice = v; }
    public Double getExitPrice()  { return exitPrice; }
    public void   setExitPrice(Double v) { this.exitPrice = v; }
    public String getCloseReason() { return closeReason; }
    public void setCloseReason(String v) { this.closeReason = v; }
    public Integer getSlHitCount() { return slHitCount; }
    public void setSlHitCount(Integer v) { this.slHitCount = v; }
}
