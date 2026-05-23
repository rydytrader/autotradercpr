package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "stocks")
public class StockEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String ticker;

    private String name;

    /** The stock's primary index — drives the bot's alignment / HTF hurdle / 5m hurdle
     *  filters. Replaces the legacy {@code sector} field; each stock now points directly
     *  to the index it should be aligned to (NIFTY 50 or a sector index). */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "primary_index_id")
    private IndexEntity primaryIndex;

    @Column(nullable = false)
    private boolean enabled = true;

    public enum Membership { NIFTY_50, NIFTY_NEXT_50, OTHERS }

    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    private Membership membership = Membership.OTHERS;

    public StockEntity() {}

    public StockEntity(String ticker, String name, IndexEntity primaryIndex, boolean enabled) {
        this(ticker, name, primaryIndex, enabled, Membership.OTHERS);
    }

    public StockEntity(String ticker, String name, IndexEntity primaryIndex, boolean enabled, Membership membership) {
        this.ticker = ticker;
        this.name = name;
        this.primaryIndex = primaryIndex;
        this.enabled = enabled;
        this.membership = membership != null ? membership : Membership.OTHERS;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getTicker() { return ticker; }
    public void setTicker(String ticker) { this.ticker = ticker; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public IndexEntity getPrimaryIndex() { return primaryIndex; }
    public void setPrimaryIndex(IndexEntity primaryIndex) { this.primaryIndex = primaryIndex; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public Membership getMembership() { return membership; }
    public void setMembership(Membership membership) { this.membership = membership != null ? membership : Membership.OTHERS; }
}
