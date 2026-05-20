package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * NSE index master — NIFTY 50 plus the sectoral indices a stock can be aligned to.
 * Replaces the legacy {@code SectorEntity}: each {@link StockEntity} maps to one
 * primary index, and the bot's alignment / HTF hurdle / 5m hurdle filters run against
 * that primary index instead of always NIFTY 50.
 */
@Entity
@Table(name = "index_master")
public class IndexEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Display name, e.g. "NIFTY 50", "Nifty Bank". Unique. */
    @Column(unique = true, nullable = false)
    private String name;

    /** Fyers ticker, e.g. "NIFTY50", "NIFTYBANK". Unique. */
    @Column(unique = true, nullable = false)
    private String ticker;

    public IndexEntity() {}

    public IndexEntity(String name, String ticker) {
        this.name = name;
        this.ticker = ticker;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getTicker() { return ticker; }
    public void setTicker(String ticker) { this.ticker = ticker; }
}
