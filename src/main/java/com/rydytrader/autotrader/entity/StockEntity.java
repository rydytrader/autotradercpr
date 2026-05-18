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

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "sector_id")
    private SectorEntity sector;

    @Column(nullable = false)
    private boolean enabled = true;

    public StockEntity() {}

    public StockEntity(String ticker, String name, SectorEntity sector, boolean enabled) {
        this.ticker = ticker;
        this.name = name;
        this.sector = sector;
        this.enabled = enabled;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getTicker() { return ticker; }
    public void setTicker(String ticker) { this.ticker = ticker; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public SectorEntity getSector() { return sector; }
    public void setSector(SectorEntity sector) { this.sector = sector; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
