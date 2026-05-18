package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "sectors")
public class SectorEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String name;

    @Column(name = "index_ticker")
    private String indexTicker;

    public SectorEntity() {}

    public SectorEntity(String name, String indexTicker) {
        this.name = name;
        this.indexTicker = indexTicker;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getIndexTicker() { return indexTicker; }
    public void setIndexTicker(String indexTicker) { this.indexTicker = indexTicker; }
}
