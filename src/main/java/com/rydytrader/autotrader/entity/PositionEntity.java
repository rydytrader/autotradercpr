package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "positions")
public class PositionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String symbol;

    private String side;
    private int qty;
    private double avgPrice;
    private String setup;
    private String entryTime;
    private double slPrice;
    private double targetPrice;
    private String slOrderId;
    private String targetOrderId;
    private LocalDateTime updatedAt;

    @Column(columnDefinition = "TEXT")
    private String description;

    private String probability;

    // NIFTY HTF Hurdle break — early-exit guard. Captured at entry when the trade was
    // gated by an active NIFTY 15-min hurdle confirmation. niftyHurdleGuardLow defended
    // for LONG; niftyHurdleGuardHigh defended for SHORT. Null = no guard (no early-exit
    // applies to this position).
    private Double niftyHurdleGuardLow;
    private Double niftyHurdleGuardHigh;

    public PositionEntity() {}

    @PrePersist
    @PreUpdate
    public void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public int getQty() { return qty; }
    public void setQty(int qty) { this.qty = qty; }

    public double getAvgPrice() { return avgPrice; }
    public void setAvgPrice(double avgPrice) { this.avgPrice = avgPrice; }

    public String getSetup() { return setup; }
    public void setSetup(String setup) { this.setup = setup; }

    public String getEntryTime() { return entryTime; }
    public void setEntryTime(String entryTime) { this.entryTime = entryTime; }

    public double getSlPrice() { return slPrice; }
    public void setSlPrice(double slPrice) { this.slPrice = slPrice; }

    public double getTargetPrice() { return targetPrice; }
    public void setTargetPrice(double targetPrice) { this.targetPrice = targetPrice; }

    public String getSlOrderId() { return slOrderId; }
    public void setSlOrderId(String slOrderId) { this.slOrderId = slOrderId; }

    public String getTargetOrderId() { return targetOrderId; }
    public void setTargetOrderId(String targetOrderId) { this.targetOrderId = targetOrderId; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getProbability() { return probability; }
    public void setProbability(String probability) { this.probability = probability; }

    public Double getNiftyHurdleGuardLow() { return niftyHurdleGuardLow; }
    public void setNiftyHurdleGuardLow(Double v) { this.niftyHurdleGuardLow = v; }

    public Double getNiftyHurdleGuardHigh() { return niftyHurdleGuardHigh; }
    public void setNiftyHurdleGuardHigh(Double v) { this.niftyHurdleGuardHigh = v; }
}
