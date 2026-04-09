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
    // Split target fields
    private String target1OrderId;   // T1 order ID (half qty at partial target)
    private String target2OrderId;   // T2 order ID (half qty at full target)
    private Double target1Price;     // T1 price (nullable)
    private Double target2Price;     // T2 price (nullable)
    private Integer remainingQty;    // qty left after T1 (null = full qty still open)
    private Boolean t1Filled;        // T1 has been hit
    private LocalDateTime updatedAt;

    @Column(columnDefinition = "TEXT")
    private String description;

    private String probability;

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

    public String getTarget1OrderId() { return target1OrderId; }
    public void setTarget1OrderId(String target1OrderId) { this.target1OrderId = target1OrderId; }

    public String getTarget2OrderId() { return target2OrderId; }
    public void setTarget2OrderId(String target2OrderId) { this.target2OrderId = target2OrderId; }

    public double getTarget1Price() { return target1Price != null ? target1Price : 0; }
    public void setTarget1Price(double target1Price) { this.target1Price = target1Price; }

    public double getTarget2Price() { return target2Price != null ? target2Price : 0; }
    public void setTarget2Price(double target2Price) { this.target2Price = target2Price; }

    public int getRemainingQty() { return remainingQty != null ? remainingQty : 0; }
    public void setRemainingQty(int remainingQty) { this.remainingQty = remainingQty; }

    public boolean isT1Filled() { return t1Filled != null && t1Filled; }
    public void setT1Filled(boolean t1Filled) { this.t1Filled = t1Filled; }
}
