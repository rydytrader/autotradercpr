package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
@Table(name = "trades", indexes = {
    @Index(name = "idx_trades_trade_date", columnList = "tradeDate")
})
public class TradeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private LocalDate tradeDate;

    private String timestamp;
    private String symbol;
    private String side;
    private int qty;
    private double entryPrice;
    private double exitPrice;
    private String exitReason;
    private String setup;
    private double pnl;
    private double charges;
    private double netPnl;

    @Column(columnDefinition = "TEXT")
    private String description;

    private String probability;

    private String strategy = "DAY"; // DAY or SWING

    public TradeEntity() {}

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public LocalDate getTradeDate() { return tradeDate; }
    public void setTradeDate(LocalDate tradeDate) { this.tradeDate = tradeDate; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public int getQty() { return qty; }
    public void setQty(int qty) { this.qty = qty; }

    public double getEntryPrice() { return entryPrice; }
    public void setEntryPrice(double entryPrice) { this.entryPrice = entryPrice; }

    public double getExitPrice() { return exitPrice; }
    public void setExitPrice(double exitPrice) { this.exitPrice = exitPrice; }

    public String getExitReason() { return exitReason; }
    public void setExitReason(String exitReason) { this.exitReason = exitReason; }

    public String getSetup() { return setup; }
    public void setSetup(String setup) { this.setup = setup; }

    public double getPnl() { return pnl; }
    public void setPnl(double pnl) { this.pnl = pnl; }

    public double getCharges() { return charges; }
    public void setCharges(double charges) { this.charges = charges; }

    public double getNetPnl() { return netPnl; }
    public void setNetPnl(double netPnl) { this.netPnl = netPnl; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getProbability() { return probability; }
    public void setProbability(String probability) { this.probability = probability; }

    public String getStrategy() { return strategy; }
    public void setStrategy(String strategy) { this.strategy = strategy; }
}
