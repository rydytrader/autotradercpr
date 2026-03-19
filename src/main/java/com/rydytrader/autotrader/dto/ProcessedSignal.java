package com.rydytrader.autotrader.dto;

public class ProcessedSignal {

    private final String signal;      // "BUY" or "SELL"
    private final String symbol;
    private final int    quantity;
    private final double target;
    private final double stoploss;
    private final String setup;
    private final String probability;
    private final boolean rejected;
    private final String rejectionReason;

    private ProcessedSignal(Builder b) {
        this.signal          = b.signal;
        this.symbol          = b.symbol;
        this.quantity        = b.quantity;
        this.target          = b.target;
        this.stoploss        = b.stoploss;
        this.setup           = b.setup;
        this.probability     = b.probability;
        this.rejected        = b.rejected;
        this.rejectionReason = b.rejectionReason;
    }

    public static ProcessedSignal rejected(String setup, String symbol, String reason) {
        return new Builder().setup(setup).symbol(symbol).rejected(true).rejectionReason(reason).build();
    }

    public String  getSignal()          { return signal; }
    public String  getSymbol()          { return symbol; }
    public int     getQuantity()        { return quantity; }
    public double  getTarget()          { return target; }
    public double  getStoploss()        { return stoploss; }
    public String  getSetup()           { return setup; }
    public String  getProbability()     { return probability; }
    public boolean isRejected()         { return rejected; }
    public String  getRejectionReason() { return rejectionReason; }

    public static class Builder {
        private String signal;
        private String symbol;
        private int    quantity;
        private double target;
        private double stoploss;
        private String setup;
        private String probability;
        private boolean rejected;
        private String rejectionReason;

        public Builder signal(String v)          { this.signal = v; return this; }
        public Builder symbol(String v)          { this.symbol = v; return this; }
        public Builder quantity(int v)            { this.quantity = v; return this; }
        public Builder target(double v)           { this.target = v; return this; }
        public Builder stoploss(double v)         { this.stoploss = v; return this; }
        public Builder setup(String v)            { this.setup = v; return this; }
        public Builder probability(String v)      { this.probability = v; return this; }
        public Builder rejected(boolean v)        { this.rejected = v; return this; }
        public Builder rejectionReason(String v)  { this.rejectionReason = v; return this; }

        public ProcessedSignal build() { return new ProcessedSignal(this); }
    }
}
