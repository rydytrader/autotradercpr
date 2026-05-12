package com.rydytrader.autotrader.dto;

public class ProcessedSignal {

    private final String signal;      // "BUY" or "SELL"
    private final String symbol;
    private final int    quantity;
    private final double target;
    private final Double target1Price; // nullable — absolute T1 price when target rescue splits the trade; null means single-target
    private final double stoploss;
    private final String setup;
    private final String probability;
    private final boolean rejected;
    private final boolean silent;   // true = rejected without an event-log entry (native-LPT when LPT disabled)
    private final String rejectionReason;
    private final double atr;
    private final double atrMultiplier;
    private final String description;
    private final boolean rescueShifted; // true when target was modified by the rescue (vs original computeTargets value)
    private final boolean useStructuralSl;

    private ProcessedSignal(Builder b) {
        this.signal          = b.signal;
        this.symbol          = b.symbol;
        this.quantity        = b.quantity;
        this.target          = b.target;
        this.target1Price    = b.target1Price;
        this.stoploss        = b.stoploss;
        this.setup           = b.setup;
        this.probability     = b.probability;
        this.rejected        = b.rejected;
        this.silent          = b.silent;
        this.rejectionReason = b.rejectionReason;
        this.atr             = b.atr;
        this.atrMultiplier   = b.atrMultiplier;
        this.description     = b.description;
        this.rescueShifted   = b.rescueShifted;
        this.useStructuralSl = b.useStructuralSl;
    }

    public static ProcessedSignal rejected(String setup, String symbol, String reason) {
        return new Builder().setup(setup).symbol(symbol).rejected(true).rejectionReason(reason).build();
    }

    /** Rejected but suppresses the event-log entry — used for expected skips (e.g. native-LPT when LPT disabled). */
    public static ProcessedSignal silentlyRejected(String setup, String symbol) {
        return new Builder().setup(setup).symbol(symbol).rejected(true).silent(true).build();
    }

    public String  getSignal()          { return signal; }
    public String  getSymbol()          { return symbol; }
    public int     getQuantity()        { return quantity; }
    public double  getTarget()          { return target; }
    public Double  getTarget1Price()    { return target1Price; }
    public double  getStoploss()        { return stoploss; }
    public String  getSetup()           { return setup; }
    public String  getProbability()     { return probability; }
    public boolean isRejected()         { return rejected; }
    public boolean isSilent()           { return silent; }
    public String  getRejectionReason() { return rejectionReason; }
    public double  getAtr()             { return atr; }
    public double  getAtrMultiplier()   { return atrMultiplier; }
    public String  getDescription()     { return description; }
    public boolean isRescueShifted()    { return rescueShifted; }
    public boolean isUseStructuralSl() { return useStructuralSl; }

    public static class Builder {
        private String signal;
        private String symbol;
        private int    quantity;
        private double target;
        private Double target1Price;
        private double stoploss;
        private String setup;
        private String probability;
        private boolean rejected;
        private boolean silent;
        private String rejectionReason;
        private double atr;
        private double atrMultiplier;
        private String description;
        private boolean rescueShifted;
        private boolean useStructuralSl;

        public Builder signal(String v)          { this.signal = v; return this; }
        public Builder symbol(String v)          { this.symbol = v; return this; }
        public Builder quantity(int v)            { this.quantity = v; return this; }
        public Builder target(double v)           { this.target = v; return this; }
        public Builder target1Price(Double v)     { this.target1Price = v; return this; }
        public Builder stoploss(double v)         { this.stoploss = v; return this; }
        public Builder setup(String v)            { this.setup = v; return this; }
        public Builder probability(String v)      { this.probability = v; return this; }
        public Builder rejected(boolean v)        { this.rejected = v; return this; }
        public Builder silent(boolean v)          { this.silent = v; return this; }
        public Builder rejectionReason(String v)  { this.rejectionReason = v; return this; }
        public Builder atr(double v)              { this.atr = v; return this; }
        public Builder atrMultiplier(double v)    { this.atrMultiplier = v; return this; }
        public Builder description(String v)     { this.description = v; return this; }
        public Builder rescueShifted(boolean v)   { this.rescueShifted = v; return this; }
        public Builder useStructuralSl(boolean v) { this.useStructuralSl = v; return this; }

        public ProcessedSignal build() { return new ProcessedSignal(this); }
    }
}
