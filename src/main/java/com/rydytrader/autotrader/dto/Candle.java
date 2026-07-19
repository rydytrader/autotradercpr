package com.rydytrader.autotrader.dto;

/**
 * Single OHLCV candle emitted by {@link com.rydytrader.autotrader.service.CandleAggregator}.
 * {@code startMillis} is the candle's opening tick timestamp in epoch millis (IST-aware
 * at the broker side); the bucket is closed when a tick arrives in the next bucket.
 */
public record Candle(
    double open,
    double high,
    double low,
    double close,
    long   volume,
    long   startMillis,
    /** Session VWAP as of the bar's last sample (Fyers ATP). 0 when the feed hasn't
     *  yielded a positive VWAP yet — index symbols never carry ATP, so the aggregator's
     *  VWAP overlay stays flat 0 for NIFTY spot but populates for option legs. */
    double vwap
) {
    public boolean isGreen() { return close > open; }
    public boolean isRed()   { return close < open; }
}
