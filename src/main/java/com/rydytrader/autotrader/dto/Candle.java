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
    long   startMillis
) {
    public boolean isGreen() { return close > open; }
    public boolean isRed()   { return close < open; }
}
