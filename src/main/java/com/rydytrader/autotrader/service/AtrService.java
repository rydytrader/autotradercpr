package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import com.rydytrader.autotrader.store.TokenStore;
import com.rydytrader.autotrader.config.FyersProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ATR(14) calculation service.
 * Fetches historical 15-min candles from Fyers /data/history at 9:00 AM.
 * Updates ATR live from CandleAggregator completed candles.
 */
@Service
public class AtrService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(AtrService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final int ATR_PERIOD = 14;

    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final RiskSettingsStore riskSettings;
    private final CandleAggregator candleAggregator;
    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, Double> atrBySymbol = new ConcurrentHashMap<>();

    public AtrService(TokenStore tokenStore,
                      FyersProperties fyersProperties,
                      RiskSettingsStore riskSettings,
                      CandleAggregator candleAggregator) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.riskSettings = riskSettings;
        this.candleAggregator = candleAggregator;
    }

    /**
     * Fetch ATR for all watchlist symbols. Called at 9:00 AM or on restart.
     */
    public void fetchAtrForSymbols(List<String> fyersSymbols) {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[AtrService] No access token, cannot fetch ATR");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;
        int timeframe = riskSettings.getScannerTimeframe();

        log.info("[AtrService] Fetching ATR({}) for {} symbols ({}min candles)", ATR_PERIOD, fyersSymbols.size(), timeframe);

        int success = 0;
        for (String symbol : fyersSymbols) {
            try {
                List<CandleAggregator.CandleBar> candles = fetchHistoricalCandles(symbol, timeframe, authHeader);
                if (candles.size() >= ATR_PERIOD) {
                    double atr = calculateAtr(candles, ATR_PERIOD);
                    atrBySymbol.put(symbol, atr);
                    // Seed candle aggregator with historical candles
                    candleAggregator.seedCandles(symbol, candles);
                    success++;
                } else {
                    log.warn("[AtrService] Only {} candles for {} (need {})", candles.size(), symbol, ATR_PERIOD);
                }
                // Throttle: 100ms between calls
                Thread.sleep(100);
            } catch (Exception e) {
                log.error("[AtrService] Failed to fetch ATR for {}: {}", symbol, e.getMessage());
            }
        }
        log.info("[AtrService] ATR loaded for {}/{} symbols", success, fyersSymbols.size());
    }

    /**
     * Fetch historical candles from Fyers /data/history API.
     */
    private List<CandleAggregator.CandleBar> fetchHistoricalCandles(String symbol, int timeframeMin, String authHeader) throws Exception {
        // Resolution: "15" for 15-min candles
        String resolution = String.valueOf(timeframeMin);

        // Date range: last 5 trading days to ensure 14+ candles
        long toEpoch = Instant.now().getEpochSecond();
        long fromEpoch = toEpoch - (7 * 24 * 3600); // 7 days back

        String urlStr = "https://api-t1.fyers.in/data/history?symbol=" + symbol
            + "&resolution=" + resolution
            + "&date_format=0"
            + "&range_from=" + fromEpoch
            + "&range_to=" + toEpoch
            + "&cont_flag=1";

        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", authHeader);
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);

        int status = conn.getResponseCode();
        if (status != 200) {
            throw new IOException("HTTP " + status + " for " + symbol);
        }

        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }

        JsonNode root = mapper.readTree(sb.toString());
        JsonNode candles = root.get("candles");
        if (candles == null || !candles.isArray()) {
            return Collections.emptyList();
        }

        List<CandleAggregator.CandleBar> result = new ArrayList<>();
        for (JsonNode c : candles) {
            // [timestamp, open, high, low, close, volume]
            if (c.size() < 5) continue;
            CandleAggregator.CandleBar bar = new CandleAggregator.CandleBar();
            long epochSec = c.get(0).asLong();
            LocalTime lt = Instant.ofEpochSecond(epochSec).atZone(IST).toLocalTime();
            bar.startMinute = lt.getHour() * 60L + lt.getMinute();
            bar.open = c.get(1).asDouble();
            bar.high = c.get(2).asDouble();
            bar.low = c.get(3).asDouble();
            bar.close = c.get(4).asDouble();
            if (c.size() >= 6) bar.volume = c.get(5).asLong();
            result.add(bar);
        }

        return result;
    }

    /**
     * Calculate ATR using Simple Moving Average of True Range.
     */
    private double calculateAtr(List<CandleAggregator.CandleBar> candles, int period) {
        if (candles.size() < period) return 0;

        double sum = 0;
        int start = candles.size() - period;
        for (int i = start; i < candles.size(); i++) {
            CandleAggregator.CandleBar prev = i > 0 ? candles.get(i - 1) : null;
            sum += candles.get(i).trueRange(prev);
        }
        return sum / period;
    }

    // ── CandleCloseListener — update ATR on each new candle ──────────────────

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar completedCandle) {
        List<CandleAggregator.CandleBar> candles = candleAggregator.getCompletedCandles(fyersSymbol);
        if (candles.size() >= ATR_PERIOD) {
            double atr = calculateAtr(candles, ATR_PERIOD);
            atrBySymbol.put(fyersSymbol, atr);
        }
    }

    // ── Public API ───────────────────────────────────────────────────────────

    public double getAtr(String symbol) {
        return atrBySymbol.getOrDefault(symbol, 0.0);
    }

    public Map<String, Double> getAllAtr() {
        return Collections.unmodifiableMap(atrBySymbol);
    }

    public boolean hasAtr(String symbol) {
        return atrBySymbol.containsKey(symbol);
    }
}
