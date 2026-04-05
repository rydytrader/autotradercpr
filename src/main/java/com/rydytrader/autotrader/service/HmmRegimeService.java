package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.dto.RegimeState;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Classifies each NFO stock into a market regime (Bullish / Bearish / Neutral)
 * using a Gaussian HMM trained on ~1 year of daily returns + volatility.
 *
 * - Startup: backfills 1 year of daily closes per symbol from Fyers history API
 * - Daily at 8:46 AM: appends today's close from bhavcopy, retrains HMM
 * - Persists history to store/config/hmm-history.json
 */
@Service
public class HmmRegimeService {

    private static final Logger log = LoggerFactory.getLogger(HmmRegimeService.class);
    private static final String HISTORY_FILE = "../store/config/hmm-history.json";
    private static final int MAX_DAYS = 250;
    private static final int MIN_DAYS_FOR_TRAINING = 60;
    private static final int VOLATILITY_WINDOW = 10;
    private static final int NUM_STATES = 3;
    private static final int EM_ITERATIONS = 30;
    private static final double CONFIDENCE_THRESHOLD = 0.60;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final TokenStore tokenStore;
    private final FyersProperties fyersProperties;
    private final BhavcopyService bhavcopyService;
    private final EventService eventService;

    // symbol → rolling daily closes (oldest first)
    private final ConcurrentHashMap<String, LinkedList<Double>> closeHistory = new ConcurrentHashMap<>();
    // symbol → current regime
    private final ConcurrentHashMap<String, RegimeState> regimes = new ConcurrentHashMap<>();

    public HmmRegimeService(TokenStore tokenStore,
                            FyersProperties fyersProperties,
                            BhavcopyService bhavcopyService,
                            EventService eventService) {
        this.tokenStore = tokenStore;
        this.fyersProperties = fyersProperties;
        this.bhavcopyService = bhavcopyService;
        this.eventService = eventService;
    }

    @PostConstruct
    public void init() {
        loadFromFile();
        log.info("[HmmRegime] Loaded history for {} symbols", closeHistory.size());
    }

    /** Runs daily after BhavcopyService + MomentumService (8:47 AM). */
    @Scheduled(cron = "0 47 8 * * MON-FRI")
    public void scheduledUpdate() {
        appendTodayAndClassify();
    }

    /** Public API for UI. */
    public RegimeState getRegime(String symbol) {
        return regimes.get(symbol);
    }

    public Map<String, RegimeState> getAllRegimes() {
        return Collections.unmodifiableMap(regimes);
    }

    // ── Core logic ────────────────────────────────────────────────────────────

    /**
     * Ensure history is present for all bhavcopy symbols. Called lazily when tokens available.
     * If any symbol has < MIN_DAYS_FOR_TRAINING, fetch 1 year of history from Fyers.
     */
    public void ensureBackfill() {
        String accessToken = tokenStore.getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            log.warn("[HmmRegime] Cannot backfill — no access token");
            return;
        }
        String authHeader = fyersProperties.getClientId() + ":" + accessToken;

        Set<String> allSymbols = bhavcopyService.getTodayCache().keySet();
        List<String> needsBackfill = new ArrayList<>();
        for (String sym : allSymbols) {
            LinkedList<Double> hist = closeHistory.get(sym);
            if (hist == null || hist.size() < MIN_DAYS_FOR_TRAINING) {
                needsBackfill.add(sym);
            }
        }

        if (needsBackfill.isEmpty()) {
            log.info("[HmmRegime] No backfill needed — {} symbols have sufficient history", allSymbols.size());
            return;
        }

        log.info("[HmmRegime] Backfilling {} symbols (1 year history)", needsBackfill.size());
        eventService.log("[INFO] Starting HMM regime backfill for " + needsBackfill.size() + " symbols");

        int success = 0;
        for (String sym : needsBackfill) {
            try {
                List<Double> closes = fetch1YearCloses("NSE:" + sym + "-EQ", authHeader);
                if (closes != null && closes.size() >= MIN_DAYS_FOR_TRAINING) {
                    closeHistory.put(sym, new LinkedList<>(closes));
                    success++;
                }
                Thread.sleep(200); // rate limiting
            } catch (Exception e) {
                log.warn("[HmmRegime] Failed to fetch history for {}: {}", sym, e.getMessage());
            }
        }

        saveToFile();
        log.info("[HmmRegime] Backfill complete: {}/{} symbols", success, needsBackfill.size());
        eventService.log("[SUCCESS] HMM backfill complete: " + success + " symbols loaded");

        // Train HMMs for all symbols
        classifyAll();
    }

    /**
     * Append today's close (from bhavcopy) to each symbol's history, then retrain.
     */
    public void appendTodayAndClassify() {
        // First ensure backfill is done
        ensureBackfill();

        var todayCache = bhavcopyService.getTodayCache();
        int appended = 0;
        for (var entry : todayCache.entrySet()) {
            String sym = entry.getKey();
            double close = entry.getValue().getClose();
            if (close <= 0) continue;

            LinkedList<Double> hist = closeHistory.computeIfAbsent(sym, k -> new LinkedList<>());

            // Check if this close is already the last entry (avoid double-append on retries)
            if (hist.isEmpty() || Math.abs(hist.getLast() - close) > 0.01) {
                hist.addLast(close);
                while (hist.size() > MAX_DAYS) hist.removeFirst();
                appended++;
            }
        }

        saveToFile();
        log.info("[HmmRegime] Appended today's close for {} symbols", appended);

        classifyAll();
    }

    /** Train HMM and classify regime for all symbols with enough history. */
    public void classifyAll() {
        int success = 0;
        for (var entry : closeHistory.entrySet()) {
            String sym = entry.getKey();
            LinkedList<Double> hist = entry.getValue();
            if (hist.size() < MIN_DAYS_FOR_TRAINING) continue;

            try {
                RegimeState state = trainAndClassify(sym, hist);
                if (state != null) {
                    regimes.put(sym, state);
                    success++;
                }
            } catch (Exception e) {
                log.warn("[HmmRegime] Failed to classify {}: {}", sym, e.getMessage());
            }
        }
        log.info("[HmmRegime] Classified {} symbols", success);
    }

    private RegimeState trainAndClassify(String symbol, List<Double> closes) {
        // Build feature matrix: [log_return, 10-day volatility]
        int n = closes.size();
        if (n < VOLATILITY_WINDOW + 2) return null;

        double[] logReturns = new double[n - 1];
        for (int i = 1; i < n; i++) {
            double prev = closes.get(i - 1);
            double curr = closes.get(i);
            logReturns[i - 1] = (prev > 0) ? Math.log(curr / prev) : 0;
        }

        int T = logReturns.length - VOLATILITY_WINDOW + 1;
        if (T < MIN_DAYS_FOR_TRAINING) return null;

        double[][] observations = new double[T][2];
        for (int i = 0; i < T; i++) {
            int end = i + VOLATILITY_WINDOW;
            double mean = 0;
            for (int j = i; j < end; j++) mean += logReturns[j];
            mean /= VOLATILITY_WINDOW;
            double var = 0;
            for (int j = i; j < end; j++) {
                double diff = logReturns[j] - mean;
                var += diff * diff;
            }
            var /= VOLATILITY_WINDOW;
            observations[i][0] = logReturns[end - 1]; // latest return
            observations[i][1] = Math.sqrt(var);      // rolling volatility
        }

        // Z-score normalize
        normalizeColumns(observations);

        // Train HMM
        GaussianHmm hmm = new GaussianHmm(NUM_STATES, 2, symbol.hashCode());
        hmm.fit(observations, EM_ITERATIONS);

        // Identify states by mean return (column 0)
        double[][] means = hmm.getMeans();
        int bullishState = 0, bearishState = 0;
        double maxMean = Double.NEGATIVE_INFINITY, minMean = Double.POSITIVE_INFINITY;
        for (int i = 0; i < NUM_STATES; i++) {
            if (means[i][0] > maxMean) { maxMean = means[i][0]; bullishState = i; }
            if (means[i][0] < minMean) { minMean = means[i][0]; bearishState = i; }
        }
        int neutralState = 3 - bullishState - bearishState; // 0+1+2=3

        // Get current state posteriors
        double[] posteriors = hmm.getCurrentPosteriors(observations);

        // Map posteriors to regime order [bullish, bearish, neutral]
        double[] ordered = new double[]{
            posteriors[bullishState],
            posteriors[bearishState],
            posteriors[neutralState]
        };

        // Pick max-probability regime
        String regime;
        double confidence;
        if (ordered[0] >= ordered[1] && ordered[0] >= ordered[2]) {
            regime = "BULLISH"; confidence = ordered[0];
        } else if (ordered[1] >= ordered[2]) {
            regime = "BEARISH"; confidence = ordered[1];
        } else {
            regime = "NEUTRAL"; confidence = ordered[2];
        }

        // Low confidence → neutral
        if (confidence < CONFIDENCE_THRESHOLD) {
            regime = "NEUTRAL";
        }

        return new RegimeState(symbol, regime, confidence, ordered, System.currentTimeMillis());
    }

    private void normalizeColumns(double[][] data) {
        int rows = data.length, cols = data[0].length;
        for (int c = 0; c < cols; c++) {
            double mean = 0;
            for (int r = 0; r < rows; r++) mean += data[r][c];
            mean /= rows;
            double var = 0;
            for (int r = 0; r < rows; r++) { double d = data[r][c] - mean; var += d * d; }
            var /= rows;
            double std = Math.sqrt(Math.max(var, 1e-10));
            for (int r = 0; r < rows; r++) data[r][c] = (data[r][c] - mean) / std;
        }
    }

    // ── Fyers history fetch ──────────────────────────────────────────────────

    private List<Double> fetch1YearCloses(String fyersSymbol, String authHeader) throws Exception {
        long toEpoch = Instant.now().getEpochSecond();
        long fromEpoch = toEpoch - (364L * 24 * 3600); // 364 days (Fyers max is 365) — ~250 trading days

        String urlStr = "https://api-t1.fyers.in/data/history?symbol=" + java.net.URLEncoder.encode(fyersSymbol, StandardCharsets.UTF_8)
            + "&resolution=1D"
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
        if (status != 200) throw new IOException("HTTP " + status);

        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }

        JsonNode root = mapper.readTree(sb.toString());
        JsonNode candles = root.get("candles");
        if (candles == null || !candles.isArray() || candles.isEmpty()) return null;

        List<Double> closes = new ArrayList<>();
        for (int i = 0; i < candles.size(); i++) {
            JsonNode c = candles.get(i);
            // [epoch, open, high, low, close, volume]
            closes.add(c.get(4).asDouble());
        }
        // Trim to last 250
        while (closes.size() > MAX_DAYS) closes.remove(0);
        return closes;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    private void saveToFile() {
        try {
            Path path = Paths.get(HISTORY_FILE);
            Files.createDirectories(path.getParent());
            Map<String, List<Double>> snapshot = new LinkedHashMap<>();
            for (var entry : closeHistory.entrySet()) {
                snapshot.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("lastSaved", System.currentTimeMillis());
            data.put("history", snapshot);
            mapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), data);
        } catch (Exception e) {
            log.error("[HmmRegime] Failed to save: {}", e.getMessage());
        }
    }

    private void loadFromFile() {
        try {
            Path path = Paths.get(HISTORY_FILE);
            if (!Files.exists(path)) return;
            JsonNode root = mapper.readTree(path.toFile());
            JsonNode histNode = root.get("history");
            if (histNode == null) return;

            histNode.fields().forEachRemaining(entry -> {
                LinkedList<Double> closes = new LinkedList<>();
                JsonNode arr = entry.getValue();
                if (arr.isArray()) {
                    for (JsonNode c : arr) closes.add(c.asDouble());
                }
                closeHistory.put(entry.getKey(), closes);
            });
        } catch (Exception e) {
            log.error("[HmmRegime] Failed to load: {}", e.getMessage());
        }
    }
}
