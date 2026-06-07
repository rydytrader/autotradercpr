package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * NIFTY playbook-recommendation service. Computes two daily-resolution indicators off a
 * single Fyers historical fetch and combines them into a single label ("MEAN-REV" or
 * "TREND") that recommends which short-straddle SL config to use today.
 *
 * <ol>
 *   <li><b>Hurst exponent (H)</b> via Classical R/S analysis on 100 daily log returns.
 *       Classifies the tape as MEAN_REVERTING (H &lt; 0.45) / RANDOM / TRENDING (H &gt; 0.55).</li>
 *   <li><b>ATR ratio</b> = ATR(5) / ATR(20) on daily True Range. Classifies recent realised
 *       volatility as COMPRESSED (&lt; 0.7) / NORMAL / SHOCK (&gt; 1.5).</li>
 * </ol>
 *
 * <p>A 3×3 lookup folds the two axes into <b>MEAN-REV</b> or <b>TREND</b>. The label is
 * display-only — the operator reads it off the analytics page and chooses whether to apply
 * the recommended SL config to their next entry.
 *
 * <p>Runs on boot (best-effort) and at 16:05 IST every weekday (35 min after market close,
 * so Fyers has settled the daily index candle). Single Spring singleton — all straddle
 * instances share one NIFTY-wide signal.
 */
@Service
public class MarketRegimeService {

    private static final Logger log = LoggerFactory.getLogger(MarketRegimeService.class);

    private static final String NIFTY            = "NSE:NIFTY50-INDEX";
    private static final int    LOOKBACK_RETURNS = 100;
    // Chunk sizes for the R/S regression. Dropped n=10 (versus the classical 10/20/25/50)
    // because the smallest chunk computes its std-dev from only 10 returns, biasing the
    // rescaled-range upward and lifting H by ~0.03–0.05 on quiet tapes. Keeping 20/25/50
    // gives three well-spaced regression points with much smaller small-sample noise.
    private static final int[]  RS_CHUNK_SIZES   = {20, 25, 50};
    private static final int    ATR_SHORT_N      = 5;
    private static final int    ATR_LONG_N       = 20;
    private static final int    CANDLES_TO_FETCH = 150;  // slack for holidays/weekends + ATR warmup
    private static final ZoneId IST              = ZoneId.of("Asia/Kolkata");

    // ── Cached snapshot ──────────────────────────────────────────────────────
    private volatile double hurst      = Double.NaN;
    private volatile String hurstLabel = "UNKNOWN";  // MEAN_REVERTING / RANDOM / TRENDING
    private volatile double atrShort   = 0;
    private volatile double atrLong    = 0;
    private volatile double atrRatio   = Double.NaN;
    private volatile String atrLabel   = "UNKNOWN";  // COMPRESSED / NORMAL / SHOCK
    private volatile String regime     = "UNKNOWN";  // MEAN-REV / TREND — exposed
    private volatile long   asOfMillis = 0;

    private final FyersClientRouter fyersClient;
    private final TokenStore        tokenStore;
    private final FyersProperties   fyersProperties;
    private final EventService      eventService;

    public MarketRegimeService(FyersClientRouter fyersClient,
                                TokenStore tokenStore,
                                FyersProperties fyersProperties,
                                EventService eventService) {
        this.fyersClient     = fyersClient;
        this.tokenStore      = tokenStore;
        this.fyersProperties = fyersProperties;
        this.eventService    = eventService;
    }

    public double hurstValue()    { return hurst; }
    public double atrRatioValue() { return atrRatio; }
    public double atrShortValue() { return atrShort; }
    public double atrLongValue()  { return atrLong; }
    public String hurstAxis()     { return hurstLabel; }
    public String atrAxis()       { return atrLabel; }
    public String marketRegime()  { return regime; }
    public long   asOfMillis()    { return asOfMillis; }

    @PostConstruct
    public void boot() {
        // Best-effort — the token may not be set on the very first boot. The 16:05 cron
        // (or the next /api/market-regime hit, since we don't lazy-fetch there) will catch up.
        try { tryCompute(); }
        catch (Exception e) { log.warn("[MarketRegime] boot compute failed: {}", e.getMessage()); }
    }

    /** Every weekday at 16:05 IST — 35 min after the 15:30 close, gives Fyers time to
     *  settle the daily candle for the underlying index. */
    @Scheduled(cron = "0 5 16 * * MON-FRI", zone = "Asia/Kolkata")
    public void refresh() {
        try { tryCompute(); }
        catch (Exception e) { log.warn("[MarketRegime] scheduled compute failed: {}", e.getMessage()); }
    }

    // ── Compute pipeline ─────────────────────────────────────────────────────

    private synchronized void tryCompute() {
        if (!tokenStore.isTokenAvailable()) {
            log.info("[MarketRegime] skip compute — no access token");
            return;
        }
        List<DailyCandle> candles = fetchCandles();
        if (candles == null || candles.size() < ATR_LONG_N + 2) {
            log.warn("[MarketRegime] skip compute — only {} candles fetched (need {})",
                candles == null ? 0 : candles.size(), ATR_LONG_N + 2);
            return;
        }

        // Log returns on closes — last LOOKBACK_RETURNS values (or as many as we have).
        double[] returns = logReturns(candles);
        int n = Math.min(LOOKBACK_RETURNS, returns.length);
        double[] tail = new double[n];
        System.arraycopy(returns, returns.length - n, tail, 0, n);
        double H = rescaledRange(tail);
        String hAxis = classifyHurst(H);

        // ATR on the latest 20 (or fewer) true ranges.
        double[] trs = trueRanges(candles);
        double aShort = smaTail(trs, ATR_SHORT_N);
        double aLong  = smaTail(trs, ATR_LONG_N);
        double ratio  = (aLong > 0) ? aShort / aLong : Double.NaN;
        String aAxis  = classifyAtr(ratio);

        String newRegime = combine(hAxis, aAxis);
        String oldRegime = this.regime;

        // Volatile writes — per-field atomicity is enough; no consumer reads multiple
        // fields and expects coherence within a tick.
        this.hurst      = round4(H);
        this.hurstLabel = hAxis;
        this.atrShort   = round2(aShort);
        this.atrLong    = round2(aLong);
        this.atrRatio   = round4(ratio);
        this.atrLabel   = aAxis;
        this.regime     = newRegime;
        this.asOfMillis = System.currentTimeMillis();

        String summary = String.format(
            "NIFTY Playbook: H=%.4f (%s) · ATR %d/%d = %.2fx (%s) → %s",
            H, hAxis, ATR_SHORT_N, ATR_LONG_N, ratio, aAxis, newRegime);
        log.info("[MarketRegime] {}", summary);
        // Only emit an event-log entry on the FIRST compute or a combined-regime transition.
        // Underlying axis flips that don't change the combined output stay silent — avoids
        // chatter on borderline H or ATR ratio values.
        if (!newRegime.equals(oldRegime)) {
            if ("UNKNOWN".equals(oldRegime)) {
                eventService.log("[INFO] [MarketRegime] " + summary);
            } else {
                eventService.log("[INFO] [MarketRegime] Playbook transition " + oldRegime
                    + " → " + newRegime + " (" + summary + ")");
            }
        }
    }

    private List<DailyCandle> fetchCandles() {
        try {
            String auth = fyersProperties.getClientId() + ":" + tokenStore.getAccessToken();
            LocalDate today = LocalDate.now(IST);
            LocalDate from  = today.minusDays(CANDLES_TO_FETCH);
            JsonNode root = fyersClient.getHistoricalCandles(NIFTY, "1D", from.toString(), today.toString(), auth);
            if (root == null || !root.has("candles") || !root.get("candles").isArray()) {
                log.warn("[MarketRegime] historical fetch returned no candles: {}", root);
                return null;
            }
            List<DailyCandle> out = new ArrayList<>();
            for (JsonNode c : root.get("candles")) {
                if (!c.isArray() || c.size() < 5) continue;
                long ts   = c.get(0).asLong();
                double o  = c.get(1).asDouble();
                double h  = c.get(2).asDouble();
                double l  = c.get(3).asDouble();
                double cl = c.get(4).asDouble();
                out.add(new DailyCandle(ts, o, h, l, cl));
            }
            out.sort(Comparator.comparingLong(DailyCandle::ts));
            return out;
        } catch (Exception e) {
            log.warn("[MarketRegime] candle fetch failed: {}", e.getMessage());
            return null;
        }
    }

    // ── Hurst — Classical R/S Rescaled Range ─────────────────────────────────

    /** R/S Rescaled-Range estimator. Returns NaN when the series is too short to chunk
     *  meaningfully. For each chunk size n in {@link #RS_CHUNK_SIZES} we average R/S across
     *  N/n non-overlapping chunks, then linear-regress log(RS) vs log(n) — the slope is H. */
    static double rescaledRange(double[] r) {
        if (r == null || r.length < RS_CHUNK_SIZES[0] * 2) return Double.NaN;
        java.util.List<double[]> points = new java.util.ArrayList<>();
        for (int n : RS_CHUNK_SIZES) {
            if (n > r.length) continue;
            int chunks = r.length / n;
            if (chunks < 2) continue;
            double rsSum = 0; int rsCount = 0;
            for (int i = 0; i < chunks; i++) {
                int from = i * n;
                double mean = 0;
                for (int j = 0; j < n; j++) mean += r[from + j];
                mean /= n;
                double cum = 0, min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
                double sqSum = 0;
                for (int j = 0; j < n; j++) {
                    double d = r[from + j] - mean;
                    cum += d;
                    if (cum < min) min = cum;
                    if (cum > max) max = cum;
                    sqSum += d * d;
                }
                double range = max - min;
                double sd    = Math.sqrt(sqSum / n);
                if (sd <= 0) continue;
                rsSum += range / sd;
                rsCount++;
            }
            if (rsCount > 0) {
                points.add(new double[]{ Math.log(n), Math.log(rsSum / rsCount) });
            }
        }
        if (points.size() < 2) return Double.NaN;
        // Simple linear regression — slope of log(RS) on log(n).
        double meanX = 0, meanY = 0;
        for (double[] p : points) { meanX += p[0]; meanY += p[1]; }
        meanX /= points.size(); meanY /= points.size();
        double num = 0, den = 0;
        for (double[] p : points) {
            double dx = p[0] - meanX;
            num += dx * (p[1] - meanY);
            den += dx * dx;
        }
        if (den == 0) return Double.NaN;
        return num / den;
    }

    private static String classifyHurst(double H) {
        if (Double.isNaN(H)) return "UNKNOWN";
        if (H < 0.45) return "MEAN_REVERTING";
        if (H > 0.55) return "TRENDING";
        return "RANDOM";
    }

    // ── ATR ─────────────────────────────────────────────────────────────────

    /** True Range across the series. tr[i] uses candle[i] and the prior candle's close.
     *  Returned array length = candles.size() − 1 (no prevClose for the first row). */
    static double[] trueRanges(List<DailyCandle> candles) {
        if (candles == null || candles.size() < 2) return new double[0];
        double[] trs = new double[candles.size() - 1];
        for (int i = 1; i < candles.size(); i++) {
            double prevClose = candles.get(i - 1).close();
            DailyCandle c = candles.get(i);
            double a = c.high() - c.low();
            double b = Math.abs(c.high() - prevClose);
            double d = Math.abs(c.low()  - prevClose);
            trs[i - 1] = Math.max(a, Math.max(b, d));
        }
        return trs;
    }

    private static double smaTail(double[] xs, int n) {
        if (xs == null || xs.length < n || n <= 0) return 0;
        double s = 0;
        for (int i = xs.length - n; i < xs.length; i++) s += xs[i];
        return s / n;
    }

    private static String classifyAtr(double ratio) {
        if (Double.isNaN(ratio) || ratio <= 0) return "UNKNOWN";
        if (ratio < 0.7) return "COMPRESSED";
        if (ratio > 1.5) return "SHOCK";
        return "NORMAL";
    }

    // ── 3×3 combine ─────────────────────────────────────────────────────────

    /** Maps (Hurst axis, ATR axis) → playbook recommendation. See the plan's 3×3 table:
     *  vol SHOCK always pushes us to TREND; structural TRENDING + non-compressed vol also
     *  pushes to TREND; everything else recommends MEAN-REV. */
    static String combine(String hAxis, String aAxis) {
        if ("UNKNOWN".equals(hAxis) || "UNKNOWN".equals(aAxis)) return "UNKNOWN";
        if ("SHOCK".equals(aAxis)) return "TREND";
        if ("TRENDING".equals(hAxis) && !"COMPRESSED".equals(aAxis)) return "TREND";
        return "MEAN-REV";
    }

    // ── Internal types + helpers ────────────────────────────────────────────

    private record DailyCandle(long ts, double open, double high, double low, double close) {}

    private static double[] logReturns(List<DailyCandle> candles) {
        double[] r = new double[candles.size() - 1];
        for (int i = 1; i < candles.size(); i++) {
            double prev = candles.get(i - 1).close();
            double curr = candles.get(i).close();
            r[i - 1] = (prev > 0 && curr > 0) ? Math.log(curr / prev) : 0;
        }
        return r;
    }

    private static double round2(double v) { return Double.isFinite(v) ? Math.round(v * 100.0)   / 100.0   : v; }
    private static double round4(double v) { return Double.isFinite(v) ? Math.round(v * 10000.0) / 10000.0 : v; }
}
