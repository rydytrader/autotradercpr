package com.rydytrader.autotrader.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Fetches the prior-day OHLC for NSE indices (NIFTY 50, BANKNIFTY, …) from the
 * NSE public archives.
 *
 * <p>URL pattern: {@code https://archives.nseindia.com/content/indices/ind_close_all_DDMMMYYYY.csv}
 * (e.g. {@code ind_close_all_21JUN2026.csv}). The file is published roughly
 * 30–60 minutes after market close (~16:00–17:00 IST) and is the authoritative
 * source for the NSE official daily close — same value TradingView's NIFTY
 * cash chart sources from. Avoids the Fyers v2 history endpoint's quirks where
 * the "D" candle close for index symbols can differ from the published
 * settlement value by a few points.
 *
 * <p>The file format includes the columns:
 * <pre>
 *   Index Name, Index Date, Open Index Value, High Index Value, Low Index Value,
 *   Closing Index Value, Points Change, Change(%), Volume, Turnover (Rs. Cr.),
 *   P/E, P/B, Div Yield
 * </pre>
 *
 * <p>NSE requires a browser-like {@code User-Agent}; raw {@code java.net.HttpURLConnection}
 * defaults get rejected. We set both UA and Accept.
 */
@Service
public class NseIndicesBhavcopyService {

    private static final Logger log = LoggerFactory.getLogger(NseIndicesBhavcopyService.class);
    /** NSE indices archive uses purely numeric date (DDMMYYYY) — e.g. {@code 23062026},
     *  not {@code 23JUN2026}. Confirmed against the working code in the equities-trader
     *  version of this bot (see archived BhavcopyService.java for the same pattern). */
    private static final DateTimeFormatter DATE_FMT =
        DateTimeFormatter.ofPattern("ddMMyyyy", Locale.ENGLISH);
    private static final String UA =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/124.0.0.0 Safari/537.36";

    /** Plain OHLC tuple — only the four fields the Camarilla pivot formula needs. */
    public record Ohlc(double open, double high, double low, double close, LocalDate date) {}

    /** Fetch the prior trading-day OHLC for the index name (NSE label, e.g. {@code "Nifty 50"}).
     *  Walks backward from {@code asOf} by calendar days, skipping weekends, until we find a
     *  bhavcopy that exists AND contains the requested index name. Returns null on any
     *  unrecoverable error. */
    public Ohlc fetchPriorDayOhlc(String indexName, LocalDate asOf) {
        if (indexName == null || indexName.isBlank() || asOf == null) return null;
        log.info("[NseBhavcopy] fetchPriorDayOhlc start — index='{}' asOf={}", indexName, asOf);
        LocalDate d = asOf.minusDays(1);
        for (int hop = 0; hop < 7; hop++) {
            if (d.getDayOfWeek() == DayOfWeek.SATURDAY) { d = d.minusDays(1); continue; }
            if (d.getDayOfWeek() == DayOfWeek.SUNDAY)   { d = d.minusDays(1); continue; }
            Ohlc o = tryFetch(indexName, d, hop);
            if (o != null) return o;
            d = d.minusDays(1);
        }
        log.warn("[NseBhavcopy] could not find bhavcopy for {} within 7 days back from {}",
            indexName, asOf);
        return null;
    }

    /** Aggregate the OHLC of the index across the most recent completed Mon-Fri trading
     *  week before {@code asOf}, fetching each weekday's bhavcopy individually and combining:
     *  <ul>
     *    <li>{@code open}  — first weekday with a bhavcopy (typically Monday).</li>
     *    <li>{@code high}  — max of all weekday highs in the window.</li>
     *    <li>{@code low}   — min of all weekday lows in the window.</li>
     *    <li>{@code close} — close of the LAST weekday with a bhavcopy (typically Friday).</li>
     *    <li>{@code date}  — date of the last weekday with a bhavcopy (anchor for the week).</li>
     *  </ul>
     *  Holiday days (no bhavcopy) are silently skipped. Returns null when no weekday in the
     *  prior-week window has a bhavcopy at all.
     *
     *  <p>"Prior week" = the Monday → Friday strictly before {@code asOf}'s week. If
     *  {@code asOf} is Mon Jun 29, the window is Mon Jun 22 → Fri Jun 26. */
    public Ohlc fetchPriorWeekOhlc(String indexName, LocalDate asOf) {
        if (indexName == null || indexName.isBlank() || asOf == null) return null;
        LocalDate priorFriday = asOf.with(java.time.temporal.TemporalAdjusters.previous(DayOfWeek.FRIDAY));
        LocalDate priorMonday = priorFriday.minusDays(4);
        log.info("[NseBhavcopy] fetchPriorWeekOhlc start — index='{}' window={}..{}",
            indexName, priorMonday, priorFriday);
        double weekHigh = Double.NEGATIVE_INFINITY;
        double weekLow  = Double.POSITIVE_INFINITY;
        double weekOpen = 0;
        double weekClose = 0;
        LocalDate latestDate = null;
        boolean haveOpen = false;
        for (LocalDate d = priorMonday; !d.isAfter(priorFriday); d = d.plusDays(1)) {
            if (d.getDayOfWeek() == DayOfWeek.SATURDAY || d.getDayOfWeek() == DayOfWeek.SUNDAY) continue;
            Ohlc o = tryFetch(indexName, d, 0);
            if (o == null) continue;
            if (!haveOpen) { weekOpen = o.open(); haveOpen = true; }
            if (o.high() > weekHigh) weekHigh = o.high();
            if (o.low()  < weekLow)  weekLow  = o.low();
            weekClose = o.close();
            latestDate = d;
        }
        if (latestDate == null) {
            log.warn("[NseBhavcopy] no bhavcopy days found in prior week {}..{} for {}",
                priorMonday, priorFriday, indexName);
            return null;
        }
        log.info("[NseBhavcopy] prior-week OHLC for {} ({}..{}): O={} H={} L={} C={}",
            indexName, priorMonday, latestDate, weekOpen, weekHigh, weekLow, weekClose);
        return new Ohlc(weekOpen, weekHigh, weekLow, weekClose, latestDate);
    }

    private static final String[] HOSTS = {
        // NSE has migrated their public archives over time — try the newer host
        // first, fall back to the legacy host on 404.
        "https://nsearchives.nseindia.com",
        "https://archives.nseindia.com"
    };

    private Ohlc tryFetch(String indexName, LocalDate date, int hop) {
        // Numeric date format (DDMMYYYY) — uppercase is a no-op but kept for safety.
        String fmt = date.format(DATE_FMT);
        for (String host : HOSTS) {
            String url = host + "/content/indices/ind_close_all_" + fmt + ".csv";
            Ohlc o = tryFetchUrl(indexName, date, hop, url);
            if (o != null) return o;
        }
        return null;
    }

    private Ohlc tryFetchUrl(String indexName, LocalDate date, int hop, String url) {
        HttpURLConnection conn = null;
        try {
            log.info("[NseBhavcopy] GET {} (hop={})", url, hop);
            conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
            conn.setRequestProperty("User-Agent", UA);
            conn.setRequestProperty("Accept", "text/csv,text/plain,*/*");
            conn.setRequestProperty("Referer", "https://www.nseindia.com/");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(20_000);
            int code = conn.getResponseCode();
            if (code != 200) {
                // INFO on first hop so the operator can diagnose; DEBUG for subsequent
                // hops (where 404 is expected — holiday days don't have a bhavcopy).
                if (hop == 0) log.info("[NseBhavcopy] HTTP {} for {}", code, url);
                else          log.debug("[NseBhavcopy] HTTP {} for {} (walk-back)", code, url);
                return null;
            }
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String header = r.readLine();
                if (header == null) return null;
                int[] col = locateColumns(header);
                if (col == null) {
                    log.warn("[NseBhavcopy] header parse failed for {} — header was: {}", url, header);
                    return null;
                }
                String line;
                while ((line = r.readLine()) != null) {
                    String[] row = splitCsv(line);
                    if (row.length <= col[4]) continue;
                    String name = unquote(row[col[0]]).trim();
                    if (!indexName.equalsIgnoreCase(name)) continue;
                    return new Ohlc(
                        parseD(row[col[1]]),
                        parseD(row[col[2]]),
                        parseD(row[col[3]]),
                        parseD(row[col[4]]),
                        date);
                }
                log.debug("[NseBhavcopy] {} row not found in {}", indexName, url);
            }
        } catch (IOException e) {
            log.info("[NseBhavcopy] fetch failed for {}: {}", url, e.getMessage());
        } finally {
            if (conn != null) conn.disconnect();
        }
        return null;
    }

    /** Returns the column indices for {@code [Index Name, Open, High, Low, Close]} or null when
     *  any required column is missing. Tolerant of column reordering across format changes. */
    private static int[] locateColumns(String header) {
        String[] cols = splitCsv(header);
        int name = -1, open = -1, high = -1, low = -1, close = -1;
        for (int i = 0; i < cols.length; i++) {
            String c = unquote(cols[i]).trim();
            if      (c.equalsIgnoreCase("Index Name"))           name  = i;
            else if (c.equalsIgnoreCase("Open Index Value"))     open  = i;
            else if (c.equalsIgnoreCase("High Index Value"))     high  = i;
            else if (c.equalsIgnoreCase("Low Index Value"))      low   = i;
            else if (c.equalsIgnoreCase("Closing Index Value"))  close = i;
        }
        if (name < 0 || open < 0 || high < 0 || low < 0 || close < 0) return null;
        return new int[]{name, open, high, low, close};
    }

    /** Minimal CSV split — index file is well-behaved (no embedded commas in quoted index
     *  names beyond the trivial case). Quoted fields are stripped of their surrounding quotes
     *  in {@link #unquote}. */
    private static String[] splitCsv(String line) {
        return line.split(",");
    }

    private static String unquote(String s) {
        if (s == null) return "";
        s = s.trim();
        if (s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static double parseD(String s) {
        try { return Double.parseDouble(unquote(s).replace(",", "").trim()); }
        catch (NumberFormatException e) { return 0; }
    }
}
