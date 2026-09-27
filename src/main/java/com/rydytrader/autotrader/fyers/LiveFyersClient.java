package com.rydytrader.autotrader.fyers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

@Component
public class LiveFyersClient implements FyersClient {

    private static final Logger log = LoggerFactory.getLogger(LiveFyersClient.class);
    private static final String BASE = "https://api-t1.fyers.in/api/v3";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public JsonNode placeOrder(String orderJson, String authHeader) throws Exception {
        return post(BASE + "/orders/sync", orderJson, authHeader);
    }

    @Override
    public JsonNode cancelOrder(String orderId, String authHeader) throws Exception {
        String body = "{\"id\":\"" + orderId + "\"}";
        return delete(BASE + "/orders/sync", body, authHeader);
    }

    @Override
    public JsonNode getOrder(String orderId, String authHeader) throws Exception {
        return get(BASE + "/orders?id=" + orderId, authHeader);
    }

    @Override
    public JsonNode getOrders(String authHeader) throws Exception {
        return get(BASE + "/orders", authHeader);
    }

    @Override
    public JsonNode getPositions(String authHeader) throws Exception {
        return get(BASE + "/positions", authHeader);
    }

    @Override
    public JsonNode getTradebook(String authHeader) throws Exception {
        return get(BASE + "/tradebook", authHeader);
    }

    @Override
    public JsonNode validateAuthCode(String requestBody) throws Exception {
        return post(BASE + "/validate-authcode", requestBody, null);
    }

    private static volatile boolean unparseableChainLoggedThisSession = false;

    @Override
    public JsonNode getOptionChain(String symbol, int strikeCount, String authHeader) throws Exception {
        JsonNode root = getOptionChain(symbol, strikeCount, 0L, authHeader);
        String returnedExpiry = firstParseableExpiry(root);
        // Only log when the parser can't decode ANY row — and only once per session, so
        // we don't spam every 5s polling tick.
        if (returnedExpiry.isEmpty() && !unparseableChainLoggedThisSession) {
            unparseableChainLoggedThisSession = true;
            log.warn("[fyers-client] Option chain for {} — parser could not decode any row's expiry. "
                + "See the sample-symbols dump above so we can extend parseExpiryFromSymbol.", symbol);
        }
        // Guard against Fyers lingering on a just-expired chain on new-expiry-day mornings.
        long futureTs = pickFutureExpiryTsIfStale(root);
        if (futureTs > 0) {
            log.info("[fyers-client] Option chain for {} came back with expired expiry — re-fetching with timestamp={}",
                symbol, futureTs);
            return getOptionChain(symbol, strikeCount, futureTs, authHeader);
        }
        return root;
    }

    private static volatile boolean sampleSymbolsLoggedThisSession = false;

    /** First non-empty parsed expiry across a chain response's rows. Public-ish only for
     *  the log line above. */
    private static String firstParseableExpiry(JsonNode root) {
        if (root == null) return "";
        JsonNode data = root.has("data") ? root.get("data") : null;
        JsonNode rows = data != null && data.has("optionsChain") ? data.get("optionsChain")
            : (root.has("optionsChain") ? root.get("optionsChain") : null);
        if (rows == null || !rows.isArray()) return "";
        String result = "";
        for (JsonNode r : rows) {
            String sym = r.has("symbol") ? r.get("symbol").asText("") : "";
            String exp = parseExpiryFromSymbol(sym);
            if (!exp.isEmpty()) { result = exp; break; }
        }
        // If we couldn't parse anything, dump a sample once so we can debug the format.
        if (result.isEmpty() && !sampleSymbolsLoggedThisSession) {
            sampleSymbolsLoggedThisSession = true;
            StringBuilder dump = new StringBuilder("[fyers-client] chain sample symbols (unparseable): ");
            int shown = 0;
            for (JsonNode r : rows) {
                if (shown >= 3) break;
                String sym = r.has("symbol") ? r.get("symbol").asText("") : "";
                if (sym.isEmpty()) continue;
                dump.append(sym).append(" | ");
                shown++;
            }
            log.warn("{}", dump);
            JsonNode ed = data != null && data.has("expiryData") ? data.get("expiryData") : null;
            log.warn("[fyers-client] expiryData in response: {}", ed);
        }
        return result;
    }

    @Override
    public JsonNode getOptionChain(String symbol, int strikeCount, long expiryTsSecs, String authHeader) throws Exception {
        String url = "https://api-t1.fyers.in/data/options-chain-v3?symbol="
            + java.net.URLEncoder.encode(symbol, java.nio.charset.StandardCharsets.UTF_8)
            + "&strikecount=" + strikeCount
            + "&timestamp=" + (expiryTsSecs > 0 ? expiryTsSecs : "");
        return get(url, authHeader);
    }

    /** Inspects a chain response. Uses {@code data.expiryData[0].date} — Fyers's own
     *  declaration of the expiry the response is serving — rather than parsing symbols
     *  (Fyers ships monthly expiries in a MMM format my symbol parser can't decode).
     *  If that first entry's date is already in the past (Fyers lingering on the just-
     *  expired chain on new-expiry-day mornings), returns the epoch-seconds of the next
     *  future entry from {@code expiryData}. Otherwise returns 0. */
    private static long pickFutureExpiryTsIfStale(JsonNode root) {
        if (root == null) return 0;
        JsonNode data = root.has("data") ? root.get("data") : null;
        JsonNode expiryData = data != null && data.has("expiryData") ? data.get("expiryData") : null;
        if (expiryData == null || !expiryData.isArray() || expiryData.size() == 0) return 0;

        java.time.LocalDate today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata"));
        long todayEpoch = today.atStartOfDay(java.time.ZoneId.of("Asia/Kolkata")).toEpochSecond();

        // Fyers's first expiryData entry is the expiry the response is currently serving.
        JsonNode first = expiryData.get(0);
        java.time.LocalDate firstDate = parseFyersDate(first);
        if (firstDate == null) return 0;
        if (!firstDate.isBefore(today)) return 0; // fresh — no retry needed

        // Otherwise, walk expiryData for the first entry whose date is >= today.
        log.info("[fyers-client] Stale chain — Fyers is serving expiry {} < today {}. Scanning expiryData for future entry.",
            firstDate, today);
        for (JsonNode entry : expiryData) {
            java.time.LocalDate d = parseFyersDate(entry);
            if (d == null || d.isBefore(today)) continue;
            long ts = readEpochField(entry, "expiry");
            if (ts <= 0) ts = d.atStartOfDay(java.time.ZoneId.of("Asia/Kolkata")).toEpochSecond();
            if (ts >= todayEpoch) return ts;
        }
        log.warn("[fyers-client] expiryData had no future entry (all {} entries in the past)",
            expiryData.size());
        return 0;
    }

    /** Parses a Fyers expiryData entry's date. Accepts either the {@code date} field
     *  ({@code dd-MM-yyyy}) or, as a fallback, the {@code expiry} epoch seconds field. */
    private static java.time.LocalDate parseFyersDate(JsonNode entry) {
        if (entry == null) return null;
        if (entry.has("date") && entry.get("date").isTextual()) {
            String d = entry.get("date").asText().trim();
            if (!d.isEmpty()) {
                try {
                    return java.time.LocalDate.parse(d,
                        java.time.format.DateTimeFormatter.ofPattern("dd-MM-yyyy"));
                } catch (Exception ignored) {}
            }
        }
        long ts = readEpochField(entry, "expiry");
        if (ts > 0) {
            try {
                return java.time.LocalDateTime.ofEpochSecond(ts, 0,
                    java.time.ZoneOffset.ofHoursMinutes(5, 30)).toLocalDate();
            } catch (Exception ignored) {}
        }
        return null;
    }

    /** Reads a Fyers epoch field that may arrive as a JSON number, a numeric string,
     *  or a {@code dd-MM-yyyy} date string. Returns 0 when the field is absent or
     *  can't be parsed. */
    private static long readEpochField(JsonNode entry, String key) {
        if (entry == null || !entry.has(key) || entry.get(key).isNull()) return 0;
        JsonNode v = entry.get(key);
        if (v.isNumber()) return v.asLong(0);
        if (v.isTextual()) {
            String s = v.asText().trim();
            if (s.isEmpty()) return 0;
            try { return Long.parseLong(s); } catch (NumberFormatException ignored) {}
            // Fyers sometimes ships date-only fields as "dd-MM-yyyy" — treat those as
            // that day's midnight IST epoch.
            try {
                java.time.LocalDate d = java.time.LocalDate.parse(s,
                    java.time.format.DateTimeFormatter.ofPattern("dd-MM-yyyy"));
                return d.atStartOfDay(java.time.ZoneId.of("Asia/Kolkata")).toEpochSecond();
            } catch (Exception ignored) {}
        }
        return 0;
    }

    /** Mirror of {@code OptionChainController.parseExpiryFromSymbol}. Handles both:
     *  <ul>
     *    <li>WEEKLY: {@code YYMDD} (e.g. {@code 26929} → 2026-09-29). M is 1-9 / O / N / D.</li>
     *    <li>MONTHLY: {@code YYMON} (e.g. {@code 26SEP} → last Tuesday of Sep-2026).
     *        Fyers uses this format when the last weekly Tuesday of a month IS the monthly
     *        expiry; the day itself isn't encoded, so we compute it as the last Tuesday
     *        of {@code (yr, MON)}.</li>
     *  </ul>
     *  Returns "" on unparseable input. */
    private static String parseExpiryFromSymbol(String fyersSymbol) {
        if (fyersSymbol == null) return "";
        try {
            int hash = -1;
            String matched = null;
            for (String p : new String[]{"NIFTY", "SENSEX"}) {
                int idx = fyersSymbol.indexOf(p);
                if (idx >= 0) { hash = idx; matched = p; break; }
            }
            if (hash < 0 || matched == null) return "";
            String tail = fyersSymbol.substring(hash + matched.length());
            if (tail.length() < 5) return "";
            int yr = Integer.parseInt(tail.substring(0, 2));
            String maybeMonth3 = tail.substring(2, 5).toUpperCase();
            int monthlyIdx = "JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC".indexOf(maybeMonth3);
            if (monthlyIdx >= 0 && monthlyIdx % 3 == 0) {
                int month = monthlyIdx / 3 + 1;
                java.time.DayOfWeek dow = "SENSEX".equals(matched)
                    ? java.time.DayOfWeek.THURSDAY
                    : java.time.DayOfWeek.TUESDAY;
                return lastDayOfMonthMatching(2000 + yr, month, dow).toString();
            }
            char monthCh = tail.charAt(2);
            int month;
            if (monthCh >= '1' && monthCh <= '9') month = monthCh - '0';
            else if (monthCh == 'O') month = 10;
            else if (monthCh == 'N') month = 11;
            else if (monthCh == 'D') month = 12;
            else return "";
            int day = Integer.parseInt(tail.substring(3, 5));
            return java.time.LocalDate.of(2000 + yr, month, day).toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static java.time.LocalDate lastDayOfMonthMatching(int year, int month,
                                                                java.time.DayOfWeek dow) {
        java.time.LocalDate last = java.time.LocalDate.of(year, month, 1)
            .withDayOfMonth(java.time.YearMonth.of(year, month).lengthOfMonth());
        while (last.getDayOfWeek() != dow) {
            last = last.minusDays(1);
        }
        return last;
    }

    @Override
    public JsonNode getQuotes(String symbols, String authHeader) throws Exception {
        String url = "https://api-t1.fyers.in/data/quotes/?symbols=" + symbols;
        return get(url, authHeader);
    }

    @Override
    public JsonNode getProfile(String authHeader) throws Exception {
        return get(BASE + "/profile", authHeader);
    }

    @Override
    public JsonNode modifyOrder(String orderJson, String authHeader) throws Exception {
        return patch(BASE + "/orders/sync", orderJson, authHeader);
    }

    @Override
    public JsonNode getHistory(String symbol, String resolution, String fromDate, String toDate,
                                String authHeader) throws Exception {
        String url = "https://api-t1.fyers.in/data/history?symbol="
            + java.net.URLEncoder.encode(symbol, java.nio.charset.StandardCharsets.UTF_8)
            + "&resolution=" + java.net.URLEncoder.encode(resolution, java.nio.charset.StandardCharsets.UTF_8)
            + "&date_format=1"
            + "&range_from=" + fromDate
            + "&range_to=" + toDate
            + "&cont_flag=1";
        return get(url, authHeader);
    }

    // ── HTTP HELPERS ──────────────────────────────────────────────────────────
    private static final int CONNECT_TIMEOUT = 10_000; // 10 seconds
    private static final int READ_TIMEOUT    = 10_000; // 10 seconds

    private JsonNode get(String urlStr, String authHeader) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setReadTimeout(READ_TIMEOUT);
        conn.setRequestMethod("GET");
        if (authHeader != null) conn.setRequestProperty("Authorization", authHeader);
        return readResponse(conn);
    }

    private JsonNode post(String urlStr, String body, String authHeader) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setReadTimeout(READ_TIMEOUT);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        if (authHeader != null) conn.setRequestProperty("Authorization", authHeader);
        conn.setDoOutput(true);
        conn.getOutputStream().write(body.getBytes());
        conn.getOutputStream().close();
        return readResponse(conn);
    }

    private JsonNode patch(String urlStr, String body, String authHeader) throws Exception {
        // HttpURLConnection doesn't support PATCH — use Java 11+ HttpClient instead
        java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofMillis(CONNECT_TIMEOUT))
            .build();
        var reqBuilder = java.net.http.HttpRequest.newBuilder()
            .uri(java.net.URI.create(urlStr))
            .timeout(java.time.Duration.ofMillis(READ_TIMEOUT))
            .header("Content-Type", "application/json")
            .method("PATCH", java.net.http.HttpRequest.BodyPublishers.ofString(body));
        if (authHeader != null) reqBuilder.header("Authorization", authHeader);
        java.net.http.HttpResponse<String> resp = client.send(reqBuilder.build(),
            java.net.http.HttpResponse.BodyHandlers.ofString());
        try {
            return mapper.readTree(resp.body());
        } catch (Exception e) {
            return mapper.createObjectNode().put("s", "error").put("code", resp.statusCode())
                .put("message", "HTTP " + resp.statusCode() + ": " + resp.body().substring(0, Math.min(resp.body().length(), 200)));
        }
    }

    private JsonNode put(String urlStr, String body, String authHeader) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setReadTimeout(READ_TIMEOUT);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        if (authHeader != null) conn.setRequestProperty("Authorization", authHeader);
        conn.setDoOutput(true);
        conn.getOutputStream().write(body.getBytes());
        conn.getOutputStream().close();
        return readResponse(conn);
    }

    private JsonNode delete(String urlStr, String body, String authHeader) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setReadTimeout(READ_TIMEOUT);
        conn.setRequestMethod("DELETE");
        conn.setRequestProperty("Content-Type", "application/json");
        if (authHeader != null) conn.setRequestProperty("Authorization", authHeader);
        conn.setDoOutput(true);
        conn.getOutputStream().write(body.getBytes());
        conn.getOutputStream().close();
        return readResponse(conn);
    }

    private JsonNode readResponse(HttpURLConnection conn) throws Exception {
        int httpStatus = conn.getResponseCode();
        InputStream is = httpStatus < 400 ? conn.getInputStream() : conn.getErrorStream();
        if (is == null) {
            return mapper.createObjectNode().put("s", "error").put("code", httpStatus).put("message", "HTTP " + httpStatus + " (no response body)");
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sb.append(line);
        br.close();
        String body = sb.toString();
        try {
            return mapper.readTree(body);
        } catch (Exception e) {
            // Non-JSON response (e.g. HTML 404 page)
            return mapper.createObjectNode().put("s", "error").put("code", httpStatus).put("message", "HTTP " + httpStatus + ": " + body.substring(0, Math.min(body.length(), 200)));
        }
    }
}