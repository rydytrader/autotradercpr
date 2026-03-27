package com.rydytrader.autotrader.fyers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

@Component
public class LiveFyersClient implements FyersClient {

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

    @Override
    public JsonNode getOptionChain(String symbol, int strikeCount, String authHeader) throws Exception {
        String url = "https://api-t1.fyers.in/data/options-chain-v3?symbol=" + symbol + "&strikecount=" + strikeCount + "&timestamp=";
        return get(url, authHeader);
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