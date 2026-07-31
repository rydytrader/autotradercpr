package com.rydytrader.autotrader.gdfl;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the Global Data Feeds (GDFL) WebSocket client.
 *
 * <p>Backed by the {@code gdfl.*} properties in {@code application.properties}. The API
 * key MUST come from the {@code GDFL_API_KEY} environment variable — never checked into
 * source. When {@link #enabled} is {@code false} (default), no WS connection is opened
 * and the Fyers feed remains the sole source for every symbol.
 *
 * <p>Subscription happens DYNAMICALLY once {@code OptionSelling} resolves the day's ATM CE
 * and PE (~09:18 IST). The two Fyers symbols carry the expiry embedded, so
 * {@code GdflSymbolMapper} decodes it from the string itself — no operator config for
 * the weekly expiry rollover.
 *
 * <p>Rate limits per the subscription: 50 symbols and 1800 requests. Test endpoint is
 * {@code wss://test.lisuns.com:4576/}.
 */
@ConfigurationProperties(prefix = "gdfl")
public class GdflProperties {

    /** Master on/off — false = no WS connection, Fyers remains sole source. */
    private boolean enabled = false;

    /** WebSocket endpoint URL, including scheme + host + port (no trailing slash needed).
     *  Example: {@code wss://test.lisuns.com:4576}. */
    private String endpoint = "";

    /** API key from the GDFL subscription (env var {@code GDFL_API_KEY}). */
    private String apiKey = "";

    /** Exchange code for subscribe messages. NFO for NIFTY options, NSE_IDX for indices. */
    private String exchange = "NFO";

    /** Seconds between reconnect attempts on disconnect. */
    private int reconnectDelaySeconds = 5;

    /** How often to poll {@code OptionSelling} for the resolved ATM. Once the CE/PE symbols
     *  appear, GdflService subscribes them and stops polling for the day. */
    private int atmPollIntervalSeconds = 5;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public String getApiKey() { return apiKey; }
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }

    public int getReconnectDelaySeconds() { return reconnectDelaySeconds; }
    public void setReconnectDelaySeconds(int v) { this.reconnectDelaySeconds = v; }

    public int getAtmPollIntervalSeconds() { return atmPollIntervalSeconds; }
    public void setAtmPollIntervalSeconds(int v) { this.atmPollIntervalSeconds = v; }
}
