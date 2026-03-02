package com.rydytrader.autotrader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Binds Fyers API credentials from {@code application.properties} (prefix {@code fyers}).
 * Expected properties: {@code fyers.client-id} and {@code fyers.secret-key}.
 */
@Component
@ConfigurationProperties(prefix = "fyers")
public class FyersProperties {

    private String clientId;
    private String secretKey;

    /** Returns the Fyers API client ID used in the Authorization header. */
    public String getClientId() {
        return clientId;
    }

    /** Sets the Fyers API client ID (injected by Spring from application.properties). */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /** Returns the Fyers API secret key used for SHA-256 app ID hash generation. */
    public String getSecretKey() {
        return secretKey;
    }

    /** Sets the Fyers API secret key (injected by Spring from application.properties). */
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
}
