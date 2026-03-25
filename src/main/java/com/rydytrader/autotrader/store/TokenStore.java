package com.rydytrader.autotrader.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TokenStore {

    private static final Logger log = LoggerFactory.getLogger(TokenStore.class);

    private String accessToken;

    public void setAccessToken(String token) {
        this.accessToken = token;
        log.info("Access token updated in memory");
    }

    public String getAccessToken() {
        return accessToken;
    }

    public boolean isTokenAvailable() {
        return accessToken != null && !accessToken.isEmpty();
    }
}
