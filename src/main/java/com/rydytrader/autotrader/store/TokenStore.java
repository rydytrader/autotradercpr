package com.rydytrader.autotrader.store;

import org.springframework.stereotype.Component;

@Component
public class TokenStore {

    private String accessToken;

    public void setAccessToken(String token) {
        this.accessToken = token;
        System.out.println("Access token updated in memory");
    }

    public String getAccessToken() {
        return accessToken;
    }

    public boolean isTokenAvailable() {
        return accessToken != null && !accessToken.isEmpty();
    }
}
