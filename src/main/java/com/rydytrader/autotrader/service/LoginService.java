package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.fyers.FyersClientRouter;
import com.rydytrader.autotrader.store.ModeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;

@Service
public class LoginService {

    private static final Logger log = LoggerFactory.getLogger(LoginService.class);

    private final FyersProperties    fyersProperties;
    private final FyersClientRouter  fyersClient;
    private final ModeStore          modeStore;

    public LoginService(FyersProperties fyersProperties,
                        FyersClientRouter fyersClient,
                        ModeStore modeStore) {
        this.fyersProperties = fyersProperties;
        this.fyersClient     = fyersClient;
        this.modeStore       = modeStore;
    }

    public String generateAccessToken(String authCode) {
        try {
            if (modeStore.isSimulator()) {
                // In simulator mode, bypass real auth
                JsonNode resp = fyersClient.validateAuthCode("{}");
                return resp.get("access_token").asText();
            }

            String appIdHash = sha256(fyersProperties.getClientId() + ":" + fyersProperties.getSecretKey());
            String requestBody = "{"
                    + "\"grant_type\":\"authorization_code\","
                    + "\"appIdHash\":\"" + appIdHash + "\","
                    + "\"code\":\"" + authCode + "\""
                    + "}";

            JsonNode node = fyersClient.validateAuthCode(requestBody);
            String token = node.get("access_token").asText();
            log.info("Access Token Generated Successfully [{}]", modeStore.getMode());
            return token;

        } catch (Exception e) {
            log.error("Error generating access token", e);
            return null;
        }
    }

    public static String sha256(String input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes("UTF-8"));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) sb.append('0');
            sb.append(hex);
        }
        return sb.toString();
    }
}