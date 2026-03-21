package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.config.TelegramProperties;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class TelegramService {

    private final TelegramProperties props;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public TelegramService(TelegramProperties props) {
        this.props = props;
    }

    public void sendMessage(String text) {
        if (!props.isEnabled() || props.getBotToken().isBlank() || props.getChatId().isBlank()) return;

        executor.submit(() -> {
            try {
                String encoded = URLEncoder.encode(text, StandardCharsets.UTF_8);
                String url = "https://api.telegram.org/bot" + props.getBotToken()
                    + "/sendMessage?chat_id=" + props.getChatId()
                    + "&text=" + encoded;

                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    System.err.println("[Telegram] Failed to send message: HTTP " + response.statusCode()
                        + " | " + response.body());
                }
            } catch (Exception e) {
                System.err.println("[Telegram] Error sending message: " + e.getMessage());
            }
        });
    }
}
