package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.service.strategy.Strangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Pushes Strangle strategy state to every connected browser via Server-Sent Events.
 * State-change {@link #publish()} calls from the strategy + a 2-second heartbeat that
 * keeps LTP / MTM refreshing on open positions.
 */
@Service
public class StrangleStreamBroker {

    private static final Logger log = LoggerFactory.getLogger(StrangleStreamBroker.class);

    private final ObjectProvider<Strangle> strangleProvider;
    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .findAndRegisterModules();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public StrangleStreamBroker(ObjectProvider<Strangle> strangleProvider) {
        this.strangleProvider = strangleProvider;
    }

    public void addEmitter(SseEmitter e) {
        emitters.add(e);
        e.onCompletion(() -> emitters.remove(e));
        e.onTimeout(()    -> emitters.remove(e));
        e.onError(t       -> emitters.remove(e));
        sendSnapshot(e);
    }

    public void publish() {
        if (emitters.isEmpty()) return;
        broadcast();
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void heartbeat() {
        if (emitters.isEmpty()) return;
        broadcast();
    }

    private void sendSnapshot(SseEmitter e) {
        try {
            Map<String, Object> payload = currentState();
            if (payload == null) return;
            String json = mapper.writeValueAsString(payload);
            e.send(SseEmitter.event().name("state").data(json));
        } catch (Exception ex) {
            log.warn("[StrangleStream] initial snapshot send failed: {}", ex.getMessage());
            emitters.remove(e);
        }
    }

    private void broadcast() {
        Map<String, Object> payload = currentState();
        if (payload == null) return;
        String json;
        try { json = mapper.writeValueAsString(payload); }
        catch (Exception e) {
            log.warn("[StrangleStream] serialize failed (heartbeat will skip): {}", e.getMessage());
            return;
        }
        for (SseEmitter e : emitters) {
            try { e.send(SseEmitter.event().name("state").data(json)); }
            catch (IOException ex) {
                emitters.remove(e);
                try { e.complete(); } catch (Exception ignored) {}
            } catch (Exception ex) {
                log.debug("[StrangleStream] emitter send failed (removing): {}", ex.getMessage());
                emitters.remove(e);
            }
        }
    }

    private Map<String, Object> currentState() {
        Strangle s = strangleProvider.getIfAvailable();
        if (s == null) return null;
        try { return s.dashboardState(); }
        catch (Exception e) { log.warn("[StrangleStream] dashboardState threw: {}", e.getMessage()); return null; }
    }
}
