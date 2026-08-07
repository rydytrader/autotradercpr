package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rydytrader.autotrader.service.strategy.OptionScalping;
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
 * Pushes ATM VWAP strategy state to every connected browser via Server-Sent Events.
 *
 * <p>The strategy calls {@link #publish()} whenever something material changes — a new entry,
 * an exit, an ATM resolution, an event log line. A 2-second heartbeat ({@link #heartbeat()})
 * also emits the latest payload so LTP/MTM keep refreshing on the open positions even when
 * no state mutation is happening.
 *
 * <p>The {@link OptionScalping} strategy is injected lazily via {@link ObjectProvider} to break the
 * OptionScalping → Broker → OptionScalping circular dependency on bean construction.
 */
@Service
public class OptionScalpingStreamBroker {

    private static final Logger log = LoggerFactory.getLogger(OptionScalpingStreamBroker.class);

    private final ObjectProvider<OptionScalping> optionScalpingProvider;
    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .findAndRegisterModules();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public OptionScalpingStreamBroker(ObjectProvider<OptionScalping> optionScalpingProvider) {
        this.optionScalpingProvider = optionScalpingProvider;
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
            log.warn("[OptionScalpingStream] initial snapshot send failed: {}", ex.getMessage());
            emitters.remove(e);
        }
    }

    private void broadcast() {
        Map<String, Object> payload = currentState();
        if (payload == null) return;
        String json;
        try { json = mapper.writeValueAsString(payload); }
        catch (Exception e) {
            log.warn("[OptionScalpingStream] serialize failed (heartbeat will skip): {}", e.getMessage());
            return;
        }
        for (SseEmitter e : emitters) {
            try { e.send(SseEmitter.event().name("state").data(json)); }
            catch (IOException ex) {
                emitters.remove(e);
                try { e.complete(); } catch (Exception ignored) {}
            } catch (Exception ex) {
                log.debug("[OptionScalpingStream] emitter send failed (removing): {}", ex.getMessage());
                emitters.remove(e);
            }
        }
    }

    private Map<String, Object> currentState() {
        OptionScalping c = optionScalpingProvider.getIfAvailable();
        if (c == null) return null;
        try { return c.dashboardState(); }
        catch (Exception e) { log.warn("[OptionScalpingStream] dashboardState threw: {}", e.getMessage()); return null; }
    }
}
