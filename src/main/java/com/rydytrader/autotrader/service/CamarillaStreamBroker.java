package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.service.strategy.Camarilla;
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
 * Pushes Camarilla strategy state to every connected browser via Server-Sent Events.
 *
 * <p>The strategy calls {@link #publish()} whenever something material changes — a new entry,
 * an exit, an ATM shift, an event log line. A 2-second heartbeat ({@link #heartbeat()}) also
 * emits the latest payload so LTP/MTM keep refreshing on the open positions even when no
 * state mutation is happening.
 *
 * <p>The {@link Camarilla} strategy is injected lazily via {@link ObjectProvider} to break the
 * Camarilla → Broker → Camarilla circular dependency on bean construction.
 */
@Service
public class CamarillaStreamBroker {

    private static final Logger log = LoggerFactory.getLogger(CamarillaStreamBroker.class);

    private final ObjectProvider<Camarilla> camarillaProvider;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public CamarillaStreamBroker(ObjectProvider<Camarilla> camarillaProvider) {
        this.camarillaProvider = camarillaProvider;
    }

    public void addEmitter(SseEmitter e) {
        emitters.add(e);
        e.onCompletion(() -> emitters.remove(e));
        e.onTimeout(()    -> emitters.remove(e));
        e.onError(t       -> emitters.remove(e));
        // Send the latest snapshot immediately so the page populates without delay.
        sendSnapshot(e);
    }

    /** Called by the strategy on every state mutation. Broadcasts the dashboard JSON to all
     *  connected browsers. */
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
            if (payload != null) e.send(SseEmitter.event().name("state").data(mapper.writeValueAsString(payload)));
        } catch (Exception ex) {
            // Drop on failure — the emitter list will clean up via onError.
            emitters.remove(e);
        }
    }

    private void broadcast() {
        Map<String, Object> payload = currentState();
        if (payload == null) return;
        String json;
        try { json = mapper.writeValueAsString(payload); }
        catch (Exception e) { return; }
        for (SseEmitter e : emitters) {
            try { e.send(SseEmitter.event().name("state").data(json)); }
            catch (IOException ex) {
                emitters.remove(e);
                try { e.complete(); } catch (Exception ignored) {}
            } catch (Exception ex) {
                emitters.remove(e);
            }
        }
    }

    private Map<String, Object> currentState() {
        Camarilla c = camarillaProvider.getIfAvailable();
        if (c == null) return null;
        try { return c.dashboardState(); }
        catch (Exception e) { log.warn("[CamarillaStream] dashboardState threw: {}", e.getMessage()); return null; }
    }
}
