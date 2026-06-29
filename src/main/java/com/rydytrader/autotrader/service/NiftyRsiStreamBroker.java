package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Pushes the live NIFTY RSI tip to every connected browser via Server-Sent Events.
 *
 * <p>Modelled on {@link CamarillaStreamBroker} — same emitter fan-out + scheduled-broadcast
 * pattern. The strategy doesn't drive pushes here; a 1 Hz timer queries
 * {@link NiftyRsiService#liveTip()} and broadcasts the snapshot. The snapshot carries both
 * the in-progress 5-min bucket's live LTP-projected RSI ("tip") and the most recently
 * closed bucket's canonical RSI ("lastClosed") so the frontend can overwrite both cells
 * each push and avoid drift at the 5-min bar boundary.
 *
 * <p>The scheduled broadcast is a no-op when no browsers are connected — the panel doesn't
 * cost anything to keep available.
 */
@Service
public class NiftyRsiStreamBroker {

    private static final Logger log = LoggerFactory.getLogger(NiftyRsiStreamBroker.class);

    private final NiftyRsiService rsi;
    private final ObjectMapper    mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .findAndRegisterModules();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public NiftyRsiStreamBroker(NiftyRsiService rsi) {
        this.rsi = rsi;
    }

    public void addEmitter(SseEmitter e) {
        emitters.add(e);
        e.onCompletion(() -> emitters.remove(e));
        e.onTimeout(()    -> emitters.remove(e));
        e.onError(t       -> emitters.remove(e));
        sendSnapshot(e);
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 2000)
    public void heartbeat() {
        if (emitters.isEmpty()) return;
        broadcast();
    }

    private void sendSnapshot(SseEmitter e) {
        try {
            String json = mapper.writeValueAsString(rsi.liveTip());
            e.send(SseEmitter.event().name("tip").data(json));
        } catch (Exception ex) {
            log.warn("[NiftyRsiStream] initial snapshot send failed: {}", ex.getMessage());
            emitters.remove(e);
        }
    }

    private void broadcast() {
        String json;
        try { json = mapper.writeValueAsString(rsi.liveTip()); }
        catch (Exception e) {
            log.warn("[NiftyRsiStream] serialize failed: {}", e.getMessage());
            return;
        }
        for (SseEmitter e : emitters) {
            try { e.send(SseEmitter.event().name("tip").data(json)); }
            catch (IOException ex) {
                emitters.remove(e);
                try { e.complete(); } catch (Exception ignored) {}
            } catch (Exception ex) {
                log.debug("[NiftyRsiStream] emitter send failed (removing): {}", ex.getMessage());
                emitters.remove(e);
            }
        }
    }
}
