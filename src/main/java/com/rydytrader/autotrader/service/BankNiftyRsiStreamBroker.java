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
 * BANKNIFTY companion of {@link NiftyRsiStreamBroker} — pushes the live
 * BANKNIFTY RSI tip to every connected browser via Server-Sent Events on the
 * same 1 Hz cadence. Consumed by the second RSI chart on the trade page.
 */
@Service
public class BankNiftyRsiStreamBroker {

    private static final Logger log = LoggerFactory.getLogger(BankNiftyRsiStreamBroker.class);

    private final BankNiftyRsiService rsi;
    private final ObjectMapper        mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .findAndRegisterModules();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public BankNiftyRsiStreamBroker(BankNiftyRsiService rsi) {
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
            log.warn("[BankNiftyRsiStream] initial snapshot send failed: {}", ex.getMessage());
            emitters.remove(e);
        }
    }

    private void broadcast() {
        String json;
        try { json = mapper.writeValueAsString(rsi.liveTip()); }
        catch (Exception e) {
            log.warn("[BankNiftyRsiStream] serialize failed: {}", e.getMessage());
            return;
        }
        for (SseEmitter e : emitters) {
            try { e.send(SseEmitter.event().name("tip").data(json)); }
            catch (IOException ex) {
                emitters.remove(e);
                try { e.complete(); } catch (Exception ignored) {}
            } catch (Exception ex) {
                log.debug("[BankNiftyRsiStream] emitter send failed (removing): {}", ex.getMessage());
                emitters.remove(e);
            }
        }
    }
}
