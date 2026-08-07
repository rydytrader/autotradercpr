package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.gdfl.GdflService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Operator-only diagnostic endpoints for probing the GDFL WebSocket. Used
 * to discover unknown InstrumentIdentifiers (e.g. what GDFL calls NIFTY 50
 * on the NSE_IDX exchange) before wiring them into the strategy code.
 *
 * <p>Every endpoint here talks to the LIVE WS — do not hit them during
 * market hours unless you understand the effect on the 50-symbol cap.
 */
@RestController
@RequestMapping("/api/gdfl/diag")
public class GdflDiagnosticController {

    private final GdflService gdflService;

    public GdflDiagnosticController(GdflService gdflService) {
        this.gdflService = gdflService;
    }

    /** Fires GDFL's {@code GetInstrumentsOnSearch} with the given query on the
     *  given exchange and waits up to {@code waitMs} for a response frame to
     *  land in the WS's unknown-frames ring, then returns everything currently
     *  in that ring. The response frame's exact {@code MessageType} varies by
     *  GDFL API version, so we don't filter — the operator reads the raw
     *  payloads and picks out the {@code InstrumentIdentifier} strings. */
    @GetMapping("/search")
    public Map<String, Object> search(@RequestParam(defaultValue = "NSE_IDX") String exchange,
                                      @RequestParam(defaultValue = "NIFTY") String q,
                                      @RequestParam(defaultValue = "2000")  long waitMs) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("connectionStatus", gdflService.connectionStatus());
        String payload = "{\"MessageType\":\"GetInstrumentsOnSearch\","
                       + "\"Exchange\":\""            + escape(exchange) + "\","
                       + "\"Search\":\""              + escape(q)        + "\"}";
        out.put("sent", payload);
        boolean ok = gdflService.sendRaw(payload);
        out.put("sendOk", ok);
        if (ok && waitMs > 0) {
            try { Thread.sleep(Math.min(waitMs, 10_000)); } catch (InterruptedException ignored) {}
        }
        out.put("frames", gdflService.recentUnknownFrames());
        return out;
    }

    /** Returns the most-recent unhandled GDFL frames without sending anything.
     *  Useful for reading responses to a previous {@code /search} call, or
     *  for browsing whatever error / ack frames the server has emitted. */
    @GetMapping("/frames")
    public Map<String, Object> frames() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("connectionStatus", gdflService.connectionStatus());
        List<String> frames = gdflService.recentUnknownFrames();
        out.put("count",  frames.size());
        out.put("frames", frames);
        return out;
    }

    /** Sends an arbitrary GDFL payload — for probing message types this
     *  controller doesn't wrap. The response (if any) lands in the frames
     *  ring, readable via {@link #frames()}. */
    @GetMapping("/send-raw")
    public Map<String, Object> sendRaw(@RequestParam String payload) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("connectionStatus", gdflService.connectionStatus());
        out.put("sent",   payload);
        out.put("sendOk", gdflService.sendRaw(payload));
        return out;
    }

    private static String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
