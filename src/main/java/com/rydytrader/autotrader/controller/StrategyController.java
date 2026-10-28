package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.strategy.Strategy;
import com.rydytrader.autotrader.service.strategy.StrategyRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic per-strategy REST endpoints. The dashboard JS for {@code /strategies/{id}} polls
 * these instead of any strategy-specific routes — the same JS file works for every strategy.
 *
 * <p>Leg-level endpoints address a leg by the {@code id} the dashboard payload's {@code legs}
 * list carries ({@code L1}, {@code L2}, …). "Open" endpoints take a side ({@code ce} / {@code pe}).
 */
@RestController
public class StrategyController {

    private final StrategyRegistry registry;
    private final com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;
    private final com.rydytrader.autotrader.service.strategy.PayoffService payoffService;

    public StrategyController(StrategyRegistry registry,
                              com.rydytrader.autotrader.store.RiskSettingsStore riskSettings,
                              com.rydytrader.autotrader.service.strategy.PayoffService payoffService) {
        this.registry = registry;
        this.riskSettings = riskSettings;
        this.payoffService = payoffService;
    }

    // ── SIMULATE modal (read-only — no orders) ────────────────────────────────
    @GetMapping("/api/strategies/{id}/sim/context")
    public ResponseEntity<Map<String, Object>> simContext(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        try { return ResponseEntity.ok(s.simContext()); }
        catch (Exception e) { return ResponseEntity.status(502).body(Map.of("error", String.valueOf(e.getMessage()))); }
    }

    @GetMapping("/api/strategies/{id}/sim/chain")
    public ResponseEntity<Map<String, Object>> simChain(@PathVariable String id, @RequestParam(required = false) String expiry) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        try { return ResponseEntity.ok(s.simChain(expiry)); }
        catch (Exception e) { return ResponseEntity.status(502).body(Map.of("error", String.valueOf(e.getMessage()))); }
    }

    @PostMapping("/api/strategies/{id}/sim/margin")
    public ResponseEntity<Map<String, Object>> simMargin(@PathVariable String id,
                                                         @RequestParam(required = false) String product,
                                                         @RequestBody com.rydytrader.autotrader.service.strategy.PayoffService.Request body) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        try { return ResponseEntity.ok(s.simMargin(body.legs, product)); }
        catch (Exception e) { return ResponseEntity.ok(Map.of("error", String.valueOf(e.getMessage()))); }
    }

    @PostMapping("/api/strategies/{id}/sim/execute")
    public ResponseEntity<Map<String, Object>> simExecute(@PathVariable String id,
                                                          @RequestParam(required = false) String product,
                                                          @RequestParam(required = false, defaultValue = "-1") double slPct,
                                                          @RequestBody com.rydytrader.autotrader.service.strategy.PayoffService.Request body) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        List<Map<String, Object>> results;
        try { results = s.simExecute(body.legs, product, slPct); }
        catch (Exception e) { return ResponseEntity.ok(result(false, "EXCEPTION", "Execute failed: " + e.getMessage())); }
        int ok = 0;
        for (Map<String, Object> r : results) if (Boolean.TRUE.equals(r.get("ok"))) ok++;
        Map<String, Object> out = result(ok == results.size() && !results.isEmpty(), ok == results.size() ? "OK" : "PARTIAL",
            results.isEmpty() ? "Nothing to execute — no SIM rows." : ok + " of " + results.size() + " order(s) placed.");
        out.put("results", results);
        return ResponseEntity.ok(out);
    }

    @PostMapping("/api/strategies/{id}/sim/payoff")
    public ResponseEntity<Map<String, Object>> simPayoff(@PathVariable String id,
                                                         @RequestBody com.rydytrader.autotrader.service.strategy.PayoffService.Request body) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        try {
            Map<String, Object> ctx = s.simContext();
            double spot = ctx.get("spot") instanceof Number n ? n.doubleValue() : 0;
            int lot = ctx.get("lotSize") instanceof Number n ? n.intValue() : 1;
            return ResponseEntity.ok(payoffService.compute(body, spot, lot));
        } catch (Exception e) {
            return ResponseEntity.status(502).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** Aggregate day P&L + charges across every enabled strategy — header-strip P&L chip. */
    @GetMapping("/api/portfolio-pnl-today")
    public Map<String, Object> portfolioPnlToday() {
        double dayPnl = 0;
        double charges = 0;
        for (Strategy s : registry.all()) {
            if (!s.isEnabled()) continue;
            try { dayPnl  += s.liveNetPnlToday(); } catch (Exception ignored) {}
            try { charges += s.liveChargesToday(); } catch (Exception ignored) {}
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("dayPnl",  Math.round(dayPnl * 100.0) / 100.0);
        out.put("charges", Math.round(charges * 100.0) / 100.0);
        double maxLoss = riskSettings.getPortfolioMaxDailyLoss();
        double usedPct = (maxLoss > 0 && dayPnl < 0) ? (-dayPnl / maxLoss * 100.0) : 0;
        out.put("maxLoss",     Math.round(maxLoss * 100.0) / 100.0);
        out.put("lossUsedPct", Math.round(usedPct * 10.0) / 10.0);
        out.put("warnPct",     Math.round(com.rydytrader.autotrader.service.PortfolioRiskService.WARN_FRACTION * 100.0));
        out.put("riskWarn",    maxLoss > 0 && usedPct >= com.rydytrader.autotrader.service.PortfolioRiskService.WARN_FRACTION * 100.0);
        return out;
    }

    @GetMapping("/api/strategies")
    public List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Strategy s : registry.all()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", s.id());
            m.put("displayName", s.displayName());
            m.put("description", s.description());
            m.put("shortCode", s.shortCode());
            m.put("enabled", s.isEnabled());
            m.put("currentState", s.currentState());
            m.put("navIcon", s.navIcon());
            m.put("underlying", s.underlyingName());
            m.put("type", s.strategyType());
            out.add(m);
        }
        return out;
    }

    @GetMapping("/api/strategies/{id}/status")
    public ResponseEntity<Map<String, Object>> status(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.getStatus());
    }

    @GetMapping("/api/strategies/{id}/dashboard")
    public ResponseEntity<Map<String, Object>> dashboard(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.getDashboard());
    }

    @GetMapping("/api/strategies/{id}/settings/schema")
    public ResponseEntity<List<Map<String, Object>>> settingsSchema(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.getSettingsSchema());
    }

    @GetMapping("/api/strategies/{id}/settings")
    public ResponseEntity<Map<String, Object>> getSettings(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.getSettingsValues());
    }

    @PostMapping("/api/strategies/{id}/settings")
    public ResponseEntity<Map<String, Object>> saveSettings(@PathVariable String id,
                                                            @RequestBody Map<String, Object> values) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        s.saveSettings(values);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", true);
        out.put("message", "Settings saved for " + id);
        return ResponseEntity.ok(out);
    }

    @PostMapping("/api/strategies/{id}/reset")
    public ResponseEntity<Map<String, Object>> reset(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        boolean ok = s.resetToIdle("manual");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok);
        out.put("message", ok
            ? "Strategy " + id + " state reset to ARMED."
            : "Reset refused — legs are still open. Squareoff first.");
        return ResponseEntity.ok(out);
    }

    /** Square off every open leg (short + hedge). */
    @PostMapping("/api/strategies/{id}/close")
    public ResponseEntity<Map<String, Object>> close(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        boolean ok = s.forceClose("MANUAL_DASHBOARD");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok);
        out.put("message", ok
            ? "Strategy " + id + " — all legs flattened. State → DONE_FOR_DAY."
            : "Nothing to square off — no open legs for " + id + ".");
        return ResponseEntity.ok(out);
    }

    // ── Opening legs ─────────────────────────────────────────────────────────
    @GetMapping("/api/strategies/{id}/open-leg-candidates/{side}")
    public ResponseEntity<Map<String, Object>> openLegCandidates(@PathVariable String id, @PathVariable String side,
                                                                 @RequestParam(required = false) String expiry) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String up = side == null ? "" : side.toUpperCase();
        if (!"CE".equals(up) && !"PE".equals(up)) return ResponseEntity.badRequest().body(Map.of("error", "side must be 'ce' or 'pe'"));
        try { return ResponseEntity.ok(s.openLegCandidates(up, expiry)); }
        catch (Exception e) { return ResponseEntity.status(502).body(Map.of("error", String.valueOf(e.getMessage()))); }
    }

    @PostMapping("/api/strategies/{id}/open-leg/{side}")
    public ResponseEntity<Map<String, Object>> openLeg(@PathVariable String id, @PathVariable String side,
                                                       @RequestParam(required = false) String symbol,
                                                       @RequestParam(required = false, defaultValue = "INTRADAY") String product,
                                                       @RequestParam(required = false, defaultValue = "1") int lots,
                                                       @RequestParam(required = false, defaultValue = "0") double slPct) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String up = side == null ? "" : side.toUpperCase();
        if (!"CE".equals(up) && !"PE".equals(up)) return ResponseEntity.badRequest().body(Map.of("success", false, "message", "side must be 'ce' or 'pe'"));
        String code;
        try { code = s.openOneLeg(up, symbol, product, lots, slPct); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"             -> "SHORT " + up + " sold" + (symbol != null && !symbol.isEmpty() ? " at " + symbol : "") + ".";
            case "MARKET_CLOSED"  -> "Refused — market is closed.";
            case "PAST_SQUAREOFF" -> "Refused — past the 15:20 intraday squareoff; use OVERNIGHT.";
            case "REJECTED"       -> up + " order was rejected by the broker — see event log.";
            case "INVALID"        -> "Refused — symbol does not match the side.";
            case "UNSUPPORTED"    -> "This strategy doesn't support opening legs.";
            default               -> up + " open aborted (" + code + ") — see event log.";
        };
        return ResponseEntity.ok(result(ok, code, message));
    }

    @GetMapping("/api/strategies/{id}/pair-candidates")
    public ResponseEntity<Map<String, Object>> pairCandidates(@PathVariable String id, @RequestParam(required = false) String expiry) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        try { return ResponseEntity.ok(s.pairCandidates(expiry)); }
        catch (Exception e) { return ResponseEntity.status(502).body(Map.of("error", String.valueOf(e.getMessage()))); }
    }

    /** + STRADDLE / + STRANGLE: sell a CE and a PE in one go. */
    @PostMapping("/api/strategies/{id}/open-pair")
    public ResponseEntity<Map<String, Object>> openPair(@PathVariable String id,
                                                        @RequestParam String ceSymbol,
                                                        @RequestParam String peSymbol,
                                                        @RequestParam(required = false, defaultValue = "INTRADAY") String product,
                                                        @RequestParam(required = false, defaultValue = "1") int lots,
                                                        @RequestParam(required = false, defaultValue = "0") double slPct) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        Map<String, String> codes;
        try { codes = s.openPair(ceSymbol, peSymbol, product, lots, slPct); }
        catch (Exception e) { codes = Map.of("ce", "EXCEPTION: " + e.getMessage(), "pe", "SKIPPED"); }
        String ce = codes.getOrDefault("ce", "UNKNOWN"), pe = codes.getOrDefault("pe", "UNKNOWN");
        boolean ok = "OK".equals(ce) && "OK".equals(pe);
        String message = ok ? "CE and PE sold."
            : ("OK".equals(ce) ? "CE sold; PE failed (" + pe + ")."
            : "OK".equals(pe) ? "PE sold; CE failed (" + ce + ")."
            : "Both legs failed (CE " + ce + ", PE " + pe + ").");
        Map<String, Object> out = result(ok, ce + "/" + pe, message);
        out.put("ce", ce); out.put("pe", pe);
        return ResponseEntity.ok(out);
    }

    @GetMapping("/api/strategies/{id}/hedge-candidates/{side}")
    public ResponseEntity<Map<String, Object>> hedgeCandidates(@PathVariable String id, @PathVariable String side,
                                                               @RequestParam(required = false) String expiry) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String up = side == null ? "" : side.toUpperCase();
        if (!"CE".equals(up) && !"PE".equals(up)) return ResponseEntity.badRequest().body(Map.of("error", "side must be 'ce' or 'pe'"));
        try { return ResponseEntity.ok(s.hedgeCandidates(up, expiry)); }
        catch (Exception e) { return ResponseEntity.status(502).body(Map.of("error", String.valueOf(e.getMessage()))); }
    }

    @PostMapping("/api/strategies/{id}/hedge/{side}/open")
    public ResponseEntity<Map<String, Object>> openHedge(@PathVariable String id, @PathVariable String side,
                                                         @RequestParam(required = false) String symbol,
                                                         @RequestParam(required = false, defaultValue = "INTRADAY") String product,
                                                         @RequestParam(required = false, defaultValue = "1") int lots) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String up = side == null ? "" : side.toUpperCase();
        String code;
        try { code = s.openHedgeLeg(up, symbol, product, lots); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"             -> "HEDGE " + up + " bought" + (symbol != null && !symbol.isEmpty() ? " at " + symbol : "") + ".";
            case "MARKET_CLOSED"  -> "Refused — market is closed.";
            case "PAST_SQUAREOFF" -> "Refused — past the 15:20 intraday squareoff; use OVERNIGHT.";
            case "REJECTED"       -> up + " hedge order was rejected by the broker — see event log.";
            case "UNSUPPORTED"    -> "This strategy doesn't support hedge legs.";
            default               -> up + " hedge open failed (" + code + ").";
        };
        return ResponseEntity.ok(result(ok, code, message));
    }

    // ── Per-leg actions (by leg id) ───────────────────────────────────────────
    @PostMapping("/api/strategies/{id}/leg/{legId}/reopen")
    public ResponseEntity<Map<String, Object>> reopenLeg(@PathVariable String id, @PathVariable String legId,
                                                         @RequestParam(required = false) String product,
                                                         @RequestParam(required = false, defaultValue = "0") int lots,
                                                         @RequestParam(required = false, defaultValue = "-1") double slPct) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String code;
        try { code = s.reopenLeg(legId, product, lots, slPct); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"             -> "Leg re-opened.";
            case "NOT_FOUND"      -> "Leg not found.";
            case "ALREADY_OPEN"   -> "That leg is still open.";
            case "EXPIRED"        -> "Refused — that contract has expired.";
            case "MARKET_CLOSED"  -> "Refused — market is closed.";
            case "PAST_SQUAREOFF" -> "Refused — past the intraday squareoff; use OVERNIGHT.";
            case "REJECTED"       -> "Order was rejected by the broker — see event log.";
            case "ABORTED"        -> "Could not place the order — see event log.";
            case "UNSUPPORTED"    -> "This strategy doesn't support re-opening legs.";
            default                -> "Re-open failed (" + code + ").";
        };
        return ResponseEntity.ok(result(ok, code, message));
    }

    @PostMapping("/api/strategies/{id}/leg/{legId}/close")
    public ResponseEntity<Map<String, Object>> closeLeg(@PathVariable String id, @PathVariable String legId) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        boolean ok = s.closeOneLeg(legId, "MANUAL_DASHBOARD");
        return ResponseEntity.ok(result(ok, ok ? "OK" : "NOT_OPEN",
            ok ? "Close submitted for leg " + legId + "." : "Nothing to close — leg " + legId + " is not open."));
    }

    @PostMapping("/api/strategies/{id}/leg/{legId}/lots/{dir}")
    public ResponseEntity<Map<String, Object>> adjustLots(@PathVariable String id, @PathVariable String legId,
                                                          @PathVariable String dir) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        int d = "add".equalsIgnoreCase(dir) ? 1 : "remove".equalsIgnoreCase(dir) ? -1 : 0;
        String code;
        try { code = s.adjustLegLots(legId, d); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code) || "CLOSED".equals(code);
        String message = switch (code) {
            case "OK"             -> d > 0 ? "One lot added." : "One lot exited.";
            case "CLOSED"         -> "Last lot exited — leg closed.";
            case "NOT_OPEN"       -> "Leg is not open.";
            case "PAST_SQUAREOFF" -> "Refused — past the 15:20 intraday squareoff.";
            case "REJECTED"       -> "Order was rejected by the broker — see event log.";
            case "UNSUPPORTED"    -> "This strategy doesn't support lot adjustment.";
            default               -> "Lot adjust failed (" + code + ").";
        };
        return ResponseEntity.ok(result(ok, code, message));
    }

    @PostMapping("/api/strategies/{id}/leg/{legId}/sl/{dir}")
    public ResponseEntity<Map<String, Object>> adjustSl(@PathVariable String id, @PathVariable String legId,
                                                        @PathVariable String dir) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        int d = "up".equalsIgnoreCase(dir) ? 1 : "down".equalsIgnoreCase(dir) ? -1 : 0;
        String code;
        try { code = s.adjustLegSl(legId, d); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"          -> d == 0 ? "SL override cleared." : "SL moved " + (d > 0 ? "up" : "down") + " ₹1.";
            case "NOT_OPEN"    -> "Leg is not open.";
            case "NO_TRIGGER"  -> "This leg has no SL trigger.";
            case "BELOW_LTP"   -> "Refused — that would put the SL at or below the current LTP.";
            case "UNSUPPORTED" -> "This strategy doesn't support manual SL.";
            default            -> "SL adjust failed (" + code + ").";
        };
        return ResponseEntity.ok(result(ok, code, message));
    }

    @PostMapping("/api/strategies/{id}/leg/{legId}/sl-set")
    public ResponseEntity<Map<String, Object>> setSl(@PathVariable String id, @PathVariable String legId,
                                                     @RequestParam double value) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String code;
        try { code = s.setLegSl(legId, value); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code) || "CLEARED".equals(code);
        String message = switch (code) {
            case "OK"          -> "SL set to " + String.format("%.2f", value) + ".";
            case "CLEARED"     -> "SL removed — this leg has no stop.";
            case "NOT_OPEN"    -> "Leg is not open.";
            case "NO_TRIGGER"  -> "Only short legs have an SL.";
            case "INVALID"     -> "Enter a price, or 0 to remove the SL.";
            case "BELOW_LTP"   -> "Refused — that would put the SL at or below the current LTP.";
            case "UNSUPPORTED" -> "This strategy doesn't support manual SL.";
            default             -> "SL set failed (" + code + ").";
        };
        return ResponseEntity.ok(result(ok, code, message));
    }

    @PostMapping("/api/strategies/{id}/leg/{legId}/cost")
    public ResponseEntity<Map<String, Object>> toggleCost(@PathVariable String id, @PathVariable String legId) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String code;
        try { code = s.toggleLegCost(legId); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"              -> "SL cost toggle applied.";
            case "NOT_OPEN"        -> "Leg is not open.";
            case "NO_TRIGGER"      -> "Only short legs have an SL.";
            case "LTP_ABOVE_ENTRY" -> "Refused — LTP is already at or above the entry premium; the leg would close immediately. Use ✕ to close it.";
            case "UNSUPPORTED"     -> "This strategy doesn't support move-to-cost.";
            default                -> "Cost toggle failed (" + code + ").";
        };
        return ResponseEntity.ok(result(ok, code, message));
    }

    @GetMapping("/api/strategies/{id}/leg/{legId}/chart")
    public ResponseEntity<Map<String, Object>> legChart(@PathVariable String id, @PathVariable String legId) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        Map<String, Object> out = s.legChart(legId);
        if (out == null || out.isEmpty()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(out);
    }

    private static Map<String, Object> result(boolean ok, String code, String message) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok); out.put("code", code); out.put("message", message);
        return out;
    }
}
