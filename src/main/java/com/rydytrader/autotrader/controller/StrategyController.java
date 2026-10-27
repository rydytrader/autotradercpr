package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.service.strategy.BalancedAtmSelector;
import com.rydytrader.autotrader.service.strategy.ShortStraddle;
import com.rydytrader.autotrader.service.strategy.ShortStrangle;
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
 * <p>Each endpoint resolves the {@link Strategy} via {@link StrategyRegistry#get(String)} and
 * delegates to the interface methods. Unknown strategy IDs return 404.
 */
@RestController
public class StrategyController {

    private final StrategyRegistry registry;
    private final com.rydytrader.autotrader.store.RiskSettingsStore riskSettings;

    public StrategyController(StrategyRegistry registry,
                              com.rydytrader.autotrader.store.RiskSettingsStore riskSettings) {
        this.registry = registry;
        this.riskSettings = riskSettings;
    }

    /** Aggregate day P&L + charges across every enabled strategy. Drives the
     *  header-strip P&L chip on all pages; polled every 2 s by ticker.js. */
    @GetMapping("/api/portfolio-pnl-today")
    public Map<String, Object> portfolioPnlToday() {
        double dayPnl = 0;
        double charges = 0;
        for (Strategy s : registry.all()) {
            // Skip disabled strategies — their live P&L shouldn't flow into the portfolio
            // ticker. Matches AnalyticsService.loadTrades which filters disabled strategies
            // out of all historical analytics.
            if (!s.isEnabled()) continue;
            try { dayPnl  += s.liveNetPnlToday(); } catch (Exception ignored) {}
            try { charges += s.liveChargesToday(); } catch (Exception ignored) {}
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("dayPnl",  Math.round(dayPnl * 100.0) / 100.0);
        out.put("charges", Math.round(charges * 100.0) / 100.0);
        // Portfolio kill-switch proximity — drives the amber/red warning on the header P&L chip.
        double maxLoss = riskSettings.getPortfolioMaxDailyLoss();
        double usedPct = (maxLoss > 0 && dayPnl < 0) ? (-dayPnl / maxLoss * 100.0) : 0;
        out.put("maxLoss",     Math.round(maxLoss * 100.0) / 100.0);
        out.put("lossUsedPct", Math.round(usedPct * 10.0) / 10.0);
        out.put("warnPct",     Math.round(com.rydytrader.autotrader.service.PortfolioRiskService.WARN_FRACTION * 100.0));
        out.put("riskWarn",    maxLoss > 0 && usedPct >= com.rydytrader.autotrader.service.PortfolioRiskService.WARN_FRACTION * 100.0);
        return out;
    }

    /** List of all registered strategies — used by the left sidebar to render nav icons. */
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
            ? "Strategy " + id + " state reset to ARMED. Next scheduler tick will evaluate entry time."
            : "Reset refused — legs are still open. Squareoff first.");
        return ResponseEntity.ok(out);
    }

    @PostMapping("/api/strategies/{id}/close")
    public ResponseEntity<Map<String, Object>> close(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        boolean ok = s.forceClose("MANUAL_DASHBOARD");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok);
        out.put("message", ok
            ? "Strategy " + id + " — legs flattened. State → DONE_FOR_DAY."
            : "Nothing to square off — no active position for " + id + ".");
        return ResponseEntity.ok(out);
    }

    /** Manual reset of the survivor's {@code slMovedToCost} flag — surfaced as a "reset"
     *  button next to the MOVED TO COST pill in the strategy dashboard. Live escape hatch
     *  for the pre-2026-09-29 bug where a same-strike re-entry left the survivor stuck at
     *  the cost-basis SL. */
    @PostMapping("/api/strategies/{id}/reset-cost-flag/{leg}")
    public ResponseEntity<Map<String, Object>> resetCostFlag(@PathVariable String id,
                                                             @PathVariable String leg) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        if (!"CE".equals(legUpper) && !"PE".equals(legUpper)) {
            return ResponseEntity.badRequest().body(java.util.Map.of(
                "success", false, "message", "leg must be 'ce' or 'pe'"));
        }
        boolean reset;
        if (s instanceof ShortStraddle ss)      reset = ss.resetLegCostFlag(legUpper);
        else if (s instanceof ShortStrangle sg) reset = sg.resetLegCostFlag(legUpper);
        else return ResponseEntity.badRequest().body(java.util.Map.of(
            "success", false, "message", "Strategy does not expose SL cost flag"));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", reset);
        out.put("message", reset
            ? legUpper + " leg SL cost flag cleared. Original SL restored."
            : legUpper + " leg is not currently at cost — nothing to reset.");
        return ResponseEntity.ok(out);
    }

    /** "+ OPEN CE / PE" button — opens a closed leg right now, regardless of re-execute cap or
     *  pending timer. The strategy decides whether it's allowed (day cycle started, leg closed,
     *  before squareoff). */
    @GetMapping("/api/strategies/{id}/hedge-candidates/{leg}")
    public ResponseEntity<Map<String, Object>> hedgeCandidates(@PathVariable String id, @PathVariable String leg) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        try { return ResponseEntity.ok(s.hedgeCandidates(leg == null ? "" : leg.toUpperCase())); }
        catch (Exception e) { return ResponseEntity.status(502).body(java.util.Map.of("error", String.valueOf(e.getMessage()))); }
    }

    @PostMapping("/api/strategies/{id}/hedge/{leg}/open")
    public ResponseEntity<Map<String, Object>> openHedge(@PathVariable String id, @PathVariable String leg,
                                                         @RequestParam(required = false) String symbol) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        String code;
        try { code = s.openHedgeLeg(legUpper, symbol); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"             -> legUpper + " hedge wing bought" + (symbol != null && !symbol.isEmpty() ? " at " + symbol : "") + ".";
            case "ALREADY_OPEN"   -> legUpper + " hedge wing is already open.";
            case "PAST_SQUAREOFF" -> "Refused — past squareoff time.";
            case "REJECTED"       -> legUpper + " hedge order was rejected by the broker — see event log.";
            case "UNSUPPORTED"    -> "This strategy doesn't support hedge wings.";
            default               -> legUpper + " hedge open failed (" + code + ").";
        };
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok); out.put("code", code); out.put("message", message);
        return ResponseEntity.ok(out);
    }

    @PostMapping("/api/strategies/{id}/hedge/{leg}/close")
    public ResponseEntity<Map<String, Object>> closeHedge(@PathVariable String id, @PathVariable String leg) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        String code;
        try { code = s.closeHedgeLeg(legUpper); } catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"       -> legUpper + " hedge wing sold.";
            case "NOT_OPEN" -> legUpper + " hedge wing is not open.";
            case "REJECTED" -> legUpper + " hedge sell was rejected by the broker — see event log.";
            default         -> legUpper + " hedge close failed (" + code + ").";
        };
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok); out.put("code", code); out.put("message", message);
        return ResponseEntity.ok(out);
    }

    /** +/- buttons on a leg's QTY pill. {@code dir} = add | remove. */
    @PostMapping("/api/strategies/{id}/lots/{leg}/{dir}")
    public ResponseEntity<Map<String, Object>> adjustLots(@PathVariable String id,
                                                          @PathVariable String leg,
                                                          @PathVariable String dir) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        int d = "add".equalsIgnoreCase(dir) ? 1 : "remove".equalsIgnoreCase(dir) ? -1 : 0;
        String code;
        try { code = s.adjustLegLots(legUpper, d); }
        catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code) || "CLOSED".equals(code);
        String message = switch (code) {
            case "OK"             -> legUpper + (d > 0 ? ": one lot added." : ": one lot exited.");
            case "CLOSED"         -> legUpper + ": last lot exited — leg closed.";
            case "NOT_OPEN"       -> legUpper + " leg is not open.";
            case "PAST_SQUAREOFF" -> "Refused — past squareoff time.";
            case "REJECTED"       -> legUpper + " order was rejected by the broker — see event log.";
            case "UNSUPPORTED"    -> "This strategy doesn't support lot adjustment.";
            default               -> legUpper + " lot adjust failed (" + code + ").";
        };
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok); out.put("code", code); out.put("message", message);
        return ResponseEntity.ok(out);
    }

    /** +/- buttons on a leg's SL trigger. {@code dir} = up | down | reset. */
    @PostMapping("/api/strategies/{id}/sl-adjust/{leg}/{dir}")
    public ResponseEntity<Map<String, Object>> adjustSl(@PathVariable String id,
                                                        @PathVariable String leg,
                                                        @PathVariable String dir) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        int d = "up".equalsIgnoreCase(dir) ? 1 : "down".equalsIgnoreCase(dir) ? -1 : 0;
        String code;
        try { code = s.adjustLegSl(legUpper, d); }
        catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"          -> d == 0 ? legUpper + " SL override cleared." : legUpper + " SL moved " + (d > 0 ? "up" : "down") + ".";
            case "NOT_OPEN"    -> legUpper + " leg is not open.";
            case "NO_TRIGGER"  -> legUpper + " has no SL trigger yet.";
            case "BELOW_LTP"   -> "Refused — that would put the SL at or below the current LTP.";
            case "UNSUPPORTED" -> "This strategy doesn't support manual SL.";
            default            -> legUpper + " SL adjust failed (" + code + ").";
        };
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok); out.put("code", code); out.put("message", message);
        return ResponseEntity.ok(out);
    }

    /** Strike choices for the "+ OPEN CE/PE" dialog. */
    @GetMapping("/api/strategies/{id}/open-leg-candidates/{leg}")
    public ResponseEntity<Map<String, Object>> openLegCandidates(@PathVariable String id,
                                                                 @PathVariable String leg) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        if (!"CE".equals(legUpper) && !"PE".equals(legUpper)) {
            return ResponseEntity.badRequest().body(java.util.Map.of("error", "leg must be 'ce' or 'pe'"));
        }
        try { return ResponseEntity.ok(s.openLegCandidates(legUpper)); }
        catch (Exception e) { return ResponseEntity.status(502).body(java.util.Map.of("error", String.valueOf(e.getMessage()))); }
    }

    @PostMapping("/api/strategies/{id}/open-leg/{leg}")
    public ResponseEntity<Map<String, Object>> openLeg(@PathVariable String id,
                                                       @PathVariable String leg,
                                                       @RequestParam(required = false) String symbol) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        if (!"CE".equals(legUpper) && !"PE".equals(legUpper)) {
            return ResponseEntity.badRequest().body(java.util.Map.of(
                "success", false, "message", "leg must be 'ce' or 'pe'"));
        }
        String code;
        try { code = s.openOneLeg(legUpper, symbol); }
        catch (Exception e) { code = "EXCEPTION: " + e.getMessage(); }
        boolean ok = "OK".equals(code);
        String message = switch (code) {
            case "OK"           -> legUpper + " leg opened" + (symbol != null && !symbol.isEmpty() ? " at " + symbol : " at the current strike pick") + ".";
            case "NOT_STARTED"  -> "Refused — today's cycle hasn't started yet. Use + NEW STRADDLE / STRANGLE for the first entry.";
            case "ALREADY_OPEN" -> legUpper + " leg is already open.";
            case "REJECTED"     -> legUpper + " order was rejected by the broker — see event log.";
            case "UNSUPPORTED"  -> "This strategy doesn't support opening a single leg.";
            default             -> legUpper + " open aborted (" + code + ") — see event log.";
        };
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok);
        out.put("code", code);
        out.put("message", message);
        return ResponseEntity.ok(out);
    }

    @PostMapping("/api/strategies/{id}/close-leg/{leg}")
    public ResponseEntity<Map<String, Object>> closeLeg(@PathVariable String id,
                                                        @PathVariable String leg) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String legUpper = leg == null ? "" : leg.toUpperCase();
        if (!"CE".equals(legUpper) && !"PE".equals(legUpper)) {
            return ResponseEntity.badRequest().body(java.util.Map.of(
                "success", false, "message", "leg must be 'ce' or 'pe'"));
        }
        boolean ok = s.closeOneLeg(legUpper, "MANUAL_DASHBOARD");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", ok);
        out.put("message", ok
            ? legUpper + " leg close submitted for " + id + "."
            : "Nothing to close — " + legUpper + " leg is not currently open.");
        return ResponseEntity.ok(out);
    }

    /** Live preview of the balanced-ATM selection for the + NEW STRADDLE confirm modal.
     *  The UI fetches this so the modal can show the chosen strike + CE/PE LTPs before the
     *  operator commits. Backed by {@link ShortStraddle#getAtmPreview()} (30 s cache). */
    @GetMapping("/api/strategies/{id}/atm-preview")
    public ResponseEntity<Map<String, Object>> atmPreview(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        if (!(s instanceof ShortStraddle straddle)) {
            return ResponseEntity.badRequest().body(Map.of(
                "supported", false, "message", "Strategy does not support ATM preview"));
        }
        BalancedAtmSelector.AtmSelection sel = straddle.getAtmPreview();
        Map<String, Object> out = new LinkedHashMap<>();
        if (sel == null) {
            out.put("supported", true);
            out.put("available", false);
            out.put("message",   "ATM preview unavailable — NIFTY LTP or option chain not ready");
            return ResponseEntity.ok(out);
        }
        out.put("supported",  true);
        out.put("available",  true);
        out.put("spotAtm",    sel.spotAtm());
        out.put("chosenAtm",  sel.chosenAtm());
        out.put("ceLtp",      sel.ceLtpAtChosen());
        out.put("peLtp",      sel.peLtpAtChosen());
        out.put("premiumGap", sel.premiumGapAtChosen());
        out.put("ceSymbol",   sel.ceSymbolAtChosen());
        out.put("peSymbol",   sel.peSymbolAtChosen());
        return ResponseEntity.ok(out);
    }

    @PostMapping("/api/strategies/{id}/restart")
    public ResponseEntity<Map<String, Object>> restart(@PathVariable String id) {
        Strategy s = registry.get(id);
        if (s == null) return ResponseEntity.notFound().build();
        String code;
        try {
            code = s.restartFromDoneForDay("MANUAL_DASHBOARD");
        } catch (Exception e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("success", false);
            err.put("code", "EXCEPTION");
            err.put("message", "Restart failed: " + e.getMessage());
            return ResponseEntity.status(500).body(err);
        }
        if ("OK".equals(code)) {
            Map<String, Object> ok = new LinkedHashMap<>();
            ok.put("success", true);
            ok.put("code", "OK");
            ok.put("message", "New straddle entered for " + id + ".");
            return ResponseEntity.ok(ok);
        }
        String human = switch (code) {
            case "NOT_DONE_FOR_DAY"     -> "Strategy is not in DONE_FOR_DAY — nothing to restart.";
            case "DAY_DISABLED"         -> "Today is disabled in the per-day SL config.";
            case "TRADING_PAUSED"       -> "Trading is paused — toggle the Today switch off to allow new entries.";
            case "BEFORE_ENTRY_TIME"    -> "Too early — entry-time window hasn't opened yet.";
            case "AFTER_SQUAREOFF_TIME" -> "Too late — past the squareoff time.";
            case "PENDING_FILLS"        -> "Prior close fill still in flight — try again in a moment.";
            case "UNSUPPORTED"          -> "This strategy does not support same-day restart.";
            default                     -> "Restart blocked: " + code;
        };
        Map<String, Object> blocked = new LinkedHashMap<>();
        blocked.put("success", false);
        blocked.put("code", code);
        blocked.put("message", human);
        return ResponseEntity.status(409).body(blocked);
    }
}
