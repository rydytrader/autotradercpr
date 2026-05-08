package com.rydytrader.autotrader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.entity.SettingEntity;
import com.rydytrader.autotrader.repository.SettingRepository;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tracks NIFTY's <b>Virgin CPR</b> — when the index's session range never overlapped
 * today's daily CPR (BC..TC) by 15:30 IST, the day's CPR levels (TC, Pivot, BC) are
 * cached and act as additional hurdles in the NIFTY 5m hurdle filter for the next
 * {@code virginCprExpiryDays} <b>trading</b> days. (Not added to the 1h HTF hurdle
 * filter — daily-level concept stays at the daily-CPR gate.) A new virgin CPR
 * replaces any existing active record.
 *
 * <p>Persisted as a single JSON blob in the {@code settings} table under key
 * {@value #STATE_KEY} so the state survives restarts.
 */
@Service
public class VirginCprService {

    private static final Logger log = LoggerFactory.getLogger(VirginCprService.class);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String STATE_KEY = "virginCprState";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static class Snapshot {
        public String date;   // ISO yyyy-MM-dd — day on which the virgin CPR formed
        public double tc;
        public double pivot;
        public double bc;
    }

    @Autowired private SettingRepository settingRepo;
    @Autowired private RiskSettingsStore riskSettings;
    @Autowired private CandleAggregator candleAggregator;
    @Autowired private BhavcopyService bhavcopyService;
    @Autowired private MarketHolidayService marketHolidayService;
    @Autowired private EventService eventService;

    private volatile Snapshot snapshot;

    @PostConstruct
    public void init() {
        load();
    }

    /**
     * Runs at 15:30:30 IST on weekdays. After the session closes, check if NIFTY's
     * range today overlapped daily CPR. If not, form a virgin CPR (replaces any
     * existing record).
     */
    @Scheduled(cron = "30 30 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledDetect() {
        if (marketHolidayService != null && !marketHolidayService.isTradingDay()) return;
        detect();
    }

    /** Manual trigger — for diagnostics / out-of-hours testing. */
    public void triggerDetect() { detect(); }

    private synchronized void detect() {
        try {
            String niftySym = IndexTrendService.NIFTY_SYMBOL;
            double sessionHigh = candleAggregator.getDayHighExcluding(niftySym, null);
            double sessionLow  = candleAggregator.getDayLowExcluding(niftySym, null);
            if (sessionHigh <= 0 || sessionLow <= 0) {
                log.info("[VirginCPR] Session H/L not available — skipping detection (high={}, low={})", sessionHigh, sessionLow);
                return;
            }

            CprLevels cpr = bhavcopyService.getCprLevels("NIFTY50");
            if (cpr == null || cpr.getTc() <= 0 || cpr.getBc() <= 0) {
                log.warn("[VirginCPR] NIFTY CPR not loaded — skipping detection");
                return;
            }
            double cprTop = Math.max(cpr.getTc(), cpr.getBc());
            double cprBot = Math.min(cpr.getTc(), cpr.getBc());

            boolean untouched = sessionLow > cprTop || sessionHigh < cprBot;
            String today = LocalDate.now(IST).toString();
            if (!untouched) {
                log.info("[VirginCPR] {} CPR was touched (session H={} L={}, CPR {}—{}) — no virgin CPR formed",
                    today, fmt(sessionHigh), fmt(sessionLow), fmt(cprBot), fmt(cprTop));
                return;
            }

            Snapshot fresh = new Snapshot();
            fresh.date = today;
            fresh.tc = cpr.getTc();
            fresh.pivot = cpr.getPivot();
            fresh.bc = cpr.getBc();
            this.snapshot = fresh;
            save();
            log.info("[VirginCPR] Formed — date={} TC={} Pivot={} BC={} (session H={} L={}, CPR {}—{})",
                fresh.date, fmt(fresh.tc), fmt(fresh.pivot), fmt(fresh.bc),
                fmt(sessionHigh), fmt(sessionLow), fmt(cprBot), fmt(cprTop));
            eventService.log("[INFO] NIFTY Virgin CPR formed: TC=" + fmt(fresh.tc)
                + " Pivot=" + fmt(fresh.pivot) + " BC=" + fmt(fresh.bc));
        } catch (Exception e) {
            log.warn("[VirginCPR] Detection failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Returns the current active virgin CPR (within the configured trading-day expiry
     * window) or null if expired / never formed / feature disabled.
     */
    public Snapshot getActiveVirginCpr() {
        Snapshot s = snapshot;
        if (s == null || s.date == null) return null;
        int expiryDays = riskSettings != null ? riskSettings.getVirginCprExpiryDays() : 0;
        if (expiryDays <= 0) return null;
        LocalDate formed;
        try { formed = LocalDate.parse(s.date); } catch (Exception e) { return null; }
        int tradingDaysSince = countTradingDaysAfter(formed, LocalDate.now(IST));
        if (tradingDaysSince > expiryDays) return null;
        return s;
    }

    /**
     * UI-friendly map of the active virgin CPR (date, levels, days remaining) or null
     * if no active record.
     */
    public Map<String, Object> getActiveStatus() {
        Snapshot s = getActiveVirginCpr();
        if (s == null) return null;
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("date", s.date);
        r.put("tc", s.tc);
        r.put("pivot", s.pivot);
        r.put("bc", s.bc);
        try {
            int used = countTradingDaysAfter(LocalDate.parse(s.date), LocalDate.now(IST));
            int remaining = Math.max(0, riskSettings.getVirginCprExpiryDays() - used);
            r.put("tradingDaysSince", used);
            r.put("daysRemaining", remaining);
        } catch (Exception e) {
            r.put("tradingDaysSince", 0);
            r.put("daysRemaining", 0);
        }
        return r;
    }

    /** Trading days strictly after {@code from} up to and including {@code to}. */
    private int countTradingDaysAfter(LocalDate from, LocalDate to) {
        if (!to.isAfter(from)) return 0;
        int count = 0;
        LocalDate cur = from.plusDays(1);
        while (!cur.isAfter(to)) {
            if (marketHolidayService == null || marketHolidayService.isTradingDay(cur)) count++;
            cur = cur.plusDays(1);
        }
        return count;
    }

    private void save() {
        try {
            String json = mapper.writeValueAsString(snapshot);
            SettingEntity entity = settingRepo.findBySettingKey(STATE_KEY).orElse(new SettingEntity(STATE_KEY, json));
            entity.setSettingValue(json);
            settingRepo.save(entity);
        } catch (Exception e) {
            log.error("[VirginCPR] Save failed: {}", e.getMessage());
        }
    }

    private void load() {
        try {
            settingRepo.findBySettingKey(STATE_KEY).ifPresent(e -> {
                String json = e.getSettingValue();
                if (json == null || json.isBlank()) return;
                try {
                    snapshot = mapper.readValue(json, Snapshot.class);
                    if (snapshot != null) {
                        log.info("[VirginCPR] Loaded: date={} TC={} Pivot={} BC={}",
                            snapshot.date, fmt(snapshot.tc), fmt(snapshot.pivot), fmt(snapshot.bc));
                    }
                } catch (Exception ex) {
                    log.warn("[VirginCPR] Load parse failed: {}", ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("[VirginCPR] Load failed: {}", e.getMessage());
        }
    }

    private static String fmt(double v) { return String.format("%.2f", v); }
}
