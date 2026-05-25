package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.CprLevels;
import com.rydytrader.autotrader.store.PositionStateStore;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Level-walk trailing SL. As price closes through each successive CPR level in the
 * trade direction, the SL ratchets to that level's structural anchor — i.e., the
 * same SL formula an entry on that level would have computed.
 *
 * <p>Order for buys (climbing): R1+PDH → R2 → R3 → R4. Each rung uses the matching
 * structural anchor from {@link SignalProcessor#computeStructuralAnchor}:
 * R1+PDH → {@code min(R1, PDH)} (or midpoint if adaptive state is AVERAGE);
 * R2/R3/R4 → that single line. SL = anchor − (structuralSlBufferAtr + extraSingleLevel) × ATR.
 *
 * <p>Mirrors for sells: S1+PDL → S2 → S3 → S4 with the opposite-sign buffer.
 *
 * <p>Runs on every 5-min candle close via the {@link CandleAggregator.CandleCloseListener}
 * hook. Never widens an existing SL — only ratchets in the favourable direction.
 * Coexists with the breakeven-SL trigger in {@link MarketDataService}: both can run;
 * whichever sets a tighter SL first wins, since both honour the never-widen guard
 * inside {@link OrderService#modifySlOrder}.
 *
 * <p>Particularly useful with Open Target Mode (no fixed target → trades ride
 * structurally-locked SL until SL hit or auto-squareoff).
 */
@Service
public class LevelWalkTrailingService implements CandleAggregator.CandleCloseListener {

    private static final Logger log = LoggerFactory.getLogger(LevelWalkTrailingService.class);

    /** Climbing ladder for long trades. Detection walks high→low and stops at the
     *  first level whose breakout-line has been cleared by the latest close. */
    private static final String[] BUY_LADDER = {
        "BUY_ABOVE_R4", "BUY_ABOVE_R3", "BUY_ABOVE_R2", "BUY_ABOVE_R1_PDH"
    };
    /** Descending ladder for short trades. */
    private static final String[] SELL_LADDER = {
        "SELL_BELOW_S4", "SELL_BELOW_S3", "SELL_BELOW_S2", "SELL_BELOW_S1_PDL"
    };

    private final CandleAggregator candleAggregator;
    private final PositionStateStore positionStateStore;
    private final BhavcopyService bhavcopyService;
    private final AtrService atrService;
    private final OrderService orderService;
    private final RiskSettingsStore riskSettings;
    private final EventService eventService;
    private final PollingService pollingService;
    private final OrderEventService orderEventService;

    public LevelWalkTrailingService(CandleAggregator candleAggregator,
                                    PositionStateStore positionStateStore,
                                    BhavcopyService bhavcopyService,
                                    AtrService atrService,
                                    OrderService orderService,
                                    RiskSettingsStore riskSettings,
                                    EventService eventService,
                                    @Lazy PollingService pollingService,
                                    @Lazy OrderEventService orderEventService) {
        this.candleAggregator = candleAggregator;
        this.positionStateStore = positionStateStore;
        this.bhavcopyService = bhavcopyService;
        this.atrService = atrService;
        this.orderService = orderService;
        this.riskSettings = riskSettings;
        this.eventService = eventService;
        this.pollingService = pollingService;
        this.orderEventService = orderEventService;
    }

    @PostConstruct
    public void init() {
        candleAggregator.addListener(this);
    }

    @Override
    public void onCandleClose(String fyersSymbol, CandleAggregator.CandleBar bar) {
        if (!riskSettings.isEnableLevelWalkTrailingSl()) return;
        if (fyersSymbol == null || bar == null || bar.close <= 0) return;
        // Only honour bar-aligned closes on the standard scanner timeframe. The
        // aggregator already fires once per completed bar, so no additional gate needed.
        try {
            evaluate(fyersSymbol, bar.close);
        } catch (Exception e) {
            log.error("[LevelWalkTrailing] evaluate failed for {}: {}", fyersSymbol, e.getMessage());
        }
    }

    private void evaluate(String fyersSymbol, double barClose) {
        Map<String, Map<String, Object>> all = positionStateStore.loadAll();
        Map<String, Object> pos = all.get(fyersSymbol);
        if (pos == null) return;

        String side = String.valueOf(pos.getOrDefault("side", "")).trim();
        boolean isBuy;
        if ("LONG".equals(side))       isBuy = true;
        else if ("SHORT".equals(side)) isBuy = false;
        else                            return;

        double currentSl = toDouble(pos.get("slPrice"));
        if (currentSl <= 0) return;
        String slOrderId = String.valueOf(pos.getOrDefault("slOrderId", "")).trim();
        if (slOrderId.isEmpty()) return;

        CprLevels lv = bhavcopyService.getCprLevels(fyersSymbol);
        if (lv == null) return;

        double r1 = lv.getR1(), r2 = lv.getR2(), r3 = lv.getR3(), r4 = lv.getR4();
        double s1 = lv.getS1(), s2 = lv.getS2(), s3 = lv.getS3(), s4 = lv.getS4();
        double ph = lv.getPh(), pl = lv.getPl();
        double tc = lv.getTc(), bc = lv.getBc();

        double atr = atrService.getAtr(fyersSymbol);
        if (atr <= 0) return;

        boolean useMidpoint = false;
        String ticker = extractTicker(fyersSymbol);
        BhavcopyService.AdaptiveCprResult adaptive = bhavcopyService.getAdaptiveCpr(ticker);
        if (adaptive != null) useMidpoint = adaptive.state() == BhavcopyService.CprState.AVERAGE;

        // Walk the ladder high → low; the FIRST stage whose breakout-line has been
        // cleared by barClose is the highest cleared rung. Compute its structural SL
        // and only modify if it tightens the existing SL.
        String[] ladder = isBuy ? BUY_LADDER : SELL_LADDER;
        for (String stage : ladder) {
            double levelLine = SignalProcessor.computeBreakoutLevel(stage,
                r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc);
            if (levelLine <= 0) continue;
            boolean cleared = isBuy ? (barClose > levelLine) : (barClose < levelLine);
            if (!cleared) continue;

            double anchor = SignalProcessor.computeStructuralAnchor(stage,
                r1, r2, r3, r4, s1, s2, s3, s4, ph, pl, tc, bc, useMidpoint);
            if (anchor <= 0) return;

            double buffer = riskSettings.getStructuralSlBufferAtr();
            double extra  = appliesSingleLevelSlBuffer(stage, tc, bc, ph, pl)
                ? riskSettings.getSingleLevelSlBufferAtr() : 0;
            double totalBufferAtr = buffer + extra;
            double desiredSl = isBuy
                ? anchor - atr * totalBufferAtr
                : anchor + atr * totalBufferAtr;
            desiredSl = orderService.roundToTick(desiredSl, fyersSymbol);

            // Never-widen guard — only ratchet in the favourable direction.
            if (isBuy && desiredSl <= currentSl) return;
            if (!isBuy && desiredSl >= currentSl) return;

            int rc = orderService.modifySlOrder(slOrderId, desiredSl, fyersSymbol);
            if (rc == -1) {
                eventService.log("[INFO] Level-walk SL skipped for " + fyersSymbol
                    + " — SL order already filled/cancelled, syncing position");
                if (pollingService != null) pollingService.syncPositionOnce();
                return;
            }
            if (rc != 1) {
                eventService.log("[ERROR] Level-walk SL failed for " + fyersSymbol
                    + " — could not modify SL to " + String.format("%.2f", desiredSl));
                return;
            }

            // Persist the new SL price so monitorOCO / squareoff read the updated value.
            String targetOrderId = String.valueOf(pos.getOrDefault("targetOrderId", "")).trim();
            double targetPrice   = toDouble(pos.get("targetPrice"));
            positionStateStore.saveOcoState(fyersSymbol, slOrderId, targetOrderId, desiredSl, targetPrice);
            if (orderEventService != null) orderEventService.markAsTrailed(slOrderId);

            String stageLabel = stage.replaceFirst("^(BUY_ABOVE_|SELL_BELOW_)", "");
            String ts = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            positionStateStore.appendDescription(fyersSymbol,
                ts + " [LEVELWALK_SL] " + stageLabel + " cleared @ " + String.format("%.2f", barClose)
                + " — SL → " + String.format("%.2f", desiredSl) + " (anchor " + String.format("%.2f", anchor)
                + " ± " + totalBufferAtr + "×ATR " + String.format("%.2f", atr) + ")");

            eventService.log("[SUCCESS] " + fyersSymbol + " " + side
                + " LEVEL-WALK SL — " + stageLabel + " cleared (close " + String.format("%.2f", barClose)
                + " > " + String.format("%.2f", levelLine) + "): "
                + String.format("%.2f", currentSl) + " → " + String.format("%.2f", desiredSl));
            return;
        }
    }

    /** Replicates {@code SignalProcessor.appliesSingleLevelSlBuffer} — kept inline so
     *  this service has no instance-method dependency on SignalProcessor.
     *  Single-level setups always get the extra buffer; zone setups get it when CPR
     *  width is below the narrowCprZoneCollapseWidthPct threshold (squeeze case). */
    private boolean appliesSingleLevelSlBuffer(String stage, double tc, double bc, double ph, double pl) {
        if (SignalProcessor.isSingleLevelSetup(stage)) return true;
        double pdc = (tc > 0 && bc > 0) ? (tc + bc) / 2.0 : 0;
        if (pdc <= 0) return false;
        double cprWidthPct = (tc > 0 && bc > 0) ? Math.abs(tc - bc) / pdc * 100.0 : 0;
        double collapseThreshold = riskSettings.getNarrowCprZoneCollapseWidthPct();
        return collapseThreshold > 0 && cprWidthPct > 0 && cprWidthPct < collapseThreshold;
    }

    private static double toDouble(Object o) {
        if (o == null) return 0;
        try { return Double.parseDouble(o.toString()); } catch (NumberFormatException e) { return 0; }
    }

    private static String extractTicker(String fyersSymbol) {
        if (fyersSymbol == null) return "";
        String s = fyersSymbol;
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(colon + 1);
        return s.replaceAll("-(EQ|INDEX|MF|BE|BL|SM)$", "");
    }
}
