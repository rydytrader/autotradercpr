package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.dto.TradeRecord;
import com.rydytrader.autotrader.entity.TradeEntity;
import com.rydytrader.autotrader.repository.TradeRepository;
import com.rydytrader.autotrader.store.RiskSettingsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TradeHistoryService {

    private static final Logger log = LoggerFactory.getLogger(TradeHistoryService.class);

    private final RiskSettingsStore riskSettings;
    private final List<TradeRecord> trades = Collections.synchronizedList(new ArrayList<>());
    // Dedup: track last recorded exit per symbol to prevent duplicate entries
    private final ConcurrentHashMap<String, Long> lastRecordTime = new ConcurrentHashMap<>();
    private static final long DEDUP_WINDOW_MS = 5000; // 5 seconds

    @Autowired
    private TradeRepository tradeRepo;

    public TradeHistoryService(RiskSettingsStore riskSettings) {
        this.riskSettings = riskSettings;
    }

    @jakarta.annotation.PostConstruct
    public void init() {
        loadTodaysTradesFromDb();
    }

    public void addRecord(TradeRecord record) {
        trades.add(0, record);
        saveToDb(record);
        log.info("[LIVE] Trade: {} | {} | P&L: {} | Charges: {} | Net: {} | {}", record.getSymbol(), record.getSide(), record.getPnl(), record.getCharges(), record.getNetPnl(), record.getResult());
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason) {
        record(symbol, side, qty, entryPrice, exitPrice, exitReason, "", null, null);
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup) {
        record(symbol, side, qty, entryPrice, exitPrice, exitReason, setup, null, null);
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup, String description) {
        record(symbol, side, qty, entryPrice, exitPrice, exitReason, setup, description, null);
    }

    public void record(String symbol, String side, int qty,
                       double entryPrice, double exitPrice, String exitReason, String setup,
                       String description, String probability) {
        // Dedup: skip if same symbol was recorded within the last 5 seconds
        long now = System.currentTimeMillis();
        Long lastTime = lastRecordTime.get(symbol);
        if (lastTime != null && (now - lastTime) < DEDUP_WINDOW_MS) {
            log.info("[TradeHistory] Duplicate record skipped for {} (last recorded {}ms ago)", symbol, now - lastTime);
            return;
        }
        lastRecordTime.put(symbol, now);
        double brokerage = riskSettings.getBrokeragePerOrder();
        addRecord(new TradeRecord(symbol, side, qty, entryPrice, exitPrice, exitReason, setup, brokerage, description, probability,
            riskSettings.getSttRate(), riskSettings.getExchangeRate(), riskSettings.getGstRate(),
            riskSettings.getSebiRate(), riskSettings.getStampDutyRate(), riskSettings.getBrokeragePct()));
    }

    public List<TradeRecord> getTrades() { return new ArrayList<>(trades); }

    /**
     * Today's wins and losses for a single symbol, used by the per-symbol daily-trade-limit gate.
     * Counts only fully-closed trade rows — TARGET_1 partial fills are excluded so a split
     * trade that hits T1 then T2 counts as one win, not two.
     *
     * @return int[]{wins, losses} — both >= 0
     */
    public int[] getSymbolTodayResult(String symbol) {
        if (symbol == null) return new int[]{0, 0};
        int wins = 0;
        int losses = 0;
        synchronized (trades) {
            for (TradeRecord r : trades) {
                if (!symbol.equals(r.getSymbol())) continue;
                if ("TARGET_1".equals(r.getExitReason())) continue; // partial fill
                if ("PROFIT".equals(r.getResult())) wins++;
                else if ("LOSS".equals(r.getResult())) losses++;
            }
        }
        return new int[]{wins, losses};
    }

    public List<TradeRecord> getTradesForRange(LocalDate from, LocalDate to) {
        List<TradeRecord> result = new ArrayList<>();
        List<TradeEntity> entities = tradeRepo.findByTradeDateBetween(from, to);
        for (TradeEntity e : entities) {
            result.add(entityToRecord(e));
        }
        return result;
    }

    public void reloadForCurrentMode() {
        // No-op: DB handles persistence; just refresh in-memory cache
        trades.clear();
        loadTodaysTradesFromDb();
    }

    @Transactional
    public void clearToday() {
        trades.clear();
        tradeRepo.deleteByTradeDate(LocalDate.now());
    }

    private void saveToDb(TradeRecord r) {
        try {
            TradeEntity entity = new TradeEntity();
            entity.setTradeDate(LocalDate.now());
            entity.setTimestamp(r.getTimestamp());
            entity.setSymbol(r.getSymbol());
            entity.setSide(r.getSide());
            entity.setQty(r.getQty());
            entity.setEntryPrice(r.getEntryPrice());
            entity.setExitPrice(r.getExitPrice());
            entity.setExitReason(r.getExitReason());
            entity.setSetup(r.getSetup());
            entity.setPnl(r.getPnl());
            entity.setCharges(r.getCharges());
            entity.setNetPnl(r.getNetPnl());
            entity.setDescription(r.getDescription());
            entity.setProbability(r.getProbability());
            tradeRepo.save(entity);
        } catch (Exception e) {
            log.error("Error saving trade to DB", e);
        }
    }

    private void loadTodaysTradesFromDb() {
        try {
            List<TradeEntity> entities = tradeRepo.findByTradeDate(LocalDate.now());
            // Load in reverse order (newest first) to match previous behavior
            for (int i = entities.size() - 1; i >= 0; i--) {
                trades.add(entityToRecord(entities.get(i)));
            }
            if (!entities.isEmpty()) {
                log.info("Loaded {} trade(s) from DB for today", entities.size());
            }
        } catch (Exception e) {
            log.error("Error loading today's trades from DB", e);
        }
    }

    private TradeRecord entityToRecord(TradeEntity e) {
        // Combine tradeDate + time into a full yyyy-MM-dd HH:mm:ss timestamp so downstream
        // code (JournalService.computeMetrics dailyPnl/equity) can extract the day key.
        String fullTs = e.getTradeDate() != null
            ? e.getTradeDate().toString() + " " + e.getTimestamp()
            : e.getTimestamp();
        return new TradeRecord(fullTs, e.getSymbol(), e.getSide(),
                e.getQty(), e.getEntryPrice(), e.getExitPrice(),
                e.getExitReason(), e.getSetup(), e.getCharges(), true, e.getDescription(), e.getProbability());
    }
}
