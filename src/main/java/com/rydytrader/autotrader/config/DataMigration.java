package com.rydytrader.autotrader.config;

import com.rydytrader.autotrader.entity.TradeEntity;
import com.rydytrader.autotrader.repository.TradeRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * One-time migration: imports existing CSV trade history files into SQLite.
 * Runs on startup. Skips if CSV files don't exist or DB already has data for that date.
 */
@Component
public class DataMigration {

    private static final Logger log = LoggerFactory.getLogger(DataMigration.class);
    private static final String HISTORY_DIR = "../store/data/history";

    @Autowired
    private TradeRepository tradeRepo;

    @PostConstruct
    public void migrate() {
        migrateTradeHistory();
    }

    private void migrateTradeHistory() {
        Path dir = Paths.get(HISTORY_DIR);
        if (!Files.exists(dir)) return;

        try {
            File[] csvFiles = dir.toFile().listFiles((d, name) -> name.startsWith("trades-history-") && name.endsWith(".csv"));
            if (csvFiles == null || csvFiles.length == 0) return;

            int totalImported = 0;
            for (File csv : csvFiles) {
                // Extract date from filename: trades-history-2026-03-20.csv
                String name = csv.getName();
                String dateStr = name.replace("trades-history-", "").replace(".csv", "");
                LocalDate tradeDate;
                try {
                    tradeDate = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
                } catch (Exception e) {
                    log.warn("[Migration] Skipping unrecognized file: {}", name);
                    continue;
                }

                // Skip if DB already has trades for this date
                if (!tradeRepo.findByTradeDate(tradeDate).isEmpty()) {
                    continue;
                }

                int count = importCsv(csv, tradeDate);
                if (count > 0) {
                    totalImported += count;
                    log.info("[Migration] Imported {} trades from {}", count, name);
                }
            }

            if (totalImported > 0) {
                log.info("[Migration] Total: imported {} trades from {} CSV files into SQLite", totalImported, csvFiles.length);
            }
        } catch (Exception e) {
            log.error("[Migration] Trade history migration failed: {}", e.getMessage());
        }
    }

    private int importCsv(File csv, LocalDate tradeDate) {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
            String header = br.readLine(); // skip header
            if (header == null) return 0;

            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] cols = line.split(",", -1);
                if (cols.length < 11) continue;

                try {
                    TradeEntity entity = new TradeEntity();
                    entity.setTradeDate(tradeDate);
                    entity.setTimestamp(cols[0].trim());
                    entity.setSymbol(cols[1].trim());
                    entity.setSide(cols[2].trim());
                    entity.setQty(Integer.parseInt(cols[3].trim()));
                    entity.setEntryPrice(Double.parseDouble(cols[4].trim()));
                    entity.setExitPrice(Double.parseDouble(cols[5].trim()));
                    entity.setExitReason(cols[6].trim());
                    entity.setSetup(cols[7].trim());
                    entity.setPnl(Double.parseDouble(cols[8].trim()));
                    entity.setCharges(Double.parseDouble(cols[9].trim()));
                    entity.setNetPnl(Double.parseDouble(cols[10].trim()));
                    tradeRepo.save(entity);
                    count++;
                } catch (NumberFormatException e) {
                    log.warn("[Migration] Skipping malformed line in {}: {}", csv.getName(), line);
                }
            }
        } catch (IOException e) {
            log.error("[Migration] Error reading {}: {}", csv.getName(), e.getMessage());
        }
        return count;
    }
}
