package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.entity.IndexEntity;
import com.rydytrader.autotrader.entity.StockEntity;
import com.rydytrader.autotrader.repository.IndexRepository;
import com.rydytrader.autotrader.repository.StockRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One-time seed on first boot: ~15 NSE indices (NIFTY 50 + sector indices) and the
 * NIFTY 50 stock list mapped to its primary index. Idempotent — skips entries that
 * already exist. Replaces the legacy sector-based universe (where each stock pointed
 * to a sector and the sector held the index ticker) with a direct stock → primary
 * index mapping so the bot's alignment / HTF hurdle / 5m hurdle filters run against
 * that specific index instead of always NIFTY 50.
 */
@Service
public class StockUniverseSeederService {

    private static final Logger log = LoggerFactory.getLogger(StockUniverseSeederService.class);

    private final IndexRepository indexRepo;
    private final StockRepository stockRepo;

    public StockUniverseSeederService(IndexRepository indexRepo, StockRepository stockRepo) {
        this.indexRepo = indexRepo;
        this.stockRepo = stockRepo;
    }

    /**
     * Tickers that were once seeded but have since been removed from NIFTY 50 by NSE.
     * Removed here so older installs (where the seeder already wrote them) drop them on
     * the next boot. New installs never add them in the first place.
     */
    private static final List<String> REMOVED_FROM_NIFTY50 = List.of(
        "DIVISLAB",   // removed by NSE in March 2023 (replaced by ADANIENT)
        "BPCL"        // removed by NSE in March 2025 rebalancing (replaced by BEL)
    );

    @PostConstruct
    @Transactional
    public void seed() {
        seedIndices();
        pruneRemovedStocks();
        seedOrMigrateStocks();
    }

    private void pruneRemovedStocks() {
        int removed = 0;
        for (String ticker : REMOVED_FROM_NIFTY50) {
            var row = stockRepo.findByTicker(ticker);
            if (row.isPresent()) {
                stockRepo.delete(row.get());
                removed++;
            }
        }
        if (removed > 0) {
            log.info("[StockUniverseSeeder] Pruned {} stock(s) no longer in NIFTY 50", removed);
        }
    }

    /**
     * Master list of indices a stock can be aligned to. Deduplicated by ticker — sectors
     * that historically shared an index ticker (e.g., Cement / Construction / Power all
     * fell back to NIFTYINFRA) collapse to one row. NIFTY 50 is included so stocks whose
     * primary alignment is the broad index can be mapped explicitly.
     */
    private static final Map<String, String> INDICES = new LinkedHashMap<>() {{
        put("NIFTY 50",                   "NIFTY50");
        put("Nifty Bank",                 "NIFTYBANK");
        put("Nifty Auto",                 "NIFTYAUTO");
        put("Nifty IT",                   "NIFTYIT");
        put("Nifty Pharma",               "NIFTYPHARMA");
        put("Nifty FMCG",                 "NIFTYFMCG");
        put("Nifty Metal",                "NIFTYMETAL");
        put("Nifty Energy",               "NIFTYENERGY");
        put("Nifty Oil & Gas",            "NIFTYOILANDGAS");
        put("Nifty Healthcare",           "NIFTYHEALTHCARE");
        put("Nifty Financial Services",   "FINNIFTY");
        put("Nifty Realty",               "NIFTYREALTY");
        put("Nifty Media",                "NIFTYMEDIA");
        put("Nifty Commodities",          "NIFTYCOMMODITIES");
        put("Nifty Infrastructure",       "NIFTYINFRA");
        put("Nifty Services Sector",      "NIFTYSERVSECTOR");
        put("Nifty Consumer Durables",    "NIFTYCONSRDURBL");
        put("Nifty Consumption",          "NIFTYCONSUMPTION");
    }};

    private void seedIndices() {
        int added = 0;
        for (Map.Entry<String, String> e : INDICES.entrySet()) {
            if (!indexRepo.existsByTicker(e.getValue())) {
                indexRepo.save(new IndexEntity(e.getKey(), e.getValue()));
                added++;
            }
        }
        if (added > 0) {
            log.info("[StockUniverseSeeder] Seeded {} new indices (table size now {})", added, indexRepo.count());
        }
    }

    private void seedOrMigrateStocks() {
        // ticker → { display name, primary-index ticker }
        Map<String, String[]> stocks = new LinkedHashMap<>();
        stocks.put("ADANIENT",    new String[] { "Adani Enterprises",     "NIFTYCOMMODITIES" });
        stocks.put("ADANIPORTS",  new String[] { "Adani Ports",            "NIFTYINFRA" });
        stocks.put("APOLLOHOSP",  new String[] { "Apollo Hospitals",       "NIFTYHEALTHCARE" });
        stocks.put("ASIANPAINT",  new String[] { "Asian Paints",           "NIFTYFMCG" });
        stocks.put("AXISBANK",    new String[] { "Axis Bank",              "NIFTYBANK" });
        stocks.put("BAJAJ-AUTO",  new String[] { "Bajaj Auto",             "NIFTYAUTO" });
        stocks.put("BAJFINANCE",  new String[] { "Bajaj Finance",          "FINNIFTY" });
        stocks.put("BAJAJFINSV",  new String[] { "Bajaj Finserv",          "FINNIFTY" });
        stocks.put("BHARTIARTL",  new String[] { "Bharti Airtel",          "NIFTYSERVSECTOR" });
        stocks.put("BRITANNIA",   new String[] { "Britannia",              "NIFTYFMCG" });
        stocks.put("CIPLA",       new String[] { "Cipla",                  "NIFTYPHARMA" });
        stocks.put("COALINDIA",   new String[] { "Coal India",             "NIFTYOILANDGAS" });
        stocks.put("DRREDDY",     new String[] { "Dr Reddy's Labs",        "NIFTYPHARMA" });
        stocks.put("EICHERMOT",   new String[] { "Eicher Motors",          "NIFTYAUTO" });
        stocks.put("GRASIM",      new String[] { "Grasim Industries",      "NIFTYINFRA" });
        stocks.put("HCLTECH",     new String[] { "HCL Technologies",       "NIFTYIT" });
        stocks.put("HDFCBANK",    new String[] { "HDFC Bank",              "NIFTYBANK" });
        stocks.put("HDFCLIFE",    new String[] { "HDFC Life",              "FINNIFTY" });
        stocks.put("HEROMOTOCO",  new String[] { "Hero MotoCorp",          "NIFTYAUTO" });
        stocks.put("HINDALCO",    new String[] { "Hindalco",               "NIFTYMETAL" });
        stocks.put("HINDUNILVR",  new String[] { "Hindustan Unilever",     "NIFTYFMCG" });
        stocks.put("ICICIBANK",   new String[] { "ICICI Bank",             "NIFTYBANK" });
        stocks.put("INDIGO",      new String[] { "Interglobe Aviation",    "NIFTYSERVSECTOR" });
        stocks.put("INFY",        new String[] { "Infosys",                "NIFTYIT" });
        stocks.put("ITC",         new String[] { "ITC",                    "NIFTYFMCG" });
        stocks.put("JIOFIN",      new String[] { "Jio Financial Services", "FINNIFTY" });
        stocks.put("JSWSTEEL",    new String[] { "JSW Steel",              "NIFTYMETAL" });
        stocks.put("KOTAKBANK",   new String[] { "Kotak Mahindra Bank",    "NIFTYBANK" });
        stocks.put("LT",          new String[] { "Larsen & Toubro",        "NIFTYINFRA" });
        stocks.put("M&M",         new String[] { "Mahindra & Mahindra",    "NIFTYAUTO" });
        stocks.put("MARUTI",      new String[] { "Maruti Suzuki",          "NIFTYAUTO" });
        stocks.put("MAXHEALTH",   new String[] { "Max Healthcare",         "NIFTYHEALTHCARE" });
        stocks.put("NESTLEIND",   new String[] { "Nestle India",           "NIFTYFMCG" });
        stocks.put("NTPC",        new String[] { "NTPC",                   "NIFTYINFRA" });
        stocks.put("ONGC",        new String[] { "ONGC",                   "NIFTYOILANDGAS" });
        stocks.put("POWERGRID",   new String[] { "Power Grid",             "NIFTYINFRA" });
        stocks.put("RELIANCE",    new String[] { "Reliance Industries",    "NIFTYOILANDGAS" });
        stocks.put("SBILIFE",     new String[] { "SBI Life Insurance",     "FINNIFTY" });
        stocks.put("SHRIRAMFIN",  new String[] { "Shriram Finance",        "FINNIFTY" });
        stocks.put("SBIN",        new String[] { "State Bank of India",    "NIFTYBANK" });
        stocks.put("SUNPHARMA",   new String[] { "Sun Pharma",             "NIFTYHEALTHCARE" });
        stocks.put("TCS",         new String[] { "TCS",                    "NIFTYIT" });
        stocks.put("TATACONSUM",  new String[] { "Tata Consumer",          "NIFTYFMCG" });
        stocks.put("TATAMOTORS",  new String[] { "Tata Motors",            "NIFTYAUTO" });
        stocks.put("TATASTEEL",   new String[] { "Tata Steel",             "NIFTYMETAL" });
        stocks.put("TECHM",       new String[] { "Tech Mahindra",          "NIFTYIT" });
        stocks.put("TITAN",       new String[] { "Titan Company",          "NIFTYCONSRDURBL" });
        stocks.put("TRENT",       new String[] { "Trent",                  "NIFTYCONSUMPTION" });
        stocks.put("ULTRACEMCO",  new String[] { "UltraTech Cement",       "NIFTYINFRA" });
        stocks.put("WIPRO",       new String[] { "Wipro",                  "NIFTYIT" });

        int added = 0, migrated = 0, skippedNoIndex = 0;
        for (Map.Entry<String, String[]> e : stocks.entrySet()) {
            String ticker = e.getKey();
            String name = e.getValue()[0];
            String indexTicker = e.getValue()[1];

            IndexEntity idx = indexRepo.findByTicker(indexTicker).orElse(null);
            if (idx == null) {
                log.warn("[StockUniverseSeeder] Skipping {} — index '{}' not found", ticker, indexTicker);
                skippedNoIndex++;
                continue;
            }

            var existing = stockRepo.findByTicker(ticker);
            if (existing.isPresent()) {
                // Migration path: existing stock from the legacy sector seeder. If its
                // primary index is null (DB just got the new column), backfill it from
                // the canonical mapping. Don't overwrite a user-customized mapping.
                StockEntity row = existing.get();
                if (row.getPrimaryIndex() == null) {
                    row.setPrimaryIndex(idx);
                    stockRepo.save(row);
                    migrated++;
                }
                continue;
            }
            stockRepo.save(new StockEntity(ticker, name, idx, true));
            added++;
        }
        if (added > 0 || migrated > 0 || skippedNoIndex > 0) {
            log.info("[StockUniverseSeeder] Stocks: added {}, migrated {} (skipped {} no-index); table size now {}",
                added, migrated, skippedNoIndex, stockRepo.count());
        }
    }
}
