package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.entity.SectorEntity;
import com.rydytrader.autotrader.entity.StockEntity;
import com.rydytrader.autotrader.repository.SectorRepository;
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
 * One-time seed on first boot: 18 sectors and the NIFTY 50 stock list with their
 * sector mapping. Idempotent — skips entries that already exist so subsequent boots
 * are no-ops. Subsequent edits (Add / Edit / Delete) come from the Settings UI.
 */
@Service
public class StockUniverseSeederService {

    private static final Logger log = LoggerFactory.getLogger(StockUniverseSeederService.class);

    private final SectorRepository sectorRepo;
    private final StockRepository stockRepo;

    public StockUniverseSeederService(SectorRepository sectorRepo, StockRepository stockRepo) {
        this.sectorRepo = sectorRepo;
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
        seedSectors();
        pruneRemovedStocks();
        seedStocks();
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
     * Migration: short → NSE-canonical sector names. Runs before seed so existing rows
     * created by the older seeder ("Bank", "Auto", …) get upgraded in place to the NSE
     * naming ("Nifty Bank", "Nifty Auto", …) without losing their FK references from
     * existing stock rows. Sectors with no canonical NSE index (Telecommunication,
     * Cement, Construction, Power) keep their existing display names.
     */
    private static final Map<String, String> SECTOR_RENAMES = Map.ofEntries(
        Map.entry("Bank",               "Nifty Bank"),
        Map.entry("Auto",               "Nifty Auto"),
        Map.entry("IT",                 "Nifty IT"),
        Map.entry("Pharma",             "Nifty Pharma"),
        Map.entry("FMCG",               "Nifty FMCG"),
        Map.entry("Metal",              "Nifty Metal"),
        Map.entry("Oil & Gas",          "Nifty Oil & Gas"),
        Map.entry("Healthcare",         "Nifty Healthcare"),
        Map.entry("Financial Services", "Nifty Financial Services"),
        Map.entry("Commodities",        "Nifty Commodities"),
        Map.entry("Infrastructure",     "Nifty Infrastructure"),
        Map.entry("Services",           "Nifty Services Sector"),
        Map.entry("Consumer Durables",  "Nifty Consumer Durables"),
        Map.entry("Consumer Services",  "Nifty Consumption")
    );

    private void seedSectors() {
        // Step 1 — rename old short names to NSE canonical. If the target name already
        // exists (fresh install AFTER this code shipped), skip. Don't touch index_ticker.
        int renamed = 0;
        for (Map.Entry<String, String> r : SECTOR_RENAMES.entrySet()) {
            var existing = sectorRepo.findByName(r.getKey());
            if (existing.isEmpty()) continue;
            if (sectorRepo.findByName(r.getValue()).isPresent()) continue;
            SectorEntity row = existing.get();
            row.setName(r.getValue());
            sectorRepo.save(row);
            renamed++;
        }
        if (renamed > 0) {
            log.info("[StockUniverseSeeder] Renamed {} sectors to NSE canonical naming", renamed);
        }

        // Step 2 — seed new sectors (idempotent by name).
        Map<String, String> sectors = new LinkedHashMap<>();
        // 14 NSE sectoral / thematic indices (canonical NSE naming).
        sectors.put("Nifty Bank",                  "NIFTYBANK");
        sectors.put("Nifty Auto",                  "NIFTYAUTO");
        sectors.put("Nifty IT",                    "NIFTYIT");
        sectors.put("Nifty Pharma",                "NIFTYPHARMA");
        sectors.put("Nifty FMCG",                  "NIFTYFMCG");
        sectors.put("Nifty Metal",                 "NIFTYMETAL");
        sectors.put("Nifty Oil & Gas",             "NIFTYOILANDGAS");
        sectors.put("Nifty Healthcare",            "NIFTYHEALTHCARE");
        sectors.put("Nifty Financial Services",    "FINNIFTY");
        sectors.put("Nifty Commodities",           "NIFTYCOMMODITIES");
        sectors.put("Nifty Infrastructure",        "NIFTYINFRA");
        sectors.put("Nifty Services Sector",       "NIFTYSERVSECTOR");
        sectors.put("Nifty Consumer Durables",     "NIFTYCONSRDURBL");
        sectors.put("Nifty Consumption",           "NIFTYCONSUMPTION");
        // 4 sectors without a dedicated NSE sectoral index — kept with descriptive names.
        // Telecom: NIFTY MidSmall IT & Telecom is missing from Fyers' HSM index_dict; the
        // single NIFTY 50 telecom name (BHARTIARTL) is also a Services Sector constituent.
        sectors.put("Telecommunication",           "NIFTYSERVSECTOR");
        sectors.put("Cement",                      "NIFTYINFRA");
        sectors.put("Construction",                "NIFTYINFRA");
        // Power: no dedicated NSE sectoral index — falls back to NIFTYINFRA (NTPC and
        // POWERGRID are both NIFTY 50 power utilities and Infrastructure constituents).
        sectors.put("Power",                       "NIFTYINFRA");

        int added = 0;
        for (Map.Entry<String, String> e : sectors.entrySet()) {
            if (sectorRepo.findByName(e.getKey()).isEmpty()) {
                sectorRepo.save(new SectorEntity(e.getKey(), e.getValue()));
                added++;
            }
        }
        if (added > 0) {
            log.info("[StockUniverseSeeder] Seeded {} new sectors (table size now {})", added, sectorRepo.count());
        }
    }

    private void seedStocks() {
        // ticker → sector name (must match a SectorEntity.name from seedSectors)
        Map<String, String[]> stocks = new LinkedHashMap<>();
        stocks.put("ADANIENT",    new String[] { "Adani Enterprises",       "Nifty Commodities" });
        stocks.put("ADANIPORTS",  new String[] { "Adani Ports",              "Nifty Infrastructure" });
        stocks.put("APOLLOHOSP",  new String[] { "Apollo Hospitals",         "Nifty Healthcare" });
        stocks.put("ASIANPAINT",  new String[] { "Asian Paints",             "Nifty FMCG" });
        stocks.put("AXISBANK",    new String[] { "Axis Bank",                "Nifty Bank" });
        stocks.put("BAJAJ-AUTO",  new String[] { "Bajaj Auto",               "Nifty Auto" });
        stocks.put("BAJFINANCE",  new String[] { "Bajaj Finance",            "Nifty Financial Services" });
        stocks.put("BAJAJFINSV",  new String[] { "Bajaj Finserv",            "Nifty Financial Services" });
        // BPCL — removed from NIFTY 50 in the March 2025 rebalancing (replaced by BEL).
        // stocks.put("BPCL",     new String[] { "BPCL",                     "Nifty Oil & Gas" });
        stocks.put("BHARTIARTL",  new String[] { "Bharti Airtel",            "Telecommunication" });
        stocks.put("BRITANNIA",   new String[] { "Britannia",                "Nifty FMCG" });
        stocks.put("CIPLA",       new String[] { "Cipla",                    "Nifty Pharma" });
        stocks.put("COALINDIA",   new String[] { "Coal India",               "Nifty Oil & Gas" });
        // DIVISLAB — removed from NIFTY 50 in March 2023 (replaced by ADANIENT).
        // stocks.put("DIVISLAB", new String[] { "Divis Laboratories",       "Nifty Pharma" });
        stocks.put("DRREDDY",     new String[] { "Dr Reddy's Labs",          "Nifty Pharma" });
        stocks.put("EICHERMOT",   new String[] { "Eicher Motors",            "Nifty Auto" });
        stocks.put("GRASIM",      new String[] { "Grasim Industries",        "Cement" });
        stocks.put("HCLTECH",     new String[] { "HCL Technologies",         "Nifty IT" });
        stocks.put("HDFCBANK",    new String[] { "HDFC Bank",                "Nifty Bank" });
        stocks.put("HDFCLIFE",    new String[] { "HDFC Life",                "Nifty Financial Services" });
        stocks.put("HEROMOTOCO",  new String[] { "Hero MotoCorp",            "Nifty Auto" });
        stocks.put("HINDALCO",    new String[] { "Hindalco",                 "Nifty Metal" });
        stocks.put("HINDUNILVR",  new String[] { "Hindustan Unilever",       "Nifty FMCG" });
        stocks.put("ICICIBANK",   new String[] { "ICICI Bank",               "Nifty Bank" });
        stocks.put("INDIGO",      new String[] { "Interglobe Aviation",      "Nifty Services Sector" });
        stocks.put("INFY",        new String[] { "Infosys",                  "Nifty IT" });
        stocks.put("ITC",         new String[] { "ITC",                      "Nifty FMCG" });
        stocks.put("JIOFIN",      new String[] { "Jio Financial Services",   "Nifty Financial Services" });
        stocks.put("JSWSTEEL",    new String[] { "JSW Steel",                "Nifty Metal" });
        stocks.put("KOTAKBANK",   new String[] { "Kotak Mahindra Bank",      "Nifty Bank" });
        stocks.put("LT",          new String[] { "Larsen & Toubro",          "Construction" });
        stocks.put("M&M",         new String[] { "Mahindra & Mahindra",      "Nifty Auto" });
        stocks.put("MARUTI",      new String[] { "Maruti Suzuki",            "Nifty Auto" });
        stocks.put("MAXHEALTH",   new String[] { "Max Healthcare",           "Nifty Healthcare" });
        stocks.put("NESTLEIND",   new String[] { "Nestle India",             "Nifty FMCG" });
        stocks.put("NTPC",        new String[] { "NTPC",                     "Power" });
        stocks.put("ONGC",        new String[] { "ONGC",                     "Nifty Oil & Gas" });
        stocks.put("POWERGRID",   new String[] { "Power Grid",               "Power" });
        stocks.put("RELIANCE",    new String[] { "Reliance Industries",      "Nifty Oil & Gas" });
        stocks.put("SBILIFE",     new String[] { "SBI Life Insurance",       "Nifty Financial Services" });
        stocks.put("SHRIRAMFIN",  new String[] { "Shriram Finance",          "Nifty Financial Services" });
        stocks.put("SBIN",        new String[] { "State Bank of India",      "Nifty Bank" });
        stocks.put("SUNPHARMA",   new String[] { "Sun Pharma",               "Nifty Healthcare" });
        stocks.put("TCS",         new String[] { "TCS",                      "Nifty IT" });
        stocks.put("TATACONSUM",  new String[] { "Tata Consumer",            "Nifty FMCG" });
        stocks.put("TATAMOTORS",  new String[] { "Tata Motors",              "Nifty Auto" });
        stocks.put("TATASTEEL",   new String[] { "Tata Steel",               "Nifty Metal" });
        stocks.put("TECHM",       new String[] { "Tech Mahindra",            "Nifty IT" });
        stocks.put("TITAN",       new String[] { "Titan Company",            "Nifty Consumer Durables" });
        stocks.put("TRENT",       new String[] { "Trent",                    "Nifty Consumption" });
        stocks.put("ULTRACEMCO",  new String[] { "UltraTech Cement",         "Cement" });
        stocks.put("WIPRO",       new String[] { "Wipro",                    "Nifty IT" });

        int added = 0, skippedNoSector = 0;
        for (Map.Entry<String, String[]> e : stocks.entrySet()) {
            String ticker = e.getKey();
            String name = e.getValue()[0];
            String sectorName = e.getValue()[1];

            if (stockRepo.existsByTicker(ticker)) continue;

            SectorEntity sector = sectorRepo.findByName(sectorName).orElse(null);
            if (sector == null) {
                log.warn("[StockUniverseSeeder] Skipping {} — sector '{}' not found in DB", ticker, sectorName);
                skippedNoSector++;
                continue;
            }
            stockRepo.save(new StockEntity(ticker, name, sector, true));
            added++;
        }
        if (added > 0 || skippedNoSector > 0) {
            log.info("[StockUniverseSeeder] Seeded {} new stocks (skipped {} no-sector); table size now {}",
                added, skippedNoSector, stockRepo.count());
        }
    }
}
