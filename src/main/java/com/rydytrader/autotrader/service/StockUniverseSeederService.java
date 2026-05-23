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
import java.util.Set;

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
     * Canonical NIFTY 50 constituents (post Mar-2025 rebalancing). Used to stamp the
     * {@link StockEntity#membership} column on first seed and to keep existing rows in
     * sync on subsequent boots — canonical N50/NN50 wins over any other value.
     * Manually upgraded OTHERS → N50/NN50 (i.e., stocks NSE just promoted before we
     * updated this list) is preserved because we don't downgrade.
     */
    private static final Set<String> NIFTY_50_TICKERS = Set.of(
        "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
        "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BHARTIARTL", "BRITANNIA",
        "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "GRASIM",
        "HCLTECH", "HDFCBANK", "HDFCLIFE", "ETERNAL", "HINDALCO",
        "HINDUNILVR", "ICICIBANK", "INDUSINDBK", "INFY", "ITC",
        "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT", "M&M",
        "MARUTI", "NESTLEIND", "NTPC", "ONGC", "POWERGRID",
        "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN", "SUNPHARMA",
        "TCS", "TATACONSUM", "TMPV", "TATASTEEL", "TECHM",
        "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
        "BEL"  // added to NIFTY 50 in March 2025
    );

    /**
     * NIFTY Next 50 constituents (approximate, as of 2026 — NSE rebalances twice a
     * year so this drifts). Only includes tickers that exist in our seeded universe;
     * stocks NSE lists in Next 50 but we don't trade (LICI, IRFC, NAUKRI, BAJAJHLDNG,
     * DMART, ADANIPOWER, etc.) are omitted. The UI lets users override if needed.
     */
    private static final Set<String> NIFTY_NEXT_50_TICKERS = Set.of(
        "ABB", "ADANIGREEN", "AMBUJACEM", "BANKBARODA", "BHARATFORG",
        "BOSCHLTD", "BPCL", "CHOLAFIN", "COLPAL", "CUMMINSIND",
        "DABUR", "DIVISLAB", "DLF", "GAIL", "GODREJCP",
        "HAL", "HAVELLS", "HDFCAMC", "HEROMOTOCO", "HINDZINC",
        "HPCL", "ICICIGI", "ICICIPRULI", "INDIGO", "INDUSTOWER",
        "IOC", "JINDALSTEL", "JSWENERGY", "LODHA", "LTIM",
        "MANKIND", "MARICO", "MAXHEALTH", "MOTHERSON", "NHPC",
        "PFC", "PIDILITIND", "PNB", "POLYCAB", "RECLTD",
        "SBICARD", "SHREECEM", "SIEMENS", "SRF", "TATAPOWER",
        "TORNTPHARM", "TVSMOTOR", "VBL", "VEDL", "ZYDUSLIFE"
    );

    private static StockEntity.Membership classifyMembership(String ticker) {
        if (NIFTY_50_TICKERS.contains(ticker)) return StockEntity.Membership.NIFTY_50;
        if (NIFTY_NEXT_50_TICKERS.contains(ticker)) return StockEntity.Membership.NIFTY_NEXT_50;
        return StockEntity.Membership.OTHERS;
    }

    /**
     * Tickers that were once seeded but have since been removed from NIFTY 50 by NSE.
     * Removed here so older installs (where the seeder already wrote them) drop them on
     * the next boot. New installs never add them in the first place.
     */
    private static final List<String> REMOVED_FROM_NIFTY50 = List.of(
        "DIVISLAB",   // removed by NSE in March 2023 (replaced by ADANIENT)
        "BPCL",       // removed by NSE in March 2025 rebalancing (replaced by BEL)
        "HEROMOTOCO", // removed by NSE in 2025 (replaced by ETERNAL — Zomato's rebrand)
        "TATAMOTORS"  // demerged in October 2025 into TMPV (passenger) and TMCV (commercial);
                      // TMPV took the NIFTY 50 slot. The legacy symbol no longer trades.
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
        stocks.put("ETERNAL",     new String[] { "Eternal (Zomato)",       "NIFTYSERVSECTOR" });
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
        stocks.put("TMPV",        new String[] { "Tata Motors Passenger Vehicles", "NIFTYAUTO" });
        stocks.put("TATASTEEL",   new String[] { "Tata Steel",             "NIFTYMETAL" });
        stocks.put("TECHM",       new String[] { "Tech Mahindra",          "NIFTYIT" });
        stocks.put("TITAN",       new String[] { "Titan Company",          "NIFTYCONSRDURBL" });
        stocks.put("TRENT",       new String[] { "Trent",                  "NIFTYCONSUMPTION" });
        stocks.put("ULTRACEMCO",  new String[] { "UltraTech Cement",       "NIFTYINFRA" });
        stocks.put("WIPRO",       new String[] { "Wipro",                  "NIFTYIT" });

        // ── Sector-index extensions (FNO eligible, not part of NIFTY 50) ─────────
        // Banks (NIFTYBANK)
        stocks.put("INDUSINDBK",  new String[] { "IndusInd Bank",          "NIFTYBANK" });
        stocks.put("PNB",         new String[] { "Punjab National Bank",   "NIFTYBANK" });
        stocks.put("BANKBARODA",  new String[] { "Bank of Baroda",         "NIFTYBANK" });
        stocks.put("IDFCFIRSTB",  new String[] { "IDFC First Bank",        "NIFTYBANK" });
        stocks.put("FEDERALBNK",  new String[] { "Federal Bank",           "NIFTYBANK" });
        stocks.put("AUBANK",      new String[] { "AU Small Finance Bank",  "NIFTYBANK" });
        stocks.put("CANBK",       new String[] { "Canara Bank",            "NIFTYBANK" });
        // Financial Services (FINNIFTY) — not also in NIFTYBANK
        stocks.put("ICICIPRULI",  new String[] { "ICICI Prudential Life",  "FINNIFTY" });
        stocks.put("CHOLAFIN",    new String[] { "Cholamandalam Finance",  "FINNIFTY" });
        stocks.put("PFC",         new String[] { "Power Finance Corp",     "FINNIFTY" });
        stocks.put("RECLTD",      new String[] { "REC Limited",            "FINNIFTY" });
        stocks.put("ICICIGI",     new String[] { "ICICI Lombard",          "FINNIFTY" });
        stocks.put("LICHSGFIN",   new String[] { "LIC Housing Finance",    "FINNIFTY" });
        stocks.put("HDFCAMC",     new String[] { "HDFC AMC",               "FINNIFTY" });
        stocks.put("SBICARD",     new String[] { "SBI Cards",              "FINNIFTY" });
        stocks.put("M&MFIN",      new String[] { "M&M Financial Services", "FINNIFTY" });
        stocks.put("MUTHOOTFIN",  new String[] { "Muthoot Finance",        "FINNIFTY" });
        // IT (NIFTYIT)
        stocks.put("LTIM",        new String[] { "LTIMindtree",            "NIFTYIT" });
        stocks.put("PERSISTENT",  new String[] { "Persistent Systems",     "NIFTYIT" });
        stocks.put("MPHASIS",     new String[] { "Mphasis",                "NIFTYIT" });
        stocks.put("COFORGE",     new String[] { "Coforge",                "NIFTYIT" });
        stocks.put("LTTS",        new String[] { "L&T Technology Services","NIFTYIT" });
        // Pharma (NIFTYPHARMA). DIVISLAB was in NIFTY 50; pruned & re-added here.
        stocks.put("DIVISLAB",    new String[] { "Divi's Labs",            "NIFTYPHARMA" });
        stocks.put("LUPIN",       new String[] { "Lupin",                  "NIFTYPHARMA" });
        stocks.put("AUROPHARMA",  new String[] { "Aurobindo Pharma",       "NIFTYPHARMA" });
        stocks.put("ZYDUSLIFE",   new String[] { "Zydus Lifesciences",     "NIFTYPHARMA" });
        stocks.put("MANKIND",     new String[] { "Mankind Pharma",         "NIFTYPHARMA" });
        stocks.put("ALKEM",       new String[] { "Alkem Labs",             "NIFTYPHARMA" });
        stocks.put("TORNTPHARM",  new String[] { "Torrent Pharma",         "NIFTYPHARMA" });
        stocks.put("BIOCON",      new String[] { "Biocon",                 "NIFTYPHARMA" });
        stocks.put("LAURUSLABS",  new String[] { "Laurus Labs",            "NIFTYPHARMA" });
        stocks.put("GLENMARK",    new String[] { "Glenmark Pharma",        "NIFTYPHARMA" });
        // Healthcare (NIFTYHEALTHCARE) — not also in NIFTYPHARMA
        stocks.put("FORTIS",      new String[] { "Fortis Healthcare",      "NIFTYHEALTHCARE" });
        // Auto (NIFTYAUTO). HEROMOTOCO was in NIFTY 50; pruned & re-added here.
        stocks.put("HEROMOTOCO",  new String[] { "Hero MotoCorp",          "NIFTYAUTO" });
        stocks.put("TVSMOTOR",    new String[] { "TVS Motor",              "NIFTYAUTO" });
        stocks.put("BHARATFORG",  new String[] { "Bharat Forge",           "NIFTYAUTO" });
        stocks.put("MOTHERSON",   new String[] { "Samvardhana Motherson",  "NIFTYAUTO" });
        stocks.put("BOSCHLTD",    new String[] { "Bosch",                  "NIFTYAUTO" });
        stocks.put("ASHOKLEY",    new String[] { "Ashok Leyland",          "NIFTYAUTO" });
        stocks.put("BALKRISIND",  new String[] { "Balkrishna Industries",  "NIFTYAUTO" });
        stocks.put("MRF",         new String[] { "MRF",                    "NIFTYAUTO" });
        stocks.put("TIINDIA",     new String[] { "Tube Investments",       "NIFTYAUTO" });
        stocks.put("APOLLOTYRE",  new String[] { "Apollo Tyres",           "NIFTYAUTO" });
        // FMCG (NIFTYFMCG)
        stocks.put("GODREJCP",    new String[] { "Godrej Consumer",        "NIFTYFMCG" });
        stocks.put("DABUR",       new String[] { "Dabur",                  "NIFTYFMCG" });
        stocks.put("MARICO",      new String[] { "Marico",                 "NIFTYFMCG" });
        stocks.put("COLPAL",      new String[] { "Colgate-Palmolive",      "NIFTYFMCG" });
        stocks.put("UBL",         new String[] { "United Breweries",       "NIFTYFMCG" });
        stocks.put("VBL",         new String[] { "Varun Beverages",        "NIFTYFMCG" });
        // Metal (NIFTYMETAL)
        stocks.put("VEDL",        new String[] { "Vedanta",                "NIFTYMETAL" });
        stocks.put("JINDALSTEL",  new String[] { "Jindal Steel & Power",   "NIFTYMETAL" });
        stocks.put("SAIL",        new String[] { "Steel Authority of India","NIFTYMETAL" });
        stocks.put("APLAPOLLO",   new String[] { "APL Apollo Tubes",       "NIFTYMETAL" });
        stocks.put("HINDZINC",    new String[] { "Hindustan Zinc",         "NIFTYMETAL" });
        stocks.put("NMDC",        new String[] { "NMDC",                   "NIFTYMETAL" });
        stocks.put("NATIONALUM",  new String[] { "National Aluminium",     "NIFTYMETAL" });
        // Energy (NIFTYENERGY)
        stocks.put("TATAPOWER",   new String[] { "Tata Power",             "NIFTYENERGY" });
        stocks.put("ADANIGREEN",  new String[] { "Adani Green Energy",     "NIFTYENERGY" });
        // Oil & Gas (NIFTYOILANDGAS). BPCL was in NIFTY 50; pruned & re-added here.
        stocks.put("BPCL",        new String[] { "Bharat Petroleum",       "NIFTYOILANDGAS" });
        stocks.put("IOC",         new String[] { "Indian Oil",             "NIFTYOILANDGAS" });
        stocks.put("GAIL",        new String[] { "GAIL India",             "NIFTYOILANDGAS" });
        stocks.put("HPCL",        new String[] { "Hindustan Petroleum",    "NIFTYOILANDGAS" });
        stocks.put("PETRONET",    new String[] { "Petronet LNG",           "NIFTYOILANDGAS" });
        stocks.put("OIL",         new String[] { "Oil India",              "NIFTYOILANDGAS" });
        stocks.put("IGL",         new String[] { "Indraprastha Gas",       "NIFTYOILANDGAS" });
        // Realty (NIFTYREALTY)
        stocks.put("DLF",         new String[] { "DLF",                    "NIFTYREALTY" });
        stocks.put("GODREJPROP",  new String[] { "Godrej Properties",      "NIFTYREALTY" });
        stocks.put("LODHA",       new String[] { "Macrotech Developers",   "NIFTYREALTY" });
        stocks.put("PRESTIGE",    new String[] { "Prestige Estates",       "NIFTYREALTY" });
        stocks.put("PHOENIXLTD",  new String[] { "Phoenix Mills",          "NIFTYREALTY" });
        stocks.put("OBEROIRLTY",  new String[] { "Oberoi Realty",          "NIFTYREALTY" });
        // Media (NIFTYMEDIA)
        stocks.put("ZEEL",        new String[] { "Zee Entertainment",      "NIFTYMEDIA" });
        stocks.put("SUNTV",       new String[] { "Sun TV Network",         "NIFTYMEDIA" });
        stocks.put("PVRINOX",     new String[] { "PVR INOX",               "NIFTYMEDIA" });
        // Consumer Durables (NIFTYCONSRDURBL)
        stocks.put("HAVELLS",     new String[] { "Havells India",          "NIFTYCONSRDURBL" });
        stocks.put("BERGEPAINT",  new String[] { "Berger Paints",          "NIFTYCONSRDURBL" });
        stocks.put("VOLTAS",      new String[] { "Voltas",                 "NIFTYCONSRDURBL" });
        stocks.put("DIXON",       new String[] { "Dixon Technologies",     "NIFTYCONSRDURBL" });
        stocks.put("CROMPTON",    new String[] { "Crompton Greaves Consumer","NIFTYCONSRDURBL" });
        stocks.put("AMBER",       new String[] { "Amber Enterprises",      "NIFTYCONSRDURBL" });

        // ── Composite-index extensions (cement / defence / chemicals / capital goods /
        //    capital markets / power / retail / telecom infra). All FNO eligible. ───
        // Cement (NIFTYINFRA — joins ULTRACEMCO)
        stocks.put("AMBUJACEM",   new String[] { "Ambuja Cements",         "NIFTYINFRA" });
        stocks.put("SHREECEM",    new String[] { "Shree Cement",           "NIFTYINFRA" });
        stocks.put("DALBHARAT",   new String[] { "Dalmia Bharat",          "NIFTYINFRA" });
        stocks.put("ACC",         new String[] { "ACC",                    "NIFTYINFRA" });
        // Defence / Aerospace (NIFTYINFRA)
        stocks.put("BEL",         new String[] { "Bharat Electronics",     "NIFTYINFRA" });
        stocks.put("HAL",         new String[] { "Hindustan Aeronautics",  "NIFTYINFRA" });
        stocks.put("BDL",         new String[] { "Bharat Dynamics",        "NIFTYINFRA" });
        stocks.put("MAZDOCK",     new String[] { "Mazagon Dock Shipbuilders","NIFTYINFRA" });
        // Capital Goods (NIFTYINFRA — joins LT)
        stocks.put("SIEMENS",     new String[] { "Siemens",                "NIFTYINFRA" });
        stocks.put("ABB",         new String[] { "ABB India",              "NIFTYINFRA" });
        stocks.put("CUMMINSIND",  new String[] { "Cummins India",          "NIFTYINFRA" });
        stocks.put("BHEL",        new String[] { "BHEL",                   "NIFTYINFRA" });
        stocks.put("POLYCAB",     new String[] { "Polycab India",          "NIFTYINFRA" });
        // Capital Markets / Exchanges / Brokerage (FINNIFTY)
        stocks.put("BSE",         new String[] { "BSE Limited",            "FINNIFTY" });
        stocks.put("CDSL",        new String[] { "Central Depository Services", "FINNIFTY" });
        stocks.put("MCX",         new String[] { "Multi Commodity Exchange","FINNIFTY" });
        stocks.put("ANGELONE",    new String[] { "Angel One",              "FINNIFTY" });
        // Power / Renewable extras (NIFTYENERGY)
        stocks.put("JSWENERGY",   new String[] { "JSW Energy",             "NIFTYENERGY" });
        stocks.put("NHPC",        new String[] { "NHPC",                   "NIFTYENERGY" });
        stocks.put("SJVN",        new String[] { "SJVN",                   "NIFTYENERGY" });
        // Chemicals (NIFTYCOMMODITIES)
        stocks.put("PIDILITIND",  new String[] { "Pidilite Industries",    "NIFTYCOMMODITIES" });
        stocks.put("SRF",         new String[] { "SRF",                    "NIFTYCOMMODITIES" });
        stocks.put("UPL",         new String[] { "UPL",                    "NIFTYCOMMODITIES" });
        // Retail / Consumption (NIFTYCONSUMPTION — joins TRENT, ETERNAL)
        stocks.put("ABFRL",       new String[] { "Aditya Birla Fashion",   "NIFTYCONSUMPTION" });
        stocks.put("NYKAA",       new String[] { "FSN E-Commerce (Nykaa)", "NIFTYCONSUMPTION" });
        stocks.put("JUBLFOOD",    new String[] { "Jubilant FoodWorks",     "NIFTYCONSUMPTION" });
        // Telecom Infrastructure (NIFTYSERVSECTOR — joins BHARTIARTL)
        stocks.put("INDUSTOWER",  new String[] { "Indus Towers",           "NIFTYSERVSECTOR" });

        int added = 0, migrated = 0, membershipBackfilled = 0, skippedNoIndex = 0;
        for (Map.Entry<String, String[]> e : stocks.entrySet()) {
            String ticker = e.getKey();
            String name = e.getValue()[0];
            String indexTicker = e.getValue()[1];
            StockEntity.Membership membership = classifyMembership(ticker);

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
                boolean dirty = false;
                if (row.getPrimaryIndex() == null) {
                    row.setPrimaryIndex(idx);
                    dirty = true;
                    migrated++;
                }
                // Membership reconciliation: for every stock the seeder manages, the
                // canonical classification is authoritative — sync the row to whatever
                // canonical says (N50, NN50, or OTHERS). This means a stock removed from
                // NN50 in the canonical list will be downgraded to OTHERS on next boot,
                // which is the desired behavior so the table counts always match the
                // hardcoded list. Stocks added manually via the UI (not in the seeder
                // map) are never touched by this loop.
                StockEntity.Membership current = row.getMembership();
                if (current != membership) {
                    row.setMembership(membership);
                    dirty = true;
                    membershipBackfilled++;
                }
                if (dirty) stockRepo.save(row);
                continue;
            }
            stockRepo.save(new StockEntity(ticker, name, idx, true, membership));
            added++;
        }
        if (added > 0 || migrated > 0 || membershipBackfilled > 0 || skippedNoIndex > 0) {
            log.info("[StockUniverseSeeder] Stocks: added {}, primary-index backfilled {}, membership backfilled {} (skipped {} no-index); table size now {}",
                added, migrated, membershipBackfilled, skippedNoIndex, stockRepo.count());
        }
    }
}
