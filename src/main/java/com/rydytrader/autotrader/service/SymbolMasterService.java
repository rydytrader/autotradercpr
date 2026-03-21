package com.rydytrader.autotrader.service;

import jakarta.annotation.PostConstruct;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SymbolMasterService {

    private final EventService eventService;

    public SymbolMasterService(EventService eventService) {
        this.eventService = eventService;
    }

    // Column indices (0-based), verified against actual CSV sample:
    // [4] = tick size (e.g. 0.1), [9] = Fyers symbol (e.g. NSE:NIFTY26MARFUT)
    private static final int COL_TICK_SIZE = 4;
    private static final int COL_SYMBOL    = 9;

    private static final String[] CSV_URLS = {
        "https://public.fyers.in/sym_details/NSE_FO.csv",
        "https://public.fyers.in/sym_details/NSE_CM.csv"
    };

    private final ConcurrentHashMap<String, Double> tickSizes = new ConcurrentHashMap<>();

    /** Load on startup so tick sizes are available immediately regardless of start time. */
    @PostConstruct
    public void loadOnStartup() {
        loadAll();
    }

    /** Reload every trading day at 9:00 AM to pick up new contract expirations. */
    @Scheduled(cron = "0 0 9 * * MON-FRI")
    public void reloadAtMarketOpen() {
        loadAll();
    }

    /** Returns the tick size for the given Fyers symbol, defaulting to 0.05 if not found. */
    public double getTickSize(String symbol) {
        return tickSizes.getOrDefault(symbol, 0.05);
    }

    /** Returns number of symbols currently loaded. */
    public int getLoadedCount() {
        return tickSizes.size();
    }

    private void loadAll() {
        int loaded = 0;
        for (String csvUrl : CSV_URLS) {
            loaded += loadCsv(csvUrl);
        }
        String msg = "[SymbolMaster] Loaded " + loaded + " symbol tick sizes from Fyers symbol master";
        System.out.println(msg);
        eventService.log(msg);
    }

    private int loadCsv(String csvUrl) {
        int count = 0;
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(csvUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(60_000);

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()))) {
                String line;
                boolean firstLine = true;
                while ((line = br.readLine()) != null) {
                    if (firstLine) { firstLine = false; continue; }  // skip header row
                    String[] cols = line.split(",");
                    if (cols.length <= Math.max(COL_TICK_SIZE, COL_SYMBOL)) continue;
                    try {
                        String symbol = cols[COL_SYMBOL].trim();
                        double tick   = Double.parseDouble(cols[COL_TICK_SIZE].trim());
                        if (!symbol.isEmpty() && tick > 0) {
                            tickSizes.put(symbol, tick);
                            count++;
                        }
                    } catch (NumberFormatException ignored) {}
                }
            }
        } catch (Exception e) {
            System.err.println("[SymbolMaster] Failed to load " + csvUrl + ": " + e.getMessage());
        }
        return count;
    }
}
