package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.config.TelegramProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class TelegramService {

    private static final Logger log = LoggerFactory.getLogger(TelegramService.class);

    private final TelegramProperties props;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public TelegramService(TelegramProperties props) {
        this.props = props;
    }

    // ── Internal send ─────────────────────────────────────────────────────────

    public void sendMessage(String text) {
        if (!props.isEnabled() || props.getBotToken().isBlank() || props.getChatId().isBlank()) return;

        executor.submit(() -> {
            try {
                String encoded = URLEncoder.encode(text, StandardCharsets.UTF_8);
                String url = "https://api.telegram.org/bot" + props.getBotToken()
                    + "/sendMessage?chat_id=" + props.getChatId()
                    + "&text=" + encoded;

                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    log.error("[Telegram] Failed to send message: HTTP {} | {}", response.statusCode(), response.body());
                }
            } catch (Exception e) {
                log.error("[Telegram] Error sending message: {}", e.getMessage());
            }
        });
    }

    private static String fmt(double v) { return String.format("%,.2f", v); }
    private static String pnlFmt(double v) { return (v >= 0 ? "+" : "") + fmt(v); }

    // ── Structured notifications ────────────────────────────────────────────

    public void notifyTradeOpened(String symbol, String side, int qty,
                                   double entry, double sl, double target,
                                   String setup, String probability) {
        sendMessage("TRADE OPENED\n"
            + symbol + " | " + side + " | " + qty + " qty\n"
            + "Entry: " + fmt(entry) + "\n"
            + "SL: " + fmt(sl) + "\n"
            + "Target: " + fmt(target) + "\n"
            + "Setup: " + setup + " | " + probability);
    }

    public void notifySlModified(String symbol, String side, int qty,
                                  double newSl, double oldSl, double peak) {
        sendMessage("SL MODIFIED\n"
            + symbol + " | " + side + " | " + qty + " qty\n"
            + "New SL: " + fmt(newSl) + " (was " + fmt(oldSl) + ")\n"
            + "Peak: " + fmt(peak));
    }

    public void notifyT1Hit(String symbol, String side, int qty,
                             double exit, double pnl,
                             double breakevenSl, int remainingQty) {
        sendMessage("TARGET 1 HIT\n"
            + symbol + " | " + side + " | " + qty + " qty closed\n"
            + "Exit: " + fmt(exit) + " | P&L: " + pnlFmt(pnl) + "\n"
            + "SL moved to breakeven (" + fmt(breakevenSl) + ") for remaining " + remainingQty + " qty");
    }

    public void notifyT2Hit(String symbol, String side, int qty,
                             double exit, double pnl) {
        sendMessage("TARGET 2 HIT\n"
            + symbol + " | " + side + " | " + qty + " qty closed\n"
            + "Exit: " + fmt(exit) + " | P&L: " + pnlFmt(pnl) + "\n"
            + "Position fully closed");
    }

    public void notifySlHit(String symbol, String side, int qty,
                             double exit, double pnl, String reason) {
        sendMessage(reason.toUpperCase() + "\n"
            + symbol + " | " + side + " | " + qty + " qty closed\n"
            + "Exit: " + fmt(exit) + " | P&L: " + pnlFmt(pnl));
    }

    public void notifyAutoSquareoff(List<String> lines, double totalPnl) {
        StringBuilder sb = new StringBuilder("AUTO SQUAREOFF\n");
        for (String line : lines) sb.append(line).append("\n");
        sb.append("---\nTotal: ").append(pnlFmt(totalPnl));
        sendMessage(sb.toString());
    }

    public void notifyBotReady(int watchlistCount, int narrowCount, int insideCount,
                                int atrLoaded, int weeklyLevels, int higherTfMinutes) {
        sendMessage("BOT READY\n"
            + "Watchlist: " + watchlistCount + " symbols\n"
            + "Narrow CPR: " + narrowCount + " | Inside CPR: " + insideCount + "\n"
            + "ATR loaded: " + atrLoaded + " | Weekly levels: " + weeklyLevels + "\n"
            + "Trading TF: 5 min | Higher TF: " + higherTfMinutes + " min\n"
            + "System ready for trading");
    }

    public void notifyDaySummary(int trades, int wins, int losses,
                                  double grossPnl, double charges, double netPnl) {
        if (trades == 0) {
            sendMessage("DAY SUMMARY\nNo trades today.");
            return;
        }
        double winRate = trades > 0 ? (wins * 100.0 / trades) : 0;
        sendMessage("DAY SUMMARY\n"
            + "Trades: " + trades + " | Wins: " + wins + " | Losses: " + losses + "\n"
            + "Win Rate: " + String.format("%.1f", winRate) + "%\n"
            + "Gross P&L: " + pnlFmt(grossPnl) + "\n"
            + "Charges: " + fmt(charges) + "\n"
            + "Net P&L: " + pnlFmt(netPnl));
    }
}
