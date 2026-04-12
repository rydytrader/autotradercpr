package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.dto.TradeRecord;
import com.rydytrader.autotrader.service.JournalService;
import com.rydytrader.autotrader.service.MarketHolidayService;
import com.rydytrader.autotrader.service.TradeHistoryService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.*;

@Controller
public class CalendarController {

    private final TradeHistoryService tradeHistoryService;
    private final JournalService journalService;
    private final MarketHolidayService holidayService;

    public CalendarController(TradeHistoryService tradeHistoryService,
                              JournalService journalService,
                              MarketHolidayService holidayService) {
        this.tradeHistoryService = tradeHistoryService;
        this.journalService = journalService;
        this.holidayService = holidayService;
    }

    @GetMapping("/calendar")
    public String calendarPage() {
        return "calendar";
    }

    /**
     * Month view: daily P&L map for the requested month + full summary stats.
     * Query: /api/calendar/month?year=2026&month=4
     */
    @GetMapping("/api/calendar/month")
    @ResponseBody
    public Map<String, Object> monthView(@RequestParam int year, @RequestParam int month) {
        YearMonth ym = YearMonth.of(year, month);
        LocalDate from = ym.atDay(1);
        LocalDate to = ym.atEndOfMonth();

        List<TradeRecord> trades = tradeHistoryService.getTradesForRange(from, to);

        // Aggregate per-day P&L as plain maps (Jackson-friendly, no static-analysis noise)
        Map<String, Map<String, Number>> perDay = new TreeMap<>();
        for (TradeRecord t : trades) {
            String date = extractDate(t, from);
            if (date == null) continue;
            Map<String, Number> d = perDay.computeIfAbsent(date, k -> {
                Map<String, Number> m = new LinkedHashMap<>();
                m.put("netPnl", 0.0);
                m.put("trades", 0);
                m.put("wins", 0);
                m.put("losses", 0);
                return m;
            });
            d.put("netPnl", d.get("netPnl").doubleValue() + t.getNetPnl());
            d.put("trades", d.get("trades").intValue() + 1);
            if (t.getPnl() > 0) d.put("wins", d.get("wins").intValue() + 1);
            else if (t.getPnl() < 0) d.put("losses", d.get("losses").intValue() + 1);
        }

        // Trading holidays in this month: { "YYYY-MM-DD": "Holiday Name" }
        Map<String, String> holidays = new LinkedHashMap<>();
        for (int d = 1; d <= ym.lengthOfMonth(); d++) {
            LocalDate day = ym.atDay(d);
            if (holidayService.isHoliday(day)) {
                holidays.put(day.toString(), holidayService.getHolidayName(day));
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("year", year);
        result.put("month", month);
        result.put("firstDay", from.toString());
        result.put("lastDay", to.toString());
        result.put("daysInMonth", ym.lengthOfMonth());
        result.put("firstDayOfWeek", from.getDayOfWeek().getValue()); // Mon=1..Sun=7
        result.put("dailyPnl", perDay);
        result.put("holidays", holidays);
        result.put("summary", journalService.computeMetrics(trades));
        return result;
    }

    /**
     * Financial Year view: 12 months from April(fyStart) to March(fyStart+1).
     * Query: /api/calendar/fy?fyStart=2026  →  April 2026 through March 2027
     */
    @GetMapping("/api/calendar/fy")
    @ResponseBody
    public Map<String, Object> fyView(@RequestParam int fyStart) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("fyStart", fyStart);
        result.put("fyEnd", fyStart + 1);

        List<Map<String, Object>> months = new ArrayList<>();
        List<TradeRecord> allFyTrades = new ArrayList<>();

        // April(fyStart) → December(fyStart), then January(fyStart+1) → March(fyStart+1)
        int[] monthNums = {4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3};
        for (int m : monthNums) {
            int year = (m >= 4) ? fyStart : fyStart + 1;
            YearMonth ym = YearMonth.of(year, m);
            LocalDate from = ym.atDay(1);
            LocalDate to = ym.atEndOfMonth();
            List<TradeRecord> trades = tradeHistoryService.getTradesForRange(from, to);
            allFyTrades.addAll(trades);

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("month", m);
            entry.put("year", year);
            entry.put("name", ym.getMonth().name());
            entry.put("summary", journalService.computeMetrics(trades));
            months.add(entry);
        }

        result.put("months", months);
        result.put("fySummary", journalService.computeMetrics(allFyTrades));
        return result;
    }

    private String extractDate(TradeRecord t, LocalDate fallback) {
        String ts = t.getTimestamp();
        if (ts != null && ts.length() >= 10) return ts.substring(0, 10);
        return fallback.toString();
    }
}
