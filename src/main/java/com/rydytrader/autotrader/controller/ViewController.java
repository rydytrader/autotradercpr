package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.service.LoginService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.store.ModeStore;
import com.rydytrader.autotrader.store.TokenStore;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@Controller
public class ViewController {

    private final TokenStore        tokenStore;
    private final PollingService    pollingService;
    private final LoginService      loginService;
    private final FyersProperties   fyersProperties;
    private final ModeStore         modeStore;
    private final MarketDataService marketDataService;
    private final OrderEventService orderEventService;

    public ViewController(TokenStore tokenStore,
                           PollingService pollingService,
                           LoginService loginService,
                           FyersProperties fyersProperties,
                           ModeStore modeStore,
                           MarketDataService marketDataService,
                           OrderEventService orderEventService) {
        this.tokenStore        = tokenStore;
        this.pollingService    = pollingService;
        this.loginService      = loginService;
        this.fyersProperties   = fyersProperties;
        this.modeStore         = modeStore;
        this.marketDataService = marketDataService;
        this.orderEventService = orderEventService;
    }

    @GetMapping("/")
    public String home() {
        // Already logged in — go straight to home dashboard
        if (tokenStore.isTokenAvailable()) {
            return "redirect:/home";
        }
        return "login";
    }

    @GetMapping("/login")
    public void redirectToFyers(@RequestParam(value = "mode", required = false) String mode,
                                 HttpServletResponse response) throws IOException {
        // Safety net: ensure mode is LIVE if passed as query param
        if ("LIVE".equalsIgnoreCase(mode) && !modeStore.isLive()) {
            modeStore.setMode(ModeStore.Mode.LIVE);
        }
        String loginUrl = "https://api-t1.fyers.in/api/v3/generate-authcode"
                + "?client_id=" + fyersProperties.getClientId()
                + "&redirect_uri=http://localhost:9090/login/callback"
                + "&response_type=code"
                + "&state=sample";
        response.sendRedirect(loginUrl);
    }

    @GetMapping("/login/callback")
    public String callback(@RequestParam("auth_code") String authCode) {
        String token = loginService.generateAccessToken(authCode);
        if (token != null) {
            tokenStore.setAccessToken(token);
            pollingService.syncPositionOnce();
            pollingService.startPositionSync();
            marketDataService.start();
            orderEventService.start();
            return "redirect:/home";
        }
        System.out.println("Login callback error");
        return "error";
    }

    // ── PAGES (all require token) ─────────────────────────────────────────────
    @GetMapping("/home")
    public String dashboard() {
        if (!tokenStore.isTokenAvailable()) return "redirect:/";
        return "home";
    }

    @GetMapping("/positions")
    public String positions() {
        if (!tokenStore.isTokenAvailable()) return "redirect:/";
        return "positions";
    }

    @GetMapping("/trades")
    public String trades() {
        if (!tokenStore.isTokenAvailable()) return "redirect:/";
        return "trades";
    }

    @GetMapping("/journal")
    public String journal() {
        if (!tokenStore.isTokenAvailable()) return "redirect:/";
        return "journal";
    }

    @GetMapping("/simulator")
    public String simulator() {
        if (!tokenStore.isTokenAvailable()) return "redirect:/";
        return "simulator";
    }

    @GetMapping("/settings")
    public String settings() {
        if (!tokenStore.isTokenAvailable()) return "redirect:/";
        return "settings";
    }
}