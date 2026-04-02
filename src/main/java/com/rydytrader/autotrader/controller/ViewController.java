package com.rydytrader.autotrader.controller;

import com.rydytrader.autotrader.config.FyersProperties;
import com.rydytrader.autotrader.entity.AppUser;
import com.rydytrader.autotrader.repository.AppUserRepository;
import com.rydytrader.autotrader.service.LoginService;
import com.rydytrader.autotrader.service.MarketDataService;
import com.rydytrader.autotrader.service.OrderEventService;
import com.rydytrader.autotrader.service.PollingService;
import com.rydytrader.autotrader.store.TokenStore;
import org.springframework.security.crypto.password.PasswordEncoder;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@Controller
public class ViewController {

    private static final Logger log = LoggerFactory.getLogger(ViewController.class);

    private final TokenStore        tokenStore;
    private final PollingService    pollingService;
    private final LoginService      loginService;
    private final FyersProperties   fyersProperties;
    private final MarketDataService marketDataService;
    private final OrderEventService orderEventService;
    private final AppUserRepository userRepo;
    private final PasswordEncoder  passwordEncoder;

    public ViewController(TokenStore tokenStore,
                           PollingService pollingService,
                           LoginService loginService,
                           FyersProperties fyersProperties,
                           MarketDataService marketDataService,
                           OrderEventService orderEventService,
                           AppUserRepository userRepo,
                           PasswordEncoder passwordEncoder) {
        this.tokenStore        = tokenStore;
        this.pollingService    = pollingService;
        this.loginService      = loginService;
        this.fyersProperties   = fyersProperties;
        this.marketDataService = marketDataService;
        this.orderEventService = orderEventService;
        this.userRepo          = userRepo;
        this.passwordEncoder   = passwordEncoder;
    }

    // ── ROOT ────────────────────────────────────────────────────────────────
    @GetMapping("/")
    public String root() {
        return "redirect:/home";
    }

    // ── APP LOGIN (handled by Spring Security form login) ───────────────────
    @GetMapping("/login")
    public String loginPage() {
        return "login";
    }

    // ── FYERS BROKER CONNECTION (from Settings page) ────────────────────────
    @GetMapping("/fyers/login")
    public void redirectToFyers(HttpServletResponse response) throws IOException {
        String loginUrl = "https://api-t1.fyers.in/api/v3/generate-authcode"
                + "?client_id=" + fyersProperties.getClientId()
                + "&redirect_uri=http://localhost:9090/fyers/callback"
                + "&response_type=code"
                + "&state=sample";
        response.sendRedirect(loginUrl);
    }

    @GetMapping("/fyers/callback")
    public String fyersCallback(@RequestParam("auth_code") String authCode) {
        String token = loginService.generateAccessToken(authCode);
        if (token != null) {
            tokenStore.setAccessToken(token);
            pollingService.syncPositionOnce();
            pollingService.startPositionSync();
            marketDataService.start();
            orderEventService.start();
            return "redirect:/home";
        }
        log.error("Fyers login callback error");
        return "redirect:/home?fyers=error";
    }

    @GetMapping("/api/fyers/status")
    @ResponseBody
    public Map<String, Object> fyersStatus() {
        return Map.of(
            "connected", tokenStore.isTokenAvailable(),
            "clientId", fyersProperties.getClientId()
        );
    }

    @GetMapping("/api/user/me")
    @ResponseBody
    public Map<String, Object> currentUser(org.springframework.security.core.Authentication auth) {
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        if (auth != null && auth.isAuthenticated()) {
            String email = auth.getName();
            var user = userRepo.findByEmail(email);
            if (user.isPresent()) {
                var u = user.get();
                result.put("email", u.getEmail());
                result.put("firstName", u.getFirstName() != null ? u.getFirstName() : "");
                result.put("lastName", u.getLastName() != null ? u.getLastName() : "");
                result.put("role", u.getRole());
                result.put("type", "ROLE_ADMIN".equals(u.getRole()) ? "Trader" : "Observer");
            }
        }
        result.put("fyersConnected", tokenStore.isTokenAvailable());
        return result;
    }

    // ── USER MANAGEMENT (ADMIN only — POST endpoints blocked for VIEWER by SecurityConfig) ──

    @GetMapping("/api/users")
    @ResponseBody
    public java.util.List<Map<String, Object>> listUsers() {
        return userRepo.findAll().stream().map(u -> {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("id", u.getId());
            m.put("email", u.getEmail());
            m.put("firstName", u.getFirstName() != null ? u.getFirstName() : "");
            m.put("lastName", u.getLastName() != null ? u.getLastName() : "");
            m.put("role", u.getRole());
            m.put("type", "ROLE_ADMIN".equals(u.getRole()) ? "Trader" : "Observer");
            return m;
        }).collect(java.util.stream.Collectors.toList());
    }

    @PostMapping("/api/users")
    @ResponseBody
    public Map<String, Object> createUser(@RequestBody Map<String, String> payload) {
        String email = payload.get("email");
        String password = payload.get("password");
        String role = payload.getOrDefault("role", "ROLE_VIEWER");
        String firstName = payload.getOrDefault("firstName", "");
        String lastName = payload.getOrDefault("lastName", "");

        if (email == null || email.isBlank() || password == null || password.isBlank()) {
            return Map.of("ok", false, "error", "Email and password are required");
        }
        if (userRepo.findByEmail(email).isPresent()) {
            return Map.of("ok", false, "error", "User already exists");
        }

        AppUser user = new AppUser(email, passwordEncoder.encode(password), role, firstName, lastName);
        userRepo.save(user);
        return Map.of("ok", true);
    }

    @PostMapping("/api/users/{id}/update")
    @ResponseBody
    public Map<String, Object> updateUser(@PathVariable Long id, @RequestBody Map<String, String> payload) {
        var opt = userRepo.findById(id);
        if (opt.isEmpty()) return Map.of("ok", false, "error", "User not found");

        AppUser user = opt.get();
        if (payload.containsKey("firstName")) user.setFirstName(payload.get("firstName"));
        if (payload.containsKey("lastName")) user.setLastName(payload.get("lastName"));
        if (payload.containsKey("email")) user.setEmail(payload.get("email"));
        if (payload.containsKey("role")) user.setRole(payload.get("role"));
        if (payload.containsKey("password") && !payload.get("password").isBlank()) {
            user.setPassword(passwordEncoder.encode(payload.get("password")));
        }
        userRepo.save(user);
        return Map.of("ok", true);
    }

    @PostMapping("/api/users/{id}/delete")
    @ResponseBody
    public Map<String, Object> deleteUser(@PathVariable Long id) {
        if (!userRepo.existsById(id)) return Map.of("ok", false, "error", "User not found");
        userRepo.deleteById(id);
        return Map.of("ok", true);
    }

    // ── PAGES (all protected by Spring Security — any authenticated user) ──
    @GetMapping("/home")
    public String dashboard() { return "home"; }

    @GetMapping("/positions")
    public String positions() { return "positions"; }

    @GetMapping("/trades")
    public String trades() { return "trades"; }

    @GetMapping("/journal")
    public String journal() { return "journal"; }

    @GetMapping("/scanner")
    public String scanner() { return "scanner"; }

    @GetMapping("/console")
    public String console() { return "console"; }

    @GetMapping("/monitoring")
    public String monitoring() { return "monitoring"; }

    @GetMapping("/settings")
    public String settings() { return "settings"; }
}
