package com.rydytrader.autotrader.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    @org.springframework.core.annotation.Order(1)
    public SecurityFilterChain sseFilterChain(HttpSecurity http) throws Exception {
        http
            .securityMatcher("/api/market-ticker/stream")
            .csrf(csrf -> csrf.disable())
            .sessionManagement(session -> session
                .sessionCreationPolicy(org.springframework.security.config.http.SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
        return http.build();
    }

    @Bean
    @org.springframework.core.annotation.Order(2)
    public SecurityFilterChain filterChain(HttpSecurity http,
            org.springframework.security.core.userdetails.UserDetailsService userDetailsService) throws Exception {
        CsrfTokenRequestAttributeHandler csrfHandler = new CsrfTokenRequestAttributeHandler();
        csrfHandler.setCsrfRequestAttributeName("_csrf");

        // Only save real browser navigations as the "post-login redirect target". Without this,
        // a background fetch('/api/user/me') from common.js that 302s to /login gets captured as
        // the saved request — so after the user later logs in via /h2-console, Spring replays
        // the stale XHR URL instead of taking them to /h2-console.
        org.springframework.security.web.savedrequest.HttpSessionRequestCache requestCache =
            new org.springframework.security.web.savedrequest.HttpSessionRequestCache();
        requestCache.setRequestMatcher(req -> {
            String accept = req.getHeader("Accept");
            String xrw    = req.getHeader("X-Requested-With");
            if ("XMLHttpRequest".equalsIgnoreCase(xrw)) return false;
            if (accept != null && accept.contains("application/json") && !accept.contains("text/html")) return false;
            if (req.getRequestURI() != null && req.getRequestURI().startsWith("/api/")) return false;
            return true;
        });

        http
            .requestCache(rc -> rc.requestCache(requestCache))
            .csrf(csrf -> csrf
                .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
                .csrfTokenRequestHandler(csrfHandler)
                // Exempt TradingView webhook, login, logout, and the H2 console
                // (its own form login POSTs without a Spring Security CSRF token).
                .ignoringRequestMatchers("/placeorder", "/login", "/app-logout", "/h2-console/**")
            )
            // Eagerly load CSRF token so the cookie is always set
            .addFilterAfter(new CsrfCookieFilter(), BasicAuthenticationFilter.class)
            .authorizeHttpRequests(auth -> auth
                // Allow async dispatches (SSE pushes) without re-auth
                .dispatcherTypeMatchers(jakarta.servlet.DispatcherType.ASYNC).permitAll()

                // Public: login page, static resources, TradingView webhook
                .requestMatchers("/login", "/css/**", "/js/**", "/favicon*", "/error").permitAll()
                .requestMatchers(HttpMethod.POST, "/placeorder").permitAll()

                // ADMIN only: all mutations (POST/PUT/DELETE) except login
                .requestMatchers(HttpMethod.POST, "/api/**").hasRole("ADMIN")
                .requestMatchers(HttpMethod.PUT, "/api/**").hasRole("ADMIN")
                .requestMatchers(HttpMethod.DELETE, "/api/**").hasRole("ADMIN")

                // Settings and Fyers — ADMIN only
                .requestMatchers("/settings", "/fyers/login", "/fyers/callback").hasRole("ADMIN")

                // H2 console — ADMIN only
                .requestMatchers("/h2-console/**").hasRole("ADMIN")

                // All other requests: authenticated (any role)
                .anyRequest().authenticated()
            )
            .formLogin(form -> form
                .loginPage("/login")
                .loginProcessingUrl("/login")
                .usernameParameter("email")
                .passwordParameter("password")
                // alwaysUse=false → after login, return to the originally requested URL
                // (e.g. /h2-console) instead of always bouncing to /home.
                .defaultSuccessUrl("/home", false)
                .failureUrl("/login?error=true")
                .permitAll()
            )
            .rememberMe(remember -> remember
                .key("traderedge-remember-key-2026")
                .tokenValiditySeconds(7 * 24 * 60 * 60) // 7 days
                .rememberMeParameter("remember-me")
                .userDetailsService(userDetailsService)
            )
            .logout(logout -> logout
                .logoutUrl("/app-logout")
                .logoutSuccessUrl("/login?logout=true")
                .invalidateHttpSession(true)
                .deleteCookies("JSESSIONID", "remember-me")
                .permitAll()
            )
            // Graceful access denied handling
            .exceptionHandling(ex -> ex
                .accessDeniedHandler((request, response, accessDeniedException) -> {
                    String uri = request.getRequestURI();
                    String method = request.getMethod();
                    String user = request.getUserPrincipal() != null ? request.getUserPrincipal().getName() : "anonymous";
                    org.slf4j.LoggerFactory.getLogger("SecurityConfig")
                        .warn("[ACCESS DENIED] {} {} by user={}", method, uri, user);
                    if (uri.startsWith("/api/")) {
                        response.setStatus(403);
                        response.setContentType("application/json");
                        response.getWriter().write("{\"error\":\"Access denied\",\"ok\":false}");
                    } else {
                        response.sendRedirect("/home");
                    }
                })
            )
            // Allow H2 console frames
            .headers(headers -> headers
                .frameOptions(frame -> frame.sameOrigin())
            );

        return http.build();
    }

    /** Eagerly resolves the CSRF token so the XSRF-TOKEN cookie is set on every response. */
    static class CsrfCookieFilter extends OncePerRequestFilter {
        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                        FilterChain filterChain) throws ServletException, IOException {
            CsrfToken csrfToken = (CsrfToken) request.getAttribute(CsrfToken.class.getName());
            if (csrfToken != null) {
                csrfToken.getToken(); // Force token generation + cookie write
            }
            filterChain.doFilter(request, response);
        }
    }
}
