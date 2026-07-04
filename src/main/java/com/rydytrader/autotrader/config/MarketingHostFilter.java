package com.rydytrader.autotrader.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Set;

/**
 * Serves the static marketing HTML when the request's Host header is the
 * marketing hostname (www.traderedge.in or apex traderedge.in). Runs at
 * highest filter precedence so it short-circuits BEFORE Spring Security —
 * the app's login page never leaks on the marketing host.
 *
 * <p>For app.traderedge.in (or any other host) the filter is a no-op and
 * the request falls through to the normal Spring Security + MVC chain.
 *
 * <p>The marketing HTML lives at {@code classpath:marketing/index.html}
 * (NOT under /static, so it isn't also served as a public static asset
 * on app.traderedge.in). It's loaded once at boot and served from an
 * in-memory byte array — zero disk I/O per request.
 */
@Configuration
public class MarketingHostFilter {

    private static final Logger log = LoggerFactory.getLogger(MarketingHostFilter.class);

    private static final Set<String> MARKETING_HOSTS = Set.of(
        "www.traderedge.in",
        "traderedge.in"
    );

    @Bean
    public FilterRegistrationBean<OncePerRequestFilter> marketingHostFilterRegistration() throws IOException {
        byte[] marketingHtml;
        try (var in = new ClassPathResource("marketing/index.html").getInputStream()) {
            marketingHtml = StreamUtils.copyToByteArray(in);
        }
        log.info("[Marketing] loaded {} bytes from classpath:marketing/index.html — hosts: {}",
            marketingHtml.length, MARKETING_HOSTS);

        OncePerRequestFilter filter = new OncePerRequestFilter() {
            @Override
            protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp,
                                            FilterChain chain) throws ServletException, IOException {
                String host = req.getServerName();
                if (host != null && MARKETING_HOSTS.contains(host.toLowerCase())) {
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.setContentType("text/html;charset=utf-8");
                    resp.setContentLength(marketingHtml.length);
                    resp.setHeader("Cache-Control", "public, max-age=300");
                    resp.getOutputStream().write(marketingHtml);
                    resp.getOutputStream().flush();
                    return;
                }
                chain.doFilter(req, resp);
            }
        };

        FilterRegistrationBean<OncePerRequestFilter> reg = new FilterRegistrationBean<>(filter);
        reg.setOrder(Ordered.HIGHEST_PRECEDENCE);
        reg.addUrlPatterns("/*");
        return reg;
    }
}
