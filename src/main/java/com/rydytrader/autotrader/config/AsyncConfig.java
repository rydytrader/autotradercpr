package com.rydytrader.autotrader.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Enable async request processing for SSE emitters.
 * Without this, each SSE connection holds a Tomcat thread,
 * which can exhaust the thread pool and cause page hangs.
 */
@Configuration
@EnableAsync
public class AsyncConfig implements WebMvcConfigurer {

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(-1); // no timeout for SSE
    }
}
