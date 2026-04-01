package com.rydytrader.autotrader.config;

import jakarta.servlet.http.HttpServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class H2ConsoleConfig {

    @SuppressWarnings("unchecked")
    @Bean
    ServletRegistrationBean<HttpServlet> h2Console() throws Exception {
        Class<? extends HttpServlet> clazz =
            (Class<? extends HttpServlet>) Class.forName("org.h2.server.web.JakartaWebServlet");
        HttpServlet servlet = clazz.getDeclaredConstructor().newInstance();
        ServletRegistrationBean<HttpServlet> registration =
            new ServletRegistrationBean<>(servlet, "/h2-console/*");
        registration.setLoadOnStartup(1);
        return registration;
    }
}
