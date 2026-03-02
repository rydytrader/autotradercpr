package com.rydytrader.autotrader;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point for the Autotrader Spring Boot application.
 * Bootstraps the Spring context, auto-configuration, and all registered beans.
 */
@SpringBootApplication
@EnableConfigurationProperties
public class AutotraderApplication {

	/**
	 * Launches the Spring Boot application.
	 *
	 * @param args command-line arguments passed at startup
	 */
	public static void main(String[] args) {
		SpringApplication.run(AutotraderApplication.class, args);
	}

}
