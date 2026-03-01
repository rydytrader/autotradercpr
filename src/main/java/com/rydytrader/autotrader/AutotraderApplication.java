package com.rydytrader.autotrader;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class AutotraderApplication {

	public static void main(String[] args) {
		SpringApplication.run(AutotraderApplication.class, args);
	}

}
