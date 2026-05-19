package com.swiftpay.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.TimeZone;

@SpringBootApplication
public class AnalyticsWorkerApplication {

	public static void main(String[] args) {
		// 1. Force the JVM timezone instantly before Spring Boot boots up
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

		// 2. Now start the application safely
		SpringApplication.run(AnalyticsWorkerApplication.class, args);
	}

}
