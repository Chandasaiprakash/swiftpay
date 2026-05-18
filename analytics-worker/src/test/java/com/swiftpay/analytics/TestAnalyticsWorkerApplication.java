package com.swiftpay.analytics;

import org.springframework.boot.SpringApplication;

public class TestAnalyticsWorkerApplication {

	public static void main(String[] args) {
		SpringApplication.from(AnalyticsWorkerApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
