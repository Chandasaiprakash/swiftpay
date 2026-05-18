package com.swiftpay.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.TimeZone;

@SpringBootApplication
@EnableScheduling
public class TransactionGatewayApplication {

	public static void main(String[] args) {
		// 1. Force the JVM timezone instantly before Spring Boot boots up
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

		// 2. Now start the application safely
		SpringApplication.run(TransactionGatewayApplication.class, args);
	}
}