package com.swiftpay.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

@SpringBootApplication
public class LedgerServiceApplication {

	public static void main(String[] args) {
		// Force the JVM timezone instantly before Spring Boot boots up
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		SpringApplication.run(LedgerServiceApplication.class, args);
	}

}
