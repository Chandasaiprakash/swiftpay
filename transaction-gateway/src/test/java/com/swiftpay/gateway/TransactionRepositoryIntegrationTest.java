package com.swiftpay.gateway;

import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.entity.TransactionStatus;
import com.swiftpay.gateway.repository.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest // 🧠 SENIOR TIP: Only boots up JPA components, completely ignoring Web, Kafka, and Redis!
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE) // Prevents Spring from trying to find an embedded H2 DB
class TransactionRepositoryIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16") // Using lean alpine for faster test boots
                    .withDatabaseName("swiftpay")
                    .withUsername("swiftpay")
                    .withPassword("swiftpay");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        // Ensure Hibernate auto-creates tables directly inside our clean Testcontainer instance
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.flyway.enabled", () -> "false");
    }

    @Autowired
    private TransactionRepository repository;

    @Test
    void shouldSaveTransactionSuccessfully() {
        TransactionEntity transaction = TransactionEntity.builder()
                .transactionId(UUID.randomUUID())
                .senderId(1001L)
                .receiverId(2005L)
                .amount(new BigDecimal("150.50"))
                .currency("USD")
                .status(TransactionStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        TransactionEntity saved = repository.save(transaction);

        assertThat(saved.getTransactionId()).isNotNull();
        assertThat(saved.getStatus()).isEqualTo(TransactionStatus.PENDING);
    }
}