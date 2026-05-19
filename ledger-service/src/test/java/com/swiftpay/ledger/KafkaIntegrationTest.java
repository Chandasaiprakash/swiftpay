package com.swiftpay.ledger;

import com.swiftpay.ledger.event.PaymentCompletedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaIntegrationTest {

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(
                    DockerImageName.parse(
                            "confluentinc/cp-kafka:7.6.1"
                    )
            );

    @Test
    void shouldPublishAndConsumeEvent() {

        Map<String, Object> producerProps =
                Map.of(
                        "bootstrap.servers",
                        kafka.getBootstrapServers(),

                        "key.serializer",
                        org.apache.kafka.common.serialization
                                .StringSerializer.class,

                        "value.serializer",
                        JsonSerializer.class
                );

        KafkaTemplate<String, Object> kafkaTemplate =
                new KafkaTemplate<>(
                        new DefaultKafkaProducerFactory<>(
                                producerProps
                        )
                );

        PaymentCompletedEvent event =
                PaymentCompletedEvent.builder()
                        .transactionId(
                                UUID.randomUUID()
                        )
                        .senderId(1001L)
                        .receiverId(2005L)
                        .amount(
                                new BigDecimal("150.50")
                        )
                        .currency("USD")
                        .completedAt(
                                LocalDateTime.now()
                        )
                        .build();

        kafkaTemplate.send(
                "payments.completed",
                event.getTransactionId().toString(),
                event
        );

        Map<String, Object> consumerProps =
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafka.getBootstrapServers(),

                        ConsumerConfig.GROUP_ID_CONFIG,
                        "test-group",

                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",

                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class,

                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        JsonDeserializer.class,

                        JsonDeserializer.TRUSTED_PACKAGES,
                        "*"
                );

        JsonDeserializer<PaymentCompletedEvent>
                deserializer =
                new JsonDeserializer<>(
                        PaymentCompletedEvent.class
                );

        deserializer.addTrustedPackages("*");

        KafkaConsumer<String, PaymentCompletedEvent>
                consumer =
                new KafkaConsumer<>(
                        consumerProps,
                        new StringDeserializer(),
                        deserializer
                );

        consumer.subscribe(
                java.util.List.of(
                        "payments.completed"
                )
        );

        ConsumerRecords<
                String,
                PaymentCompletedEvent
                > records =
                consumer.poll(
                        Duration.ofSeconds(10)
                );

        assertThat(records.count())
                .isGreaterThan(0);

        PaymentCompletedEvent received =
                records.iterator()
                        .next()
                        .value();

        assertThat(received.getCurrency())
                .isEqualTo("USD");

        assertThat(received.getAmount())
                .isEqualByComparingTo(
                        "150.50"
                );

        consumer.close();
    }
}