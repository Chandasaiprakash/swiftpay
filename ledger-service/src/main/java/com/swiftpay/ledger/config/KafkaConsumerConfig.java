package com.swiftpay.ledger.config;

import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<String, PaymentInitiatedEvent> consumerFactory() {
        // Inherits default bootstrap servers, group IDs, etc., cleanly from your YAML
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Explicitly point the configuration property to String class to avoid the Json clash
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Build the target deserializer cleanly
        JsonDeserializer<PaymentInitiatedEvent> jsonDeserializer = new JsonDeserializer<>(
                PaymentInitiatedEvent.class,
                false // false means do NOT use the properties headers configuration path
        );
        jsonDeserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentInitiatedEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PaymentInitiatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        /*
         * CRITICAL FOR FINTECH LEDGER SAFETY
         * Manual acknowledgement only: Offset commits only after DB persistence succeeds.
         */
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Tuned for high-throughput scaling matching our 250 TPS target
        factory.setConcurrency(3);

        return factory;
    }
}