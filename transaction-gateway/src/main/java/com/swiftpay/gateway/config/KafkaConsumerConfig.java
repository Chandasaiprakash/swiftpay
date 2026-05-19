package com.swiftpay.gateway.config;

import com.swiftpay.gateway.event.PaymentCompletedEvent;
import com.swiftpay.gateway.event.PaymentFailedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerConfig(
            KafkaProperties kafkaProperties
    ) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<String, PaymentCompletedEvent>
    paymentCompletedConsumerFactory() {

        JsonDeserializer<PaymentCompletedEvent>
                deserializer =
                new JsonDeserializer<>(
                        PaymentCompletedEvent.class
                );

        deserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            PaymentCompletedEvent
            > paymentCompletedKafkaListenerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                PaymentCompletedEvent
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                paymentCompletedConsumerFactory()
        );

        factory.getContainerProperties()
                .setAckMode(
                        ContainerProperties.AckMode.MANUAL
                );

        return factory;
    }

    @Bean
    public ConsumerFactory<String, PaymentFailedEvent>
    paymentFailedConsumerFactory() {

        JsonDeserializer<PaymentFailedEvent>
                deserializer =
                new JsonDeserializer<>(
                        PaymentFailedEvent.class
                );

        deserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            PaymentFailedEvent
            > paymentFailedKafkaListenerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                PaymentFailedEvent
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                paymentFailedConsumerFactory()
        );

        factory.getContainerProperties()
                .setAckMode(
                        ContainerProperties.AckMode.MANUAL
                );

        return factory;
    }
}