package com.swiftpay.ledger.config;

import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

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
    public ConsumerFactory<String, PaymentInitiatedEvent>
    consumerFactory() {

        Map<String, Object> props =
                new HashMap<>(
                        kafkaProperties.buildConsumerProperties()
                );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        JsonDeserializer<PaymentInitiatedEvent>
                jsonDeserializer =
                new JsonDeserializer<>(
                        PaymentInitiatedEvent.class
                );

        jsonDeserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
        );
    }

    @Bean
    public ProducerFactory<String, Object>
    producerFactory() {

        Map<String, Object> props =
                new HashMap<>(
                        kafkaProperties.buildProducerProperties()
                );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class
        );

        props.put(
                JsonSerializer.ADD_TYPE_INFO_HEADERS,
                false
        );

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {

        return new KafkaTemplate<>(
                producerFactory()
        );
    }

    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<String, Object> kafkaTemplate
    ) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, ex) ->
                                new TopicPartition(
                                        "payments.dlt",
                                        record.partition()
                                )
                );

        /*
         * Retry 3 times
         * 2-second interval
         */
        FixedBackOff backOff =
                new FixedBackOff(2000L, 3);

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(
                        recoverer,
                        backOff
                );

        /*
         * DO NOT retry business failures
         */
        errorHandler.addNotRetryableExceptions(
                RuntimeException.class
        );

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            PaymentInitiatedEvent
            > kafkaListenerContainerFactory(
            DefaultErrorHandler errorHandler
    ) {

        ConcurrentKafkaListenerContainerFactory<
                String,
                PaymentInitiatedEvent
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                consumerFactory()
        );

        factory.setCommonErrorHandler(
                errorHandler
        );

        factory.getContainerProperties()
                .setAckMode(
                        ContainerProperties.AckMode.MANUAL
                );

        factory.setConcurrency(3);

        return factory;
    }
}