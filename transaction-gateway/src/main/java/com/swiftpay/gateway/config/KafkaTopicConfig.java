package com.swiftpay.gateway.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic paymentsInitiatedTopic() {

        return TopicBuilder
                .name("payments.initiated")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentsCompletedTopic() {

        return TopicBuilder
                .name("payments.completed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentsFailedTopic() {

        return TopicBuilder
                .name("payments.failed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentsDltTopic() {

        return TopicBuilder
                .name("payments.dlt")
                .partitions(3)
                .replicas(1)
                .build();
    }
}