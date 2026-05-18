package com.swiftpay.ledger.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Counter paymentCompletedCounter(
            MeterRegistry registry
    ) {

        return Counter.builder(
                        "swiftpay_payment_completed_total"
                )
                .description(
                        "Total successfully completed payments"
                )
                .register(registry);
    }

    @Bean
    public Counter paymentFailedCounter(
            MeterRegistry registry
    ) {

        return Counter.builder(
                        "swiftpay_payment_failed_total"
                )
                .description(
                        "Total failed payments"
                )
                .register(registry);
    }

    @Bean
    public Counter duplicateEventCounter(
            MeterRegistry registry
    ) {

        return Counter.builder(
                        "swiftpay_duplicate_event_total"
                )
                .description(
                        "Duplicate Kafka events ignored"
                )
                .register(registry);
    }

    @Bean
    public Counter insufficientFundsCounter(
            MeterRegistry registry
    ) {

        return Counter.builder(
                        "swiftpay_insufficient_funds_total"
                )
                .description(
                        "Payments rejected due to insufficient funds"
                )
                .register(registry);
    }

    @Bean
    public Timer paymentProcessingTimer(
            MeterRegistry registry
    ) {

        return Timer.builder(
                        "swiftpay_payment_processing_latency"
                )
                .description(
                        "Payment processing latency"
                )
                .register(registry);
    }
}