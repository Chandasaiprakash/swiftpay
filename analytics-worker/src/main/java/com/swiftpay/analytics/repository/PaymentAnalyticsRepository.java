package com.swiftpay.analytics.repository;

import com.swiftpay.analytics.entity.PaymentAnalyticsEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentAnalyticsRepository
        extends JpaRepository<
        PaymentAnalyticsEntity,
        Long
        > {

    Optional<PaymentAnalyticsEntity>
    findByMetricNameAndCurrency(
            String metricName,
            String currency
    );
}