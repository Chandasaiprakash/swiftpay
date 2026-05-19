package com.swiftpay.analytics.service;

import com.swiftpay.analytics.entity.PaymentAnalyticsEntity;
import com.swiftpay.analytics.repository.PaymentAnalyticsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyticsService {

    private final PaymentAnalyticsRepository
            repository;

    @Transactional
    public void incrementCompletedPayments(
            String currency
    ) {

        updateMetric(
                "completed_payments",
                BigDecimal.ONE,
                currency
        );
    }

    @Transactional
    public void incrementFailedPayments(
            String currency
    ) {

        updateMetric(
                "failed_payments",
                BigDecimal.ONE,
                currency
        );
    }

    @Transactional
    public void addPaymentVolume(
            BigDecimal amount,
            String currency
    ) {

        updateMetric(
                "payment_volume",
                amount,
                currency
        );
    }

    private void updateMetric(
            String metricName,
            BigDecimal increment,
            String currency
    ) {

        PaymentAnalyticsEntity metric =
                repository
                        .findByMetricNameAndCurrency(
                                metricName,
                                currency
                        )
                        .orElse(
                                PaymentAnalyticsEntity
                                        .builder()
                                        .metricName(metricName)
                                        .metricValue(BigDecimal.ZERO)
                                        .currency(currency)
                                        .updatedAt(
                                                LocalDateTime.now()
                                        )
                                        .build()
                        );

        metric.setMetricValue(
                metric.getMetricValue()
                        .add(increment)
        );

        metric.setUpdatedAt(
                LocalDateTime.now()
        );

        repository.save(metric);

        log.info(
                "Updated analytics metric={} value={} currency={}",
                metricName,
                metric.getMetricValue(),
                currency
        );
    }
}