package com.swiftpay.analytics.service;

import com.swiftpay.analytics.dto.AnalyticsResponse;
import com.swiftpay.analytics.entity.PaymentAnalyticsEntity;
import com.swiftpay.analytics.repository.PaymentAnalyticsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class AnalyticsQueryService {

    private final PaymentAnalyticsRepository
            repository;

    public List<AnalyticsResponse> getAllMetrics() {

        return repository.findAll()
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    private AnalyticsResponse mapToResponse(
            PaymentAnalyticsEntity entity
    ) {

        return AnalyticsResponse.builder()
                .metricName(
                        entity.getMetricName()
                )
                .metricValue(
                        entity.getMetricValue()
                )
                .currency(
                        entity.getCurrency()
                )
                .updatedAt(
                        entity.getUpdatedAt()
                )
                .build();
    }
}