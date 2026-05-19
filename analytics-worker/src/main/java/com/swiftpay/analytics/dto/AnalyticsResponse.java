package com.swiftpay.analytics.dto;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AnalyticsResponse {

    private String metricName;

    private BigDecimal metricValue;

    private String currency;

    private LocalDateTime updatedAt;
}