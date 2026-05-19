package com.swiftpay.analytics.event;

import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentFailedEvent {

    private UUID transactionId;

    private String currency;

    private String reason;

    private LocalDateTime failedAt;
}