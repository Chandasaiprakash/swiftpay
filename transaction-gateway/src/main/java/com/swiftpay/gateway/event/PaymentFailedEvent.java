package com.swiftpay.gateway.event;

import com.fasterxml.jackson.annotation.JsonFormat;
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

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private LocalDateTime failedAt;
}