package com.swiftpay.gateway.dto;

import com.swiftpay.gateway.entity.TransactionStatus;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentResponse {

    private UUID transactionId;

    private TransactionStatus status;

    private String message;

    private LocalDateTime timestamp = LocalDateTime.now();
}