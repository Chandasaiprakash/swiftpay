package com.swiftpay.gateway.dto;

import com.swiftpay.gateway.entity.TransactionStatus;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentStatusResponse {

    private UUID transactionId;

    private TransactionStatus status;

    private Long senderId;

    private Long receiverId;

    private BigDecimal amount;

    private String currency;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}