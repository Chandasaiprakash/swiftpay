package com.swiftpay.gateway.dto;

import com.swiftpay.gateway.entity.TransactionStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public record TransactionHistoryResponse(

        UUID transactionId,

        Long senderId,

        Long receiverId,

        BigDecimal amount,

        String currency,

        TransactionStatus status,

        LocalDateTime createdAt

) {
}