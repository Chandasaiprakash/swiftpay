package com.swiftpay.gateway.service;

import com.swiftpay.gateway.dto.PaymentStatusResponse;
import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentQueryService {

    private final TransactionRepository
            transactionRepository;

    public PaymentStatusResponse getPaymentStatus(
            UUID transactionId
    ) {

        TransactionEntity transaction =
                transactionRepository
                        .findByTransactionId(
                                transactionId
                        )
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Transaction not found"
                                        )
                        );

        return PaymentStatusResponse.builder()
                .transactionId(
                        transaction.getTransactionId()
                )
                .status(
                        transaction.getStatus()
                )
                .senderId(
                        transaction.getSenderId()
                )
                .receiverId(
                        transaction.getReceiverId()
                )
                .amount(
                        transaction.getAmount()
                )
                .currency(
                        transaction.getCurrency()
                )
                .createdAt(
                        transaction.getCreatedAt()
                )
                .updatedAt(
                        transaction.getUpdatedAt()
                )
                .build();
    }
}