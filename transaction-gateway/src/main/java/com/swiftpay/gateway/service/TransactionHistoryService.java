package com.swiftpay.gateway.service;

import com.swiftpay.gateway.dto.TransactionHistoryResponse;
import com.swiftpay.gateway.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TransactionHistoryService {

    private final TransactionRepository
            transactionRepository;

    public List<TransactionHistoryResponse>
    getUserTransactionHistory(
            Long userId
    ) {

        return transactionRepository
                .findBySenderIdOrReceiverIdOrderByCreatedAtDesc(
                        userId,
                        userId
                )
                .stream()
                .map(transaction -> new TransactionHistoryResponse(

                        transaction.getTransactionId(),

                        transaction.getSenderId(),

                        transaction.getReceiverId(),

                        transaction.getAmount(),

                        transaction.getCurrency(),

                        transaction.getStatus(),

                        transaction.getCreatedAt()
                ))
                .toList();
    }
}
