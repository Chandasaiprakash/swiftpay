package com.swiftpay.gateway.service;

import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.entity.TransactionStatus;
import com.swiftpay.gateway.repository.TransactionRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionStatusUpdaterService {

    private final TransactionRepository
            transactionRepository;

    @Transactional
    public void markCompleted(
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

        transaction.setStatus(
                TransactionStatus.COMPLETED
        );

        transaction.setUpdatedAt(
                LocalDateTime.now()
        );

        transactionRepository.save(transaction);

        log.info(
                "Transaction marked COMPLETED transactionId={}",
                transactionId
        );
    }

    @Transactional
    public void markFailed(
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

        transaction.setStatus(
                TransactionStatus.FAILED
        );

        transactionRepository.save(transaction);

        log.info(
                "Transaction marked FAILED transactionId={}",
                transactionId
        );
    }
}