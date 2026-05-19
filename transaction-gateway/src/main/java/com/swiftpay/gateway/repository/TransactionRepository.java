package com.swiftpay.gateway.repository;

import com.swiftpay.gateway.entity.TransactionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<TransactionEntity, UUID> {
    Optional<TransactionEntity> findByTransactionId(UUID transactionId);
}