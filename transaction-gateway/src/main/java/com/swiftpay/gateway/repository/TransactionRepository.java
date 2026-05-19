package com.swiftpay.gateway.repository;

import com.swiftpay.gateway.entity.TransactionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<TransactionEntity, UUID> {
    List<TransactionEntity> findBySenderIdOrReceiverIdOrderByCreatedAtDesc(
            Long senderId,
            Long receiverId
    );
}