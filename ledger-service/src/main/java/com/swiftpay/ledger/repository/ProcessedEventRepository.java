package com.swiftpay.ledger.repository;

import com.swiftpay.ledger.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEventEntity, UUID> {
}