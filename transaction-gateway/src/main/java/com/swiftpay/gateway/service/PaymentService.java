package com.swiftpay.gateway.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.swiftpay.gateway.dto.PaymentRequest;
import com.swiftpay.gateway.dto.PaymentResponse;
import com.swiftpay.gateway.entity.OutboxEventEntity;
import com.swiftpay.gateway.entity.OutboxStatus;
import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.entity.TransactionStatus;
import com.swiftpay.gateway.event.PaymentInitiatedEvent;
import com.swiftpay.gateway.exception.DuplicateTransactionException;
import com.swiftpay.gateway.repository.OutboxRepository;
import com.swiftpay.gateway.repository.TransactionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class PaymentService {

    private static final String IDEMPOTENCY_PREFIX =
            "payment:idempotency:";

    private static final String IDEMPOTENCY_LOCK_VALUE =
            "LOCKED";

    private final TransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    private final Counter paymentInitiatedCounter;
    private final Counter duplicatePaymentCounter;

    private final Timer paymentInitiationTimer;

    public PaymentService(
            TransactionRepository transactionRepository,
            OutboxRepository outboxRepository,
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry
    ) {

        this.transactionRepository = transactionRepository;
        this.outboxRepository = outboxRepository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;

        this.paymentInitiatedCounter =
                Counter.builder("swiftpay_payment_initiated_total")
                        .description("Total initiated payments")
                        .register(meterRegistry);

        this.duplicatePaymentCounter =
                Counter.builder("swiftpay_payment_duplicate_total")
                        .description("Duplicate payment requests rejected")
                        .register(meterRegistry);

        this.paymentInitiationTimer =
                Timer.builder("swiftpay_payment_initiation_latency")
                        .description("Payment initiation latency")
                        .register(meterRegistry);
    }

    @Transactional
    public PaymentResponse initiatePayment(
            String idempotencyKey,
            PaymentRequest request
    ) {

        Timer.Sample sample = Timer.start(meterRegistry);

        String redisKey = IDEMPOTENCY_PREFIX + idempotencyKey;

        try {

            /*
             * Redis SETNX
             * Prevents duplicate payment processing
             */
            Boolean acquired = redisTemplate
                    .opsForValue()
                    .setIfAbsent(
                            redisKey,
                            IDEMPOTENCY_LOCK_VALUE,
                            Duration.ofHours(24)
                    );

            if (Boolean.FALSE.equals(acquired)) {

                duplicatePaymentCounter.increment();

                throw new DuplicateTransactionException(
                        "Duplicate payment request detected"
                );
            }

            UUID transactionId = UUID.randomUUID();

            LocalDateTime now = LocalDateTime.now();

            /*
             * Persist transaction
             */
            TransactionEntity transaction =
                    TransactionEntity.builder()
                            .transactionId(transactionId)
                            .senderId(request.getSenderId())
                            .receiverId(request.getReceiverId())
                            .amount(request.getAmount())
                            .currency(request.getCurrency())
                            .status(TransactionStatus.PENDING)
                            .createdAt(now)
                            .updatedAt(now)
                            .build();

            transactionRepository.save(transaction);

            /*
             * Build domain event
             */
            PaymentInitiatedEvent event =
                    PaymentInitiatedEvent.builder()
                            .transactionId(transactionId)
                            .senderId(request.getSenderId())
                            .receiverId(request.getReceiverId())
                            .amount(request.getAmount())
                            .currency(request.getCurrency())
                            .createdAt(now)
                            .build();

            /*
             * Persist transactional outbox
             */
            OutboxEventEntity outbox =
                    OutboxEventEntity.builder()
                            .id(UUID.randomUUID())
                            .aggregateId(transactionId)
                            .eventType("PaymentInitiated")
                            .payload(
                                    objectMapper.writeValueAsString(event)
                            )
                            .status(OutboxStatus.PENDING)
                            .createdAt(now)
                            .build();

            outboxRepository.save(outbox);

            paymentInitiatedCounter.increment();

            return PaymentResponse.builder()
                    .transactionId(transactionId)
                    .status(TransactionStatus.PENDING)
                    .message(
                            "Payment request queued successfully"
                    )
                    .build();

        } catch (JsonProcessingException ex) {

            /*
             * Prevent stale idempotency lock
             */
            redisTemplate.delete(redisKey);

            throw new RuntimeException(
                    "Failed to serialize payment event",
                    ex
            );

        } catch (Exception ex) {

            /*
             * Cleanup Redis lock if DB transaction fails
             */
            redisTemplate.delete(redisKey);

            throw ex;

        } finally {

            /*
             * Full request lifecycle latency
             */
            sample.stop(paymentInitiationTimer);
        }
    }
}