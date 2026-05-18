package com.swiftpay.ledger.service;

import com.swiftpay.ledger.entity.AccountEntity;
import com.swiftpay.ledger.entity.ProcessedEventEntity;
import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import com.swiftpay.ledger.exception.InsufficientFundsException;
import com.swiftpay.ledger.repository.AccountRepository;
import com.swiftpay.ledger.repository.ProcessedEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
public class LedgerService {

    private final AccountRepository accountRepository;

    private final ProcessedEventRepository processedEventRepository;

    private final Counter paymentCompletedCounter;

    private final Counter paymentFailedCounter;

    private final Counter duplicateEventCounter;

    private final Counter insufficientFundsCounter;

    private final Timer paymentProcessingTimer;

    public LedgerService(
            AccountRepository accountRepository,
            ProcessedEventRepository processedEventRepository,
            Counter paymentCompletedCounter,
            Counter paymentFailedCounter,
            Counter duplicateEventCounter,
            Counter insufficientFundsCounter,
            Timer paymentProcessingTimer
    ) {

        this.accountRepository = accountRepository;

        this.processedEventRepository =
                processedEventRepository;

        this.paymentCompletedCounter =
                paymentCompletedCounter;

        this.paymentFailedCounter =
                paymentFailedCounter;

        this.duplicateEventCounter =
                duplicateEventCounter;

        this.insufficientFundsCounter =
                insufficientFundsCounter;

        this.paymentProcessingTimer =
                paymentProcessingTimer;
    }

    @Transactional
    public void processPayment(
            PaymentInitiatedEvent event
    ) {

        Timer.Sample sample =
                Timer.start();

        try {

            /*
             * Idempotent consumer protection
             */
            boolean alreadyProcessed =
                    processedEventRepository.existsById(
                            event.getTransactionId()
                    );

            if (alreadyProcessed) {

                duplicateEventCounter.increment();

                log.warn(
                        "Duplicate event ignored transactionId={}",
                        event.getTransactionId()
                );

                return;
            }

            /*
             * Ordered pessimistic locking
             * Prevents double-spend race conditions
             */
            List<AccountEntity> accounts =
                    accountRepository.lockAccounts(
                            List.of(
                                    event.getSenderId(),
                                    event.getReceiverId()
                            )
                    );

            if (accounts.size() != 2) {

                paymentFailedCounter.increment();

                throw new RuntimeException(
                        "Accounts not found"
                );
            }

            AccountEntity sender =
                    accounts.get(0).getUserId()
                            .equals(event.getSenderId())
                            ? accounts.get(0)
                            : accounts.get(1);

            AccountEntity receiver =
                    accounts.get(0).getUserId()
                            .equals(event.getReceiverId())
                            ? accounts.get(0)
                            : accounts.get(1);

            /*
             * Balance validation
             */
            if (
                    sender.getBalance()
                            .compareTo(event.getAmount()) < 0
            ) {

                insufficientFundsCounter.increment();

                paymentFailedCounter.increment();

                throw new InsufficientFundsException(
                        "Insufficient balance"
                );
            }

            /*
             * Atomic balance transfer
             */
            sender.setBalance(
                    sender.getBalance()
                            .subtract(event.getAmount())
            );

            receiver.setBalance(
                    receiver.getBalance()
                            .add(event.getAmount())
            );

            LocalDateTime now =
                    LocalDateTime.now();

            sender.setUpdatedAt(now);

            receiver.setUpdatedAt(now);

            accountRepository.save(sender);

            accountRepository.save(receiver);

            /*
             * Mark Kafka event as processed
             */
            processedEventRepository.save(
                    ProcessedEventEntity.builder()
                            .eventId(
                                    event.getTransactionId()
                            )
                            .processedAt(now)
                            .build()
            );

            paymentCompletedCounter.increment();

            log.info(
                    """
                    Payment processed successfully
                    transactionId={}
                    senderId={}
                    receiverId={}
                    amount={}
                    """,
                    event.getTransactionId(),
                    event.getSenderId(),
                    event.getReceiverId(),
                    event.getAmount()
            );

        } catch (Exception ex) {

            paymentFailedCounter.increment();

            log.error(
                    "Payment processing failed transactionId={}",
                    event.getTransactionId(),
                    ex
            );

            throw ex;

        } finally {

            sample.stop(
                    paymentProcessingTimer
            );
        }
    }
}