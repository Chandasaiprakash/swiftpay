package com.swiftpay.ledger.service;

import com.swiftpay.ledger.entity.AccountEntity;
import com.swiftpay.ledger.entity.ProcessedEventEntity;
import com.swiftpay.ledger.event.PaymentCompletedEvent;
import com.swiftpay.ledger.event.PaymentFailedEvent;
import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import com.swiftpay.ledger.exception.AccountNotFoundException;
import com.swiftpay.ledger.exception.InsufficientFundsException;
import com.swiftpay.ledger.producer.LedgerEventProducer;
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

    private final ProcessedEventRepository
            processedEventRepository;

    private final LedgerEventProducer
            ledgerEventProducer;

    private final Counter paymentCompletedCounter;

    private final Counter paymentFailedCounter;

    private final Counter duplicateEventCounter;

    private final Counter insufficientFundsCounter;

    private final Timer paymentProcessingTimer;

    public LedgerService(
            AccountRepository accountRepository,
            ProcessedEventRepository processedEventRepository,
            LedgerEventProducer ledgerEventProducer,
            Counter paymentCompletedCounter,
            Counter paymentFailedCounter,
            Counter duplicateEventCounter,
            Counter insufficientFundsCounter,
            Timer paymentProcessingTimer
    ) {

        this.accountRepository = accountRepository;

        this.processedEventRepository =
                processedEventRepository;

        this.ledgerEventProducer =
                ledgerEventProducer;

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
             * Pessimistic locking
             * Ordered locking reduces deadlocks
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

                ledgerEventProducer.publishPaymentFailed(
                        PaymentFailedEvent.builder()
                                .transactionId(
                                        event.getTransactionId()
                                )
                                .reason(
                                        "Accounts not found"
                                )
                                .failedAt(
                                        LocalDateTime.now()
                                )
                                .build()
                );

                throw new AccountNotFoundException(
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
                            .compareTo(
                                    event.getAmount()
                            ) < 0
            ) {

                paymentFailedCounter.increment();

                insufficientFundsCounter.increment();

                ledgerEventProducer.publishPaymentFailed(
                        PaymentFailedEvent.builder()
                                .transactionId(
                                        event.getTransactionId()
                                )
                                .reason(
                                        "Insufficient funds"
                                )
                                .failedAt(
                                        LocalDateTime.now()
                                )
                                .build()
                );

                throw new InsufficientFundsException(
                        "Insufficient balance"
                );
            }

            /*
             * Atomic balance transfer
             */
            sender.setBalance(
                    sender.getBalance()
                            .subtract(
                                    event.getAmount()
                            )
            );

            receiver.setBalance(
                    receiver.getBalance()
                            .add(
                                    event.getAmount()
                            )
            );

            sender.setUpdatedAt(
                    LocalDateTime.now()
            );

            receiver.setUpdatedAt(
                    LocalDateTime.now()
            );

            accountRepository.save(sender);

            accountRepository.save(receiver);

            /*
             * Mark event processed
             */
            processedEventRepository.save(
                    ProcessedEventEntity.builder()
                            .eventId(
                                    event.getTransactionId()
                            )
                            .processedAt(
                                    LocalDateTime.now()
                            )
                            .build()
            );

            /*
             * Publish success event
             */
            ledgerEventProducer.publishPaymentCompleted(
                    PaymentCompletedEvent.builder()
                            .transactionId(
                                    event.getTransactionId()
                            )
                            .senderId(
                                    event.getSenderId()
                            )
                            .receiverId(
                                    event.getReceiverId()
                            )
                            .amount(
                                    event.getAmount()
                            )
                            .currency(
                                    event.getCurrency()
                            )
                            .completedAt(
                                    LocalDateTime.now()
                            )
                            .build()
            );

            paymentCompletedCounter.increment();

            log.info(
                    "Payment processed successfully transactionId={}",
                    event.getTransactionId()
            );

        } finally {

            sample.stop(
                    paymentProcessingTimer
            );
        }
    }
}