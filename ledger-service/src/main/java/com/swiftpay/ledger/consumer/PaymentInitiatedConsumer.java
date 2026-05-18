package com.swiftpay.ledger.consumer;

import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import com.swiftpay.ledger.service.LedgerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentInitiatedConsumer {

    private final LedgerService ledgerService;

    @KafkaListener(
            topics = "payments.initiated",
            groupId = "ledger-service-group",
            containerFactory =
                    "kafkaListenerContainerFactory"
    )
    public void consumePaymentInitiated(
            PaymentInitiatedEvent event,
            Acknowledgment acknowledgment
    ) {

        log.info(
                "Received PaymentInitiated event transactionId={}",
                event.getTransactionId()
        );

        try {

            /*
             * Process transactional ledger update
             */
            ledgerService.processPayment(event);

            /*
             * ACK ONLY AFTER successful DB commit
             */
            acknowledgment.acknowledge();

            log.info(
                    "Kafka offset acknowledged transactionId={}",
                    event.getTransactionId()
            );

        } catch (Exception ex) {

            log.error(
                    "Payment processing failed transactionId={}",
                    event.getTransactionId(),
                    ex
            );

            /*
             * NO ACK
             *
             * Kafka will retry delivery
             */
        }
    }
}