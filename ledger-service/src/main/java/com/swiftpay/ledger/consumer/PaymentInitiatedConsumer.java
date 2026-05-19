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
                "Received payment event transactionId={}",
                event.getTransactionId()
        );

        /*
         * Let exceptions propagate
         * DefaultErrorHandler handles retry + DLT
         */
        ledgerService.processPayment(event);

        /*
         * ACK only after successful processing
         */
        acknowledgment.acknowledge();

        log.info(
                "Payment processed and acknowledged transactionId={}",
                event.getTransactionId()
        );
    }
}