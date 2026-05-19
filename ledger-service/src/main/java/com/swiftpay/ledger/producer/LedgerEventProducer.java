package com.swiftpay.ledger.producer;

import com.swiftpay.ledger.event.PaymentCompletedEvent;
import com.swiftpay.ledger.event.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LedgerEventProducer {

    private final KafkaTemplate<String, Object>
            kafkaTemplate;

    public void publishPaymentCompleted(
            PaymentCompletedEvent event
    ) {

        kafkaTemplate.send(
                "payments.completed",
                event.getTransactionId().toString(),
                event
        );

        log.info(
                "Published PaymentCompletedEvent transactionId={}",
                event.getTransactionId()
        );
    }

    public void publishPaymentFailed(
            PaymentFailedEvent event
    ) {

        kafkaTemplate.send(
                "payments.failed",
                event.getTransactionId().toString(),
                event
        );

        log.info(
                "Published PaymentFailedEvent transactionId={}",
                event.getTransactionId()
        );
    }
}