package com.swiftpay.gateway.consumer;

import com.swiftpay.gateway.event.PaymentCompletedEvent;
import com.swiftpay.gateway.event.PaymentFailedEvent;
import com.swiftpay.gateway.service.TransactionStatusUpdaterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentWorkflowConsumer {

    private final TransactionStatusUpdaterService
            transactionStatusUpdaterService;

    @KafkaListener(
            topics = "payments.completed",
            groupId = "transaction-gateway-group-v1",
            containerFactory =
                    "paymentCompletedKafkaListenerFactory"
    )
    public void consumeCompleted(
            PaymentCompletedEvent event,
            Acknowledgment acknowledgment
    ) {

        log.info(
                "Received payment completed transactionId={}",
                event.getTransactionId()
        );

        transactionStatusUpdaterService.markCompleted(
                event.getTransactionId()
        );

        acknowledgment.acknowledge();
    }

    @KafkaListener(
            topics = "payments.failed",
            groupId = "transaction-gateway-group-v1",
            containerFactory =
                    "paymentFailedKafkaListenerFactory"
    )
    public void consumeFailed(
            PaymentFailedEvent event,
            Acknowledgment acknowledgment
    ) {

        log.info(
                "Received payment failed transactionId={}",
                event.getTransactionId()
        );

        transactionStatusUpdaterService.markFailed(
                event.getTransactionId()
        );

        acknowledgment.acknowledge();
    }
}