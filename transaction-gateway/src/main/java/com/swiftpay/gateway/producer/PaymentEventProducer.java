package com.swiftpay.gateway.producer;

import com.swiftpay.gateway.event.KafkaTopics;
import com.swiftpay.gateway.event.PaymentInitiatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishPaymentInitiated(
            PaymentInitiatedEvent event
    ) {

        String transactionId =
                event.getTransactionId().toString();

        kafkaTemplate.send(
                KafkaTopics.PAYMENTS_INITIATED,
                transactionId,
                event
        );

        log.info(
                "Published PaymentInitiated event for transactionId={}",
                transactionId
        );
    }
}