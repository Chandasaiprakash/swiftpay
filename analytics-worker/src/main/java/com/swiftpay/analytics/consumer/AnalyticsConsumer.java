package com.swiftpay.analytics.consumer;

import com.swiftpay.analytics.event.PaymentCompletedEvent;
import com.swiftpay.analytics.event.PaymentFailedEvent;
import com.swiftpay.analytics.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyticsConsumer {

    private final AnalyticsService
            analyticsService;

    @KafkaListener(
            topics = "payments.completed",
            groupId = "analytics-worker-group",
            containerFactory =
                    "paymentCompletedKafkaListenerFactory"
    )
    public void consumeCompleted(
            PaymentCompletedEvent event,
            Acknowledgment acknowledgment
    ) {

        log.info(
                "Analytics processing completed payment transactionId={}",
                event.getTransactionId()
        );

        analyticsService.incrementCompletedPayments(
                event.getCurrency()
        );

        analyticsService.addPaymentVolume(
                event.getAmount(),
                event.getCurrency()
        );

        acknowledgment.acknowledge();
    }

    @KafkaListener(
            topics = "payments.failed",
            groupId = "analytics-worker-group",
            containerFactory =
                    "paymentFailedKafkaListenerFactory"
    )
    public void consumeFailed(
            PaymentFailedEvent event,
            Acknowledgment acknowledgment
    ) {

        log.info(
                "Analytics processing failed payment transactionId={}",
                event.getTransactionId()
        );

        analyticsService.incrementFailedPayments(
                "USD"
        );

        acknowledgment.acknowledge();
    }
}