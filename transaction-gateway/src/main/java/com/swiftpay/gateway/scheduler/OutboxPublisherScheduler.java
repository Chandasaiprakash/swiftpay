package com.swiftpay.gateway.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swiftpay.gateway.entity.OutboxEventEntity;
import com.swiftpay.gateway.entity.OutboxStatus;
import com.swiftpay.gateway.event.PaymentInitiatedEvent;
import com.swiftpay.gateway.producer.PaymentEventProducer;
import com.swiftpay.gateway.repository.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisherScheduler {

    private final OutboxRepository outboxRepository;
    private final PaymentEventProducer producer;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void publishPendingEvents() {

        Counter publishSuccessCounter =
                Counter.builder(
                                "swiftpay_outbox_publish_success_total"
                        )
                        .description(
                                "Successfully published outbox events"
                        )
                        .register(meterRegistry);

        Counter publishFailureCounter =
                Counter.builder(
                                "swiftpay_outbox_publish_failure_total"
                        )
                        .description(
                                "Failed outbox event publishes"
                        )
                        .register(meterRegistry);

        List<OutboxEventEntity> pendingEvents =
                outboxRepository
                        .findTop100ByStatusOrderByCreatedAtAsc(
                                OutboxStatus.PENDING
                        );

        if (pendingEvents.isEmpty()) {

            log.debug("No pending outbox events found");

            return;
        }

        log.info(
                "Publishing {} pending outbox events",
                pendingEvents.size()
        );

        for (OutboxEventEntity outbox : pendingEvents) {

            try {

                PaymentInitiatedEvent event =
                        objectMapper.readValue(
                                outbox.getPayload(),
                                PaymentInitiatedEvent.class
                        );

                producer.publishPaymentInitiated(event);

                outbox.setStatus(
                        OutboxStatus.PUBLISHED
                );

                outboxRepository.save(outbox);

                publishSuccessCounter.increment();

                log.info(
                        "Successfully published outbox eventId={}",
                        outbox.getId()
                );

            } catch (Exception ex) {

                publishFailureCounter.increment();

                log.error(
                        "Failed publishing outbox eventId={}",
                        outbox.getId(),
                        ex
                );
            }
        }
    }
}