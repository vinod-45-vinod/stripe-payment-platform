package com.paymentplatform.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.events.OutboxEventType;
import com.paymentplatform.events.PaymentEvents;
import com.paymentplatform.entity.OutboxEvent;
import com.paymentplatform.entity.Payment;
import com.paymentplatform.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Writes outbox_events rows in the SAME @Transactional scope as the payment
 * state change. Never publishes to Kafka directly — that is the
 * {@link OutboxPublisherJob}'s responsibility.
 *
 * Pattern: if the DB commit succeeds, the event row is guaranteed to exist.
 * If Kafka is down, the event will sit in the table and be retried on the
 * next scheduler run. If the DB commit fails, neither the payment change nor
 * the event row is persisted — perfect atomicity.
 *
 * Event types and DTOs are imported from the shared-events module.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /** Called from PaymentService.createPayment() within the same transaction. */
    public void recordPaymentCreated(Payment payment) {
        PaymentEvents.PaymentCreatedPayload payload = PaymentEvents.PaymentCreatedPayload.builder()
                .paymentId(payment.getId())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .customerId(payment.getCustomerId())
                .merchantId(payment.getMerchantId())
                .createdAt(payment.getCreatedAt())
                .build();

        save(OutboxEventType.PAYMENT_CREATED, payment.getId(), payload);
    }

    /** Called from PaymentService.authorizePayment() within the same transaction. */
    public void recordPaymentAuthorized(Payment payment) {
        PaymentEvents.PaymentAuthorizedPayload payload = PaymentEvents.PaymentAuthorizedPayload.builder()
                .paymentId(payment.getId())
                .authorizationCode(payment.getAuthorizationCode())
                .authorizedAt(LocalDateTime.now())
                .build();

        save(OutboxEventType.PAYMENT_AUTHORIZED, payment.getId(), payload);
    }

    /** Called from PaymentService.capturePayment() within the same transaction. */
    public void recordPaymentCaptured(Payment payment) {
        PaymentEvents.PaymentCapturedPayload payload = PaymentEvents.PaymentCapturedPayload.builder()
                .paymentId(payment.getId())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .customerId(payment.getCustomerId())
                .merchantId(payment.getMerchantId())
                .capturedAt(LocalDateTime.now())
                .build();

        save(OutboxEventType.PAYMENT_CAPTURED, payment.getId(), payload);
    }

    /** Called from PaymentService.refundPayment() within the same transaction. */
    public void recordRefundCreated(Payment payment) {
        PaymentEvents.RefundCreatedPayload payload = PaymentEvents.RefundCreatedPayload.builder()
                .paymentId(payment.getId())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .refundedAt(LocalDateTime.now())
                .build();

        save(OutboxEventType.REFUND_CREATED, payment.getId(), payload);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void save(OutboxEventType eventType, UUID paymentId, Object payload) {
        try {
            PaymentEvents.PaymentEventEnvelope envelope = PaymentEvents.PaymentEventEnvelope.builder()
                    .eventId(UUID.randomUUID())
                    .eventType(eventType)
                    .paymentId(paymentId)
                    .occurredAt(LocalDateTime.now())
                    .payload(payload)
                    .build();

            String json = objectMapper.writeValueAsString(envelope);

            OutboxEvent event = OutboxEvent.builder()
                    .paymentId(paymentId)
                    .eventType(eventType)
                    .payload(json)
                    .build();

            outboxEventRepository.save(event);
            log.debug("Outbox event recorded: type={}, paymentId={}", eventType, paymentId);

        } catch (Exception e) {
            // If serialisation fails here the transaction will roll back, which is correct.
            throw new RuntimeException("Failed to serialise outbox event payload", e);
        }
    }
}
