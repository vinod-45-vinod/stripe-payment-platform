package com.paymentplatform.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Shared event DTOs for payment lifecycle Kafka messages.
 *
 * All services (payment-service, ledger-service, notification-service, fraud-service)
 * depend on this module so they share an identical event schema.
 *
 * Every Kafka message is a JSON-serialised {@link PaymentEventEnvelope}. The
 * {@code eventType} field tells consumers how to interpret the {@code payload}.
 *
 * Shared event DTOs extracted into this standalone shared-events module
 * so downstream consumers don't depend on the full payment-service codebase.
 */
public final class PaymentEvents {

    private PaymentEvents() {}

    // -------------------------------------------------------------------------
    // Envelope — wraps every event written to Kafka
    // -------------------------------------------------------------------------

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PaymentEventEnvelope {
        /** Unique event ID — used for downstream idempotency checks. */
        private UUID eventId;
        /** The event type — determines which topic and how to parse {@code payload}. */
        private OutboxEventType eventType;
        /** The payment this event relates to. */
        private UUID paymentId;
        /** ISO-8601 timestamp of when this event was emitted. */
        private LocalDateTime occurredAt;
        /**
         * The typed payload — serialised as a nested JSON object inside the envelope.
         * Consumers cast this based on eventType.
         */
        private Object payload;
    }

    // -------------------------------------------------------------------------
    // Typed payloads
    // -------------------------------------------------------------------------

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PaymentCreatedPayload {
        private UUID paymentId;
        private BigDecimal amount;
        private String currency;
        private String customerId;
        private String merchantId;
        private LocalDateTime createdAt;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PaymentAuthorizedPayload {
        private UUID paymentId;
        private String authorizationCode;
        private LocalDateTime authorizedAt;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PaymentCapturedPayload {
        private UUID paymentId;
        private BigDecimal amount;
        private String currency;
        private String customerId;   // needed by ledger-service to debit the customer account
        private String merchantId;
        private LocalDateTime capturedAt;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RefundCreatedPayload {
        private UUID paymentId;
        private BigDecimal amount;
        private String currency;
        private LocalDateTime refundedAt;
    }
}
