package com.paymentplatform.events;

/**
 * Kafka event type identifiers shared across all services.
 * Used in the PaymentEventEnvelope so consumers know which payload type to deserialize.
 */
public enum OutboxEventType {
    PAYMENT_CREATED,
    PAYMENT_AUTHORIZED,
    PAYMENT_CAPTURED,
    REFUND_CREATED
}
