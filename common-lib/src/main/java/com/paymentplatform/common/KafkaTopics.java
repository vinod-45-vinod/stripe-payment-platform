package com.paymentplatform.common;

/**
 * Shared constants used across all microservices.
 * Kafka topic names are centralised here so they never drift between producer and consumers.
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    public static final String PAYMENT_CREATED    = "payment-created";
    public static final String PAYMENT_AUTHORIZED = "payment-authorized";
    public static final String PAYMENT_CAPTURED   = "payment-captured";
    public static final String REFUND_CREATED     = "refund-created";
}
