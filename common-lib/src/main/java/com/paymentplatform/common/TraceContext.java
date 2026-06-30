package com.paymentplatform.common;

import org.slf4j.MDC;

import java.util.UUID;

/**
 * Trace context utilities for structured logging across microservices.
 *
 * A traceId follows a payment's journey across all services via Kafka message headers.
 * Each service extracts the traceId from the incoming Kafka record header and places
 * it in SLF4J's MDC (Mapped Diagnostic Context) so every log line within that
 * processing context automatically includes [traceId=xxx].
 *
 * Usage (producer side — OutboxPublisherJob):
 *   ProducerRecord headers get TraceContext.HEADER_NAME → traceId bytes
 *
 * Usage (consumer side — each PaymentEventConsumer):
 *   TraceContext.setFromHeader(record.headers().lastHeader(TraceContext.HEADER_NAME));
 *   try { ... process ... } finally { TraceContext.clear(); }
 */
public final class TraceContext {

    public static final String HEADER_NAME = "X-Trace-Id";
    public static final String MDC_KEY     = "traceId";
    public static final String PAYMENT_MDC = "paymentId";

    private TraceContext() {}

    /**
     * Generate a new traceId (UUID, no dashes for log compactness).
     */
    public static String newTraceId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Set the traceId in MDC from a raw byte array (Kafka header value).
     * Falls back to a generated ID if the header is missing or malformed.
     */
    public static void setFromBytes(byte[] headerValue) {
        String traceId = (headerValue != null && headerValue.length > 0)
                ? new String(headerValue)
                : newTraceId();
        MDC.put(MDC_KEY, traceId);
    }

    /**
     * Set both traceId and paymentId in MDC.
     */
    public static void set(String traceId, String paymentId) {
        if (traceId != null) MDC.put(MDC_KEY, traceId);
        if (paymentId != null) MDC.put(PAYMENT_MDC, paymentId);
    }

    /**
     * Clear all trace keys from MDC. Always call in a finally block.
     */
    public static void clear() {
        MDC.remove(MDC_KEY);
        MDC.remove(PAYMENT_MDC);
    }

    /** Get current traceId from MDC, or generate one. */
    public static String current() {
        String traceId = MDC.get(MDC_KEY);
        if (traceId == null || traceId.isBlank()) {
            traceId = newTraceId();
            MDC.put(MDC_KEY, traceId);
        }
        return traceId;
    }
}
