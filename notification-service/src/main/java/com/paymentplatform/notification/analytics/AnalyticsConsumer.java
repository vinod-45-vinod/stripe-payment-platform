package com.paymentplatform.notification.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.common.KafkaTopics;
import com.paymentplatform.events.PaymentEvents;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

/**
 * Analytics Kafka consumer — observes all payment lifecycle events and feeds
 * the {@link PaymentAnalyticsStore} to maintain rolling metrics.
 *
 * Runs in the same notification-service process, separate consumer group
 * so it receives every event independently of the webhook consumer.
 *
 * Failure in this consumer must never affect webhook delivery — all exceptions
 * are caught and swallowed here.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AnalyticsConsumer {

    private final PaymentAnalyticsStore analyticsStore;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.PAYMENT_CREATED, groupId = "analytics-service")
    public void onPaymentCreated(String message) {
        try {
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    message, PaymentEvents.PaymentEventEnvelope.class);
            Map<?, ?> payload = (Map<?, ?>) envelope.getPayload();
            String paymentId = payload.get("paymentId").toString();
            String merchantId = safeStr(payload, "merchantId", "unknown");
            BigDecimal amount = safeBigDecimal(payload, "amount");
            analyticsStore.recordCreated(paymentId, merchantId, amount);
            log.debug("[ANALYTICS] recorded payment.created paymentId={} merchant={} amount={}", paymentId, merchantId,
                    amount);
        } catch (Exception e) {
            log.warn("[ANALYTICS] Failed to process payment.created: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_AUTHORIZED, groupId = "analytics-service")
    public void onPaymentAuthorized(String message) {
        try {
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    message, PaymentEvents.PaymentEventEnvelope.class);
            String paymentId = envelope.getPaymentId() != null
                    ? envelope.getPaymentId().toString()
                    : ((Map<?, ?>) envelope.getPayload()).get("paymentId").toString();
            analyticsStore.recordAuthorized(paymentId);
            log.debug("[ANALYTICS] recorded payment.authorized paymentId={}", paymentId);
        } catch (Exception e) {
            log.warn("[ANALYTICS] Failed to process payment.authorized: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_CAPTURED, groupId = "analytics-service")
    public void onPaymentCaptured(String message) {
        try {
            analyticsStore.recordCaptured();
            log.debug("[ANALYTICS] recorded payment.captured");
        } catch (Exception e) {
            log.warn("[ANALYTICS] Failed to process payment.captured: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaTopics.REFUND_CREATED, groupId = "analytics-service")
    public void onRefundCreated(String message) {
        try {
            analyticsStore.recordRefunded();
            log.debug("[ANALYTICS] recorded refund.created");
        } catch (Exception e) {
            log.warn("[ANALYTICS] Failed to process refund.created: {}", e.getMessage());
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private String safeStr(Map<?, ?> map, String key, String fallback) {
        Object val = map.get(key);
        return val != null ? val.toString() : fallback;
    }

    private BigDecimal safeBigDecimal(Map<?, ?> map, String key) {
        try {
            Object val = map.get(key);
            return val != null ? new BigDecimal(val.toString()) : BigDecimal.ZERO;
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }
}
