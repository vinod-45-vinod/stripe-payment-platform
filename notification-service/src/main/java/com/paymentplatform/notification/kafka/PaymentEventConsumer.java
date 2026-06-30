package com.paymentplatform.notification.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.common.KafkaTopics;
import com.paymentplatform.common.TraceContext;
import com.paymentplatform.events.PaymentEvents;
import com.paymentplatform.notification.entity.Merchant;
import com.paymentplatform.notification.entity.WebhookEvent;
import com.paymentplatform.notification.repository.MerchantRepository;
import com.paymentplatform.notification.repository.WebhookEventRepository;
import com.paymentplatform.notification.service.WebhookDeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Notification service Kafka consumer.
 *
 * For each relevant event:
 *  1. Parses the event envelope
 *  2. Looks up the merchant's registered webhook URL
 *  3. Persists a webhook_events row with status=PENDING
 *  4. Attempts immediate HTTP delivery (success → DELIVERED; failure → retry scheduled)
 *
 * IMPORTANT: This service must NEVER affect payment-service.
 * DLQ is handled by the KafkaConsumerConfig error handler — 3 retries then DLQ topic.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final ObjectMapper objectMapper;
    private final MerchantRepository merchantRepository;
    private final WebhookEventRepository webhookEventRepository;
    private final WebhookDeliveryService webhookDeliveryService;

    // ── payment.captured ─────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaTopics.PAYMENT_CAPTURED, groupId = "notification-service")
    @Transactional
    public void onPaymentCaptured(ConsumerRecord<String, String> record) {
        extractTrace(record);
        try {
            String message = record.value();
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    message, PaymentEvents.PaymentEventEnvelope.class);

            Map<?, ?> payloadMap = (Map<?, ?>) envelope.getPayload();
            UUID paymentId = UUID.fromString(payloadMap.get("paymentId").toString());
            String merchantId = payloadMap.get("merchantId").toString();
            String amount = payloadMap.get("amount").toString();
            String currency = payloadMap.get("currency").toString();

            log.info("[NOTIFICATION] payment.captured received | paymentId={} merchantId={}", paymentId, merchantId);

            // Log email stub
            log.info("[NOTIFICATION-EMAIL] Would notify customer: payment {} captured ({} {})",
                    paymentId, amount, currency);

            // Deliver webhook to merchant
            deliverWebhook(paymentId, merchantId, "payment.captured", buildPayload(paymentId, "payment.captured",
                    Map.of("amount", amount, "currency", currency, "merchantId", merchantId)));

        } catch (Exception e) {
            log.error("[NOTIFICATION] Error processing payment-captured: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process payment-captured event", e);
        } finally {
            TraceContext.clear();
        }
    }

    // ── refund.created ────────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaTopics.REFUND_CREATED, groupId = "notification-service")
    @Transactional
    public void onRefundCreated(ConsumerRecord<String, String> record) {
        extractTrace(record);
        try {
            String message = record.value();
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    message, PaymentEvents.PaymentEventEnvelope.class);

            Map<?, ?> payloadMap = (Map<?, ?>) envelope.getPayload();
            UUID paymentId = UUID.fromString(payloadMap.get("paymentId").toString());
            Object merchantIdObj = payloadMap.get("merchantId");
            String merchantId = merchantIdObj != null ? merchantIdObj.toString() : "unknown";

            String amount = payloadMap.get("amount").toString();
            String currency = payloadMap.get("currency").toString();

            log.info("[NOTIFICATION] refund.created received | paymentId={} amount={}", paymentId, amount);

            log.info("[NOTIFICATION-EMAIL] Would notify customer: refund {} {} for payment {}",
                    amount, currency, paymentId);

            deliverWebhook(paymentId, merchantId, "refund.created", buildPayload(paymentId, "refund.created",
                    Map.of("amount", amount, "currency", currency)));

        } catch (Exception e) {
            log.error("[NOTIFICATION] Error processing refund-created: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process refund-created event", e);
        } finally {
            TraceContext.clear();
        }
    }

    // ── payment.created (email only — no webhook for PENDING payment) ─────────

    @KafkaListener(topics = KafkaTopics.PAYMENT_CREATED, groupId = "notification-service")
    public void onPaymentCreated(ConsumerRecord<String, String> record) {
        extractTrace(record);
        try {
            String message = record.value();
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    message, PaymentEvents.PaymentEventEnvelope.class);

            Map<?, ?> payloadMap = (Map<?, ?>) envelope.getPayload();
            UUID paymentId = UUID.fromString(payloadMap.get("paymentId").toString());
            String customerId = payloadMap.get("customerId").toString();

            log.info("[NOTIFICATION-EMAIL] Would send confirmation email to customer {}: payment {} created",
                    customerId, paymentId);

        } catch (Exception e) {
            // payment-created: log and swallow — email stubs don't warrant DLQ
            log.error("[NOTIFICATION] Error processing payment-created: {}", e.getMessage(), e);
        } finally {
            TraceContext.clear();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Persists a WebhookEvent row and attempts immediate HTTP delivery.
     * If delivery fails, the row is left in FAILED state with next_retry_at set.
     * The WebhookRetryScheduler will pick it up on the next scheduled run.
     */
    private void deliverWebhook(UUID paymentId, String merchantId, String eventType, String payload) {
        Optional<Merchant> merchantOpt = merchantRepository.findByIdAndActiveTrue(merchantId);

        if (merchantOpt.isEmpty()) {
            log.debug("[NOTIFICATION] No merchant found for id={} — skipping webhook", merchantId);
            return;
        }

        Merchant merchant = merchantOpt.get();
        if (merchant.getWebhookUrl() == null || merchant.getWebhookUrl().isBlank()) {
            log.debug("[NOTIFICATION] Merchant {} has no webhook URL registered", merchantId);
            return;
        }

        WebhookEvent event = WebhookEvent.builder()
                .paymentId(paymentId)
                .merchantId(merchantId)
                .merchantUrl(merchant.getWebhookUrl())
                .eventType(eventType)
                .payload(payload)
                .build();

        // Save PENDING row first (so the attempt is tracked even if delivery call crashes)
        WebhookEvent saved = webhookEventRepository.save(event);

        // Attempt immediate delivery
        webhookDeliveryService.attemptDelivery(saved);
    }

    private String buildPayload(UUID paymentId, String eventType, Map<String, String> extra) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"eventType\":\"").append(eventType).append("\"");
        sb.append(",\"paymentId\":\"").append(paymentId).append("\"");
        extra.forEach((k, v) -> sb.append(",\"").append(k).append("\":\"").append(v).append("\""));
        sb.append("}");
        return sb.toString();
    }

    private void extractTrace(ConsumerRecord<?, ?> record) {
        Header traceHeader = record.headers().lastHeader(TraceContext.HEADER_NAME);
        byte[] traceBytes = traceHeader != null ? traceHeader.value() : null;
        TraceContext.setFromBytes(traceBytes);
        if (record.key() != null) TraceContext.set(null, record.key().toString());
    }
}
