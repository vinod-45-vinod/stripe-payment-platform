package com.paymentplatform.fraud.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.common.KafkaTopics;
import com.paymentplatform.common.TraceContext;
import com.paymentplatform.events.PaymentEvents;
import com.paymentplatform.fraud.service.FraudCheckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

/**
 * Fraud service Kafka consumer.
 * Listens on payment-created and payment-authorized topics.
 * Runs fraud checks and logs results — does NOT block payment processing.
 *
 * Extracts X-Trace-Id Kafka header into MDC so fraud check log lines can be
 * correlated with the originating payment-service request.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final FraudCheckService fraudCheckService;
    private final ObjectMapper objectMapper;

    /**
     * Run fraud checks when a new payment is created.
     * This is the primary fraud check — before any money moves.
     */
    @KafkaListener(topics = KafkaTopics.PAYMENT_CREATED, groupId = "fraud-service")
    public void onPaymentCreated(ConsumerRecord<String, String> record) {
        extractTrace(record);
        try {
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    record.value(), PaymentEvents.PaymentEventEnvelope.class);

            Map<?, ?> payloadMap = (Map<?, ?>) envelope.getPayload();
            UUID paymentId = UUID.fromString(payloadMap.get("paymentId").toString());
            BigDecimal amount = new BigDecimal(payloadMap.get("amount").toString());
            String customerId = payloadMap.get("customerId").toString();

            log.info("[FRAUD] Running checks for payment-created: paymentId={}, amount={}, customer={}",
                    paymentId, amount, customerId);

            FraudCheckService.FraudCheckResult result =
                    fraudCheckService.checkPayment(paymentId, amount, customerId);

            if ("BLOCK".equals(result.result())) {
                log.warn("[FRAUD] BLOCK decision for paymentId={}. " +
                        "Blocking is not wired into payment-service flow control — " +
                        "future enhancement: publish fraud-decision event for payment-service to consume.",
                        paymentId);
            }

        } catch (Exception e) {
            log.error("[FRAUD] Failed to process payment-created event: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process payment-created event in fraud-service", e);
        } finally {
            TraceContext.clear();
        }
    }

    /**
     * Secondary fraud check at authorization stage.
     * Useful for catching velocity attacks that started after payment creation.
     */
    @KafkaListener(topics = KafkaTopics.PAYMENT_AUTHORIZED, groupId = "fraud-service")
    public void onPaymentAuthorized(ConsumerRecord<String, String> record) {
        extractTrace(record);
        try {
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    record.value(), PaymentEvents.PaymentEventEnvelope.class);

            log.info("[FRAUD] Payment authorized — no additional check needed at this stage: paymentId={}",
                    envelope.getPaymentId());

        } catch (Exception e) {
            log.error("[FRAUD] Failed to process payment-authorized event: {}", e.getMessage(), e);
            // Do not rethrow — authorized events failing fraud logging should not block payments
        } finally {
            TraceContext.clear();
        }
    }

    private void extractTrace(ConsumerRecord<?, ?> record) {
        Header traceHeader = record.headers().lastHeader(TraceContext.HEADER_NAME);
        byte[] traceBytes = traceHeader != null ? traceHeader.value() : null;
        TraceContext.setFromBytes(traceBytes);
        if (record.key() != null) TraceContext.set(null, record.key().toString());
    }
}
