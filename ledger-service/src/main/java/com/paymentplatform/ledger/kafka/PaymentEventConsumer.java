package com.paymentplatform.ledger.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.common.KafkaTopics;
import com.paymentplatform.common.TraceContext;
import com.paymentplatform.events.PaymentEvents;
import com.paymentplatform.ledger.service.LedgerService;
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
 * Consumes payment lifecycle events from Kafka and drives ledger bookkeeping.
 *
 * payment-captured  → recordCapture (debit customer, credit merchant + platform)
 * refund-created    → recordRefund  (reverse all entries)
 *
 * Each consumer method extracts the X-Trace-Id Kafka header and populates SLF4J MDC
 * so every log line within the processing context carries [traceId=xxx paymentId=yyy],
 * enabling a payment's full journey to be traced across all services.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final LedgerService ledgerService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.PAYMENT_CAPTURED, groupId = "ledger-service")
    public void onPaymentCaptured(ConsumerRecord<String, String> record) {
        extractTrace(record);
        try {
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    record.value(), PaymentEvents.PaymentEventEnvelope.class);

            Map<?, ?> payloadMap = (Map<?, ?>) envelope.getPayload();
            UUID paymentId = UUID.fromString(payloadMap.get("paymentId").toString());
            BigDecimal amount = new BigDecimal(payloadMap.get("amount").toString());
            String merchantId = payloadMap.get("merchantId").toString();
            String customerId = payloadMap.get("customerId").toString();

            log.info("[LEDGER] processing payment-captured: paymentId={} amount={} customer={} merchant={}",
                    paymentId, amount, customerId, merchantId);

            ledgerService.recordCapture(paymentId, amount, customerId, merchantId);

        } catch (Exception e) {
            log.error("[LEDGER] Failed to process payment-captured: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process payment-captured event", e);
        } finally {
            TraceContext.clear();
        }
    }

    @KafkaListener(topics = KafkaTopics.REFUND_CREATED, groupId = "ledger-service")
    public void onRefundCreated(ConsumerRecord<String, String> record) {
        extractTrace(record);
        try {
            PaymentEvents.PaymentEventEnvelope envelope = objectMapper.readValue(
                    record.value(), PaymentEvents.PaymentEventEnvelope.class);

            Map<?, ?> payloadMap = (Map<?, ?>) envelope.getPayload();
            UUID paymentId = UUID.fromString(payloadMap.get("paymentId").toString());
            BigDecimal amount = new BigDecimal(payloadMap.get("amount").toString());

            log.info("[LEDGER] processing refund-created: paymentId={} amount={}", paymentId, amount);

            ledgerService.recordRefund(paymentId, amount);

        } catch (Exception e) {
            log.error("[LEDGER] Failed to process refund-created: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process refund-created event", e);
        } finally {
            TraceContext.clear();
        }
    }

    private void extractTrace(ConsumerRecord<?, ?> record) {
        Header traceHeader = record.headers().lastHeader(TraceContext.HEADER_NAME);
        byte[] traceBytes = traceHeader != null ? traceHeader.value() : null;
        TraceContext.setFromBytes(traceBytes);
        if (record.key() != null) {
            TraceContext.set(null, record.key().toString());
        }
    }
}
