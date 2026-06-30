package com.paymentplatform.service;

import com.paymentplatform.common.KafkaTopics;
import com.paymentplatform.common.TraceContext;
import com.paymentplatform.entity.OutboxEvent;
import com.paymentplatform.events.OutboxEventType;
import com.paymentplatform.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Outbox publisher — polls unpublished outbox rows and forwards them to Kafka.
 *
 * Runs every 5 seconds. Reads up to 100 unpublished events, oldest-first.
 * For each event:
 *  1. Generates a traceId and injects it into the Kafka message header (X-Trace-Id).
 *  2. Publishes the JSON payload to the appropriate Kafka topic.
 *  3. Marks the row published=true within the same DB transaction.
 *
 * If Kafka is temporarily down, {@link KafkaTemplate#send} throws an exception.
 * The row remains published=false and will be retried on the next scheduler run —
 * no event is ever silently lost.
 *
 * Failure modes handled:
 * - Kafka down: loop catches exception per-event, logs it, continues to next event.
 *   Avoids one bad event blocking the entire batch.
 * - DB failure on published=true update: the event will be re-published on the next
 *   run (at-least-once delivery). Consumers MUST be idempotent.
 *
 * Topic names sourced from KafkaTopics constants in common-lib.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisherJob {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Poll every 5 seconds. Fixed-delay (not fixed-rate) so concurrent runs
     * cannot pile up if a run takes longer than the interval.
     */
    @Scheduled(fixedDelayString = "${outbox.publisher.delay-ms:5000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findTop100ByPublishedFalseOrderByCreatedAtAsc();

        if (pending.isEmpty()) {
            return; // Nothing to do — no log noise when idle
        }

        log.debug("Outbox publisher: processing {} pending event(s)", pending.size());
        int published = 0;
        int failed = 0;

        for (OutboxEvent event : pending) {
            String traceId = TraceContext.newTraceId();
            TraceContext.set(traceId, event.getPaymentId().toString());
            try {
                String topic = topicFor(event.getEventType());
                // paymentId as Kafka key ensures all events for the same payment go to
                // the same partition, preserving per-payment ordering.
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        topic, event.getPaymentId().toString(), event.getPayload());

                // Inject traceId into Kafka header so consumers can populate MDC
                record.headers().add(TraceContext.HEADER_NAME, traceId.getBytes(StandardCharsets.UTF_8));

                kafkaTemplate.send(record).get();
                event.setPublished(true);
                outboxEventRepository.save(event);
                published++;
                log.debug("Published outbox event: id={}, type={}, topic={}, traceId={}",
                        event.getId(), event.getEventType(), topic, traceId);
            } catch (Exception e) {
                failed++;
                log.warn("Failed to publish outbox event id={}, type={}: {}",
                        event.getId(), event.getEventType(), e.getMessage());
                // Do NOT rethrow — continue processing remaining events.
                // This event will be retried on the next scheduler run.
            } finally {
                TraceContext.clear();
            }
        }

        if (published > 0 || failed > 0) {
            log.info("Outbox publisher run complete: published={}, failed={}", published, failed);
        }
    }

    private String topicFor(OutboxEventType eventType) {
        return switch (eventType) {
            case PAYMENT_CREATED    -> KafkaTopics.PAYMENT_CREATED;
            case PAYMENT_AUTHORIZED -> KafkaTopics.PAYMENT_AUTHORIZED;
            case PAYMENT_CAPTURED   -> KafkaTopics.PAYMENT_CAPTURED;
            case REFUND_CREATED     -> KafkaTopics.REFUND_CREATED;
        };
    }
}
