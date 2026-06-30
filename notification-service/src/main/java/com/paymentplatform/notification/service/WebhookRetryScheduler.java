package com.paymentplatform.notification.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled job that polls the webhook_events table for PENDING/FAILED events
 * whose next_retry_at is in the past, and attempts delivery.
 *
 * Interval: configurable via notification.webhook.scheduler-interval-ms (default 30s).
 * This is the same pattern as the OutboxPublisherJob in payment-service.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class WebhookRetryScheduler {

    private final WebhookDeliveryService webhookDeliveryService;

    /**
     * Run every 30 seconds (configurable).
     * Picks up any due retries — including first-attempt webhooks that failed
     * the immediate delivery in the Kafka consumer.
     */
    @Scheduled(fixedDelayString = "${notification.webhook.scheduler-interval-ms:30000}")
    public void processRetries() {
        log.debug("[WEBHOOK-SCHEDULER] Polling for due webhook retries");
        try {
            webhookDeliveryService.processDueWebhooks();
        } catch (Exception e) {
            // Scheduler must never crash — log and continue
            log.error("[WEBHOOK-SCHEDULER] Unexpected error during webhook retry poll: {}", e.getMessage(), e);
        }
    }
}
