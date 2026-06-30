package com.paymentplatform.notification.service;

import com.paymentplatform.notification.entity.WebhookEvent;
import com.paymentplatform.notification.entity.WebhookEvent.WebhookStatus;
import com.paymentplatform.notification.repository.WebhookEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClient;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Handles actual HTTP delivery of webhook events and manages retry state.
 *
 * Retry schedule (configurable via notification.webhook.retry-delays-minutes):
 *   default: 1, 5, 15, 60 minutes
 *   After max retries exhausted → status = FAILED (permanently)
 *
 * Design decisions:
 * - Each delivery attempt is its own DB transaction (no long TX across HTTP call)
 * - RestClient (Spring Boot 4) used for synchronous HTTP POST — simple and observable
 * - On HTTP 4xx/5xx or timeout: retryCount++, nextRetryAt = now + delay[retryCount]
 * - On success: status = DELIVERED, nextRetryAt = null
 */
@Service
@Slf4j
public class WebhookDeliveryService {

    private final WebhookEventRepository webhookEventRepository;
    private final RestClient restClient;

    public WebhookDeliveryService(
            WebhookEventRepository webhookEventRepository,
            @Qualifier("webhookRestClient") RestClient restClient) {
        this.webhookEventRepository = webhookEventRepository;
        this.restClient = restClient;
    }

    /** Comma-separated retry delays in minutes (index = retry_count at time of failure) */
    @Value("${notification.webhook.retry-delays-minutes:1,5,15,60}")
    private String retryDelaysConfig;

    @Value("${notification.webhook.max-retries:4}")
    private int maxRetries;

    /**
     * Attempt delivery of a single webhook event.
     * Called by: (1) the Kafka consumer on first attempt, (2) the retry scheduler.
     *
     * Each invocation runs in its own transaction so HTTP latency doesn't hold a DB lock.
     */
    @Transactional
    public void attemptDelivery(WebhookEvent event) {
        log.info("[WEBHOOK] Attempting delivery to {} | paymentId={} eventType={} retryCount={}",
                event.getMerchantUrl(), event.getPaymentId(), event.getEventType(), event.getRetryCount());

        try {
            restClient.post()
                    .uri(event.getMerchantUrl())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(event.getPayload())
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, (req, resp) -> {
                        throw new WebhookDeliveryException(
                                "HTTP " + resp.getStatusCode() + " from merchant endpoint");
                    })
                    .toBodilessEntity();

            // Success path
            event.setStatus(WebhookStatus.DELIVERED);
            event.setNextRetryAt(null);
            event.setLastError(null);
            webhookEventRepository.save(event);

            log.info("[WEBHOOK] DELIVERED | id={} paymentId={} to {}",
                    event.getId(), event.getPaymentId(), event.getMerchantUrl());

        } catch (Exception e) {
            handleDeliveryFailure(event, e.getMessage());
        }
    }

    private void handleDeliveryFailure(WebhookEvent event, String errorMessage) {
        int newRetryCount = event.getRetryCount() + 1;
        int[] delays = parseDelays();

        if (newRetryCount > maxRetries) {
            // All retries exhausted — mark permanently FAILED
            event.setStatus(WebhookStatus.FAILED);
            event.setRetryCount(newRetryCount);
            event.setNextRetryAt(null);
            event.setLastError("MAX RETRIES EXCEEDED. Last error: " + errorMessage);
            webhookEventRepository.save(event);

            log.warn("[WEBHOOK] PERMANENTLY FAILED after {} attempts | id={} paymentId={} error={}",
                    newRetryCount, event.getId(), event.getPaymentId(), errorMessage);
        } else {
            // Schedule next retry
            int delayMinutes = (newRetryCount - 1 < delays.length)
                    ? delays[newRetryCount - 1]
                    : delays[delays.length - 1];

            LocalDateTime nextRetry = LocalDateTime.now().plusMinutes(delayMinutes);
            event.setStatus(WebhookStatus.FAILED);
            event.setRetryCount(newRetryCount);
            event.setNextRetryAt(nextRetry);
            event.setLastError(errorMessage);
            webhookEventRepository.save(event);

            log.warn("[WEBHOOK] FAILED (attempt {}/{}) | id={} paymentId={} nextRetry={} error={}",
                    newRetryCount, maxRetries, event.getId(), event.getPaymentId(), nextRetry, errorMessage);
        }
    }

    /** Poll and deliver all webhook events that are due (called by scheduler). */
    public void processDueWebhooks() {
        List<WebhookEvent> due = webhookEventRepository.findDueForDelivery(LocalDateTime.now());
        if (!due.isEmpty()) {
            log.info("[WEBHOOK-SCHEDULER] Processing {} due webhook(s)", due.size());
            due.forEach(this::attemptDelivery);
        }
    }

    private int[] parseDelays() {
        String[] parts = retryDelaysConfig.split(",");
        int[] delays = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            delays[i] = Integer.parseInt(parts[i].trim());
        }
        return delays;
    }

    /** Delivery-specific exception for clean error messages. */
    public static class WebhookDeliveryException extends RuntimeException {
        public WebhookDeliveryException(String message) { super(message); }
    }
}
