package com.paymentplatform.notification.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Tracks every webhook delivery attempt to a merchant's registered endpoint.
 *
 * Retry schedule:
 *   Attempt 1: immediate
 *   Attempt 2: +1 minute   (retry_count=1)
 *   Attempt 3: +5 minutes  (retry_count=2)
 *   Attempt 4: +15 minutes (retry_count=3)
 *   Attempt 5: +60 minutes (retry_count=4) → if fails, status=FAILED permanently
 *
 * Status lifecycle:
 *   PENDING   → DELIVERED (success)
 *   PENDING   → FAILED (transient, retry_count < max, next_retry_at set)
 *   FAILED    → DELIVERED (retry succeeded)
 *   FAILED    → FAILED (retry exhausted) → status stays FAILED permanently
 */
@Entity
@Table(name = "webhook_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WebhookEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id")
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "merchant_id", nullable = false)
    private String merchantId;

    @Column(name = "merchant_url", nullable = false, length = 1024)
    private String merchantUrl;

    /** Logical event type, e.g. "payment.captured", "refund.created" */
    @Column(name = "event_type", nullable = false)
    private String eventType;

    /** JSON payload sent in the HTTP POST body to the merchant. */
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private WebhookStatus status = WebhookStatus.PENDING;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private int retryCount = 0;

    /** When null or in the past: eligible for delivery. Scheduler polls this. */
    @Column(name = "next_retry_at")
    private LocalDateTime nextRetryAt;

    /** Last error message for debugging / admin inspection. */
    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    void prePersist() {
        LocalDateTime now = LocalDateTime.now();
        if (createdAt == null) createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public enum WebhookStatus {
        PENDING, DELIVERED, FAILED
    }
}
