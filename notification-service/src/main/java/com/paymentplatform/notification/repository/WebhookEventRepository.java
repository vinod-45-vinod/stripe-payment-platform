package com.paymentplatform.notification.repository;

import com.paymentplatform.notification.entity.WebhookEvent;
import com.paymentplatform.notification.entity.WebhookEvent.WebhookStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface WebhookEventRepository extends JpaRepository<WebhookEvent, UUID> {

    /**
     * Polls for webhook events that are eligible for delivery:
     * - Status is PENDING or FAILED (not permanently exhausted, not DELIVERED)
     * - next_retry_at is null (first attempt) OR in the past (retry due)
     *
     * Capped at 50 per run to prevent memory pressure.
     */
    @Query("""
            SELECT w FROM WebhookEvent w
            WHERE w.status IN ('PENDING', 'FAILED')
              AND (w.nextRetryAt IS NULL OR w.nextRetryAt <= :now)
            ORDER BY w.createdAt ASC
            LIMIT 50
            """)
    List<WebhookEvent> findDueForDelivery(@Param("now") LocalDateTime now);

    /** Count by status — useful for health/admin endpoints. */
    long countByStatus(WebhookStatus status);

    /** Find all webhook events for a given payment (for admin inspection). */
    List<WebhookEvent> findByPaymentIdOrderByCreatedAtAsc(UUID paymentId);
}
