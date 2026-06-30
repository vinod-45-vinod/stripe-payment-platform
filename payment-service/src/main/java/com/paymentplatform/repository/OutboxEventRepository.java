package com.paymentplatform.repository;

import com.paymentplatform.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

/**
 * Repository for outbox_events.
 * The scheduler uses {@link #findTop100ByPublishedFalseOrderByCreatedAtAsc()} to
 * batch-process a bounded number of events per run, avoiding unbounded memory usage.
 */
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Fetch up to 100 unpublished events, oldest first — used by the scheduler.
     * Relies on the partial index: idx_outbox_unpublished WHERE published = FALSE.
     */
    List<OutboxEvent> findTop100ByPublishedFalseOrderByCreatedAtAsc();

    /** Count unpublished events — useful for health checks and tests. */
    @Query("SELECT COUNT(e) FROM OutboxEvent e WHERE e.published = false")
    long countUnpublished();
}
