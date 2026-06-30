package com.paymentplatform.entity;

import com.paymentplatform.events.OutboxEventType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Outbox event row — written in the SAME database transaction as the payment state change.
 *
 * The {@link com.paymentplatform.service.OutboxPublisherJob} scheduler polls
 * {@code published = false} rows, publishes them to Kafka, and flips the flag.
 * This guarantees at-least-once delivery even if Kafka is temporarily unavailable.
 *
 * OutboxEventType lives in the shared-events module (com.paymentplatform.events)
 * so all consumer services share the same enum definition.
 */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 64)
    private OutboxEventType eventType;

    /** JSON-serialised event payload — see com.paymentplatform.events package. */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(nullable = false)
    @Builder.Default
    private boolean published = false;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}
