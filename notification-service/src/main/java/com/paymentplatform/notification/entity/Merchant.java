package com.paymentplatform.notification.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Represents a registered merchant in the notification-service.
 * Each merchant can register a webhook_url to receive payment event notifications.
 */
@Entity
@Table(name = "merchants")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Merchant {

    @Id
    @Column(name = "id", nullable = false)
    private String id;  // e.g. "merch_001" — matches merchantId in payment events

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "webhook_url")
    private String webhookUrl;  // registered HTTPS endpoint; null = no webhook

    @Builder.Default
    @Column(name = "active", nullable = false)
    private boolean active = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = LocalDateTime.now();
    }
}
