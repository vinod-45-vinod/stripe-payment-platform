package com.paymentplatform.entity;

import com.paymentplatform.statemachine.PaymentState;
import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Payment entity. Implements Serializable so it can be stored in Redis cache.
 * @Version enables JPA optimistic locking — if two concurrent writes attempt
 * to update the same row, the second one throws ObjectOptimisticLockingFailureException.
 */
@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Payment implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentState status;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(name = "merchant_id", nullable = false)
    private String merchantId;

    @Column(name = "card_token")
    private String cardToken;

    @Column(name = "authorization_code")
    private String authorizationCode;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    /**
     * JPA optimistic locking via @Version.
     * The `version` column is part of every UPDATE: "UPDATE payments SET ... WHERE id=? AND version=?",
     * bumping the version on success. A concurrent write on a stale version gets 0 rows updated →
     * JPA throws ObjectOptimisticLockingFailureException.
     */
    @Version
    @Column(nullable = false)
    private Long version;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (status == null) {
            status = PaymentState.CREATED;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
