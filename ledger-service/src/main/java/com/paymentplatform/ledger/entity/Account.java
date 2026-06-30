package com.paymentplatform.ledger.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Represents a ledger account for a participant (CUSTOMER, MERCHANT, or PLATFORM).
 * Balance is a running total updated on every captured payment and refund.
 *
 * owner_type + owner_id is unique — the application uses getOrCreate logic
 * to auto-provision accounts on first encounter.
 */
@Entity
@Table(name = "accounts",
       uniqueConstraints = @UniqueConstraint(columnNames = {"owner_type", "owner_id"}))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "owner_type", nullable = false, length = 20)
    private String ownerType;  // CUSTOMER | MERCHANT | PLATFORM

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    @Column(nullable = false, precision = 19, scale = 4)
    @Builder.Default
    private BigDecimal balance = BigDecimal.ZERO;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
