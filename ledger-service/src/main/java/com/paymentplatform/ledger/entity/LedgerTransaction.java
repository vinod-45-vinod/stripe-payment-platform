package com.paymentplatform.ledger.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Groups all ledger entries belonging to one captured payment or refund.
 * reference_id = payment_id from Kafka event.
 * reference_type = "PAYMENT_CAPTURED" or "REFUND_CREATED".
 *
 * The sum of all LedgerEntry amounts in a transaction must equal zero
 * (enforced by LedgerService and unit-tested).
 */
@Entity
@Table(name = "ledger_transactions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LedgerTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "reference_id", nullable = false)
    private UUID referenceId;  // payment_id

    @Column(name = "reference_type", nullable = false, length = 32)
    private String referenceType;  // PAYMENT_CAPTURED | REFUND_CREATED

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @OneToMany(mappedBy = "transaction", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<LedgerEntry> entries;
}
