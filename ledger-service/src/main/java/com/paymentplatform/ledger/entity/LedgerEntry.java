package com.paymentplatform.ledger.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * A single line in a double-entry ledger transaction.
 * Each entry is either a DEBIT or CREDIT on a specific account.
 *
 * Invariant: For each LedgerTransaction, sum of all DEBIT amounts
 * must equal sum of all CREDIT amounts → net sum = 0.
 * (Stored as positive amounts with explicit entry_type direction.)
 */
@Entity
@Table(name = "ledger_entries")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "transaction_id", nullable = false)
    private LedgerTransaction transaction;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    /**
     * DEBIT  = money leaving an account (reduces balance for asset accounts)
     * CREDIT = money entering an account (increases balance for asset accounts)
     */
    @Column(name = "entry_type", nullable = false, length = 10)
    private String entryType;  // DEBIT | CREDIT

    /** Always positive — direction is encoded in entry_type. */
    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}
