package com.paymentplatform.fraud.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Fraud check result for a payment event.
 * Every payment that passes through fraud-service gets a row here.
 *
 * result: APPROVE | REVIEW_REQUIRED | BLOCK
 *
 * BLOCK results are logged only — not yet wired back into
 * payment-service flow control (see FraudServiceApplication javadoc for rationale).
 */
@Entity
@Table(name = "fraud_logs")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FraudLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "customer_id")
    private String customerId;

    @Column(precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "rule_triggered", nullable = false, length = 64)
    private String ruleTriggered;  // HIGH_VALUE | VELOCITY | MULTIPLE_RULES | NONE

    @Column(nullable = false, length = 20)
    private String result;  // APPROVE | REVIEW_REQUIRED | BLOCK

    @Column(columnDefinition = "TEXT")
    private String details;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}
