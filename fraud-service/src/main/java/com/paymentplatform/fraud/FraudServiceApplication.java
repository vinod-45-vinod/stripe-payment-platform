package com.paymentplatform.fraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Fraud Service — rule-based fraud detection for the payment platform.
 *
 * Consumes Kafka events:
 *  - payment-created  → run fraud checks (high-value, velocity)
 *  - payment-authorized → optionally run checks at authorization stage
 *
 * Results (APPROVE / REVIEW_REQUIRED / BLOCK) are logged to fraud_logs table.
 *
 * Design decision: results are logged only — blocking is NOT wired back
 * into payment-service's flow control. This is noted as a future enhancement.
 * Rationale: adding synchronous fraud-check calls would introduce coupling between
 * payment-service and fraud-service, violating the event-driven decoupling principle.
 * Future approach: fraud-service publishes a fraud-decision event, and
 * payment-service consumes it to update payment status.
 *
 * Port: 8083
 */
@SpringBootApplication
public class FraudServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(FraudServiceApplication.class, args);
    }
}
