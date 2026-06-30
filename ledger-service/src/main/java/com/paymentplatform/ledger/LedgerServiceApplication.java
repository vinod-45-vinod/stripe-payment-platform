package com.paymentplatform.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Ledger Service — double-entry bookkeeping microservice.
 *
 * Consumes Kafka events:
 *  - payment-captured → debit customer, credit merchant (minus fee), credit platform-fee
 *  - refund-created   → reverse all ledger entries for that payment
 *
 * Exposes: GET /accounts/{ownerId}/balance
 * Port: 8081
 */
@SpringBootApplication
public class LedgerServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LedgerServiceApplication.class, args);
    }
}
