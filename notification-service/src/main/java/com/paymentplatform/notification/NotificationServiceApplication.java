package com.paymentplatform.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Notification Service.
 *
 * Responsibilities:
 *  - Consume payment lifecycle Kafka events
 *  - Persist webhook_events rows for merchants
 *  - Deliver webhooks via HTTP POST with retry (1m → 5m → 15m → 1h → FAILED)
 *  - DLQ: failed Kafka messages routed to payment-dlq after 3 retries
 *
 * Design principle: this service failing must NEVER affect payment-service.
 * Port: 8082
 */
@SpringBootApplication
@EnableScheduling
public class NotificationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
