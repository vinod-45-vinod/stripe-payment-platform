package com.paymentplatform.notification.analytics;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes rolling payment analytics derived from event consumption.
 *
 * Demonstrates the downstream value of the event-driven architecture:
 * no direct DB query to payment-service — all data comes from Kafka events
 * consumed by this service independently.
 *
 * GET /analytics → current snapshot of metrics since startup.
 */
@RestController
@RequestMapping("/analytics")
@RequiredArgsConstructor
@Tag(name = "Analytics", description = "Rolling payment metrics derived from Kafka event consumption")
public class AnalyticsController {

    private final PaymentAnalyticsStore analyticsStore;

    @GetMapping
    @Operation(
        summary = "Get payment analytics",
        description = "Returns rolling metrics since service startup: payment volumes per merchant, " +
                      "success rate, and average authorization-to-capture time."
    )
    public ResponseEntity<PaymentAnalyticsStore.AnalyticsSnapshot> getAnalytics() {
        return ResponseEntity.ok(analyticsStore.snapshot());
    }
}
