package com.paymentplatform.notification.controller;

import com.paymentplatform.notification.entity.WebhookEvent;
import com.paymentplatform.notification.entity.WebhookEvent.WebhookStatus;
import com.paymentplatform.notification.repository.WebhookEventRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Admin endpoints for webhook event inspection.
 * Useful for diagnosing delivery issues and verifying retry behaviour.
 */
@RestController
@RequestMapping("/admin/webhooks")
@RequiredArgsConstructor
@Tag(name = "Webhook Admin", description = "Webhook delivery inspection and diagnostics")
public class WebhookAdminController {

    private final WebhookEventRepository webhookEventRepository;

    @GetMapping("/stats")
    @Operation(summary = "Webhook delivery stats", description = "Returns count of PENDING, DELIVERED, and FAILED webhook events.")
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of(
                "pending",   webhookEventRepository.countByStatus(WebhookStatus.PENDING),
                "delivered", webhookEventRepository.countByStatus(WebhookStatus.DELIVERED),
                "failed",    webhookEventRepository.countByStatus(WebhookStatus.FAILED)
        ));
    }

    @GetMapping("/payment/{paymentId}")
    @Operation(summary = "Get webhooks for a payment", description = "Returns all webhook events for a given payment ID, ordered by creation time.")
    public ResponseEntity<List<WebhookEvent>> byPayment(@PathVariable UUID paymentId) {
        return ResponseEntity.ok(
                webhookEventRepository.findByPaymentIdOrderByCreatedAtAsc(paymentId));
    }
}
