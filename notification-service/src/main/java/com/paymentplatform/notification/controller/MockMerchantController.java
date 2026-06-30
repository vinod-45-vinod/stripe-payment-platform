package com.paymentplatform.notification.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock merchant webhook endpoint — active only on "dev" profile (and during tests).
 *
 * Used to test the retry logic end-to-end:
 *   - POST /mock/webhook  — accepts a webhook; fails the first N calls then succeeds
 *   - POST /mock/webhook/configure?failCount=N — set N before a test run
 *   - GET  /mock/webhook/stats — see how many calls were received / failed
 *
 * This controller is explicitly NOT a production component.
 * It exists so the retry logic is actually testable without an external server.
 */
@RestController
@RequestMapping("/mock")
@Profile({"dev", "test"})
@Slf4j
public class MockMerchantController {

    /** How many times to fail before succeeding. Reset via /mock/webhook/configure */
    private final AtomicInteger failRemaining = new AtomicInteger(0);
    private final AtomicInteger totalReceived  = new AtomicInteger(0);
    private final AtomicInteger totalFailed    = new AtomicInteger(0);
    private final AtomicInteger totalDelivered = new AtomicInteger(0);

    /**
     * The mock webhook receiver endpoint.
     * Returns 500 for the first `failRemaining` calls, then 200.
     */
    @PostMapping("/webhook")
    public ResponseEntity<Map<String, Object>> receiveMerchantWebhook(
            @RequestBody String payload) {

        int callNumber = totalReceived.incrementAndGet();
        int remaining = failRemaining.getAndUpdate(v -> Math.max(0, v - 1));

        log.info("[MOCK-MERCHANT] Webhook received (call #{}) | payload={}", callNumber, payload);

        if (remaining > 0) {
            totalFailed.incrementAndGet();
            log.warn("[MOCK-MERCHANT] Simulating failure ({} failures remaining after this)", remaining - 1);
            return ResponseEntity.status(500)
                    .body(Map.of("error", "Simulated merchant server error", "callNumber", callNumber));
        }

        totalDelivered.incrementAndGet();
        log.info("[MOCK-MERCHANT] Webhook ACCEPTED (call #{})", callNumber);
        return ResponseEntity.ok(Map.of("status", "received", "callNumber", callNumber));
    }

    /**
     * Configure how many times the next calls should fail.
     * Call this before each test to set up a specific failure scenario.
     */
    @PostMapping("/webhook/configure")
    public ResponseEntity<Map<String, Object>> configure(
            @RequestParam(defaultValue = "0") int failCount) {
        failRemaining.set(failCount);
        totalReceived.set(0);
        totalFailed.set(0);
        totalDelivered.set(0);
        log.info("[MOCK-MERCHANT] Configured: will fail next {} calls", failCount);
        return ResponseEntity.ok(Map.of("configured", true, "failCount", failCount));
    }

    /** Stats endpoint for test assertions. */
    @GetMapping("/webhook/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of(
                "totalReceived", totalReceived.get(),
                "totalFailed", totalFailed.get(),
                "totalDelivered", totalDelivered.get(),
                "failRemaining", failRemaining.get()
        ));
    }
}
