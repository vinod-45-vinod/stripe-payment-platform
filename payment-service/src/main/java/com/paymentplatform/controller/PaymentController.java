package com.paymentplatform.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.dto.CreatePaymentRequest;
import com.paymentplatform.dto.PaymentResponse;
import com.paymentplatform.exception.RateLimitExceededException;
import com.paymentplatform.service.PaymentService;
import com.paymentplatform.service.RateLimitService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
@Tag(name = "Payments", description = "Payment lifecycle management — create, authorize, capture, cancel, refund")
public class PaymentController {

    private final PaymentService paymentService;
    private final RateLimitService rateLimitService;
    private final ObjectMapper objectMapper;

    /**
     * POST /payments — create a new payment (status = CREATED).
     * Supports Idempotency-Key header: duplicate requests with the same key + body
     * return the original response without creating a second payment.
     * Rate-limited per customerId.
     */
    @Operation(summary = "Create a new payment",
               description = "Creates a payment in CREATED state. Supports idempotency via Idempotency-Key header.")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Payment created"),
        @ApiResponse(responseCode = "400", description = "Validation error"),
        @ApiResponse(responseCode = "409", description = "Idempotency conflict"),
        @ApiResponse(responseCode = "429", description = "Rate limit exceeded")
    })
    @PostMapping
    public ResponseEntity<PaymentResponse> createPayment(
            @Parameter(description = "Optional idempotency key for deduplication")
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody CreatePaymentRequest request) throws JsonProcessingException {

        checkRateLimit(request.getCustomerId());

        String requestBodyJson = objectMapper.writeValueAsString(request);
        PaymentResponse response = paymentService.createPayment(idempotencyKey, requestBodyJson, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * POST /payments/{id}/authorize — CREATED → AUTHORIZED.
     */
    @Operation(summary = "Authorize a payment", description = "Transitions payment from CREATED to AUTHORIZED.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Payment authorized"),
        @ApiResponse(responseCode = "404", description = "Payment not found"),
        @ApiResponse(responseCode = "409", description = "Invalid state transition")
    })
    @PostMapping("/{id}/authorize")
    public ResponseEntity<PaymentResponse> authorizePayment(
            @Parameter(description = "Payment UUID") @PathVariable UUID id) {
        return ResponseEntity.ok(paymentService.authorizePayment(id));
    }

    /**
     * POST /payments/{id}/capture — AUTHORIZED → CAPTURED → SUCCEEDED (auto).
     */
    @Operation(summary = "Capture a payment", description = "Transitions AUTHORIZED → SUCCEEDED. Protected by optimistic locking.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Payment captured and succeeded"),
        @ApiResponse(responseCode = "404", description = "Payment not found"),
        @ApiResponse(responseCode = "409", description = "Invalid transition or concurrent modification")
    })
    @PostMapping("/{id}/capture")
    public ResponseEntity<PaymentResponse> capturePayment(
            @Parameter(description = "Payment UUID") @PathVariable UUID id) {
        return ResponseEntity.ok(paymentService.capturePayment(id));
    }

    /**
     * POST /payments/{id}/cancel — CREATED or AUTHORIZED → CANCELLED.
     */
    @Operation(summary = "Cancel a payment", description = "Cancels a payment that has not yet been captured.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Payment cancelled"),
        @ApiResponse(responseCode = "404", description = "Payment not found"),
        @ApiResponse(responseCode = "409", description = "Invalid state transition (cannot cancel after capture)")
    })
    @PostMapping("/{id}/cancel")
    public ResponseEntity<PaymentResponse> cancelPayment(
            @Parameter(description = "Payment UUID") @PathVariable UUID id) {
        return ResponseEntity.ok(paymentService.cancelPayment(id));
    }

    /**
     * POST /payments/{id}/refund — SUCCEEDED → REFUNDED.
     */
    @Operation(summary = "Refund a payment", description = "Refunds a succeeded payment. Triggers double-entry reversal in ledger-service via Kafka.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Payment refunded"),
        @ApiResponse(responseCode = "404", description = "Payment not found"),
        @ApiResponse(responseCode = "409", description = "Invalid state transition (must be SUCCEEDED)")
    })
    @PostMapping("/{id}/refund")
    public ResponseEntity<PaymentResponse> refundPayment(
            @Parameter(description = "Payment UUID") @PathVariable UUID id) {
        return ResponseEntity.ok(paymentService.refundPayment(id));
    }

    /**
     * GET /payments/{id} — retrieve a single payment by ID.
     * Response is cached in Redis (TTL 10 min); cache is evicted on any write.
     */
    @Operation(summary = "Get a payment by ID", description = "Returns payment details. Redis-cached for 10 minutes.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Payment found"),
        @ApiResponse(responseCode = "404", description = "Payment not found")
    })
    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getPayment(
            @Parameter(description = "Payment UUID") @PathVariable UUID id) {
        return ResponseEntity.ok(paymentService.getPayment(id));
    }

    /**
     * GET /payments/history — list all payments, newest first.
     */
    @Operation(summary = "List all payments", description = "Returns all payments ordered by creation time descending.")
    @ApiResponse(responseCode = "200", description = "List of payments")
    @GetMapping("/history")
    public ResponseEntity<List<PaymentResponse>> getPaymentHistory() {
        return ResponseEntity.ok(paymentService.getPaymentHistory());
    }

    // -------------------------------------------------------------------------

    private void checkRateLimit(String customerId) {
        RateLimitService.RateLimitResult result = rateLimitService.checkAndRecord(customerId);
        if (!result.allowed()) {
            throw new RateLimitExceededException(customerId, result.retryAfterSeconds());
        }
    }
}
