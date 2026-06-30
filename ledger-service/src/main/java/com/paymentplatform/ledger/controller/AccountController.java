package com.paymentplatform.ledger.controller;

import com.paymentplatform.ledger.service.LedgerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

/**
 * REST API for ledger balance queries.
 * Used for sanity checking and demo purposes.
 *
 * GET /accounts/{ownerType}/{ownerId}/balance
 *   ownerType: CUSTOMER | MERCHANT | PLATFORM
 *   ownerId:   the account identifier (customerId, merchantId, or "platform-fee-account")
 */
@RestController
@RequestMapping("/accounts")
@RequiredArgsConstructor
@Tag(name = "Ledger Accounts", description = "Double-entry ledger balance queries")
public class AccountController {

    private final LedgerService ledgerService;

    @GetMapping("/{ownerType}/{ownerId}/balance")
    @Operation(
        summary = "Get account balance",
        description = "Returns the current balance for a CUSTOMER, MERCHANT, or PLATFORM account. " +
                      "Balances are updated by Kafka-driven double-entry transactions."
    )
    @ApiResponse(responseCode = "200", description = "Balance returned")
    public ResponseEntity<Map<String, Object>> getBalance(
            @Parameter(description = "Account owner type: CUSTOMER | MERCHANT | PLATFORM")
            @PathVariable String ownerType,
            @Parameter(description = "Account owner identifier (e.g. customerId, merchantId)")
            @PathVariable String ownerId) {

        BigDecimal balance = ledgerService.getBalance(ownerType.toUpperCase(), ownerId);

        return ResponseEntity.ok(Map.of(
                "ownerType", ownerType.toUpperCase(),
                "ownerId", ownerId,
                "balance", balance
        ));
    }
}
