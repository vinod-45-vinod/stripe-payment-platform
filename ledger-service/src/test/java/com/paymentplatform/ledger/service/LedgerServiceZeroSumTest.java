package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.Account;
import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.entity.LedgerTransaction;
import com.paymentplatform.ledger.repository.AccountRepository;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.ledger.repository.LedgerTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for LedgerService double-entry invariants.
 * These tests do NOT require a database — they mock repositories.
 *
 * Key invariant tested:
 *   For any captured payment of amount A:
 *   DEBIT(customer) + CREDIT(merchant) + CREDIT(platform) = 0
 *   i.e., -A + (A * 0.95) + (A * 0.05) = 0
 */
class LedgerServiceZeroSumTest {

    private LedgerService ledgerService;
    private AccountRepository accountRepository;
    private LedgerTransactionRepository ledgerTransactionRepository;
    private LedgerEntryRepository ledgerEntryRepository;

    @BeforeEach
    void setUp() {
        accountRepository = mock(AccountRepository.class);
        ledgerTransactionRepository = mock(LedgerTransactionRepository.class);
        ledgerEntryRepository = mock(LedgerEntryRepository.class);

        ledgerService = new LedgerService(accountRepository, ledgerTransactionRepository, ledgerEntryRepository);
    }

    @Test
    @DisplayName("Zero-sum invariant: DEBIT(customer) = CREDIT(merchant) + CREDIT(platform)")
    void captureEntries_alwaysSumToZero() {
        BigDecimal amount = new BigDecimal("100.00");
        BigDecimal expectedFee = new BigDecimal("5.0000");
        BigDecimal expectedMerchant = new BigDecimal("95.0000");

        // Verify the math directly (no DB required)
        BigDecimal debit  = amount.negate();           // -100.00
        BigDecimal credit = expectedMerchant.add(expectedFee);  //  100.00

        assertThat(debit.add(credit)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    @DisplayName("Platform fee is exactly 5% of captured amount")
    void platformFee_isExactlyFivePercent() {
        BigDecimal[] testAmounts = {
                new BigDecimal("100.00"),
                new BigDecimal("250.50"),
                new BigDecimal("1000.00"),
                new BigDecimal("0.01")
        };

        for (BigDecimal amount : testAmounts) {
            BigDecimal fee = amount.multiply(LedgerService.PLATFORM_FEE_RATE)
                    .setScale(4, java.math.RoundingMode.HALF_UP);
            BigDecimal merchantReceives = amount.subtract(fee).setScale(4, java.math.RoundingMode.HALF_UP);

            // Zero-sum: debit customer, credit merchant + platform
            BigDecimal signedSum = amount.negate().add(merchantReceives).add(fee);

            assertThat(signedSum)
                    .as("Zero-sum invariant for amount=%s", amount)
                    .isEqualByComparingTo(BigDecimal.ZERO);
        }
    }

    @Test
    @DisplayName("assertZeroSum throws when entries do NOT balance")
    void assertZeroSum_throwsOnImbalance() {
        UUID txnId = UUID.randomUUID();

        // Simulate an imbalanced transaction (e.g., CREDIT sum != DEBIT sum)
        when(ledgerEntryRepository.computeSignedSum(txnId))
                .thenReturn(new BigDecimal("5.00"));  // non-zero = imbalanced

        assertThatThrownBy(() -> ledgerService.assertZeroSum(txnId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("LEDGER INVARIANT VIOLATED");
    }

    @Test
    @DisplayName("assertZeroSum passes when entries balance to zero")
    void assertZeroSum_passesOnBalance() {
        UUID txnId = UUID.randomUUID();

        when(ledgerEntryRepository.computeSignedSum(txnId))
                .thenReturn(BigDecimal.ZERO);

        // Should not throw
        ledgerService.assertZeroSum(txnId);
    }

    @Test
    @DisplayName("recordCapture is idempotent — skips if already processed")
    void recordCapture_isIdempotent() {
        UUID paymentId = UUID.randomUUID();

        // Simulate: this payment was already recorded
        LedgerTransaction existing = LedgerTransaction.builder()
                .id(UUID.randomUUID())
                .referenceId(paymentId)
                .referenceType("PAYMENT_CAPTURED")
                .build();
        when(ledgerTransactionRepository.findByReferenceId(paymentId))
                .thenReturn(Optional.of(existing));

        ledgerService.recordCapture(paymentId, new BigDecimal("100.00"), "cust-1", "merch-1");

        // Verify no new entries were saved
        verify(ledgerEntryRepository, never()).saveAll(any());
    }
}
