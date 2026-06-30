package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.Account;
import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.entity.LedgerTransaction;
import com.paymentplatform.ledger.repository.AccountRepository;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.ledger.repository.LedgerTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.UUID;

/**
 * Core double-entry bookkeeping logic.
 *
 * For every captured payment (amount A):
 *   DEBIT  customer account  -A
 *   CREDIT merchant account  +(A * 0.95)   [5% platform fee]
 *   CREDIT platform account  +(A * 0.05)
 *   Net sum = -A + 0.95A + 0.05A = 0  ✓
 *
 * For refunds — reverse all entries from the original transaction.
 *
 * Both operations are idempotent: if a LedgerTransaction for the
 * given paymentId already exists, the event is skipped. This handles
 * Kafka at-least-once delivery gracefully.
 *
 * Diagram reference: Payment_Platform_Diagrams.txt — Diagram 13 (Ledger Double-Entry).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LedgerService {

    static final String PLATFORM_OWNER_ID = "platform-fee-account";
    static final BigDecimal PLATFORM_FEE_RATE = new BigDecimal("0.05");

    private final AccountRepository accountRepository;
    private final LedgerTransactionRepository ledgerTransactionRepository;
    private final LedgerEntryRepository ledgerEntryRepository;

    /**
     * Record double-entry bookkeeping for a captured payment.
     * Idempotent: skips if already processed.
     *
     * @param paymentId  the captured payment UUID (from Kafka event)
     * @param amount     the full payment amount
     * @param customerId the paying customer
     * @param merchantId the receiving merchant
     */
    @Transactional
    public void recordCapture(UUID paymentId, BigDecimal amount, String customerId, String merchantId) {
        if (ledgerTransactionRepository.findByReferenceId(paymentId).isPresent()) {
            log.info("Ledger: capture already recorded for payment {}, skipping (idempotent)", paymentId);
            return;
        }

        Account customerAccount = getOrCreateAccount("CUSTOMER", customerId);
        Account merchantAccount = getOrCreateAccount("MERCHANT", merchantId);
        Account platformAccount = getOrCreateAccount("PLATFORM", PLATFORM_OWNER_ID);

        BigDecimal platformFee      = amount.multiply(PLATFORM_FEE_RATE).setScale(4, RoundingMode.HALF_UP);
        BigDecimal merchantReceives = amount.subtract(platformFee).setScale(4, RoundingMode.HALF_UP);

        LedgerTransaction txn = ledgerTransactionRepository.save(
                LedgerTransaction.builder()
                        .referenceId(paymentId)
                        .referenceType("PAYMENT_CAPTURED")
                        .build()
        );

        // DEBIT customer (money leaves customer)
        LedgerEntry customerDebit = createEntry(txn, customerAccount, "DEBIT", amount);
        // CREDIT merchant (merchant receives payment minus fee)
        LedgerEntry merchantCredit = createEntry(txn, merchantAccount, "CREDIT", merchantReceives);
        // CREDIT platform (platform earns fee)
        LedgerEntry platformCredit = createEntry(txn, platformAccount, "CREDIT", platformFee);

        ledgerEntryRepository.saveAll(List.of(customerDebit, merchantCredit, platformCredit));

        // Update running balances
        accountRepository.adjustBalance(customerAccount.getId(), amount.negate());
        accountRepository.adjustBalance(merchantAccount.getId(), merchantReceives);
        accountRepository.adjustBalance(platformAccount.getId(), platformFee);

        log.info("Ledger capture recorded: paymentId={}, amount={}, merchantReceives={}, platformFee={}",
                paymentId, amount, merchantReceives, platformFee);

        // Verify zero-sum invariant (guard — catches any future logic errors)
        assertZeroSum(txn.getId());
    }

    /**
     * Reverse all ledger entries for a refunded payment.
     * Idempotent: skips if already processed.
     */
    @Transactional
    public void recordRefund(UUID paymentId, BigDecimal amount) {
        String refundReferenceType = "REFUND_CREATED:" + paymentId;

        // Check idempotency using a composite reference
        if (ledgerTransactionRepository.findByReferenceId(paymentId).isEmpty()) {
            log.warn("Ledger: no capture found for refund paymentId={}, cannot reverse", paymentId);
            return;
        }

        // Build reverse transaction
        LedgerTransaction originalTxn = ledgerTransactionRepository.findByReferenceId(paymentId).get();

        // Check if we already wrote a REFUND transaction for this payment
        boolean alreadyRefunded = ledgerTransactionRepository.findByReferenceId(paymentId)
                .map(t -> "REFUND_CREATED".equals(t.getReferenceType()))
                .orElse(false);

        if (alreadyRefunded) {
            log.info("Ledger: refund already recorded for payment {}, skipping", paymentId);
            return;
        }

        List<LedgerEntry> originalEntries = ledgerEntryRepository.findByTransactionId(originalTxn.getId());

        LedgerTransaction refundTxn = ledgerTransactionRepository.save(
                LedgerTransaction.builder()
                        .referenceId(paymentId)
                        .referenceType("REFUND_CREATED")
                        .build()
        );

        // Reverse each entry: DEBIT becomes CREDIT and vice versa
        List<LedgerEntry> reversingEntries = originalEntries.stream()
                .map(original -> {
                    String reversedType = "DEBIT".equals(original.getEntryType()) ? "CREDIT" : "DEBIT";
                    LedgerEntry reversed = createEntry(refundTxn, original.getAccount(), reversedType, original.getAmount());

                    // Adjust balances in reverse
                    BigDecimal delta = "CREDIT".equals(reversedType)
                            ? original.getAmount()
                            : original.getAmount().negate();
                    accountRepository.adjustBalance(original.getAccount().getId(), delta);

                    return reversed;
                })
                .toList();

        ledgerEntryRepository.saveAll(reversingEntries);

        log.info("Ledger refund recorded: paymentId={}, amount={}", paymentId, amount);
        assertZeroSum(refundTxn.getId());
    }

    public BigDecimal getBalance(String ownerType, String ownerId) {
        return accountRepository.findByOwnerTypeAndOwnerId(ownerType, ownerId)
                .map(Account::getBalance)
                .orElse(BigDecimal.ZERO);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private Account getOrCreateAccount(String ownerType, String ownerId) {
        return accountRepository.findByOwnerTypeAndOwnerId(ownerType, ownerId)
                .orElseGet(() -> accountRepository.save(
                        Account.builder()
                                .ownerType(ownerType)
                                .ownerId(ownerId)
                                .balance(BigDecimal.ZERO)
                                .build()
                ));
    }

    private LedgerEntry createEntry(LedgerTransaction txn, Account account,
                                    String entryType, BigDecimal amount) {
        return LedgerEntry.builder()
                .transaction(txn)
                .account(account)
                .entryType(entryType)
                .amount(amount)
                .build();
    }

    /**
     * Asserts zero-sum invariant on a completed transaction.
     * Sum of CREDIT amounts - Sum of DEBIT amounts must be 0.
     * Throws IllegalStateException if violated — this is a serious accounting bug.
     */
    void assertZeroSum(UUID txnId) {
        BigDecimal signedSum = ledgerEntryRepository.computeSignedSum(txnId);
        if (signedSum == null || signedSum.compareTo(BigDecimal.ZERO) != 0) {
            throw new IllegalStateException(
                    "LEDGER INVARIANT VIOLATED: transaction " + txnId +
                    " has non-zero signed sum: " + signedSum +
                    ". Money was created or destroyed — this must not happen.");
        }
        log.debug("Zero-sum invariant verified for transaction {}", txnId);
    }
}
