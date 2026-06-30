package com.paymentplatform.ledger.repository;

import com.paymentplatform.ledger.entity.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    List<LedgerEntry> findByTransactionId(UUID transactionId);

    /**
     * Zero-sum invariant check — used in tests and can be used for audit.
     * Returns the signed sum: CREDIT amounts as positive, DEBIT amounts as negative.
     * A balanced transaction returns 0.
     */
    @Query("""
        SELECT SUM(CASE WHEN e.entryType = 'CREDIT' THEN e.amount ELSE -e.amount END)
        FROM LedgerEntry e
        WHERE e.transaction.id = :txnId
        """)
    BigDecimal computeSignedSum(@Param("txnId") UUID txnId);
}
