package com.paymentplatform.ledger.repository;

import com.paymentplatform.ledger.entity.LedgerTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface LedgerTransactionRepository extends JpaRepository<LedgerTransaction, UUID> {

    /** Check if we've already processed this payment — for idempotent consumption. */
    Optional<LedgerTransaction> findByReferenceId(UUID referenceId);
}
