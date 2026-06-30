package com.paymentplatform.ledger.repository;

import com.paymentplatform.ledger.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

public interface AccountRepository extends JpaRepository<Account, UUID> {

    Optional<Account> findByOwnerTypeAndOwnerId(String ownerType, String ownerId);

    @Modifying
    @Query("UPDATE Account a SET a.balance = a.balance + :delta WHERE a.id = :id")
    void adjustBalance(@Param("id") UUID id, @Param("delta") BigDecimal delta);
}
