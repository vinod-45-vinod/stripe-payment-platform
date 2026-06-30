package com.paymentplatform.notification.repository;

import com.paymentplatform.notification.entity.Merchant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface MerchantRepository extends JpaRepository<Merchant, String> {

    /** Find an active merchant by their ID. */
    Optional<Merchant> findByIdAndActiveTrue(String id);
}
