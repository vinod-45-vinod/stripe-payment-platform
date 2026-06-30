package com.paymentplatform.repository;

import com.paymentplatform.entity.Payment;
import com.paymentplatform.statemachine.PaymentState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    List<Payment> findAllByOrderByCreatedAtDesc();

    List<Payment> findByStatus(PaymentState status);

    List<Payment> findByCustomerId(String customerId);

    List<Payment> findByMerchantId(String merchantId);
}
