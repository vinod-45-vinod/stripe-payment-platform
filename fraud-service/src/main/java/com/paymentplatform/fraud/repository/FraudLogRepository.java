package com.paymentplatform.fraud.repository;

import com.paymentplatform.fraud.entity.FraudLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface FraudLogRepository extends JpaRepository<FraudLog, UUID> {

    List<FraudLog> findByPaymentId(UUID paymentId);

    List<FraudLog> findByCustomerId(String customerId);
}
