package com.paymentplatform.dto;

import com.paymentplatform.statemachine.PaymentState;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Implements Serializable so it can be stored in Redis cache
 * (via @Cacheable and the IdempotencyService).
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentResponse implements Serializable {

    private UUID id;
    private BigDecimal amount;
    private String currency;
    private PaymentState status;
    private String customerId;
    private String merchantId;
    private String cardToken;
    private String authorizationCode;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private Long version;
}
