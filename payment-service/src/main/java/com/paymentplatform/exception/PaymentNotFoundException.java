package com.paymentplatform.exception;

import java.util.UUID;

public class PaymentNotFoundException extends RuntimeException {

    public PaymentNotFoundException(UUID paymentId) {
        super(String.format("Payment not found with id: %s", paymentId));
    }
}
