package com.paymentplatform.statemachine;

/**
 * All possible states in the payment lifecycle.
 *
 * Happy path: CREATED → AUTHORIZED → CAPTURED → SUCCEEDED → REFUNDED
 * Terminal states: SUCCEEDED, REFUNDED, FAILED, CANCELLED
 */
public enum PaymentState {
    CREATED,
    AUTHORIZED,
    CAPTURED,
    SUCCEEDED,
    REFUNDED,
    FAILED,
    CANCELLED
}
