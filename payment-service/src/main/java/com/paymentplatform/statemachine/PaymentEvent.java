package com.paymentplatform.statemachine;

/**
 * Events that trigger payment state transitions.
 * SUCCEED is an internal event fired automatically after CAPTURE.
 */
public enum PaymentEvent {
    AUTHORIZE,
    CAPTURE,
    SUCCEED,   // internal: auto-fired after CAPTURE to move CAPTURED → SUCCEEDED
    REFUND,
    CANCEL,
    FAIL
}
