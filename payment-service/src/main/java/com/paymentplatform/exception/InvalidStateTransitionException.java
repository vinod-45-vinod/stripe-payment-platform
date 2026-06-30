package com.paymentplatform.exception;

import com.paymentplatform.statemachine.PaymentEvent;
import com.paymentplatform.statemachine.PaymentState;

public class InvalidStateTransitionException extends RuntimeException {

    public InvalidStateTransitionException(PaymentState currentState, PaymentEvent event) {
        super(String.format(
                "Invalid state transition: cannot apply event '%s' to payment in state '%s'",
                event, currentState));
    }

    public InvalidStateTransitionException(String message) {
        super(message);
    }
}
