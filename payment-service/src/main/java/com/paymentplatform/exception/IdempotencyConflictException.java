package com.paymentplatform.exception;

/**
 * Thrown when an Idempotency-Key is reused with a different request body.
 * Per the spec: same key + same body → return cached response (200/201).
 *               same key + different body → 409 Conflict.
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String key) {
        super("Idempotency-Key '" + key + "' was already used with a different request body. " +
              "If retrying the same request, ensure the body is identical.");
    }
}
