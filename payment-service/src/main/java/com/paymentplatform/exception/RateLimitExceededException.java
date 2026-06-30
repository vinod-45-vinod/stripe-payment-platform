package com.paymentplatform.exception;

/**
 * Thrown when a customer exceeds the configured rate limit on payment endpoints.
 * Translated to HTTP 429 by {@link GlobalExceptionHandler}.
 */
public class RateLimitExceededException extends RuntimeException {

    private final long retryAfterSeconds;

    public RateLimitExceededException(String customerId, long retryAfterSeconds) {
        super("Rate limit exceeded for customer: " + customerId +
              ". Retry after " + retryAfterSeconds + " seconds.");
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
