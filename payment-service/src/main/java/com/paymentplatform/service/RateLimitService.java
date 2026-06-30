package com.paymentplatform.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Redis-based sliding-window rate limiter for payment-service endpoints.
 *
 * Strategy: fixed-window counter per (customerId, window).
 *   - Key: "rate:{customerId}:{windowEpochSeconds}"
 *   - Each request increments the counter; if it exceeds the limit → 429.
 *   - TTL equals the window size so keys self-expire.
 *
 * This is deliberately simple (fixed-window, not sliding) — it is fully
 * sufficient to demonstrate the concept and explain the trade-off in an
 * interview. A sliding log or token-bucket approach can be layered in later.
 *
 * Configuration (application.yml):
 *   rate-limit.max-requests: 10        # requests per window
 *   rate-limit.window-seconds: 60      # window size in seconds
 */
@Service
@Slf4j
public class RateLimitService {

    private static final String KEY_PREFIX = "rate:";

    private final RedisTemplate<String, Object> redisTemplate;
    private final int maxRequests;
    private final long windowSeconds;

    public RateLimitService(
            RedisTemplate<String, Object> redisTemplate,
            @Value("${rate-limit.max-requests:10}") int maxRequests,
            @Value("${rate-limit.window-seconds:60}") long windowSeconds) {
        this.redisTemplate = redisTemplate;
        this.maxRequests = maxRequests;
        this.windowSeconds = windowSeconds;
    }

    /**
     * Check and record a request for the given customerId.
     *
     * @param customerId the customer making the request
     * @return a {@link RateLimitResult} indicating whether the request is allowed
     */
    public RateLimitResult checkAndRecord(String customerId) {
        if (customerId == null || customerId.isBlank()) {
            return RateLimitResult.allowed(maxRequests, maxRequests);
        }

        long windowStart = System.currentTimeMillis() / 1000 / windowSeconds;
        String key = KEY_PREFIX + customerId + ":" + windowStart;

        try {
            Long count = redisTemplate.opsForValue().increment(key);
            if (count == null) count = 1L;

            // Set TTL on first request in this window
            if (count == 1) {
                redisTemplate.expire(key, Duration.ofSeconds(windowSeconds));
            }

            int remaining = (int) Math.max(0, maxRequests - count);
            boolean allowed = count <= maxRequests;

            if (!allowed) {
                log.warn("[RATE-LIMIT] Customer {} exceeded limit: {}/{} in {}s window",
                        customerId, count, maxRequests, windowSeconds);
            }

            return new RateLimitResult(allowed, remaining, windowSeconds);

        } catch (Exception e) {
            // Redis unavailable — fail open (allow request) to avoid blocking payments
            log.warn("[RATE-LIMIT] Redis unavailable, failing open for customer {}: {}", customerId, e.getMessage());
            return RateLimitResult.allowed(maxRequests, maxRequests);
        }
    }

    /**
     * Encapsulates the result of a rate-limit check.
     *
     * @param allowed       true if the request is within limits
     * @param remaining     remaining requests in the current window
     * @param retryAfterSeconds  seconds until the window resets (for Retry-After header)
     */
    public record RateLimitResult(boolean allowed, int remaining, long retryAfterSeconds) {
        public static RateLimitResult allowed(int remaining, int max) {
            return new RateLimitResult(true, remaining, 0);
        }
    }

    public int getMaxRequests() { return maxRequests; }
    public long getWindowSeconds() { return windowSeconds; }
}
