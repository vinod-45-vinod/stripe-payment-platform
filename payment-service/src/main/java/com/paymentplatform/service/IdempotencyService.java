package com.paymentplatform.service;

import com.paymentplatform.dto.PaymentResponse;
import com.paymentplatform.exception.IdempotencyConflictException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

/**
 * Redis-backed idempotency layer for POST /payments.
 *
 * Protocol:
 *  - On first call: store {requestBodyHash, response} in Redis under the key, TTL 24h.
 *  - On duplicate call with SAME body hash: return cached PaymentResponse (no DB hit).
 *  - On duplicate call with DIFFERENT body hash: throw IdempotencyConflictException → 409.
 *
 * Redis key format: "idempotency:{clientSuppliedKey}"
 *
 * Uses Java Serialization (JdkSerializationRedisSerializer) to avoid Jackson 2/3 conflicts.
 * Both IdempotencyEntry and PaymentResponse implement Serializable.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private static final String KEY_PREFIX = "idempotency:";
    private static final Duration TTL = Duration.ofHours(24);

    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * Check Redis for a previously stored response for this idempotency key.
     *
     * @param idempotencyKey  the client-supplied header value
     * @param requestBodyJson the raw JSON body of the current request
     * @return cached PaymentResponse if this is a legitimate retry, null if key is new
     * @throws IdempotencyConflictException if the key was seen before with a different body
     */
    public PaymentResponse checkExisting(String idempotencyKey, String requestBodyJson) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        IdempotencyEntry existing = (IdempotencyEntry) redisTemplate.opsForValue().get(redisKey);

        if (existing == null) {
            // First time seeing this key — new request, proceed normally
            return null;
        }

        String currentHash = sha256(requestBodyJson);
        if (!currentHash.equals(existing.requestHash)) {
            log.warn("Idempotency conflict: key='{}' seen before with a different body", idempotencyKey);
            throw new IdempotencyConflictException(idempotencyKey);
        }

        log.info("Idempotency cache hit: key='{}' — returning cached response", idempotencyKey);
        return existing.response;
    }

    /**
     * Store the response in Redis after a successful payment creation.
     *
     * @param idempotencyKey  the client-supplied header value
     * @param requestBodyJson the raw JSON body (used for hash comparison on retries)
     * @param response        the PaymentResponse to cache
     */
    public void store(String idempotencyKey, String requestBodyJson, PaymentResponse response) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        IdempotencyEntry entry = new IdempotencyEntry(sha256(requestBodyJson), response);
        redisTemplate.opsForValue().set(redisKey, entry, TTL);
        log.debug("Idempotency entry stored: key='{}', ttl={}h", idempotencyKey, TTL.toHours());
    }

    // -------------------------------------------------------------------------

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    // -------------------------------------------------------------------------
    // Inner DTO stored in Redis per idempotency key
    // -------------------------------------------------------------------------

    /**
     * Redis value: stores the SHA-256 hash of the original request body
     * and the cached PaymentResponse.
     * Implements Serializable for JdkSerializationRedisSerializer.
     */
    static class IdempotencyEntry implements Serializable {
        final String requestHash;
        final PaymentResponse response;

        IdempotencyEntry(String requestHash, PaymentResponse response) {
            this.requestHash = requestHash;
            this.response = response;
        }
    }
}
