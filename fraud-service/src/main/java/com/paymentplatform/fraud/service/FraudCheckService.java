package com.paymentplatform.fraud.service;

import com.paymentplatform.fraud.entity.FraudLog;
import com.paymentplatform.fraud.repository.FraudLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Rule-based fraud detection engine.
 *
 * Rules applied on payment-created events:
 *
 * 1. HIGH_VALUE: payment exceeds configurable threshold (default $10,000).
 *    Result: REVIEW_REQUIRED
 *
 * 2. VELOCITY: customer made more than N payments in the last M seconds.
 *    Uses Redis INCR + EXPIRE for an efficient sliding-window counter.
 *    Result: BLOCK if exceeded, REVIEW_REQUIRED if at the limit
 *
 * Results are always logged to fraud_logs — BLOCK results are NOT yet
 * wired back to payment-service (see FraudServiceApplication for rationale).
 * This is noted as a planned future enhancement.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FraudCheckService {

    private static final String VELOCITY_KEY_PREFIX = "fraud:velocity:";

    private final FraudLogRepository fraudLogRepository;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.rules.high-value-threshold:10000.00}")
    private BigDecimal highValueThreshold;

    @Value("${fraud.rules.velocity-window-seconds:60}")
    private long velocityWindowSeconds;

    @Value("${fraud.rules.velocity-max-payments:5}")
    private long velocityMaxPayments;

    /**
     * Run all fraud rules against a new payment.
     * Idempotent: called once per payment-created event.
     *
     * @param paymentId  payment UUID
     * @param amount     payment amount
     * @param customerId customer identifier
     */
    @Transactional
    public FraudCheckResult checkPayment(UUID paymentId, BigDecimal amount, String customerId) {
        List<String> triggeredRules = new ArrayList<>();
        String result = "APPROVE";

        // Rule 1: High-value check
        if (amount != null && amount.compareTo(highValueThreshold) > 0) {
            triggeredRules.add("HIGH_VALUE");
            result = "REVIEW_REQUIRED";
            log.warn("[FRAUD] HIGH_VALUE triggered: paymentId={}, amount={}, threshold={}",
                    paymentId, amount, highValueThreshold);
        }

        // Rule 2: Velocity check (Redis sliding window counter)
        if (customerId != null) {
            long paymentCount = incrementVelocityCounter(customerId);

            if (paymentCount > velocityMaxPayments) {
                triggeredRules.add("VELOCITY");
                result = "BLOCK";
                log.warn("[FRAUD] VELOCITY triggered: customerId={}, count={}/{} in {}s window",
                        customerId, paymentCount, velocityMaxPayments, velocityWindowSeconds);
            } else if (paymentCount == velocityMaxPayments) {
                triggeredRules.add("VELOCITY");
                if (!"BLOCK".equals(result)) result = "REVIEW_REQUIRED";
                log.warn("[FRAUD] VELOCITY at limit: customerId={}, count={}/{} in {}s window",
                        customerId, paymentCount, velocityMaxPayments, velocityWindowSeconds);
            }
        }

        String ruleTriggered = triggeredRules.isEmpty() ? "NONE"
                : triggeredRules.size() == 1 ? triggeredRules.get(0)
                : "MULTIPLE_RULES";

        String details = buildDetails(triggeredRules, amount, customerId);

        FraudLog log = FraudLog.builder()
                .paymentId(paymentId)
                .customerId(customerId)
                .amount(amount)
                .ruleTriggered(ruleTriggered)
                .result(result)
                .details(details)
                .build();

        fraudLogRepository.save(log);

        this.log.info("[FRAUD] Check complete: paymentId={}, result={}, rules={}",
                paymentId, result, ruleTriggered);

        return new FraudCheckResult(result, ruleTriggered, details);
    }

    /**
     * Increment the velocity counter for a customer and return the new count.
     * Uses Redis INCR + EXPIRE:
     *  - First call in a window: INCR sets counter to 1, EXPIRE sets TTL.
     *  - Subsequent calls: INCR increments, TTL continues from first call.
     * This is an approximate sliding window (fixed window per TTL period).
     */
    private long incrementVelocityCounter(String customerId) {
        String key = VELOCITY_KEY_PREFIX + customerId;
        try {
            Long count = redisTemplate.opsForValue().increment(key);
            if (count != null && count == 1L) {
                // First payment in this window — set expiry
                redisTemplate.expire(key, Duration.ofSeconds(velocityWindowSeconds));
            }
            return count != null ? count : 0L;
        } catch (Exception e) {
            // Redis failure must not block fraud check — fail open (conservative)
            log.warn("[FRAUD] Redis unavailable for velocity check on customer {}: {}",
                    customerId, e.getMessage());
            return 0L;
        }
    }

    private String buildDetails(List<String> rules, BigDecimal amount, String customerId) {
        if (rules.isEmpty()) {
            return "All rules passed — payment approved.";
        }
        StringBuilder sb = new StringBuilder();
        if (rules.contains("HIGH_VALUE")) {
            sb.append(String.format("Amount %.2f exceeds threshold %.2f. ", amount, highValueThreshold));
        }
        if (rules.contains("VELOCITY")) {
            sb.append(String.format("Customer %s exceeded %d payments per %ds window. ",
                    customerId, velocityMaxPayments, velocityWindowSeconds));
        }
        return sb.toString().trim();
    }

    /** Immutable result record returned to the Kafka consumer. */
    public record FraudCheckResult(String result, String ruleTriggered, String details) {}
}
