package com.paymentplatform.service;

import com.paymentplatform.dto.CreatePaymentRequest;
import com.paymentplatform.dto.PaymentResponse;
import com.paymentplatform.exception.IdempotencyConflictException;
import com.paymentplatform.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test for Redis-backed idempotency.
 *
 * Demonstrates:
 * 1. Same Idempotency-Key + same body → only ONE payment row in DB, second call returns cached response.
 * 2. Same Idempotency-Key + different body → 409 Conflict (IdempotencyConflictException).
 * 3. No Idempotency-Key → normal behaviour, no deduplication.
 *
 * Uses Testcontainers for a real Redis instance. Skipped if Docker is not available.
 */
@SpringBootTest(properties = {
        "spring.cache.type=redis",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "outbox.publisher.delay-ms=3600000"
})
@Testcontainers(disabledWithoutDocker = true)
class IdempotencyIntegrationTest {

    // ---------- Infrastructure ----------

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    // ---------- Beans ----------

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // ---------- Setup ----------

    @BeforeEach
    void cleanUp() {
        paymentRepository.deleteAll();
        // Flush all Redis keys between tests
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    // ---------- Tests ----------

    @Test
    @DisplayName("Duplicate request with same Idempotency-Key and same body → only one payment created, cached response returned")
    void duplicateRequest_sameBody_returnsCachedResponse() {
        String idempotencyKey = "idem-test-" + System.nanoTime();
        CreatePaymentRequest request = buildRequest("100.00", "cust_001", "merch_001");
        String bodyJson = toJson(request);

        // First call — creates payment
        PaymentResponse first = paymentService.createPayment(idempotencyKey, bodyJson, request);
        assertThat(first.getId()).isNotNull();

        // Second call — same key, same body → should return cached response, no new DB row
        PaymentResponse second = paymentService.createPayment(idempotencyKey, bodyJson, request);

        // IDs must match — same response, no duplicate payment
        assertThat(second.getId()).isEqualTo(first.getId());

        // Critically: only ONE row in the database
        long count = paymentRepository.count();
        assertThat(count)
                .as("Only one payment row should exist for a duplicate idempotent request")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("Same Idempotency-Key with different body → 409 IdempotencyConflictException")
    void duplicateRequest_differentBody_throws409() {
        String idempotencyKey = "idem-conflict-" + System.nanoTime();

        CreatePaymentRequest originalRequest = buildRequest("100.00", "cust_001", "merch_001");
        String originalBodyJson = toJson(originalRequest);
        paymentService.createPayment(idempotencyKey, originalBodyJson, originalRequest);

        // Send a different body with the same key
        CreatePaymentRequest differentRequest = buildRequest("999.00", "cust_999", "merch_999");
        String differentBodyJson = toJson(differentRequest);

        assertThatThrownBy(() ->
                paymentService.createPayment(idempotencyKey, differentBodyJson, differentRequest))
                .isInstanceOf(IdempotencyConflictException.class)
                .hasMessageContaining(idempotencyKey);

        // Still only the original payment in DB
        assertThat(paymentRepository.count()).isEqualTo(1L);
    }

    @Test
    @DisplayName("Request without Idempotency-Key → two separate calls create two separate payments")
    void noIdempotencyKey_createsTwoPayments() {
        CreatePaymentRequest request = buildRequest("50.00", "cust_002", "merch_002");
        String bodyJson = toJson(request);

        paymentService.createPayment(null, bodyJson, request);
        paymentService.createPayment(null, bodyJson, request);

        assertThat(paymentRepository.count())
                .as("Without an idempotency key, two calls should create two payments")
                .isEqualTo(2L);
    }

    // ---------- Helpers ----------

    private CreatePaymentRequest buildRequest(String amount, String customerId, String merchantId) {
        CreatePaymentRequest r = new CreatePaymentRequest();
        r.setAmount(new BigDecimal(amount));
        r.setCurrency("USD");
        r.setCustomerId(customerId);
        r.setMerchantId(merchantId);
        return r;
    }

    private String toJson(Object obj) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper()
                    .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                    .writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
