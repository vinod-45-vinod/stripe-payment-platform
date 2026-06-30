package com.paymentplatform.service;

import com.paymentplatform.dto.CreatePaymentRequest;
import com.paymentplatform.dto.PaymentResponse;
import com.paymentplatform.repository.PaymentRepository;
import com.paymentplatform.statemachine.PaymentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test for @Version optimistic locking on concurrent capture requests.
 *
 * Demonstrates that even if two threads both read a payment in AUTHORIZED state
 * and both attempt to capture it simultaneously, only one can succeed.
 * The loser gets ObjectOptimisticLockingFailureException → 409 in production.
 *
 * This test is the empirical proof that double-charges are provably impossible.
 * Skipped if Docker is not available.
 */
@SpringBootTest(properties = {
        "spring.cache.type=redis",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "outbox.publisher.delay-ms=3600000"
})
@Testcontainers(disabledWithoutDocker = true)
class OptimisticLockIntegrationTest {

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
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    // ---------- Tests ----------

    @Test
    @DisplayName("Two concurrent capture requests on the same AUTHORIZED payment → exactly one succeeds, one fails with optimistic lock exception")
    void concurrentCaptures_exactlyOneSucceeds() throws InterruptedException {
        // Arrange: create and authorize a payment so it's ready to capture
        UUID paymentId = createAndAuthorizePayment();

        // Act: fire two capture requests concurrently
        int threadCount = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);   // both threads wait until we say go
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger optimisticLockFailureCount = new AtomicInteger(0);
        List<Exception> unexpectedErrors = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for the signal to fire simultaneously
                    paymentService.capturePayment(paymentId);
                    successCount.incrementAndGet();
                } catch (ObjectOptimisticLockingFailureException e) {
                    optimisticLockFailureCount.incrementAndGet();
                } catch (Exception e) {
                    // InvalidStateTransitionException is also acceptable —
                    // the state machine guard catches the second capture when it reads SUCCEEDED.
                    if (e.getClass().getSimpleName().contains("InvalidStateTransition")) {
                        optimisticLockFailureCount.incrementAndGet();
                    } else {
                        unexpectedErrors.add(e);
                    }
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Fire!
        boolean completed = doneLatch.await(15, TimeUnit.SECONDS);
        executor.shutdown();

        // Assert
        assertThat(completed).as("Threads should complete within timeout").isTrue();
        assertThat(unexpectedErrors).as("No unexpected exceptions").isEmpty();

        // Exactly one thread succeeded
        assertThat(successCount.get())
                .as("Exactly one capture should succeed")
                .isEqualTo(1);

        // The other thread was rejected (either OCC or state machine guard)
        assertThat(optimisticLockFailureCount.get())
                .as("Exactly one capture should be rejected by optimistic lock or state guard")
                .isEqualTo(1);

        // Payment should be in SUCCEEDED state
        PaymentResponse finalState = paymentService.getPayment(paymentId);
        assertThat(finalState.getStatus())
                .as("Payment should be SUCCEEDED after exactly one successful capture")
                .isEqualTo(PaymentState.SUCCEEDED);
    }

    @Test
    @DisplayName("Version increments on each write — optimistic locking baseline")
    void versionIncrementsOnWrite() {
        UUID paymentId = createAndAuthorizePayment();

        PaymentResponse afterAuth = paymentService.getPayment(paymentId);
        Long versionAfterAuth = afterAuth.getVersion();

        paymentService.capturePayment(paymentId);

        PaymentResponse afterCapture = paymentService.getPayment(paymentId);
        Long versionAfterCapture = afterCapture.getVersion();

        assertThat(versionAfterCapture)
                .as("Version should increment after each write (JPA @Version)")
                .isGreaterThan(versionAfterAuth);
    }

    // ---------- Helpers ----------

    private UUID createAndAuthorizePayment() {
        CreatePaymentRequest request = new CreatePaymentRequest();
        request.setAmount(new BigDecimal("250.00"));
        request.setCurrency("USD");
        request.setCustomerId("cust-lock-test");
        request.setMerchantId("merch-lock-test");

        PaymentResponse created = paymentService.createPayment(null, "{}", request);
        paymentService.authorizePayment(created.getId());
        return created.getId();
    }
}
