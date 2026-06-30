package com.paymentplatform;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-End integration test for the full payment flow.
 *
 * Uses real Testcontainers: PostgreSQL, Redis, Kafka.
 * Tests the full HTTP lifecycle against a running Spring Boot application:
 *   1. POST /payments → CREATED
 *   2. POST /payments/{id}/authorize → AUTHORIZED
 *   3. POST /payments/{id}/capture  → SUCCEEDED
 *   4. GET  /payments/{id}          → final state = SUCCEEDED
 *   5. Verify payment-captured Kafka event was published
 *   6. GET  /actuator/health        → status = UP
 *
 * Runs identically in CI (GitHub Actions) and locally.
 * Skipped automatically if Docker is not available.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.jpa.hibernate.ddl-auto=create-drop",
                "spring.flyway.enabled=false"
        }
)
@Testcontainers(disabledWithoutDocker = true)
class PaymentFlowE2ETest {

    // ── Testcontainers ────────────────────────────────────────────────────────

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("payment_platform")
            .withUsername("postgres")
            .withPassword("admin");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Postgres
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        // Redis
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        // Kafka
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        // Outbox: fast polling for test
        registry.add("outbox.publisher.delay-ms", () -> "500");
    }

    // ── Test dependencies ─────────────────────────────────────────────────────

    @LocalServerPort
    private int port;


    // Use RestClient (Spring Boot 4 — TestRestTemplate removed)
    private RestClient http;

    @BeforeEach
    void setUp() {
        http = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultHeader("Content-Type", "application/json")
                .build();
    }


    // ── Test: full payment lifecycle ──────────────────────────────────────────

    @Test
    @DisplayName("Full payment lifecycle: CREATE → AUTHORIZE → CAPTURE → SUCCEEDED")
    void fullPaymentFlow_createAuthorizeCapture() throws Exception {

        // 1. Create payment
        String createBody = """
                {
                  "amount": 150.00,
                  "currency": "USD",
                  "customerId": "cust_e2e_001",
                  "merchantId": "merch_e2e_001"
                }
                """;
        var createResp = http.post().uri("/payments")
                .body(createBody)
                .retrieve()
                .toEntity(java.util.Map.class);

        assertThat(createResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String paymentId = createResp.getBody().get("id").toString();
        String status = createResp.getBody().get("status").toString();
        assertThat(status).isEqualTo("CREATED");
        assertThat(paymentId).isNotBlank();

        // 2. Authorize
        var authResp = http.post().uri("/payments/" + paymentId + "/authorize")
                .retrieve().toEntity(java.util.Map.class);
        assertThat(authResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(authResp.getBody().get("status").toString()).isEqualTo("AUTHORIZED");

        // 3. Capture
        var captureResp = http.post().uri("/payments/" + paymentId + "/capture")
                .retrieve().toEntity(java.util.Map.class);
        assertThat(captureResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(captureResp.getBody().get("status").toString()).isEqualTo("SUCCEEDED");

        // 4. GET final state (tests Redis cache as well)
        var getResp = http.get().uri("/payments/" + paymentId)
                .retrieve().toEntity(java.util.Map.class);
        assertThat(getResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResp.getBody().get("status").toString()).isEqualTo("SUCCEEDED");
        assertThat(getResp.getBody().get("id").toString()).isEqualTo(paymentId);

        // 5. Verify payment-captured Kafka event was published (outbox polls every 500ms)
        Thread.sleep(3000); // Allow outbox publisher to run at least once
        boolean kafkaEventPublished = checkKafkaEvent("payment-captured", paymentId);
        assertThat(kafkaEventPublished)
                .as("payment-captured Kafka event should be published for payment " + paymentId)
                .isTrue();
    }

    @Test
    @DisplayName("Actuator /health endpoint returns UP")
    void actuatorHealth_returnsUp() {
        var healthResp = http.get().uri("/actuator/health")
                .retrieve().toEntity(java.util.Map.class);
        assertThat(healthResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(healthResp.getBody().get("status").toString()).isEqualTo("UP");
    }

    @Test
    @DisplayName("Idempotency: duplicate POST with same key returns same payment")
    void idempotencyKey_duplicateRequest_returnsSamePayment() {
        String createBody = """
                {
                  "amount": 75.00,
                  "currency": "USD",
                  "customerId": "cust_e2e_002",
                  "merchantId": "merch_e2e_001"
                }
                """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "e2e-idempotency-test-key-001");

        // First request
        var first = http.post().uri("/payments")
                .headers(h -> h.set("Idempotency-Key", "e2e-idempotency-test-key-001"))
                .body(createBody)
                .retrieve().toEntity(java.util.Map.class);
        assertThat(first.getStatusCode().is2xxSuccessful()).isTrue();
        String firstId = first.getBody().get("id").toString();

        // Second identical request with same idempotency key
        var second = http.post().uri("/payments")
                .headers(h -> h.set("Idempotency-Key", "e2e-idempotency-test-key-001"))
                .body(createBody)
                .retrieve().toEntity(java.util.Map.class);
        assertThat(second.getBody().get("id").toString()).isEqualTo(firstId);
    }

    // ── Kafka verification helper ─────────────────────────────────────────────

    /**
     * Creates a short-lived Kafka consumer and polls for a record in the given topic
     * that contains the given paymentId in its value.
     */
    private boolean checkKafkaEvent(String topic, String paymentId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 10_000; // 10s max wait
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() != null && record.value().contains(paymentId)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
