package com.paymentplatform.service;

import com.paymentplatform.common.KafkaTopics;
import com.paymentplatform.dto.CreatePaymentRequest;
import com.paymentplatform.dto.PaymentResponse;
import com.paymentplatform.events.OutboxEventType;
import com.paymentplatform.repository.OutboxEventRepository;
import com.paymentplatform.repository.PaymentRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the Outbox Pattern.
 *
 * Demonstrates:
 * 1. Creating a payment writes an outbox_events row in the SAME transaction.
 * 2. The OutboxPublisherJob scheduler (running every 2s in this test) picks up
 *    the unpublished row and delivers it to the Kafka topic.
 * 3. A Kafka consumer can receive the event from the topic.
 * 4. The outbox row is marked published=true after successful delivery.
 *
 * Uses Testcontainers for real Kafka (KRaft mode) and Redis.
 * Skipped if Docker is not available.
 */
@SpringBootTest(properties = {
        "spring.cache.type=redis",
        "outbox.publisher.delay-ms=2000"    // Fast poll for test
})
@Testcontainers(disabledWithoutDocker = true)
class OutboxIntegrationTest {

    // ---------- Infrastructure ----------

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    // ---------- Beans ----------

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    // ---------- Setup ----------

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();
        outboxEventRepository.deleteAll();
    }

    @AfterEach
    void tearDown() {
        paymentRepository.deleteAll();
        outboxEventRepository.deleteAll();
    }

    // ---------- Tests ----------

    @Test
    @DisplayName("Creating a payment writes an outbox_events row in the same transaction")
    void createPayment_writesOutboxRow() {
        CreatePaymentRequest request = buildRequest("100.00", "cust_outbox", "merch_outbox");

        paymentService.createPayment(null, "{}", request);

        // Outbox row must exist immediately after the service call
        List<com.paymentplatform.entity.OutboxEvent> events = outboxEventRepository.findAll();
        assertThat(events)
                .as("One outbox event should be written for a created payment")
                .hasSize(1);

        com.paymentplatform.entity.OutboxEvent event = events.get(0);
        assertThat(event.getEventType()).isEqualTo(OutboxEventType.PAYMENT_CREATED);
        assertThat(event.isPublished()).isFalse();
        assertThat(event.getPayload()).contains("PAYMENT_CREATED");
    }

    @Test
    @DisplayName("OutboxPublisherJob delivers the event to Kafka and marks the row published=true")
    void outboxPublisher_deliversToKafkaAndMarksPublished() throws Exception {
        // Arrange: create a payment (writes outbox row)
        CreatePaymentRequest request = buildRequest("250.00", "cust_kafka", "merch_kafka");
        PaymentResponse created = paymentService.createPayment(null, "{}", request);
        assertThat(outboxEventRepository.countUnpublished()).isEqualTo(1);

        // Act: wait for the OutboxPublisherJob scheduler to fire (delay-ms=2000)
        // and allow a few extra seconds for the Kafka send to complete.
        Thread.sleep(8_000);

        // Assert DB: outbox row should now be published=true
        assertThat(outboxEventRepository.countUnpublished())
                .as("Outbox row should be marked published after scheduler run")
                .isEqualTo(0);

        // Assert Kafka: the event should be on the payment-created topic
        try (KafkaConsumer<String, String> consumer = buildConsumer()) {
            consumer.subscribe(Collections.singletonList(KafkaTopics.PAYMENT_CREATED));

            // Poll for up to 10 seconds to receive the message
            long deadline = System.currentTimeMillis() + 10_000;
            boolean found = false;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.key().equals(created.getId().toString())) {
                        assertThat(record.value())
                                .contains("PAYMENT_CREATED")
                                .contains(created.getId().toString());
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
            assertThat(found)
                    .as("Event should be consumable from the payment-created Kafka topic")
                    .isTrue();
        }
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

    private KafkaConsumer<String, String> buildConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }
}
