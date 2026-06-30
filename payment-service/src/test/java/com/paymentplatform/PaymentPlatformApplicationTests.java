package com.paymentplatform;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies the Spring context starts cleanly with H2 + a real Redis (via Testcontainers).
 * Kafka auto-configuration is excluded so no real Kafka connection is attempted —
 * the outbox scheduler is also set to a 1-hour delay to silence noise.
 * Skipped automatically if Docker is not available (disabledWithoutDocker = true).
 */
@SpringBootTest(properties = {
        "spring.cache.type=redis",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "outbox.publisher.delay-ms=3600000",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect"
})
@Testcontainers(disabledWithoutDocker = true)
class PaymentPlatformApplicationTests {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Test
    void contextLoads() {
        // Verifies the full Spring context starts without errors (H2 + Redis via Testcontainers)
    }
}
