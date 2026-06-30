package com.paymentplatform.notification.service;

import com.paymentplatform.notification.entity.Merchant;
import com.paymentplatform.notification.entity.WebhookEvent;
import com.paymentplatform.notification.entity.WebhookEvent.WebhookStatus;
import com.paymentplatform.notification.repository.MerchantRepository;
import com.paymentplatform.notification.repository.WebhookEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for webhook delivery logic using the mock merchant endpoint.
 *
 * Uses H2 in-memory DB (via test properties). Kafka and Flyway are disabled.
 * MockMerchantController is active under "dev" + "test" profiles.
 *
 * Covers:
 *  1. Successful delivery on first attempt → status=DELIVERED
 *  2. Fails once then succeeds → status=DELIVERED, retryCount persisted correctly
 *  3. All retries exhausted → status=FAILED, lastError contains "MAX RETRIES EXCEEDED"
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                // H2 in-memory database (no PostgreSQL needed)
                "spring.datasource.url=jdbc:h2:mem:webhook_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.hibernate.ddl-auto=create-drop",
                "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
                "spring.jpa.show-sql=false",
                // Disable Flyway migrations (H2 schema created by JPA)
                "spring.flyway.enabled=false",
                // Point Kafka at a non-existent broker so auto-config fails fast;
                // listener is disabled so no consumer starts
                "spring.kafka.bootstrap-servers=localhost:19099",
                "spring.kafka.listener.auto-startup=false",
                // Webhook retry config — fast for tests
                "notification.webhook.max-retries=2",
                "notification.webhook.retry-delays-minutes=0,0",
                // Disable the scheduler (test controls retries manually)
                "notification.webhook.scheduler-interval-ms=999999999"
        }
)
@ActiveProfiles({"dev", "test"})
class WebhookDeliveryIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private WebhookDeliveryService webhookDeliveryService;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private MerchantRepository merchantRepository;

    private String mockWebhookUrl;

    @BeforeEach
    void setUp() {
        webhookEventRepository.deleteAll();
        merchantRepository.deleteAll();
        mockWebhookUrl = "http://localhost:" + port + "/mock/webhook";

        // Reset mock merchant failure counter
        org.springframework.web.client.RestClient.create()
                .post()
                .uri(mockWebhookUrl + "/configure?failCount=0")
                .retrieve()
                .toBodilessEntity();
    }

    private Merchant saveMerchant(String id) {
        return merchantRepository.save(Merchant.builder()
                .id(id)
                .name("Test Merchant " + id)
                .webhookUrl(mockWebhookUrl)
                .active(true)
                .build());
    }

    private WebhookEvent saveEvent(Merchant merchant) {
        return webhookEventRepository.save(WebhookEvent.builder()
                .paymentId(UUID.randomUUID())
                .merchantId(merchant.getId())
                .merchantUrl(merchant.getWebhookUrl())
                .eventType("payment.captured")
                .payload("{\"eventType\":\"payment.captured\",\"paymentId\":\"" + UUID.randomUUID() + "\"}")
                .build());
    }

    // ── Test 1: delivered on first attempt ───────────────────────────────────

    @Test
    @DisplayName("Webhook delivered on first attempt → status=DELIVERED")
    void webhook_deliveredOnFirstAttempt() {
        Merchant merchant = saveMerchant("test-merch-pass");
        WebhookEvent event = saveEvent(merchant);

        webhookDeliveryService.attemptDelivery(event);

        WebhookEvent result = webhookEventRepository.findById(event.getId()).orElseThrow();
        assertThat(result.getStatus()).isEqualTo(WebhookStatus.DELIVERED);
        assertThat(result.getRetryCount()).isZero();
        assertThat(result.getNextRetryAt()).isNull();
        assertThat(result.getLastError()).isNull();
    }

    // ── Test 2: fails once then succeeds ─────────────────────────────────────

    @Test
    @DisplayName("Webhook fails once then succeeds → final status=DELIVERED")
    void webhook_failsOnceThenSucceeds() {
        org.springframework.web.client.RestClient.create()
                .post()
                .uri(mockWebhookUrl + "/configure?failCount=1")
                .retrieve()
                .toBodilessEntity();

        Merchant merchant = saveMerchant("test-merch-flaky");
        WebhookEvent event = saveEvent(merchant);

        // Attempt 1: fails → FAILED, retryCount=1
        webhookDeliveryService.attemptDelivery(event);
        WebhookEvent afterFail = webhookEventRepository.findById(event.getId()).orElseThrow();
        assertThat(afterFail.getStatus()).isEqualTo(WebhookStatus.FAILED);
        assertThat(afterFail.getRetryCount()).isEqualTo(1);
        assertThat(afterFail.getNextRetryAt()).isNotNull();

        // Attempt 2: mock now succeeds → DELIVERED
        webhookDeliveryService.attemptDelivery(afterFail);
        WebhookEvent afterRetry = webhookEventRepository.findById(event.getId()).orElseThrow();
        assertThat(afterRetry.getStatus()).isEqualTo(WebhookStatus.DELIVERED);
    }

    // ── Test 3: all retries exhausted ────────────────────────────────────────

    @Test
    @DisplayName("All retries exhausted (max=2) → permanent FAILED with MAX RETRIES EXCEEDED")
    void webhook_exhaustsAllRetries() {
        org.springframework.web.client.RestClient.create()
                .post()
                .uri(mockWebhookUrl + "/configure?failCount=99")
                .retrieve()
                .toBodilessEntity();

        Merchant merchant = saveMerchant("test-merch-dead");
        WebhookEvent event = saveEvent(merchant);

        // Attempt 1: fails → retryCount=1
        webhookDeliveryService.attemptDelivery(event);
        event = webhookEventRepository.findById(event.getId()).orElseThrow();
        assertThat(event.getRetryCount()).isEqualTo(1);

        // Attempt 2: fails → retryCount=2
        webhookDeliveryService.attemptDelivery(event);
        event = webhookEventRepository.findById(event.getId()).orElseThrow();
        assertThat(event.getRetryCount()).isEqualTo(2);

        // Attempt 3: exceeds max (2) → permanently FAILED, nextRetryAt=null
        webhookDeliveryService.attemptDelivery(event);
        WebhookEvent finalState = webhookEventRepository.findById(event.getId()).orElseThrow();
        assertThat(finalState.getStatus()).isEqualTo(WebhookStatus.FAILED);
        assertThat(finalState.getRetryCount()).isEqualTo(3);
        assertThat(finalState.getNextRetryAt()).isNull();
        assertThat(finalState.getLastError()).contains("MAX RETRIES EXCEEDED");
    }
}
