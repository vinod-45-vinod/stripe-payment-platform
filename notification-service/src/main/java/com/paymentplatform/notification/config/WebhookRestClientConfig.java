package com.paymentplatform.notification.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/**
 * Configures the RestClient used for webhook HTTP delivery.
 * Timeout is configurable via notification.webhook.delivery-timeout-seconds.
 */
@Configuration
public class WebhookRestClientConfig {

    @Value("${notification.webhook.delivery-timeout-seconds:10}")
    private int timeoutSeconds;

    @Bean
    public RestClient webhookRestClient() {
        return RestClient.builder()
                .requestInterceptor((request, body, execution) -> {
                    request.getHeaders().set("Content-Type", "application/json");
                    request.getHeaders().set("User-Agent", "PaymentPlatform-Webhook/1.0");
                    return execution.execute(request, body);
                })
                .build();
    }
}
