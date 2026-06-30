package com.paymentplatform.fraud.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for fraud-service with DLQ support.
 * Follows the same retry-then-DLQ pattern as ledger-service and notification-service.
 *
 * Fraud-service also has a FraudRedisConfig — bean naming is explicit to avoid conflicts.
 */
@Configuration
@Slf4j
public class KafkaDlqConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private static final String DLQ_TOPIC = "payment-dlq";
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000L;

    @Bean("dlqKafkaTemplate")
    public KafkaTemplate<String, String> dlqKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public DefaultErrorHandler fraudDlqErrorHandler(
            @org.springframework.beans.factory.annotation.Qualifier("dlqKafkaTemplate")
            KafkaTemplate<String, String> dlqTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dlqTemplate,
                (record, ex) -> {
                    log.error("[FRAUD-DLQ] Sending failed record to {} | topic={} offset={} error={}",
                            DLQ_TOPIC, record.topic(), record.offset(), ex.getMessage());
                    return new org.apache.kafka.common.TopicPartition(DLQ_TOPIC, 0);
                }
        );

        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(RETRY_BACKOFF_MS, MAX_RETRIES)
        );

        handler.addNotRetryableExceptions(
                com.fasterxml.jackson.core.JsonProcessingException.class
        );

        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler fraudDlqErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(fraudDlqErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
