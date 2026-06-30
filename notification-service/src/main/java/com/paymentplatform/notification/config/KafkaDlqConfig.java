package com.paymentplatform.notification.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
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
 * Kafka consumer configuration for notification-service with DLQ support.
 * Same retry-then-DLQ pattern as ledger-service.
 *
 * payment-created consumer intentionally swallows errors (email stub).
 * payment-captured and refund-created consumers rethrow on failure → DLQ.
 *
 * @ConditionalOnBean(ConsumerFactory.class): only active when Kafka is on the classpath
 * and autoconfigured — skipped in unit tests that exclude Kafka.
 */
@Configuration
@ConditionalOnBean(ConsumerFactory.class)
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
    public DefaultErrorHandler notificationDlqErrorHandler(
            @org.springframework.beans.factory.annotation.Qualifier("dlqKafkaTemplate")
            KafkaTemplate<String, String> dlqTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dlqTemplate,
                (record, ex) -> {
                    log.error("[NOTIFICATION-DLQ] Sending failed record to {} | topic={} offset={} error={}",
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
            DefaultErrorHandler notificationDlqErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(notificationDlqErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
