package com.paymentplatform.config;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

/**
 * Redis configuration for payment-service.
 *
 * Spring Boot 4 uses Jackson 3 (tools.jackson.*). To avoid Jackson version
 * conflicts, we use Java Serialization for Redis values — PaymentResponse and
 * IdempotencyEntry implement Serializable.
 *
 * For the CacheManager (@Cacheable on getPayment), we use JdkSerializationRedisSerializer
 * (built into Spring Data Redis, no Jackson dependency).
 *
 * Two beans:
 * - RedisTemplate<String, Object>: used by IdempotencyService for key/value ops.
 * - CacheManager: backs @Cacheable / @CacheEvict on PaymentService (TTL 10 min).
 */
@Configuration
@EnableCaching
public class RedisConfig {

    /**
     * General-purpose RedisTemplate used by IdempotencyService.
     * Keys: plain Strings. Values: Java-serialized objects.
     * All stored types (IdempotencyEntry, PaymentResponse) implement Serializable.
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        // JdkSerializationRedisSerializer: uses Java Serialization — no Jackson dependency
        template.setValueSerializer(RedisSerializer.java());
        template.setHashValueSerializer(RedisSerializer.java());
        template.afterPropertiesSet();
        return template;
    }

    /**
     * Spring Cache abstraction backed by Redis.
     * Used by @Cacheable("payments") on PaymentService.getPayment().
     * Values serialized via Java Serialization (PaymentResponse implements Serializable).
     * TTL: 10 minutes.
     */
    @Bean
    public CacheManager cacheManager(RedisConnectionFactory connectionFactory) {
        RedisCacheConfiguration cacheConfig = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(10))
                .serializeKeysWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(RedisSerializer.java()))
                .disableCachingNullValues();

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(cacheConfig)
                .build();
    }
}
